"""
PostgreSQLConnector — the main entry point for all database operations.

Features:
  - Full CRUD: find, find_one, find_by_id, create, update, delete, count, exists
  - LoopBack-style filter DSL (where, order, limit, skip, fields, include)
  - Nested relation loading with zero N+1 queries
  - Typed model instances returned everywhere
  - Raw SQL escape hatch
  - Transaction support
  - Async-first (FastAPI / asyncio ready)

Usage:
    connector = PostgreSQLConnector(host="localhost", database="mydb",
                                    user="postgres", password="secret")
    await connector.connect()

    users = await connector.find(User, {
        "where": {"active": True},
        "include": ["orders.items"],
        "order": ["name ASC"],
        "limit": 20,
    })

    for user in users:
        print(user.name, user.orders)   # fully typed
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Type, Union, Dict, List, Tuple

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

from .model import PostgreSQLModel
from .pool import ConnectionPool
from .query_builder import QueryBuilder
from .relation_resolver import RelationResolver, _dict_to_filter, _parse_include, stitch_keys, with_keys
from .transaction import TransactionContext
from .types import FilterOptions, IncludeItem
from .migrator import Migrator
from .exceptions import (
    QueryError,
    ConfigurationError,
    QueryTimeoutError,
    IntegrityError,
    UniqueConstraintError,
    ForeignKeyConstraintError,
    NotNullConstraintError,
    CheckConstraintError,
    InvalidValueError,
    ReadOnlyTransactionError,
)

logger = logging.getLogger(__name__)

# Exception types indicating the connection itself failed (DB restart, network
# blip, connection recycled) rather than the query being wrong. Verified
# empirically against a real Postgres instance: killing the DB mid-connect
# surfaces a raw ConnectionRefusedError, while killing it under an already-open
# connection surfaces asyncpg.exceptions.InterfaceError ("connection is closed").
# Read queries (find/count/exists) retry on these — see _fetch().
#
# Deliberately ConnectionError, NOT the broader OSError: ConnectionRefusedError/
# ConnectionResetError/ConnectionAbortedError/BrokenPipeError are all
# ConnectionError subclasses, but so is TimeoutError's sibling under OSError —
# and TimeoutError must NOT be retried here (a slow/timed-out query is a
# different problem than a dropped connection; blindly retrying it risks a
# retry storm against an already-struggling database). See QueryTimeoutError.
_RETRYABLE_CONNECTION_ERRORS: Tuple[type, ...] = (
    ConnectionError,
    asyncpg.exceptions.InterfaceError,
    asyncpg.exceptions.PostgresConnectionError,
    asyncpg.exceptions.TooManyConnectionsError,
) if asyncpg else (ConnectionError,)

# asyncpg exception -> typed, catchable exception. Verified against a real
# Postgres instance (triggered each violation for real, inspected the
# actual .constraint_name/.table_name/.column_name/.detail values) rather
# than assumed from the class names.
_INTEGRITY_ERROR_MAP: Dict[type, type] = {
    asyncpg.exceptions.UniqueViolationError:     UniqueConstraintError,
    asyncpg.exceptions.ForeignKeyViolationError:  ForeignKeyConstraintError,
    asyncpg.exceptions.NotNullViolationError:     NotNullConstraintError,
    asyncpg.exceptions.CheckViolationError:       CheckConstraintError,
} if asyncpg else {}


def _translate_integrity_error(exc: "asyncpg.exceptions.IntegrityConstraintViolationError") -> IntegrityError:
    target_cls = _INTEGRITY_ERROR_MAP.get(type(exc), IntegrityError)
    return target_cls(
        str(exc),
        constraint=exc.constraint_name,
        table=exc.table_name,
        column=exc.column_name,
        detail=exc.detail,
    )


# Bad values: the server's SQLSTATE class 22 errors, and asyncpg's client-side
# DataError for a Python value it can't encode as the parameter's type
# (e.g. 'abc' for an integer). The two are unrelated classes in asyncpg.
_DATA_ERRORS: Tuple[type, ...] = tuple(
    cls for cls in (
        asyncpg.exceptions.DataError,
        getattr(getattr(asyncpg.exceptions, "_base", None), "DataError", None),  # private path; guard a rename
    ) if cls is not None
) if asyncpg else ()


@asynccontextmanager
async def _translate_db_errors(timeout: Optional[float] = None):
    """
    Shared translation for the driver-call chokepoints (_fetch/_execute,
    plus create()/create_many() which bypass those for their RETURNING
    round trip): a bare TimeoutError becomes QueryTimeoutError, and a
    constraint violation becomes the matching IntegrityError subclass —
    see exceptions.py for why (typed, catchable, FastAPI-mappable errors
    instead of raw asyncpg internals).

    `timeout` is accepted explicitly (not parsed from the exception —
    asyncpg's TimeoutError doesn't carry the configured value) so the
    error message is useful even at call sites with no per-call override,
    which still pass through whatever the pool's configured default is.

    Catches `asyncio.TimeoutError` explicitly alongside the builtin
    `TimeoutError` — they're the same object on Python 3.11+, but on 3.8-3.10
    `asyncio.TimeoutError` is a distinct class (a `concurrent.futures.
    TimeoutError` subclass, not an `OSError`/`TimeoutError` one), so
    `except TimeoutError` alone silently misses it there and a raw
    `asyncio.TimeoutError` leaks out instead of `QueryTimeoutError`.
    """
    try:
        yield
    except (TimeoutError, asyncio.TimeoutError) as exc:
        raise QueryTimeoutError(f"Query exceeded its timeout ({timeout}s)", timeout=timeout) from exc
    except asyncpg.exceptions.IntegrityConstraintViolationError as exc:  # type: ignore[union-attr]
        raise _translate_integrity_error(exc) from exc
    except _DATA_ERRORS as exc:
        raise InvalidValueError(str(exc), detail=getattr(exc, "detail", None)) from exc
    except asyncpg.exceptions.ReadOnlySQLTransactionError as exc:  # type: ignore[union-attr]
        raise ReadOnlyTransactionError(str(exc)) from exc


# asyncpg's limit on bind parameters in one statement.
_MAX_QUERY_ARGS = 32767


def _utc_now() -> datetime:
    """
    The stamp for auto_now / auto_now_add: an aware UTC datetime. Those
    columns are TIMESTAMPTZ, so the instant is the same whether a row is
    written here or by a column DEFAULT in any session time zone.
    """
    return datetime.now(timezone.utc)


def _value_matches_type(value: Any, python_type: type) -> bool:
    """
    isinstance(), adjusted for numbers the way Python itself treats them:
    a `float` field accepts int and Decimal too (asyncpg encodes both into
    DOUBLE PRECISION losslessly enough — PEP 484 likewise lets int stand in
    for float), while `bool` is rejected for numeric fields even though it
    subclasses int — `True` silently stored as 1 is almost always a bug.
    """
    if python_type in (int, float) and isinstance(value, bool):
        return False
    if python_type is float:
        return isinstance(value, (int, float, Decimal))
    return isinstance(value, python_type)


class PostgreSQLConnector:
    """
    Production PostgreSQL connector with nested relation support.

    Args:
        host:       PostgreSQL server host.
        port:       PostgreSQL server port (default 5432).
        user:       Username.
        password:   Password.
        database:   Database name.
        minsize:    Minimum pool connections (default 2).
        maxsize:    Maximum pool connections (default 10).
        echo:       Log every SQL statement (debug mode).
        max_query_retries:
                    How many times a read-only query (find/count/exists)
                    retries after a transient connection failure (DB
                    restart, network blip) before giving up. 0 disables
                    retry entirely. Writes are never auto-retried outside
                    a transaction — see find()/create() docstrings. Note:
                    a query that times out is NOT treated as a retryable
                    connection failure — see command_timeout below.
        connect_timeout:
                    Seconds allowed to establish a new connection (default
                    10). Distinct from command_timeout — this is about
                    reaching the server at all, not how long a query runs.
        command_timeout:
                    Default seconds allowed per query on this connector
                    (default 30; None disables it). Exceeding it raises
                    QueryTimeoutError, not a bare TimeoutError. Override
                    for one call via `timeout=` on raw()/raw_execute() —
                    everything else uses this default.
    """

    def __init__(
        self,
        host:     str  = "127.0.0.1",
        port:     int  = 5432,
        user:     str  = "postgres",
        password: str  = "",
        database: str  = "",
        minsize:  int  = 2,
        maxsize:  int  = 10,
        echo:     bool = False,
        max_query_retries: int = 3,
        connect_timeout:   float = 10.0,
        command_timeout:   Optional[float] = 30.0,
    ):
        self._max_query_retries = max_query_retries
        self._pool = ConnectionPool(
            host     = host,
            port     = port,
            user     = user,
            password = password,
            database = database,
            minsize  = minsize,
            maxsize  = maxsize,
            echo     = echo,
            connect_timeout = connect_timeout,
            command_timeout = command_timeout,
        )
        self._resolver = RelationResolver(self._pool)
        self._migrator = Migrator(self._pool)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Open the connection pool. Call this on app startup."""
        await self._pool.connect()

    async def disconnect(self) -> None:
        """Close all connections. Call this on app shutdown."""
        await self._pool.disconnect()

    async def ping(self) -> bool:
        """Return True if the DB is reachable."""
        return await self._pool.ping()

    async def migrate(self) -> None:
        """
        Synchronize models with database.
        Detects model changes and applies CREATE/ALTER commands automatically.
        """
        await self._migrator.migrate()

    async def generate_migrations(self, directory: str) -> Optional[str]:
        """
        Generate a .sql migration file containing schema changes.
        """
        return await self._migrator.generate_migrations(directory)

    async def run_migrations(self, directory: str) -> int:
        """
        Run all pending .sql migrations from the given directory.
        """
        return await self._migrator.run_migrations(directory)

    async def rollback_migrations(self, directory: str) -> int:
        """
        Rollback the most recently applied migration.
        """
        return await self._migrator.rollback_migrations(directory)

    @property
    def is_connected(self) -> bool:
        return self._pool.is_connected

    # ── find ─────────────────────────────────────────────────────────────

    async def find(
        self,
        model:  Type[PostgreSQLModel],
        filter: Union[dict, FilterOptions] = {},
        session: Optional[Any] = None,
    ) -> List[PostgreSQLModel]:
        """
        Find all records matching the filter.

        Returns a list of typed model instances with relations populated
        if `include` is specified.

        Example:
            users = await connector.find(User, {
                "where": {"active": True, "age": {"gte": 18}},
                "include": ["orders.items.product"],
                "order": ["name ASC"],
                "limit": 50,
                "skip": 0,
            })
        """
        opts = self._to_filter(filter)
        if opts.include:
            # Keep the keys the includes are attached by, even when `fields` omits them.
            opts.fields = with_keys(opts.fields, stitch_keys(model, opts.include))

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_select(opts)

        rows      = await self._fetch(sql, params, session=session, retryable=True)
        instances = [model._from_row(row) for row in rows]

        if opts.include and instances:
            await self._resolver.resolve(instances, opts.include, model, session=session)

        return instances

    # ── find_one ──────────────────────────────────────────────────────────

    async def find_one(
        self,
        model:  Type[PostgreSQLModel],
        filter: Union[dict, FilterOptions] = {},
        session: Optional[Any] = None,
    ) -> Optional[PostgreSQLModel]:
        """
        Find the first record matching the filter, or None.

        Example:
            user = await connector.find_one(User, {
                "where": {"email": "raj@example.com"},
                "include": ["profile"],
            })
        """
        opts       = self._to_filter(filter)
        opts.limit = 1
        results    = await self.find(model, opts, session=session)
        return results[0] if results else None

    # ── find_by_id ────────────────────────────────────────────────────────

    async def find_by_id(
        self,
        model:  Type[PostgreSQLModel],
        id:     Any,
        filter: Union[dict, FilterOptions] = {},
        session: Optional[Any] = None,
    ) -> Optional[PostgreSQLModel]:
        """
        Find a record by its primary key.

        Example:
            user = await connector.find_by_id(User, 42, {
                "include": ["orders.items"]
            })
        """
        pk = self._require_pk(model)
        opts = self._to_filter(filter)
        opts.where = {**(opts.where or {}), pk: id}
        return await self.find_one(model, opts, session=session)

    # ── create ────────────────────────────────────────────────────────────

    async def create(
        self,
        model: Type[PostgreSQLModel],
        data:  Dict[str, Any],
        session: Optional[Any] = None,
    ) -> PostgreSQLModel:
        """
        Insert a new row and return the created model instance.
        The primary key is auto-populated via RETURNING.

        Example:
            user = await connector.create(User, {
                "name": "Raj",
                "email": "raj@example.com",
            })
            print(user.id)   # populated
        """
        self._reject_unknown_fields(model, data)

        # Stamp auto_now_add (unless given) and auto_now — on a copy, so the
        # caller's dict is never mutated. Same rules as upsert().
        data = dict(data)
        now = _utc_now()
        for name, field_def in model._fields.items():
            if field_def.auto_now_add and name not in data:
                data[name] = now
            if field_def.auto_now:
                data[name] = now

        self._validate_field_types(model, data)

        # Create temporary instance to trigger hooks
        instance = model(**data)
        await instance._trigger_hook("before_create")

        # Get data back from instance (hooks may have mutated it).
        # Skip auto-increment fields with no value — let PostgreSQL SERIAL generate them.
        final_data = {
            name: getattr(instance, name)
            for name, field_def in model._fields.items()
            if hasattr(instance, name)
            and not (field_def.auto_increment and getattr(instance, name) is None)
        }

        pk = model._get_primary_key()
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_insert(final_data, returning_all=True)

        async with _translate_db_errors():
            if session:
                self._pool.log_query(sql, params)
                raw_row = await session.fetchrow(sql, *params)
                row = dict(raw_row) if raw_row else None
            else:
                row = await self._pool.fetch_one(sql, params)

        if row:
            for key, val in row.items():
                if key in model._fields:
                    object.__setattr__(instance, key, val)

        await instance._trigger_hook("after_create")
        return instance

    # ── create_many ───────────────────────────────────────────────────────

    async def create_many(
        self,
        model: Type[PostgreSQLModel],
        records: List[Dict[str, Any]],
        session: Optional[Any] = None,
    ) -> List[PostgreSQLModel]:
        """
        Bulk insert multiple rows in a single INSERT statement.
        Returns a list of created model instances with PKs populated.

        Example:
            users = await connector.create_many(User, [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
            ])
        """
        if not records:
            return []
        for data in records:
            self._reject_unknown_fields(model, data)

        now = _utc_now()

        # Normalize all records: apply auto_now_add and Python-side Field
        # defaults (as create() does), collect column set
        all_columns: set = set()
        normalized: List[Dict[str, Any]] = []
        for data in records:
            row = dict(data)
            for name, field_def in model._fields.items():
                if field_def.auto_now_add and name not in row:
                    row[name] = now
                elif name not in row and name in model._field_defaults:
                    row[name] = model._default_for(name)
            # Skip auto-increment PK if not provided
            for name, field_def in model._fields.items():
                if field_def.auto_increment and name not in row:
                    continue
                if name in row:
                    all_columns.add(name)
            normalized.append(row)

        columns = sorted(all_columns)
        col_sql = ", ".join([f'"{c}"' for c in columns])

        def build_insert(chunk: List[Dict[str, Any]]) -> Tuple[str, List[Any]]:
            # Build multi-row VALUES. JSON/JSONB values are left as native
            # Python objects — the connection-level type codec (pool.py's
            # _init_connection) handles serialization; dumping here too would
            # double-encode them into a JSON string scalar instead of an object.
            param_idx = 0
            params: List[Any] = []
            value_groups: List[str] = []
            for row in chunk:
                placeholders = []
                for col in columns:
                    # A column some other record sets but this one omits gets the
                    # column's DEFAULT (serial id, server default) — not NULL.
                    if col not in row:
                        placeholders.append("DEFAULT")
                        continue
                    param_idx += 1
                    placeholders.append(f"${param_idx}")
                    params.append(row[col])
                value_groups.append(f"({', '.join(placeholders)})")
            sql = f'INSERT INTO "{model.__table__}" ({col_sql}) VALUES {", ".join(value_groups)} RETURNING *'
            return sql, params

        # asyncpg accepts at most 32,767 bind parameters per statement, so big
        # batches go out as several INSERTs. They share one transaction (the
        # caller's, if `session` is given), so create_many() stays all-or-nothing.
        per_statement = max(1, _MAX_QUERY_ARGS // max(1, len(columns)))
        chunks = [normalized[i:i + per_statement] for i in range(0, len(normalized), per_statement)]

        async def insert_all(conn: Any) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for chunk in chunks:
                sql, params = build_insert(chunk)
                self._pool.log_query(sql, params)
                out.extend(dict(r) for r in await conn.fetch(sql, *params))
            return out

        async with _translate_db_errors():
            if len(chunks) == 1:
                sql, params = build_insert(chunks[0])
                if session:
                    self._pool.log_query(sql, params)
                    rows = [dict(r) for r in await session.fetch(sql, *params)]
                else:
                    rows = await self._pool.fetch_all(sql, params)
            elif session:
                async with session.transaction():  # a savepoint inside the caller's transaction
                    rows = await insert_all(session)
            else:
                async with self._pool.transaction() as conn:
                    rows = await insert_all(conn)

        instances = [model._from_row(row) for row in rows]
        return instances

    # ── upsert ────────────────────────────────────────────────────────────

    async def upsert(
        self,
        model:           Type[PostgreSQLModel],
        data:            Dict[str, Any],
        conflict_target: List[str],
        update_fields:   Optional[List[str]] = None,
        session:         Optional[Any] = None,
    ) -> Optional[PostgreSQLModel]:
        """
        Atomic insert-or-update via `INSERT ... ON CONFLICT (...) DO UPDATE`
        — one round trip, no race between a check and a write.

        Args:
            data:            Fields to insert. `auto_now`/`auto_now_add`
                              are applied automatically, same as create().
            conflict_target: The unique/PK column(s) whose constraint
                              triggers the conflict (e.g. ["email"]). Must
                              actually have a unique or PK constraint in the
                              database — Postgres requires this to detect
                              the conflict.
            update_fields:   Columns to update when a conflict happens.
                              Defaults to every field in `data` except
                              `conflict_target`. Pass `update_fields=[]` to
                              make conflicts a no-op (`ON CONFLICT DO
                              NOTHING`) instead of an update.

        Returns the resulting row (freshly inserted, or updated on
        conflict), or None if `update_fields=[]` hit an existing
        conflicting row — nothing changed, so there's nothing to return.

        Example:
            user = await connector.upsert(
                User,
                {"email": "raj@example.com", "name": "Raj", "login_count": 1},
                conflict_target=["email"],
            )
            # calling again with the same email updates name/login_count
            # instead of raising a unique-constraint error
        """
        now = _utc_now()
        prepared = dict(data)
        for name, field_def in model._fields.items():
            if field_def.auto_now_add and name not in prepared:
                prepared[name] = now
            if field_def.auto_now:
                prepared[name] = now
        self._validate_field_types(model, prepared)

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_upsert(prepared, conflict_target, update_fields)
        rows = await self._fetch(sql, params, session=session)
        return model._from_row(rows[0]) if rows else None

    # ── update_all ────────────────────────────────────────────────────────

    async def update_all(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any],
        data:  Dict[str, Any],
        session: Optional[Any] = None,
    ) -> int:
        """
        Update all records matching `where`. Returns affected row count.

        Example:
            count = await connector.update_all(
                User,
                where={"active": False},
                data={"deleted_at": datetime.now(timezone.utc)},
            )
        """
        data = self._prepare_update_data(model, data)
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_update(where, data)
        return await self._execute(sql, params, session=session)

    def _prepare_update_data(self, model: Type[PostgreSQLModel], data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply auto_now stamps and validate field types, returning a NEW
        dict — the caller's `data` is never mutated in place.
        """
        prepared = dict(data)

        now = _utc_now()
        for name, field_def in model._fields.items():
            if field_def.auto_now:
                prepared[name] = now

        self._validate_field_types(model, prepared)
        return prepared

    @staticmethod
    def _reject_unknown_fields(model: Type[PostgreSQLModel], data: Dict[str, Any]) -> None:
        """
        Raise QueryError for keys that are not fields of `model`, so a typo in
        a write payload fails loudly instead of silently dropping the value.
        Same message as the filter and update paths.
        """
        for key in data:
            if key in model._fields:
                continue
            if key in model._relations:
                raise QueryError(
                    f"'{key}' is a relation on model '{model.__table__}'; "
                    f"relations are not writable via create(). Create the related rows separately."
                )
            raise QueryError(f"Field '{key}' does not exist on model '{model.__table__}'")

    def _validate_field_types(self, model: Type[PostgreSQLModel], data: Dict[str, Any]) -> None:
        for name, value in data.items():
            field_def = model._fields.get(name)
            if not field_def or value is None or field_def.is_json:
                continue
            # auto_now/auto_now_add are always framework-stamped as a real
            # datetime (they map to a TIMESTAMPTZ column regardless of the
            # model declaring them `str`, which is the conventional way to
            # declare them in this ORM) — trust our own stamp rather than
            # validate it against the model's annotation.
            if field_def.auto_now or field_def.auto_now_add:
                continue
            if not _value_matches_type(value, field_def.python_type):
                raise ConfigurationError(f"Field '{name}' expects {field_def.python_type.__name__}, got {type(value).__name__}")

    # ── update_by_id ──────────────────────────────────────────────────────

    async def update_by_id(
        self,
        model: Type[PostgreSQLModel],
        id:    Any,
        data:  Dict[str, Any],
        session: Optional[Any] = None,
    ) -> Optional[PostgreSQLModel]:
        """
        Update a single record by PK and return the updated instance, in
        one round trip via `UPDATE ... RETURNING *` (previously an UPDATE
        followed by a separate SELECT).

        If the model has `before_update`/`after_update` hooks (methods or
        observers), the current row is loaded first so `before_update(data)`
        can run on it (and mutate `data`), and `after_update()` runs on the
        updated row. Returns
        None without firing hooks if no row has that PK. Inside a
        transaction (`session=` / `begin()`) the row is loaded `FOR UPDATE`,
        so it can't change between the hook and the UPDATE; outside one,
        the load and the UPDATE are separate statements.

        Example:
            user = await connector.update_by_id(User, 42, {"name": "Rajesh"})
        """
        pk = self._require_pk(model)
        data = dict(data)

        has_hooks = model._has_hook("before_update") or model._has_hook("after_update")
        if has_hooks:
            current = await self._load_for_hook(model, id, session)
            if current is None:
                return None
            await current._trigger_hook("before_update", data)

        prepared = self._prepare_update_data(model, data)
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_update({pk: id}, prepared, returning_all=True)
        rows = await self._fetch(sql, params, session=session)
        if not rows:
            return None

        updated = model._from_row(rows[0])
        if has_hooks:
            await updated._trigger_hook("after_update")
        return updated

    async def _load_for_hook(self, model: Type[PostgreSQLModel], id: Any, session: Optional[Any]) -> Optional[PostgreSQLModel]:
        """
        Load the row a before_update/before_delete hook runs on. Inside a
        transaction it's locked FOR UPDATE, so the hook's view of the row
        stays true until the write (READ COMMITTED alone wouldn't stop a
        concurrent transaction changing it in between). Outside one a lock
        would be released immediately, so none is taken.
        """
        pk = self._require_pk(model)
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_select(FilterOptions(where={pk: id}, limit=1), for_update=session is not None)
        rows = await self._fetch(sql, params, session=session, retryable=session is None)
        return model._from_row(rows[0]) if rows else None

    # ── delete_all ────────────────────────────────────────────────────────

    async def delete_all(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any],
        session: Optional[Any] = None,
    ) -> int:
        """
        Delete all records matching `where`. Returns affected row count.

        Always a real `DELETE` — this connector has no opinion on soft
        delete; that's application policy, not a query-library concern. If
        you want it, filter/stamp a `deleted_at`-style column yourself
        (e.g. `update_all(Model, where, {"deleted_at": datetime.now(timezone.utc)})`
        and add `{"deleted_at": None}` to your own read filters) — it's a
        few lines in your own code, not something worth a framework hook.

        Example:
            count = await connector.delete_all(User, {"active": False})
        """
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_delete(where)
        return await self._execute(sql, params, session=session)

    # ── delete_by_id ──────────────────────────────────────────────────────

    async def delete_by_id(
        self,
        model: Type[PostgreSQLModel],
        id:    Any,
        session: Optional[Any] = None,
    ) -> bool:
        """
        Delete a record by PK. Returns True if a row was deleted.

        If the model has `before_delete`/`after_delete` hooks (methods or
        observers), the row is loaded first (`FOR UPDATE` inside a
        transaction, as in update_by_id): `before_delete` runs on it before
        the DELETE and `after_delete` after it. Returns False without firing
        either if no row has that PK.

        Example:
            deleted = await connector.delete_by_id(User, 42)
        """
        pk = self._require_pk(model)
        current = None
        if model._has_hook("before_delete") or model._has_hook("after_delete"):
            current = await self._load_for_hook(model, id, session)
            if current is None:
                return False
            await current._trigger_hook("before_delete")

        affected = await self.delete_all(model, {pk: id}, session=session)
        if affected and current is not None:
            await current._trigger_hook("after_delete")
        return affected > 0

    # ── count / exists ────────────────────────────────────────────────────

    async def count(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any] = {},
        session: Optional[Any] = None,
    ) -> int:
        """
        Count records matching `where`.

        Example:
            n = await connector.count(User, {"active": True})
        """
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_count(where)
        rows = await self._fetch(sql, params, session=session, retryable=True)
        return int(rows[0].get("_count", 0)) if rows else 0

    async def find_and_count(
        self,
        model:  Type[PostgreSQLModel],
        filter: Union[dict, FilterOptions] = {},
        session: Optional[Any] = None,
    ) -> Tuple[List[PostgreSQLModel], int]:
        """
        One page of results plus the total number of matches, for paginated
        endpoints. `filter` is the same as for find(); the count uses only
        its `where` (no limit/skip/order/include).

        Without a session the two queries run concurrently on two pooled
        connections. Inside begin() / with `session=` they run one after the
        other on that connection, since a connection runs one query at a time.

        Example:
            users, total = await connector.find_and_count(
                User, {"where": {"active": True}, "limit": 20, "skip": 40}
            )
        """
        opts = self._to_filter(filter)
        where = opts.where or {}
        if session is not None:
            items = await self.find(model, opts, session=session)
            return items, await self.count(model, where, session=session)
        items, total = await asyncio.gather(self.find(model, opts), self.count(model, where))
        return items, total

    async def exists(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any],
        session: Optional[Any] = None,
    ) -> bool:
        """
        Return True if at least one record matches `where`.

        Uses `SELECT EXISTS(...)` rather than `COUNT(*)` — Postgres can stop
        at the first match instead of scanning/counting every row.

        Example:
            taken = await connector.exists(User, {"email": "raj@example.com"})
        """
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_exists(where)
        rows = await self._fetch(sql, params, session=session, retryable=True)
        return bool(rows[0]["_exists"]) if rows else False

    # ── Raw SQL ───────────────────────────────────────────────────────────

    async def raw(
        self,
        sql:    str,
        params: List[Any] = [],
        session: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> List[dict]:
        """
        Execute a raw SELECT and return a list of plain dicts.

        Use $1, $2, ... placeholders for parameters (asyncpg style).
        `timeout` overrides the connector-wide command_timeout default for
        just this call (e.g. a report query you know runs long).

        Example:
            rows = await connector.raw(
                "SELECT u.*, COUNT(o.id) AS order_count "
                "FROM users u LEFT JOIN orders o ON o.user_id = u.id "
                "WHERE u.active = $1 GROUP BY u.id",
                [True],
            )
        """
        return await self._fetch(sql, params, session=session, timeout=timeout)

    async def raw_execute(
        self,
        sql:    str,
        params: List[Any] = [],
        session: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """
        Execute a raw INSERT / UPDATE / DELETE / DDL and return affected row count.
        `timeout` overrides the connector-wide command_timeout default for
        just this call.
        """
        return await self._execute(sql, params, session=session, timeout=timeout)

    # ── Transactions ──────────────────────────────────────────────────────

    def begin(
        self,
        *,
        isolation:  Optional[str] = None,
        readonly:   bool = False,
        deferrable: bool = False,
    ):
        """
        Async context manager yielding a `TransactionContext` — a view of
        this connector whose find/create/update/delete/raw calls are all
        implicitly bound to one connection/transaction, so you never pass
        `session=` and can never accidentally run a write outside the
        transaction.

        Args:
            isolation:  "read_committed" (default), "repeatable_read", or
                        "serializable".
            readonly:   Mark the transaction READ ONLY.
            deferrable: Only meaningful with isolation="serializable" and
                        readonly=True — lets Postgres delay the snapshot to
                        avoid serialization failures entirely.

        Example:
            async with connector.begin() as tx:
                order = await tx.create(Order, {"user_id": user.id, "total": 42})
                await tx.update_by_id(Account, user.account_id, {
                    "balance": account.balance - order.total,
                })
            # both writes committed together, or both rolled back

        Nested `async with tx.begin():` blocks use SAVEPOINTs, so a nested
        block can roll back independently without aborting the whole
        transaction. See `TransactionContext.begin()`.
        """
        return self._begin_ctx(isolation=isolation, readonly=readonly, deferrable=deferrable)

    @asynccontextmanager
    async def _begin_ctx(self, *, isolation, readonly, deferrable):
        async with self._pool.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable) as conn:
            yield TransactionContext(self, conn)

    # ── Pool status ───────────────────────────────────────────────────────

    @property
    def pool_size(self) -> int:
        return self._pool.pool_size

    @property
    def free_connections(self) -> int:
        return self._pool.free_connections

    # ── Internal helpers ──────────────────────────────────────────────────

    def _to_filter(self, filter: Union[dict, FilterOptions]) -> FilterOptions:
        if isinstance(filter, FilterOptions):
            return FilterOptions(
                where   = filter.where,
                order   = filter.order,
                limit   = filter.limit,
                skip    = filter.skip,
                fields  = filter.fields,
                include = filter.include,
            )
        if isinstance(filter, dict):
            return _dict_to_filter(filter)
        raise ConfigurationError(
            f"filter must be a dict or FilterOptions, got {type(filter).__name__}"
        )

    def _require_pk(self, model: Type[PostgreSQLModel]) -> str:
        pk = model._get_primary_key()
        if not pk:
            raise QueryError(
                f"Model '{model.__name__}' has no primary key defined."
            )
        return pk

    async def _fetch(
        self,
        sql:       str,
        params:    List[Any],
        session:   Optional[Any] = None,
        retryable: bool = False,
        timeout:   Optional[float] = None,
    ) -> List[dict]:
        """
        `retryable=True` is ONLY safe for read-only queries with no
        `session` (i.e. not part of a caller-managed transaction). It must
        never be set for a write's RETURNING round trip (create,
        update_by_id, upsert, ...) — retrying a write blindly risks
        double-executing it if the connection dropped after the server
        committed but before the client saw the acknowledgment.

        `timeout=None` uses the connector-wide default (command_timeout).

        Constraint violations (unique/FK/not-null/check) are translated
        into typed exceptions (UniqueConstraintError, ...), and a timeout
        into QueryTimeoutError — see exceptions.py — so callers never have
        to catch raw asyncpg/TimeoutError internals.
        """
        async with _translate_db_errors(timeout=timeout):
            if session:
                self._pool.log_query(sql, params)
                rows = await session.fetch(sql, *params, timeout=timeout)
                return [dict(r) for r in rows]

            if not retryable or self._max_query_retries <= 0:
                return await self._pool.fetch_all(sql, params, timeout=timeout)

            last_exc: Optional[BaseException] = None
            for attempt in range(1, self._max_query_retries + 1):
                try:
                    return await self._pool.fetch_all(sql, params, timeout=timeout)
                except _RETRYABLE_CONNECTION_ERRORS as exc:
                    last_exc = exc
                    if attempt == self._max_query_retries:
                        raise
                    wait = min(0.05 * (2 ** (attempt - 1)), 1.0)
                    logger.warning(
                        "Retrying query after %s (attempt %d/%d): %s",
                        exc.__class__.__name__, attempt, self._max_query_retries, exc,
                    )
                    await asyncio.sleep(wait)
            raise last_exc  # pragma: no cover — unreachable, keeps type checkers happy

    async def _execute(
        self,
        sql:     str,
        params:  List[Any],
        session: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """
        Constraint violations and timeouts are translated into typed
        exceptions — see `_fetch()`'s docstring for details.
        """
        async with _translate_db_errors(timeout=timeout):
            if session:
                self._pool.log_query(sql, params)
                result = await session.execute(sql, *params, timeout=timeout)
                try:
                    return int(result.split()[-1])
                except (ValueError, IndexError, AttributeError):
                    return 0
            return await self._pool.execute(sql, params, timeout=timeout)

    def __repr__(self) -> str:
        return f"PostgreSQLConnector({self._pool!r})"
