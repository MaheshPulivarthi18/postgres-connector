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
from datetime import datetime
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional, Type, TypeVar, Union, Dict, List, Tuple

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

from .model import PostgreSQLModel
from .pool import ConnectionPool
from .procedures import Procedure, procedures
from .query_builder import QueryBuilder
from .relation_resolver import RelationResolver, _dict_to_filter, _parse_include
from .transaction import TransactionContext
from .types import FilterOptions, IncludeItem
from .migrator import Migrator
from .exceptions import (
    QueryError,
    ConfigurationError,
    TransactionError,
    QueryTimeoutError,
    IntegrityError,
    UniqueConstraintError,
    ForeignKeyConstraintError,
    NotNullConstraintError,
    CheckConstraintError,
)

logger = logging.getLogger(__name__)

# SQLSTATEs worth retrying a transaction for — both indicate the transaction
# was aborted purely due to concurrent contention, not a real data problem.
_RETRYABLE_SQLSTATES = {
    "40001",  # serialization_failure (SERIALIZABLE isolation)
    "40P01",  # deadlock_detected
}

# Exception types indicating the connection itself failed (DB restart, network
# blip, connection recycled) rather than the query being wrong. Verified
# empirically against a real Postgres instance: killing the DB mid-connect
# surfaces a raw ConnectionRefusedError, while killing it under an already-open
# connection surfaces asyncpg.exceptions.InterfaceError ("connection is closed").
# Neither of those is an asyncpg.PostgresError with a sqlstate, so they need
# their own check everywhere _RETRYABLE_SQLSTATES is consulted.
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


T = TypeVar("T")


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
                    for one call via `timeout=` on raw()/raw_execute()/
                    call() — everything else uses this default.
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
        self._listeners: List[Tuple[str, Any, Any]] = []  # (channel, conn, wrapped_callback)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Open the connection pool. Call this on app startup."""
        await self._pool.connect()

    async def disconnect(self) -> None:
        """
        Close all connections, including any dedicated LISTEN connections
        opened by listen() that weren't explicitly stopped. Call this on
        app shutdown.
        """
        for channel, conn, wrapped in list(self._listeners):
            await self._pool.unlisten(channel, conn, wrapped)
        self._listeners.clear()
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

        if opts.lock and session is None:
            raise QueryError(
                "filter['lock'] requires running inside a transaction — pass session=, "
                "or use connector.begin()/run_transaction(). A lock taken outside an "
                "explicit transaction is released the instant the query finishes and "
                "has no effect."
            )

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_select(opts)

        rows      = await self._fetch(sql, params, session=session, retryable=True)
        instances = [model._from_row(row) for row in rows]

        if opts.include and instances:
            await self._resolver.resolve(instances, opts.include, model)

        return instances

    # ── stream ───────────────────────────────────────────────────────────

    async def stream(
        self,
        model:      Type[PostgreSQLModel],
        filter:     Union[dict, FilterOptions] = {},
        batch_size: int = 100,
        session:    Optional[Any] = None,
    ) -> AsyncGenerator[PostgreSQLModel, None]:
        """
        Stream rows one at a time via a server-side cursor, instead of
        materializing the whole result set in memory like find() does.
        Use for large exports, backfills, or any batch job over more rows
        than comfortably fit in memory at once.

        This holds ONE connection open for as long as you keep iterating
        — that's the trade: bounded memory in exchange for a held
        connection. Don't leave a stream half-consumed under heavy
        concurrent load on a small pool. If `session` isn't given, a
        dedicated transaction is opened and committed automatically when
        the stream ends (or closed early, e.g. via `break`).

        `include` (relation batch-loading) is not supported — resolving
        relations a row at a time would defeat the point of streaming, and
        batching them requires buffering many rows first.

        Example:
            async for user in connector.stream(User, {"where": {"active": True}}):
                await export_row(user)   # only ~batch_size rows in memory
        """
        opts = self._to_filter(filter)
        if opts.include:
            raise QueryError("stream() does not support 'include' — relation batch-loading defeats streaming.")

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_select(opts)

        if session is not None:
            async for row in self._stream_cursor(session, sql, params, batch_size):
                yield model._from_row(row)
            return

        async with self._pool.transaction() as conn:
            async for row in self._stream_cursor(conn, sql, params, batch_size):
                yield model._from_row(row)

    @staticmethod
    async def _stream_cursor(conn: Any, sql: str, params: List[Any], batch_size: int) -> AsyncGenerator[dict, None]:
        async for record in conn.cursor(sql, *params, prefetch=batch_size):
            yield dict(record)

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
        # Handle auto_now_add fields
        now = datetime.utcnow()
        for name, field_def in model._fields.items():
            if field_def.auto_now_add and name not in data:
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

        now = datetime.utcnow()

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
                    row[name] = model._field_defaults[name]
            # Skip auto-increment PK if not provided
            for name, field_def in model._fields.items():
                if field_def.auto_increment and name not in row:
                    continue
                if name in row:
                    all_columns.add(name)
            normalized.append(row)

        columns = sorted(all_columns)
        col_sql = ", ".join([f'"{c}"' for c in columns])

        # Build multi-row VALUES. JSON/JSONB values are left as native
        # Python objects — the connection-level type codec (pool.py's
        # _init_connection) handles serialization; dumping here too would
        # double-encode them into a JSON string scalar instead of an object.
        param_idx = 0
        all_params: List[Any] = []
        value_groups: List[str] = []
        for row in normalized:
            placeholders = []
            for col in columns:
                # A column some other record sets but this one omits gets the
                # column's DEFAULT (serial id, server default) — not NULL.
                if col not in row:
                    placeholders.append("DEFAULT")
                    continue
                val = row[col]
                param_idx += 1
                placeholders.append(f"${param_idx}")
                all_params.append(val)
            value_groups.append(f"({', '.join(placeholders)})")

        sql = f'INSERT INTO "{model.__table__}" ({col_sql}) VALUES {", ".join(value_groups)} RETURNING *'

        async with _translate_db_errors():
            if session:
                self._pool.log_query(sql, all_params)
                raw_rows = await session.fetch(sql, *all_params)
                rows = [dict(r) for r in raw_rows]
            else:
                rows = await self._pool.fetch_all(sql, all_params)

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
        now = datetime.utcnow()
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
                data={"deleted_at": datetime.utcnow()},
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

        now = datetime.utcnow()
        for name, field_def in model._fields.items():
            if field_def.auto_now:
                prepared[name] = now

        self._validate_field_types(model, prepared)
        return prepared

    def _validate_field_types(self, model: Type[PostgreSQLModel], data: Dict[str, Any]) -> None:
        for name, value in data.items():
            field_def = model._fields.get(name)
            if not field_def or value is None or field_def.is_json:
                continue
            # auto_now/auto_now_add are always framework-stamped as a real
            # datetime (they map to a TIMESTAMP column regardless of the
            # model declaring them `str`, which is the conventional way to
            # declare them in this ORM) — trust our own stamp rather than
            # validate it against the model's annotation.
            if field_def.auto_now or field_def.auto_now_add:
                continue
            if not isinstance(value, field_def.python_type):
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

        Example:
            user = await connector.update_by_id(User, 42, {"name": "Rajesh"})
        """
        pk = self._require_pk(model)
        prepared = self._prepare_update_data(model, data)
        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_update({pk: id}, prepared, returning_all=True)
        rows = await self._fetch(sql, params, session=session)
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
        (e.g. `update_all(Model, where, {"deleted_at": datetime.utcnow()})`
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

        Example:
            deleted = await connector.delete_by_id(User, 42)
        """
        pk       = self._require_pk(model)
        affected = await self.delete_all(model, {pk: id}, session=session)
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

    # ── LISTEN / NOTIFY ──────────────────────────────────────────────────────

    async def listen(
        self,
        channel:  str,
        callback: Callable[[str], Any],
    ) -> Callable[[], Awaitable[None]]:
        """
        Start listening on a Postgres NOTIFY channel using a dedicated
        connection — LISTEN registers state on one specific connection, so
        it can't come from the shared pool (a pooled connection would be
        handed to other queries while it's supposed to be listening).

        `callback` receives the notification payload (str). It may be a
        plain function or a coroutine function — both are supported.

        Returns an async `stop()` — call it to stop listening and release
        the dedicated connection. Any listener not explicitly stopped is
        automatically cleaned up by `disconnect()`.

        Example:
            async def on_job_ready(payload):
                print("new job:", payload)

            stop = await connector.listen("jobs_channel", on_job_ready)
            ...
            await stop()
        """
        conn, wrapped = await self._pool.listen(channel, callback)
        entry = (channel, conn, wrapped)
        self._listeners.append(entry)

        async def stop() -> None:
            await self._pool.unlisten(channel, conn, wrapped)
            if entry in self._listeners:
                self._listeners.remove(entry)

        return stop

    async def notify(
        self,
        channel: str,
        payload: str = "",
        session: Optional[Any] = None,
    ) -> None:
        """
        Send a NOTIFY on `channel` — every listener (in-process ones
        registered via `listen()`, or any other Postgres session running
        `LISTEN channel`) has its callback fire with `payload`.

        Note: NOTIFY is transactional. Sent via `session=` (or `tx.notify(...)`
        inside `begin()`/`run_transaction()`), the notification is only
        delivered once that transaction COMMITS — and silently dropped if
        it rolls back. This is usually what you want: notify listeners only
        once the data they'd go read is actually visible.

        Example:
            await connector.notify("jobs_channel", json.dumps({"job_id": 42}))
        """
        if session:
            await session.execute("SELECT pg_notify($1, $2)", channel, payload)
        else:
            await self._pool.notify(channel, payload)

    # ── Stored procedures / functions ───────────────────────────────────────

    async def call(
        self,
        proc: Union[str, Procedure],
        session: Optional[Any] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Call a registered Procedure (or look one up by name).

        - `kind="function"` with `returns=Model` runs
          `SELECT * FROM name(...)` and hands back a list of hydrated
          model instances.
        - `kind="function"` with no `returns` runs `SELECT name(...)` and
          hands back the raw scalar value.
        - `kind="procedure"` runs `CALL name(...)` and always returns None.

        Every parameter declared on the Procedure must be passed as a
        keyword argument — missing or unexpected ones raise QueryError
        before anything is sent to the database. `timeout` overrides the
        connector-wide command_timeout default for just this call — useful
        for a procedure you know can run long.

        Example:
            add_numbers = Procedure("add_numbers", params=["a", "b"])
            total = await connector.call(add_numbers, a=2, b=3)
            total = await connector.call("add_numbers", a=2, b=3)  # by name
        """
        procedure = procedures.get(proc) if isinstance(proc, str) else proc

        missing = [p for p in procedure.params if p not in kwargs]
        if missing:
            raise QueryError(f"Missing arguments for procedure '{procedure.name}': {missing}")
        extra = [k for k in kwargs if k not in procedure.params]
        if extra:
            raise QueryError(f"Unknown arguments for procedure '{procedure.name}': {extra}")

        values = [kwargs[p] for p in procedure.params]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))

        if procedure.kind == "procedure":
            await self._execute(f'CALL "{procedure.name}"({placeholders})', values, session=session, timeout=timeout)
            return None

        if procedure.returns is not None:
            rows = await self._fetch(f'SELECT * FROM "{procedure.name}"({placeholders})', values, session=session, timeout=timeout)
            return [procedure.returns._from_row(r) for r in rows]

        rows = await self._fetch(f'SELECT "{procedure.name}"({placeholders}) AS result', values, session=session, timeout=timeout)
        return rows[0]["result"] if rows else None

    async def sync_procedures(self, session: Optional[Any] = None) -> int:
        """
        Create/replace every registered Procedure that declared a `body`,
        running each one's SQL exactly as written — nothing is generated
        or inferred, this just executes what you wrote. Procedures with no
        `body` are calling-only bindings for something that must already
        exist in the database, and are skipped here.

        Deliberately separate from migrate() — that's schema (tables,
        columns, indexes), this is executable code. Write `body` as
        `CREATE OR REPLACE FUNCTION/PROCEDURE ...` so this is safe to run
        repeatedly (e.g. on every app startup), the same way migrate() is.
        This never drops a procedure that's no longer registered — same
        additive-only philosophy as migrate() never dropping a column.

        Returns the number of procedures (re)created.

        Example:
            registry.register(...)   # models, if using migrate() too
            await connector.migrate()          # schema
            await connector.sync_procedures()  # functions/procedures
        """
        count = 0
        for procedure in procedures.all().values():
            if procedure.body is None:
                continue
            await self.raw_execute(procedure.body, session=session)
            count += 1
        return count

    # ── Transactions ──────────────────────────────────────────────────────

    def transaction(self):
        """
        Async context manager for explicit transactions. Yields the raw
        asyncpg connection — use this when you want to run your own SQL
        via `conn.execute(...)` / `conn.fetch(...)`.
        Commits on clean exit, rolls back on exception.

        For typed ORM calls (find/create/update_by_id/...) inside a
        transaction, prefer `begin()` — it binds every call to the same
        connection automatically instead of requiring `session=conn`.

        Example:
            async with connector.transaction() as conn:
                await conn.execute("INSERT INTO orders ...")
                await conn.execute("UPDATE inventory SET ...")
            # auto-committed
        """
        return self._pool.transaction()

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
                        "serializable". Use "serializable" together with
                        `run_transaction()` for automatic retry on
                        conflicting concurrent writes.
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

        This does NOT retry on serialization failures / deadlocks — the
        `async with` body only runs once. Use `run_transaction()` if you
        need automatic retry.
        """
        return self._begin_ctx(isolation=isolation, readonly=readonly, deferrable=deferrable)

    @asynccontextmanager
    async def _begin_ctx(self, *, isolation, readonly, deferrable):
        async with self._pool.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable) as conn:
            yield TransactionContext(self, conn)

    async def run_transaction(
        self,
        func: Callable[[TransactionContext], Awaitable[T]],
        *,
        isolation:   Optional[str] = None,
        readonly:    bool = False,
        deferrable:  bool = False,
        max_retries: int = 3,
    ) -> T:
        """
        Run `func(tx)` inside a transaction, automatically retrying the
        *entire callback* on serialization failures / deadlocks — the two
        failure modes concurrent transactions are expected to hit under
        contention, especially with isolation="serializable".

        This is the "intelligent" transaction entry point: prefer it over
        `begin()` whenever the transaction body is idempotent (safe to
        re-run from scratch) and might race with other writers.

        Args:
            func:        async callable taking a `TransactionContext`.
                         Must be safe to call more than once — no side
                         effects outside the database (e.g. don't send an
                         email from inside `func`; do it after this
                         returns).
            max_retries: total attempts before giving up (default 3).

        Raises:
            TransactionError: if all attempts are exhausted, or a non
                              retryable database error occurs. The original
                              exception is chained via `__cause__`.

        Example:
            async def transfer(tx):
                sender = await tx.find_by_id(Account, sender_id)
                if sender.balance < amount:
                    raise InsufficientFundsError()
                await tx.update_by_id(Account, sender_id, {"balance": sender.balance - amount})
                receiver = await tx.find_by_id(Account, receiver_id)
                await tx.update_by_id(Account, receiver_id, {"balance": receiver.balance + amount})

            await connector.run_transaction(transfer, isolation="serializable")
        """
        last_exc: Optional[BaseException] = None

        for attempt in range(1, max_retries + 1):
            try:
                async with self.begin(isolation=isolation, readonly=readonly, deferrable=deferrable) as tx:
                    return await func(tx)
            except (asyncpg.PostgresError, *_RETRYABLE_CONNECTION_ERRORS) as exc:  # type: ignore[union-attr]
                last_exc = exc
                sqlstate = getattr(exc, "sqlstate", None)
                is_retryable = sqlstate in _RETRYABLE_SQLSTATES or isinstance(exc, _RETRYABLE_CONNECTION_ERRORS)
                if not is_retryable or attempt == max_retries:
                    raise TransactionError(
                        f"Transaction failed after {attempt} attempt(s): {exc}"
                    ) from exc
                wait = min(0.05 * (2 ** (attempt - 1)), 1.0)
                logger.warning(
                    "Retrying transaction after %s (sqlstate=%s, attempt %d/%d)",
                    exc.__class__.__name__, sqlstate, attempt, max_retries,
                )
                await asyncio.sleep(wait)

        # Unreachable, but keeps type checkers happy.
        raise TransactionError(f"Transaction failed: {last_exc}") from last_exc

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
                lock    = filter.lock,
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
