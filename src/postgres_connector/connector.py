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

import logging
from datetime import datetime
from typing import Any, Optional, Type, Union, Dict, List

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

from .model import PostgreSQLModel
from .pool import ConnectionPool
from .query_builder import QueryBuilder
from .relation_resolver import RelationResolver, _dict_to_filter, _parse_include
from .types import FilterOptions, IncludeItem
from .migrator import Migrator
from .exceptions import QueryError, ConfigurationError, TransactionError

logger = logging.getLogger(__name__)


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
    ):
        self._pool = ConnectionPool(
            host     = host,
            port     = port,
            user     = user,
            password = password,
            database = database,
            minsize  = minsize,
            maxsize  = maxsize,
            echo     = echo,
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

        # Apply soft delete filter by default if model has deleted_at
        if "deleted_at" in model._fields:
            if not opts.where:
                opts.where = {}
            if "deleted_at" not in opts.where:
                opts.where["deleted_at"] = None

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_select(opts)

        rows      = await self._fetch(sql, params, session=session)
        instances = [model._from_row(row) for row in rows]

        if opts.include and instances:
            await self._resolver.resolve(instances, opts.include, model)

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
        # Handle auto_now_add fields
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for name, field_def in model._fields.items():
            if field_def.auto_now_add and name not in data:
                data[name] = now

        # Validate input data
        for name, value in data.items():
            field_def = model._fields.get(name)
            if field_def and value is not None and not field_def.is_json:
                if not isinstance(value, field_def.python_type):
                    raise ConfigurationError(f"Field '{name}' expects {field_def.python_type.__name__}, got {type(value).__name__}")

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
        # Handle auto_now fields
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for name, field_def in model._fields.items():
            if field_def.auto_now:
                data[name] = now

        # Validate input data
        for name, value in data.items():
            field_def = model._fields.get(name)
            if field_def and value is not None and not field_def.is_json:
                if not isinstance(value, field_def.python_type):
                    raise ConfigurationError(f"Field '{name}' expects {field_def.python_type.__name__}, got {type(value).__name__}")

        qb = QueryBuilder(model.__table__, model._fields)
        sql, params = qb.build_update(where, data)
        return await self._execute(sql, params, session=session)

    # ── update_by_id ──────────────────────────────────────────────────────

    async def update_by_id(
        self,
        model: Type[PostgreSQLModel],
        id:    Any,
        data:  Dict[str, Any],
        session: Optional[Any] = None,
    ) -> Optional[PostgreSQLModel]:
        """
        Update a single record by PK and return the updated instance.

        Example:
            user = await connector.update_by_id(User, 42, {"name": "Rajesh"})
        """
        pk = self._require_pk(model)
        await self.update_all(model, {pk: id}, data, session=session)
        return await self.find_by_id(model, id, session=session)

    # ── delete_all ────────────────────────────────────────────────────────

    async def delete_all(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any],
        session: Optional[Any] = None,
    ) -> int:
        """
        Delete all records matching `where`. Returns affected row count.

        Example:
            count = await connector.delete_all(User, {"active": False})
        """
        if "deleted_at" in model._fields:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            return await self.update_all(model, where, {"deleted_at": now}, session=session)

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
        rows = await self._fetch(sql, params, session=session)
        return int(rows[0].get("_count", 0)) if rows else 0

    async def exists(
        self,
        model: Type[PostgreSQLModel],
        where: Dict[str, Any],
        session: Optional[Any] = None,
    ) -> bool:
        """
        Return True if at least one record matches `where`.

        Example:
            taken = await connector.exists(User, {"email": "raj@example.com"})
        """
        return await self.count(model, where, session=session) > 0

    # ── Raw SQL ───────────────────────────────────────────────────────────

    async def raw(
        self,
        sql:    str,
        params: List[Any] = [],
        session: Optional[Any] = None,
    ) -> List[dict]:
        """
        Execute a raw SELECT and return a list of plain dicts.

        Use $1, $2, ... placeholders for parameters (asyncpg style).

        Example:
            rows = await connector.raw(
                "SELECT u.*, COUNT(o.id) AS order_count "
                "FROM users u LEFT JOIN orders o ON o.user_id = u.id "
                "WHERE u.active = $1 GROUP BY u.id",
                [True],
            )
        """
        return await self._fetch(sql, params, session=session)

    async def raw_execute(
        self,
        sql:    str,
        params: List[Any] = [],
        session: Optional[Any] = None,
    ) -> int:
        """
        Execute a raw INSERT / UPDATE / DELETE / DDL and return affected row count.
        """
        return await self._execute(sql, params, session=session)

    # ── Transactions ──────────────────────────────────────────────────────

    def transaction(self):
        """
        Async context manager for explicit transactions.
        Commits on clean exit, rolls back on exception.

        Example:
            async with connector.transaction() as conn:
                await conn.execute("INSERT INTO orders ...")
                await conn.execute("UPDATE inventory SET ...")
            # auto-committed
        """
        return self._pool.transaction()

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

    async def _fetch(self, sql: str, params: List[Any], session: Optional[Any] = None) -> List[dict]:
        if session:
            self._pool.log_query(sql, params)
            rows = await session.fetch(sql, *params)
            return [dict(r) for r in rows]
        return await self._pool.fetch_all(sql, params)

    async def _execute(self, sql: str, params: List[Any], session: Optional[Any] = None) -> int:
        if session:
            self._pool.log_query(sql, params)
            result = await session.execute(sql, *params)
            try:
                return int(result.split()[-1])
            except (ValueError, IndexError, AttributeError):
                return 0
        return await self._pool.execute(sql, params)

    def __repr__(self) -> str:
        return f"PostgreSQLConnector({self._pool!r})"
