"""
Async PostgreSQL connection pool — wraps asyncpg.Pool.

Features:
  - Lazy connect with double-checked locking
  - Helper methods for fetch/execute/fetchrow
  - Transaction context manager
  - Health check / ping
  - Graceful shutdown
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional, Dict, List

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

from .exceptions import ConnectionPoolError

logger = logging.getLogger(__name__)


async def _init_connection(conn: "asyncpg.Connection") -> None:
    """Register JSON/JSONB codecs so Python dicts are handled transparently."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


class ConnectionPool:
    """
    Async PostgreSQL connection pool.

    Usage:
        pool = ConnectionPool(host="localhost", user="postgres", password="", database="mydb")
        await pool.connect()

        rows = await pool.fetch_all("SELECT * FROM users WHERE active = $1", [True])
        count = await pool.execute("DELETE FROM sessions WHERE expired = $1", [True])

        async with pool.transaction() as conn:
            await conn.execute("INSERT INTO ...")
            await conn.execute("UPDATE ...")

        await pool.disconnect()
    """

    def __init__(
        self,
        host:            str   = "127.0.0.1",
        port:            int   = 5432,
        user:            str   = "postgres",
        password:        str   = "",
        database:        str   = "",
        minsize:         int   = 2,
        maxsize:         int   = 10,
        connect_timeout: float = 10.0,
        echo:            bool  = False,
    ):
        self._config: Dict[str, Any] = {
            "host":             host,
            "port":             port,
            "user":             user,
            "password":         password,
            "database":         database,
            "min_size":         minsize,
            "max_size":         maxsize,
            "command_timeout":  connect_timeout,
        }
        self._echo:  bool                       = echo
        self._pool:  Optional["asyncpg.Pool"]   = None
        self._lock   = asyncio.Lock()
        self._connected = False

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Create the connection pool. Safe to call multiple times."""
        async with self._lock:
            if self._connected and self._pool is not None:
                return
            try:
                self._pool = await asyncpg.create_pool(
                    **self._config,
                    init=_init_connection,
                )
                self._connected = True
                logger.info(
                    "✅ PostgreSQL pool connected  host=%s:%s  db=%s  min=%s  max=%s",
                    self._config["host"], self._config["port"],
                    self._config["database"],
                    self._config["min_size"], self._config["max_size"],
                )
            except Exception as exc:
                raise ConnectionPoolError(
                    f"Cannot connect to PostgreSQL at "
                    f"{self._config['host']}:{self._config['port']} — {exc}"
                ) from exc

    async def disconnect(self) -> None:
        """Gracefully close all connections in the pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._connected = False
            logger.info("PostgreSQL pool disconnected.")

    async def ping(self) -> bool:
        """Return True if the pool can issue a SELECT 1."""
        try:
            rows = await self.fetch_all("SELECT 1 AS ok", [])
            return len(rows) == 1
        except Exception:
            return False

    # ── Query helpers ─────────────────────────────────────────────────────

    async def fetch_all(self, sql: str, params: List[Any]) -> List[Dict[str, Any]]:
        """Execute a SELECT and return all rows as a list of dicts."""
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def fetch_one(self, sql: str, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Execute a SELECT and return the first row as a dict, or None."""
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(sql, *params)
            return dict(row) if row else None

    async def execute(self, sql: str, params: List[Any]) -> int:
        """
        Execute a DML/DDL statement and return the affected row count.
        For DDL (CREATE TABLE, ALTER TABLE), returns 0.
        """
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            result: str = await conn.execute(sql, *params)
            # asyncpg returns strings like "UPDATE 3", "DELETE 2", "INSERT 0 1"
            try:
                return int(result.split()[-1])
            except (ValueError, IndexError):
                return 0

    # ── Transaction ───────────────────────────────────────────────────────

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator["asyncpg.Connection", None]:
        """
        Yield a connection with an open transaction.
        Commits on success, rolls back on any exception.

        Usage:
            async with pool.transaction() as conn:
                await conn.execute("INSERT INTO ...")
                await conn.execute("UPDATE ...")
            # auto-committed here
        """
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction():
                yield conn

    # ── Status ────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected and self._pool is not None

    @property
    def pool_size(self) -> int:
        if self._pool is None:
            return 0
        try:
            return self._pool.get_size()
        except Exception:
            return 0

    @property
    def free_connections(self) -> int:
        if self._pool is None:
            return 0
        try:
            return self._pool.get_idle_size()
        except Exception:
            return 0

    def _assert_connected(self) -> None:
        if not self.is_connected:
            raise ConnectionPoolError(
                "Connection pool is not connected. Call connector.connect() first."
            )

    def log_query(self, sql: str, params: Optional[List[Any]] = None) -> None:
        """Print SQL queries to the console if echo is enabled."""
        if self._echo:
            cleaned_sql = " ".join(sql.split())
            print(f"\033[94m[ORM QUERY]\033[0m {cleaned_sql}")
            if params:
                print(f"\033[92m[PARAMS]   \033[0m {params}")

    def __repr__(self) -> str:
        status = "connected" if self.is_connected else "disconnected"
        return (
            f"ConnectionPool({self._config['host']}:{self._config['port']} "
            f"db={self._config['database']} [{status}])"
        )
