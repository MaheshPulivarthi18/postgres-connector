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
import inspect
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Optional, Dict, List, Tuple

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
        command_timeout: Optional[float] = 30.0,
        echo:            bool  = False,
    ):
        # Plain connection params — reused for one-off dedicated connections
        # (LISTEN needs a connection held open outside the pool; a pooled
        # connection would be recycled for other queries while "listening").
        self._connect_kwargs: Dict[str, Any] = {
            "host":     host,
            "port":     port,
            "user":     user,
            "password": password,
            "database": database,
        }
        self._config: Dict[str, Any] = {
            **self._connect_kwargs,
            "min_size":         minsize,
            "max_size":         maxsize,
            # `timeout` = time allowed to establish a connection.
            # `command_timeout` = default per-query timeout on connections
            # from this pool (overridable per-call — see fetch_all/execute).
            "timeout":          connect_timeout,
            "command_timeout":  command_timeout,
        }
        self._echo:  bool                       = echo
        self._pool:  Optional["asyncpg.Pool"]   = None
        self._lock   = asyncio.Lock()
        self._connected = False

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Create the connection pool. Safe to call multiple times. Retries up to 3 times with exponential backoff."""
        async with self._lock:
            if self._connected and self._pool is not None:
                return

            max_retries = 3
            last_exc: Optional[Exception] = None

            for attempt in range(1, max_retries + 1):
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
                    return
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        wait = 2 ** (attempt - 1)  # 1s, 2s, 4s
                        logger.warning(
                            "⚠️  PostgreSQL connect attempt %d/%d failed: %s. Retrying in %ds...",
                            attempt, max_retries, exc, wait,
                        )
                        await asyncio.sleep(wait)

            raise ConnectionPoolError(
                f"Cannot connect to PostgreSQL at "
                f"{self._config['host']}:{self._config['port']} after {max_retries} attempts — {last_exc}"
            ) from last_exc

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

    async def fetch_all(self, sql: str, params: List[Any], timeout: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT and return all rows as a list of dicts.
        `timeout` overrides this pool's default command_timeout for just
        this call; leave it None to use the configured default.
        """
        self._assert_connected()
        self.log_query(sql, params)
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql, *params, timeout=timeout)
            return [dict(r) for r in rows]

    async def fetch_one(self, sql: str, params: List[Any], timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Execute a SELECT and return the first row as a dict, or None."""
        self._assert_connected()
        self.log_query(sql, params)
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(sql, *params, timeout=timeout)
            return dict(row) if row else None

    async def execute(self, sql: str, params: List[Any], timeout: Optional[float] = None) -> int:
        """
        Execute a DML/DDL statement and return the affected row count.
        For DDL (CREATE TABLE, ALTER TABLE), returns 0.
        """
        self._assert_connected()
        self.log_query(sql, params)
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            result: str = await conn.execute(sql, *params, timeout=timeout)
            # asyncpg returns strings like "UPDATE 3", "DELETE 2", "INSERT 0 1"
            try:
                return int(result.split()[-1])
            except (ValueError, IndexError):
                return 0

    # ── LISTEN / NOTIFY ──────────────────────────────────────────────────────

    async def notify(self, channel: str, payload: str = "") -> None:
        """Send a NOTIFY on `channel` using a pooled connection (one-shot, safe to pool)."""
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute("SELECT pg_notify($1, $2)", channel, payload)

    async def listen(self, channel: str, callback: Callable[[str], Any]) -> Tuple["asyncpg.Connection", Callable]:
        """
        Start listening on `channel` using a DEDICATED connection outside
        the pool — LISTEN registers state on a specific connection, and a
        pooled connection would be handed to other queries while it's
        supposed to be listening, silently breaking delivery.

        `callback` receives just the payload (str); it may be a plain
        function or a coroutine function.

        Returns (connection, wrapped_callback) — hang onto both, they're
        needed to unlisten() later.
        """
        self._assert_connected()
        conn = await asyncpg.connect(**self._connect_kwargs)
        await _init_connection(conn)

        def _wrapped(_connection, _pid, _channel, payload) -> None:
            try:
                result = callback(payload)
                if inspect.isawaitable(result):
                    asyncio.ensure_future(result)
            except Exception:
                logger.exception("Error in LISTEN callback for channel '%s'", channel)

        await conn.add_listener(channel, _wrapped)
        logger.info("Listening on channel '%s' via a dedicated connection.", channel)
        return conn, _wrapped

    async def unlisten(self, channel: str, conn: "asyncpg.Connection", wrapped: Callable) -> None:
        """Stop listening and release the dedicated connection opened by listen()."""
        try:
            if not conn.is_closed():
                await conn.remove_listener(channel, wrapped)
        finally:
            await conn.close()

    # ── Transaction ───────────────────────────────────────────────────────

    @asynccontextmanager
    async def transaction(
        self,
        *,
        isolation:  Optional[str] = None,
        readonly:   bool = False,
        deferrable: bool = False,
    ) -> AsyncGenerator["asyncpg.Connection", None]:
        """
        Yield a connection with an open transaction.
        Commits on success, rolls back on any exception.

        Args:
            isolation:  One of "read_committed", "repeatable_read",
                        "serializable" (asyncpg names). None uses the
                        server default (read committed).
            readonly:   Mark the transaction READ ONLY.
            deferrable: Mark the transaction DEFERRABLE (only meaningful
                        for serializable + readonly).

        Usage:
            async with pool.transaction() as conn:
                await conn.execute("INSERT INTO ...")
                await conn.execute("UPDATE ...")
            # auto-committed here
        """
        self._assert_connected()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            async with conn.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable):
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
        """Log SQL queries if echo is enabled."""
        if self._echo:
            cleaned_sql = " ".join(sql.split())
            logger.info("\033[94m[ORM QUERY]\033[0m %s", cleaned_sql)
            if params:
                logger.info("\033[92m[PARAMS]   \033[0m %s", params)

    def __repr__(self) -> str:
        status = "connected" if self.is_connected else "disconnected"
        return (
            f"ConnectionPool({self._config['host']}:{self._config['port']} "
            f"db={self._config['database']} [{status}])"
        )
