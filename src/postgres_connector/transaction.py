"""
TransactionContext — an ergonomic, transaction-bound view of PostgreSQLConnector.

Every CRUD call made through it is bound to the same connection and
transaction automatically, so no call can accidentally run outside it:

    async with connector.begin() as tx:
        user = await tx.create(User, {...})
        await tx.update_by_id(Account, user.account_id, {...})
    # both committed together, or both rolled back on exception

Nested `tx.begin()` calls use PostgreSQL SAVEPOINTs (asyncpg does this
natively when `Connection.transaction()` is entered while a transaction is
already active on that connection), so a nested block can roll back on its
own without aborting the outer transaction.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Type, TYPE_CHECKING, Union

from .types import FilterOptions

if TYPE_CHECKING:
    from .connector import PostgreSQLConnector
    from .model import PostgreSQLModel


class TransactionContext:
    """
    Bound to a single connection/transaction. Mirrors PostgreSQLConnector's
    CRUD surface exactly, minus the `session` parameter (it's implicit).
    """

    def __init__(self, connector: "PostgreSQLConnector", conn: Any):
        self._connector = connector
        self._conn = conn

    # ── CRUD (all implicitly bound to self._conn) ───────────────────────────

    async def find(
        self,
        model:  Type["PostgreSQLModel"],
        filter: Union[dict, FilterOptions] = {},
    ) -> List["PostgreSQLModel"]:
        return await self._connector.find(model, filter, session=self._conn)

    async def find_one(
        self,
        model:  Type["PostgreSQLModel"],
        filter: Union[dict, FilterOptions] = {},
    ) -> Optional["PostgreSQLModel"]:
        return await self._connector.find_one(model, filter, session=self._conn)

    async def find_by_id(
        self,
        model:  Type["PostgreSQLModel"],
        id:     Any,
        filter: Union[dict, FilterOptions] = {},
    ) -> Optional["PostgreSQLModel"]:
        return await self._connector.find_by_id(model, id, filter, session=self._conn)

    async def create(
        self,
        model: Type["PostgreSQLModel"],
        data:  Dict[str, Any],
    ) -> "PostgreSQLModel":
        return await self._connector.create(model, data, session=self._conn)

    async def create_many(
        self,
        model:   Type["PostgreSQLModel"],
        records: List[Dict[str, Any]],
    ) -> List["PostgreSQLModel"]:
        return await self._connector.create_many(model, records, session=self._conn)

    async def upsert(
        self,
        model:           Type["PostgreSQLModel"],
        data:            Dict[str, Any],
        conflict_target: List[str],
        update_fields:   Optional[List[str]] = None,
    ) -> Optional["PostgreSQLModel"]:
        return await self._connector.upsert(
            model, data, conflict_target, update_fields, session=self._conn
        )

    async def update_all(
        self,
        model: Type["PostgreSQLModel"],
        where: Dict[str, Any],
        data:  Dict[str, Any],
    ) -> int:
        return await self._connector.update_all(model, where, data, session=self._conn)

    async def update_by_id(
        self,
        model: Type["PostgreSQLModel"],
        id:    Any,
        data:  Dict[str, Any],
    ) -> Optional["PostgreSQLModel"]:
        return await self._connector.update_by_id(model, id, data, session=self._conn)

    async def delete_all(
        self,
        model: Type["PostgreSQLModel"],
        where: Dict[str, Any],
    ) -> int:
        return await self._connector.delete_all(model, where, session=self._conn)

    async def delete_by_id(
        self,
        model: Type["PostgreSQLModel"],
        id:    Any,
    ) -> bool:
        return await self._connector.delete_by_id(model, id, session=self._conn)

    async def count(
        self,
        model: Type["PostgreSQLModel"],
        where: Dict[str, Any] = {},
    ) -> int:
        return await self._connector.count(model, where, session=self._conn)

    async def exists(
        self,
        model: Type["PostgreSQLModel"],
        where: Dict[str, Any],
    ) -> bool:
        return await self._connector.exists(model, where, session=self._conn)

    async def raw(self, sql: str, params: List[Any] = [], timeout: Optional[float] = None) -> List[dict]:
        return await self._connector.raw(sql, params, session=self._conn, timeout=timeout)

    async def raw_execute(self, sql: str, params: List[Any] = [], timeout: Optional[float] = None) -> int:
        return await self._connector.raw_execute(sql, params, session=self._conn, timeout=timeout)

    # ── Nested transactions (savepoints) ────────────────────────────────────

    def begin(
        self,
        *,
        isolation:  Optional[str] = None,
        readonly:   bool = False,
        deferrable: bool = False,
    ):
        """
        Open a nested transaction (PostgreSQL SAVEPOINT) on the same
        connection. Rolling back the nested block only undoes its own
        writes — the outer transaction can continue and still commit.

        Example:
            async with connector.begin() as tx:
                await tx.create(User, {...})
                try:
                    async with tx.begin():
                        await tx.create(AuditLog, {...})
                        raise SomeRecoverableError()
                except SomeRecoverableError:
                    pass  # AuditLog insert rolled back; User insert still stands
        """
        return self._nested(isolation=isolation, readonly=readonly, deferrable=deferrable)

    @asynccontextmanager
    async def _nested(self, *, isolation, readonly, deferrable) -> AsyncGenerator["TransactionContext", None]:
        async with self._conn.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable):
            yield self

    def __repr__(self) -> str:
        return f"TransactionContext({self._connector!r})"
