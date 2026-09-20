import asyncpg
import pytest

from postgres_connector import Field, PostgreSQLModel, TransactionContext
from postgres_connector.exceptions import TransactionError
from postgres_connector.registry import registry


class Account(PostgreSQLModel):
    __table__ = "tx_accounts"
    id:      int = Field(primary_key=True)
    name:    str = Field(max_length=50)
    balance: int = Field(default=0)


async def _setup(connector):
    registry.register(Account)
    await connector.migrate()


async def _teardown(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS tx_accounts")


@pytest.mark.asyncio
async def test_begin_commits_all_writes_together(connector):
    await _setup(connector)
    try:
        async with connector.begin() as tx:
            assert isinstance(tx, TransactionContext)
            await tx.create(Account, {"name": "alice", "balance": 100})
            await tx.create(Account, {"name": "bob", "balance": 50})

        rows = await connector.find(Account, {"order": ["name ASC"]})
        assert [r.name for r in rows] == ["alice", "bob"]
        assert [r.balance for r in rows] == [100, 50]
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_begin_rolls_back_all_writes_on_exception(connector):
    await _setup(connector)
    try:
        with pytest.raises(RuntimeError):
            async with connector.begin() as tx:
                await tx.create(Account, {"name": "carol", "balance": 10})
                raise RuntimeError("boom")

        count = await connector.count(Account)
        assert count == 0
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_begin_binds_update_by_id_without_session_arg(connector):
    """update_by_id inside begin() must see the not-yet-committed row from the same tx."""
    await _setup(connector)
    try:
        async with connector.begin() as tx:
            acct = await tx.create(Account, {"name": "dave", "balance": 20})
            updated = await tx.update_by_id(Account, acct.id, {"balance": 30})
            assert updated.balance == 30

        final = await connector.find_by_id(Account, acct.id)
        assert final.balance == 30
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_nested_savepoint_rolls_back_independently(connector):
    """A failing nested tx.begin() block must not abort the outer transaction."""
    await _setup(connector)
    try:
        async with connector.begin() as tx:
            outer = await tx.create(Account, {"name": "outer", "balance": 1})

            with pytest.raises(RuntimeError):
                async with tx.begin():
                    await tx.create(Account, {"name": "inner", "balance": 2})
                    raise RuntimeError("nested failure")

            # Outer transaction connection must still be usable after the
            # savepoint rollback.
            still_there = await tx.find_by_id(Account, outer.id)
            assert still_there is not None

        rows = await connector.find(Account)
        names = {r.name for r in rows}
        assert names == {"outer"}  # inner's insert was rolled back, outer's stood
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_run_transaction_retries_on_serialization_failure(connector):
    await _setup(connector)
    try:
        attempts = 0

        async def flaky(tx: TransactionContext):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise asyncpg.exceptions.SerializationError("simulated conflict")
            return await tx.create(Account, {"name": "retried", "balance": 5})

        result = await connector.run_transaction(flaky, max_retries=5)
        assert attempts == 3
        assert result.name == "retried"

        count = await connector.count(Account)
        assert count == 1  # only the successful attempt's insert survived
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_run_transaction_retries_on_connection_error(connector):
    """
    Connection-level failures (asyncpg.exceptions.InterfaceError, OSError)
    aren't asyncpg.PostgresError with a sqlstate — this proves the
    broadened exception handling in run_transaction() actually catches and
    retries them too, not just sqlstate-based serialization failures.
    """
    await _setup(connector)
    try:
        attempts = 0

        async def flaky(tx: TransactionContext):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise asyncpg.exceptions.InterfaceError("connection is closed")
            return await tx.create(Account, {"name": "reconnected", "balance": 1})

        result = await connector.run_transaction(flaky, max_retries=5)
        assert attempts == 3
        assert result.name == "reconnected"

        count = await connector.count(Account)
        assert count == 1
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_run_transaction_gives_up_after_max_retries(connector):
    await _setup(connector)
    try:
        attempts = 0

        async def always_conflicts(tx: TransactionContext):
            nonlocal attempts
            attempts += 1
            raise asyncpg.exceptions.DeadlockDetectedError("simulated deadlock")

        with pytest.raises(TransactionError):
            await connector.run_transaction(always_conflicts, max_retries=3)

        assert attempts == 3
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_run_transaction_does_not_retry_non_retryable_errors(connector):
    await _setup(connector)
    try:
        attempts = 0

        async def bad_query(tx: TransactionContext):
            nonlocal attempts
            attempts += 1
            await tx.raw("SELECT * FROM this_table_does_not_exist")

        with pytest.raises(TransactionError):
            await connector.run_transaction(bad_query, max_retries=5)

        assert attempts == 1  # undefined_table is not in the retryable set
    finally:
        await _teardown(connector)
