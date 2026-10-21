"""#17: stream() raises the same typed errors as every other query path."""
import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.exceptions import QueryTimeoutError
from postgres_connector.registry import registry


class SlowRow(PostgreSQLModel):
    __table__ = "slow_stream_view"
    id: int = Field(primary_key=True)


# Backed by a view, not a table: keep it out of the registry (at import,
# before any test runs) so other tests' migrate() never creates a table here.
registry.unregister("SlowRow")


@pytest.fixture
async def slow_connector(connector):
    await connector.raw_execute("DROP VIEW IF EXISTS slow_stream_view")
    await connector.raw_execute("CREATE VIEW slow_stream_view AS SELECT 1 AS id FROM pg_sleep(2)")
    fast = PostgreSQLConnector(**connector._pool._connect_kwargs, command_timeout=0.5)
    await fast.connect()
    yield fast
    await fast.disconnect()
    await connector.raw_execute("DROP VIEW IF EXISTS slow_stream_view")


@pytest.mark.asyncio
async def test_stream_timeout_raises_query_timeout_error(slow_connector):
    with pytest.raises(QueryTimeoutError) as excinfo:
        async for _ in slow_connector.stream(SlowRow):
            pass
    assert excinfo.value.timeout == 0.5


@pytest.mark.asyncio
async def test_stream_timeout_inside_a_transaction_raises_query_timeout_error(slow_connector):
    with pytest.raises(QueryTimeoutError):
        async with slow_connector.begin() as tx:
            async for _ in tx.stream(SlowRow):
                pass


@pytest.mark.asyncio
async def test_find_and_stream_agree(slow_connector):
    with pytest.raises(QueryTimeoutError):
        await slow_connector.find(SlowRow)
