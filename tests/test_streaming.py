import asyncio
import os

import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.exceptions import QueryError
from postgres_connector.registry import registry


class StreamRow(PostgreSQLModel):
    __table__ = "stream_rows"
    id:    int = Field(primary_key=True)
    value: int = Field()


async def _setup(connector, count=25):
    registry.register(StreamRow)
    await connector.migrate()
    await connector.create_many(StreamRow, [{"value": i} for i in range(count)])


async def _teardown(connector):
    await connector.raw_execute("DROP TABLE stream_rows")


@pytest.mark.asyncio
async def test_stream_yields_all_matching_rows_in_order(connector):
    await _setup(connector, count=25)
    try:
        seen = []
        async for row in connector.stream(StreamRow, {"order": ["value ASC"]}, batch_size=4):
            seen.append(row.value)

        assert seen == list(range(25))
        assert all(isinstance(v, int) for v in seen)
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_respects_where_filter(connector):
    await _setup(connector, count=25)
    try:
        seen = []
        async for row in connector.stream(StreamRow, {"where": {"value": {"gte": 20}}, "order": ["value ASC"]}):
            seen.append(row.value)

        assert seen == [20, 21, 22, 23, 24]
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_rejects_include(connector):
    await _setup(connector, count=1)
    try:
        with pytest.raises(QueryError):
            async for _ in connector.stream(StreamRow, {"include": ["whatever"]}):
                pass
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_can_be_stopped_early_with_break(connector):
    await _setup(connector, count=25)
    try:
        seen = []
        async for row in connector.stream(StreamRow, {"order": ["value ASC"]}, batch_size=3):
            seen.append(row.value)
            if len(seen) == 5:
                break

        assert seen == [0, 1, 2, 3, 4]

        # The connection used by the stream must have been released back
        # to the pool cleanly (not leaked) — a subsequent normal query
        # must still work.
        count = await connector.count(StreamRow)
        assert count == 25
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_break_actually_releases_the_connection(connector):
    """
    Rigorous version of the test above: use a pool with exactly ONE
    connection. If breaking out of stream() early didn't release it, this
    next query would hang forever waiting for a free connection — bounded
    with a timeout so a real leak fails loudly instead of hanging the suite.
    """
    await _setup(connector, count=25)

    solo = PostgreSQLConnector(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=os.getenv("DB_NAME", "test_db"),
        minsize=1,
        maxsize=1,
    )
    await solo.connect()
    try:
        seen = []
        async for row in solo.stream(StreamRow, {"order": ["value ASC"]}, batch_size=3):
            seen.append(row.value)
            if len(seen) == 5:
                break

        # With maxsize=1, this can only succeed if the stream's connection
        # was actually returned to the pool.
        count = await asyncio.wait_for(solo.count(StreamRow), timeout=2)
        assert count == 25
    finally:
        await solo.disconnect()
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_inside_transaction_via_tx(connector):
    await _setup(connector, count=10)
    try:
        seen = []
        async with connector.begin() as tx:
            async for row in tx.stream(StreamRow, {"order": ["value ASC"]}, batch_size=2):
                seen.append(row.value)

        assert seen == list(range(10))
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_stream_empty_result_set(connector):
    await _setup(connector, count=0)
    try:
        seen = []
        async for row in connector.stream(StreamRow, {}):
            seen.append(row)
        assert seen == []
    finally:
        await _teardown(connector)
