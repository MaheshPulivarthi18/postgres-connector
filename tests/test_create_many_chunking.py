"""create_many() inserts batches over asyncpg's 32,767-argument limit, atomically (#31)."""
import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector import connector as connector_module
from postgres_connector.exceptions import UniqueConstraintError


class CmRow(PostgreSQLModel):
    __table__ = "cm_rows"
    id:   int = Field(primary_key=True)
    name: str = Field(unique=True)
    age:  int = Field(nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS cm_rows")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS cm_rows")


async def test_more_rows_than_one_statement_can_carry(connector, table):
    rows = [{"name": f"n{i}", "age": i} for i in range(22000)]  # 44,000 arguments
    created = await connector.create_many(CmRow, rows)
    assert len(created) == 22000
    assert [c.name for c in created[:3]] == ["n0", "n1", "n2"]
    assert created[-1].name == "n21999"
    assert await connector.count(CmRow) == 22000


async def test_a_failing_chunk_rolls_back_the_whole_call(connector, table, monkeypatch):
    monkeypatch.setattr(connector_module, "_MAX_QUERY_ARGS", 4)  # 2 rows per statement
    rows = [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "a"}]  # duplicate lands in the 2nd chunk
    with pytest.raises(UniqueConstraintError):
        await connector.create_many(CmRow, rows)
    assert await connector.count(CmRow) == 0


async def test_chunks_inside_a_caller_transaction(connector, table, monkeypatch):
    monkeypatch.setattr(connector_module, "_MAX_QUERY_ARGS", 4)
    async with connector.begin() as tx:
        created = await tx.create_many(CmRow, [{"name": f"t{i}"} for i in range(5)])
        assert len(created) == 5
    assert await connector.count(CmRow) == 5
