import pytest

from postgres_connector import Field, JSONField, PostgreSQLModel


class BulkRow(PostgreSQLModel):
    __table__ = "bulk_rows"
    id:    int = Field(primary_key=True)
    name:  str = Field()
    score: int = Field(nullable=False)
    note:  str = Field(nullable=True)
    tier:  str = Field(default="free")
    meta:  dict = JSONField(default={})


@pytest.fixture
async def table(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "bulk_rows"')
    await connector.migrate()
    # A server-side default the model doesn't know about.
    await connector.raw_execute('ALTER TABLE "bulk_rows" ALTER COLUMN "score" SET DEFAULT 10')
    yield
    await connector.raw_execute('DROP TABLE IF EXISTS "bulk_rows"')


@pytest.mark.asyncio
async def test_omitted_columns_use_the_column_default_not_null(connector, table):
    rows = await connector.create_many(BulkRow, [
        {"id": 100, "name": "a", "score": 1, "note": "n"},
        {"name": "b"},  # omits id, score and note, which the first record sets
    ])
    a, b = rows
    assert (a.id, a.score, a.note) == (100, 1, "n")
    assert b.id is not None and b.id != 100  # serial default, not NULL
    assert b.score == 10                     # server default, not a NOT NULL violation
    assert b.note is None                    # nullable column without default


@pytest.mark.asyncio
async def test_python_field_defaults_apply_like_create(connector, table):
    single = await connector.create(BulkRow, {"name": "s", "score": 1})
    a, b = await connector.create_many(BulkRow, [
        {"name": "a", "score": 1, "tier": "pro", "meta": {"k": 1}},
        {"name": "b", "score": 1},
    ])
    assert (a.tier, a.meta) == ("pro", {"k": 1})
    assert (b.tier, b.meta) == (single.tier, single.meta) == ("free", {})


@pytest.mark.asyncio
async def test_explicit_none_is_still_inserted_as_null(connector, table):
    (row,) = await connector.create_many(BulkRow, [{"name": "a", "score": 1, "note": None}])
    assert row.note is None
