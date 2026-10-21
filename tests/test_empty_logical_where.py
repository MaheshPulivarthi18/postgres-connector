import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.exceptions import QueryError


class LogicItem(PostgreSQLModel):
    __table__ = "logic_items"
    id:   int = Field(primary_key=True)
    name: str = Field()
    qty:  int = Field()


@pytest.fixture
async def items(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "logic_items"')
    await connector.migrate()
    await connector.create_many(LogicItem, [
        {"name": "a", "qty": 1},
        {"name": "b", "qty": 2},
        {"name": "c", "qty": 3},
    ])
    yield
    await connector.raw_execute('DROP TABLE IF EXISTS "logic_items"')


async def _names(connector, where):
    rows = await connector.find(LogicItem, {"where": where, "order": ["name ASC"]})
    return [r.name for r in rows]


@pytest.mark.asyncio
async def test_empty_and_matches_everything(connector, items):
    assert await _names(connector, {"and": []}) == ["a", "b", "c"]
    assert await _names(connector, {"and": [], "qty": {"gte": 2}}) == ["b", "c"]
    assert await _names(connector, {"and": [{}, {"qty": 1}]}) == ["a"]
    assert await connector.count(LogicItem, {"and": []}) == 3


@pytest.mark.asyncio
async def test_empty_or_matches_nothing(connector, items):
    assert await _names(connector, {"or": []}) == []
    assert await connector.exists(LogicItem, {"or": []}) is False


@pytest.mark.asyncio
async def test_or_with_an_empty_member_matches_everything_and_keeps_params_aligned(connector, items):
    # The discarded member's param must not shift the later "$N" placeholders.
    assert await _names(connector, {"or": [{"qty": 1}, {}], "name": {"neq": "b"}}) == ["a", "c"]


@pytest.mark.asyncio
async def test_unconditional_logical_where_cannot_delete_or_update_everything(connector, items):
    with pytest.raises(QueryError, match="DELETE without a WHERE"):
        await connector.delete_all(LogicItem, {"and": []})
    with pytest.raises(QueryError, match="UPDATE without a WHERE"):
        await connector.update_all(LogicItem, {"and": [{}]}, {"qty": 0})
    assert await connector.count(LogicItem) == 3
    # An empty OR matches nothing, so this is a harmless no-op.
    assert await connector.delete_all(LogicItem, {"or": []}) == 0
