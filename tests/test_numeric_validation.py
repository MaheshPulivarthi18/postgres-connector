from decimal import Decimal

import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.exceptions import ConfigurationError


class Priced(PostgreSQLModel):
    __table__ = "priced_items"
    id:     int = Field(primary_key=True)
    price:  float = Field(nullable=True)
    qty:    int = Field(nullable=True)
    active: bool = Field(nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "priced_items"')
    await connector.migrate()
    yield
    await connector.raw_execute('DROP TABLE IF EXISTS "priced_items"')


@pytest.mark.asyncio
async def test_float_field_accepts_int_and_decimal(connector, table):
    item = await connector.create(Priced, {"price": 5})
    assert (await connector.find_by_id(Priced, item.id)).price == 5.0

    await connector.update_by_id(Priced, item.id, {"price": Decimal("2.5")})
    assert (await connector.find_by_id(Priced, item.id)).price == 2.5

    upserted = await connector.upsert(Priced, {"id": item.id, "price": 7}, conflict_target=["id"])
    assert upserted.price == 7.0


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["price", "qty"])
async def test_bool_is_rejected_for_numeric_fields(connector, table, field):
    with pytest.raises(ConfigurationError, match="got bool"):
        await connector.create(Priced, {field: True})


@pytest.mark.asyncio
async def test_unchanged_rules_for_other_types(connector, table):
    with pytest.raises(ConfigurationError, match="expects int, got float"):
        await connector.create(Priced, {"qty": 1.5})
    with pytest.raises(ConfigurationError, match="expects float, got str"):
        await connector.create(Priced, {"price": "5"})
    item = await connector.create(Priced, {"qty": 3, "active": True})
    assert (item.qty, item.active) == (3, True)
