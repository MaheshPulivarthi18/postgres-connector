"""FilterParam's max_limit caps include scopes too (per parent), and include depth is bounded (#67)."""
import json
from typing import List

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import Depends  # noqa: E402

from postgres_connector import Field, FilterOptions, PostgreSQLModel, Relation  # noqa: E402
from postgres_connector.fastapi import FilterParam, register_exception_handlers  # noqa: E402


class CapUser(PostgreSQLModel):
    __table__ = "cap_users"
    id:     int = Field(primary_key=True)
    name:   str = Field()
    orders: List["CapOrder"] = Relation(type="hasMany", foreign_key="user_id")


class CapOrder(PostgreSQLModel):
    __table__ = "cap_orders"
    id:      int = Field(primary_key=True)
    user_id: int = Field()
    user:    "CapUser" = Relation(type="belongsTo", foreign_key="user_id")
    items:   List["CapItem"] = Relation(type="hasMany", foreign_key="order_id")


class CapItem(PostgreSQLModel):
    __table__ = "cap_items"
    id:       int = Field(primary_key=True)
    order_id: int = Field()


async def _drop(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS cap_items, cap_orders, cap_users CASCADE")


@pytest.fixture
async def client(connector):
    await _drop(connector)
    await connector.migrate()
    users = await connector.create_many(CapUser, [{"name": "a"}, {"name": "b"}])
    for u in users:
        orders = await connector.create_many(CapOrder, [{"user_id": u.id} for _ in range(20)])
        await connector.create_many(CapItem, [{"order_id": o.id} for o in orders for _ in range(20)])

    app = fastapi.FastAPI()
    register_exception_handlers(app)

    @app.get("/capped")
    async def capped(filter: FilterOptions = Depends(FilterParam(CapUser, max_limit=5, max_include_depth=2))):
        return await connector.find(CapUser, filter)

    @app.get("/uncapped")
    async def uncapped(filter: FilterOptions = Depends(FilterParam(CapUser, max_limit=None, max_include_depth=None))):
        return await connector.find(CapUser, filter)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c
    await _drop(connector)


async def get(client, path, flt):
    return await client.get(path, params={"filter": json.dumps(flt)})


async def test_include_without_scope_limit_gets_max_limit_per_parent(client):
    resp = await get(client, "/capped", {"include": ["orders"]})
    assert resp.status_code == 200
    assert [len(u["orders"]) for u in resp.json()] == [5, 5]


async def test_nested_dotted_include_is_capped_at_every_level(client):
    resp = await get(client, "/capped", {"include": ["orders.items"]})
    assert resp.status_code == 200
    users = resp.json()
    assert [len(u["orders"]) for u in users] == [5, 5]
    assert {len(o["items"]) for u in users for o in u["orders"]} == {5}


async def test_smaller_scope_limit_is_kept(client):
    resp = await get(client, "/capped", {"include": [{"relation": "orders", "scope": {"limit": 2}}]})
    assert [len(u["orders"]) for u in resp.json()] == [2, 2]


async def test_larger_scope_limit_is_400(client):
    resp = await get(client, "/capped", {"include": [{"relation": "orders", "scope": {"limit": 100000}}]})
    assert resp.status_code == 400
    assert "'limit' in the 'orders' include must be an integer no greater than 5" in resp.json()["detail"]


async def test_include_depth_is_capped(client):
    assert (await get(client, "/capped", {"include": ["orders.user"]})).status_code == 200
    resp = await get(client, "/capped", {"include": ["orders.user.orders"]})
    assert resp.status_code == 400
    assert "may nest at most 2 levels" in resp.json()["detail"]


async def test_none_disables_both_caps(client):
    resp = await get(client, "/uncapped", {"include": ["orders.user.orders"]})
    assert resp.status_code == 200
    assert [len(u["orders"]) for u in resp.json()] == [20, 20]
