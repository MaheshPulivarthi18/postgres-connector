"""FilterParam: LoopBack-style ?filter= for FastAPI routes, validated, with 400 on bad input (#43)."""
import json
from typing import List

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import Depends  # noqa: E402

from postgres_connector import Field, FilterOptions, PostgreSQLModel, Relation  # noqa: E402
from postgres_connector.fastapi import FilterParam, register_exception_handlers  # noqa: E402


class FpUser(PostgreSQLModel):
    __table__ = "fp_users"
    id:     int = Field(primary_key=True)
    name:   str = Field()
    age:    int = Field(nullable=True)
    orders: List["FpOrder"] = Relation(type="hasMany", foreign_key="user_id")


class FpOrder(PostgreSQLModel):
    __table__ = "fp_orders"
    id:      int   = Field(primary_key=True)
    user_id: int   = Field()
    total:   float = Field()


@pytest.fixture
async def client(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS fp_orders, fp_users")
    await connector.migrate()
    users = await connector.create_many(FpUser, [{"name": f"u{i}", "age": 20 + i} for i in range(5)])
    await connector.create(FpOrder, {"user_id": users[0].id, "total": 9.5})

    app = fastapi.FastAPI()
    register_exception_handlers(app)

    @app.get("/users")
    async def list_users(filter: FilterOptions = Depends(FilterParam(FpUser, max_limit=3))):
        return [u.to_dict() for u in await connector.find(FpUser, filter)]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c
    await connector.raw_execute("DROP TABLE IF EXISTS fp_orders, fp_users")


async def get(client, flt):
    params = {} if flt is None else {"filter": flt if isinstance(flt, str) else json.dumps(flt)}
    return await client.get("/users", params=params)


async def test_valid_filter_with_include(client):
    resp = await get(client, {"where": {"age": {"gte": 22}}, "order": ["age ASC"], "include": ["orders"], "limit": 2})
    assert resp.status_code == 200
    assert [u["name"] for u in resp.json()] == ["u2", "u3"]
    assert resp.json()[0]["orders"] == []


async def test_missing_filter_applies_max_limit(client):
    resp = await get(client, None)
    assert resp.status_code == 200 and len(resp.json()) == 3


@pytest.mark.parametrize(
    "flt, message",
    [
        ("{not json", "not valid JSON"),
        ("[1, 2]", "must be a JSON object"),
        ({"wher": {"name": "u1"}}, "Unknown filter key"),
        ({"where": {"nope": 1}}, "Field 'nope' does not exist"),
        ({"where": {"age": {"bigger": 1}}}, "Unknown operator 'bigger'"),
        ({"order": ["nope ASC"]}, "nope"),
        ({"include": ["invoices"]}, "Relation 'invoices' is not defined"),
        ({"include": [{"relation": "orders", "scope": {"where": {"amount": 1}}}]}, "Field 'amount' does not exist"),
        ({"limit": 50}, "no greater than 3"),
        ({"limit": "2"}, "no greater than 3"),
    ],
)
async def test_bad_filters_are_400(client, flt, message):
    resp = await get(client, flt)
    assert resp.status_code == 400, resp.text
    assert message in resp.json()["detail"]


async def test_bad_value_is_400_through_the_error_handlers(client):
    resp = await get(client, {"where": {"id": "abc"}})
    assert resp.status_code == 400
    assert resp.json()["error"] == "InvalidValueError"


async def test_filter_is_documented_in_openapi(client):
    schema = (await client.get("/openapi.json")).json()
    params = schema["paths"]["/users"]["get"]["parameters"]
    assert [p["name"] for p in params] == ["filter"]
    assert "LoopBack-style filter" in params[0]["description"]
