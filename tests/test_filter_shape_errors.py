"""Malformed filters are a QueryError (HTTP 400 through FilterParam), never AttributeError or a raw asyncpg error (#66)."""
import json

import pytest

from postgres_connector import Field, FilterOptions, PostgreSQLModel
from postgres_connector.exceptions import QueryError
from postgres_connector.registry import registry


class ShapeUser(PostgreSQLModel):
    __table__ = "shape_users"
    id:   int = Field(primary_key=True)
    name: str = Field()


BAD_FILTERS = [
    ({"order": [1]}, "'order' items must be strings"),
    ({"order": {"name": 1}}, "'order' must be a string or a list"),
    ({"where": []}, "'where' must be an object"),
    ({"where": "x"}, "'where' must be an object"),
    ({"where": {"and": [1]}}, "'where' must be an object"),
    ({"where": {"or": ["x"]}}, "'where' must be an object"),
    ({"order": ["and"]}, "Field 'and' does not exist"),
    ({"fields": ["or"]}, "Field 'or' does not exist"),
    ({"where": {"or": 1}}, "'or' clause must be a list"),
]


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS shape_users")
    await connector.migrate()
    await connector.create(ShapeUser, {"name": "raj"})
    try:
        yield
    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS shape_users")


@pytest.mark.parametrize("flt, message", BAD_FILTERS)
async def test_find_raises_query_error(connector, table, flt, message):
    with pytest.raises(QueryError, match=message):
        await connector.find(ShapeUser, flt)


async def test_filter_options_from_python_are_checked_too(connector, table):
    with pytest.raises(QueryError, match="'where' must be an object"):
        await connector.find(ShapeUser, FilterOptions(where=[]))


async def test_empty_where_object_is_still_no_condition(connector, table):
    assert len(await connector.find(ShapeUser, {"where": {}})) == 1
    assert len(await connector.find(ShapeUser, {"where": {"and": []}})) == 1


async def test_and_or_still_work_as_where_keys(connector, table):
    rows = await connector.find(ShapeUser, {"where": {"or": [{"name": "raj"}, {"name": "x"}], "and": [{"id": {"gt": 0}}]}})
    assert [r.name for r in rows] == ["raj"]


fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")


@pytest.mark.parametrize("flt, message", BAD_FILTERS)
async def test_filter_param_answers_400(connector, table, flt, message):
    from fastapi import Depends

    from postgres_connector.fastapi import FilterParam, register_exception_handlers

    app = fastapi.FastAPI()
    register_exception_handlers(app)

    @app.get("/users")
    async def list_users(filter: FilterOptions = Depends(FilterParam(ShapeUser))):
        return await connector.find(ShapeUser, filter)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.get("/users", params={"filter": json.dumps(flt)})
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"] == "QueryError"
    assert message in resp.json()["detail"]
