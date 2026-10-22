"""Fields a `fields` projection leaves out are absent, not None, and projected responses pass generated models (#68)."""
import json
from typing import List

import pytest

from postgres_connector import Field, FilterOptions, PostgreSQLModel


class SelUser(PostgreSQLModel):
    __table__ = "sel_users"
    id:    int = Field(primary_key=True)
    name:  str = Field(nullable=False)
    email: str = Field(nullable=False)
    bio:   str = Field(nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS sel_users")
    await connector.migrate()
    await connector.create(SelUser, {"name": "raj", "email": "raj@x.io", "bio": None})
    try:
        yield
    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS sel_users")


async def test_unselected_fields_are_omitted_but_real_nulls_stay(connector, table):
    [user] = await connector.find(SelUser, {"fields": ["id", "name", "bio"]})
    assert user.to_dict() == {"id": 1, "name": "raj", "bio": None}   # bio is a real NULL
    assert dict(user) == user.to_dict()


async def test_reading_an_unselected_field_says_so(connector, table):
    [user] = await connector.find(SelUser, {"fields": {"email": False}})
    assert not hasattr(user, "email")
    with pytest.raises(AttributeError, match=r"'SelUser.email' was not selected"):
        user.email


async def test_without_projection_every_field_is_present(connector, table):
    [user] = await connector.find(SelUser, {})
    assert user.to_dict() == {"id": 1, "name": "raj", "email": "raj@x.io", "bio": None}
    assert SelUser(name="new").to_dict() == {"id": None, "name": "new", "email": None, "bio": None}


fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")


def test_all_optional_schema_has_no_required_fields():
    assert SelUser.pydantic_model().model_json_schema()["required"] == ["id", "name", "email"]
    assert "required" not in SelUser.pydantic_model(all_optional=True).model_json_schema()


@pytest.fixture
async def client(connector, table):
    from fastapi import Depends

    from postgres_connector.fastapi import FilterParam

    app = fastapi.FastAPI()
    UserOut = SelUser.pydantic_model(all_optional=True)

    @app.get("/plain")
    async def plain(filter: FilterOptions = Depends(FilterParam(SelUser))):
        return await connector.find(SelUser, filter)

    @app.get("/typed", response_model=List[UserOut], response_model_exclude_unset=True)
    async def typed(filter: FilterOptions = Depends(FilterParam(SelUser))):
        return await connector.find(SelUser, filter)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c


@pytest.mark.parametrize("path", ["/plain", "/typed"])
async def test_projected_response_omits_unselected_fields(client, path):
    resp = await client.get(path, params={"filter": json.dumps({"fields": ["id", "name"]})})
    assert resp.status_code == 200, resp.text
    assert resp.json() == [{"id": 1, "name": "raj"}]
    resp = await client.get(path, params={"filter": json.dumps({"fields": {"email": False}})})
    assert resp.json() == [{"id": 1, "name": "raj", "bio": None}]
    resp = await client.get(path)
    assert resp.json() == [{"id": 1, "name": "raj", "email": "raj@x.io", "bio": None}]
