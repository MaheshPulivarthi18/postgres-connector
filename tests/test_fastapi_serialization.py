"""Models returned from FastAPI routes serialize like to_dict() (#30)."""
from typing import List

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi.encoders import jsonable_encoder  # noqa: E402

from postgres_connector import Field, PostgreSQLModel, Relation  # noqa: E402


class FaUser(PostgreSQLModel):
    __table__ = "fa_users"
    id:     int = Field(primary_key=True)
    name:   str = Field()
    orders: List["FaOrder"] = Relation(type="hasMany", foreign_key="user_id")


class FaOrder(PostgreSQLModel):
    __table__ = "fa_orders"
    id:      int   = Field(primary_key=True)
    user_id: int   = Field()
    total:   float = Field()


def test_dict_of_a_model_is_its_public_data():
    user = FaUser(id=1, name="raj")
    assert dict(user) == user.to_dict() == {"id": 1, "name": "raj"}


def test_jsonable_encoder_without_relations_loaded():
    assert jsonable_encoder(FaUser(id=1, name="raj")) == {"id": 1, "name": "raj"}


@pytest.fixture
async def app(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS fa_orders, fa_users")
    await connector.migrate()
    user = await connector.create(FaUser, {"name": "raj"})
    await connector.create(FaOrder, {"user_id": user.id, "total": 9.5})

    api = fastapi.FastAPI()

    @api.get("/users/{user_id}")
    async def get_user(user_id: int):
        return await connector.find_by_id(FaUser, user_id)

    @api.get("/users-with-orders")
    async def users_with_orders():
        return await connector.find(FaUser, {"include": ["orders"]})

    yield api, user
    await connector.raw_execute("DROP TABLE IF EXISTS fa_orders, fa_users")


async def test_routes_return_models_as_json(app):
    api, user = app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://t") as client:
        one = await client.get(f"/users/{user.id}")
        many = await client.get("/users-with-orders")
    assert one.status_code == 200
    assert one.json() == {"id": user.id, "name": "raj"}
    assert many.status_code == 200
    assert many.json() == [
        {"id": user.id, "name": "raj", "orders": [{"id": 1, "user_id": user.id, "total": 9.5}]}
    ]
