"""PostgreSQLModel.pydantic_model(): response models and OpenAPI schemas from the model (#42)."""
from typing import List

import pytest

pydantic = pytest.importorskip("pydantic")
fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from postgres_connector import Field, JSONField, PostgreSQLModel, Relation  # noqa: E402
from postgres_connector.exceptions import ConfigurationError  # noqa: E402


class PmUser(PostgreSQLModel):
    __table__ = "pm_users"
    id:            int  = Field(primary_key=True)
    name:          str  = Field(max_length=20, nullable=False)
    password_hash: str  = Field(nullable=True)
    meta:          dict = JSONField(default={})
    created_at:    str  = Field(auto_now_add=True)
    orders:        List["PmOrder"] = Relation(type="hasMany", foreign_key="user_id")


class PmOrder(PostgreSQLModel):
    __table__ = "pm_orders"
    id:      int   = Field(primary_key=True)
    user_id: int   = Field(nullable=False)
    total:   float = Field()
    items:   List["PmItem"] = Relation(type="hasMany", foreign_key="order_id")
    user:    "PmUser" = Relation(type="belongsTo", foreign_key="user_id")


class PmItem(PostgreSQLModel):
    __table__ = "pm_items"
    id:       int = Field(primary_key=True)
    order_id: int = Field(nullable=False)
    sku:      str = Field()


def test_fields_types_and_requiredness():
    schema = PmUser.pydantic_model().model_json_schema()
    props = schema["properties"]
    assert list(props) == ["id", "name", "password_hash", "meta", "created_at"]
    assert set(schema["required"]) == {"id", "name"}
    assert props["name"]["maxLength"] == 20
    assert props["created_at"]["anyOf"][0]["format"] == "date-time"


def test_exclude_and_nested_includes():
    model = PmUser.pydantic_model(include=["orders.items"], exclude=["password_hash"])
    assert "password_hash" not in model.model_fields
    order_model = model.model_fields["orders"].annotation.__args__[0]
    assert "items" in order_model.model_fields
    belongs = PmOrder.pydantic_model(include=["user"])
    assert belongs.model_fields["user"].default is None


def test_cached_per_arguments():
    assert PmUser.pydantic_model(include=["orders"]) is PmUser.pydantic_model(include=["orders"])
    assert PmUser.pydantic_model() is not PmUser.pydantic_model(include=["orders"])


def test_unknown_names_are_configuration_errors():
    with pytest.raises(ConfigurationError, match="unknown relation 'invoices'"):
        PmUser.pydantic_model(include=["invoices"])
    with pytest.raises(ConfigurationError, match="unknown fields"):
        PmUser.pydantic_model(exclude=["nope"])


def test_validates_an_orm_instance_directly():
    user = PmUser(id=1, name="raj", password_hash="secret")
    out = PmUser.pydantic_model(exclude=["password_hash"]).model_validate(user)
    assert out.model_dump()["name"] == "raj"
    assert "password_hash" not in out.model_dump()


@pytest.fixture
async def client(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS pm_items, pm_orders, pm_users")
    await connector.migrate()
    user = await connector.create(PmUser, {"name": "raj", "password_hash": "secret"})
    order = await connector.create(PmOrder, {"user_id": user.id, "total": 9.5})
    await connector.create(PmItem, {"order_id": order.id, "sku": "A1"})

    app = fastapi.FastAPI()
    UserWithOrders = PmUser.pydantic_model(include=["orders.items"], exclude=["password_hash"])

    @app.get("/users/{user_id}", response_model=UserWithOrders)
    async def get_user(user_id: int):
        return await connector.find_by_id(PmUser, user_id, {"include": ["orders.items"]})

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c, user
    await connector.raw_execute("DROP TABLE IF EXISTS pm_items, pm_orders, pm_users")


async def test_route_returns_the_orm_instance_through_response_model(client):
    c, user = client
    resp = await c.get(f"/users/{user.id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "password_hash" not in body
    assert body["name"] == "raj"
    assert body["orders"][0]["total"] == 9.5
    assert body["orders"][0]["items"][0]["sku"] == "A1"


async def test_openapi_schema_lists_the_generated_model(client):
    c, _ = client
    schemas = (await c.get("/openapi.json")).json()["components"]["schemas"]
    assert "PmUserWithOrdersItemsPublicOut" in schemas
    assert "password_hash" not in schemas["PmUserWithOrdersItemsPublicOut"]["properties"]
