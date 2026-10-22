"""Raw driver errors become typed errors, and FastAPI maps typed errors to HTTP statuses (#44)."""
import json

import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.exceptions import InvalidValueError, ReadOnlyTransactionError

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from postgres_connector.fastapi import register_exception_handlers  # noqa: E402


class EmUser(PostgreSQLModel):
    __table__ = "em_users"
    id:    int = Field(primary_key=True)
    email: str = Field(unique=True, nullable=False)
    name:  str = Field(max_length=5)
    age:   int = Field(nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS em_users")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS em_users")


# -- typed errors instead of raw asyncpg ones --


async def test_text_for_an_integer_filter_is_invalid_value(connector, table):
    with pytest.raises(InvalidValueError):
        await connector.find(EmUser, {"where": {"id": "abc"}})


async def test_wrong_type_in_create_many_is_invalid_value(connector, table):
    with pytest.raises(InvalidValueError):
        await connector.create_many(EmUser, [{"email": "a@x", "name": "a", "age": "old"}])


async def test_too_long_value_is_invalid_value(connector, table):
    with pytest.raises(InvalidValueError) as info:
        await connector.create(EmUser, {"email": "a@x", "name": "much too long"})
    assert "too long" in str(info.value)


async def test_write_in_a_read_only_transaction(connector, table):
    with pytest.raises(ReadOnlyTransactionError):
        async with connector.begin(readonly=True) as tx:
            await tx.create(EmUser, {"email": "a@x", "name": "a"})


# -- HTTP mapping --


@pytest.fixture
async def client(connector, table):
    app = fastapi.FastAPI()
    register_exception_handlers(app)

    @app.post("/users")
    async def create_user(payload: dict):
        return (await connector.create(EmUser, payload)).to_dict()

    @app.get("/search")
    async def search(filter: str):
        return [u.to_dict() for u in await connector.find(EmUser, json.loads(filter))]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c


async def test_duplicate_is_409_with_the_constraint(client):
    assert (await client.post("/users", json={"email": "raj@x.io", "name": "raj"})).status_code == 200
    resp = await client.post("/users", json={"email": "raj@x.io", "name": "raj"})
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "UniqueConstraintError"
    assert body["constraint"] == "uq_em_users_email"


async def test_missing_required_value_is_422(client):
    resp = await client.post("/users", json={"name": "raj"})
    assert resp.status_code == 422
    assert resp.json()["error"] == "NotNullConstraintError"


async def test_bad_filter_key_is_400(client):
    resp = await client.get("/search", params={"filter": json.dumps({"wher": {"name": "raj"}})})
    assert resp.status_code == 400
    assert resp.json()["error"] == "QueryError"


async def test_bad_filter_value_is_400(client):
    resp = await client.get("/search", params={"filter": json.dumps({"where": {"id": "abc"}})})
    assert resp.status_code == 400
    assert resp.json()["error"] == "InvalidValueError"


async def test_wrong_payload_type_is_400(client):
    resp = await client.post("/users", json={"email": "a@x", "name": "a", "age": "old"})
    assert resp.status_code == 400
    assert resp.json()["error"] == "ConfigurationError"


async def test_duplicate_body_does_not_quote_stored_values_by_default(client):
    await client.post("/users", json={"email": "raj@x.io", "name": "raj"})
    resp = await client.post("/users", json={"email": "raj@x.io", "name": "raj"})
    assert resp.status_code == 409
    assert "raj@x.io" not in resp.text
    assert "db_detail" not in resp.json()


async def test_expose_db_detail_adds_postgres_detail(connector, table):
    app = fastapi.FastAPI()
    register_exception_handlers(app, expose_db_detail=True)

    @app.post("/users")
    async def create_user(payload: dict):
        return (await connector.create(EmUser, payload)).to_dict()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/users", json={"email": "raj@x.io", "name": "raj"})
        resp = await c.post("/users", json={"email": "raj@x.io", "name": "raj"})
    assert resp.status_code == 409
    assert resp.json()["db_detail"] == "Key (email)=(raj@x.io) already exists."
