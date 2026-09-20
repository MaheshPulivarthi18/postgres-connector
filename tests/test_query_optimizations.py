import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.registry import registry


class OptUser(PostgreSQLModel):
    __table__ = "opt_users"
    id:    int = Field(primary_key=True)
    name:  str = Field(max_length=50)
    email: str = Field(unique=True)


@pytest.mark.asyncio
async def test_exists_true_and_false(connector):
    registry.register(OptUser)
    await connector.migrate()
    try:
        await connector.create(OptUser, {"name": "Raj", "email": "raj@example.com"})

        assert await connector.exists(OptUser, {"email": "raj@example.com"}) is True
        assert await connector.exists(OptUser, {"email": "nobody@example.com"}) is False
    finally:
        await connector.raw_execute("DROP TABLE opt_users")


@pytest.mark.asyncio
async def test_update_by_id_returns_updated_instance(connector):
    registry.register(OptUser)
    await connector.migrate()
    try:
        user = await connector.create(OptUser, {"name": "Raj", "email": "raj@example.com"})

        updated = await connector.update_by_id(OptUser, user.id, {"name": "Rajesh"})
        assert updated is not None
        assert updated.name == "Rajesh"

        refetched = await connector.find_by_id(OptUser, user.id)
        assert refetched.name == "Rajesh"
    finally:
        await connector.raw_execute("DROP TABLE opt_users")


@pytest.mark.asyncio
async def test_update_by_id_missing_id_returns_none(connector):
    registry.register(OptUser)
    await connector.migrate()
    try:
        result = await connector.update_by_id(OptUser, 999999, {"name": "Nobody"})
        assert result is None
    finally:
        await connector.raw_execute("DROP TABLE opt_users")


@pytest.mark.asyncio
async def test_update_by_id_makes_exactly_one_round_trip(connector):
    """
    Concrete proof the two-round-trip bug (UPDATE, then a separate SELECT)
    is gone: update_by_id must issue exactly one query.
    """
    registry.register(OptUser)
    await connector.migrate()
    try:
        user = await connector.create(OptUser, {"name": "Raj", "email": "raj@example.com"})

        calls = {"fetch": 0, "execute": 0}
        real_fetch = connector._fetch
        real_execute = connector._execute

        async def counting_fetch(*args, **kwargs):
            calls["fetch"] += 1
            return await real_fetch(*args, **kwargs)

        async def counting_execute(*args, **kwargs):
            calls["execute"] += 1
            return await real_execute(*args, **kwargs)

        connector._fetch = counting_fetch
        connector._execute = counting_execute
        try:
            updated = await connector.update_by_id(OptUser, user.id, {"name": "Rajesh"})
        finally:
            connector._fetch = real_fetch
            connector._execute = real_execute

        assert updated.name == "Rajesh"
        assert calls == {"fetch": 1, "execute": 0}
    finally:
        await connector.raw_execute("DROP TABLE opt_users")
