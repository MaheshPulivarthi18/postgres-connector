import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.registry import registry


class UpsertUser(PostgreSQLModel):
    __table__ = "upsert_users"
    id:          int = Field(primary_key=True)
    email:       str = Field(unique=True)
    name:        str = Field(max_length=100)
    login_count: int = Field(default=0)


async def _setup(connector):
    registry.register(UpsertUser)
    await connector.migrate()


async def _teardown(connector):
    await connector.raw_execute("DROP TABLE upsert_users")


@pytest.mark.asyncio
async def test_upsert_inserts_when_no_conflict(connector):
    await _setup(connector)
    try:
        user = await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "Raj", "login_count": 1},
            conflict_target=["email"],
        )
        assert user is not None
        assert user.id is not None
        assert user.name == "Raj"
        assert user.login_count == 1

        count = await connector.count(UpsertUser)
        assert count == 1
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_upsert_updates_on_conflict_in_one_round_trip(connector):
    await _setup(connector)
    try:
        first = await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "Raj", "login_count": 1},
            conflict_target=["email"],
        )

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
            second = await connector.upsert(
                UpsertUser,
                {"email": "raj@example.com", "name": "Rajesh", "login_count": 2},
                conflict_target=["email"],
            )
        finally:
            connector._fetch = real_fetch
            connector._execute = real_execute

        # Same row (same PK), fields updated — not a second row inserted.
        assert second.id == first.id
        assert second.name == "Rajesh"
        assert second.login_count == 2
        assert calls == {"fetch": 1, "execute": 0}

        count = await connector.count(UpsertUser)
        assert count == 1
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_upsert_only_updates_declared_update_fields(connector):
    await _setup(connector)
    try:
        first = await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "Raj", "login_count": 5},
            conflict_target=["email"],
        )

        # Only bump login_count; name must be left untouched.
        second = await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "IGNORED", "login_count": 6},
            conflict_target=["email"],
            update_fields=["login_count"],
        )
        assert second.id == first.id
        assert second.login_count == 6
        assert second.name == "Raj"  # unchanged
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_upsert_do_nothing_on_conflict_returns_none(connector):
    await _setup(connector)
    try:
        await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "Raj", "login_count": 1},
            conflict_target=["email"],
        )

        result = await connector.upsert(
            UpsertUser,
            {"email": "raj@example.com", "name": "Someone Else", "login_count": 99},
            conflict_target=["email"],
            update_fields=[],
        )
        assert result is None

        # Confirm nothing actually changed.
        existing = await connector.find_one(UpsertUser, {"where": {"email": "raj@example.com"}})
        assert existing.name == "Raj"
        assert existing.login_count == 1

        count = await connector.count(UpsertUser)
        assert count == 1
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_upsert_inside_transaction(connector):
    await _setup(connector)
    try:
        async with connector.begin() as tx:
            user = await tx.upsert(
                UpsertUser,
                {"email": "raj@example.com", "name": "Raj", "login_count": 1},
                conflict_target=["email"],
            )
            assert user is not None

        found = await connector.find_one(UpsertUser, {"where": {"email": "raj@example.com"}})
        assert found is not None
        assert found.name == "Raj"
    finally:
        await _teardown(connector)
