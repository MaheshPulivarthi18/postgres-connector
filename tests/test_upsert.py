import pytest

from postgres_connector import Field, JSONField, PostgreSQLModel
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


class UpsertLead(PostgreSQLModel):
    __table__ = "upsert_leads"
    id:         int = Field(primary_key=True)
    email:      str = Field(unique=True)
    source:     str = Field(max_length=20)
    score:      int = Field(default=0)
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True)


@pytest.mark.asyncio
async def test_upsert_conflict_keeps_created_at_and_updates_auto_now(connector):
    registry.register(UpsertLead)
    await connector.migrate()
    try:
        data = {"email": "keep@crm.example", "source": "web", "score": 1}
        await connector.upsert(UpsertLead, data, conflict_target=["email"])
        await connector.raw_execute(
            "UPDATE upsert_leads SET created_at = '2020-01-01T00:00:00Z', "
            "updated_at = '2020-01-01T00:00:00Z'"
        )
        await connector.upsert(
            UpsertLead, {**data, "score": 2}, conflict_target=["email"]
        )
        rows = await connector.raw(
            "SELECT created_at, updated_at, score FROM upsert_leads"
        )
        assert rows[0]["score"] == 2
        assert rows[0]["created_at"].year == 2020
        assert rows[0]["updated_at"].year != 2020
    finally:
        await connector.raw_execute("DROP TABLE upsert_leads")


class UpsertPref(PostgreSQLModel):
    __table__ = "upsert_prefs"
    id:   int  = Field(primary_key=True)
    key:  str  = Field(unique=True)
    data: dict = JSONField(default={"theme": "light"})


@pytest.mark.asyncio
async def test_upsert_applies_python_defaults_on_insert_only(connector):
    registry.register(UpsertPref)
    await connector.migrate()
    try:
        first = await connector.upsert(
            UpsertPref, {"key": "a"}, conflict_target=["key"]
        )
        assert first.data == {"theme": "light"}
        second = await connector.upsert(
            UpsertPref, {"key": "b"}, conflict_target=["key"]
        )
        # each insert gets a fresh copy, not the shared class-level default
        assert second.data == {"theme": "light"}
        assert second.data is not UpsertPref._field_defaults["data"]
        assert first.data is not second.data

        # on conflict the stored value is not reset to the default
        await connector.raw_execute(
            """UPDATE upsert_prefs SET data = '{"theme": "dark"}' WHERE key='a'"""
        )
        await connector.upsert(
            UpsertPref, {"key": "a"}, conflict_target=["key"]
        )
        rows = await connector.raw("SELECT data FROM upsert_prefs WHERE key='a'")
        assert rows[0]["data"] == {"theme": "dark"}
    finally:
        await connector.raw_execute("DROP TABLE upsert_prefs")
