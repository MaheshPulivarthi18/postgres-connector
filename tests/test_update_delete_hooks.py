import pytest

from postgres_connector import Field, PostgreSQLModel

calls = []


class HookedNote(PostgreSQLModel):
    __table__ = "hooked_notes"
    id:     int = Field(primary_key=True)
    body:   str = Field()
    locked: bool = Field(default=False)

    async def before_update(self, data):
        calls.append(("before_update", self.body, dict(data)))
        data["body"] = data["body"].strip()

    def after_update(self):  # sync hooks are supported too
        calls.append(("after_update", self.body))

    async def before_delete(self):
        calls.append(("before_delete", self.body))
        if self.locked:
            raise PermissionError("note is locked")


class PlainNote(PostgreSQLModel):
    __table__ = "plain_notes"
    id:   int = Field(primary_key=True)
    body: str = Field()


@pytest.fixture
async def tables(connector):
    for t in ("hooked_notes", "plain_notes"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}"')
    await connector.migrate()
    calls.clear()
    yield
    for t in ("hooked_notes", "plain_notes"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}"')


@pytest.mark.asyncio
async def test_update_by_id_fires_before_and_after_update(connector, tables):
    note = await connector.create(HookedNote, {"body": "old"})
    caller_data = {"body": "  new  "}

    updated = await connector.update_by_id(HookedNote, note.id, caller_data)

    assert updated.body == "new"  # before_update's mutation was applied
    assert caller_data == {"body": "  new  "}  # the caller's dict is untouched
    assert calls == [
        ("before_update", "old", {"body": "  new  "}),  # runs on the current row
        ("after_update", "new"),                       # runs on the updated row
    ]


@pytest.mark.asyncio
async def test_delete_by_id_fires_before_delete_and_it_can_veto(connector, tables):
    free = await connector.create(HookedNote, {"body": "free"})
    locked = await connector.create(HookedNote, {"body": "locked", "locked": True})

    assert await connector.delete_by_id(HookedNote, free.id) is True
    with pytest.raises(PermissionError):
        await connector.delete_by_id(HookedNote, locked.id)

    assert calls == [("before_delete", "free"), ("before_delete", "locked")]
    assert await connector.exists(HookedNote, {"id": locked.id})


@pytest.mark.asyncio
async def test_hooks_do_not_fire_for_a_missing_row(connector, tables):
    assert await connector.update_by_id(HookedNote, 999, {"body": "x"}) is None
    assert await connector.delete_by_id(HookedNote, 999) is False
    assert calls == []


@pytest.mark.asyncio
async def test_hooks_fire_inside_a_transaction(connector, tables):
    note = await connector.create(HookedNote, {"body": "old"})
    async with connector.begin() as tx:
        await tx.update_by_id(HookedNote, note.id, {"body": "new"})
        await tx.delete_by_id(HookedNote, note.id)
    assert [c[0] for c in calls] == ["before_update", "after_update", "before_delete"]


@pytest.mark.asyncio
async def test_models_without_hooks_skip_the_extra_load(connector, tables):
    note = await connector.create(PlainNote, {"body": "old"})
    fetches = []
    real_fetch = connector._fetch

    async def counting_fetch(*args, **kwargs):
        fetches.append(args[0])
        return await real_fetch(*args, **kwargs)

    connector._fetch = counting_fetch
    try:
        await connector.update_by_id(PlainNote, note.id, {"body": "new"})
        await connector.delete_by_id(PlainNote, note.id)
    finally:
        del connector._fetch

    assert len(fetches) == 1 and fetches[0].startswith("UPDATE")
