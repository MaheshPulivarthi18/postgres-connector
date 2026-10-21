"""LoopBack-style operation hooks: Model.observe(event, fn), plus after_delete."""
import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.exceptions import ConfigurationError
from postgres_connector.registry import registry

log = []


class ObsBase(PostgreSQLModel):
    __table__ = "obs_base"
    id:   int = Field(primary_key=True)
    body: str = Field()

    async def before_create(self):          # the model's own hook method
        log.append("method:before_create")


class ObsChild(ObsBase):
    __table__ = "obs_child"


class ObsPlain(PostgreSQLModel):           # observers only, no hook methods
    __table__ = "obs_plain"
    id:   int = Field(primary_key=True)
    body: str = Field()


@ObsBase.observe("before_create")
async def first(inst):
    log.append(("obs1:before_create", inst.body))


def second(inst):                           # sync observers work too
    log.append("obs2:before_create")


ObsBase.observe("before_create", second)


@ObsBase.observe("before_update")
def trim(inst, data):
    data["body"] = data["body"].strip()


@ObsPlain.observe("before_delete")
def guard(inst):
    log.append(("before_delete", inst.body))
    if inst.body == "keep":
        raise PermissionError("protected")


@ObsPlain.observe("after_delete")
async def audit(inst):
    log.append(("after_delete", inst.body))


TABLES = ("obs_base", "obs_child", "obs_plain")


@pytest.fixture
async def tables(connector):
    for t in TABLES:
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}"')
    await connector.migrate()
    log.clear()
    yield
    for t in TABLES:
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}"')


@pytest.mark.asyncio
async def test_method_hook_then_observers_in_registration_order(connector, tables):
    await connector.create(ObsBase, {"body": "x"})
    assert log == ["method:before_create", ("obs1:before_create", "x"), "obs2:before_create"]


@pytest.mark.asyncio
async def test_observers_apply_to_subclasses(connector, tables):
    await connector.create(ObsChild, {"body": "c"})
    assert log == ["method:before_create", ("obs1:before_create", "c"), "obs2:before_create"]


@pytest.mark.asyncio
async def test_before_update_observer_can_mutate_data(connector, tables):
    row = await connector.create(ObsBase, {"body": "old"})
    updated = await connector.update_by_id(ObsBase, row.id, {"body": "  new  "})
    assert updated.body == "new"


@pytest.mark.asyncio
async def test_observers_alone_fire_delete_hooks_and_can_veto(connector, tables):
    gone = await connector.create(ObsPlain, {"body": "gone"})
    keep = await connector.create(ObsPlain, {"body": "keep"})

    assert await connector.delete_by_id(ObsPlain, gone.id) is True
    with pytest.raises(PermissionError):
        await connector.delete_by_id(ObsPlain, keep.id)

    assert log == [("before_delete", "gone"), ("after_delete", "gone"), ("before_delete", "keep")]
    assert await connector.exists(ObsPlain, {"id": keep.id})
    assert await connector.delete_by_id(ObsPlain, 999_999) is False  # no row: no hooks
    assert len(log) == 3


@pytest.mark.asyncio
async def test_after_delete_method_hook(connector, tables):
    class ObsAudited(PostgreSQLModel):
        __table__ = "obs_plain"
        id:   int = Field(primary_key=True)
        body: str = Field()

        def after_delete(self):
            log.append(("method:after_delete", self.body))

    try:
        row = await connector.create(ObsPlain, {"body": "m"})
        assert await connector.delete_by_id(ObsAudited, row.id) is True
        assert log == [("method:after_delete", "m")]
    finally:
        registry.unregister("ObsAudited")  # it shares obs_plain; keep it out of later migrate() runs


def test_unknown_event_is_rejected():
    with pytest.raises(ConfigurationError, match="Unknown hook event 'before_save'"):
        ObsBase.observe("before_save", lambda inst: None)
