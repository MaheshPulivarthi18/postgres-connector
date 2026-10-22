"""Hook methods from several mixins all run (#39), bases first, then the model's own, then observers."""
import pytest

from postgres_connector import Field, PostgreSQLModel

log = []


class TimestampMixin:
    async def before_create(self):
        log.append("TimestampMixin")


class AuditMixin:
    def before_create(self):  # a sync hook works too
        log.append("AuditMixin")


class MhDoc(TimestampMixin, AuditMixin, PostgreSQLModel):
    __table__ = "mh_docs"
    id:   int = Field(primary_key=True)
    body: str = Field()

    async def before_create(self):
        log.append("MhDoc")


class MhNote(TimestampMixin, AuditMixin, PostgreSQLModel):  # no hook of its own
    __table__ = "mh_notes"
    id:   int = Field(primary_key=True)
    body: str = Field()


class MhBase(PostgreSQLModel):
    __table__ = "mh_bases"
    id:   int = Field(primary_key=True)
    name: str = Field()

    async def before_create(self):
        log.append("MhBase")


class MhChild(MhBase):
    __table__ = "mh_children"

    async def before_create(self):
        log.append("MhChild")


@pytest.fixture
async def tables(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS mh_docs, mh_notes, mh_bases, mh_children")
    await connector.migrate()
    log.clear()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS mh_docs, mh_notes, mh_bases, mh_children")


async def test_every_mixin_hook_runs_bases_first_then_the_model(connector, tables):
    await connector.create(MhDoc, {"body": "x"})
    assert log == ["AuditMixin", "TimestampMixin", "MhDoc"]


async def test_mixin_hooks_run_without_a_model_hook(connector, tables):
    await connector.create(MhNote, {"body": "x"})
    assert log == ["AuditMixin", "TimestampMixin"]


async def test_observers_run_after_hook_methods(connector, tables):
    def observer(doc):
        log.append("observer")

    MhNote.observe("before_create", observer)
    try:
        await connector.create(MhNote, {"body": "x"})
    finally:
        MhNote._observers["before_create"].remove(observer)
    assert log == ["AuditMixin", "TimestampMixin", "observer"]


async def test_model_subclass_still_overrides_its_parents_hook(connector, tables):
    await connector.create(MhChild, {"name": "c"})
    assert log == ["MhChild"]
