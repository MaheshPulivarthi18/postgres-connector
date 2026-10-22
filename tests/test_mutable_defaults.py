"""Mutable Field defaults are copied per instance, never shared."""
import pytest

from postgres_connector import Field, JSONField, PostgreSQLModel


class MdDoc(PostgreSQLModel):
    __table__ = "md_docs"
    id:   int  = Field(primary_key=True)
    name: str  = Field()
    meta: dict = JSONField(default={})
    tags: list = JSONField(default=[])


def test_instances_do_not_share_a_mutable_default():
    x, y = MdDoc(name="x"), MdDoc(name="y")
    x.meta["k"] = 1
    x.tags.append("t")
    assert y.meta == {} and y.tags == []
    assert x.meta is not y.meta
    assert MdDoc(name="z").meta == {}


@pytest.fixture
async def md_table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS md_docs")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS md_docs")


async def test_hook_mutating_a_default_does_not_leak_into_later_inserts(connector, md_table):
    async def stamp(doc):
        doc.meta["stamped"] = doc.name

    MdDoc.observe("before_create", stamp)
    try:
        first = await connector.create(MdDoc, {"name": "first"})
        second = await connector.create(MdDoc, {"name": "second"})
    finally:
        MdDoc._observers["before_create"].remove(stamp)
    assert first.meta == {"stamped": "first"}
    assert second.meta == {"stamped": "second"}


async def test_create_many_copies_defaults_per_row(connector, md_table):
    rows = await connector.create_many(MdDoc, [{"name": "a"}, {"name": "b"}])
    assert [r.meta for r in rows] == [{}, {}]
    assert MdDoc._field_defaults["meta"] == {}
