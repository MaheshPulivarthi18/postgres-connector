from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation
from postgres_connector.registry import registry


class InhOwner(PostgreSQLModel):
    __table__ = "inh_owners"
    id:   int = Field(primary_key=True)
    name: str = Field()


class InhBase(PostgreSQLModel):
    __table__ = "inh_base"
    id:       int = Field(primary_key=True)
    owner_id: int = Field(nullable=True)
    title:    str = Field(max_length=40, default="untitled")
    owner:    "InhOwner" = Relation(type="belongsTo", foreign_key="owner_id")


class InhChild(InhBase):
    __table__ = "inh_child"
    extra: int = Field(default=7)


class PlainMixin:
    note: str


class InhMixed(PlainMixin, PostgreSQLModel):
    __table__ = "inh_mixed"
    id: int = Field(primary_key=True)


def test_child_model_keeps_inherited_field_and_relation_definitions():
    assert InhChild._get_primary_key() == "id"
    assert InhChild._fields["id"].primary_key
    assert InhChild._fields["title"].max_length == 40
    assert InhChild._field_defaults == {"title": "untitled", "extra": 7}
    assert "owner" in InhChild._relations
    assert "owner" not in InhChild._fields
    # The parent itself is unaffected.
    assert "extra" not in InhBase._fields


def test_annotations_from_plain_mixins_still_become_fields():
    assert InhMixed._fields["note"].python_type is str


@pytest.mark.asyncio
async def test_child_model_round_trips_with_inherited_relation(connector):
    for t in ("inh_child", "inh_base", "inh_mixed", "inh_owners"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    try:
        await connector.migrate()
        owner = await connector.create(InhOwner, {"name": "raj"})
        child = await connector.create(InhChild, {"owner_id": owner.id})
        assert (child.id, child.title, child.extra) == (1, "untitled", 7)

        found = await connector.find_by_id(InhChild, child.id, {"include": ["owner"]})
        assert found.owner.name == "raj"
    finally:
        for t in ("inh_child", "inh_base", "inh_mixed", "inh_owners"):
            await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')


def test_child_without_annotations_of_its_own_inherits_everything():
    class InhBare(InhBase):
        __table__ = "inh_bare"

    try:
        assert InhBare._get_primary_key() == "id"
        assert set(InhBare._fields) == set(InhBase._fields)
        assert set(InhBare._relations) == {"owner"}
    finally:
        registry.unregister("InhBare")
