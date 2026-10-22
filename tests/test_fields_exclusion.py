"""`fields: {name: false}` hides that column, in find() and in an include scope (LoopBack semantics)."""
from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation


class ExclUser(PostgreSQLModel):
    __table__ = "excl_users"
    id:            int = Field(primary_key=True)
    email:         str = Field()
    password_hash: str = Field()
    notes:         List["ExclNote"] = Relation(type="hasMany", foreign_key="user_id")


class ExclNote(PostgreSQLModel):
    __table__ = "excl_notes"
    id:      int = Field(primary_key=True)
    user_id: int = Field()
    body:    str = Field()
    secret:  str = Field()


async def _drop(connector):
    for table in ("excl_notes", "excl_users"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


@pytest.fixture
async def seeded(connector):
    await _drop(connector)
    await connector.migrate()
    user = await connector.create(ExclUser, {"email": "a@x.io", "password_hash": "h"})
    await connector.create(ExclNote, {"user_id": user.id, "body": "hi", "secret": "s"})
    try:
        yield
    finally:
        await _drop(connector)


async def test_false_field_is_hidden(connector, seeded):
    [user] = await connector.find(ExclUser, {"fields": {"password_hash": False}})
    assert user.to_dict() == {"id": 1, "email": "a@x.io", "password_hash": None}


async def test_false_field_is_hidden_in_an_include_scope(connector, seeded):
    [user] = await connector.find(ExclUser, {
        "fields": {"password_hash": False},
        "include": [{"relation": "notes", "scope": {"fields": {"secret": False}}}],
    })
    assert [n.to_dict() for n in user.notes] == [{"id": 1, "user_id": 1, "body": "hi", "secret": None}]
