"""create() and create_many() reject keys that are not model fields."""
from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation
from postgres_connector.exceptions import QueryError


class UkUser(PostgreSQLModel):
    __table__ = "uk_users"
    id:    int = Field(primary_key=True)
    name:  str = Field()
    age:   int = Field(nullable=True)
    posts: List["UkPost"] = Relation(type="hasMany", foreign_key="user_id")


class UkPost(PostgreSQLModel):
    __table__ = "uk_posts"
    id:      int = Field(primary_key=True)
    user_id: int = Field()


@pytest.fixture
async def tables(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS uk_posts, uk_users")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS uk_posts, uk_users")


async def test_create_rejects_a_typo(connector, tables):
    with pytest.raises(QueryError, match="Field 'agee' does not exist on model 'uk_users'"):
        await connector.create(UkUser, {"name": "d", "agee": 40})
    assert await connector.count(UkUser) == 0


async def test_create_many_rejects_a_typo_before_inserting_anything(connector, tables):
    with pytest.raises(QueryError, match="Field 'nmae' does not exist"):
        await connector.create_many(UkUser, [{"name": "ok"}, {"nmae": "typo"}])
    assert await connector.count(UkUser) == 0


async def test_relations_get_a_clear_message(connector, tables):
    with pytest.raises(QueryError, match="'posts' is a relation"):
        await connector.create(UkUser, {"name": "d", "posts": []})


async def test_valid_payloads_still_work(connector, tables):
    user = await connector.create(UkUser, {"name": "d", "age": 40})
    assert user.age == 40
