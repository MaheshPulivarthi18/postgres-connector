from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation
from postgres_connector.exceptions import QueryError


class MergeUser(PostgreSQLModel):
    __table__ = "merge_users"
    id:    int = Field(primary_key=True)
    name:  str = Field()
    posts: List["MergePost"] = Relation(type="hasMany", foreign_key="user_id")


class MergePost(PostgreSQLModel):
    __table__ = "merge_posts"
    id:       int = Field(primary_key=True)
    user_id:  int = Field()
    title:    str = Field()
    comments: List["MergeComment"] = Relation(type="hasMany", foreign_key="post_id")
    tags:     List["MergeTag"] = Relation(type="hasMany", foreign_key="post_id")


class MergeComment(PostgreSQLModel):
    __table__ = "merge_comments"
    id:      int = Field(primary_key=True)
    post_id: int = Field()


class MergeTag(PostgreSQLModel):
    __table__ = "merge_tags"
    id:      int = Field(primary_key=True)
    post_id: int = Field()


async def _drop(connector):
    for table in ("merge_comments", "merge_tags", "merge_posts", "merge_users"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


@pytest.fixture
async def user_id(connector):
    await _drop(connector)
    await connector.migrate()
    user = await connector.create(MergeUser, {"name": "raj"})
    for title in ("a", "b"):
        post = await connector.create(MergePost, {"user_id": user.id, "title": title})
        await connector.create(MergeComment, {"post_id": post.id})
        await connector.create(MergeTag, {"post_id": post.id})
    yield user.id
    await _drop(connector)


def _assert_fully_loaded(user):
    assert len(user.posts) == 2
    for post in user.posts:
        assert len(post.comments) == 1
        assert len(post.tags) == 1


@pytest.mark.asyncio
async def test_dotted_includes_sharing_a_prefix_load_every_branch(connector, user_id):
    user = await connector.find_by_id(MergeUser, user_id, {"include": ["posts.comments", "posts.tags"]})
    _assert_fully_loaded(user)


@pytest.mark.asyncio
async def test_plain_and_scoped_includes_of_the_same_relation_are_merged(connector, user_id):
    user = await connector.find_by_id(MergeUser, user_id, {"include": [
        "posts.comments",
        {"relation": "posts", "scope": {"order": ["title DESC"], "include": ["tags"]}},
    ]})
    _assert_fully_loaded(user)
    assert [p.title for p in user.posts] == ["b", "a"]


@pytest.mark.asyncio
async def test_conflicting_scopes_for_the_same_relation_raise(connector, user_id):
    with pytest.raises(QueryError, match="Conflicting 'limit'"):
        await connector.find_by_id(MergeUser, user_id, {"include": [
            {"relation": "posts", "scope": {"limit": 1}},
            {"relation": "posts", "scope": {"limit": 2}},
        ]})
