"""A `fields` projection never breaks includes: the key columns used to attach relations are selected (#32)."""
from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation


class PkUser(PostgreSQLModel):
    __table__ = "pk_users"
    id:    int = Field(primary_key=True)
    name:  str = Field()
    posts: List["PkPost"] = Relation(type="hasMany", foreign_key="user_id")


class PkPost(PostgreSQLModel):
    __table__ = "pk_posts"
    id:       int = Field(primary_key=True)
    user_id:  int = Field()
    title:    str = Field()
    user:     "PkUser" = Relation(type="belongsTo", foreign_key="user_id")
    comments: List["PkComment"] = Relation(type="hasMany", foreign_key="post_id")


class PkComment(PostgreSQLModel):
    __table__ = "pk_comments"
    id:      int = Field(primary_key=True)
    post_id: int = Field()
    body:    str = Field()


@pytest.fixture
async def data(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS pk_comments, pk_posts, pk_users")
    await connector.migrate()
    users = await connector.create_many(PkUser, [{"name": n} for n in ("a", "b", "c")])
    posts = await connector.create_many(PkPost, [{"user_id": u.id, "title": f"t{u.name}"} for u in users])
    await connector.create_many(PkComment, [{"post_id": p.id, "body": "hi"} for p in posts])
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS pk_comments, pk_posts, pk_users")


async def test_scope_fields_without_the_foreign_key(connector, data):
    users = await connector.find(
        PkUser, {"order": ["id ASC"], "include": [{"relation": "posts", "scope": {"fields": ["id", "title"]}}]}
    )
    assert [len(u.posts) for u in users] == [1, 1, 1]
    assert users[0].posts[0].title == "ta"


async def test_parent_fields_without_the_primary_key(connector, data):
    users = await connector.find(PkUser, {"fields": ["name"], "order": ["name ASC"], "include": ["posts"]})
    assert [len(u.posts) for u in users] == [1, 1, 1]


async def test_belongs_to_with_projections_on_both_sides(connector, data):
    posts = await connector.find(
        PkPost,
        {"fields": ["title"], "order": ["title ASC"], "include": [{"relation": "user", "scope": {"fields": ["name"]}}]},
    )
    assert [p.user.name for p in posts] == ["a", "b", "c"]


async def test_nested_include_keeps_the_middle_keys(connector, data):
    users = await connector.find(
        PkUser,
        {
            "fields": ["name"],
            "order": ["name ASC"],
            "include": [{"relation": "posts", "scope": {"fields": ["title"], "include": ["comments"]}}],
        },
    )
    assert [len(u.posts[0].comments) for u in users] == [1, 1, 1]


async def test_callers_filter_is_not_mutated(connector, data):
    flt = {"fields": ["name"], "include": [{"relation": "posts", "scope": {"fields": ["title"]}}]}
    await connector.find(PkUser, flt)
    assert flt == {"fields": ["name"], "include": [{"relation": "posts", "scope": {"fields": ["title"]}}]}


async def test_exclusion_form_keeps_hiding_other_fields(connector, data):
    # {name: false} must stay an exclusion: adding stitch keys used to turn it
    # into a list of the excluded names, selecting exactly the hidden column.
    users = await connector.find(PkUser, {"fields": {"name": False}, "order": ["id ASC"], "include": ["posts"]})
    assert ["name" in u.to_dict() for u in users] == [False, False, False]
    assert [len(u.posts) for u in users] == [1, 1, 1]
    users = await connector.find(
        PkUser, {"order": ["id ASC"], "include": [{"relation": "posts", "scope": {"fields": {"title": False}}}]}
    )
    assert ["title" in p.to_dict() for u in users for p in u.posts] == [False, False, False]


async def test_exclusion_of_a_key_column_still_stitches(connector, data):
    users = await connector.find(PkUser, {"fields": {"id": False}, "include": ["posts"]})
    assert [len(u.posts) for u in users] == [1, 1, 1]
