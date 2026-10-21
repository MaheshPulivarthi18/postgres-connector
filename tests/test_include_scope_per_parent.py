from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation


class ScopeUser(PostgreSQLModel):
    __table__ = "scope_users"
    id:     int = Field(primary_key=True)
    name:   str = Field()
    posts:  List["ScopePost"] = Relation(type="hasMany", foreign_key="user_id")
    latest: "ScopePost" = Relation(type="hasOne", foreign_key="user_id")
    groups: List["ScopeGroup"] = Relation(
        type="hasManyThrough", through="ScopeMembership", foreign_key="user_id", other_key="group_id",
    )


class ScopePost(PostgreSQLModel):
    __table__ = "scope_posts"
    id:      int = Field(primary_key=True)
    user_id: int = Field()
    rank:    int = Field()


class ScopeGroup(PostgreSQLModel):
    __table__ = "scope_groups"
    id:   int = Field(primary_key=True)
    name: str = Field()


class ScopeMembership(PostgreSQLModel):
    __table__ = "scope_memberships"
    id:       int = Field(primary_key=True)
    user_id:  int = Field()
    group_id: int = Field()


TABLES = ("scope_memberships", "scope_groups", "scope_posts", "scope_users")


@pytest.fixture
async def users(connector):
    for t in TABLES:
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    await connector.migrate()
    groups = await connector.create_many(ScopeGroup, [{"name": n} for n in ("a", "b", "c", "d")])
    ids = []
    for name in ("u1", "u2", "u3"):
        user = await connector.create(ScopeUser, {"name": name})
        ids.append(user.id)
        await connector.create_many(ScopePost, [{"user_id": user.id, "rank": r} for r in (1, 2, 3, 4)])
        # Join in reverse name order so junction order != name order.
        await connector.create_many(ScopeMembership, [
            {"user_id": user.id, "group_id": g.id} for g in reversed(groups)
        ])
    yield ids
    for t in TABLES:
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')


async def _load(connector, include):
    return await connector.find(ScopeUser, {"order": ["id ASC"], "include": [include]})


@pytest.mark.asyncio
async def test_has_many_limit_applies_per_parent(connector, users):
    loaded = await _load(connector, {"relation": "posts", "scope": {"order": ["rank DESC"], "limit": 2}})
    assert [[p.rank for p in u.posts] for u in loaded] == [[4, 3], [4, 3], [4, 3]]


@pytest.mark.asyncio
async def test_has_many_skip_applies_per_parent(connector, users):
    loaded = await _load(connector, {"relation": "posts", "scope": {"order": ["rank ASC"], "skip": 1, "limit": 2}})
    assert [[p.rank for p in u.posts] for u in loaded] == [[2, 3], [2, 3], [2, 3]]

    loaded = await _load(connector, {"relation": "posts", "scope": {"order": ["rank ASC"], "skip": 3}})
    assert [[p.rank for p in u.posts] for u in loaded] == [[4], [4], [4]]


@pytest.mark.asyncio
async def test_has_many_limit_with_where_and_fields(connector, users):
    loaded = await _load(connector, {"relation": "posts", "scope": {
        "where": {"rank": {"neq": 4}}, "order": ["rank DESC"], "limit": 1, "fields": ["id", "user_id", "rank"],
    }})
    assert [[p.rank for p in u.posts] for u in loaded] == [[3], [3], [3]]


@pytest.mark.asyncio
async def test_has_one_skip_picks_the_next_row_per_parent(connector, users):
    loaded = await _load(connector, {"relation": "latest", "scope": {"order": ["rank DESC"], "skip": 1}})
    assert [u.latest.rank for u in loaded] == [3, 3, 3]


@pytest.mark.asyncio
async def test_has_many_through_honours_order_skip_and_limit_per_parent(connector, users):
    loaded = await _load(connector, {"relation": "groups", "scope": {"order": ["name ASC"]}})
    assert [[g.name for g in u.groups] for u in loaded] == [["a", "b", "c", "d"]] * 3

    loaded = await _load(connector, {"relation": "groups", "scope": {"order": ["name ASC"], "skip": 1, "limit": 2}})
    assert [[g.name for g in u.groups] for u in loaded] == [["b", "c"]] * 3


@pytest.mark.asyncio
async def test_scopes_without_limit_or_skip_are_unchanged(connector, users):
    loaded = await _load(connector, {"relation": "posts", "scope": {"order": ["rank DESC"], "where": {"rank": {"gt": 2}}}})
    assert [[p.rank for p in u.posts] for u in loaded] == [[4, 3], [4, 3], [4, 3]]
