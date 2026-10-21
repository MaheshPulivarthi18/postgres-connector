import asyncio
from typing import List

import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel, Relation


class TxIncUser(PostgreSQLModel):
    __table__ = "tx_inc_users"
    id:    int = Field(primary_key=True)
    name:  str = Field()
    posts: List["TxIncPost"] = Relation(type="hasMany", foreign_key="user_id")


class TxIncPost(PostgreSQLModel):
    __table__ = "tx_inc_posts"
    id:       int = Field(primary_key=True)
    user_id:  int = Field()
    title:    str = Field()
    author:   "TxIncUser" = Relation(type="belongsTo", foreign_key="user_id")
    comments: List["TxIncComment"] = Relation(type="hasMany", foreign_key="post_id")


class TxIncComment(PostgreSQLModel):
    __table__ = "tx_inc_comments"
    id:      int = Field(primary_key=True)
    post_id: int = Field()


async def _drop(connector):
    for table in ("tx_inc_comments", "tx_inc_posts", "tx_inc_users"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


@pytest.fixture
async def tables(connector):
    await _drop(connector)
    await connector.migrate()
    yield
    await _drop(connector)


@pytest.mark.asyncio
async def test_include_sees_uncommitted_rows_of_the_same_transaction(connector, tables):
    async with connector.begin() as tx:
        user = await tx.create(TxIncUser, {"name": "raj"})
        post = await tx.create(TxIncPost, {"user_id": user.id, "title": "in-tx"})
        await tx.create(TxIncComment, {"post_id": post.id})

        # Sibling + nested includes, all run on the transaction's connection.
        found = await tx.find_by_id(TxIncUser, user.id, {"include": ["posts.comments", "posts.author"]})
        assert [p.title for p in found.posts] == ["in-tx"]

        posts = await tx.find(TxIncPost, {"include": ["author", "comments"]})
        assert posts[0].author.name == "raj"
        assert len(posts[0].comments) == 1


@pytest.mark.asyncio
async def test_include_in_transaction_does_not_need_a_second_pooled_connection(connector, tables):
    kwargs = connector._pool._connect_kwargs
    small = PostgreSQLConnector(**kwargs, minsize=1, maxsize=1)
    await small.connect()
    try:
        async def body():
            async with small.begin() as tx:
                user = await tx.create(TxIncUser, {"name": "raj"})
                await tx.create(TxIncPost, {"user_id": user.id, "title": "p"})
                found = await tx.find_by_id(TxIncUser, user.id, {"include": ["posts"]})
                return len(found.posts)

        # Before the fix this hung: the include waited for a pooled
        # connection the transaction itself was holding.
        assert await asyncio.wait_for(body(), timeout=10) == 1
    finally:
        await small.disconnect()
