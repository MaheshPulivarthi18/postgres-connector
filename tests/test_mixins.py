"""User-written mixins attached with plain inheritance, LoopBack-style."""
from typing import List

import pytest

from postgres_connector import Field, PostgreSQLModel, Relation

calls = []


class TimestampMixin:
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True, nullable=True)


class SluggedMixin:
    slug: str = Field(unique=True, max_length=40, default="untitled")

    async def before_create(self):
        calls.append(("before_create", type(self).__name__, self.slug))


class OwnedMixin:
    owner_id: int = Field(nullable=True)
    owner: "MixOwner" = Relation(type="belongsTo", foreign_key="owner_id")


class MixOwner(PostgreSQLModel):
    __table__ = "mix_owners"
    id:   int = Field(primary_key=True)
    name: str = Field()


class MixArticle(TimestampMixin, SluggedMixin, OwnedMixin, PostgreSQLModel):
    __table__ = "mix_articles"
    id:    int = Field(primary_key=True)
    title: str = Field()


class MixPage(TimestampMixin, PostgreSQLModel):  # a second model reusing a mixin
    __table__ = "mix_pages"
    id: int = Field(primary_key=True)


def test_mixin_field_options_are_kept():
    f = MixArticle._fields
    assert f["created_at"].auto_now_add and f["updated_at"].auto_now
    assert f["slug"].unique and f["slug"].max_length == 40
    assert MixArticle._field_defaults["slug"] == "untitled"
    assert "owner" in MixArticle._relations and "owner" not in f


def test_one_mixin_serves_many_models():
    assert MixPage._fields["created_at"].auto_now_add
    assert MixArticle._fields["created_at"].auto_now_add


@pytest.mark.asyncio
async def test_mixin_fields_hooks_and_relations_work_end_to_end(connector):
    tables = ("mix_articles", "mix_pages", "mix_owners")
    for t in tables:
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    calls.clear()
    try:
        await connector.migrate()
        owner = await connector.create(MixOwner, {"name": "raj"})
        article = await connector.create(MixArticle, {"title": "t", "owner_id": owner.id})
        assert article.created_at is not None and article.updated_at is not None  # stamped
        assert article.slug == "untitled"                                         # mixin default
        assert calls == [("before_create", "MixArticle", "untitled")]             # mixin hook

        found = await connector.find_by_id(MixArticle, article.id, {"include": ["owner"]})
        assert found.owner.name == "raj"                                           # mixin relation

        uniq = await connector.raw(
            "SELECT count(*) AS n FROM pg_constraint WHERE conrelid = 'mix_articles'::regclass AND contype = 'u'"
        )
        assert uniq == [{"n": 1}]                                                  # slug unique=True
    finally:
        for t in tables:
            await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')


def test_unloaded_mixin_relation_raises_like_any_relation():
    from postgres_connector.exceptions import RelationNotLoadedError

    article = MixArticle._from_row({"id": 1, "title": "t", "owner_id": None})
    with pytest.raises(RelationNotLoadedError):
        article.owner
    article.owner = None  # assigning still works and marks it loaded
    assert article.owner is None
