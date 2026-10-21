import sys
from datetime import date, datetime

import pytest

from models_future_annotations import FutAuthor, FutBook


def test_string_annotations_resolve_to_real_types():
    fields = FutAuthor._fields
    assert fields["born"].python_type is datetime
    assert fields["joined"].python_type is datetime
    assert fields["rating"].python_type is float
    assert fields["date"].python_type is date
    # Plain Optional[...] annotations are nullable, bare ones are not.
    assert fields["nickname"].nullable is True
    assert fields["joined"].nullable is False
    # Relations are still relations, not columns.
    assert set(FutAuthor._relations) == {"books"}
    assert FutAuthor._relations["books"].target_model == "FutBook"
    assert FutBook._relations["author"].target_model == "FutAuthor"


@pytest.mark.skipif(sys.version_info < (3, 10), reason="PEP 604 unions need Python 3.10+")
def test_pep604_optional_is_unwrapped_and_nullable():
    from postgres_connector import Field, PostgreSQLModel
    from postgres_connector.registry import registry

    class Pep604(PostgreSQLModel):
        __table__ = "pep604"
        id:    int = Field(primary_key=True)
        score: float | None
        when:  datetime | None = Field()

    try:
        assert Pep604._fields["score"].python_type is float
        assert Pep604._fields["score"].nullable is True
        assert Pep604._fields["when"].python_type is datetime
    finally:
        registry.unregister("Pep604")


@pytest.mark.asyncio
async def test_datetime_field_under_future_annotations_round_trips(connector):
    for t in ("fut_books", "fut_authors"):
        await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    try:
        await connector.migrate()
        cols = await connector.raw(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'fut_authors' AND column_name IN ('born', 'joined')"
        )
        assert {c["data_type"] for c in cols} == {"timestamp without time zone"}

        when = datetime(2026, 1, 2, 3, 4, 5)
        author = await connector.create(FutAuthor, {"name": "raj", "born": when, "joined": when, "rating": 4.5})
        await connector.create(FutBook, {"author_id": author.id})

        found = await connector.find_by_id(FutAuthor, author.id, {"include": ["books"]})
        assert found.born == when
        assert len(found.books) == 1
    finally:
        for t in ("fut_books", "fut_authors"):
            await connector.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
