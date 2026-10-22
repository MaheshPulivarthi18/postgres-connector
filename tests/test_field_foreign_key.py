"""Field(foreign_key="table.column") creates a real constraint in migrate() (#37)."""
import pytest

from postgres_connector import Field, PostgreSQLModel, Relation
from postgres_connector.exceptions import ConfigurationError, ForeignKeyConstraintError
from postgres_connector.registry import registry


class FkAuthor(PostgreSQLModel):
    __table__ = "fk_authors"
    id:   int = Field(primary_key=True)
    name: str = Field()


class FkBook(PostgreSQLModel):
    __table__ = "fk_books"
    id:        int = Field(primary_key=True)
    author_id: int = Field(index=True)
    editor_id: int = Field(nullable=True, foreign_key="fk_authors.id")  # declared on the Field only
    title:     str = Field()
    author:    "FkAuthor" = Relation(type="belongsTo", foreign_key="author_id")


@pytest.fixture
async def tables(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS fk_books, fk_authors")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS fk_books, fk_authors")


async def test_field_foreign_key_creates_a_constraint(connector, tables):
    rows = await connector.raw(
        "SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint "
        "WHERE conrelid = 'fk_books'::regclass AND contype = 'f' ORDER BY conname"
    )
    defs = {r["conname"]: r["def"] for r in rows}
    assert defs["fk_fk_books_author_id"].startswith("FOREIGN KEY (author_id) REFERENCES fk_authors(id)")
    assert defs["fk_fk_books_editor_id"] == "FOREIGN KEY (editor_id) REFERENCES fk_authors(id) ON DELETE SET NULL"


async def test_the_constraint_is_enforced(connector, tables):
    author = await connector.create(FkAuthor, {"name": "a"})
    with pytest.raises(ForeignKeyConstraintError):
        await connector.create(FkBook, {"author_id": author.id, "editor_id": 999, "title": "t"})


async def test_migrate_is_idempotent(connector, tables):
    await connector.migrate()  # no duplicate-constraint error on a second run


async def test_malformed_reference_is_a_configuration_error(connector):
    class FkBad(PostgreSQLModel):
        __table__ = "fk_bad"
        id:  int = Field(primary_key=True)
        ref: int = Field(foreign_key="fk_authors")

    try:
        with pytest.raises(ConfigurationError, match="foreign_key must look like 'table.column'"):
            await connector.migrate()
    finally:
        registry.unregister("FkBad")
        await connector.raw_execute("DROP TABLE IF EXISTS fk_bad")
