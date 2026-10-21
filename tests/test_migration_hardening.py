import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.registry import registry


async def _columns(connector, table):
    rows = await connector.raw(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
        [table],
    )
    return [r["column_name"] for r in rows]


@pytest.mark.asyncio
async def test_migrate_rolls_back_every_statement_when_one_fails(connector):
    """#13: migrate() must not leave the schema half-migrated."""
    await connector.raw_execute('DROP TABLE IF EXISTS "atomic_dup"')
    await connector.raw_execute('CREATE TABLE "atomic_dup" (id SERIAL PRIMARY KEY, email VARCHAR(255))')
    await connector.raw_execute("INSERT INTO atomic_dup (email) VALUES ('a@x'), ('a@x')")

    class AtomicDup(PostgreSQLModel):
        __table__ = "atomic_dup"
        id:       int = Field(primary_key=True)
        email:    str = Field(unique=True, nullable=True)  # fails: duplicates exist
        nickname: str = Field(nullable=True)               # applied first on main

    try:
        with pytest.raises(Exception, match="uq_atomic_dup_email"):
            await connector.migrate()
        assert await _columns(connector, "atomic_dup") == ["id", "email"]
    finally:
        registry.unregister("AtomicDup")
        await connector.raw_execute('DROP TABLE IF EXISTS "atomic_dup"')
