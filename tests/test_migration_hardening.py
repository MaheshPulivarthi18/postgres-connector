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


@pytest.mark.asyncio
async def test_migration_files_with_semicolons_inside_statements(connector, tmp_path):
    """#14: ';' inside a function body, string literal or comment is not a statement break."""
    async def cleanup():
        await connector.raw_execute('DROP TABLE IF EXISTS "semi_notes"')
        await connector.raw_execute("DROP FUNCTION IF EXISTS semi_add_one(INT)")
        await connector.raw_execute("DROP TABLE IF EXISTS schema_migrations")

    await cleanup()
    version = "20260102_000000_semi"
    (tmp_path / f"{version}_up.sql").write_text(
        "-- creates a helper; and a table\n"
        "CREATE FUNCTION semi_add_one(i INT) RETURNS INT AS $$\n"
        "BEGIN RETURN i + 1; END;\n"
        "$$ LANGUAGE plpgsql;\n"
        "CREATE TABLE semi_notes (body TEXT DEFAULT 'a;b');\n"
        "INSERT INTO semi_notes VALUES ('x;y');\n"
    )
    (tmp_path / f"{version}_down.sql").write_text(
        "DROP TABLE semi_notes; DROP FUNCTION semi_add_one(INT);\n"
    )
    try:
        assert await connector.run_migrations(str(tmp_path)) == 1
        assert await connector.raw("SELECT semi_add_one(41) AS v") == [{"v": 42}]
        await connector.raw_execute("INSERT INTO semi_notes DEFAULT VALUES")
        rows = await connector.raw("SELECT body FROM semi_notes ORDER BY body")
        assert [r["body"] for r in rows] == ["a;b", "x;y"]

        assert await connector.rollback_migrations(str(tmp_path)) == 1
        assert await connector.raw("SELECT to_regclass('semi_notes') AS t") == [{"t": None}]
    finally:
        await cleanup()


@pytest.mark.asyncio
async def test_failing_migration_file_is_rolled_back_and_not_recorded(connector, tmp_path):
    """#14: the whole file plus its bookkeeping row commit or roll back together."""
    await connector.raw_execute('DROP TABLE IF EXISTS "semi_partial"')
    await connector.raw_execute("DROP TABLE IF EXISTS schema_migrations")
    (tmp_path / "20260103_000000_bad_up.sql").write_text(
        "CREATE TABLE semi_partial (id INT);\nSELECT * FROM table_that_does_not_exist;\n"
    )
    try:
        with pytest.raises(Exception, match="table_that_does_not_exist"):
            await connector.run_migrations(str(tmp_path))
        assert await connector.raw("SELECT to_regclass('semi_partial') AS t") == [{"t": None}]
        assert await connector.raw("SELECT version FROM schema_migrations") == []
    finally:
        await connector.raw_execute('DROP TABLE IF EXISTS "semi_partial"')
        await connector.raw_execute("DROP TABLE IF EXISTS schema_migrations")


async def _unique_constraints(connector, table):
    rows = await connector.raw(
        "SELECT conname FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'u' ORDER BY conname",
        [table],
    )
    return [r["conname"] for r in rows]


@pytest.mark.asyncio
async def test_unique_column_gets_exactly_one_constraint(connector):
    """#15: new tables and newly added columns get one unique constraint, not two."""
    await connector.raw_execute('DROP TABLE IF EXISTS "uniq_once"')

    class UniqOnce(PostgreSQLModel):
        __table__ = "uniq_once"
        id:    int = Field(primary_key=True)
        email: str = Field(unique=True)

    try:
        await connector.migrate()
        assert await _unique_constraints(connector, "uniq_once") == ["uq_uniq_once_email"]

        class UniqOnce(PostgreSQLModel):  # noqa: F811 — add a unique column to the existing table
            __table__ = "uniq_once"
            id:     int = Field(primary_key=True)
            email:  str = Field(unique=True)
            handle: str = Field(unique=True, nullable=True)

        await connector.migrate()
        assert await _unique_constraints(connector, "uniq_once") == ["uq_uniq_once_email", "uq_uniq_once_handle"]

        # Uniqueness is still enforced, and a re-run is a no-op.
        await connector.raw_execute("INSERT INTO uniq_once (email) VALUES ('a@x')")
        with pytest.raises(Exception, match="uq_uniq_once_email"):
            await connector.raw_execute("INSERT INTO uniq_once (email) VALUES ('a@x')")
        await connector.migrate()
        assert await _unique_constraints(connector, "uniq_once") == ["uq_uniq_once_email", "uq_uniq_once_handle"]
    finally:
        registry.unregister("UniqOnce")
        await connector.raw_execute('DROP TABLE IF EXISTS "uniq_once"')
