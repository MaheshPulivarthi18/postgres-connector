import pytest


async def _cleanup(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "file_migration_tester"')
    await connector.raw_execute("DROP TABLE IF EXISTS schema_migrations")


@pytest.mark.asyncio
async def test_run_and_rollback_migration_files(connector, tmp_path):
    await _cleanup(connector)
    version = "20260101_000000_auto"
    (tmp_path / f"{version}_up.sql").write_text('CREATE TABLE "file_migration_tester" (id INT);\n')
    (tmp_path / f"{version}_down.sql").write_text('DROP TABLE "file_migration_tester";\n')
    try:
        assert await connector.run_migrations(str(tmp_path)) == 1
        rows = await connector.raw("SELECT version FROM schema_migrations")
        assert [r["version"] for r in rows] == [version]
        assert await connector.raw("SELECT to_regclass('file_migration_tester') AS t") == [{"t": "file_migration_tester"}]

        # Already applied — a second run is a no-op.
        assert await connector.run_migrations(str(tmp_path)) == 0

        assert await connector.rollback_migrations(str(tmp_path)) == 1
        assert await connector.raw("SELECT version FROM schema_migrations") == []
        assert await connector.raw("SELECT to_regclass('file_migration_tester') AS t") == [{"t": None}]
    finally:
        await _cleanup(connector)
