import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.migrator import Migrator


class QuotedDefault(PostgreSQLModel):
    __table__ = "quoted_defaults"
    id:     int = Field(primary_key=True)
    label:  str = Field(default="O'Brien")
    tricky: str = Field(default="x'); DROP TABLE quoted_defaults; --")


def test_string_default_is_escaped_in_ddl():
    sql = Migrator(None)._build_column_sql(QuotedDefault._fields["label"])
    assert sql.endswith("DEFAULT 'O''Brien'")


@pytest.mark.asyncio
async def test_string_defaults_with_quotes_migrate_and_apply(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "quoted_defaults"')
    try:
        await connector.migrate()
        # Omit the columns so the database-side DEFAULTs are what fill them.
        await connector.raw_execute('INSERT INTO "quoted_defaults" DEFAULT VALUES')
        rows = await connector.raw('SELECT label, tricky FROM "quoted_defaults"')
        assert rows == [{"label": "O'Brien", "tricky": "x'); DROP TABLE quoted_defaults; --"}]
    finally:
        await connector.raw_execute('DROP TABLE IF EXISTS "quoted_defaults"')
