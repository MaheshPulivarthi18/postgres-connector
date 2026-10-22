"""auto_now / auto_now_add are TIMESTAMPTZ stamped with aware UTC (#36).

Rows written by the ORM and rows that get the column DEFAULT from another
session must mean the same instant, whatever the session time zone.
"""
import os
import warnings
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel


class TzDoc(PostgreSQLModel):
    __table__ = "tz_docs"
    id:         int = Field(primary_key=True)
    title:      str = Field()
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True, nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS tz_docs")
    await connector.migrate()
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS tz_docs")


async def raw_connection(**server_settings):
    return await asyncpg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=os.getenv("DB_NAME", "test_db"),
        server_settings=server_settings,
    )


async def test_columns_are_timestamptz(connector, table):
    rows = await connector.raw(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'tz_docs' AND column_name IN ('created_at', 'updated_at')"
    )
    assert {r["data_type"] for r in rows} == {"timestamp with time zone"}


async def test_orm_and_sql_default_agree_in_a_non_utc_session(connector, table):
    orm_row = await connector.create(TzDoc, {"title": "orm"})
    other = await raw_connection(timezone="Asia/Kolkata")
    try:
        await other.execute("INSERT INTO tz_docs (title) VALUES ('sql')")
        sql_created = await other.fetchval("SELECT created_at FROM tz_docs WHERE title = 'sql'")
    finally:
        await other.close()
    assert orm_row.created_at.tzinfo is not None
    assert abs(sql_created - orm_row.created_at) < timedelta(seconds=30)


async def test_stamping_uses_no_deprecated_api(connector, table):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        doc = await connector.create(TzDoc, {"title": "a"})
        await connector.update_by_id(TzDoc, doc.id, {"title": "b"})


async def test_existing_timestamp_column_is_converted_as_utc(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS tz_docs")
    await connector.raw_execute(
        "CREATE TABLE tz_docs (id SERIAL PRIMARY KEY, title VARCHAR(255) NOT NULL, "
        "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    # A value the old ORM wrote: naive, meaning UTC.
    await connector.raw_execute("INSERT INTO tz_docs (title, created_at) VALUES ('old', '2026-01-02 03:04:05')")
    # Run the migration from a database whose sessions default to Asia/Kolkata,
    # where a plain TIMESTAMP -> TIMESTAMPTZ cast would shift the value by 5.5 h.
    db = os.getenv("DB_NAME", "test_db")
    await connector.raw_execute(f"ALTER DATABASE \"{db}\" SET timezone TO 'Asia/Kolkata'")
    kolkata = PostgreSQLConnector(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=db,
    )
    try:
        await kolkata.connect()
        assert (await kolkata.raw("SHOW timezone"))[0]["TimeZone"] == "Asia/Kolkata"
        await kolkata.migrate()
        value = await kolkata.raw("SELECT created_at FROM tz_docs WHERE title = 'old'")
        assert value[0]["created_at"] == datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    finally:
        await kolkata.disconnect()
        await connector.raw_execute(f"ALTER DATABASE \"{db}\" RESET timezone")
        await connector.raw_execute("DROP TABLE IF EXISTS tz_docs")
