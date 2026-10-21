from datetime import datetime

import pytest

from postgres_connector import Field, PostgreSQLModel


class StampedDoc(PostgreSQLModel):
    __table__ = "stamped_docs"
    id:         int = Field(primary_key=True)
    title:      str = Field()
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True, nullable=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute('DROP TABLE IF EXISTS "stamped_docs"')
    await connector.migrate()
    yield
    await connector.raw_execute('DROP TABLE IF EXISTS "stamped_docs"')


@pytest.mark.asyncio
async def test_create_does_not_mutate_the_callers_dict(connector, table):
    data = {"title": "a"}
    await connector.create(StampedDoc, data)
    assert data == {"title": "a"}

    # Reusing the same dict must stamp a fresh time, not a stale one.
    first = await connector.create(StampedDoc, data)
    second = await connector.create(StampedDoc, data)
    assert second.created_at > first.created_at
    assert data == {"title": "a"}


@pytest.mark.asyncio
async def test_create_stamps_auto_now_like_upsert_does(connector, table):
    before = datetime.utcnow()
    doc = await connector.create(StampedDoc, {"title": "a"})
    assert isinstance(doc.updated_at, datetime)
    assert doc.updated_at >= before.replace(microsecond=0)
    assert doc.updated_at == doc.created_at
