"""find_and_count(): a page plus the total, concurrently outside a transaction (#45)."""
import asyncio

import pytest

from postgres_connector import Field, PostgreSQLModel


class FcUser(PostgreSQLModel):
    __table__ = "fc_users"
    id:     int  = Field(primary_key=True)
    name:   str  = Field()
    active: bool = Field(default=True)


@pytest.fixture
async def users(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS fc_users")
    await connector.migrate()
    await connector.create_many(FcUser, [{"name": f"u{i:02d}", "active": i % 2 == 0} for i in range(30)])
    yield
    await connector.raw_execute("DROP TABLE IF EXISTS fc_users")


async def test_page_and_total(connector, users):
    items, total = await connector.find_and_count(
        FcUser, {"where": {"active": True}, "order": ["name ASC"], "limit": 5, "skip": 5}
    )
    assert total == 15  # count ignores limit/skip/order
    assert [u.name for u in items] == ["u10", "u12", "u14", "u16", "u18"]


async def test_no_filter_counts_everything(connector, users):
    items, total = await connector.find_and_count(FcUser, {"limit": 3})
    assert len(items) == 3 and total == 30


async def test_inside_a_transaction(connector, users):
    async with connector.begin() as tx:
        await tx.create(FcUser, {"name": "new", "active": True})
        items, total = await tx.find_and_count(FcUser, {"where": {"name": "new"}})
    assert total == 1 and items[0].name == "new"


async def test_queries_run_concurrently_without_a_session(connector, users, monkeypatch):
    running, overlap = 0, []
    real_find, real_count = connector.find, connector.count

    def track(fn):
        async def wrapper(*args, **kwargs):
            nonlocal running
            running += 1
            overlap.append(running)
            await asyncio.sleep(0.05)
            try:
                return await fn(*args, **kwargs)
            finally:
                running -= 1
        return wrapper

    monkeypatch.setattr(connector, "find", track(real_find))
    monkeypatch.setattr(connector, "count", track(real_count))
    await connector.find_and_count(FcUser, {"limit": 1})
    assert max(overlap) == 2
