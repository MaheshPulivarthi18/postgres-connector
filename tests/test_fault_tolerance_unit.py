"""
Unit-level proof of the retry safety boundary — no live DB needed. These
monkeypatch ConnectionPool.fetch_all/fetch_one directly to inject transient
connection failures and assert exactly what retries and what doesn't.
"""

import asyncpg
import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.registry import registry


class RetryUser(PostgreSQLModel):
    __table__ = "retry_users"
    id:   int = Field(primary_key=True)
    name: str = Field(max_length=50)


def _make_connector():
    connector = PostgreSQLConnector()
    connector._pool._connected = True  # bypass connect() — we never touch the real pool
    return connector


class _FlakyFetch:
    """Raises `exc` the first `fail_times` calls, then returns `rows`."""

    def __init__(self, exc, fail_times, rows):
        self.exc = exc
        self.fail_times = fail_times
        self.rows = rows
        self.calls = 0

    async def __call__(self, sql, params, timeout=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc
        return self.rows


@pytest.mark.asyncio
async def test_find_retries_on_transient_connection_error_then_succeeds(monkeypatch):
    connector = _make_connector()
    flaky = _FlakyFetch(ConnectionResetError("simulated drop"), fail_times=2, rows=[{"id": 1, "name": "Raj"}])
    monkeypatch.setattr(connector._pool, "fetch_all", flaky)

    monkeypatch.setattr(connector._pool, "log_query", lambda *a, **k: None)
    users = await connector.find(RetryUser, {})

    assert flaky.calls == 3  # failed twice, succeeded on the 3rd
    assert len(users) == 1
    assert users[0].name == "Raj"


@pytest.mark.asyncio
async def test_find_gives_up_after_max_query_retries(monkeypatch):
    connector = PostgreSQLConnector(max_query_retries=2)
    connector._pool._connected = True
    flaky = _FlakyFetch(asyncpg.exceptions.InterfaceError("connection is closed"), fail_times=99, rows=[])
    monkeypatch.setattr(connector._pool, "fetch_all", flaky)
    monkeypatch.setattr(connector._pool, "log_query", lambda *a, **k: None)

    with pytest.raises(asyncpg.exceptions.InterfaceError):
        await connector.find(RetryUser, {})

    assert flaky.calls == 2  # exactly max_query_retries attempts, no more


@pytest.mark.asyncio
async def test_max_query_retries_zero_disables_retry(monkeypatch):
    connector = PostgreSQLConnector(max_query_retries=0)
    connector._pool._connected = True
    flaky = _FlakyFetch(ConnectionResetError("simulated drop"), fail_times=1, rows=[])
    monkeypatch.setattr(connector._pool, "fetch_all", flaky)
    monkeypatch.setattr(connector._pool, "log_query", lambda *a, **k: None)

    with pytest.raises(ConnectionResetError):
        await connector.find(RetryUser, {})

    assert flaky.calls == 1  # failed on the very first (and only) attempt


@pytest.mark.asyncio
async def test_create_never_retries_even_though_it_uses_fetch_one(monkeypatch):
    """
    The core safety guarantee: create()'s RETURNING round trip goes through
    ConnectionPool.fetch_one, not fetch_all, and is never marked retryable
    — a connection failure there must raise immediately, never retry,
    since retrying a write risks double-executing it.
    """
    connector = _make_connector()
    registry.register(RetryUser)

    calls = {"n": 0}

    async def flaky_fetch_one(sql, params):
        calls["n"] += 1
        raise ConnectionResetError("simulated drop")

    monkeypatch.setattr(connector._pool, "fetch_one", flaky_fetch_one)
    monkeypatch.setattr(connector._pool, "log_query", lambda *a, **k: None)

    with pytest.raises(ConnectionResetError):
        await connector.create(RetryUser, {"name": "Raj"})

    assert calls["n"] == 1  # no retry attempted for a write
