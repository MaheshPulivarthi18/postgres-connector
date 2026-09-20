import asyncio
import os
import time

import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.exceptions import QueryTimeoutError
from postgres_connector.registry import registry


def _make_connector(**overrides):
    kwargs = dict(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=os.getenv("DB_NAME", "test_db"),
    )
    kwargs.update(overrides)
    return PostgreSQLConnector(**kwargs)


def test_retryable_connection_errors_excludes_timeout_but_keeps_connection_errors():
    """
    Unit-level proof of the narrowing fix: _RETRYABLE_CONNECTION_ERRORS must
    no longer match TimeoutError (the retry-storm bug), but must still
    match genuine connection failures (ConnectionRefusedError/
    ConnectionResetError) exactly as before.
    """
    from postgres_connector.connector import _RETRYABLE_CONNECTION_ERRORS

    assert not isinstance(TimeoutError("slow query"), _RETRYABLE_CONNECTION_ERRORS)
    assert isinstance(ConnectionRefusedError("refused"), _RETRYABLE_CONNECTION_ERRORS)
    assert isinstance(ConnectionResetError("reset"), _RETRYABLE_CONNECTION_ERRORS)


@pytest.mark.asyncio
async def test_raw_timeout_raises_query_timeout_error_not_bare_timeout_error():
    connector = _make_connector()
    await connector.connect()
    try:
        with pytest.raises(QueryTimeoutError) as exc_info:
            await connector.raw("SELECT pg_sleep(2)", timeout=0.3)
        assert exc_info.value.timeout == 0.3
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_timeout_does_not_trigger_a_retry_storm():
    """
    The core bug fix: a timed-out query must fail on the FIRST attempt,
    not get retried max_query_retries times (each eating the same delay
    again). Measure elapsed time to prove it — with the old (buggy)
    OSError-based retry set this would take ~3x as long.
    """
    connector = _make_connector(max_query_retries=5)
    await connector.connect()
    try:
        start = time.monotonic()
        with pytest.raises(QueryTimeoutError):
            await connector.raw("SELECT pg_sleep(2)", timeout=0.3)
        elapsed = time.monotonic() - start

        # One attempt at ~0.3s, not 5 attempts. Generous upper bound to
        # avoid flakiness, but nowhere near what a retry storm would take.
        assert elapsed < 1.5, f"took {elapsed:.2f}s — looks like it retried"
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_per_call_timeout_overrides_connector_default():
    """A generous connector-wide default shouldn't stop a tight per-call override from firing."""
    connector = _make_connector(command_timeout=30.0)
    await connector.connect()
    try:
        with pytest.raises(QueryTimeoutError):
            await connector.raw("SELECT pg_sleep(1)", timeout=0.2)
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_connector_wide_command_timeout_applies_without_per_call_override():
    connector = _make_connector(command_timeout=0.3)
    await connector.connect()
    try:
        with pytest.raises(QueryTimeoutError):
            await connector.raw("SELECT pg_sleep(2)")  # no timeout= — falls back to the 0.3s default
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_fast_query_under_a_tight_timeout_succeeds():
    connector = _make_connector()
    await connector.connect()
    try:
        rows = await connector.raw("SELECT 1 AS ok", timeout=5.0)
        assert rows == [{"ok": 1}]
    finally:
        await connector.disconnect()


@pytest.mark.asyncio
async def test_run_transaction_does_not_retry_a_timeout(connector):
    """
    A slow query inside run_transaction() must raise, not get the whole
    callback retried. It propagates as QueryTimeoutError directly (already
    translated by the time it reaches run_transaction()'s retry check,
    which only matches PostgresError/connection errors — a query timeout
    is neither, so it isn't caught, retried, or re-wrapped there).
    """
    attempts = 0

    async def slow(tx):
        nonlocal attempts
        attempts += 1
        await tx.raw("SELECT pg_sleep(2)", timeout=0.3)

    start = time.monotonic()
    with pytest.raises(QueryTimeoutError):
        await connector.run_transaction(slow, max_retries=5)
    elapsed = time.monotonic() - start

    assert attempts == 1  # not retried
    assert elapsed < 2  # well under 5 attempts worth of delay


class TimeoutTestUser(PostgreSQLModel):
    __table__ = "timeout_test_users"
    id:   int = Field(primary_key=True)
    name: str = Field(max_length=50)


@pytest.mark.asyncio
async def test_create_and_find_still_work_normally_with_timeouts_configured(connector):
    """Sanity check: adding timeout plumbing hasn't broken the ordinary paths."""
    registry.register(TimeoutTestUser)
    await connector.migrate()
    try:
        user = await connector.create(TimeoutTestUser, {"name": "Raj"})
        found = await connector.find_by_id(TimeoutTestUser, user.id)
        assert found.name == "Raj"
    finally:
        await connector.raw_execute("DROP TABLE timeout_test_users")


@pytest.mark.asyncio
async def test_timeout_inside_transaction_via_tx_raw():
    connector = _make_connector()
    await connector.connect()
    try:
        with pytest.raises(QueryTimeoutError):
            async with connector.begin() as tx:
                await tx.raw("SELECT pg_sleep(2)", timeout=0.3)
    finally:
        await connector.disconnect()
