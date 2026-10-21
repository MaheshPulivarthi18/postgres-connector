"""#16: async LISTEN callbacks are referenced, their errors logged, and cancelled on stop."""
import asyncio
import logging

import pytest


def _tasks_of(connector, channel):
    (wrapped,) = [w for ch, _conn, w in connector._listeners if ch == channel]
    return getattr(wrapped, "_tasks", None)


@pytest.mark.asyncio
async def test_async_callback_error_is_logged_with_channel(connector, caplog):
    channel = "test_listen_async_error"
    failed = asyncio.Event()

    async def on_message(payload):
        failed.set()
        raise RuntimeError("boom " + payload)

    stop = await connector.listen(channel, on_message)
    try:
        with caplog.at_level(logging.ERROR, logger="postgres_connector.pool"):
            await connector.notify(channel, "x")
            await asyncio.wait_for(failed.wait(), timeout=2)
            for _ in range(10):  # let the done-callback run
                await asyncio.sleep(0)
        records = [r for r in caplog.records if r.name == "postgres_connector.pool"]
        assert [r.getMessage() for r in records] == [f"Error in LISTEN callback for channel '{channel}'"]
        assert isinstance(records[0].exc_info[1], RuntimeError)
    finally:
        await stop()


@pytest.mark.asyncio
async def test_in_flight_callback_is_strongly_referenced_until_done(connector):
    channel = "test_listen_task_ref"
    started, release = asyncio.Event(), asyncio.Event()

    async def on_message(payload):
        started.set()
        await release.wait()

    stop = await connector.listen(channel, on_message)
    try:
        await connector.notify(channel, "x")
        await asyncio.wait_for(started.wait(), timeout=2)
        tasks = _tasks_of(connector, channel)
        assert tasks is not None and len(tasks) == 1

        release.set()
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(tasks) == 0
    finally:
        await stop()


@pytest.mark.asyncio
async def test_stop_cancels_callbacks_still_running(connector):
    channel = "test_listen_task_cancel"
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def on_message(payload):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    stop = await connector.listen(channel, on_message)
    await connector.notify(channel, "x")
    await asyncio.wait_for(started.wait(), timeout=2)
    await stop()
    await asyncio.wait_for(cancelled.wait(), timeout=2)
