import asyncio

import pytest


@pytest.mark.asyncio
async def test_notify_delivers_payload_to_listener(connector):
    channel = "test_channel_basic"
    received = asyncio.Event()
    box = {}

    def on_message(payload):
        box["payload"] = payload
        received.set()

    stop = await connector.listen(channel, on_message)
    try:
        await connector.notify(channel, "hello world")
        await asyncio.wait_for(received.wait(), timeout=2)
        assert box["payload"] == "hello world"
    finally:
        await stop()


@pytest.mark.asyncio
async def test_listen_supports_async_callback(connector):
    channel = "test_channel_async"
    received = asyncio.Event()
    box = {}

    async def on_message(payload):
        await asyncio.sleep(0)  # prove it's actually awaited, not just called
        box["payload"] = payload
        received.set()

    stop = await connector.listen(channel, on_message)
    try:
        await connector.notify(channel, "async payload")
        await asyncio.wait_for(received.wait(), timeout=2)
        assert box["payload"] == "async payload"
    finally:
        await stop()


@pytest.mark.asyncio
async def test_stop_listening_stops_delivery(connector):
    channel = "test_channel_stop"
    received = asyncio.Event()

    def on_message(_payload):
        received.set()

    stop = await connector.listen(channel, on_message)
    await stop()

    await connector.notify(channel, "should not arrive")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(received.wait(), timeout=0.3)


@pytest.mark.asyncio
async def test_notify_inside_transaction_delivered_only_after_commit(connector):
    """
    Real proof of Postgres's transactional NOTIFY semantics: a notify sent
    via tx.notify() must NOT be delivered while the transaction is still
    open, and must be delivered once it commits.
    """
    channel = "test_channel_transactional"
    received = asyncio.Event()
    box = {}

    def on_message(payload):
        box["payload"] = payload
        received.set()

    stop = await connector.listen(channel, on_message)
    try:
        entered_tx = asyncio.Event()
        hold_open = asyncio.Event()

        async def sender():
            async with connector.begin() as tx:
                await tx.notify(channel, "committed-message")
                entered_tx.set()
                await hold_open.wait()
            # transaction commits here, on clean exit of the `async with` block

        sender_task = asyncio.create_task(sender())
        await asyncio.wait_for(entered_tx.wait(), timeout=2)

        # Not delivered yet — the sending transaction hasn't committed.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received.wait(), timeout=0.3)

        hold_open.set()
        await asyncio.wait_for(sender_task, timeout=2)

        # Delivered now that the transaction committed.
        await asyncio.wait_for(received.wait(), timeout=2)
        assert box["payload"] == "committed-message"
    finally:
        await stop()


@pytest.mark.asyncio
async def test_notify_inside_rolled_back_transaction_never_delivered(connector):
    channel = "test_channel_rollback"
    received = asyncio.Event()

    def on_message(_payload):
        received.set()

    stop = await connector.listen(channel, on_message)
    try:
        class _Boom(Exception):
            pass

        with pytest.raises(_Boom):
            async with connector.begin() as tx:
                await tx.notify(channel, "never arrives")
                raise _Boom()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(received.wait(), timeout=0.3)
    finally:
        await stop()


@pytest.mark.asyncio
async def test_disconnect_cleans_up_dangling_listeners(connector):
    channel = "test_channel_cleanup"

    def on_message(_payload):
        pass

    conn, wrapped = await connector._pool.listen(channel, on_message)
    connector._listeners.append((channel, conn, wrapped))

    assert not conn.is_closed()
    await connector.disconnect()
    assert conn.is_closed()
    assert connector._listeners == []

    # Restore connection for the fixture's own teardown.
    await connector.connect()
