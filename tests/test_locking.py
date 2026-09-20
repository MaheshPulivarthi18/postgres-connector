import asyncio

import pytest

from postgres_connector import Field, PostgreSQLModel
from postgres_connector.exceptions import QueryError
from postgres_connector.registry import registry


class LockJob(PostgreSQLModel):
    __table__ = "lock_jobs"
    id:     int = Field(primary_key=True)
    status: str = Field(default="pending")


async def _setup(connector):
    registry.register(LockJob)
    await connector.migrate()


async def _teardown(connector):
    await connector.raw_execute("DROP TABLE lock_jobs")


@pytest.mark.asyncio
async def test_find_with_lock_outside_transaction_raises(connector):
    await _setup(connector)
    try:
        with pytest.raises(QueryError):
            await connector.find(LockJob, {"lock": "update"})
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_find_one_with_lock_inside_transaction_works(connector):
    await _setup(connector)
    try:
        job = await connector.create(LockJob, {"status": "pending"})
        async with connector.begin() as tx:
            locked = await tx.find_one(LockJob, {"where": {"id": job.id}, "lock": "update"})
            assert locked is not None
            assert locked.id == job.id
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_update_skip_locked_skips_a_locked_row(connector):
    """
    Real concurrency proof: two 'pending' rows, one transaction holds a
    SKIP LOCKED lock on the first; a second concurrent transaction with the
    same query must get the second row instead of blocking on the first.
    """
    await _setup(connector)
    try:
        job1 = await connector.create(LockJob, {"status": "pending"})
        job2 = await connector.create(LockJob, {"status": "pending"})

        holder_ready = asyncio.Event()
        release_holder = asyncio.Event()
        holder_result = {}

        async def holder():
            async with connector.begin() as tx:
                row = await tx.find_one(LockJob, {
                    "where": {"status": "pending"},
                    "order": ["id ASC"],
                    "lock": "update_skip_locked",
                })
                holder_result["row"] = row
                holder_ready.set()
                await release_holder.wait()

        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(holder_ready.wait(), timeout=2)
        assert holder_result["row"].id == job1.id  # holder locked the first row

        async with connector.begin() as tx2:
            skipped_to = await tx2.find_one(LockJob, {
                "where": {"status": "pending"},
                "order": ["id ASC"],
                "lock": "update_skip_locked",
            })

        assert skipped_to is not None
        assert skipped_to.id == job2.id  # job1 was locked -> this must skip to job2

        release_holder.set()
        await asyncio.wait_for(holder_task, timeout=2)
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_plain_update_lock_blocks_a_second_transaction(connector):
    """
    Real concurrency proof for plain lock="update" (no skip): a second
    transaction requesting the same locked row must actually block, not
    just build the right SQL string.
    """
    await _setup(connector)
    try:
        job = await connector.create(LockJob, {"status": "pending"})

        holder_ready = asyncio.Event()
        release_holder = asyncio.Event()

        async def holder():
            async with connector.begin() as tx:
                await tx.find_one(LockJob, {"where": {"id": job.id}, "lock": "update"})
                holder_ready.set()
                await release_holder.wait()

        async def waiter():
            async with connector.begin() as tx:
                return await tx.find_one(LockJob, {"where": {"id": job.id}, "lock": "update"})

        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(holder_ready.wait(), timeout=2)

        waiter_task = asyncio.create_task(waiter())
        with pytest.raises(asyncio.TimeoutError):
            # shield: a timeout here must not cancel waiter_task — it should
            # keep waiting on the DB lock in the background so we can still
            # await it after releasing the holder below.
            await asyncio.wait_for(asyncio.shield(waiter_task), timeout=0.3)

        release_holder.set()
        await asyncio.wait_for(holder_task, timeout=2)

        row = await asyncio.wait_for(waiter_task, timeout=2)
        assert row.id == job.id
    finally:
        await _teardown(connector)
