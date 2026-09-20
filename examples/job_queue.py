"""
Job queue example — the reliable-queue-on-plain-Postgres pattern, built
entirely from features already in this connector:

  - lock="update_skip_locked"  -> two workers never grab the same job
  - connector.listen()/notify() -> workers wake up instantly, no polling loop
  - run_transaction()           -> claim-and-mark-in-progress is atomic

Run against a real Postgres instance:

    DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=postgres DB_PASSWORD=secret DB_NAME=test_db \
        python examples/job_queue.py
"""

import asyncio
import os
import random

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.registry import registry

CHANNEL     = "jobs_channel"
NUM_WORKERS = 3
NUM_JOBS    = 8


class Job(PostgreSQLModel):
    __table__ = "example_jobs"
    id:     int = Field(primary_key=True)
    status: str = Field(default="pending")
    data:   str = Field()


async def claim_one_job(connector: PostgreSQLConnector):
    """Atomically grab the oldest pending job, skipping any another worker already has."""
    async def claim(tx):
        job = await tx.find_one(Job, {
            "where": {"status": "pending"},
            "order": ["id ASC"],
            "lock": "update_skip_locked",
        })
        if job is None:
            return None
        await tx.update_by_id(Job, job.id, {"status": "in_progress"})
        return job

    return await connector.run_transaction(claim)


async def worker_loop(
    connector:  PostgreSQLConnector,
    name:       str,
    wake_event: asyncio.Event,
    results:    list,
    stop_event: asyncio.Event,
):
    """
    Runs forever (like a real worker process would) until told to stop.
    It does NOT exit just because the queue is briefly empty — more jobs
    could show up at any time, which is exactly what the "idle, waiting on
    the doorbell" phase below demonstrates.
    """
    while not stop_event.is_set():
        # A little jitter before each attempt spreads workers out instead
        # of one that just finished a job always winning the next race
        # purely because it happened to be first back in line.
        await asyncio.sleep(random.uniform(0, 0.03))

        job = await claim_one_job(connector)

        if job is not None:
            print(f"  [{name}] claimed job {job.id}: {job.data!r}")
            await asyncio.sleep(random.uniform(0.05, 0.15))  # pretend to do the work
            await connector.update_by_id(Job, job.id, {"status": "done"})
            results.append((name, job.id))
            print(f"  [{name}] finished job {job.id}")
            continue  # try to grab another before going back to sleep

        # Nothing to claim right now — wait for the doorbell (a notify) or
        # this short timeout, whichever comes first, then look again.
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=0.2)
        except asyncio.TimeoutError:
            pass


async def create_job(connector: PostgreSQLConnector, i: int):
    async def _create(tx):
        job = await tx.create(Job, {"data": f"task #{i}"})
        await tx.notify(CHANNEL, str(job.id))
        return job

    return await connector.run_transaction(_create)


async def main():
    connector = PostgreSQLConnector(
        host     = os.getenv("DB_HOST", "127.0.0.1"),
        port     = int(os.getenv("DB_PORT", "5432")),
        user     = os.getenv("DB_USER", "postgres"),
        password = os.getenv("DB_PASSWORD", "secret"),
        database = os.getenv("DB_NAME", "test_db"),
    )
    await connector.connect()
    registry.register(Job)
    await connector.migrate()

    try:
        wake_event = asyncio.Event()
        stop_event = asyncio.Event()
        stop_listening = await connector.listen(CHANNEL, lambda _payload: wake_event.set())

        results: list = []
        workers = [
            asyncio.create_task(worker_loop(connector, f"worker-{i + 1}", wake_event, results, stop_event))
            for i in range(NUM_WORKERS)
        ]

        first_batch = NUM_JOBS // 2
        print(f"Creating {first_batch} jobs all at once — {NUM_WORKERS} workers should split them up concurrently...\n")
        await asyncio.gather(*(create_job(connector, i) for i in range(first_batch)))

        await asyncio.sleep(0.5)  # let the burst drain; workers go idle waiting on the doorbell

        print(f"\nWorkers now idle. Creating {NUM_JOBS - first_batch} more jobs one at a time "
              f"to show instant wake-up via LISTEN/NOTIFY...\n")
        for i in range(first_batch, NUM_JOBS):
            await create_job(connector, i)
            await asyncio.sleep(0.3)

        # Workers run forever otherwise (they're meant to be long-lived) —
        # for this demo, stop them once every job we created is done.
        while len(results) < NUM_JOBS:
            await asyncio.sleep(0.05)
        stop_event.set()

        await asyncio.gather(*workers)
        await stop_listening()

        print(f"\nAll {len(results)} jobs completed:")
        for name, job_id in sorted(results, key=lambda r: r[1]):
            print(f"  job {job_id} -> {name}")

        job_ids = [job_id for _, job_id in results]
        assert len(job_ids) == NUM_JOBS
        assert len(job_ids) == len(set(job_ids)), "a job was processed more than once!"
        print("\nEvery job ran exactly once, no duplicates. ✅")

    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS example_jobs")
        await connector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
