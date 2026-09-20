"""
Real fault-tolerance proof: actually stop and restart the Postgres
container the suite is running against, and confirm a retryable read
survives the outage instead of just failing.

Requires the `docker` CLI plus FAULT_TOLERANCE_DOCKER_CONTAINER naming the
container to stop/start — skipped everywhere else (most environments,
including plain CI, won't set this), matching the rest of this suite's
assumption of a reachable Postgres but going one step further by also
needing control over that Postgres's container.
"""

import asyncio
import os
import shutil
import subprocess

import pytest

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel
from postgres_connector.registry import registry

DOCKER_CONTAINER = os.getenv("FAULT_TOLERANCE_DOCKER_CONTAINER")

pytestmark = pytest.mark.skipif(
    not (shutil.which("docker") and DOCKER_CONTAINER),
    reason="requires the docker CLI and FAULT_TOLERANCE_DOCKER_CONTAINER naming the test Postgres container",
)


class HealthCheck(PostgreSQLModel):
    __table__ = "fault_tolerance_health_check"
    id: int = Field(primary_key=True)


async def _docker(*args) -> int:
    proc = await asyncio.create_subprocess_exec(
        "docker", *args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return await proc.wait()


async def _wait_until_ready(db_user: str, timeout: float = 30.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        code = await _docker("exec", DOCKER_CONTAINER, "pg_isready", "-U", db_user)
        if code == 0:
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(f"{DOCKER_CONTAINER} did not become ready again in time")


@pytest.mark.asyncio
async def test_read_survives_a_real_database_restart():
    connector = PostgreSQLConnector(
        host     = os.getenv("DB_HOST", "127.0.0.1"),
        port     = int(os.getenv("DB_PORT", "5432")),
        user     = os.getenv("DB_USER", "postgres"),
        password = os.getenv("DB_PASSWORD", "secret"),
        database = os.getenv("DB_NAME", "test_db"),
        max_query_retries=10,
    )
    await connector.connect()
    registry.register(HealthCheck)
    await connector.migrate()

    try:
        # Sanity check: it works before we break anything.
        assert await connector.count(HealthCheck) == 0

        assert await _docker("stop", "-t", "2", DOCKER_CONTAINER) == 0

        async def restart_soon():
            await asyncio.sleep(1.0)
            assert await _docker("start", DOCKER_CONTAINER) == 0
            await _wait_until_ready(os.getenv("DB_USER", "postgres"))

        restart_task = asyncio.create_task(restart_soon())

        # The DB is down right now. This must retry through the outage and
        # succeed once restart_soon() brings it back — bounded so a genuine
        # failure fails the test instead of hanging the suite forever.
        count = await asyncio.wait_for(connector.count(HealthCheck), timeout=30)
        assert count == 0

        await restart_task
    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS fault_tolerance_health_check")
        await connector.disconnect()
