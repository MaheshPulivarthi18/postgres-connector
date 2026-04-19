import os
import pytest
from postgres_connector import PostgreSQLConnector


@pytest.fixture(scope="function")
async def connector():
    """Function-scoped connector fixture."""
    import asyncpg

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASSWORD", "secret")
    db_name = os.getenv("DB_NAME", "test_db")

    # Bootstrap: create test database if it doesn't exist
    sys_conn = await asyncpg.connect(
        host=db_host, port=db_port,
        user=db_user, password=db_pass,
        database="postgres",
    )
    try:
        await sys_conn.execute(f'CREATE DATABASE "{db_name}"')
    except asyncpg.exceptions.DuplicateDatabaseError:
        pass
    finally:
        await sys_conn.close()

    conn = PostgreSQLConnector(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pass,
        database=db_name,
    )
    await conn.connect()
    yield conn
    await conn.disconnect()


@pytest.fixture(autouse=True)
async def cleanup_db(connector):
    """Provide connector to all tests; tests clean up their own tables."""
    yield
