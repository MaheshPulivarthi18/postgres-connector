import pytest

from postgres_connector import Field, PostgreSQLModel, Procedure
from postgres_connector.exceptions import ProcedureNotRegisteredError, QueryError
from postgres_connector.procedures import procedures
from postgres_connector.registry import registry


class ProcUser(PostgreSQLModel):
    __table__ = "proc_users"
    id:     int  = Field(primary_key=True)
    name:   str  = Field(max_length=50)
    active: bool = Field(default=True)


class ProcCounter(PostgreSQLModel):
    __table__ = "proc_counters"
    id:    int = Field(primary_key=True)
    count: int = Field(default=0)


@pytest.mark.asyncio
async def test_call_validates_missing_and_unknown_arguments():
    """Argument validation must fail before anything touches the database."""
    from postgres_connector import PostgreSQLConnector

    proc = Procedure("does_not_matter_for_this_test", params=["a", "b"])
    connector = PostgreSQLConnector()  # never connected — validation happens first

    with pytest.raises(QueryError):
        await connector.call(proc, a=1)  # missing 'b'
    with pytest.raises(QueryError):
        await connector.call(proc, a=1, b=2, c=3)  # unexpected 'c'


def test_call_by_unregistered_name_raises():
    with pytest.raises(ProcedureNotRegisteredError):
        procedures.get("totally_made_up_procedure_name")


@pytest.mark.asyncio
async def test_call_scalar_function(connector):
    await connector.raw_execute("""
        CREATE FUNCTION proc_add_numbers(a INT, b INT) RETURNS INT AS $$
        BEGIN RETURN a + b; END;
        $$ LANGUAGE plpgsql
    """)
    try:
        add_numbers = Procedure("proc_add_numbers", params=["a", "b"])
        total = await connector.call(add_numbers, a=2, b=3)
        assert total == 5

        # Also callable by registered name.
        total2 = await connector.call("proc_add_numbers", a=10, b=20)
        assert total2 == 30
    finally:
        await connector.raw_execute("DROP FUNCTION proc_add_numbers")


@pytest.mark.asyncio
async def test_call_table_returning_function_hydrates_models(connector):
    registry.register(ProcUser)
    await connector.migrate()
    try:
        await connector.create(ProcUser, {"name": "Raj", "active": True})
        await connector.create(ProcUser, {"name": "Inactive", "active": False})

        await connector.raw_execute("""
            CREATE FUNCTION proc_get_active_users() RETURNS SETOF proc_users AS $$
            BEGIN RETURN QUERY SELECT * FROM proc_users WHERE active = true; END;
            $$ LANGUAGE plpgsql
        """)
        try:
            get_active_users = Procedure("proc_get_active_users", returns=ProcUser)
            result = await connector.call(get_active_users)

            assert len(result) == 1
            assert isinstance(result[0], ProcUser)
            assert result[0].name == "Raj"
        finally:
            await connector.raw_execute("DROP FUNCTION proc_get_active_users")
    finally:
        await connector.raw_execute("DROP TABLE proc_users")


@pytest.mark.asyncio
async def test_call_procedure_kind_has_side_effect_and_returns_none(connector):
    registry.register(ProcCounter)
    await connector.migrate()
    try:
        counter = await connector.create(ProcCounter, {"count": 0})

        await connector.raw_execute("""
            CREATE PROCEDURE proc_bump_counter(p_id INT) AS $$
            BEGIN UPDATE proc_counters SET count = count + 1 WHERE id = p_id; END;
            $$ LANGUAGE plpgsql
        """)
        try:
            bump_counter = Procedure("proc_bump_counter", params=["p_id"], kind="procedure")
            result = await connector.call(bump_counter, p_id=counter.id)
            assert result is None

            refetched = await connector.find_by_id(ProcCounter, counter.id)
            assert refetched.count == 1
        finally:
            await connector.raw_execute("DROP PROCEDURE proc_bump_counter")
    finally:
        await connector.raw_execute("DROP TABLE proc_counters")


@pytest.mark.asyncio
async def test_sync_procedures_creates_function_from_body(connector):
    """
    Unlike the tests above (which pre-create the function via raw_execute
    to isolate what call() does), this proves sync_procedures() itself
    creates the function in the database from nothing but the Procedure's
    declared `body` — the actual "register a procedure" story.

    `ProcedureRegistry` is a process-wide singleton (like the model
    registry), so every test that declares a Procedure MUST unregister it
    in `finally` — otherwise a later test's broad sync_procedures() call
    (which syncs everything currently registered) silently recreates this
    one's function in the database after this test already dropped it,
    and the next full-suite run starts from dirty state.
    """
    await connector.raw_execute("DROP FUNCTION IF EXISTS proc_sync_multiply")  # guarantee a clean baseline
    multiply = Procedure(
        "proc_sync_multiply",
        params=["a", "b"],
        body="""
            CREATE OR REPLACE FUNCTION proc_sync_multiply(a INT, b INT) RETURNS INT AS $$
            BEGIN RETURN a * b; END;
            $$ LANGUAGE plpgsql
        """,
    )
    try:
        # Not created yet — calling it now must fail.
        with pytest.raises(Exception):
            await connector.call(multiply, a=2, b=3)

        synced = await connector.sync_procedures()
        assert synced >= 1

        result = await connector.call(multiply, a=6, b=7)
        assert result == 42
    finally:
        await connector.raw_execute("DROP FUNCTION IF EXISTS proc_sync_multiply")
        procedures.unregister("proc_sync_multiply")


@pytest.mark.asyncio
async def test_sync_procedures_is_idempotent(connector):
    """CREATE OR REPLACE means running sync_procedures() twice must not error."""
    Procedure(
        "proc_sync_greet",
        params=["name"],
        body="""
            CREATE OR REPLACE FUNCTION proc_sync_greet(name TEXT) RETURNS TEXT AS $$
            BEGIN RETURN 'hello ' || name; END;
            $$ LANGUAGE plpgsql
        """,
    )
    try:
        await connector.sync_procedures()
        await connector.sync_procedures()  # must not raise "already exists"

        result = await connector.call("proc_sync_greet", name="raj")
        assert result == "hello raj"
    finally:
        await connector.raw_execute("DROP FUNCTION IF EXISTS proc_sync_greet")
        procedures.unregister("proc_sync_greet")


@pytest.mark.asyncio
async def test_sync_procedures_skips_procedures_without_a_body(connector):
    """A Procedure with no `body` is calling-only — sync_procedures() must not touch it."""
    try:
        before = await connector.sync_procedures()  # whatever's registered from earlier tests

        Procedure("proc_no_body_declared_only")  # no body — never created, just a binding
        after = await connector.sync_procedures()

        assert after == before  # the bodyless one added nothing to sync
    finally:
        procedures.unregister("proc_no_body_declared_only")


@pytest.mark.asyncio
async def test_sync_procedures_inside_transaction(connector):
    Procedure(
        "proc_sync_in_tx",
        params=["a"],
        body="""
            CREATE OR REPLACE FUNCTION proc_sync_in_tx(a INT) RETURNS INT AS $$
            BEGIN RETURN a + 100; END;
            $$ LANGUAGE plpgsql
        """,
    )
    try:
        async with connector.begin() as tx:
            synced = await tx.sync_procedures()
            assert synced >= 1
            result = await tx.call("proc_sync_in_tx", a=1)
            assert result == 101
    finally:
        await connector.raw_execute("DROP FUNCTION IF EXISTS proc_sync_in_tx")
        procedures.unregister("proc_sync_in_tx")
