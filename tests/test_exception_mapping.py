import pytest

from postgres_connector import (
    CheckConstraintError,
    Field,
    ForeignKeyConstraintError,
    NotNullConstraintError,
    PostgreSQLModel,
    Relation,
    UniqueConstraintError,
)
from postgres_connector.registry import registry


class ExcUser(PostgreSQLModel):
    __table__ = "exc_users"
    id:    int = Field(primary_key=True)
    email: str = Field(unique=True)
    name:  str = Field(nullable=True)


class ExcOrder(PostgreSQLModel):
    __table__ = "exc_orders"
    id:      int    = Field(primary_key=True)
    user_id: int    = Field(foreign_key="exc_users.id")
    # Field(foreign_key=...) alone is metadata only — the migrator only
    # creates a real FOREIGN KEY constraint from a belongsTo Relation.
    user:    "ExcUser" = Relation(type="belongsTo", foreign_key="user_id")


async def _setup(connector):
    registry.register(ExcUser)
    registry.register(ExcOrder)
    await connector.migrate()


async def _teardown(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS exc_orders")
    await connector.raw_execute("DROP TABLE IF EXISTS exc_users")


@pytest.mark.asyncio
async def test_create_unique_violation_raises_unique_constraint_error(connector):
    await _setup(connector)
    try:
        await connector.create(ExcUser, {"email": "raj@example.com"})

        with pytest.raises(UniqueConstraintError) as exc_info:
            await connector.create(ExcUser, {"email": "raj@example.com"})

        err = exc_info.value
        assert err.table == "exc_users"
        assert err.constraint is not None
        assert "raj@example.com" in (err.detail or "")
        assert err.__cause__ is not None  # original asyncpg error chained
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_create_foreign_key_violation_raises_fk_constraint_error(connector):
    await _setup(connector)
    try:
        with pytest.raises(ForeignKeyConstraintError) as exc_info:
            await connector.create(ExcOrder, {"user_id": 999999})

        err = exc_info.value
        assert err.table == "exc_orders"
        assert err.constraint is not None
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_create_many_unique_violation_raises_unique_constraint_error(connector):
    await _setup(connector)
    try:
        with pytest.raises(UniqueConstraintError):
            await connector.create_many(ExcUser, [
                {"email": "a@example.com"},
                {"email": "a@example.com"},  # duplicate within the same batch
            ])
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_update_by_id_unique_violation_raises_unique_constraint_error(connector):
    await _setup(connector)
    try:
        u1 = await connector.create(ExcUser, {"email": "one@example.com"})
        await connector.create(ExcUser, {"email": "two@example.com"})

        with pytest.raises(UniqueConstraintError):
            await connector.update_by_id(ExcUser, u1.id, {"email": "two@example.com"})
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_upsert_foreign_key_violation_raises_fk_constraint_error(connector):
    await _setup(connector)
    try:
        with pytest.raises(ForeignKeyConstraintError):
            await connector.upsert(
                ExcOrder, {"id": 1, "user_id": 999999}, conflict_target=["id"],
            )
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_raw_execute_unique_violation_raises_unique_constraint_error(connector):
    await _setup(connector)
    try:
        await connector.create(ExcUser, {"email": "raj@example.com"})

        with pytest.raises(UniqueConstraintError):
            await connector.raw_execute(
                "INSERT INTO exc_users (email) VALUES ($1)", ["raj@example.com"],
            )
    finally:
        await _teardown(connector)


@pytest.mark.asyncio
async def test_violation_inside_transaction_also_translated(connector):
    await _setup(connector)
    try:
        await connector.create(ExcUser, {"email": "raj@example.com"})

        with pytest.raises(UniqueConstraintError):
            async with connector.begin() as tx:
                await tx.create(ExcUser, {"email": "raj@example.com"})
    finally:
        await _teardown(connector)


class ExcRequiredNameUser(PostgreSQLModel):
    __table__ = "exc_required_name_users"
    id:   int = Field(primary_key=True)
    name: str = Field(nullable=False)


@pytest.mark.asyncio
async def test_not_null_violation_via_raw_execute(connector):
    """
    Model-level Field(nullable=False) is already enforced Python-side
    before hitting the DB, so the not-null path is exercised through
    raw SQL that bypasses that check, same as an app doing a partial raw
    UPDATE would.
    """
    registry.register(ExcRequiredNameUser)
    await connector.migrate()
    try:
        with pytest.raises(NotNullConstraintError):
            await connector.raw_execute(
                "INSERT INTO exc_required_name_users (id, name) VALUES ($1, $2)", [1, None],
            )
    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS exc_required_name_users")
