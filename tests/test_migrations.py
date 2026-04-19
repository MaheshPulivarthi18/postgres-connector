import pytest
from postgres_connector import PostgreSQLModel, Field, JSONField
from postgres_connector.registry import registry


@pytest.mark.asyncio
async def test_migration_workflow(connector):
    """Test table creation and column addition via migrate()."""

    # 1. Define initial model
    class MigrationTester(PostgreSQLModel):
        __table__ = "migration_tester"
        id:   int = Field(primary_key=True)
        name: str = Field(max_length=50)

    registry.register(MigrationTester)
    await connector.migrate()

    # Verify table existence by inserting
    user = await connector.create(MigrationTester, {"name": "Test Table Creation"})
    assert user.id is not None
    assert user.name == "Test Table Creation"

    # 2. Add column by redefining model
    class MigrationTester(PostgreSQLModel):  # noqa: F811
        __table__ = "migration_tester"
        id:         int  = Field(primary_key=True)
        name:       str  = Field(max_length=50)
        extra_info: dict = JSONField(default={})

    registry.register(MigrationTester)
    await connector.migrate()

    # Verify column existence
    user = await connector.create(MigrationTester, {
        "name": "Test Column Addition",
        "extra_info": {"notes": "Works perfectly!"},
    })
    assert user.extra_info["notes"] == "Works perfectly!"

    # Cleanup
    await connector.raw_execute('DROP TABLE migration_tester')
