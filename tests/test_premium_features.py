import pytest
import json
from postgres_connector import PostgreSQLModel, Field, JSONField
from postgres_connector.query_builder import QueryBuilder


class PremiumUser(PostgreSQLModel):
    __table__ = "premium_users"
    id:         int = Field(primary_key=True)
    name:       str = Field(max_length=100)
    settings:   dict = JSONField(default={})
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True)
    deleted_at: str = Field(nullable=True)


def test_json_serialization_logic():
    """Unit test for JSON serialization in QueryBuilder."""
    qb = QueryBuilder("premium_users", PremiumUser._fields)
    sql, params = qb.build_insert({"name": "Raj", "settings": {"theme": "dark"}})

    # The settings value should be serialized to a JSON string
    settings_param = next(p for p in params if isinstance(p, str) and "theme" in p)
    assert json.loads(settings_param) == {"theme": "dark"}


def test_json_deserialization_logic():
    """Unit test for JSON deserialization in PostgreSQLModel."""
    row = {
        "id": 1,
        "name": "Raj",
        "settings": '{"theme": "light", "notifications": true}',
    }
    user = PremiumUser._from_row(row)
    assert isinstance(user.settings, dict)
    assert user.settings["theme"] == "light"
    assert user.settings["notifications"] is True


def test_query_builder_param_numbering():
    """Verify that parameters use PostgreSQL $N notation."""
    qb = QueryBuilder("users", {})
    sql, params = qb.build_count({"active": True, "age": {"gte": 18}})
    assert "$1" in sql
    assert "$2" in sql
    assert "%" not in sql
    assert params == [True, 18]


@pytest.mark.asyncio
async def test_hooks(connector):
    """Test model hooks (before/after create)."""

    class HookedUser(PostgreSQLModel):
        __table__ = "hooked_users"
        id:   int = Field(primary_key=True)
        name: str = Field(max_length=50)

        async def before_create(self):
            self.name = f"PRE-{self.name}"

    from postgres_connector.registry import registry
    registry.register(HookedUser)
    await connector.migrate()

    user = await connector.create(HookedUser, {"name": "Test"})
    assert user.name == "PRE-Test"

    await connector.raw_execute('DROP TABLE hooked_users')
