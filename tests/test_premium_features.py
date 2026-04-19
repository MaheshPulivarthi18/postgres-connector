import pytest
import json
import asyncio
from datetime import datetime
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


def _build_insert_payload(model, data):
    """Helper: replicate the final_data filtering logic from connector.create()."""
    for name, field_def in model._fields.items():
        if field_def.auto_now_add and name not in data:
            data[name] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    instance = model(**data)
    final_data = {
        name: getattr(instance, name)
        for name, field_def in model._fields.items()
        if hasattr(instance, name)
        and not (field_def.auto_increment and getattr(instance, name) is None)
    }
    pk = model._get_primary_key()
    qb = QueryBuilder(model.__table__, model._fields)
    return qb.build_insert(final_data, returning_pk=pk)


class _SimpleModel(PostgreSQLModel):
    __table__ = "simple_model"
    id:   int = Field(primary_key=True)
    name: str = Field(max_length=50)


def test_create_excludes_null_auto_increment_pk():
    """SERIAL PK must not appear in the INSERT column list when not provided."""
    sql, params = _build_insert_payload(_SimpleModel, {"name": "Alice"})
    col_list = sql.split("VALUES")[0]
    assert '"id"' not in col_list, "id should be omitted so SERIAL can auto-generate"
    assert None not in params, "None must never appear as a parameter"
    assert params == ["Alice"]
    assert 'RETURNING "id"' in sql


def test_create_keeps_explicit_pk():
    """A user-supplied PK value must be preserved in the INSERT."""
    sql, params = _build_insert_payload(_SimpleModel, {"id": 99, "name": "Bob"})
    assert 99 in params, "explicit id=99 must be included"
    assert '"id"' in sql.split("VALUES")[0]


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
