import pytest
import asyncio
from datetime import datetime
from postgres_connector import PostgreSQLModel, Field, JSONField
from postgres_connector.exceptions import QueryError
from postgres_connector.query_builder import QueryBuilder


class PremiumUser(PostgreSQLModel):
    __table__ = "premium_users"
    id:         int = Field(primary_key=True)
    name:       str = Field(max_length=100)
    settings:   dict = JSONField(default={})
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True)


def test_json_serialization_logic():
    """
    QueryBuilder must NOT pre-serialize JSON/JSONB values to strings — the
    connection-level type codec (pool.py's _init_connection) already calls
    json.dumps() on the way to the wire. Serializing here too double-encodes
    the value (it gets stored as a JSON string scalar instead of an object).
    """
    qb = QueryBuilder("premium_users", PremiumUser._fields)
    sql, params = qb.build_insert({"name": "Raj", "settings": {"theme": "dark"}})

    settings_param = next(p for p in params if isinstance(p, dict))
    assert settings_param == {"theme": "dark"}


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
    # QueryBuilder validates every WHERE key against the model's field
    # definitions (SQL-injection guard) — pass the fields it's expected to
    # accept rather than an empty dict, which would reject them all.
    fields = {
        "active": Field().to_definition("active", bool),
        "age":    Field().to_definition("age", int),
    }
    qb = QueryBuilder("users", fields)
    sql, params = qb.build_count({"active": True, "age": {"gte": 18}})
    assert "$1" in sql
    assert "$2" in sql
    assert "%" not in sql
    assert params == [True, 18]


def test_build_exists_uses_exists_not_count():
    """exists() should stop at the first match, not count every row."""
    fields = {"email": Field().to_definition("email", str)}
    qb = QueryBuilder("users", fields)
    sql, params = qb.build_exists({"email": "raj@example.com"})
    assert "EXISTS(" in sql
    assert "COUNT" not in sql
    assert params == ["raj@example.com"]


def test_build_update_returning_all_appends_returning_star():
    fields = {
        "id":   Field().to_definition("id", int),
        "name": Field().to_definition("name", str),
    }
    qb = QueryBuilder("users", fields)
    sql, params = qb.build_update({"id": 1}, {"name": "Rajesh"}, returning_all=True)
    assert sql.rstrip().endswith("RETURNING *")

    qb2 = QueryBuilder("users", fields)
    sql2, _ = qb2.build_update({"id": 1}, {"name": "Rajesh"})
    assert "RETURNING" not in sql2


def test_build_select_lock_clause_modes():
    from postgres_connector.types import FilterOptions

    fields = {"status": Field().to_definition("status", str)}
    expected = {
        "update":             "FOR UPDATE",
        "update_nowait":      "FOR UPDATE NOWAIT",
        "update_skip_locked": "FOR UPDATE SKIP LOCKED",
        "share":              "FOR SHARE",
    }
    for lock_mode, clause in expected.items():
        qb = QueryBuilder("jobs", fields)
        sql, _ = qb.build_select(FilterOptions(lock=lock_mode))
        assert sql.rstrip().endswith(clause), f"{lock_mode} -> {sql!r}"

    qb_none = QueryBuilder("jobs", fields)
    sql_none, _ = qb_none.build_select(FilterOptions())
    assert "FOR " not in sql_none


def test_build_select_unknown_lock_mode_raises():
    from postgres_connector.types import FilterOptions

    fields = {"status": Field().to_definition("status", str)}
    qb = QueryBuilder("jobs", fields)
    with pytest.raises(QueryError):
        qb.build_select(FilterOptions(lock="bogus"))


def _upsert_fields():
    return {
        "email":       Field(unique=True).to_definition("email", str),
        "name":        Field().to_definition("name", str),
        "login_count": Field().to_definition("login_count", int),
    }


def test_build_upsert_default_update_fields_excludes_conflict_target():
    qb = QueryBuilder("users", _upsert_fields())
    sql, params = qb.build_upsert(
        {"email": "a@b.com", "name": "Raj", "login_count": 1},
        conflict_target=["email"],
    )
    assert 'ON CONFLICT ("email")' in sql
    assert 'DO UPDATE SET' in sql
    assert '"email" = EXCLUDED."email"' not in sql   # conflict target excluded by default
    assert '"name" = EXCLUDED."name"' in sql
    assert '"login_count" = EXCLUDED."login_count"' in sql
    assert sql.rstrip().endswith("RETURNING *")
    assert params == ["a@b.com", "Raj", 1]


def test_build_upsert_empty_update_fields_means_do_nothing():
    qb = QueryBuilder("users", _upsert_fields())
    sql, _ = qb.build_upsert(
        {"email": "a@b.com", "name": "Raj"},
        conflict_target=["email"],
        update_fields=[],
    )
    assert "DO NOTHING" in sql
    assert "DO UPDATE" not in sql


def test_build_upsert_requires_conflict_target():
    qb = QueryBuilder("users", _upsert_fields())
    with pytest.raises(QueryError):
        qb.build_upsert({"email": "a@b.com"}, conflict_target=[])


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


@pytest.mark.asyncio
async def test_create_with_auto_now_fields_against_a_real_timestamp_column(connector):
    """
    Regression test: create()/create_many()/upsert() used to stamp
    auto_now/auto_now_add fields with a Python str (strftime'd), but the
    migrator creates them as a real TIMESTAMP column (regardless of the
    model declaring them `str`, the conventional way to declare them in
    this ORM) — asyncpg's TIMESTAMP codec rejects a plain string, so every
    create() on a model with auto_now/auto_now_add was broken. Only
    surfaced under a large-dataset smoke test; no prior test actually
    created a row with these fields against a live database.
    """
    from postgres_connector.registry import registry
    registry.register(PremiumUser)
    await connector.migrate()
    try:
        user = await connector.create(PremiumUser, {"name": "Raj"})
        assert user.created_at is not None
        # updated_at (auto_now) is only stamped on UPDATE, not on CREATE.

        updated = await connector.update_by_id(PremiumUser, user.id, {"name": "Rajesh"})
        assert updated.updated_at is not None

        many = await connector.create_many(PremiumUser, [{"name": "A"}, {"name": "B"}])
        assert all(u.created_at is not None for u in many)
    finally:
        await connector.raw_execute("DROP TABLE premium_users")


@pytest.mark.asyncio
async def test_create_round_trips_json_field_as_dict(connector):
    """
    Regression test: connector.create()'s RETURNING-row path must hand back
    a real dict for JSONField columns, not a JSON string. This is the path
    that previously double-encoded JSON (QueryBuilder.build_insert() called
    json.dumps() on top of the connection-level jsonb codec, which also
    calls json.dumps() — storing '{"notes": "..."}' as a JSON *string*
    scalar instead of a JSON object).
    """
    class JsonRoundTripModel(PostgreSQLModel):
        __table__ = "json_round_trip_model"
        id:       int  = Field(primary_key=True)
        settings: dict = JSONField(default={})

    from postgres_connector.registry import registry
    registry.register(JsonRoundTripModel)
    await connector.migrate()

    user = await connector.create(JsonRoundTripModel, {
        "settings": {"theme": "dark", "notifications": True},
    })
    assert isinstance(user.settings, dict)
    assert user.settings == {"theme": "dark", "notifications": True}

    # Confirm at the database level too: the stored value must be a JSON
    # object, not a JSON string scalar containing escaped JSON text.
    rows = await connector.raw(
        'SELECT jsonb_typeof(settings) AS jtype FROM json_round_trip_model WHERE id = $1',
        [user.id],
    )
    assert rows[0]["jtype"] == "object"

    await connector.raw_execute('DROP TABLE json_round_trip_model')
