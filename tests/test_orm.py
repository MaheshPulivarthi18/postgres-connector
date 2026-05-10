"""
Comprehensive unit tests for the PostgreSQL ORM connector.
Tests model, query builder, types, fields, registry — all without a live DB.
"""

import unittest
import sys
import os
import json

# Add the src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from postgres_connector.fields import Field, Relation, JSONField
from postgres_connector.types import (
    FieldDefinition, RelationDefinition, RelationType,
    FilterOptions, IncludeScope, ColumnInfo,
)
from postgres_connector.query_builder import QueryBuilder
from postgres_connector.exceptions import (
    QueryError, ConfigurationError, RelationNotLoadedError,
    ModelNotRegisteredError,
)
from postgres_connector.registry import registry


class TestPostgreSQLORM(unittest.TestCase):

    def setUp(self):
        """Clear global registry before each test."""
        self._original_models = dict(registry._models)
        registry._models = {}

    def tearDown(self):
        """Restore global registry."""
        registry._models = self._original_models

    def _make_fields(self, *names):
        return {
            name: FieldDefinition(name=name, python_type=str)
            for name in names
        }

    # ── Model Tests ──────────────────────────────────────────────────────────

    def test_model_registers_fields(self):
        from postgres_connector.model import PostgreSQLModel

        class TestUser(PostgreSQLModel):
            __table__ = "test_users"
            id: int = Field(primary_key=True)
            name: str = Field(max_length=100)
            email: str = Field(unique=True)
            active: bool = Field(default=True)

        self.assertIn("id", TestUser._fields)
        self.assertIn("name", TestUser._fields)
        self.assertIn("email", TestUser._fields)
        self.assertIn("active", TestUser._fields)

    def test_primary_key_detection(self):
        from postgres_connector.model import PostgreSQLModel

        class TestPK(PostgreSQLModel):
            __table__ = "test_pk"
            id: int = Field(primary_key=True)
            name: str = Field()

        self.assertEqual(TestPK._get_primary_key(), "id")
        self.assertTrue(TestPK._fields["id"].primary_key)
        self.assertTrue(TestPK._fields["id"].auto_increment)

    # ── Query Builder Tests ──────────────────────────────────────────────────

    def test_simple_select(self):
        fields = self._make_fields("id", "name", "email")
        qb = QueryBuilder("users", fields)
        sql, params = qb.build_select(FilterOptions())
        self.assertIn('"users".*', sql)
        self.assertEqual(params, [])

    def test_select_with_where(self):
        fields = self._make_fields("id", "name", "active")
        qb = QueryBuilder("users", fields)
        sql, params = qb.build_select(FilterOptions(where={"active": True}))
        self.assertIn('"users"."active" = $1', sql)
        self.assertEqual(params, [True])

    def test_contains_operator(self):
        fields = self._make_fields("id", "metadata")
        qb = QueryBuilder("users", fields)
        sql, params = qb.build_select(FilterOptions(where={"metadata": {"contains": {"key": "val"}}}))
        self.assertIn("@>", sql)

    # ── Registry Tests ───────────────────────────────────────────────────────

    def test_auto_registration(self):
        from postgres_connector.model import PostgreSQLModel
        class TestAutoReg(PostgreSQLModel):
            __table__ = "test_auto_reg"
            id: int = Field(primary_key=True)

        self.assertTrue(registry.is_registered("TestAutoReg"))
        self.assertEqual(registry.get("TestAutoReg"), TestAutoReg)

    # ── Migrator Types ───────────────────────────────────────────────────────

    def test_expected_type_resolution(self):
        from postgres_connector.migrator import Migrator
        m = Migrator(pool=None)

        f_str = FieldDefinition(name="x", python_type=str)
        self.assertEqual(m._get_expected_type(f_str), "VARCHAR(255)")

        f_int = FieldDefinition(name="x", python_type=int)
        self.assertEqual(m._get_expected_type(f_int), "INTEGER")

        f_json = FieldDefinition(name="x", python_type=dict, is_json=True)
        self.assertEqual(m._get_expected_type(f_json), "JSONB")

if __name__ == "__main__":
    unittest.main()
