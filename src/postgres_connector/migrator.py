"""
ORM Migrations Engine — handles schema discovery and synchronization for PostgreSQL.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Type, Dict, TYPE_CHECKING

from .types import ColumnInfo, FieldDefinition
from .registry import registry

if TYPE_CHECKING:
    from .pool import ConnectionPool
    from .model import PostgreSQLModel

logger = logging.getLogger(__name__)


# Map Python type → PostgreSQL type string
TYPE_MAP = {
    int:   "INTEGER",
    str:   "VARCHAR(255)",
    bool:  "BOOLEAN",
    float: "DOUBLE PRECISION",
    dict:  "JSONB",
    list:  "JSONB",
}


class Migrator:
    """
    Orchestrates schema inspection and synchronization for PostgreSQL.
    Policy: additive-only (never drops columns).
    """

    def __init__(self, pool: "ConnectionPool"):
        self._pool = pool

    async def migrate(self) -> None:
        """
        Synchronize all registered models with the database.
        1. Creates missing tables.
        2. Adds missing columns to existing tables.
        """
        db_name = self._pool._config.get("database")
        if not db_name:
            logger.warning("No database name in config; skipping migrations.")
            return

        current_schema = await self._get_db_schema(db_name)

        for model in registry.all().values():
            table_name = model.__table__
            if table_name not in current_schema:
                await self._create_table(model)
            else:
                await self._sync_columns(model, current_schema[table_name])

    async def _get_db_schema(self, db_name: str) -> Dict[str, Dict[str, ColumnInfo]]:
        """
        Fetch all columns for all tables in the current database (public schema).
        Returns: { 'table_name': { 'column_name': ColumnInfo } }
        """
        sql = """
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_catalog = $1
            ORDER BY table_name, ordinal_position
        """
        rows = await self._pool.fetch_all(sql, [db_name])

        schema: Dict[str, Dict[str, ColumnInfo]] = {}
        for row in rows:
            table = row["table_name"]
            col   = row["column_name"]

            if table not in schema:
                schema[table] = {}

            schema[table][col] = ColumnInfo(
                name                     = col,
                data_type                = row["data_type"].upper(),
                is_nullable              = row["is_nullable"].upper() == "YES",
                column_default           = row["column_default"],
                character_maximum_length = row["character_maximum_length"],
            )
        return schema

    async def _create_table(self, model: Type["PostgreSQLModel"]) -> None:
        """Generate and execute a CREATE TABLE statement."""
        table_name = model.__table__
        logger.info("Migrator: Creating table '%s'", table_name)

        pks = [f.name for f in model._fields.values() if f.primary_key]
        has_composite_pk = len(pks) > 1

        column_defs = []
        for field in model._fields.values():
            # If composite PK, don't add "PRIMARY KEY" to individual columns
            col_sql = self._build_column_sql(field, skip_primary_key=has_composite_pk)
            column_defs.append(col_sql)

        if has_composite_pk:
            pk_list = ", ".join([f'"{pk}"' for pk in pks])
            column_defs.append(f"PRIMARY KEY ({pk_list})")

        sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
        await self._pool.execute(sql, [])

    async def _sync_columns(
        self,
        model:   Type["PostgreSQLModel"],
        db_cols: Dict[str, ColumnInfo],
    ) -> None:
        """Find columns in the model that are missing in the DB and ADD them."""
        table_name = model.__table__

        for col_name, field in model._fields.items():
            if col_name not in db_cols:
                logger.info("Migrator: Adding column '%s' to table '%s'", col_name, table_name)
                col_sql = self._build_column_sql(field)
                sql = f'ALTER TABLE "{table_name}" ADD COLUMN {col_sql}'
                await self._pool.execute(sql, [])

    def _build_column_sql(self, field: FieldDefinition, skip_primary_key: bool = False) -> str:
        """Convert a FieldDefinition to a SQL column definition string."""

        # Primary key with auto-increment → SERIAL PRIMARY KEY
        if field.primary_key and field.auto_increment and not skip_primary_key:
            return f'"{field.name}" SERIAL PRIMARY KEY'

        pg_type = TYPE_MAP.get(field.python_type, "VARCHAR(255)")

        if field.is_json:
            pg_type = "JSONB"
        elif field.python_type == str and field.max_length:
            pg_type = f"VARCHAR({field.max_length})"
        elif field.auto_now or field.auto_now_add:
            pg_type = "TIMESTAMP"

        parts = [f'"{field.name}"', pg_type]

        # PRIMARY KEY implies NOT NULL
        if field.primary_key and not skip_primary_key:
            parts.append("PRIMARY KEY")
        elif field.primary_key and skip_primary_key:
            parts.append("NOT NULL")
        elif not field.nullable:
            parts.append("NOT NULL")

        if field.unique and not field.primary_key:
            parts.append("UNIQUE")

        # Defaults (skip for JSONB — PostgreSQL doesn't accept JSON literal defaults easily)
        if field.is_json:
            pass
        elif field.auto_now_add:
            parts.append("DEFAULT CURRENT_TIMESTAMP")
        elif field.auto_now:
            # PostgreSQL has no ON UPDATE equivalent; default timestamp only
            parts.append("DEFAULT CURRENT_TIMESTAMP")
        elif field.has_default and field.default is not None:
            if isinstance(field.default, str):
                parts.append(f"DEFAULT '{field.default}'")
            elif isinstance(field.default, bool):
                parts.append(f"DEFAULT {'TRUE' if field.default else 'FALSE'}")
            else:
                parts.append(f"DEFAULT {field.default}")

        return " ".join(parts)
