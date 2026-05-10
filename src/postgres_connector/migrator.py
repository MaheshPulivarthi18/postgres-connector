"""
ORM Migrations Engine — handles schema discovery and synchronization for PostgreSQL.
Policy: additive-only (never drops columns).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional, Type, Dict, List, Tuple, TYPE_CHECKING

from .types import ColumnInfo, FieldDefinition
from .registry import registry

if TYPE_CHECKING:
    from .pool import ConnectionPool
    from .model import PostgreSQLModel

logger = logging.getLogger(__name__)

TYPE_MAP = {
    int:      "INTEGER",
    str:      "VARCHAR(255)",
    bool:     "BOOLEAN",
    float:    "DOUBLE PRECISION",
    dict:     "JSONB",
    list:     "JSONB",
}

class Migrator:
    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    async def _get_queries(self, db_name: str) -> Tuple[List[str], List[str]]:
        current_schema = await self._get_db_schema(db_name)
        up_queries = []
        down_queries = []

        for model in registry.all().values():
            table_name = model.__table__
            if table_name not in current_schema:
                up_sql, down_sql = self._create_table(model)
                up_queries.extend(up_sql)
                down_queries.extend(down_sql)
            else:
                up_sql, down_sql = self._sync_columns(model, current_schema[table_name])
                up_queries.extend(up_sql)
                down_queries.extend(down_sql)

        for model in registry.all().values():
            up_sql, down_sql = await self._sync_foreign_keys(model)
            up_queries.extend(up_sql)
            down_queries.extend(down_sql)
            
        down_queries.reverse()
        return up_queries, down_queries

    async def migrate(self) -> None:
        db_name = self._pool._config.get("database")
        if not db_name:
            return

        up_queries, _ = await self._get_queries(db_name)
        for q in up_queries:
            await self._pool.execute(q, [])

    async def generate_migrations(self, directory: str) -> Optional[str]:
        db_name = self._pool._config.get("database")
        if not db_name:
            return None
            
        up_queries, down_queries = await self._get_queries(db_name)
        if not up_queries:
            logger.info("No schema changes detected.")
            return None
            
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        version = f"{timestamp}_auto"
        up_filename = f"{version}_up.sql"
        down_filename = f"{version}_down.sql"
        up_filepath = os.path.join(directory, up_filename)
        down_filepath = os.path.join(directory, down_filename)
        
        with open(up_filepath, "w") as f:
            for q in up_queries:
                f.write(f"{q};\n")
                
        with open(down_filepath, "w") as f:
            for q in down_queries:
                f.write(f"{q};\n")
                
        logger.info(f"Generated migration files: {up_filepath} and {down_filepath}")
        return up_filepath

    async def run_migrations(self, directory: str) -> int:
        if not os.path.exists(directory):
            return 0
            
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR(255) PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        await self._pool.execute(create_table_sql, [])
            
        rows = await self._pool.fetch_all("SELECT version FROM schema_migrations", [])
        applied = {row["version"] for row in rows}
            
        files = sorted([f for f in os.listdir(directory) if f.endswith("_up.sql")])
        applied_count = 0
        
        for f in files:
            version = f.replace("_up.sql", "")
            if version not in applied:
                filepath = os.path.join(directory, f)
                with open(filepath, "r") as file:
                    sql_content = file.read()
                    
                statements = [s.strip() for s in sql_content.split(';') if s.strip()]
                
                async with self._pool.transaction() as conn:
                    for stmt in statements:
                        self._pool.log_query(stmt)
                        await conn.execute(stmt)
                    await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", [version])
                        
                logger.info(f"Applied migration: {version}")
                applied_count += 1
                
        return applied_count

    async def rollback_migrations(self, directory: str) -> int:
        if not os.path.exists(directory):
            return 0

        try:
            rows = await self._pool.fetch_all("SELECT version FROM schema_migrations ORDER BY applied_at DESC LIMIT 1", [])
            row = rows[0] if rows else None
        except Exception:
            row = None

        if not row:
            logger.info("No migrations to rollback.")
            return 0

        version = row["version"]
        down_file = f"{version}_down.sql"
        filepath = os.path.join(directory, down_file)

        if not os.path.exists(filepath):
            logger.error(f"Cannot rollback: {down_file} not found.")
            return 0

        with open(filepath, "r") as file:
            sql_content = file.read()

        statements = [s.strip() for s in sql_content.split(';') if s.strip()]

        async with self._pool.transaction() as conn:
            for stmt in statements:
                self._pool.log_query(stmt)
                await conn.execute(stmt)
            await conn.execute("DELETE FROM schema_migrations WHERE version = $1", [version])

        logger.info(f"Rolled back migration: {version}")
        return 1

    async def _get_db_schema(self, db_name: str) -> Dict[str, Dict[str, ColumnInfo]]:
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
            if table not in schema: schema[table] = {}
            schema[table][col] = ColumnInfo(
                name=col, data_type=row["data_type"].upper(),
                is_nullable=row["is_nullable"].upper() == "YES",
                column_default=row["column_default"],
                character_maximum_length=row["character_maximum_length"],
            )
        return schema

    def _create_table(self, model: Type[PostgreSQLModel]) -> Tuple[List[str], List[str]]:
        table_name = model.__table__
        pks = [f.name for f in model._fields.values() if f.primary_key]
        has_composite_pk = len(pks) > 1

        column_defs = []
        for field in model._fields.values():
            col_sql = self._build_column_sql(field, skip_primary_key=has_composite_pk)
            column_defs.append(col_sql)

        if has_composite_pk:
            pk_list = ", ".join([f'"{pk}"' for pk in pks])
            column_defs.append(f"PRIMARY KEY ({pk_list})")

        up_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
        down_sql = f'DROP TABLE IF EXISTS "{table_name}"'
        return [up_sql], [down_sql]

    def _sync_columns(self, model: Type[PostgreSQLModel], db_cols: Dict[str, ColumnInfo]) -> Tuple[List[str], List[str]]:
        table_name = model.__table__
        up_queries = []
        down_queries = []
        for col_name, field in model._fields.items():
            if col_name not in db_cols:
                col_sql = self._build_column_sql(field)
                up_queries.append(f'ALTER TABLE "{table_name}" ADD COLUMN {col_sql}')
                down_queries.append(f'ALTER TABLE "{table_name}" DROP COLUMN "{col_name}"')
        return up_queries, down_queries

    async def _sync_foreign_keys(self, model: Type[PostgreSQLModel]) -> Tuple[List[str], List[str]]:
        from .registry import registry
        from .types import RelationType
        
        table_name = model.__table__
        
        sql = """
            SELECT
                kcu.column_name,
                ccu.table_name AS referenced_table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = $1
        """
        rows = await self._pool.fetch_all(sql, [table_name])
        existing_fks = {row["column_name"]: row for row in rows}
        up_queries = []
        down_queries = []
        
        for rel_name, rel in model._relations.items():
            if rel.relation_type == RelationType.BELONGS_TO:
                col_name = rel.foreign_key
                target_cls = registry.get(rel.target_model)
                if not target_cls: continue
                
                target_table = target_cls.__table__
                target_pk = target_cls._get_primary_key()
                
                if col_name not in existing_fks:
                    fk_name = f"fk_{table_name}_{col_name}"
                    if len(fk_name) > 63: fk_name = fk_name[:63]
                    
                    field_def = model._fields.get(col_name)
                    on_delete = "CASCADE"
                    if field_def and field_def.nullable:
                        on_delete = "SET NULL"

                    up_queries.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ("{col_name}") REFERENCES "{target_table}" ("{target_pk}") ON DELETE {on_delete}')
                    down_queries.append(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{fk_name}"')
                    
        return up_queries, down_queries

    def _build_column_sql(self, field: FieldDefinition, skip_primary_key: bool = False) -> str:
        if field.primary_key and field.auto_increment and not skip_primary_key:
            return f'"{field.name}" SERIAL PRIMARY KEY'

        pg_type = TYPE_MAP.get(field.python_type, "VARCHAR(255)")

        if field.is_json: pg_type = "JSONB"
        elif field.python_type == str and field.max_length: pg_type = f"VARCHAR({field.max_length})"
        elif field.auto_now or field.auto_now_add: pg_type = "TIMESTAMP"

        parts = [f'"{field.name}"', pg_type]

        if field.primary_key and not skip_primary_key: parts.append("PRIMARY KEY")
        elif field.primary_key and skip_primary_key: parts.append("NOT NULL")
        elif not field.nullable: parts.append("NOT NULL")

        if field.unique and not field.primary_key: parts.append("UNIQUE")

        if field.is_json: pass
        elif field.auto_now_add: parts.append("DEFAULT CURRENT_TIMESTAMP")
        elif field.auto_now: parts.append("DEFAULT CURRENT_TIMESTAMP")
        elif field.has_default and field.default is not None:
            if isinstance(field.default, str): parts.append(f"DEFAULT '{field.default}'")
            elif isinstance(field.default, bool): parts.append(f"DEFAULT {'TRUE' if field.default else 'FALSE'}")
            else: parts.append(f"DEFAULT {field.default}")

        return " ".join(parts)
