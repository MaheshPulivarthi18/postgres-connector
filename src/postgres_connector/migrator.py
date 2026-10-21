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
    datetime: "TIMESTAMP",
}

def _quote_literal(value: str) -> str:
    """
    Quote a string as a SQL literal for DDL (DEFAULT clauses can't take bind
    parameters). Doubling single quotes is sufficient with
    standard_conforming_strings, which is on by default since PostgreSQL 9.1.
    """
    return "'" + value.replace("'", "''") + "'"


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

        for model in registry.all().values():
            up_sql, down_sql = await self._sync_unique_constraints(model)
            up_queries.extend(up_sql)
            down_queries.extend(down_sql)

        for model in registry.all().values():
            up_sql, down_sql = await self._sync_indexes(model)
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
                    await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", version)
                        
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
            await conn.execute("DELETE FROM schema_migrations WHERE version = $1", version)

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
                # New column — add it
                col_sql = self._build_column_sql(field)
                up_queries.append(f'ALTER TABLE "{table_name}" ADD COLUMN {col_sql}')
                down_queries.append(f'ALTER TABLE "{table_name}" DROP COLUMN "{col_name}"')
            else:
                # Existing column — detect alterations
                db_col = db_cols[col_name]
                alter_up, alter_down = self._detect_column_changes(table_name, field, db_col)
                up_queries.extend(alter_up)
                down_queries.extend(alter_down)
        return up_queries, down_queries

    def _detect_column_changes(self, table_name: str, field: FieldDefinition, db_col: ColumnInfo) -> Tuple[List[str], List[str]]:
        """Compare a model field against its current DB column and generate ALTER statements."""
        up_queries = []
        down_queries = []

        # Skip primary key / auto_increment (SERIAL) columns — altering these is too dangerous
        if field.primary_key or field.auto_increment:
            return up_queries, down_queries

        # --- Type change detection ---
        expected_type = self._get_expected_type(field)
        db_type = db_col.data_type.upper()

        if not self._types_match(expected_type, db_type, db_col):
            up_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" TYPE {expected_type}')
            # Restore original type
            original_type = db_type
            if db_type == "CHARACTER VARYING" and db_col.character_maximum_length:
                original_type = f"VARCHAR({db_col.character_maximum_length})"
            down_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" TYPE {original_type}')

        # --- Nullable change detection ---
        expected_nullable = field.nullable
        if expected_nullable != db_col.is_nullable:
            if not expected_nullable:
                up_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" SET NOT NULL')
                down_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" DROP NOT NULL')
            else:
                up_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" DROP NOT NULL')
                down_queries.append(f'ALTER TABLE "{table_name}" ALTER COLUMN "{field.name}" SET NOT NULL')

        # --- Unique constraint detection ---
        # We check via information_schema in _sync_unique_constraints (called separately)
        # but for simplicity, we handle it here using a naming convention
        model_wants_unique = field.unique and not field.primary_key
        # We'll check unique via the DB column_default pattern — PostgreSQL doesn't store UNI in ColumnInfo
        # So we do a separate pass below

        return up_queries, down_queries

    async def _sync_unique_constraints(self, model: Type[PostgreSQLModel]) -> Tuple[List[str], List[str]]:
        """Detect added/removed UNIQUE constraints (single and composite)."""
        table_name = model.__table__
        
        # 1. Build expected unique constraints
        expected_uniques: Dict[str, Tuple[str, ...]] = {}
        
        # Single column unique=True
        for col_name, field in model._fields.items():
            if field.unique and not field.primary_key:
                cname = f"uq_{table_name}_{col_name}"[:63]
                expected_uniques[cname] = (col_name,)
                
        # Composite unique from __unique__
        for cols in getattr(model, "__unique__", []):
            cols_tuple = tuple(cols)
            cname = f"uq_{table_name}_{'_'.join(cols_tuple)}"[:63]
            expected_uniques[cname] = cols_tuple

        # 2. Query existing unique constraints
        sql = """
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_name = $1
              AND tc.constraint_name LIKE 'uq_%'
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        rows = await self._pool.fetch_all(sql, [table_name])
        
        existing_uniques: Dict[str, List[str]] = {}
        for row in rows:
            cname = row["constraint_name"]
            if cname not in existing_uniques:
                existing_uniques[cname] = []
            existing_uniques[cname].append(row["column_name"])
            
        existing_uniques_final = {k: tuple(v) for k, v in existing_uniques.items()}

        up_queries = []
        down_queries = []

        # Add missing constraints
        for cname, cols in expected_uniques.items():
            if cname not in existing_uniques_final:
                cols_sql = ", ".join([f'"{c}"' for c in cols])
                up_queries.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{cname}" UNIQUE ({cols_sql})')
                down_queries.append(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{cname}"')

        # Drop removed constraints
        for cname, cols in existing_uniques_final.items():
            if cname not in expected_uniques:
                cols_sql = ", ".join([f'"{c}"' for c in cols])
                up_queries.append(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{cname}"')
                down_queries.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{cname}" UNIQUE ({cols_sql})')

        return up_queries, down_queries

    def _get_expected_type(self, field: FieldDefinition) -> str:
        if field.is_json:
            return "JSONB"
        if field.auto_now or field.auto_now_add:
            return "TIMESTAMP"
        if field.python_type == str and field.max_length:
            return f"VARCHAR({field.max_length})"
        return TYPE_MAP.get(field.python_type, "VARCHAR(255)")

    def _types_match(self, expected: str, db_type: str, db_col: ColumnInfo) -> bool:
        """Check if the expected type matches the DB type."""
        expected_upper = expected.upper().replace(" ", "")
        db_upper = db_type.upper().replace(" ", "")

        # Direct match (ignoring spaces)
        if expected_upper == db_upper:
            return True

        # VARCHAR vs CHARACTER VARYING
        if expected_upper.startswith("VARCHAR"):
            if db_upper == "CHARACTERVARYING" or db_upper == "VARCHAR":
                expected_len = expected.upper().replace("VARCHAR(", "").replace(")", "").strip()
                # If no length specified in expected, assume 255
                if "(" not in expected.upper(): expected_len = "255"
                
                db_len = str(db_col.character_maximum_length) if db_col.character_maximum_length else "255"
                return expected_len == db_len

        # INTEGER variants
        if expected_upper in ("INTEGER", "INT", "INT4"):
            return db_upper in ("INTEGER", "INT", "INT4")

        # BIGINT variants
        if expected_upper in ("BIGINT", "INT8"):
            return db_upper in ("BIGINT", "INT8")

        # DOUBLE PRECISION variants
        if expected_upper in ("DOUBLEPRECISION", "FLOAT8"):
            return db_upper in ("DOUBLEPRECISION", "FLOAT8")

        # BOOLEAN variants
        if expected_upper in ("BOOLEAN", "BOOL"):
            return db_upper in ("BOOLEAN", "BOOL")

        # TIMESTAMP variants
        if expected_upper == "TIMESTAMP":
            return db_upper in ("TIMESTAMP", "TIMESTAMPWITHOUTTIMEZONE")

        return False

    async def _sync_foreign_keys(self, model: Type[PostgreSQLModel]) -> Tuple[List[str], List[str]]:
        from .registry import registry
        from .types import RelationType
        
        table_name = model.__table__
        
        # 1. Query existing foreign keys
        sql = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = $1
              AND tc.constraint_name LIKE 'fk_%'
        """
        rows = await self._pool.fetch_all(sql, [table_name])
        existing_fks = {row["column_name"]: row for row in rows}
        
        # 2. Build expected foreign keys
        expected_fks = {}
        for rel_name, rel in model._relations.items():
            if rel.relation_type == RelationType.BELONGS_TO:
                target_cls = registry.get(rel.target_model)
                if not target_cls: continue
                expected_fks[rel.foreign_key] = {
                    "target_table": target_cls.__table__,
                    "target_pk": target_cls._get_primary_key()
                }

        up_queries = []
        down_queries = []
        
        # Create missing FKs
        for col_name, info in expected_fks.items():
            if col_name not in existing_fks:
                fk_name = f"fk_{table_name}_{col_name}"[:63]
                field_def = model._fields.get(col_name)
                on_delete = "CASCADE"
                if field_def and field_def.nullable:
                    on_delete = "SET NULL"

                up_queries.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ("{col_name}") REFERENCES "{info["target_table"]}" ("{info["target_pk"]}") ON DELETE {on_delete}')
                down_queries.append(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{fk_name}"')
                
        # Drop removed FKs
        for col_name, row in existing_fks.items():
            if col_name not in expected_fks:
                fk_name = row["constraint_name"]
                up_queries.append(f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{fk_name}"')
                    
        return up_queries, down_queries

    async def _sync_indexes(self, model: Type[PostgreSQLModel]) -> Tuple[List[str], List[str]]:
        table_name = model.__table__
        
        # Build expected indexes: single-column from index=True fields
        expected_indexes: dict[str, tuple] = {}
        for col_name, field in model._fields.items():
            if field.index:
                idx_name = f"idx_{table_name}_{col_name}"[:63]
                expected_indexes[idx_name] = (col_name,)
        
        # Add composite indexes from __indexes__
        for cols in model._composite_indexes:
            cols_tuple = tuple(cols)
            idx_name = f"idx_{table_name}_{'_'.join(cols_tuple)}"[:63]
            expected_indexes[idx_name] = cols_tuple
        
        # Query existing ORM-managed indexes
        sql = """
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE schemaname = 'public' AND tablename = $1 AND indexname LIKE 'idx_%%'
        """
        rows = await self._pool.fetch_all(sql, [table_name])
            
        existing_indexes = {row["indexname"]: row["indexdef"] for row in rows}
        
        up_queries = []
        down_queries = []
        
        # Create missing indexes
        for idx_name, cols in expected_indexes.items():
            if idx_name not in existing_indexes:
                cols_sql = ", ".join([f'"{c}"' for c in cols])
                up_queries.append(f'CREATE INDEX "{idx_name}" ON "{table_name}" ({cols_sql})')
                down_queries.append(f'DROP INDEX "{idx_name}"')
                
        # Drop removed indexes (use original indexdef for perfect rollback)
        for idx_name, indexdef in existing_indexes.items():
            if idx_name not in expected_indexes:
                up_queries.append(f'DROP INDEX "{idx_name}"')
                down_queries.append(indexdef)
                
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
            if isinstance(field.default, str): parts.append(f"DEFAULT {_quote_literal(field.default)}")
            elif isinstance(field.default, bool): parts.append(f"DEFAULT {'TRUE' if field.default else 'FALSE'}")
            else: parts.append(f"DEFAULT {field.default}")

        return " ".join(parts)
