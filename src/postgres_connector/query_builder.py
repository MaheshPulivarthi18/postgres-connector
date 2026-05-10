"""
QueryBuilder — converts LoopBack-style filter DSL to parameterized PostgreSQL SQL.

Differences from MySQL version:
  - Identifiers quoted with double-quotes instead of backticks
  - Parameters use $1, $2, $3 ... notation (asyncpg style)
  - ILIKE operator is native (case-insensitive LIKE)
  - REGEXP → ~ operator

Supported where operators:
    eq, neq, gt, gte, lt, lte, like, nlike, ilike, inq, nin, between, regexp

Supports:
    - Nested AND / OR clauses
    - SELECT with field projection
    - ORDER BY (multiple, ASC/DESC)
    - LIMIT / OFFSET (skip)
    - INSERT with RETURNING, UPDATE, DELETE, COUNT
"""

from __future__ import annotations

from typing import Any, Optional, Dict, List, Tuple

from .types import FieldDefinition, FilterOptions
from .exceptions import QueryError

# LoopBack operator → SQL operator
_OPERATORS: Dict[str, str] = {
    "eq":      "=",
    "neq":     "!=",
    "gt":      ">",
    "gte":     ">=",
    "lt":      "<",
    "lte":     "<=",
    "like":    "LIKE",
    "nlike":   "NOT LIKE",
    "ilike":   "ILIKE",
    "inq":     "IN",
    "nin":     "NOT IN",
    "between": "BETWEEN",
    "regexp":  "~",
    "contains": "@>",
}


class QueryBuilder:
    """
    Builds parameterized PostgreSQL SQL for a single table.

    Create one per query (not shared / not thread-safe during build).

    Example:
        qb  = QueryBuilder("users", user_fields)
        sql, params = qb.build_select(FilterOptions(
            where={"active": True, "age": {"gte": 18}},
            order=["name ASC"],
            limit=10,
        ))
        # sql    → 'SELECT "users".* FROM "users" WHERE "users"."active" = $1
        #            AND "users"."age" >= $2 ORDER BY "users"."name" ASC LIMIT 10'
        # params → [True, 18]
    """

    def __init__(self, table: str, fields: Dict[str, FieldDefinition]):
        self.table  = table
        self.fields = fields
        self._params: List[Any] = []
        self._param_idx: int = 0

    # ── Parameter helper ──────────────────────────────────────────────────

    def _p(self, value: Any) -> str:
        """Add a parameter and return its numbered placeholder ($N)."""
        self._params.append(value)
        self._param_idx += 1
        return f"${self._param_idx}"

    def _placeholders(self, values: List[Any]) -> str:
        """Return comma-separated $N placeholders for a list of values."""
        return ", ".join(self._p(v) for v in values)

    def _validate_field(self, field: str) -> None:
        """Ensure the field exists on the model to prevent SQL injection via keys."""
        if field not in self.fields and field not in ("and", "or"):
            raise QueryError(f"Field '{field}' does not exist on model '{self.table}'")

    # ── Public builders ───────────────────────────────────────────────────

    def build_select(self, opts: FilterOptions) -> Tuple[str, List[Any]]:
        self._params = []
        self._param_idx = 0
        parts = [
            self._select_clause(opts.fields),
            self._where_clause(opts.where),
            self._order_clause(opts.order),
            self._limit_clause(opts.limit),
            self._offset_clause(opts.skip),
        ]
        return self._join(parts), list(self._params)

    def build_select_by_fk(
        self,
        fk_field:   str,
        fk_values:  List[Any],
        opts:       Optional[FilterOptions] = None,
    ) -> Tuple[str, List[Any]]:
        """
        SELECT * FROM "table" WHERE "fk_field" IN ($1, $2, ...)
        Used by RelationResolver for batch loading.
        """
        self._params = []
        self._param_idx = 0
        self._validate_field(fk_field)
        sql_select = self._select_clause(opts.fields if opts else None)
        sql_where  = f'"{self.table}"."{fk_field}" = ANY({self._p(fk_values)})'
        where_parts = [sql_where]

        if opts and opts.where:
            extra = self._build_where_dict(opts.where)
            if extra:
                where_parts.append(f"({extra})")

        where = "WHERE " + " AND ".join(where_parts)

        parts = [
            sql_select,
            where,
            self._order_clause(opts.order if opts else None),
            self._limit_clause(opts.limit if opts else None),
        ]
        return self._join(parts), list(self._params)

    def build_select_by_pk(
        self,
        pk_field:  str,
        pk_values: List[Any],
        opts:      Optional[FilterOptions] = None,
    ) -> Tuple[str, List[Any]]:
        """
        SELECT * FROM "table" WHERE "pk_field" IN ($1, $2, ...)
        Used by RelationResolver for belongsTo batch loading.
        """
        self._params = []
        self._param_idx = 0
        self._validate_field(pk_field)
        sql_select = self._select_clause(opts.fields if opts else None)
        sql_where  = f'"{self.table}"."{pk_field}" = ANY({self._p(pk_values)})'
        where_parts = [sql_where]

        if opts and opts.where:
            extra = self._build_where_dict(opts.where)
            if extra:
                where_parts.append(f"({extra})")

        where = "WHERE " + " AND ".join(where_parts)
        parts = [sql_select, where]
        return self._join(parts), list(self._params)

    def build_insert(
        self,
        data:          Dict[str, Any],
        returning_pk:  Optional[str] = None,
        returning_all: bool = False,
    ) -> Tuple[str, List[Any]]:
        self._params = []
        self._param_idx = 0
        import json

        serialized_data = {}
        for k, v in data.items():
            self._validate_field(k)
            field_def = self.fields.get(k)
            if field_def and field_def.is_json and not isinstance(v, (str, bytes)) and v is not None:
                serialized_data[k] = json.dumps(v)
            else:
                serialized_data[k] = v

        cols  = ", ".join(f'"{k}"' for k in serialized_data)
        marks = self._placeholders(list(serialized_data.values()))
        sql   = f'INSERT INTO "{self.table}" ({cols}) VALUES ({marks})'

        if returning_all:
            sql += ' RETURNING *'
        elif returning_pk:
            sql += f' RETURNING "{returning_pk}"'

        return sql, list(self._params)

    def build_update(
        self,
        where: Dict[str, Any],
        data:  Dict[str, Any],
    ) -> Tuple[str, List[Any]]:
        if not data:
            raise QueryError("UPDATE requires at least one field to set.")
        if not where:
            raise QueryError("UPDATE without a WHERE clause is not allowed. Pass at least {pk: value}.")

        self._params = []
        self._param_idx = 0
        import json

        set_parts = []
        for k, v in data.items():
            self._validate_field(k)
            field_def = self.fields.get(k)
            if field_def and field_def.is_json and not isinstance(v, (str, bytes)) and v is not None:
                v = json.dumps(v)
            set_parts.append(f'"{k}" = {self._p(v)}')

        set_clause = ", ".join(set_parts)
        where_sql  = self._build_where_dict(where)
        sql = f'UPDATE "{self.table}" SET {set_clause} WHERE {where_sql}'
        return sql, list(self._params)

    def build_delete(self, where: Dict[str, Any]) -> Tuple[str, List[Any]]:
        if not where:
            raise QueryError("DELETE without a WHERE clause is not allowed.")
        self._params = []
        self._param_idx = 0
        where_sql = self._build_where_dict(where)
        sql       = f'DELETE FROM "{self.table}" WHERE {where_sql}'
        return sql, list(self._params)

    def build_count(self, where: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        self._params = []
        self._param_idx = 0
        sql = f'SELECT COUNT(*) AS _count FROM "{self.table}"'
        if where:
            where_sql = self._build_where_dict(where)
            if where_sql:
                sql += f" WHERE {where_sql}"
        return sql, list(self._params)

    # ── Clause builders ───────────────────────────────────────────────────

    def _select_clause(self, fields: Optional[List[str]]) -> str:
        if fields:
            for f in fields:
                self._validate_field(f)
            cols = ", ".join(f'"{self.table}"."{f}"' for f in fields)
        else:
            cols = f'"{self.table}".*'
        return f'SELECT {cols} FROM "{self.table}"'

    def _where_clause(self, where: Optional[Dict[str, Any]]) -> str:
        if not where:
            return ""
        sql = self._build_where_dict(where)
        return f"WHERE {sql}" if sql else ""

    def _build_where_dict(self, where: Dict[str, Any]) -> str:
        parts: List[str] = []

        for key, value in where.items():
            if key == "and":
                if not isinstance(value, list):
                    raise QueryError("'and' clause must be a list of conditions.")
                sub = " AND ".join(
                    f"({self._build_where_dict(clause)})" for clause in value
                )
                parts.append(f"({sub})")

            elif key == "or":
                if not isinstance(value, list):
                    raise QueryError("'or' clause must be a list of conditions.")
                sub = " OR ".join(
                    f"({self._build_where_dict(clause)})" for clause in value
                )
                parts.append(f"({sub})")

            elif isinstance(value, dict):
                self._validate_field(key)
                # Operator-based: {"age": {"gt": 18, "lte": 65}}
                for op, op_val in value.items():
                    parts.append(self._operator_clause(key, op, op_val))

            elif value is None:
                self._validate_field(key)
                parts.append(f'"{self.table}"."{key}" IS NULL')

            else:
                self._validate_field(key)
                # Simple equality
                parts.append(f'"{self.table}"."{key}" = {self._p(value)}')

        return " AND ".join(parts)

    def _operator_clause(self, field: str, op: str, value: Any) -> str:
        if op not in _OPERATORS:
            raise QueryError(
                f"Unknown operator '{op}'. "
                f"Supported: {sorted(_OPERATORS.keys())}"
            )
        sql_op = _OPERATORS[op]

        if op in ("inq", "nin"):
            if not isinstance(value, (list, tuple, set)):
                raise QueryError(f"Operator '{op}' requires a list/tuple value.")
            values = list(value)
            if not values:
                return "1=0" if op == "inq" else "1=1"
            placeholders = self._placeholders(values)
            return f'"{self.table}"."{field}" {sql_op} ({placeholders})'

        elif op == "between":
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise QueryError("'between' requires exactly [low, high].")
            return f'"{self.table}"."{field}" BETWEEN {self._p(value[0])} AND {self._p(value[1])}'

        elif value is None and op == "eq":
            return f'"{self.table}"."{field}" IS NULL'

        elif value is None and op == "neq":
            return f'"{self.table}"."{field}" IS NOT NULL'

        else:
            return f'"{self.table}"."{field}" {sql_op} {self._p(value)}'

    def _order_clause(self, order: Optional[List[str]]) -> str:
        if not order:
            return ""
        parts = []
        for item in order:
            tokens = item.strip().split()
            if not tokens:
                continue
            field     = tokens[0]
            self._validate_field(field)
            direction = tokens[1].upper() if len(tokens) > 1 else "ASC"
            if direction not in ("ASC", "DESC"):
                raise QueryError(
                    f"Invalid ORDER direction '{direction}'. Use ASC or DESC."
                )
            parts.append(f'"{self.table}"."{field}" {direction}')
        return f"ORDER BY {', '.join(parts)}" if parts else ""

    def _limit_clause(self, limit: Optional[int]) -> str:
        if limit is None:
            return ""
        if limit < 0:
            raise QueryError("LIMIT must be a non-negative integer.")
        return f"LIMIT {int(limit)}"

    def _offset_clause(self, skip: Optional[int]) -> str:
        if skip is None or skip == 0:
            return ""
        if skip < 0:
            raise QueryError("OFFSET (skip) must be a non-negative integer.")
        return f"OFFSET {int(skip)}"

    # ── Utilities ─────────────────────────────────────────────────────────

    @staticmethod
    def _join(parts: List[str]) -> str:
        return " ".join(p for p in parts if p)
