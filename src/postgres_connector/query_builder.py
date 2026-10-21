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

# FilterOptions.lock value → trailing SQL clause
_LOCK_MODES: Dict[str, str] = {
    "update":             "FOR UPDATE",
    "update_nowait":      "FOR UPDATE NOWAIT",
    "update_skip_locked": "FOR UPDATE SKIP LOCKED",
    "share":              "FOR SHARE",
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
            self._lock_clause(opts.lock),
        ]
        return self._join(parts), list(self._params)

    def build_select_by_fk(
        self,
        fk_field:   str,
        fk_values:  List[Any],
        opts:       Optional[FilterOptions] = None,
    ) -> Tuple[str, List[Any]]:
        """
        SELECT * FROM "table" WHERE "fk_field" = ANY($1)
        Used by RelationResolver for batch loading.

        `limit`/`skip` in `opts` apply PER fk value (i.e. per parent), the
        way an include scope means them — "each user's 5 latest orders",
        not "5 orders across all users". That's done with a
        ROW_NUMBER() OVER (PARTITION BY fk) window, still in one query.
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
        order = self._order_clause(opts.order if opts else None)

        limit = opts.limit if opts else None
        skip  = (opts.skip if opts else None) or 0
        if limit is None and not skip:
            return self._join([sql_select, where, order]), list(self._params)

        if limit is not None and limit < 0:
            raise QueryError("LIMIT must be a non-negative integer.")
        if skip < 0:
            raise QueryError("OFFSET (skip) must be a non-negative integer.")

        # Number each parent's rows in the requested order (primary key as
        # the tiebreak, so pages are stable), then keep rows skip+1..skip+limit.
        order_terms = [order[len("ORDER BY "):]] if order else []
        pk = next((name for name, f in self.fields.items() if f.primary_key), None)
        if pk:
            order_terms.append(f'"{self.table}"."{pk}" ASC')
        window_order = f"ORDER BY {', '.join(order_terms)}" if order_terms else ""
        cols = self._select_columns(opts.fields if opts else None)
        numbered = (
            f'SELECT {cols}, ROW_NUMBER() OVER '
            f'(PARTITION BY "{self.table}"."{fk_field}" {window_order}) AS "_rn" '
            f'FROM "{self.table}" {where}'
        )
        bounds = [f'"{self.table}"."_rn" > {int(skip)}']
        if limit is not None:
            bounds.append(f'"{self.table}"."_rn" <= {int(skip) + int(limit)}')
        sql = (
            f'SELECT * FROM ({numbered}) AS "{self.table}" '
            f'WHERE {" AND ".join(bounds)} ORDER BY "{self.table}"."_rn"'
        )
        return sql, list(self._params)

    def build_select_by_pk(
        self,
        pk_field:  str,
        pk_values: List[Any],
        opts:      Optional[FilterOptions] = None,
    ) -> Tuple[str, List[Any]]:
        """
        SELECT * FROM "table" WHERE "pk_field" IN ($1, $2, ...)
        Used by RelationResolver for belongsTo / hasManyThrough batch loading.
        `order` is applied; `limit`/`skip` are not — they mean "per parent",
        which the resolver applies after stitching.
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
        parts = [sql_select, where, self._order_clause(opts.order if opts else None)]
        return self._join(parts), list(self._params)

    def build_insert(
        self,
        data:          Dict[str, Any],
        returning_pk:  Optional[str] = None,
        returning_all: bool = False,
    ) -> Tuple[str, List[Any]]:
        self._params = []
        self._param_idx = 0

        # NOTE: JSON/JSONB values are NOT pre-serialized here — the
        # connection-level type codec registered in pool.py's
        # _init_connection() already calls json.dumps() on the way to the
        # wire. Serializing here too double-encodes the value (it gets
        # stored as a JSON *string* scalar instead of a JSON object/array).
        for k in data:
            self._validate_field(k)
        serialized_data = dict(data)

        cols  = ", ".join(f'"{k}"' for k in serialized_data)
        marks = self._placeholders(list(serialized_data.values()))
        sql   = f'INSERT INTO "{self.table}" ({cols}) VALUES ({marks})'

        if returning_all:
            sql += ' RETURNING *'
        elif returning_pk:
            sql += f' RETURNING "{returning_pk}"'

        return sql, list(self._params)

    def build_upsert(
        self,
        data:            Dict[str, Any],
        conflict_target: List[str],
        update_fields:   Optional[List[str]] = None,
    ) -> Tuple[str, List[Any]]:
        """
        INSERT ... ON CONFLICT (conflict_target) DO UPDATE SET ... RETURNING *

        `conflict_target` must have a unique or primary-key constraint in
        the database (Postgres requires this to detect the conflict).

        `update_fields` defaults to every column in `data` except
        `conflict_target` itself. Pass `update_fields=[]` to make it
        `ON CONFLICT DO NOTHING` instead — in that case RETURNING yields no
        row when an existing row already satisfied the conflict.
        """
        if not data:
            raise QueryError("UPSERT requires at least one field to insert.")
        if not conflict_target:
            raise QueryError("UPSERT requires conflict_target (the unique/PK column(s) to detect a conflict on).")

        self._params = []
        self._param_idx = 0

        for k in data:
            self._validate_field(k)
        for k in conflict_target:
            self._validate_field(k)

        cols          = ", ".join(f'"{k}"' for k in data)
        marks         = self._placeholders(list(data.values()))
        conflict_cols = ", ".join(f'"{k}"' for k in conflict_target)
        sql = f'INSERT INTO "{self.table}" ({cols}) VALUES ({marks}) ON CONFLICT ({conflict_cols})'

        if update_fields is None:
            update_fields = [k for k in data if k not in conflict_target]
        for k in update_fields:
            self._validate_field(k)

        if not update_fields:
            sql += ' DO NOTHING'
        else:
            set_clause = ", ".join(f'"{k}" = EXCLUDED."{k}"' for k in update_fields)
            sql += f' DO UPDATE SET {set_clause}'

        sql += ' RETURNING *'
        return sql, list(self._params)

    def build_update(
        self,
        where:         Dict[str, Any],
        data:          Dict[str, Any],
        returning_all: bool = False,
    ) -> Tuple[str, List[Any]]:
        if not data:
            raise QueryError("UPDATE requires at least one field to set.")
        if not where:
            raise QueryError("UPDATE without a WHERE clause is not allowed. Pass at least {pk: value}.")

        self._params = []
        self._param_idx = 0

        # See build_insert(): JSON/JSONB values are left as native Python
        # objects — the connection-level type codec handles serialization.
        set_parts = []
        for k, v in data.items():
            self._validate_field(k)
            set_parts.append(f'"{k}" = {self._p(v)}')

        set_clause = ", ".join(set_parts)
        where_sql  = self._build_where_dict(where)
        if not where_sql:
            raise QueryError("UPDATE without a WHERE clause is not allowed. Pass at least {pk: value}.")
        sql = f'UPDATE "{self.table}" SET {set_clause} WHERE {where_sql}'
        if returning_all:
            sql += ' RETURNING *'
        return sql, list(self._params)

    def build_delete(self, where: Dict[str, Any]) -> Tuple[str, List[Any]]:
        if not where:
            raise QueryError("DELETE without a WHERE clause is not allowed.")
        self._params = []
        self._param_idx = 0
        where_sql = self._build_where_dict(where)
        if not where_sql:
            raise QueryError("DELETE without a WHERE clause is not allowed.")
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

    def build_exists(self, where: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        """
        SELECT EXISTS(SELECT 1 FROM "table" WHERE ...) AS "_exists"

        Cheaper than build_count() for a plain existence check — Postgres
        can stop at the first match instead of counting every row.
        """
        self._params = []
        self._param_idx = 0
        sql = f'SELECT EXISTS(SELECT 1 FROM "{self.table}"'
        if where:
            where_sql = self._build_where_dict(where)
            if where_sql:
                sql += f" WHERE {where_sql}"
        sql += ') AS "_exists"'
        return sql, list(self._params)

    # ── Clause builders ───────────────────────────────────────────────────

    def _select_columns(self, fields: Optional[List[str]]) -> str:
        if fields:
            for f in fields:
                self._validate_field(f)
            return ", ".join(f'"{self.table}"."{f}"' for f in fields)
        return f'"{self.table}".*'

    def _select_clause(self, fields: Optional[List[str]]) -> str:
        return f'SELECT {self._select_columns(fields)} FROM "{self.table}"'

    def _where_clause(self, where: Optional[Dict[str, Any]]) -> str:
        if not where:
            return ""
        sql = self._build_where_dict(where)
        return f"WHERE {sql}" if sql else ""

    def _build_where_dict(self, where: Dict[str, Any]) -> str:
        """
        Returns "" when `where` imposes no condition at all (e.g. `{}` or
        `{"and": []}`) — callers treat that as "no WHERE clause".
        """
        parts: List[str] = []

        for key, value in where.items():
            if key == "and":
                if not isinstance(value, list):
                    raise QueryError("'and' clause must be a list of conditions.")
                # Empty sub-clauses impose nothing, so they drop out; an
                # empty list is the empty conjunction (always true).
                subs = [s for s in (self._build_where_dict(clause) for clause in value) if s]
                if subs:
                    parts.append("(" + " AND ".join(f"({s})" for s in subs) + ")")

            elif key == "or":
                if not isinstance(value, list):
                    raise QueryError("'or' clause must be a list of conditions.")
                # An empty list is the empty disjunction (matches nothing,
                # like `inq: []`); an empty member makes the whole OR true.
                if not value:
                    parts.append("FALSE")
                    continue
                mark = (len(self._params), self._param_idx)
                subs = [self._build_where_dict(clause) for clause in value]
                if all(subs):
                    parts.append("(" + " OR ".join(f"({s})" for s in subs) + ")")
                else:
                    # Discarded members must not leave their params behind.
                    del self._params[mark[0]:]
                    self._param_idx = mark[1]

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

    def _lock_clause(self, lock: Optional[str]) -> str:
        if lock is None:
            return ""
        if lock not in _LOCK_MODES:
            raise QueryError(f"Unknown lock mode '{lock}'. Use one of: {sorted(_LOCK_MODES)}")
        return _LOCK_MODES[lock]

    # ── Utilities ─────────────────────────────────────────────────────────

    @staticmethod
    def _join(parts: List[str]) -> str:
        return " ".join(p for p in parts if p)
