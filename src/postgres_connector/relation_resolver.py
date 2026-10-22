"""
RelationResolver — loads nested relations with zero N+1 queries.

Strategy:
  For each relation at a given depth:
    1. Collect all parent PKs / FKs into a set
    2. Issue ONE batch query  WHERE fk IN ($1, $2, ...)
    3. Build model instances from the rows
    4. Recurse into sub-includes on those instances
    5. Stitch results back onto parent instances

All sibling relations at the same depth run concurrently via asyncio.gather —
except inside a transaction (`session` given), where every query must run on
that one connection, and asyncpg allows only one operation per connection at
a time, so siblings run sequentially instead.

Supported relation types:
  - hasMany          → parent.pk  → child.fk
  - hasOne           → parent.pk  → child.fk  (returns first match)
  - belongsTo        → child.fk   → parent.pk
  - hasManyThrough   → junction table with two FK columns
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, Type, Dict, List, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .pool import ConnectionPool
    from .model import PostgreSQLModel

from .types import (
    FilterOptions, IncludeScope, IncludeItem,
    RelationDefinition, RelationType,
)
from .exceptions import RelationNotDefinedError, QueryError
from .query_builder import QueryBuilder

logger = logging.getLogger(__name__)


# ── Include normalizer ────────────────────────────────────────────────────────

def _parse_include(item: IncludeItem) -> IncludeScope:
    """Normalise a raw include item into an IncludeScope object."""
    if isinstance(item, IncludeScope):
        return item

    if isinstance(item, str):
        # Dot-notation: "orders.items.product"
        head, _, tail = item.partition(".")
        if tail:
            inner_scope = FilterOptions(include=[tail])
            return IncludeScope(relation=head, scope=inner_scope)
        return IncludeScope(relation=head)

    if isinstance(item, dict):
        relation  = item.get("relation", "")
        scope_raw = item.get("scope")
        scope     = _dict_to_filter(scope_raw) if scope_raw else None
        return IncludeScope(relation=relation, scope=scope)

    raise ValueError(f"Unsupported include item type: {type(item)!r} — {item!r}")


def _merge_includes(scopes: List[IncludeScope]) -> List[IncludeScope]:
    """
    Combine include items that target the same relation into one, so it's
    loaded once with all requested sub-includes — e.g. "posts.comments" and
    "posts.tags" both name `posts`, and resolving them separately would load
    `posts` twice, the second load overwriting the first (and its comments).

    Sub-includes are concatenated. Other scope options (where, order, limit,
    ...) can't be combined meaningfully, so at most one of the merged items
    may set them; two different ones raise QueryError.
    """
    merged: Dict[str, IncludeScope] = {}
    for scope in scopes:
        existing = merged.get(scope.relation)
        if existing is None:
            merged[scope.relation] = scope
            continue

        a = existing.scope or FilterOptions()
        b = scope.scope or FilterOptions()
        options = {}
        for name in ("where", "order", "limit", "skip", "fields"):
            va, vb = getattr(a, name), getattr(b, name)
            if va is not None and vb is not None and va != vb:
                raise QueryError(
                    f"Conflicting '{name}' scopes for included relation '{scope.relation}'. "
                    f"Include it once with a single scope instead."
                )
            options[name] = va if va is not None else vb

        include = list(a.include or []) + list(b.include or [])
        merged[scope.relation] = IncludeScope(
            relation=scope.relation,
            scope=FilterOptions(include=include or None, **options),
        )
    return list(merged.values())


_FILTER_KEYS = ("where", "order", "limit", "skip", "fields", "include")


def _norm_int(name: str, value: Any) -> Optional[int]:
    """Coerce a `limit`/`skip` that arrived as an integer string, or refuse it.

    JSON from API clients sends query parameters as strings ("10"), which used
    to reach the query builder and crash as a raw TypeError in SQL text. The
    value is now an int before it leaves the filter parse, and anything that is
    not a non-negative integer is a QueryError the caller can map to HTTP 400.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str):
            try:
                value = int(value.strip())
            except ValueError:
                raise QueryError(
                    f"'{name}' must be a non-negative integer, got {value!r}."
                ) from None
        else:
            raise QueryError(
                f"'{name}' must be a non-negative integer, got {value!r}."
            )
    if value < 0:
        raise QueryError(
            f"'{name}' must be a non-negative integer, got {value!r}."
        )
    return int(value)


def _norm_fields(fields: Any) -> Union[List[str], Dict[str, bool], None]:
    """Normalise the LoopBack `Fields` shapes, as loopback-datasource-juggler's fieldsToArray does.

    - a list selects those fields; a string selects one field
    - `{name: bool}` with any true value selects only the true keys
    - `{name: false, ...}` with only false values selects every field except
      those; it stays a dict, since only the query builder knows the columns
    - `{}` selects every field
    """
    if fields is None or isinstance(fields, (list, tuple)):
        return fields
    if isinstance(fields, str):
        return [fields]
    if isinstance(fields, dict):
        if not fields:
            return None
        selected = [str(name) for name, include in fields.items() if include]
        if selected:
            return selected
        return {str(name): False for name in fields}
    raise QueryError(
        f"'fields' must be a list of names, a name, or an object of name: bool, "
        f"got {type(fields).__name__}."
    )


def _dict_to_filter(d: Dict) -> FilterOptions:
    unknown = sorted(set(d) - set(_FILTER_KEYS))
    if unknown:
        raise QueryError(f"Unknown filter key(s) {unknown}. Supported: {list(_FILTER_KEYS)}")
    order = d.get("order")
    if isinstance(order, str):
        # LoopBack clients send a single "name DESC" string as one entry;
        # iterating it character by character produced an error about a field
        # called 'n'.
        order = [order]
    return FilterOptions(
        where   = d.get("where"),
        order   = order,
        limit   = _norm_int("limit", d.get("limit")),
        skip    = _norm_int("skip", d.get("skip")),
        fields  = _norm_fields(d.get("fields")),
        include = d.get("include"),
    )


# ── Resolver ──────────────────────────────────────────────────────────────────

class RelationResolver:
    """
    Resolves nested relation includes for lists of model instances.
    Mutates instances in-place (calls instance._set_relation(...)).
    """

    def __init__(self, pool: "ConnectionPool"):
        self._pool = pool

    # ── Public entry point ────────────────────────────────────────────────

    async def resolve(
        self,
        instances:   List["PostgreSQLModel"],
        include:     List[IncludeItem],
        model_class: Type["PostgreSQLModel"],
        session:     Optional[Any] = None,
    ) -> None:
        """
        Resolve all top-level includes concurrently, then recurse.
        Safe to call with an empty list.

        `session` binds every relation query to that connection/transaction
        (so relations see the transaction's own uncommitted writes, and a
        transaction never needs a second pooled connection).
        """
        if not instances or not include:
            return

        scopes = _merge_includes([_parse_include(item) for item in include])

        if session is not None:
            for scope in scopes:
                await self._resolve_one(instances, scope, model_class, session)
            return

        results = await asyncio.gather(*(
            self._resolve_one(instances, scope, model_class)
            for scope in scopes
        ), return_exceptions=True)

        for result in results:
            if isinstance(result, BaseException):
                raise result

    # ── Per-relation dispatch ─────────────────────────────────────────────

    async def _resolve_one(
        self,
        parents:      List["PostgreSQLModel"],
        scope:        IncludeScope,
        parent_model: Type["PostgreSQLModel"],
        session:      Optional[Any] = None,
    ) -> None:
        from .registry import registry

        rel_name = scope.relation
        if rel_name not in parent_model._relations:
            raise RelationNotDefinedError(rel_name, parent_model.__name__)

        rel_def: RelationDefinition = parent_model._relations[rel_name]
        target_cls = registry.get(rel_def.target_model)

        # Ensure scope exists
        if not scope.scope:
            scope.scope = FilterOptions()

        if rel_def.relation_type == RelationType.HAS_MANY:
            await self._has_many(parents, scope, rel_def, target_cls, session)

        elif rel_def.relation_type == RelationType.HAS_ONE:
            await self._has_one(parents, scope, rel_def, target_cls, session)

        elif rel_def.relation_type == RelationType.BELONGS_TO:
            await self._belongs_to(parents, scope, rel_def, target_cls, session)

        elif rel_def.relation_type == RelationType.HAS_MANY_THROUGH:
            await self._has_many_through(parents, scope, rel_def, target_cls, session)

    # ── hasMany ───────────────────────────────────────────────────────────

    async def _has_many(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
        session:    Optional[Any] = None,
    ) -> None:
        """parent.pk → child.foreign_key  (1-to-many)"""
        parent_pk  = self._pk(parents[0].__class__)
        parent_ids = self._collect_pk_vals(parents, parent_pk)

        if not parent_ids:
            for p in parents:
                p._set_relation(rel_def.name, [])
            return

        try:
            rows = await self._query_by_fk(
                target_cls.__table__, rel_def.foreign_key, parent_ids,
                scope.scope, target_cls._fields, session,
            )
            children = [target_cls._from_row(r) for r in rows]

            if scope.scope and scope.scope.include:
                await self.resolve(children, scope.scope.include, target_cls, session)

            grouped: dict = {pid: [] for pid in parent_ids}
            for child in children:
                fk_val = child.__dict__.get(rel_def.foreign_key)
                if fk_val in grouped:
                    grouped[fk_val].append(child)

            for parent in parents:
                pk_val = parent.__dict__.get(parent_pk)
                parent._set_relation(rel_def.name, grouped.get(pk_val, []))
        except Exception:
            for p in parents:
                p._set_relation(rel_def.name, [])
            raise

    # ── hasOne ────────────────────────────────────────────────────────────

    async def _has_one(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
        session:    Optional[Any] = None,
    ) -> None:
        """parent.pk → child.foreign_key  (1-to-1, returns first child)"""
        parent_pk  = self._pk(parents[0].__class__)
        parent_ids = self._collect_pk_vals(parents, parent_pk)

        if not parent_ids:
            for p in parents:
                p._set_relation(rel_def.name, None)
            return

        try:
            rows = await self._query_by_fk(
                target_cls.__table__, rel_def.foreign_key, parent_ids,
                scope.scope, target_cls._fields, session,
            )
            children = [target_cls._from_row(r) for r in rows]

            if scope.scope and scope.scope.include:
                await self.resolve(children, scope.scope.include, target_cls, session)

            grouped: dict = {}
            for child in children:
                fk_val = child.__dict__.get(rel_def.foreign_key)
                if fk_val not in grouped:
                    grouped[fk_val] = child

            for parent in parents:
                pk_val = parent.__dict__.get(parent_pk)
                parent._set_relation(rel_def.name, grouped.get(pk_val))
        except Exception:
            for p in parents:
                p._set_relation(rel_def.name, None)
            raise

    # ── belongsTo ────────────────────────────────────────────────────────

    async def _belongs_to(
        self,
        children:   List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
        session:    Optional[Any] = None,
    ) -> None:
        """child.foreign_key → parent.pk  (many-to-1)"""
        target_pk = self._pk(target_cls)

        fk_vals = list({
            c.__dict__.get(rel_def.foreign_key)
            for c in children
            if c.__dict__.get(rel_def.foreign_key) is not None
        })

        if not fk_vals:
            for c in children:
                c._set_relation(rel_def.name, None)
            return

        try:
            rows = await self._query_by_pk(
                target_cls.__table__, target_pk, fk_vals,
                scope.scope, target_cls._fields, session,
            )
            parents = [target_cls._from_row(r) for r in rows]

            if scope.scope and scope.scope.include:
                await self.resolve(parents, scope.scope.include, target_cls, session)

            by_pk = {p.__dict__.get(target_pk): p for p in parents}

            for child in children:
                fk_val = child.__dict__.get(rel_def.foreign_key)
                child._set_relation(rel_def.name, by_pk.get(fk_val))
        except Exception:
            for c in children:
                c._set_relation(rel_def.name, None)
            raise

    # ── hasManyThrough ────────────────────────────────────────────────────

    async def _has_many_through(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
        session:    Optional[Any] = None,
    ) -> None:
        """
        parent.pk → junction.foreign_key + junction.other_key → target.pk
        """
        from .registry import registry

        parent_pk  = self._pk(parents[0].__class__)
        parent_ids = self._collect_pk_vals(parents, parent_pk)

        if not parent_ids:
            for p in parents:
                p._set_relation(rel_def.name, [])
            return

        through_cls = registry.get(rel_def.through_model)

        try:
            # Step 1: Query junction table
            junction_rows = await self._query_by_fk(
                through_cls.__table__, rel_def.foreign_key, parent_ids,
                None, through_cls._fields, session,
            )

            if not junction_rows:
                for p in parents:
                    p._set_relation(rel_def.name, [])
                return

            # Step 2: Collect unique target IDs
            target_ids = list({
                row[rel_def.other_key]
                for row in junction_rows
                if row.get(rel_def.other_key) is not None
            })

            if not target_ids:
                for p in parents:
                    p._set_relation(rel_def.name, [])
                return

            # Step 3: Query target table
            target_pk = self._pk(target_cls)
            target_rows = await self._query_by_pk(
                target_cls.__table__, target_pk, target_ids,
                scope.scope, target_cls._fields, session,
            )
            targets = [target_cls._from_row(r) for r in target_rows]

            if scope.scope and scope.scope.include:
                await self.resolve(targets, scope.scope.include, target_cls, session)

            # Step 4: Stitch via junction table, keeping the target query's
            # order (scope "order"), then apply scope skip/limit per parent.
            target_rank  = {t.__dict__.get(target_pk): i for i, t in enumerate(targets)}
            target_by_pk = {t.__dict__.get(target_pk): t for t in targets}
            parent_to_targets: dict = {pid: [] for pid in parent_ids}

            for j_row in junction_rows:
                parent_fk  = j_row.get(rel_def.foreign_key)
                target_fk  = j_row.get(rel_def.other_key)
                target_obj = target_by_pk.get(target_fk)
                if target_obj and parent_fk in parent_to_targets:
                    parent_to_targets[parent_fk].append(target_obj)

            opts = scope.scope
            skip = (opts.skip if opts else None) or 0
            limit = opts.limit if opts else None
            for pid, items in parent_to_targets.items():
                if opts and opts.order:
                    items.sort(key=lambda t: target_rank[t.__dict__.get(target_pk)])
                parent_to_targets[pid] = items[skip:None if limit is None else skip + limit]

            for parent in parents:
                pk_val = parent.__dict__.get(parent_pk)
                parent._set_relation(rel_def.name, parent_to_targets.get(pk_val, []))
        except Exception:
            for p in parents:
                p._set_relation(rel_def.name, [])
            raise

    # ── Low-level query helpers ───────────────────────────────────────────

    async def _query_by_fk(
        self,
        table:     str,
        fk_field:  str,
        fk_vals:   List[Any],
        opts:      Optional[FilterOptions],
        fields:    Dict,
        session:   Optional[Any] = None,
    ) -> List[Dict]:
        qb = QueryBuilder(table, fields)
        sql, params = qb.build_select_by_fk(fk_field, fk_vals, opts)
        return await self._fetch(sql, params, session)

    async def _query_by_pk(
        self,
        table:    str,
        pk_field: str,
        pk_vals:  List[Any],
        opts:     Optional[FilterOptions],
        fields:   Dict,
        session:  Optional[Any] = None,
    ) -> List[Dict]:
        qb = QueryBuilder(table, fields)
        sql, params = qb.build_select_by_pk(pk_field, pk_vals, opts)
        return await self._fetch(sql, params, session)

    async def _fetch(self, sql: str, params: List[Any], session: Optional[Any] = None) -> List[Dict]:
        if session is not None:
            self._pool.log_query(sql, params)
            rows = await session.fetch(sql, *params)
            return [dict(r) for r in rows]
        return await self._pool.fetch_all(sql, params)

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _pk(model_cls: Type["PostgreSQLModel"]) -> str:
        pk = model_cls._get_primary_key()
        if not pk:
            raise QueryError(
                f"Model '{model_cls.__name__}' has no primary key defined."
            )
        return pk

    @staticmethod
    def _collect_pk_vals(instances: List["PostgreSQLModel"], pk: str) -> List[Any]:
        seen = set()
        result = []
        for inst in instances:
            val = inst.__dict__.get(pk)
            if val is not None and val not in seen:
                seen.add(val)
                result.append(val)
        return result
