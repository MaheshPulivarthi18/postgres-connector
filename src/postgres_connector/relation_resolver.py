"""
RelationResolver — loads nested relations with zero N+1 queries.

Strategy:
  For each relation at a given depth:
    1. Collect all parent PKs / FKs into a set
    2. Issue ONE batch query  WHERE fk IN ($1, $2, ...)
    3. Build model instances from the rows
    4. Recurse into sub-includes on those instances
    5. Stitch results back onto parent instances

All sibling relations at the same depth run concurrently via asyncio.gather.

Supported relation types:
  - hasMany          → parent.pk  → child.fk
  - hasOne           → parent.pk  → child.fk  (returns first match)
  - belongsTo        → child.fk   → parent.pk
  - hasManyThrough   → junction table with two FK columns
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, Type, Dict, List, TYPE_CHECKING

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


def _dict_to_filter(d: Dict) -> FilterOptions:
    return FilterOptions(
        where   = d.get("where"),
        order   = d.get("order"),
        limit   = d.get("limit"),
        skip    = d.get("skip"),
        fields  = d.get("fields"),
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
    ) -> None:
        """
        Resolve all top-level includes concurrently, then recurse.
        Safe to call with an empty list.
        """
        if not instances or not include:
            return

        scopes = [_parse_include(item) for item in include]

        await asyncio.gather(*(
            self._resolve_one(instances, scope, model_class)
            for scope in scopes
        ))

    # ── Per-relation dispatch ─────────────────────────────────────────────

    async def _resolve_one(
        self,
        parents:      List["PostgreSQLModel"],
        scope:        IncludeScope,
        parent_model: Type["PostgreSQLModel"],
    ) -> None:
        from .registry import registry

        rel_name = scope.relation
        if rel_name not in parent_model._relations:
            raise RelationNotDefinedError(rel_name, parent_model.__name__)

        rel_def: RelationDefinition = parent_model._relations[rel_name]
        target_cls = registry.get(rel_def.target_model)

        if rel_def.relation_type == RelationType.HAS_MANY:
            await self._has_many(parents, scope, rel_def, target_cls)

        elif rel_def.relation_type == RelationType.HAS_ONE:
            await self._has_one(parents, scope, rel_def, target_cls)

        elif rel_def.relation_type == RelationType.BELONGS_TO:
            await self._belongs_to(parents, scope, rel_def, target_cls)

        elif rel_def.relation_type == RelationType.HAS_MANY_THROUGH:
            await self._has_many_through(parents, scope, rel_def, target_cls)

    # ── hasMany ───────────────────────────────────────────────────────────

    async def _has_many(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
    ) -> None:
        """parent.pk → child.foreign_key  (1-to-many)"""
        parent_pk  = self._pk(parents[0].__class__)
        parent_ids = self._collect_pk_vals(parents, parent_pk)

        if not parent_ids:
            for p in parents:
                p._set_relation(rel_def.name, [])
            return

        rows = await self._query_by_fk(
            target_cls.__table__, rel_def.foreign_key, parent_ids,
            scope.scope, target_cls._fields,
        )
        children = [target_cls._from_row(r) for r in rows]

        if scope.scope and scope.scope.include:
            await self.resolve(children, scope.scope.include, target_cls)

        grouped: dict = {pid: [] for pid in parent_ids}
        for child in children:
            fk_val = child.__dict__.get(rel_def.foreign_key)
            if fk_val in grouped:
                grouped[fk_val].append(child)

        for parent in parents:
            pk_val = parent.__dict__.get(parent_pk)
            parent._set_relation(rel_def.name, grouped.get(pk_val, []))

    # ── hasOne ────────────────────────────────────────────────────────────

    async def _has_one(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
    ) -> None:
        """parent.pk → child.foreign_key  (1-to-1, returns first child)"""
        parent_pk  = self._pk(parents[0].__class__)
        parent_ids = self._collect_pk_vals(parents, parent_pk)

        if not parent_ids:
            for p in parents:
                p._set_relation(rel_def.name, None)
            return

        rows = await self._query_by_fk(
            target_cls.__table__, rel_def.foreign_key, parent_ids,
            scope.scope, target_cls._fields,
        )
        children = [target_cls._from_row(r) for r in rows]

        if scope.scope and scope.scope.include:
            await self.resolve(children, scope.scope.include, target_cls)

        grouped: dict = {}
        for child in children:
            fk_val = child.__dict__.get(rel_def.foreign_key)
            if fk_val not in grouped:
                grouped[fk_val] = child

        for parent in parents:
            pk_val = parent.__dict__.get(parent_pk)
            parent._set_relation(rel_def.name, grouped.get(pk_val))

    # ── belongsTo ────────────────────────────────────────────────────────

    async def _belongs_to(
        self,
        children:   List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
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

        rows = await self._query_by_pk(
            target_cls.__table__, target_pk, fk_vals,
            scope.scope, target_cls._fields,
        )
        parents = [target_cls._from_row(r) for r in rows]

        if scope.scope and scope.scope.include:
            await self.resolve(parents, scope.scope.include, target_cls)

        by_pk = {p.__dict__.get(target_pk): p for p in parents}

        for child in children:
            fk_val = child.__dict__.get(rel_def.foreign_key)
            child._set_relation(rel_def.name, by_pk.get(fk_val))

    # ── hasManyThrough ────────────────────────────────────────────────────

    async def _has_many_through(
        self,
        parents:    List["PostgreSQLModel"],
        scope:      IncludeScope,
        rel_def:    RelationDefinition,
        target_cls: Type["PostgreSQLModel"],
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

        # Step 1: Query junction table
        junction_rows = await self._query_by_fk(
            through_cls.__table__, rel_def.foreign_key, parent_ids,
            None, through_cls._fields,
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
            scope.scope, target_cls._fields,
        )
        targets = [target_cls._from_row(r) for r in target_rows]

        if scope.scope and scope.scope.include:
            await self.resolve(targets, scope.scope.include, target_cls)

        # Step 4: Stitch via junction table
        target_by_pk = {t.__dict__.get(target_pk): t for t in targets}
        parent_to_targets: dict = {pid: [] for pid in parent_ids}

        for j_row in junction_rows:
            parent_fk  = j_row.get(rel_def.foreign_key)
            target_fk  = j_row.get(rel_def.other_key)
            target_obj = target_by_pk.get(target_fk)
            if target_obj and parent_fk in parent_to_targets:
                parent_to_targets[parent_fk].append(target_obj)

        for parent in parents:
            pk_val = parent.__dict__.get(parent_pk)
            parent._set_relation(rel_def.name, parent_to_targets.get(pk_val, []))

    # ── Low-level query helpers ───────────────────────────────────────────

    async def _query_by_fk(
        self,
        table:     str,
        fk_field:  str,
        fk_vals:   List[Any],
        opts:      Optional[FilterOptions],
        fields:    Dict,
    ) -> List[Dict]:
        qb = QueryBuilder(table, fields)
        sql, params = qb.build_select_by_fk(fk_field, fk_vals, opts)
        return await self._fetch(sql, params)

    async def _query_by_pk(
        self,
        table:    str,
        pk_field: str,
        pk_vals:  List[Any],
        opts:     Optional[FilterOptions],
        fields:   Dict,
    ) -> List[Dict]:
        qb = QueryBuilder(table, fields)
        sql, params = qb.build_select_by_pk(pk_field, pk_vals, opts)
        return await self._fetch(sql, params)

    async def _fetch(self, sql: str, params: List[Any]) -> List[Dict]:
        self._pool.log_query(sql, params)
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
