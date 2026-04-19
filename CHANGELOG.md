# Changelog

## [1.0.0] - 2026-04-19

### Added
- Initial release of `oceanic-postgres-orm`
- `PostgreSQLConnector` with full CRUD API: `find`, `find_one`, `find_by_id`, `create`, `update_all`, `update_by_id`, `delete_all`, `delete_by_id`, `count`, `exists`
- `PostgreSQLModel` base class with metaclass auto-registration
- `Field` and `Relation` descriptors with full type inference
- `JSONField` for JSONB columns with automatic serialization/deserialization
- LoopBack-style filter DSL: `where`, `order`, `limit`, `skip`, `fields`, `include`
- All filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`, `nlike`, `ilike`, `inq`, `nin`, `between`, `regexp`
- Nested AND/OR where clauses
- Zero N+1 relation loading: `hasMany`, `hasOne`, `belongsTo`, `hasManyThrough`
- Dot-notation nested includes: `"orders.items.product"`
- Concurrent sibling relation resolution via `asyncio.gather`
- Additive-only schema migrations (`migrate()`)
- Soft delete support via `deleted_at` field
- Lifecycle hooks: `before_create`, `after_create`, `before_update`, `after_update`, `before_delete`
- Transaction support via `connector.transaction()` context manager
- Raw SQL escape hatch: `raw()`, `raw_execute()`
- Debug SQL logging with `echo=True`
- `to_dict()` / `to_json()` serialization with nested relations
- FastAPI-compatible `__json__()` hook
- Full Python 3.8+ compatibility
- Typed with `py.typed` marker (PEP 561)
- `asyncpg`-based connection pool with JSON/JSONB codec auto-registration
- PostgreSQL-native `$N` parameter placeholders
- Double-quoted identifier quoting
- `SERIAL PRIMARY KEY` for auto-increment columns
- `JSONB` storage for dict/list fields
