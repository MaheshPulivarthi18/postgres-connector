# Changelog

## [3.0.0] - 2026-10-22

FastAPI integration, LoopBack filter compatibility, and fixes from the 2.0.0 review. Major version because of the timestamp change below; see [Upgrading to 3.0](README.md#upgrading-to-30).

### Breaking
- `auto_now` / `auto_now_add` columns are `TIMESTAMPTZ` and stamped with an aware UTC `datetime` (`datetime.now(timezone.utc)`). They used to be `TIMESTAMP` stamped with naive `datetime.utcnow()`, while rows written outside the ORM got the server's local time, so one column could mix two time zones (#36, #55).
  - They now come back as **timezone-aware** datetimes. Comparing them with a naive datetime raises `TypeError`; use `datetime.now(timezone.utc)`.
  - The first `migrate()` after upgrading converts each existing column once (`USING col AT TIME ZONE 'UTC'`, keeping the stored instants). This rewrites the table and locks it while it runs.
  - No more `utcnow()` `DeprecationWarning`s on Python 3.12+.

### Added
- `Model.observe(event, fn)` for any number of hooks per event, and an `after_delete` hook (#28).
- `find_and_count()`: one page plus the total, run concurrently (#59).
- `postgres_connector.fastapi`: `register_exception_handlers(app)` maps library errors to HTTP statuses (#60), and `FilterParam(Model, max_limit=...)` reads a LoopBack-style `?filter=` query parameter (#61).
- `Model.pydantic_model(include=..., exclude=...)` for FastAPI `response_model`s and OpenAPI schemas (#62). Needs Pydantic 2, now required by the `fastapi` extra.
- `InvalidValueError` (bad values, SQLSTATE class 22) and `ReadOnlyTransactionError` instead of raw asyncpg errors (#60).
- Benchmarks against SQLAlchemy 2.x in `benchmarks/` (#29).

### Changed
- `create()`/`create_many()` raise `QueryError` for keys that aren't model fields; they used to drop them silently (#50).
- Hook methods from every mixin run, bases first, then the model's own. A model hook that calls `super()` to reach a mixin's hook now runs it twice; remove the `super()` call (#53).
- `migrate()` creates foreign-key constraints for `Field(foreign_key="table.column")`. On an existing database, rows that point at a missing parent must be fixed before migrating (#54).
- Models are iterable: `dict(instance) == instance.to_dict()`, so FastAPI routes can return them as-is (#58).
- LoopBack filter shapes: `order` as a string, `limit`/`skip` as numeric strings, `fields` as a string or `{name: bool}`. `{name: false}` selects every other field (#46, #64).

### Fixed
- `Field()`/`Relation()` options declared on plain mixin classes were lost (#27).
- Unsaved instances (primary key `None`) compared equal and collapsed in sets (#47).
- Mutable defaults such as `JSONField(default={})` were shared between instances (#49).
- `inq`/`nin` with `None` in the list matched nothing (#51).
- README examples failed on Python 3.8 (#52).
- `create_many()` failed above asyncpg's 32,767 query arguments; large batches are now split inside one transaction (#57).
- An include came back empty when `fields` left out the key columns (#56).

## [2.0.0] - 2026-10-21

A leaner API, focused on what a LoopBack-style FastAPI ORM needs, plus a round of correctness fixes.

### Removed
- `listen()`/`notify()`, stored procedures (`Procedure`, `call()`, `sync_procedures()`), `connector.transaction()`, `stream()`, the `lock` filter option, and `run_transaction()`/`TransactionError`. Use `begin()` for transactions, and `raw()`/`raw_execute()` for anything else.

### Changed
- Unknown filter keys now raise `QueryError` instead of being silently ignored.
- `update_by_id()`/`delete_by_id()` now fire `before_update`/`after_update`/`before_delete`, loading the row `FOR UPDATE` inside a transaction.
- Float fields accept `int`/`Decimal`; numeric fields reject `bool`.
- A `unique=True` column gets one managed `uq_<table>_<col>` constraint instead of two.

### Fixed
- `run_migrations()`/`rollback_migrations()` always failed; migration files now run as whole scripts, and `migrate()` is atomic.
- `include` inside `begin()` used a separate pooled connection (missed uncommitted rows, could deadlock a small pool).
- Includes sharing a prefix (`"posts.comments"`, `"posts.tags"`) lost a branch; include-scope `limit`/`skip` now apply per parent, and `hasManyThrough` honours `order`.
- Empty `and`/`or` lists generated invalid SQL; string column defaults weren't escaped.
- Model subclasses lost inherited fields; `from __future__ import annotations` and `X | None` resolved to `str`.
- `create()` mutated the caller's dict and skipped `auto_now`; `create_many()` inserted `NULL` instead of column defaults.

## [1.2.0] - 2026-09-20

### Added
- **Upsert**: `connector.upsert(model, data, conflict_target=[...], update_fields=None)` — and `tx.upsert(...)` inside a transaction — runs `INSERT ... ON CONFLICT (...) DO UPDATE SET ... RETURNING *` in one round trip, eliminating the race between a check-then-write. `update_fields` defaults to every field in `data` except `conflict_target`; pass `update_fields=[]` for `ON CONFLICT DO NOTHING` (returns `None` when it hits an existing row).
- **Fault tolerance**: `find()`/`find_one()`/`find_by_id()`/`count()`/`exists()` now automatically retry (exponential backoff, up to the new `max_query_retries` connector option, default 3, `0` disables) on transient connection failures — verified against a real Postgres instance by killing it mid-connection: a raw `ConnectionRefusedError`/`OSError` when establishing a new connection, and `asyncpg.exceptions.InterfaceError` when an already-open connection dies underneath a query, neither of which is an `asyncpg.PostgresError` with a sqlstate. Writes (`create`, `create_many`, `update_all`, `update_by_id`, `delete_all`, `delete_by_id`, `upsert`, `raw`, `raw_execute`) deliberately never auto-retry outside a transaction — retrying a write blindly risks double-executing it if the connection dropped after the server committed but before the client saw the acknowledgment.
- **Typed error mapping**: `create()`, `create_many()`, `update_by_id()`, `upsert()`, and every method going through `_execute()` (`update_all`, `delete_all`, `raw_execute`) now translate common Postgres constraint violations into typed, catchable exceptions instead of leaking raw `asyncpg` errors — `UniqueConstraintError` (23505), `ForeignKeyConstraintError` (23503), `NotNullConstraintError` (23502), `CheckConstraintError` (23514), all inheriting from a new `IntegrityError` base. Each carries `.constraint`/`.table`/`.column`/`.detail` from the underlying Postgres error and chains the original exception via `__cause__`. Lets a FastAPI app register `@app.exception_handler(UniqueConstraintError)`-style handlers once at the app level instead of every route catching `asyncpg` internals.

  **This changes what gets raised for constraint violations** — code that was catching raw `asyncpg.exceptions.UniqueViolationError` (etc.) directly needs to catch `UniqueConstraintError` (etc.) instead, or the common `IntegrityError`/`ConnectorError` base. The original `asyncpg` exception is still available via `exc.__cause__`.

- **Timeouts**: `connect_timeout`/`command_timeout` are now exposed on `PostgreSQLConnector(...)` (default 10s / 30s respectively; `command_timeout=None` disables it), and `raw()`/`raw_execute()` (and their `tx.` forms) accept a per-call `timeout=` override. Exceeding a timeout raises `QueryTimeoutError` instead of a bare `TimeoutError`.

### Fixed
- **`command_timeout` was silently the only timeout that existed, and wasn't configurable**: `ConnectionPool`'s `connect_timeout` parameter was named and documented as a connection-establishment timeout, but was actually wired to asyncpg's `command_timeout` — the default timeout applied to *every query* — and wasn't even exposed on `PostgreSQLConnector` at all, so every query has had an undocumented, fixed, unconfigurable 10-second cap since the transaction-API work landed. Now split into two correctly-named, correctly-wired, and configurable parameters.
- **Retry-storm risk on query timeouts**: `_RETRYABLE_CONNECTION_ERRORS` used the broad `OSError` to catch connection failures like `ConnectionRefusedError`, but `TimeoutError` is *also* an `OSError` subclass — so a query that simply ran too long was being treated as a retryable connection failure and retried up to `max_query_retries` times, each attempt eating the same timeout again, instead of backing off. Narrowed to `ConnectionError` (the precise builtin base for `ConnectionRefusedError`/`ConnectionResetError`/`ConnectionAbortedError`/`BrokenPipeError`, verified via MRO — `TimeoutError` is a sibling under `OSError`, not a subclass of `ConnectionError`), so a timeout now fails immediately as `QueryTimeoutError` on the first attempt. Real connection-failure retries (the original fault-tolerance feature) are unaffected — reverified against the same real-database-restart test after the change.
- **`create()`/`create_many()`/`upsert()` were broken for every model with `auto_now`/`auto_now_add`**: these fields are conventionally declared as `str` in a model but the migrator creates them as a real `TIMESTAMP` column — the write path was stamping them with a `strftime`'d Python string, which asyncpg's `TIMESTAMP` codec rejects outright (`expected a datetime.date or datetime.datetime instance, got 'str'`). No prior test actually created a row against a live database with these fields, so this was live and broken. Fixed by stamping a native `datetime.utcnow()` instead; `_validate_field_types()` now skips `auto_now`/`auto_now_add` fields rather than rejecting the framework's own (correct) stamp against the model's `str` annotation. Found via a large-dataset (27,000-row) smoke test — see `examples/full_feature_smoketest.py`.
- **`QueryTimeoutError` translation silently didn't fire on Python 3.8-3.10**: `asyncio.TimeoutError` and the builtin `TimeoutError` were only unified into the same class in Python 3.11 — before that, `asyncio.TimeoutError` is a distinct `concurrent.futures.TimeoutError` subclass, so `except TimeoutError` alone missed it and a raw `asyncio.exceptions.TimeoutError` leaked out of `raw()`/`raw_execute()` instead. Caught in CI on Python 3.10 (this project's local dev/testing had only run on 3.13, where the classes are unified, so it passed there). Fixed by catching `asyncio.TimeoutError` explicitly alongside the builtin; reproduced the exact CI failure on a local Python 3.10 interpreter first, then verified the fix across 3.9/3.10/3.11/3.13.

### Removed
- **Soft delete**: `find`/`find_one`/`find_by_id`/`count`/`exists` no longer auto-filter a `deleted_at` column, `delete_by_id`/`delete_all` no longer special-case it either (both always run a real `DELETE` now — the `hard` parameter they briefly had is gone, since there's no longer a default behavior for it to override), and relation includes no longer filter soft-deleted related rows. Recognizing "deletedness" by a magic column name and rewriting query behavior around it across the whole read/write path is application policy, not something a query library should have an opinion on — and it's little enough code (a plain column + your own `update_by_id`/`where` filtering) that it isn't worth a library feature, let alone a shipped mixin. See the "Deletes" section in the README for the two-line recipe if you want this in your own app.

## [1.1.0] - 2026-09-20

### Added
- `connector.begin()` — async context manager yielding a `TransactionContext` whose `find`/`create`/`update_by_id`/`delete_by_id`/`raw`/... calls are implicitly bound to one connection/transaction (no more passing `session=conn` to every call, and no way to accidentally run a write outside the transaction).
- Nested `async with tx.begin():` — PostgreSQL SAVEPOINTs, so a nested block can roll back independently without aborting the outer transaction.
- `isolation` / `readonly` / `deferrable` parameters on `connector.begin()` and the underlying `ConnectionPool.transaction()`.
- `TransactionContext` exported from the package root.

### Performance
- `exists()` now issues `SELECT EXISTS(SELECT 1 FROM ... WHERE ...)` instead of a full `COUNT(*)` — Postgres can stop at the first match instead of scanning/counting every row on a non-selective `where`.
- `update_by_id()` now does its `UPDATE` and re-fetch in a single round trip via `UPDATE ... RETURNING *` (previously an `UPDATE` followed by a separate `SELECT`).

### Fixed
- **JSON double-encoding**: `QueryBuilder.build_insert()`/`build_update()` and `connector.create_many()` used to call `json.dumps()` on `JSONField` values before binding them as query parameters. The connection-level type codec (`pool.py`'s `_init_connection`) also serializes Python objects bound to `jsonb`/`json` columns, so the value was encoded twice — stored as a JSON *string* scalar (e.g. `'{"notes": "hi"}'`) instead of a JSON object, silently turning `some_model.some_json_field["key"]` into a `TypeError`. JSON/JSONB values are now passed through as native Python objects and serialized exactly once, by the codec.
- **`QueryBuilder` field-validation regression**: the SQL-injection guard added in a previous release rejected the empty-`fields`-dict pattern one of the unit tests relied on, so it had been failing since that guard landed. Fixed the test to construct `QueryBuilder` with real field definitions, matching how every other caller (and the guard itself) expects it to be used.

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
- Transaction support
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
