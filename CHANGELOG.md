# Changelog

## [1.2.0] - 2026-09-20

### Added
- **Stored procedures/functions**: `Procedure(name, params=[...], returns=Model, kind="function"|"procedure")` declares a Postgres function or procedure, auto-registering it (like models do). `connector.call(proc_or_name, **kwargs)` — and `tx.call(...)` inside a transaction — invokes it: `SELECT * FROM name(...)` hydrated into model instances when `returns` is set, `SELECT name(...)` for a scalar, or `CALL name(...)` for `kind="procedure"`. Missing/unexpected keyword arguments raise `QueryError` before anything reaches the database. New `ProcedureNotRegisteredError` exception; `Procedure` and `procedures` (the registry) exported from the package root.
- **Procedure registration**: `Procedure(..., body="CREATE OR REPLACE FUNCTION/PROCEDURE ...")` accepts your exact SQL (executed verbatim — nothing parsed or generated), and `connector.sync_procedures()` / `tx.sync_procedures()` creates/replaces every registered Procedure that declared one. Deliberately separate from `migrate()` (schema vs. executable code) and, like `migrate()`, never drops anything. `body` stays optional — a `Procedure` with none is still just a calling-only binding for something that already exists in the database, exactly as before.
- **Row locking**: `lock` in the filter DSL (`find`/`find_one`/`find_by_id`) adds `FOR UPDATE`, `FOR UPDATE NOWAIT`, `FOR UPDATE SKIP LOCKED`, or `FOR SHARE` to the query — the standard building block for job-queue "claim the next row" patterns. Raises `QueryError` if used without `session=` (i.e. outside `begin()`/`run_transaction()`), since a lock taken outside an explicit transaction is released the instant the query finishes and has no effect.
- **Upsert**: `connector.upsert(model, data, conflict_target=[...], update_fields=None)` — and `tx.upsert(...)` inside a transaction — runs `INSERT ... ON CONFLICT (...) DO UPDATE SET ... RETURNING *` in one round trip, eliminating the race between a check-then-write. `update_fields` defaults to every field in `data` except `conflict_target`; pass `update_fields=[]` for `ON CONFLICT DO NOTHING` (returns `None` when it hits an existing row).
- **Pub/Sub**: `connector.listen(channel, callback)` wraps Postgres `LISTEN` on a dedicated connection (held outside the pool, since LISTEN state is connection-specific) and returns an async `stop()`; `callback` may be sync or async. `connector.notify(channel, payload)` / `tx.notify(...)` wrap `NOTIFY` — the `tx.notify(...)` form is transactional, delivered only once that transaction commits and dropped on rollback. Any listener left running is automatically cleaned up by `disconnect()`.
- **Streaming**: `connector.stream(model, filter, batch_size=100)` / `tx.stream(...)` reads results through a server-side cursor instead of materializing the whole set in memory like `find()` does — for exports, backfills, and batch jobs over large tables. Manages its own transaction (cursors require one) when `session=` isn't given, and releases the connection cleanly whether the loop finishes or is stopped early with `break`. `include` isn't supported (relation batching defeats streaming).
- **Fault tolerance**: `find()`/`find_one()`/`find_by_id()`/`count()`/`exists()` now automatically retry (exponential backoff, up to the new `max_query_retries` connector option, default 3, `0` disables) on transient connection failures — verified against a real Postgres instance by killing it mid-connection: a raw `ConnectionRefusedError`/`OSError` when establishing a new connection, and `asyncpg.exceptions.InterfaceError` when an already-open connection dies underneath a query, neither of which is an `asyncpg.PostgresError` with a sqlstate. Writes (`create`, `create_many`, `update_all`, `update_by_id`, `delete_all`, `delete_by_id`, `upsert`, `raw`, `raw_execute`) deliberately never auto-retry outside a transaction — retrying a write blindly risks double-executing it if the connection dropped after the server committed but before the client saw the acknowledgment. `run_transaction()`'s existing retry loop now also treats these same connection errors as retryable, alongside the existing serialization-failure/deadlock sqlstates.
- **Typed error mapping**: `create()`, `create_many()`, `update_by_id()`, `upsert()`, and every method going through `_execute()` (`update_all`, `delete_all`, `raw_execute`) now translate common Postgres constraint violations into typed, catchable exceptions instead of leaking raw `asyncpg` errors — `UniqueConstraintError` (23505), `ForeignKeyConstraintError` (23503), `NotNullConstraintError` (23502), `CheckConstraintError` (23514), all inheriting from a new `IntegrityError` base. Each carries `.constraint`/`.table`/`.column`/`.detail` from the underlying Postgres error and chains the original exception via `__cause__`. Lets a FastAPI app register `@app.exception_handler(UniqueConstraintError)`-style handlers once at the app level instead of every route catching `asyncpg` internals.

  **This changes what gets raised for constraint violations** — code that was catching raw `asyncpg.exceptions.UniqueViolationError` (etc.) directly needs to catch `UniqueConstraintError` (etc.) instead, or the common `IntegrityError`/`ConnectorError` base. The original `asyncpg` exception is still available via `exc.__cause__`.

- **Timeouts**: `connect_timeout`/`command_timeout` are now exposed on `PostgreSQLConnector(...)` (default 10s / 30s respectively; `command_timeout=None` disables it), and `raw()`/`raw_execute()`/`call()` (and their `tx.` forms) accept a per-call `timeout=` override. Exceeding a timeout raises `QueryTimeoutError` instead of a bare `TimeoutError`.

### Fixed
- **`command_timeout` was silently the only timeout that existed, and wasn't configurable**: `ConnectionPool`'s `connect_timeout` parameter was named and documented as a connection-establishment timeout, but was actually wired to asyncpg's `command_timeout` — the default timeout applied to *every query* — and wasn't even exposed on `PostgreSQLConnector` at all, so every query has had an undocumented, fixed, unconfigurable 10-second cap since the transaction-API work landed. Now split into two correctly-named, correctly-wired, and configurable parameters.
- **Retry-storm risk on query timeouts**: `_RETRYABLE_CONNECTION_ERRORS` used the broad `OSError` to catch connection failures like `ConnectionRefusedError`, but `TimeoutError` is *also* an `OSError` subclass — so a query that simply ran too long was being treated as a retryable connection failure and retried up to `max_query_retries` times, each attempt eating the same timeout again, instead of backing off. Narrowed to `ConnectionError` (the precise builtin base for `ConnectionRefusedError`/`ConnectionResetError`/`ConnectionAbortedError`/`BrokenPipeError`, verified via MRO — `TimeoutError` is a sibling under `OSError`, not a subclass of `ConnectionError`), so a timeout now fails immediately as `QueryTimeoutError` on the first attempt, and `run_transaction()`'s callback-level retry gets the same fix. Real connection-failure retries (the original fault-tolerance feature) are unaffected — reverified against the same real-database-restart test after the change.
- **`create()`/`create_many()`/`upsert()` were broken for every model with `auto_now`/`auto_now_add`**: these fields are conventionally declared as `str` in a model but the migrator creates them as a real `TIMESTAMP` column — the write path was stamping them with a `strftime`'d Python string, which asyncpg's `TIMESTAMP` codec rejects outright (`expected a datetime.date or datetime.datetime instance, got 'str'`). No prior test actually created a row against a live database with these fields, so this was live and broken. Fixed by stamping a native `datetime.utcnow()` instead; `_validate_field_types()` now skips `auto_now`/`auto_now_add` fields rather than rejecting the framework's own (correct) stamp against the model's `str` annotation. Found via a large-dataset (27,000-row) smoke test — see `examples/full_feature_smoketest.py`.

### Removed
- **Soft delete**: `find`/`find_one`/`find_by_id`/`count`/`exists` no longer auto-filter a `deleted_at` column, `delete_by_id`/`delete_all` no longer special-case it either (both always run a real `DELETE` now — the `hard` parameter they briefly had is gone, since there's no longer a default behavior for it to override), and relation includes no longer filter soft-deleted related rows. Recognizing "deletedness" by a magic column name and rewriting query behavior around it across the whole read/write path is application policy, not something a query library should have an opinion on — and it's little enough code (a plain column + your own `update_by_id`/`where` filtering) that it isn't worth a library feature, let alone a shipped mixin. See the "Deletes" section in the README for the two-line recipe if you want this in your own app.

## [1.1.0] - 2026-09-20

### Added
- `connector.begin()` — async context manager yielding a `TransactionContext` whose `find`/`create`/`update_by_id`/`delete_by_id`/`raw`/... calls are implicitly bound to one connection/transaction (no more passing `session=conn` to every call, and no way to accidentally run a write outside the transaction).
- Nested `async with tx.begin():` — PostgreSQL SAVEPOINTs, so a nested block can roll back independently without aborting the outer transaction.
- `connector.run_transaction(func, ...)` — runs `func(tx)` inside a transaction and automatically retries the whole callback on serialization failures (`40001`) and deadlocks (`40P01`), with exponential backoff, up to `max_retries`. Raises `TransactionError` (chaining the original error) once retries are exhausted or a non-retryable error occurs.
- `isolation` / `readonly` / `deferrable` parameters on `connector.begin()`, `connector.run_transaction()`, and the underlying `connector.transaction()` / `ConnectionPool.transaction()`.
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
