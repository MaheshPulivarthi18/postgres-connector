# oceanic-postgres-orm

A production-grade async PostgreSQL ORM with LoopBack-style nested relation queries, typed model instances, and zero N+1.

Built on [`asyncpg`](https://github.com/MagicStack/asyncpg) — the fastest PostgreSQL driver for Python.

## Features

- **Async-first** — built on `asyncpg` for non-blocking I/O
- **Zero N+1** — batch-loads all relations with a single `IN (...)` query per depth level
- **Typed instances** — IDE autocomplete on all model fields and relations
- **LoopBack filter DSL** — declarative `where`, `order`, `limit`, `skip`, `fields`, `include`
- **Nested includes** — `"orders.items.product"` resolved in O(depth) queries, not O(N)
- **4 relation types** — `hasOne`, `hasMany`, `belongsTo`, `hasManyThrough`
- **Schema migrations** — additive-only `migrate()` syncs your models to the database
- **Hooks** — `before_create`/`after_create` (`create()`), `before_update`/`after_update` (`update_by_id()`), `before_delete` (`delete_by_id()`); bulk `create_many`/`update_all`/`delete_all` skip them
- **Transactions** — `begin()`/`run_transaction()` with nested savepoints and automatic retry on serialization failures/deadlocks
- **Fault tolerant** — reads auto-retry through transient connection failures (DB restart, network blip); writes never blind-retry
- **Timeouts** — configurable default per-query timeout, plus a per-call override on `raw()`/`raw_execute()`/`call()`; a timeout is never mistaken for a retryable connection failure
- **Typed error mapping** — unique/FK/not-null/check violations become `UniqueConstraintError`/etc., not raw `asyncpg` exceptions
- **Row locking** — `FOR UPDATE`/`FOR SHARE`/`SKIP LOCKED` in the filter DSL
- **Stored procedures** — declare once with `Procedure(...)`, call typed everywhere via `connector.call(...)`; `sync_procedures()` creates/replaces them in the DB from your exact SQL
- **Upsert** — `INSERT ... ON CONFLICT DO UPDATE`/`DO NOTHING` in one round trip
- **Pub/Sub** — `listen()`/`notify()` wrapping Postgres `LISTEN`/`NOTIFY`, transaction-aware
- **Streaming** — `stream()` reads large result sets through a server-side cursor, bounded memory
- **JSONB support** — `JSONField` for dict/list columns with auto serialization
- **FastAPI ready** — `__json__()` hook for response serialization

## Installation

```bash
pip install oceanic-postgres-orm
```

## Quick Start

```python
from postgres_connector import PostgreSQLConnector, PostgreSQLModel, Field, Relation

class User(PostgreSQLModel):
    __table__ = "users"
    id:     int           = Field(primary_key=True)
    name:   str           = Field(max_length=100)
    email:  str           = Field(unique=True)
    active: bool          = Field(default=True)
    orders: list["Order"] = Relation(type="hasMany", foreign_key="user_id")

class Order(PostgreSQLModel):
    __table__ = "orders"
    id:      int    = Field(primary_key=True)
    user_id: int    = Field(foreign_key="users.id")
    total:   float
    user:    "User" = Relation(type="belongsTo", foreign_key="user_id")

connector = PostgreSQLConnector(
    host="localhost", port=5432,
    user="postgres", password="secret",
    database="mydb",
)
await connector.connect()
await connector.migrate()

# Create
user = await connector.create(User, {"name": "Raj", "email": "raj@example.com"})

# Insert-or-update in one round trip, no race
user = await connector.upsert(
    User, {"email": "raj@example.com", "name": "Raj"}, conflict_target=["email"],
)

# Find with nested relations
users = await connector.find(User, {
    "where": {"active": True},
    "include": ["orders"],
    "order": ["name ASC"],
    "limit": 10,
})

for user in users:
    print(user.name, user.orders)  # fully typed

# Update — one round trip via UPDATE ... RETURNING *
await connector.update_by_id(User, user.id, {"name": "Rajesh"})

# Existence check — SELECT EXISTS(...), not a full COUNT(*)
taken = await connector.exists(User, {"email": "raj@example.com"})

# Delete
await connector.delete_by_id(User, user.id)

await connector.disconnect()
```

## Filter DSL

```python
await connector.find(User, {
    "where": {
        "active": True,
        "age": {"gte": 18, "lte": 65},
        "or": [
            {"name": {"like": "%raj%"}},
            {"email": {"ilike": "%@gmail.com"}},
        ],
    },
    "order": ["created_at DESC", "name ASC"],
    "limit": 20,
    "skip": 40,
    "fields": ["id", "name", "email"],
    "include": ["orders.items.product"],
})
```

### Supported operators

| Operator  | SQL equivalent     |
|-----------|--------------------|
| `eq`      | `=`                |
| `neq`     | `!=`               |
| `gt`      | `>`                |
| `gte`     | `>=`               |
| `lt`      | `<`                |
| `lte`     | `<=`               |
| `like`    | `LIKE`             |
| `nlike`   | `NOT LIKE`         |
| `ilike`   | `ILIKE`            |
| `inq`     | `IN (...)`         |
| `nin`     | `NOT IN (...)`     |
| `between` | `BETWEEN x AND y`  |
| `regexp`  | `~`                |

## Streaming Large Result Sets

`find()` loads the whole result set into memory. For exports, backfills, or any batch job over more rows than comfortably fit in memory, `stream()` reads through a server-side cursor instead — bounded memory, one round trip to the server per `batch_size` rows instead of one giant round trip for everything.

```python
async for user in connector.stream(User, {"where": {"active": True}}, batch_size=500):
    await export_row(user)   # only ~500 rows in memory at a time, not the whole table
```

This holds one connection open for as long as you keep iterating — that's the trade: bounded memory in exchange for a held connection, so don't leave a stream half-consumed under heavy concurrent load on a small pool. If you don't pass `session=`, `stream()` opens and manages its own transaction (cursors require one) and releases the connection automatically when the loop ends or you `break` out of it early. Also available inside a transaction via `tx.stream(...)`. `include` (relation batch-loading) isn't supported — resolving relations a row at a time would defeat the point of streaming.

## Relations

```python
class User(PostgreSQLModel):
    __table__ = "users"
    id:      int              = Field(primary_key=True)
    # hasMany
    orders:  list["Order"]   = Relation(type="hasMany",   foreign_key="user_id")
    # hasOne
    profile: "Profile"       = Relation(type="hasOne",    foreign_key="user_id")
    # hasManyThrough
    tags:    list["Tag"]     = Relation(
        type="hasManyThrough",
        through="UserTag",
        foreign_key="user_id",
        other_key="tag_id",
    )

class Order(PostgreSQLModel):
    __table__ = "orders"
    id:      int   = Field(primary_key=True)
    user_id: int   = Field(foreign_key="users.id")
    # belongsTo
    user:    "User" = Relation(type="belongsTo", foreign_key="user_id")
```

## Upsert

`INSERT ... ON CONFLICT (...) DO UPDATE` in one round trip — no race between a check-then-write. `conflict_target` must have a unique or primary-key constraint in the database (Postgres requires this to detect the conflict).

```python
user = await connector.upsert(
    User,
    {"email": "raj@example.com", "name": "Raj", "login_count": 1},
    conflict_target=["email"],
)
# calling again with the same email updates name/login_count instead of
# raising a unique-constraint error
```

`update_fields` defaults to every field in `data` except `conflict_target`. Pass `update_fields=["login_count"]` to only touch specific columns on conflict, or `update_fields=[]` to make a conflict a no-op (`ON CONFLICT DO NOTHING`) — in that case `upsert()` returns `None` since nothing changed. Works inside a transaction too: `await tx.upsert(...)`.

## Migrations

```python
await connector.migrate()
```

Migrations are **additive-only** — they create missing tables and add missing columns. They never drop or alter existing columns.

## Transactions

Three ways to run work inside a transaction, from lowest to highest level:

**`connector.transaction()`** — raw asyncpg connection, for hand-written SQL:

```python
async with connector.transaction() as conn:
    await conn.execute("INSERT INTO orders (user_id, total) VALUES ($1, $2)", 1, 99.99)
    await conn.execute("UPDATE users SET order_count = order_count + 1 WHERE id = $1", 1)
# auto-committed; rolled back on exception
```

**`connector.begin()`** — typed ORM calls bound to one transaction automatically (no `session=` to remember, no way to accidentally write outside the transaction):

```python
async with connector.begin() as tx:
    order = await tx.create(Order, {"user_id": user.id, "total": 99.99})
    await tx.update_by_id(User, user.id, {"order_count": user.order_count + 1})
# both committed together, or both rolled back on exception
```

Nested `async with tx.begin():` blocks use PostgreSQL SAVEPOINTs, so a nested block can fail and roll back on its own without aborting the outer transaction.

**`connector.run_transaction(func, ...)`** — like `begin()`, but retries the whole callback automatically on serialization failures / deadlocks (the two failure modes concurrent transactions hit under contention). Use this with `isolation="serializable"` for money-movement / inventory-decrement style logic where two transactions might otherwise race:

```python
async def transfer(tx):
    sender = await tx.find_by_id(Account, sender_id)
    if sender.balance < amount:
        raise InsufficientFundsError()
    await tx.update_by_id(Account, sender_id, {"balance": sender.balance - amount})
    receiver = await tx.find_by_id(Account, receiver_id)
    await tx.update_by_id(Account, receiver_id, {"balance": receiver.balance + amount})

await connector.run_transaction(transfer, isolation="serializable")
```

`func` must be safe to call more than once (no side effects outside the database) since a conflicting concurrent transaction causes it to be retried from scratch. After `max_retries` attempts it raises `TransactionError` with the original database error chained via `__cause__`.

## Fault Tolerance

Transient connection failures — a DB restart, a network blip, a connection getting recycled — are automatically retried, with exponential backoff, up to `max_query_retries` (default 3, pass `max_query_retries=0` to disable):

```python
connector = PostgreSQLConnector(host="...", database="...", max_query_retries=5)
```

**The safety boundary that makes this safe to leave on by default: reads retry, writes never do (outside a transaction).** Retrying a read has no side effects — worst case you re-run the same `SELECT`. Retrying a write blindly is dangerous: if the connection drops in the gap between the server committing and the client receiving the acknowledgment, a naive retry double-executes it (a duplicate row, a double balance debit). So:

- `find()` / `find_one()` / `find_by_id()` / `count()` / `exists()` retry automatically.
- `create()` / `create_many()` / `update_all()` / `update_by_id()` / `delete_all()` / `delete_by_id()` / `upsert()` / `raw()` / `raw_execute()` never auto-retry — a connection failure there raises immediately.
- Inside `run_transaction()`, the *whole callback* retries on a connection failure too (in addition to the existing serialization-failure/deadlock retry) — safe, because nothing committed if the connection never confirmed it.

A timed-out query is deliberately **not** treated as a retryable connection failure — see Timeouts below for why.

## Timeouts

Every query has a default timeout so one slow/hung query can't freeze a web worker indefinitely:

```python
connector = PostgreSQLConnector(
    host="...", database="...",
    connect_timeout=10.0,    # seconds to establish a new connection
    command_timeout=30.0,    # default seconds allowed per query; None disables it
)
```

Override it for one call via `timeout=` on the three methods where "this specific call needs a different budget" is most likely to matter — a slow report query, a long-running procedure:

```python
rows = await connector.raw("SELECT ... /* big report */", timeout=120.0)
await connector.raw_execute("VACUUM ANALYZE big_table", timeout=300.0)
result = await connector.call(some_slow_procedure, timeout=60.0, ...)
```

(Not exposed on `find`/`create`/`update_by_id`/etc. — those use the connector-wide `command_timeout` default; use `raw()`/`raw_execute()` if one of them specifically needs a different budget.) Exceeding a timeout raises `QueryTimeoutError`, not a bare `TimeoutError` — same motivation as the typed constraint-violation errors below: one `@app.exception_handler(QueryTimeoutError)` instead of every route guessing what a bare `TimeoutError` means.

**A timeout is not retried** — `find()`/`count()`/`exists()`'s automatic retry (above) only fires for genuine connection failures. Retrying an already-slow query is usually the wrong move — it piles more load onto a database that's already struggling to keep up, instead of backing off.

## Errors

Every write (`create`, `create_many`, `update_by_id`, `upsert`, `raw_execute`, and their `session=`/`tx.` forms) translates common constraint violations into typed exceptions instead of leaking raw `asyncpg` internals into your route handlers:

| Exception | Postgres condition |
|---|---|
| `UniqueConstraintError` | `UNIQUE`/`PRIMARY KEY` violation (e.g. duplicate email) |
| `ForeignKeyConstraintError` | referenced row doesn't exist, or a still-referenced row was deleted/updated |
| `NotNullConstraintError` | a required column was left empty |
| `CheckConstraintError` | a `CHECK` constraint failed |

All four inherit from `IntegrityError` (catch that for "some constraint failed, don't care which"), which inherits from `ConnectorError` (the catch-all base for everything this package raises). Each carries `.constraint`, `.table`, `.column`, `.detail` from the underlying Postgres error, and chains the original `asyncpg` exception via `__cause__` if you need it.

This is what makes error handling a one-time FastAPI setup instead of a per-route concern — register exception handlers once, at the app level:

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from postgres_connector import UniqueConstraintError, ForeignKeyConstraintError, IntegrityError

app = FastAPI()

@app.exception_handler(UniqueConstraintError)
async def handle_conflict(request: Request, exc: UniqueConstraintError):
    return JSONResponse(status_code=409, content={"error": "already exists", "detail": exc.detail})

@app.exception_handler(ForeignKeyConstraintError)
async def handle_bad_reference(request: Request, exc: ForeignKeyConstraintError):
    return JSONResponse(status_code=422, content={"error": "invalid reference", "detail": exc.detail})

@app.exception_handler(IntegrityError)
async def handle_other_integrity_error(request: Request, exc: IntegrityError):
    return JSONResponse(status_code=422, content={"error": "constraint violation", "detail": exc.detail})
```
Every route in the app gets consistent 409/422 responses for these without a single `try`/`except` in the route itself.

## Row Locking

Pass `lock` in a filter to add `FOR UPDATE` / `FOR SHARE` to the query — the standard building block for job-queue / "claim the next row" patterns. It only makes sense inside a transaction (a lock taken outside one is released the instant the query finishes), so `find`/`find_one` raise `QueryError` if `lock` is used without `session=` — i.e. outside `begin()`/`run_transaction()`.

Modes: `"update"` (`FOR UPDATE`, blocks until the row is free), `"update_nowait"` (`FOR UPDATE NOWAIT`, raises immediately instead of waiting), `"update_skip_locked"` (`FOR UPDATE SKIP LOCKED`, skips rows another transaction already has locked), `"share"` (`FOR SHARE`).

```python
async def claim_next_job(tx):
    job = await tx.find_one(Job, {
        "where": {"status": "pending"},
        "order": ["created_at ASC"],
        "lock": "update_skip_locked",   # skip rows another worker already has locked
    })
    if job:
        await tx.update_by_id(Job, job.id, {"status": "in_progress"})
    return job

job = await connector.run_transaction(claim_next_job)
```

Combined with `listen()`/`notify()` (below), this is the full reliable-job-queue pattern — see [`examples/job_queue.py`](examples/job_queue.py) for a complete, runnable demo with multiple concurrent workers.

## Raw SQL

```python
# SELECT
rows = await connector.raw(
    "SELECT u.id, u.name, COUNT(o.id) AS order_count "
    "FROM users u LEFT JOIN orders o ON o.user_id = u.id "
    "WHERE u.active = $1 GROUP BY u.id",
    [True],
)

# DML / DDL
affected = await connector.raw_execute("DELETE FROM sessions WHERE expired = $1", [True])
```

## Stored Procedures & Functions

Declare a Postgres function or procedure once with `Procedure(...)`, call it through the same typed API everywhere — no hand-written SQL, no manual result-row hydration. Declaring a `Procedure` auto-registers it (like models do), so it can also be called later by its string name.

```python
from postgres_connector import Procedure

# Scalar function: SELECT add_numbers($1, $2)
add_numbers = Procedure("add_numbers", params=["a", "b"])

# Table-returning function: SELECT * FROM get_active_users()
get_active_users = Procedure("get_active_users", returns=User)

# Procedure (no return value): CALL bump_counter($1)
bump_counter = Procedure("bump_counter", params=["p_id"], kind="procedure")

total = await connector.call(add_numbers, a=2, b=3)     # -> 5
users = await connector.call(get_active_users)           # -> [User, User, ...]
await connector.call(bump_counter, p_id=1)                # -> None, side effect only
await connector.call("bump_counter", p_id=1)               # look up by registered name
```

Every parameter declared on a `Procedure` must be passed as a keyword argument to `call()` — missing or unexpected ones raise `QueryError` before anything reaches the database. `kind="procedure"` doesn't support `OUT` parameters — use `connector.raw(...)` for those. `call()` is also available inside a transaction via `tx.call(...)`.

### Actually creating the procedure in the database

By itself, `Procedure(...)` is a *calling* binding — it doesn't create anything, it just describes how to call something that already exists (created via `psql`, a DBA-managed migration, whatever). To have this library create/replace it too, pass `body` — your exact `CREATE OR REPLACE FUNCTION`/`PROCEDURE` SQL, executed verbatim, nothing parsed or generated — then run `connector.sync_procedures()`:

```python
add_numbers = Procedure(
    "add_numbers",
    params=["a", "b"],
    body="""
        CREATE OR REPLACE FUNCTION add_numbers(a INT, b INT) RETURNS INT AS $$
        BEGIN RETURN a + b; END;
        $$ LANGUAGE plpgsql
    """,
)

await connector.sync_procedures()   # creates/replaces every registered Procedure that has a body
total = await connector.call(add_numbers, a=2, b=3)
```

`body` is optional and stays that way for procedures you don't want this library managing — those with no `body` are skipped entirely. Write `body` as `CREATE OR REPLACE` so `sync_procedures()` is safe to run repeatedly (e.g. every app startup), the same way `migrate()` is. Deliberately kept separate from `migrate()` — that's schema, this is executable code — and, like `migrate()`, never drops anything for a `Procedure` that's no longer registered.

## Pub/Sub (LISTEN / NOTIFY)

```python
async def on_job_ready(payload):
    print("new job:", payload)

stop = await connector.listen("jobs_channel", on_job_ready)
...
await connector.notify("jobs_channel", json.dumps({"job_id": 42}))
...
await stop()   # or just leave it — disconnect() cleans up any listener you forgot to stop
```

`listen()` opens a **dedicated connection** outside the pool — Postgres `LISTEN` registers state on one specific connection, so it can never be a connection borrowed from (and returned to) the shared pool. `callback` can be a plain function or a coroutine function; either is supported.

`NOTIFY` is transactional in Postgres: sent via `connector.notify(...)` directly it's delivered immediately, but sent via `tx.notify(...)` inside `begin()`/`run_transaction()` it's only delivered once that transaction **commits** — and silently dropped if it rolls back. This is usually exactly what you want (don't wake up a worker for data that isn't visible yet):

```python
async def create_job(tx):
    job = await tx.create(Job, {"status": "pending"})
    await tx.notify("jobs_channel", str(job.id))   # delivered only after commit
    return job

await connector.run_transaction(create_job)
```

## Debug mode

```python
connector = PostgreSQLConnector(..., echo=True)
```

Prints every SQL query and its parameters to stdout with color coding.

## Deletes

`delete_by_id()`/`delete_all()` always run a real `DELETE`. There's no soft-delete convention built in — recognizing rows by a magic `deleted_at` column name and quietly rewriting `find`/`count`/`exists`/`delete` around it is application policy, not a query-library concern, and it doesn't belong baked into the core of every read path. It's also genuinely little code to own yourself:

```python
# "Soft delete": just a normal column and a normal UPDATE, in your own code.
class Post(PostgreSQLModel):
    __table__ = "posts"
    id:         int = Field(primary_key=True)
    title:      str
    deleted_at: str = Field(nullable=True)

await connector.update_by_id(Post, 1, {"deleted_at": datetime.utcnow()})
await connector.find(Post, {"where": {"deleted_at": None}})
```

## Full-Feature Verification

[`examples/full_feature_smoketest.py`](examples/full_feature_smoketest.py) seeds a large local dataset (5,000 customers, 20,000 orders, 2,000 jobs) and exercises every feature in this README against it end-to-end — bulk `create_many`, the filter DSL, zero-N+1 relation loading, upsert, transactions/savepoints, `SKIP LOCKED` row locking across concurrent workers, streaming cursors, stored procedures, LISTEN/NOTIFY, delete, typed exception mapping, and query timeouts — printing a pass/fail line per feature:

```bash
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=postgres DB_PASSWORD=secret DB_NAME=smoketest \
    python examples/full_feature_smoketest.py
```

## License

MIT
