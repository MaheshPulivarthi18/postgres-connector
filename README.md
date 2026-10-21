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
- **Hooks** — `before_create`/`after_create` (`create()`), `before_update`/`after_update` (`update_by_id()`), `before_delete` (`delete_by_id()`); bulk `create_many`/`update_all`/`delete_all` skip them. Raising from a `before_*` hook aborts the write
- **Transactions** — `begin()` with nested savepoints
- **Fault tolerant** — reads auto-retry through transient connection failures (DB restart, network blip); writes never blind-retry
- **Timeouts** — configurable default per-query timeout, plus a per-call override on `raw()`/`raw_execute()`; a timeout is never mistaken for a retryable connection failure
- **Typed error mapping** — unique/FK/not-null/check violations become `UniqueConstraintError`/etc., not raw `asyncpg` exceptions
- **Upsert** — `INSERT ... ON CONFLICT DO UPDATE`/`DO NOTHING` in one round trip
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

An include can carry its own `scope` (`where`, `order`, `limit`, `skip`, `fields`, nested `include`). `limit`/`skip` apply **per parent**, so this loads each user's 5 most recent orders, still in one query per relation:

```python
users = await connector.find(User, {
    "include": [{"relation": "orders", "scope": {"order": ["created_at DESC"], "limit": 5}}],
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

**`connector.begin()`** — every ORM call made through `tx` runs in the same transaction, so nothing can accidentally write outside it:

```python
async with connector.begin() as tx:
    order = await tx.create(Order, {"user_id": user.id, "total": 99.99})
    await tx.update_by_id(User, user.id, {"order_count": user.order_count + 1})
# both committed together, or both rolled back on exception
```

Hand-written SQL joins the same transaction through `tx.raw(...)` / `tx.raw_execute(...)`:

```python
async with connector.begin() as tx:
    await tx.raw_execute("UPDATE users SET order_count = order_count + 1 WHERE id = $1", [user.id])
```

Nested `async with tx.begin():` blocks use PostgreSQL SAVEPOINTs, so a nested block can fail and roll back on its own without aborting the outer transaction.

## Fault Tolerance

Transient connection failures — a DB restart, a network blip, a connection getting recycled — are automatically retried, with exponential backoff, up to `max_query_retries` (default 3, pass `max_query_retries=0` to disable):

```python
connector = PostgreSQLConnector(host="...", database="...", max_query_retries=5)
```

**The safety boundary that makes this safe to leave on by default: reads retry, writes never do (outside a transaction).** Retrying a read has no side effects — worst case you re-run the same `SELECT`. Retrying a write blindly is dangerous: if the connection drops in the gap between the server committing and the client receiving the acknowledgment, a naive retry double-executes it (a duplicate row, a double balance debit). So:

- `find()` / `find_one()` / `find_by_id()` / `count()` / `exists()` retry automatically.
- `create()` / `create_many()` / `update_all()` / `update_by_id()` / `delete_all()` / `delete_by_id()` / `upsert()` / `raw()` / `raw_execute()` never auto-retry — a connection failure there raises immediately.

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

Override it for one call via `timeout=` on `raw()`/`raw_execute()`, where "this specific call needs a different budget" is most likely to matter — a slow report query, a maintenance statement:

```python
rows = await connector.raw("SELECT ... /* big report */", timeout=120.0)
await connector.raw_execute("VACUUM ANALYZE big_table", timeout=300.0)
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

[`examples/full_feature_smoketest.py`](examples/full_feature_smoketest.py) seeds a large local dataset (5,000 customers, 20,000 orders) and exercises every feature in this README against it end-to-end — bulk `create_many`, the filter DSL, zero-N+1 relation loading, upsert, transactions/savepoints, delete, typed exception mapping, and query timeouts — printing a pass/fail line per feature:

```bash
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=postgres DB_PASSWORD=secret DB_NAME=smoketest \
    python examples/full_feature_smoketest.py
```

## License

MIT
