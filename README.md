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
- **Hooks** — `before_create`/`after_create` (`create()`), `before_update`/`after_update` (`update_by_id()`), `before_delete`/`after_delete` (`delete_by_id()`), as methods or registered with `Model.observe(event, fn)`; bulk `create_many`/`update_all`/`delete_all` skip them. Raising from a `before_*` hook aborts the write
- **Transactions** — `begin()` with nested savepoints
- **Fault tolerant** — reads auto-retry through transient connection failures (DB restart, network blip); writes never blind-retry
- **Timeouts** — configurable default per-query timeout, plus a per-call override on `raw()`/`raw_execute()`; a timeout is never mistaken for a retryable connection failure
- **Typed error mapping** — unique/FK/not-null/check violations become `UniqueConstraintError`/etc., not raw `asyncpg` exceptions
- **Upsert** — `INSERT ... ON CONFLICT DO UPDATE`/`DO NOTHING` in one round trip
- **JSONB support** — `JSONField` for dict/list columns with auto serialization
- **FastAPI ready** — return models (or lists of models) from routes as-is; they serialize like `to_dict()`

## Installation

```bash
pip install oceanic-postgres-orm
```

## Quick Start

```python
from typing import List

from postgres_connector import PostgreSQLConnector, PostgreSQLModel, Field, Relation

class User(PostgreSQLModel):
    __table__ = "users"
    id:     int           = Field(primary_key=True)
    name:   str           = Field(max_length=100)
    email:  str           = Field(unique=True)
    active: bool          = Field(default=True)
    orders: List["Order"] = Relation(type="hasMany", foreign_key="user_id")

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
from typing import List

class User(PostgreSQLModel):
    __table__ = "users"
    id:      int              = Field(primary_key=True)
    # hasMany
    orders:  List["Order"]   = Relation(type="hasMany",   foreign_key="user_id")
    # hasOne
    profile: "Profile"       = Relation(type="hasOne",    foreign_key="user_id")
    # hasManyThrough
    tags:    List["Tag"]     = Relation(
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

## Mixins

The library ships no mixins. You write your own as plain classes with fields, relations and hooks, and attach them through inheritance:

```python
class TimestampMixin:
    created_at: str = Field(auto_now_add=True)
    updated_at: str = Field(auto_now=True, nullable=True)

class Order(TimestampMixin, PostgreSQLModel):
    __table__ = "orders"
    id: int = Field(primary_key=True)
```

A mixin's `Field(...)` options, `Relation(...)`s and hook methods apply to every model that inherits it. Hook methods from mixins add up: if two mixins both define `before_create`, both run.

## Hooks

Write a hook as a method on the model (or on a mixin), or register any number with `observe()`. For each event, hooks run in this order:

1. hook methods from mixins, base classes first (for `class Doc(A, B, PostgreSQLModel)`, `B`'s then `A`'s)
2. the model's own hook method (a subclass model's method replaces its parent model's, as usual)
3. observers, in registration order (observers on a model also apply to its subclasses)

Mixin hooks run on their own, so a model's hook should not call `super()` to reach them; that would run the mixin's hook twice.

```python
class Order(PostgreSQLModel):
    ...
    async def before_create(self):
        self.reference = new_reference()

@Order.observe("after_delete")
async def audit(order):
    await write_audit_log("deleted", order.id)
```

Events: `before_create`, `after_create`, `before_update` (also receives the `data` being set, which it may change), `after_update`, `before_delete`, `after_delete`. Raising from a `before_*` hook aborts the write.

## Pagination

`find_and_count()` returns one page and the total number of matches, for list endpoints. It takes the same filter as `find()`; the count uses only `where`.

```python
users, total = await connector.find_and_count(
    User, {"where": {"active": True}, "order": ["id ASC"], "limit": 20, "skip": 40}
)
```

The two queries run concurrently on two pooled connections. Inside `begin()` (`await tx.find_and_count(...)`) they run one after the other on the transaction's connection.

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

Migrations create missing tables, columns, indexes and constraints, and change a column's type when the model's type changed. They never drop tables or columns.

### Upgrading to 3.0

3.0 stores `auto_now` / `auto_now_add` columns as `TIMESTAMPTZ` (UTC) instead of `TIMESTAMP`, so rows written by the ORM and by SQL `DEFAULT`s always agree.

- **Run `migrate()` once in a maintenance window.** It converts each existing column, keeping the stored values (`ALTER COLUMN ... TYPE TIMESTAMPTZ USING col AT TIME ZONE 'UTC'`). This rewrites the table and blocks reads and writes on it while it runs: instant for small tables, minutes for very large ones.
- **Compare with aware datetimes.** These fields now come back with a time zone, so `row.created_at < datetime.utcnow()` raises `TypeError`. Use `datetime.now(timezone.utc)`.
- Rows written by SQL `DEFAULT` in a non-UTC session before upgrading were stored in local time; they can't be told apart automatically. Fix them by hand if it matters.
- Also check the 3.0 behaviour changes in the [CHANGELOG](CHANGELOG.md): unknown keys in `create()` now raise, mixin hooks all run, and `Field(foreign_key=...)` now creates a constraint.

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

Two more typed errors cover failures that used to come out as raw driver exceptions:

| Exception | Condition |
|---|---|
| `InvalidValueError` | a value the column can't take: text for a number, too long for a `VARCHAR`, out of range (SQLSTATE class 22, or asyncpg rejecting a parameter) |
| `ReadOnlyTransactionError` | a write inside `begin(readonly=True)` |

### FastAPI

One call maps them all to HTTP responses, so no route needs a `try`/`except`:

```python
from fastapi import FastAPI
from postgres_connector.fastapi import register_exception_handlers

app = FastAPI()
register_exception_handlers(app)
```

| Error | HTTP status |
|---|---|
| `QueryError`, `InvalidValueError`, `ConfigurationError` (bad client input) | 400 |
| `UniqueConstraintError`, `ForeignKeyConstraintError` | 409 |
| `NotNullConstraintError`, `CheckConstraintError` | 422 |
| `QueryTimeoutError` | 504 |

The body is `{"error": "<class name>", "detail": "<message>"}`, plus `constraint`, `table` and `column` when Postgres reported them. Postgres's DETAIL text, which quotes stored values (`Key (email)=(raj@x.io) already exists`), is left out of `detail` and only sent as `db_detail` with `register_exception_handlers(app, expose_db_detail=True)`; keep it off for public APIs. To shape responses differently, register your own handlers with `@app.exception_handler(...)` instead, or after this call to override one class.

## Pydantic models for FastAPI

`Model.pydantic_model()` generates a Pydantic model from the fields and relations the model already declares, for `response_model`s and OpenAPI schemas. No hand-written duplicate class to keep in sync.

```python
UserOut        = User.pydantic_model()                              # all fields
UserWithOrders = User.pydantic_model(include=["orders.items"])      # relations, nested
UserPublic     = User.pydantic_model(exclude=["password_hash"])

@app.get("/users/{id}", response_model=UserWithOrders)
async def get_user(id: int):
    return await db.find_by_id(User, id, {"include": ["orders.items"]})
```

- Field types, nullability and `max_length` come from `Field(...)`. `auto_now` / `auto_now_add` fields are `datetime`, JSON fields accept any JSON.
- Included relations are nested generated models: a list for `hasMany` / `hasManyThrough`, an optional object for `belongsTo` / `hasOne`.
- The generated model reads ORM instances directly (`from_attributes`), so routes return the instance as-is.
- Results are cached per arguments. Pass `name=` to choose the schema name shown in OpenAPI.
- Needs Pydantic v2, which FastAPI installs.

### REST filters (`?filter=`)

Clients can send the whole query as one JSON `filter` parameter, LoopBack-style. `FilterParam` parses it, checks keys, fields, operators and relations (at any include depth) against the model, caps `limit`, and documents the parameter in OpenAPI. Bad input is an HTTP 400 with the reason.

```python
from fastapi import Depends
from postgres_connector import FilterOptions
from postgres_connector.fastapi import FilterParam

@app.get("/users")
async def list_users(filter: FilterOptions = Depends(FilterParam(User, max_limit=100))):
    return await db.find(User, filter)
```

`GET /users?filter={"where":{"active":true},"order":["name ASC"],"limit":20,"include":["orders"]}`

Without a `limit`, `max_limit` is used; a larger `limit` is rejected. Pass `max_limit=None` for no cap.

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

await connector.update_by_id(Post, 1, {"deleted_at": datetime.now(timezone.utc)})
await connector.find(Post, {"where": {"deleted_at": None}})
```

## Performance

Against the SQLAlchemy 2.x async ORM on the same database and data, this library is roughly 1.1–2.6× faster per call. The largest gain is on nested includes (2.5×), the smallest on database-bound queries (1.1×). SQLAlchemy Core is faster for bulk inserts. Methodology, full results and the scripts to re-run them are in [`benchmarks/`](benchmarks/README.md).

## Full-Feature Verification

[`examples/full_feature_smoketest.py`](examples/full_feature_smoketest.py) seeds a large local dataset (5,000 customers, 20,000 orders) and exercises every feature in this README against it end-to-end — bulk `create_many`, the filter DSL, zero-N+1 relation loading, upsert, transactions/savepoints, delete, typed exception mapping, and query timeouts — printing a pass/fail line per feature:

```bash
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=postgres DB_PASSWORD=secret DB_NAME=smoketest \
    python examples/full_feature_smoketest.py
```

## License

MIT
