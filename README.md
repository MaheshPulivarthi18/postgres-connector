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
- **Soft deletes** — automatic `deleted_at IS NULL` filtering
- **Hooks** — `before_create`, `after_create`, `before_update`, etc.
- **Transactions** — explicit transaction context manager
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

# Find with nested relations
users = await connector.find(User, {
    "where": {"active": True},
    "include": ["orders"],
    "order": ["name ASC"],
    "limit": 10,
})

for user in users:
    print(user.name, user.orders)  # fully typed

# Update
await connector.update_by_id(User, user.id, {"name": "Rajesh"})

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

## Migrations

```python
await connector.migrate()
```

Migrations are **additive-only** — they create missing tables and add missing columns. They never drop or alter existing columns.

## Transactions

```python
async with connector.transaction() as conn:
    await conn.execute("INSERT INTO orders (user_id, total) VALUES ($1, $2)", 1, 99.99)
    await conn.execute("UPDATE users SET order_count = order_count + 1 WHERE id = $1", 1)
# auto-committed; rolled back on exception
```

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

## Soft Deletes

Models with a `deleted_at` field automatically filter out soft-deleted records:

```python
class Post(PostgreSQLModel):
    __table__ = "posts"
    id:         int = Field(primary_key=True)
    title:      str
    deleted_at: str = Field(nullable=True)   # enables soft-delete

await connector.delete_by_id(Post, 1)  # sets deleted_at, does not DELETE
await connector.find(Post, {})         # auto-filters WHERE deleted_at IS NULL
```

## License

MIT
