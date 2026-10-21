"""
Full-feature smoke test — seeds a large local dataset and exercises every
feature built into this connector against it, reporting pass/fail per
feature at the end.

Run against a real (disposable) Postgres instance:

    DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=postgres DB_PASSWORD=secret DB_NAME=smoketest \
        python examples/full_feature_smoketest.py
"""

import asyncio
import os
import random
import time

from postgres_connector import (
    Field,
    PostgreSQLConnector,
    PostgreSQLModel,
    Relation,
)
from postgres_connector.exceptions import QueryTimeoutError, UniqueConstraintError
from postgres_connector.registry import registry

N_CUSTOMERS = 5_000
N_ORDERS = 20_000

results: list = []  # (feature_name, ok: bool, detail: str)


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}{' - ' + detail if detail else ''}")


class Customer(PostgreSQLModel):
    __table__ = "smoke_customers"
    id:         int = Field(primary_key=True)
    email:      str = Field(unique=True)
    name:       str = Field(max_length=100)
    created_at: str = Field(auto_now_add=True)


class Order(PostgreSQLModel):
    __table__ = "smoke_orders"
    id:          int   = Field(primary_key=True)
    customer_id: int   = Field(foreign_key="smoke_customers.id")
    status:      str   = Field(default="pending")
    total:       float = Field()
    customer:    "Customer" = Relation(type="belongsTo", foreign_key="customer_id")


async def seed(connector):
    print(f"\n== Seeding {N_CUSTOMERS} customers, {N_ORDERS} orders ==")
    t0 = time.monotonic()

    for start in range(0, N_CUSTOMERS, 1000):
        batch = [
            {"email": f"customer{i}@example.com", "name": f"Customer {i}"}
            for i in range(start, min(start + 1000, N_CUSTOMERS))
        ]
        await connector.create_many(Customer, batch)
    customer_ids = [r["id"] for r in await connector.raw("SELECT id FROM smoke_customers")]
    check("create_many: bulk customer insert", len(customer_ids) == N_CUSTOMERS, f"{len(customer_ids)} rows")

    for start in range(0, N_ORDERS, 2000):
        batch = [
            {
                "customer_id": random.choice(customer_ids),
                "status": "pending",
                "total": round(random.uniform(5, 500), 2),
            }
            for _ in range(min(2000, N_ORDERS - start))
        ]
        await connector.create_many(Order, batch)
    order_count = await connector.count(Order)
    check("create_many: bulk order insert", order_count == N_ORDERS, f"{order_count} rows")

    print(f"  seeded in {time.monotonic() - t0:.1f}s")
    return customer_ids


async def check_filter_dsl(connector):
    print("\n== Filter DSL over the large dataset ==")
    expensive = await connector.find(Order, {
        "where": {"total": {"gte": 400}},
        "order": ["total DESC"],
        "limit": 10,
    })
    check("filter DSL: where/order/limit", len(expensive) == 10 and all(o.total >= 400 for o in expensive))

    like_match = await connector.find(Customer, {
        "where": {"email": {"like": "customer1%@example.com"}},
        "limit": 5,
    })
    check("filter DSL: like operator", len(like_match) == 5)


async def check_zero_n_plus_1(connector):
    print("\n== Zero N+1 relation loading ==")
    query_count = {"n": 0}
    real_log = connector._pool.log_query

    def counting_log_query(sql, params=None):
        query_count["n"] += 1
        real_log(sql, params)

    connector._pool.log_query = counting_log_query
    connector._pool._echo = True
    try:
        orders = await connector.find(Order, {"limit": 5000, "include": ["customer"]})
    finally:
        connector._pool.log_query = real_log
        connector._pool._echo = False

    all_loaded = all(o.customer is not None for o in orders)
    # One query for orders, one batched IN(...) query for customers — not 5000.
    check(
        "zero N+1: find(include=['customer']) on 5000 rows",
        len(orders) == 5000 and all_loaded and query_count["n"] <= 3,
        f"{query_count['n']} SQL statements for 5000 rows",
    )


async def check_upsert(connector, customer_ids):
    print("\n== Upsert at scale ==")
    existing_email = "customer0@example.com"
    updated = await connector.upsert(
        Customer,
        {"email": existing_email, "name": "Renamed via upsert"},
        conflict_target=["email"],
    )
    check("upsert: update branch on existing row", updated is not None and updated.name == "Renamed via upsert")

    created = await connector.upsert(
        Customer,
        {"email": "brand-new-upsert@example.com", "name": "New"},
        conflict_target=["email"],
    )
    check("upsert: insert branch for a new row", created is not None and created.id not in customer_ids)


async def check_transactions(connector, customer_ids):
    print("\n== Transactions (begin / savepoints) ==")

    cid = customer_ids[0]

    async with connector.begin() as tx:
        order = await tx.find_one(Order, {"where": {"customer_id": cid}})
        await tx.update_by_id(Order, order.id, {"status": "processing"})
    order_id = order.id
    refreshed = await connector.find_by_id(Order, order_id)
    check("begin(): commits on success", refreshed.status == "processing")

    try:
        async with connector.begin() as tx:
            await tx.update_by_id(Order, order_id, {"status": "should_not_stick"})
            raise RuntimeError("simulated failure")
    except RuntimeError:
        pass
    refreshed = await connector.find_by_id(Order, order_id)
    check("begin(): rolls back on exception", refreshed.status == "processing")


async def check_delete(connector):
    print("\n== Delete at scale ==")
    # Soft delete is intentionally NOT a connector feature — it's
    # application policy. This connector's delete_by_id/delete_all are
    # always real deletes; an app that wants "soft delete" implements it
    # itself with a plain column + its own update_all()/where filters.
    to_delete = await connector.find(Order, {"where": {"status": "pending"}, "limit": 100})
    ids = [o.id for o in to_delete]
    for oid in ids:
        await connector.delete_by_id(Order, oid)

    still_in_db = await connector.raw("SELECT COUNT(*) AS n FROM smoke_orders WHERE id = ANY($1)", [ids])
    check("delete_by_id: removes the row for real", still_in_db[0]["n"] == 0)

    matching_before = await connector.count(Order, {"total": {"lt": 10}})
    deleted_count = await connector.delete_all(Order, {"total": {"lt": 10}})
    matching_after = await connector.count(Order, {"total": {"lt": 10}})
    check(
        "delete_all: removes all matching rows for real",
        deleted_count == matching_before and matching_after == 0,
        f"deleted {deleted_count} rows",
    )


async def check_exception_mapping(connector):
    print("\n== Typed exception mapping ==")
    try:
        await connector.create(Customer, {"email": "customer0@example.com", "name": "dup"})
        check("UniqueConstraintError on real duplicate insert", False, "did not raise")
    except UniqueConstraintError as exc:
        check("UniqueConstraintError on real duplicate insert", exc.table == "smoke_customers")


async def check_timeouts(connector):
    print("\n== Query timeouts ==")
    try:
        await connector.raw("SELECT pg_sleep(2), COUNT(*) FROM smoke_orders", timeout=0.3)
        check("QueryTimeoutError on a slow query", False, "did not raise")
    except QueryTimeoutError as exc:
        check("QueryTimeoutError on a slow query", exc.timeout == 0.3)


async def main():
    connector = PostgreSQLConnector(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=os.getenv("DB_NAME", "smoketest"),
    )
    await connector.connect()
    registry.register(Customer)
    registry.register(Order)
    await connector.migrate()

    try:
        customer_ids = await seed(connector)
        await check_filter_dsl(connector)
        await check_zero_n_plus_1(connector)
        await check_upsert(connector, customer_ids)
        await check_transactions(connector, customer_ids)
        await check_delete(connector)
        await check_exception_mapping(connector)
        await check_timeouts(connector)
    finally:
        print("\n== Cleanup ==")
        await connector.raw_execute("DROP TABLE IF EXISTS smoke_orders")
        await connector.raw_execute("DROP TABLE IF EXISTS smoke_customers")
        await connector.disconnect()

    print("\n" + "=" * 60)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = [n for n, ok, _ in results if not ok]
    print(f"RESULT: {passed}/{len(results)} feature checks passed")
    if failed:
        print("FAILED:")
        for name in failed:
            print(f"  - {name}")
        raise SystemExit(1)
    print("Every feature verified against a real, large local dataset. ✅")


if __name__ == "__main__":
    asyncio.run(main())
