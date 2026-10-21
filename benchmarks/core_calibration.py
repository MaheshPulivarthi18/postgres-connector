"""Calibration: same ops via SQLAlchemy Core (no ORM identity map / unit of work)."""
import asyncio, statistics, time
from sqlalchemy import insert, select, update
import bench_vs_sqlalchemy as B

async def timeit(fn, n, warm=10):
    for i in range(warm): await fn(i)
    t = []
    for i in range(warm, warm + n):
        t0 = time.perf_counter(); await fn(i); t.append(time.perf_counter() - t0)
    return statistics.median(t) * 1000

async def main():
    pc, engine, S, ids = await B.setup()
    U = B.SUser.__table__
    async def ours_get(i): await pc.find_by_id(B.BUser, ids[i % len(ids)])
    async def core_get(i):
        async with engine.connect() as c:
            (await c.execute(select(U).where(U.c.id == ids[i % len(ids)]))).first()
    async def ours_bulk(i): await pc.create_many(B.BUser, [dict(name="b", email=f"cb-o{i}-{k}@x.io", age=40, active=True) for k in range(1000)])
    async def core_bulk(i):
        async with engine.begin() as c:
            await c.execute(insert(U), [dict(name="b", email=f"cb-s{i}-{k}@x.io", age=40, active=True) for k in range(1000)])
    async def ours_upd(i): await pc.update_by_id(B.BUser, ids[i % len(ids)], {"name": f"o{i}"})
    async def core_upd(i):
        async with engine.begin() as c:
            (await c.execute(update(U).where(U.c.id == ids[i % len(ids)]).values(name=f"c{i}").returning(U))).first()
    for name, o, c, n in (("find_by_id", ours_get, core_get, 500), ("bulk insert 1,000", ours_bulk, core_bulk, 30), ("update_by_id", ours_upd, core_upd, 300)):
        om, cm = await timeit(o, n), await timeit(c, n)
        print(f"{name:<20} ours {om:7.3f} ms | sqlalchemy CORE {cm:7.3f} ms | x{cm/om:.2f}")
    await pc.disconnect(); await engine.dispose()
asyncio.run(main())
