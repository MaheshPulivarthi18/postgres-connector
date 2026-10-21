"""
Benchmark: oceanic-postgres-orm (postgres_connector) vs SQLAlchemy 2.x async ORM.

Both use asyncpg against the same PostgreSQL database, the same tables
(created once by postgres_connector's migrate(), plus FK indexes), and the
same seeded data. Each operation is written the idiomatic way for each
library. Iterations alternate between the two libraries to cancel drift.

Reported per operation: median and p95 latency per call, and the number of
SQL statements each library sends per call (postgres_connector: every
statement passes through ConnectionPool.log_query; SQLAlchemy: the
before_cursor_execute event). BEGIN/COMMIT are not counted on either side.
"""
import asyncio
import json
import os
import statistics
import sys
import time
from typing import List

from sqlalchemy import ForeignKey, String, delete, event, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload

from postgres_connector import Field, PostgreSQLConnector, PostgreSQLModel, Relation

DB = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"), port=int(os.getenv("DB_PORT", "5432")),
    user=os.getenv("DB_USER", "postgres"), password=os.getenv("DB_PASSWORD", "secret"),
    database=os.getenv("DB_NAME", "bench"),
)
N_USERS, POSTS_PER_USER, COMMENTS_PER_POST, N_TAGS, TAGS_PER_POST = 1000, 10, 3, 50, 2
POOL = 10


# ── postgres_connector models ───────────────────────────────────────────────
class BUser(PostgreSQLModel):
    __table__ = "b_users"
    id: int = Field(primary_key=True)
    name: str = Field(max_length=100)
    email: str = Field(max_length=200, unique=True)
    age: int = Field(index=True)
    active: bool = Field(default=True)
    posts: List["BPost"] = Relation(type="hasMany", foreign_key="user_id")


class BPost(PostgreSQLModel):
    __table__ = "b_posts"
    id: int = Field(primary_key=True)
    user_id: int = Field(index=True)
    title: str = Field(max_length=200)
    score: int = Field()
    author: "BUser" = Relation(type="belongsTo", foreign_key="user_id")
    comments: List["BComment"] = Relation(type="hasMany", foreign_key="post_id")
    tags: List["BTag"] = Relation(type="hasManyThrough", through="BPostTag", foreign_key="post_id", other_key="tag_id")


class BComment(PostgreSQLModel):
    __table__ = "b_comments"
    id: int = Field(primary_key=True)
    post_id: int = Field(index=True)
    body: str = Field(max_length=200)


class BTag(PostgreSQLModel):
    __table__ = "b_tags"
    id: int = Field(primary_key=True)
    name: str = Field(max_length=50)


class BPostTag(PostgreSQLModel):
    __table__ = "b_post_tags"
    id: int = Field(primary_key=True)
    post_id: int = Field(index=True)
    tag_id: int = Field(index=True)


# ── SQLAlchemy mappings of the same tables ──────────────────────────────────
class Base(DeclarativeBase):
    pass


class SUser(Base):
    __tablename__ = "b_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    email: Mapped[str] = mapped_column(String(200), unique=True)
    age: Mapped[int]
    active: Mapped[bool]
    posts: Mapped[List["SPost"]] = relationship(back_populates="author")


class SPostTag(Base):
    __tablename__ = "b_post_tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("b_posts.id"))
    tag_id: Mapped[int] = mapped_column(ForeignKey("b_tags.id"))


class SPost(Base):
    __tablename__ = "b_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("b_users.id"))
    title: Mapped[str] = mapped_column(String(200))
    score: Mapped[int]
    author: Mapped["SUser"] = relationship(back_populates="posts")
    comments: Mapped[List["SComment"]] = relationship()
    tags: Mapped[List["STag"]] = relationship(secondary="b_post_tags", viewonly=True)


class SComment(Base):
    __tablename__ = "b_comments"
    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("b_posts.id"))
    body: Mapped[str] = mapped_column(String(200))


class STag(Base):
    __tablename__ = "b_tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50))


# ── harness ─────────────────────────────────────────────────────────────────
class Counter:
    n = 0


async def setup():
    pc = PostgreSQLConnector(**DB, minsize=POOL, maxsize=POOL)
    await pc.connect()
    for t in ("b_post_tags", "b_comments", "b_tags", "b_posts", "b_users"):
        await pc.raw_execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    await pc.migrate()

    users = [{"name": f"user{i}", "email": f"u{i}@x.io", "age": 18 + i % 50, "active": i % 3 != 0} for i in range(N_USERS)]
    for i in range(0, N_USERS, 500):
        await pc.create_many(BUser, users[i:i + 500])
    user_ids = [r["id"] for r in await pc.raw('SELECT id FROM b_users ORDER BY id')]
    posts = [{"user_id": u, "title": f"post {u}-{k}", "score": k} for u in user_ids for k in range(POSTS_PER_USER)]
    for i in range(0, len(posts), 2000):
        await pc.create_many(BPost, posts[i:i + 2000])
    post_ids = [r["id"] for r in await pc.raw('SELECT id FROM b_posts ORDER BY id')]
    comments = [{"post_id": p, "body": f"c{k}"} for p in post_ids for k in range(COMMENTS_PER_POST)]
    for i in range(0, len(comments), 3000):
        await pc.create_many(BComment, comments[i:i + 3000])
    await pc.create_many(BTag, [{"name": f"tag{i}"} for i in range(N_TAGS)])
    tag_ids = [r["id"] for r in await pc.raw('SELECT id FROM b_tags ORDER BY id')]
    links = [{"post_id": p, "tag_id": tag_ids[(j * 7 + k) % N_TAGS]} for j, p in enumerate(post_ids) for k in range(TAGS_PER_POST)]
    for i in range(0, len(links), 3000):
        await pc.create_many(BPostTag, links[i:i + 3000])
    await pc.raw_execute("ANALYZE")

    real_log = pc._pool.log_query

    def counting_log(sql, params=None):
        Counter.n += 1
        return real_log(sql, params)

    pc._pool.log_query = counting_log

    engine = create_async_engine(
        f"postgresql+asyncpg://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['database']}",
        pool_size=POOL, max_overflow=0,
    )
    event.listen(engine.sync_engine, "before_cursor_execute", lambda *a, **k: setattr(Counter, "n", Counter.n + 1))
    S = async_sessionmaker(engine, expire_on_commit=False)
    return pc, engine, S, user_ids


def summarize(samples):
    s = sorted(samples)
    return statistics.median(s) * 1000, s[max(0, int(len(s) * 0.95) - 1)] * 1000


async def bench(name, ours, theirs, iters, warmup=10, results=None):
    for i in range(warmup):
        await ours(i)
        await theirs(i)
    t_ours, t_theirs, q_ours, q_theirs = [], [], [], []
    for i in range(warmup, warmup + iters):
        for fn, times, queries in ((ours, t_ours, q_ours), (theirs, t_theirs, q_theirs)):
            Counter.n = 0
            t0 = time.perf_counter()
            await fn(i)
            times.append(time.perf_counter() - t0)
            queries.append(Counter.n)
    (om, op), (sm, sp) = summarize(t_ours), summarize(t_theirs)
    row = dict(op=name, iters=iters,
               ours_median_ms=round(om, 3), ours_p95_ms=round(op, 3), ours_queries=statistics.median(q_ours),
               sqla_median_ms=round(sm, 3), sqla_p95_ms=round(sp, 3), sqla_queries=statistics.median(q_theirs),
               speedup=round(sm / om, 2))
    results.append(row)
    print(f"{name:<44} ours {om:8.3f} ms (p95 {op:8.3f}, {row['ours_queries']:g} q) | "
          f"sqla {sm:8.3f} ms (p95 {sp:8.3f}, {row['sqla_queries']:g} q) | x{row['speedup']}", flush=True)


async def main():
    pc, engine, S, user_ids = await setup()
    results = []

    # 1. single insert
    async def o1(i):
        await pc.create(BUser, {"name": "n", "email": f"o1-{i}@x.io", "age": 30, "active": True})

    async def s1(i):
        async with S() as s:
            s.add(SUser(name="n", email=f"s1-{i}@x.io", age=30, active=True))
            await s.commit()
    await bench("create one row", o1, s1, 300, results=results)

    # 2. bulk insert 1,000 rows
    def rows(prefix, i):
        return [dict(name="b", email=f"{prefix}{i}-{k}@x.io", age=40, active=True) for k in range(1000)]

    async def o2(i):
        await pc.create_many(BUser, rows("o2-", i))

    async def s2(i):
        async with S() as s:
            s.add_all([SUser(**r) for r in rows("s2-", i)])
            await s.commit()
    await bench("bulk insert 1,000 rows", o2, s2, 30, warmup=3, results=results)

    # 3. filtered list
    async def o3(i):
        await pc.find(BUser, {"where": {"active": True, "age": {"gte": 30}}, "order": ["name ASC"], "limit": 50})

    async def s3(i):
        async with S() as s:
            (await s.scalars(select(SUser).where(SUser.active.is_(True), SUser.age >= 30)
                             .order_by(SUser.name).limit(50))).all()
    await bench("find: where + order + limit 50", o3, s3, 300, results=results)

    # 4. by primary key
    async def o4(i):
        await pc.find_by_id(BUser, user_ids[i % N_USERS])

    async def s4(i):
        async with S() as s:
            await s.get(SUser, user_ids[i % N_USERS])
    await bench("find_by_id", o4, s4, 500, results=results)

    # 5. update by id, returning the row
    async def o5(i):
        await pc.update_by_id(BUser, user_ids[i % N_USERS], {"name": f"o{i}"})

    async def s5(i):
        async with S() as s:
            (await s.scalars(update(SUser).where(SUser.id == user_ids[i % N_USERS])
                             .values(name=f"s{i}").returning(SUser))).one()
            await s.commit()
    await bench("update_by_id (returning row)", o5, s5, 300, results=results)

    # 6. delete by id (fresh rows)
    del_ids = [r["id"] for r in await pc.raw(
        "INSERT INTO b_users (name, email, age, active) "
        "SELECT 'd', 'del' || g || '@x.io', 20, true FROM generate_series(1, 1240) g RETURNING id")]
    it_o, it_s = iter(del_ids[0::2]), iter(del_ids[1::2])

    async def o6(i):
        await pc.delete_by_id(BUser, next(it_o))

    async def s6(i):
        async with S() as s:
            await s.execute(delete(SUser).where(SUser.id == next(it_s)))
            await s.commit()
    await bench("delete_by_id", o6, s6, 600, results=results)

    # 7. nested includes (the ORM's main job)
    include = [{"relation": "posts", "scope": {"include": ["comments", "tags", "author"]}}]

    async def o7(i):
        users = await pc.find(BUser, {"order": ["id ASC"], "limit": 100, "include": include})
        assert len(users[0].posts) == POSTS_PER_USER and len(users[0].posts[0].comments) == COMMENTS_PER_POST

    async def s7(i):
        async with S() as s:
            users = (await s.scalars(select(SUser).order_by(SUser.id).limit(100).options(
                selectinload(SUser.posts).selectinload(SPost.comments),
                selectinload(SUser.posts).selectinload(SPost.tags),
                selectinload(SUser.posts).selectinload(SPost.author),
            ))).all()
            assert len(users[0].posts) == POSTS_PER_USER and len(users[0].posts[0].comments) == COMMENTS_PER_POST
    await bench("100 users + posts.{comments,tags,author}", o7, s7, 50, warmup=3, results=results)

    # 8. upsert on a unique column
    async def o8(i):
        await pc.upsert(BUser, {"email": f"up{i % 20}@x.io", "name": f"o{i}", "age": 33, "active": True},
                        conflict_target=["email"])

    async def s8(i):
        async with S() as s:
            stmt = pg_insert(SUser).values(email=f"up{i % 20}@x.io", name=f"s{i}", age=33, active=True)
            stmt = stmt.on_conflict_do_update(index_elements=["email"], set_={"name": stmt.excluded.name,
                                              "age": stmt.excluded.age, "active": stmt.excluded.active})
            (await s.scalars(stmt.returning(SUser))).one()
            await s.commit()
    await bench("upsert (insert or update by email)", o8, s8, 300, results=results)

    # 9. transaction: 1 user + 2 posts + 1 update, committed together
    async def o9(i):
        async with pc.begin() as tx:
            u = await tx.create(BUser, {"name": "t", "email": f"o9-{i}@x.io", "age": 25, "active": True})
            await tx.create(BPost, {"user_id": u.id, "title": "a", "score": 1})
            await tx.create(BPost, {"user_id": u.id, "title": "b", "score": 2})
            await tx.update_by_id(BUser, u.id, {"age": 26})

    async def s9(i):
        async with S() as s, s.begin():
            u = SUser(name="t", email=f"s9-{i}@x.io", age=25, active=True)
            s.add(u)
            await s.flush()
            s.add_all([SPost(user_id=u.id, title="a", score=1), SPost(user_id=u.id, title="b", score=2)])
            await s.flush()
            u.age = 26
    await bench("transaction: 3 inserts + 1 update", o9, s9, 200, results=results)

    await pc.disconnect()
    await engine.dispose()
    with open(sys.argv[1] if len(sys.argv) > 1 else "results.json", "w") as f:  # raw numbers
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
