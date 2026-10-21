# Benchmarks: postgres_connector vs SQLAlchemy

`bench_vs_sqlalchemy.py` compares this library with the **SQLAlchemy 2.x async ORM**. `core_calibration.py` compares it with **SQLAlchemy Core**, which skips the ORM's object tracking.

## How it's measured

- **Same everything except the library:** both use the asyncpg driver against the same PostgreSQL database, with the same tables and indexes (created once by `migrate()`) and the same data. The data is 1,000 users, 10,000 posts, 30,000 comments, 50 tags and 20,000 post–tag links.
- **Idiomatic code on each side:** for example, `selectinload()` chains for SQLAlchemy includes, `UPDATE … RETURNING` for updates, and `on_conflict_do_update` for upserts.
- **Interleaved runs:** iterations alternate between the two libraries, after a warm-up, to cancel out drift.
- **Reported:** median and p95 milliseconds per call, plus the number of SQL statements each call sends. Statements are counted from `ConnectionPool.log_query` on our side and SQLAlchemy's `before_cursor_execute` event on theirs; `BEGIN`/`COMMIT` aren't counted on either side.

## Results

PostgreSQL 16.15 on the same machine (4 vCPU), Python 3.13.16, SQLAlchemy 2.1.5, asyncpg 0.32.0, postgres_connector 2.0.0. Times are medians from the latest run; the last column shows the speed-up range across three full runs.

| Operation | Ours | SQLAlchemy ORM | Statements (ours / SQLA) | Ours faster by (3 runs) |
|---|---|---|---|---|
| Create one row | 1.9 ms | 3.3 ms | 1 / 1 | 1.55–1.70× |
| Bulk insert 1,000 rows | 23.8 ms | 50.8 ms | 1 / 1 | 2.05–2.14× |
| Find: where + order + limit 50 | 10.7 ms | 11.9 ms | 1 / 1 | 1.11–1.12× |
| `find_by_id` | 0.96 ms | 1.87 ms | 1 / 1 | 1.95–2.05× |
| `update_by_id` (returning the row) | 1.8 ms | 3.2 ms | 1 / 1 | 1.73–1.82× |
| `delete_by_id` | 1.6 ms | 2.7 ms | 1 / 1 | 1.63–1.75× |
| 100 users + posts → comments, tags, author | 23.4 ms | 61.4 ms | 6 / 7 | 2.53–2.62× |
| Upsert by email | 1.6 ms | 3.1 ms | 1 / 1 | 1.84–1.97× |
| Transaction: 3 inserts + 1 update | 3.8 ms | 5.2 ms | 4 / 3 | 1.27–1.42× |

**Against SQLAlchemy Core** (`core_calibration.py`, four runs). These runs time one library and then the other instead of alternating, so they're noisier:

| Operation | Ours faster by |
|---|---|
| `find_by_id` | 2.2–4.7× |
| `update_by_id` | 1.2–1.9× |
| Bulk insert 1,000 rows | **0.78–0.91×: SQLAlchemy Core is faster** |

## Reading the numbers honestly

- **Why it's faster than the ORM:** it doesn't keep an identity map or a unit of work, and it calls asyncpg directly instead of going through SQLAlchemy's async bridge. For nested includes, each relation level is one query that passes all parent ids as a single array (`= ANY($1)`) rather than a long `IN (...)` list of placeholders. Part of the speed comes from doing less: SQLAlchemy's change tracking and lazy loading cost time, but they are capabilities this library doesn't have.
- **Where it isn't faster:**
  - SQLAlchemy Core wins bulk inserts.
  - Queries where the database does most of the work come out about even.
  - SQLAlchemy batches the two post inserts in the transaction into one statement.
- **Network latency shrinks the gaps.** These runs used a database on the same machine; against a database 1–5 ms away, the per-call overhead measured here matters less.
- **p95 for the nested include is noisy on both sides**, around 110 ms vs 150 ms, likely garbage-collection pauses from building about 13,000 objects per call.

## Run it yourself

```bash
pip install -e ".[bench]"
createdb bench            # or set DB_NAME
cd benchmarks
DB_PASSWORD=secret python bench_vs_sqlalchemy.py results.json
DB_PASSWORD=secret python core_calibration.py
```

Connection settings come from `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` and `DB_NAME` (default `bench`). The scripts drop and recreate their own `b_*` tables.
