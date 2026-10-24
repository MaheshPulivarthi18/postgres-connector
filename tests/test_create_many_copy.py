"""create_many(returning=False): COPY for uniform batches, INSERT without RETURNING otherwise (#69)."""
from datetime import datetime

import pytest

from postgres_connector import Field, JSONField, PostgreSQLModel
from postgres_connector.exceptions import QueryError, UniqueConstraintError


class CopyEvent(PostgreSQLModel):
    __table__ = "copy_events"
    id:         int   = Field(primary_key=True)
    code:       str   = Field(unique=True)
    kind:       str   = Field(default="click")
    amount:     float = Field(nullable=True)
    meta:       dict  = JSONField(default={})
    created_at: str   = Field(auto_now_add=True)


@pytest.fixture
async def table(connector):
    await connector.raw_execute("DROP TABLE IF EXISTS copy_events")
    await connector.migrate()
    try:
        yield
    finally:
        await connector.raw_execute("DROP TABLE IF EXISTS copy_events")


def _logged(connector, monkeypatch):
    sent = []
    monkeypatch.setattr(connector._pool, "log_query", lambda sql, params=None: sent.append(sql))
    return sent


async def test_uniform_batch_goes_through_copy(connector, table, monkeypatch):
    sent = _logged(connector, monkeypatch)
    count = await connector.create_many(
        CopyEvent, [{"code": f"c{i}", "amount": i * 0.5, "meta": {"i": i}} for i in range(3)], returning=False
    )
    assert count == 3
    assert [s.split()[0] for s in sent] == ["COPY"]
    rows = await connector.find(CopyEvent, {"order": ["id ASC"]})
    assert [(r.code, r.kind, r.amount, r.meta) for r in rows] == [
        ("c0", "click", 0.0, {"i": 0}), ("c1", "click", 0.5, {"i": 1}), ("c2", "click", 1.0, {"i": 2}),
    ]
    assert all(isinstance(r.created_at, datetime) for r in rows)


async def test_mixed_columns_fall_back_to_insert_with_defaults(connector, table, monkeypatch):
    sent = _logged(connector, monkeypatch)
    count = await connector.create_many(CopyEvent, [{"code": "a", "amount": 1.0}, {"code": "b"}], returning=False)
    assert count == 2
    assert [s.split()[0] for s in sent] == ["INSERT"] and "RETURNING" not in sent[0]
    assert {r.code: r.amount for r in await connector.find(CopyEvent, {})} == {"a": 1.0, "b": None}


async def test_large_batch_beyond_the_argument_limit(connector, table):
    assert await connector.create_many(CopyEvent, [{"code": f"c{i}"} for i in range(40000)], returning=False) == 40000
    assert await connector.count(CopyEvent) == 40000


async def test_returning_default_is_unchanged(connector, table):
    created = await connector.create_many(CopyEvent, [{"code": "x"}])
    assert [type(c) for c in created] == [CopyEvent] and created[0].id == 1
    assert await connector.create_many(CopyEvent, [], returning=False) == 0


async def test_copy_errors_are_typed_and_atomic(connector, table):
    await connector.create(CopyEvent, {"code": "dup"})
    with pytest.raises(UniqueConstraintError):
        await connector.create_many(CopyEvent, [{"code": "new"}, {"code": "dup"}], returning=False)
    assert await connector.count(CopyEvent) == 1
    with pytest.raises(QueryError, match="does not exist"):
        await connector.create_many(CopyEvent, [{"code": "z", "nope": 1}], returning=False)


async def test_copy_inside_a_transaction_rolls_back_with_it(connector, table):
    with pytest.raises(RuntimeError):
        async with connector.begin() as tx:
            assert await tx.create_many(CopyEvent, [{"code": "t1"}, {"code": "t2"}], returning=False) == 2
            assert await tx.count(CopyEvent) == 2
            raise RuntimeError("abort")
    assert await connector.count(CopyEvent) == 0
