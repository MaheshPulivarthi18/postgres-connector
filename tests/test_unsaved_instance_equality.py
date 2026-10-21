"""Unsaved instances (pk=None) must not collapse in sets/dicts.

Regression tests for the issue where ``__eq__``/``__hash__`` compared only
the primary key *field*, so two different unsaved instances with ``pk=None``
compared equal and hashed identically.
"""

import pytest

from postgres_connector import Field, PostgreSQLModel


class EqUser(PostgreSQLModel):
    __table__ = "eq_users"
    id:   int = Field(primary_key=True)
    name: str = Field()


def test_two_unsaved_instances_are_not_equal():
    p = EqUser(name="p")
    q = EqUser(name="q")
    assert (p == q) is False
    assert (q == p) is False


def test_unsaved_instances_do_not_collapse_in_a_set():
    p = EqUser(name="p")
    q = EqUser(name="q")
    assert len({p, q}) == 2
    assert len({p, EqUser(name="r"), EqUser(name="s")}) == 3


def test_unsaved_instance_collapses_only_with_itself():
    p = EqUser(name="p")
    assert len({p, p}) == 1
    assert (p == p) is True


def test_saved_instances_with_same_pk_still_equal():
    p = EqUser(name="p")
    q = EqUser(name="q")
    p.__dict__["id"] = 1
    q.__dict__["id"] = 1
    assert (p == q) is True
    assert len({p, q}) == 1


def test_saved_instance_and_unsaved_instance_not_equal():
    p = EqUser(name="p")
    q = EqUser(name="q")
    p.__dict__["id"] = 1
    assert (p == q) is False
    assert (q == p) is False


def test_hash_differs_for_unsaved_instances():
    p = EqUser(name="p")
    q = EqUser(name="q")
    assert hash(p) != hash(q)


def test_hash_stable_for_saved_instance():
    p = EqUser(name="p")
    p.__dict__["id"] = 1
    assert hash(p) == hash((type(p).__name__, 1))


def test_eq_with_other_type():
    p = EqUser(name="p")
    assert (p == object()) is False
    assert (p == "not a model") is False


def test_eq_with_false_pk_of_other_model():
    class OtherModel(PostgreSQLModel):
        __table__ = "eq_others"
        id: int = Field(primary_key=True)
        name: str = Field()

    p = EqUser(name="p")
    o = OtherModel(name="o")
    assert (p == o) is False


def test_hash_is_usable_while_unsaved():
    p = EqUser(name="p")
    d = {}
    d[p] = "first"
    q = EqUser(name="q")
    d[q] = "second"
    assert d[p] == "first"
    assert d[q] == "second"
    assert len(d) == 2
