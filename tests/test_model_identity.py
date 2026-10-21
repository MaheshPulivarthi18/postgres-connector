"""Equality and hashing of model instances: by primary key once saved, by identity before."""
from postgres_connector import Field, PostgreSQLModel


class IdUser(PostgreSQLModel):
    __table__ = "id_users"
    id:   int = Field(primary_key=True)
    name: str = Field()


def test_unsaved_instances_compare_by_identity():
    p, q = IdUser(name="p"), IdUser(name="q")
    assert p != q
    assert p == p
    assert len({p, q}) == 2


def test_unsaved_instance_never_equals_a_saved_one():
    assert IdUser(name="new") != IdUser(id=1, name="saved")


def test_saved_instances_compare_by_primary_key():
    a, b = IdUser(id=7, name="a"), IdUser(id=7, name="b")
    assert a == b
    assert len({a, b}) == 1
    assert IdUser(id=7, name="a") != IdUser(id=8, name="a")
