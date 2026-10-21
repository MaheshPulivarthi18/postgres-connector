"""Filter-shape normalisation tests (no database): _dict_to_filter feeds every
find()/include scope, and API clients send LoopBack filters as JSON where
`fields` is an object, `order` a string and `limit` a numeric string (#40)."""
import pytest

from postgres_connector.exceptions import QueryError
from postgres_connector.query_builder import QueryBuilder
from postgres_connector.relation_resolver import _dict_to_filter


def _qb() -> QueryBuilder:
    fields = {
        "id":   {"name": "id", "type": int, "primary_key": True},
        "name": {"name": "name", "type": str},
        "age":  {"name": "age", "type": int},
    }
    return QueryBuilder("p_users", fields)


def test_fields_object_selects_true_keys_only():
    opts = _dict_to_filter({"fields": {"name": True, "age": False}})
    assert opts.fields == ["name"]
    sql, params = _qb().build_select(opts)
    assert '"p_users"."name"' in sql
    assert '"p_users"."age"' not in sql


def test_fields_object_accepts_misleading_shapes():
    assert _dict_to_filter({"fields": {"name": 1, "age": 0}}).fields == ["name"]
    assert _dict_to_filter({"fields": ["name", "age"]}).fields == ["name", "age"]
    assert _dict_to_filter({}).fields is None


def test_fields_object_with_no_true_key_is_refused():
    with pytest.raises(QueryError, match="'fields' object selects no field"):
        _dict_to_filter({"fields": {"name": False, "age": False}})


def test_fields_of_an_unsupported_shape_is_refused():
    with pytest.raises(QueryError, match="'fields' must be a list"):
        _dict_to_filter({"fields": "name"})


def test_order_string_counts_as_one_entry():
    opts = _dict_to_filter({"order": "name DESC"})
    assert opts.order == ["name DESC"]
    sql, params = _qb().build_select(opts)
    assert "ORDER BY" in sql and "name" in sql


def test_order_list_is_unchanged():
    opts = _dict_to_filter({"order": ["name DESC", "age ASC"]})
    assert opts.order == ["name DESC", "age ASC"]


def test_limit_and_skip_accept_integer_strings():
    opts = _dict_to_filter({"limit": "10", "skip": "5"})
    assert opts.limit == 10 and opts.skip == 5
    assert _dict_to_filter({"limit": 3}).limit == 3


@pytest.mark.parametrize("name", ["limit", "skip"])
def test_limit_and_skip_refuse_non_integer_strings(name):
    with pytest.raises(QueryError, match=f"'{name}' must be a non-negative integer"):
        _dict_to_filter({name: "ten"})


@pytest.mark.parametrize("name", ["limit", "skip"])
def test_limit_and_skip_refuse_negative_values(name):
    with pytest.raises(QueryError, match=f"'{name}' must be a non-negative integer"):
        _dict_to_filter({name: -1})


@pytest.mark.parametrize("name", ["limit", "skip"])
def test_limit_and_skip_refuse_non_numbers(name):
    with pytest.raises(QueryError, match=f"'{name}' must be a non-negative integer"):
        _dict_to_filter({name: [1]})


def test_unknown_keys_are_still_rejected():
    with pytest.raises(QueryError, match="Unknown filter key"):
        _dict_to_filter({"wher": {"id": 1}})
