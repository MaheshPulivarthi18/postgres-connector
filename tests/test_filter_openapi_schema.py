"""FilterParam documents ?filter= with a per-model JSON schema and example in OpenAPI (#70)."""
from typing import List

import pytest

fastapi = pytest.importorskip("fastapi")

from fastapi import Depends  # noqa: E402

from postgres_connector import Field, FilterOptions, PostgreSQLModel, Relation  # noqa: E402
from postgres_connector.fastapi import FilterParam, filter_example, filter_json_schema, parse_filter  # noqa: E402


class SchUser(PostgreSQLModel):
    __table__ = "sch_users"
    id:     int = Field(primary_key=True)
    name:   str = Field()
    age:    int = Field(nullable=True)
    orders: List["SchOrder"] = Relation(type="hasMany", foreign_key="user_id")


class SchOrder(PostgreSQLModel):
    __table__ = "sch_orders"
    id:      int = Field(primary_key=True)
    user_id: int = Field()


def _param(model, **kwargs):
    app = fastapi.FastAPI()

    @app.get("/items")
    async def items(filter: FilterOptions = Depends(FilterParam(model, **kwargs))):
        return []

    [param] = app.openapi()["paths"]["/items"]["get"]["parameters"]
    return param


def test_openapi_parameter_carries_the_model_schema():
    param = _param(SchUser, max_limit=50)
    schema = param["schema"]
    assert schema["contentMediaType"] == "application/json"
    content = schema["contentSchema"]
    assert content["title"] == "SchUserFilter"
    assert content["properties"]["fields"]["oneOf"][0]["items"]["enum"] == ["id", "name", "age"]
    assert content["properties"]["include"]["items"]["oneOf"][1]["properties"]["relation"]["enum"] == ["orders"]
    assert content["properties"]["limit"] == {"type": "integer", "minimum": 0, "maximum": 50}
    assert "LoopBack-style filter for SchUser" in param["description"]
    assert "Relations: orders." in param["description"]


def test_example_uses_the_models_own_fields_and_parses():
    example = filter_example(SchUser, max_limit=10)
    assert example == {"where": {"name": "text"}, "order": ["name ASC"], "limit": 10,
                       "fields": ["id", "name"], "include": ["orders"]}
    import json
    parse_filter(SchUser, json.dumps(example), max_limit=10)   # a usable example, not just text
    assert _param(SchUser)["examples"]["example"]["value"] == json.dumps(filter_example(SchUser, 100))


def test_model_without_relations_has_no_include():
    assert "include" not in filter_json_schema(SchOrder)["properties"]
    assert "include" not in filter_example(SchOrder)


jsonschema = pytest.importorskip("jsonschema")


@pytest.mark.parametrize("flt", [
    {},
    {"where": {"name": "raj", "and": [{"age": {"gt": 1}}]}, "order": "name DESC", "limit": 5, "skip": 0},
    {"fields": {"age": False}, "order": ["id", "age asc"], "include": ["orders", {"relation": "orders", "scope": {"limit": 2}}]},
    {"include": ["orders.items"]},
])
def test_valid_filters_match_the_schema(flt):
    jsonschema.validate(flt, filter_json_schema(SchUser, max_limit=50))


@pytest.mark.parametrize("flt", [
    {"fields": ["nope"]},
    {"where": {"nope": 1}},
    {"order": ["nope DESC"]},
    {"include": ["invoices"]},
    {"limit": 51},
    {"wher": {}},
])
def test_invalid_filters_do_not_match_the_schema(flt):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(flt, filter_json_schema(SchUser, max_limit=50))
