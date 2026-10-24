"""
FastAPI integration: HTTP responses for the library's typed errors, and a
`?filter=` query parameter for LoopBack-style REST querying.

    from fastapi import FastAPI
    from postgres_connector.fastapi import register_exception_handlers

    app = FastAPI()
    register_exception_handlers(app)

Without handlers, a duplicate email or a bad filter is an HTTP 500. With them:

    QueryError, InvalidValueError, ConfigurationError    400  bad client input
    UniqueConstraintError, ForeignKeyConstraintError      409  conflicts with existing data
    NotNullConstraintError, CheckConstraintError          422  a required or valid value is missing
    QueryTimeoutError                                     504  the database took too long

Every response body is `{"error": <class name>, "detail": <message>}`, plus
`constraint` / `table` / `column` when Postgres reported them. Postgres's
DETAIL text (`db_detail`) is only added with `expose_db_detail=True`, since
it quotes stored values.
Requires FastAPI (or Starlette), which this module imports lazily.
"""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Type

from .exceptions import (
    CheckConstraintError,
    ConfigurationError,
    ConnectorError,
    ForeignKeyConstraintError,
    IntegrityError,
    InvalidValueError,
    NotNullConstraintError,
    QueryError,
    QueryTimeoutError,
    RelationNotDefinedError,
    UniqueConstraintError,
)
from .query_builder import QueryBuilder
from .registry import registry
from .relation_resolver import _dict_to_filter, _parse_include
from .types import FilterOptions, IncludeScope

if TYPE_CHECKING:
    from .model import PostgreSQLModel

# Most specific first: a handler registered for a subclass wins over its base.
ERROR_STATUS: Tuple[Tuple[Type[ConnectorError], int], ...] = (
    (UniqueConstraintError, 409),
    (ForeignKeyConstraintError, 409),
    (NotNullConstraintError, 422),
    (CheckConstraintError, 422),
    (QueryTimeoutError, 504),
    (InvalidValueError, 400),
    (QueryError, 400),
    # Raised for payload values of the wrong type ("Field 'age' expects int,
    # got str") as well as for model misconfiguration, which surfaces at
    # import or migrate() time rather than inside a request.
    (ConfigurationError, 400),
)


# Set on app.state by register_exception_handlers(), so FilterParam can leave
# its 400s to those handlers and keep one error shape across the API.
_HANDLERS_FLAG = "postgres_connector_error_handlers"


def error_body(exc: ConnectorError, expose_db_detail: bool = False) -> Dict[str, Any]:
    """
    The JSON body for `exc`. Postgres's DETAIL text quotes stored values
    ("Key (email)=(raj@x.io) already exists"), so it is left out unless
    `expose_db_detail` is set; the message is cut to its first line for the
    same reason.
    """
    message = str(exc)
    if not expose_db_detail:
        message = message.split("\n", 1)[0]
    body: Dict[str, Any] = {"error": type(exc).__name__, "detail": message}
    if isinstance(exc, IntegrityError):
        for key in ("constraint", "table", "column"):
            value = getattr(exc, key)
            if value is not None:
                body[key] = value
    db_detail = getattr(exc, "detail", None)
    if expose_db_detail and db_detail is not None and isinstance(exc, (IntegrityError, InvalidValueError)):
        body["db_detail"] = db_detail
    return body


def register_exception_handlers(app: Any, expose_db_detail: bool = False) -> None:
    """
    Install one handler per error class on a FastAPI (or Starlette) app.
    `expose_db_detail=True` adds Postgres's DETAIL text (`db_detail`), which
    can quote stored values; keep it off for public APIs.
    """
    from starlette.responses import JSONResponse

    def handler_for(status: int) -> Any:
        async def handle(request: Any, exc: Exception) -> JSONResponse:
            assert isinstance(exc, ConnectorError)
            return JSONResponse(error_body(exc, expose_db_detail), status_code=status)

        return handle

    for error_cls, status in ERROR_STATUS:
        app.add_exception_handler(error_cls, handler_for(status))
    setattr(app.state, _HANDLERS_FLAG, True)


# ── ?filter= query parameter ─────────────────────────────────────────────────

_EXAMPLE_VALUES = {int: 1, float: 1.5, str: "text", bool: True}


def filter_json_schema(model: Type[PostgreSQLModel], max_limit: Optional[int] = None) -> Dict[str, Any]:
    """
    JSON Schema of a `filter` for `model`, like LoopBack's getFilterJsonSchemaFor():
    the model's field names for `fields` / `order` / `where` keys, its relation
    names for `include`, and `max_limit` on `limit`. It documents the
    parameter; parse_filter() is what enforces it.
    """
    fields = list(model._fields)
    relations = list(model._relations)
    order_item = {"type": "string", "pattern": f"^({'|'.join(fields)})( (ASC|DESC|asc|desc))?$"}
    limit: Dict[str, Any] = {"type": "integer", "minimum": 0}
    if max_limit is not None:
        limit["maximum"] = max_limit
    conditions = {"type": "array", "items": {"type": "object"}}
    properties: Dict[str, Any] = {
        "where": {
            "type": "object",
            "properties": {**{f: {} for f in fields}, "and": conditions, "or": conditions},
            "additionalProperties": False,
        },
        "fields": {"oneOf": [
            {"type": "array", "items": {"enum": fields}, "uniqueItems": True},
            {"type": "object", "properties": {f: {"type": "boolean"} for f in fields}, "additionalProperties": False},
        ]},
        "order": {"oneOf": [order_item, {"type": "array", "items": order_item}]},
        "limit": limit,
        "skip": {"type": "integer", "minimum": 0},
    }
    if relations:
        names = "|".join(relations)
        properties["include"] = {"type": "array", "items": {"oneOf": [
            {"type": "string", "pattern": f"^({names})(\\..+)?$"},  # dotted paths nest
            {
                "type": "object",
                "properties": {"relation": {"enum": relations}, "scope": {"type": "object"}},
                "required": ["relation"],
            },
        ]}}
    return {"title": f"{model.__name__}Filter", "type": "object", "properties": properties, "additionalProperties": False}


def filter_example(model: Type[PostgreSQLModel], max_limit: Optional[int] = None) -> Dict[str, Any]:
    """An example filter for `model`, built from its own fields and relations."""
    pk = model._get_primary_key()
    others = [name for name in model._fields if name != pk]
    example: Dict[str, Any] = {}
    typed = [name for name in others if model._fields[name].python_type in _EXAMPLE_VALUES]
    if typed:
        example["where"] = {typed[0]: _EXAMPLE_VALUES[model._fields[typed[0]].python_type]}
    if others:
        example["order"] = [f"{others[0]} ASC"]
    example["limit"] = min(20, max_limit) if max_limit is not None else 20
    example["fields"] = [name for name in [pk] + others[:1] if name]
    if model._relations:
        example["include"] = [next(iter(model._relations))]
    return example


def validate_filter(model: Type[PostgreSQLModel], opts: FilterOptions) -> None:
    """
    Check a filter against `model` without running it: filter keys, field
    names and operators (by building the SQL), and every included relation
    and its scope, recursively. Raises QueryError / RelationNotDefinedError.
    """
    QueryBuilder(model.__table__, model._fields).build_select(opts)
    for scope in (_parse_include(item) for item in opts.include or []):
        rel = model._relations.get(scope.relation)
        if rel is None:
            raise RelationNotDefinedError(scope.relation, model.__name__)
        if scope.scope is not None:
            validate_filter(registry.get(rel.target_model), scope.scope)


def _cap_includes(
    model: Type[PostgreSQLModel],
    opts: FilterOptions,
    max_limit: Optional[int],
    max_depth: Optional[int],
    depth: int = 1,
) -> None:
    """
    Apply the request caps to every include scope, recursively: a missing
    scope `limit` becomes `max_limit` (per parent, as scope limits are), a
    larger one is rejected, and includes may nest at most `max_depth` levels.
    """
    if not opts.include:
        return
    if max_depth is not None and depth > max_depth:
        raise QueryError(f"'include' may nest at most {max_depth} levels")
    capped = []
    for scope in (_parse_include(item) for item in opts.include):
        rel = model._relations.get(scope.relation)
        if rel is None:
            raise RelationNotDefinedError(scope.relation, model.__name__)
        inner = dataclasses.replace(scope.scope) if scope.scope else FilterOptions()
        if max_limit is not None:
            if inner.limit is None:
                inner.limit = max_limit
            elif inner.limit > max_limit:
                raise QueryError(
                    f"'limit' in the '{scope.relation}' include must be an integer no greater than {max_limit}"
                )
        _cap_includes(registry.get(rel.target_model), inner, max_limit, max_depth, depth + 1)
        capped.append(IncludeScope(relation=scope.relation, scope=inner))
    opts.include = capped


def parse_filter(
    model: Type[PostgreSQLModel],
    raw: Optional[str],
    max_limit: Optional[int] = None,
    max_include_depth: Optional[int] = None,
) -> FilterOptions:
    """
    Parse and validate a JSON `filter` string for `model`. A missing `limit`
    becomes `max_limit`; a larger one is rejected. The same cap applies to
    each include scope (per parent), and includes may nest at most
    `max_include_depth` levels. Every problem is a QueryError, i.e. bad
    client input.
    """
    try:
        data = json.loads(raw) if raw else {}
    except ValueError as exc:
        raise QueryError(f"'filter' is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise QueryError("'filter' must be a JSON object")
    try:
        opts = _dict_to_filter(data)
        if max_limit is not None:
            if opts.limit is None:
                opts.limit = max_limit
            elif opts.limit > max_limit:  # already a non-negative int (_dict_to_filter)
                raise QueryError(f"'limit' must be an integer no greater than {max_limit}")
        _cap_includes(model, opts, max_limit, max_include_depth)
        validate_filter(model, opts)
    except RelationNotDefinedError as exc:
        raise QueryError(str(exc)) from exc
    except (TypeError, ValueError, AttributeError) as exc:  # any shape the checks above missed
        raise QueryError(f"Invalid filter: {exc}") from exc
    return opts


def FilterParam(  # noqa: N802
    model: Type[PostgreSQLModel],
    max_limit: Optional[int] = 100,
    max_include_depth: Optional[int] = 3,
) -> Callable[..., FilterOptions]:
    """
    A FastAPI dependency that reads `?filter=<json>`, validates it against
    `model` and returns FilterOptions for `find()` / `find_and_count()`.
    `max_limit` caps the top-level `limit` and each include scope's `limit`
    (per parent); `max_include_depth` caps include nesting. `None` disables
    either cap. Invalid filters are HTTP 400 with the QueryError message: with
    register_exception_handlers() installed the body has the same
    {"error", "detail"} shape as every other library error, otherwise it is
    FastAPI's plain {"detail"}. The parameter is documented in the OpenAPI
    schema.

        @app.get("/users")
        async def list_users(filter: FilterOptions = Depends(FilterParam(User, max_limit=100))):
            return await db.find(User, filter)
    """
    from fastapi import HTTPException, Query, Request

    example = json.dumps(filter_example(model, max_limit))
    description = (
        f"LoopBack-style filter for {model.__name__} as JSON, e.g. {example}. "
        f"Fields: {', '.join(model._fields)}."
        + (f" Relations: {', '.join(model._relations)}." if model._relations else "")
    )
    # OpenAPI 3.1: the parameter is a string holding JSON of this shape.
    schema_extra = {"contentMediaType": "application/json", "contentSchema": filter_json_schema(model, max_limit)}

    def dependency(
        request: Request,
        filter: Optional[str] = Query(
            None,
            description=description,
            json_schema_extra=schema_extra,
            openapi_examples={"example": {"summary": f"{model.__name__} filter", "value": example}},
        ),
    ) -> FilterOptions:
        try:
            return parse_filter(model, filter, max_limit, max_include_depth)
        except QueryError as exc:
            if getattr(request.app.state, _HANDLERS_FLAG, False):
                raise  # the registered QueryError handler answers 400 in the common shape
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Request is imported lazily, so the string annotation (PEP 563) can't be
    # resolved from module globals; give FastAPI the class itself.
    dependency.__annotations__["request"] = Request
    return dependency
