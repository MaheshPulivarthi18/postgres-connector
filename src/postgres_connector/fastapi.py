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
`constraint` / `table` / `column` / `db_detail` when Postgres reported them.
Requires FastAPI (or Starlette), which this module imports lazily.
"""

from __future__ import annotations

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
from .types import FilterOptions

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


def error_body(exc: ConnectorError) -> Dict[str, Any]:
    body: Dict[str, Any] = {"error": type(exc).__name__, "detail": str(exc)}
    if isinstance(exc, IntegrityError):
        for key in ("constraint", "table", "column"):
            value = getattr(exc, key)
            if value is not None:
                body[key] = value
        if exc.detail is not None:
            body["db_detail"] = exc.detail
    elif isinstance(exc, InvalidValueError) and exc.detail is not None:
        body["db_detail"] = exc.detail
    return body


def register_exception_handlers(app: Any) -> None:
    """Install one handler per error class on a FastAPI (or Starlette) app."""
    from starlette.responses import JSONResponse

    def handler_for(status: int) -> Any:
        async def handle(request: Any, exc: Exception) -> JSONResponse:
            assert isinstance(exc, ConnectorError)
            return JSONResponse(error_body(exc), status_code=status)

        return handle

    for error_cls, status in ERROR_STATUS:
        app.add_exception_handler(error_cls, handler_for(status))


# ── ?filter= query parameter ─────────────────────────────────────────────────

FILTER_DESCRIPTION = (
    "LoopBack-style filter as JSON, e.g. "
    '{"where": {"active": true}, "order": ["name ASC"], "limit": 20, "skip": 0, '
    '"fields": ["id", "name"], "include": ["orders"]}'
)


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


def parse_filter(model: Type[PostgreSQLModel], raw: Optional[str], max_limit: Optional[int] = None) -> FilterOptions:
    """
    Parse and validate a JSON `filter` string for `model`. A missing `limit`
    becomes `max_limit`; a larger one is rejected. Every problem is a
    QueryError, i.e. bad client input.
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
            elif not isinstance(opts.limit, int) or isinstance(opts.limit, bool) or opts.limit > max_limit:
                raise QueryError(f"'limit' must be an integer no greater than {max_limit}")
        validate_filter(model, opts)
    except RelationNotDefinedError as exc:
        raise QueryError(str(exc)) from exc
    except (TypeError, ValueError) as exc:  # wrong value shapes, e.g. a string limit or skip
        raise QueryError(f"Invalid filter: {exc}") from exc
    return opts


def FilterParam(model: Type[PostgreSQLModel], max_limit: Optional[int] = 100) -> Callable[..., FilterOptions]:  # noqa: N802
    """
    A FastAPI dependency that reads `?filter=<json>`, validates it against
    `model` and returns FilterOptions for `find()` / `find_and_count()`.
    Invalid filters are HTTP 400 with the QueryError message. The parameter
    is documented in the OpenAPI schema.

        @app.get("/users")
        async def list_users(filter: FilterOptions = Depends(FilterParam(User, max_limit=100))):
            return await db.find(User, filter)
    """
    from fastapi import HTTPException, Query

    def dependency(filter: Optional[str] = Query(None, description=FILTER_DESCRIPTION)) -> FilterOptions:
        try:
            return parse_filter(model, filter, max_limit)
        except QueryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return dependency
