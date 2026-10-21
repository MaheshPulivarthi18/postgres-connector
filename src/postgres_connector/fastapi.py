"""
FastAPI integration: turn the library's typed errors into HTTP responses.

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

from typing import Any, Dict, Tuple, Type

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
    UniqueConstraintError,
)

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
