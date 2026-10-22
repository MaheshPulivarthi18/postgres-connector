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
`constraint` / `table` / `column` when Postgres reported them. Postgres's
DETAIL text (`db_detail`) is only added with `expose_db_detail=True`, since
it quotes stored values.
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
