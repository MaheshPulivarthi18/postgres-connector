"""
Custom exceptions for the PostgreSQL Connector.
All connector errors inherit from ConnectorError for easy catch-all handling.
"""

from typing import Optional


class ConnectorError(Exception):
    """Base exception for all connector errors."""
    pass


class RelationNotLoadedError(ConnectorError):
    """
    Raised when you access a relation that was NOT included in the query.

    Example:
        user = await connector.find_one(User, {"where": {"id": 1}})
        user.orders   # ← raises this if "orders" was not in include
    """
    def __init__(self, relation_name: str, model_name: str):
        super().__init__(
            f"Relation '{relation_name}' on '{model_name}' was not loaded. "
            f"Add '{relation_name}' to the 'include' list in your filter."
        )
        self.relation_name = relation_name
        self.model_name = model_name


class ModelNotRegisteredError(ConnectorError):
    """Raised when referencing a model that hasn't been registered."""
    def __init__(self, model_name: str):
        super().__init__(
            f"Model '{model_name}' is not registered. "
            f"Make sure it subclasses PostgreSQLModel and is imported before use."
        )
        self.model_name = model_name


class RelationNotDefinedError(ConnectorError):
    """Raised when trying to include a relation that doesn't exist on the model."""
    def __init__(self, relation_name: str, model_name: str):
        super().__init__(
            f"Relation '{relation_name}' is not defined on model '{model_name}'. "
            f"Check your Relation() declarations."
        )
        self.relation_name = relation_name
        self.model_name = model_name


class SchemaError(ConnectorError):
    """Raised on schema validation or mismatch errors."""
    pass


class QueryError(ConnectorError):
    """Raised when a query cannot be built or executed."""
    pass


class ConnectionPoolError(ConnectorError):
    """Raised on connection pool setup or acquire failures."""
    pass


class ConfigurationError(ConnectorError):
    """Raised on invalid connector or model configuration."""
    pass


class QueryTimeoutError(ConnectorError):
    """
    Raised when a query exceeds its timeout (either the connector-wide
    `command_timeout` default, or a per-call `timeout=` override on
    raw()/raw_execute()) — instead of a bare `TimeoutError`, which
    could be confused with an unrelated asyncio timeout elsewhere in the
    app. A slow/hung query is deliberately NOT retried automatically (see
    fault tolerance docs) — retrying an already-slow query risks piling
    more load onto an already-struggling database.
    """
    def __init__(self, message: str, *, timeout: Optional[float] = None):
        super().__init__(message)
        self.timeout = timeout


class InvalidValueError(ConnectorError):
    """
    A value the database or the driver can't accept for its column: text
    where a number is expected, a number out of range, a malformed date
    (SQLSTATE class 22, or asyncpg rejecting a Python value while encoding
    a query argument). Almost always bad client input. Chains the original
    asyncpg exception via `__cause__`.
    """
    def __init__(self, message: str, *, detail: Optional[str] = None):
        super().__init__(message)
        self.detail = detail


class ReadOnlyTransactionError(ConnectorError):
    """A write was attempted inside a read-only transaction (`begin(readonly=True)`)."""


class IntegrityError(ConnectorError):
    """
    Base class for a write rejected by a database constraint. Raised
    instead of a raw asyncpg exception so a FastAPI app can map DB errors
    to HTTP responses in one place (an exception handler per subclass)
    rather than every route hand-catching driver internals.

    Carries the underlying constraint details Postgres reported — useful
    for building an error message or deciding which field to blame in a
    response — and chains the original asyncpg exception via `__cause__`
    if you need the raw SQLSTATE/message.
    """
    def __init__(
        self,
        message:    str,
        *,
        constraint: Optional[str] = None,
        table:      Optional[str] = None,
        column:     Optional[str] = None,
        detail:     Optional[str] = None,
    ):
        super().__init__(message)
        self.constraint = constraint
        self.table      = table
        self.column     = column
        self.detail     = detail


class UniqueConstraintError(IntegrityError):
    """A write violated a UNIQUE or PRIMARY KEY constraint (e.g. a duplicate email)."""


class ForeignKeyConstraintError(IntegrityError):
    """A write referenced a row that doesn't exist, or tried to delete/update a still-referenced row."""


class NotNullConstraintError(IntegrityError):
    """A write left a required (NOT NULL) column empty."""


class CheckConstraintError(IntegrityError):
    """A write violated a CHECK constraint."""
