"""
Custom exceptions for the PostgreSQL Connector.
All connector errors inherit from ConnectorError for easy catch-all handling.
"""


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


class TransactionError(ConnectorError):
    """Raised when a transaction fails."""
    pass
