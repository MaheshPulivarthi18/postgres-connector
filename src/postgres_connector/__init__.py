"""
postgres_connector — A production-grade async PostgreSQL connector with
LoopBack-style nested relation queries and typed model instances.

Quick start:
    from postgres_connector import PostgreSQLConnector, PostgreSQLModel, Field, Relation

    class User(PostgreSQLModel):
        __table__ = "users"
        id:     int           = Field(primary_key=True)
        name:   str           = Field(max_length=100)
        email:  str           = Field(unique=True)
        orders: list["Order"] = Relation(type="hasMany", foreign_key="user_id")

    class Order(PostgreSQLModel):
        __table__ = "orders"
        id:      int    = Field(primary_key=True)
        user_id: int    = Field(foreign_key="users.id")
        total:   float
        user:    "User" = Relation(type="belongsTo", foreign_key="user_id")

    connector = PostgreSQLConnector(host="localhost", database="mydb",
                                    user="postgres", password="secret")
    await connector.connect()

    users = await connector.find(User, {
        "where": {"active": True},
        "include": ["orders"],
        "limit": 10,
    })
"""

__version__ = "1.0.0"

from .connector import PostgreSQLConnector
from .model     import PostgreSQLModel
from .fields    import Field, Relation, JSONField
from .migrator  import Migrator
from .types     import FilterOptions, IncludeScope, RelationType
from .registry  import registry

from .exceptions import (
    ConnectorError,
    RelationNotLoadedError,
    ModelNotRegisteredError,
    RelationNotDefinedError,
    SchemaError,
    QueryError,
    ConnectionPoolError,
    ConfigurationError,
    TransactionError,
)

__all__ = [
    "__version__",
    # Core
    "PostgreSQLConnector",
    "PostgreSQLModel",
    "Field",
    "Relation",
    "JSONField",
    "Migrator",
    # Types / DSL
    "FilterOptions",
    "IncludeScope",
    "RelationType",
    # Registry
    "registry",
    # Exceptions
    "ConnectorError",
    "RelationNotLoadedError",
    "ModelNotRegisteredError",
    "RelationNotDefinedError",
    "SchemaError",
    "QueryError",
    "ConnectionPoolError",
    "ConfigurationError",
    "TransactionError",
]
