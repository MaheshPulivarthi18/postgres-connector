"""
Field and Relation descriptors.

Usage in a model:
    class User(PostgreSQLModel):
        __table__ = "users"

        id:     int  = Field(primary_key=True)
        name:   str  = Field(max_length=100)
        email:  str  = Field(unique=True)
        active: bool = Field(default=True)

        orders: List["Order"] = Relation(type="hasMany", foreign_key="user_id")
"""

from __future__ import annotations
from typing import Any, Optional
from .types import RelationType, FieldDefinition, RelationDefinition
from .exceptions import ConfigurationError

# Internal sentinel — distinguishes "no default" from "default=None"
_UNSET = object()


class Field:
    """
    Declares a database column on a PostgreSQLModel.

    Args:
        primary_key:    Mark column as PK (also enables auto_increment by default).
        nullable:       Allow NULL values. Default True.
        unique:         Add a UNIQUE constraint.
        default:        Python-side default value (not DB default).
        max_length:     VARCHAR length cap.
        foreign_key:    Reference like "other_table.column".
        auto_increment: Override auto_increment behavior.
        index:          Add a regular index on this column.
    """

    def __init__(
        self,
        *,
        primary_key:    bool          = False,
        nullable:       bool          = True,
        unique:         bool          = False,
        default:        Any           = _UNSET,
        max_length:     Optional[int] = None,
        foreign_key:    Optional[str] = None,
        auto_increment: bool          = False,
        index:          bool          = False,
        auto_now:       bool          = False,
        auto_now_add:   bool          = False,
    ):
        self.primary_key    = primary_key
        self.nullable       = nullable if not primary_key else False
        self.unique         = unique or primary_key
        self._default       = default
        self.has_default    = default is not _UNSET
        self.max_length     = max_length
        self.foreign_key    = foreign_key
        self.auto_increment = auto_increment or primary_key
        self.index          = index
        self.auto_now       = auto_now
        self.auto_now_add   = auto_now_add
        self.is_json        = False

    @property
    def default(self) -> Any:
        return None if not self.has_default else self._default

    def to_definition(self, name: str, python_type: type) -> FieldDefinition:
        return FieldDefinition(
            name           = name,
            python_type    = python_type,
            primary_key    = self.primary_key,
            nullable       = self.nullable,
            unique         = self.unique,
            default        = self.default,
            has_default    = self.has_default,
            max_length     = self.max_length,
            foreign_key    = self.foreign_key,
            auto_increment = self.auto_increment,
            index          = self.index,
            is_json        = self.is_json,
            auto_now       = self.auto_now,
            auto_now_add   = self.auto_now_add,
        )

    def __repr__(self) -> str:
        flags = []
        if self.primary_key:    flags.append("pk")
        if self.unique:         flags.append("unique")
        if not self.nullable:   flags.append("not null")
        if self.has_default:    flags.append(f"default={self._default!r}")
        if self.is_json:        flags.append("json")
        if self.auto_now:       flags.append("auto_now")
        if self.auto_now_add:   flags.append("auto_now_add")
        return f"Field({', '.join(flags)})"


class JSONField(Field):
    """
    Field that automatically handles JSON serialization/deserialization.
    Stored as a JSONB column in PostgreSQL.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.is_json = True


class Relation:
    """
    Declares a relation between two PostgreSQLModel classes.

    Args:
        type:        One of "hasOne", "hasMany", "belongsTo", "hasManyThrough".
        foreign_key: The FK column name (on the child/through table).
        through:     Junction model name (only for hasManyThrough).
        other_key:   Target-side FK in the junction table (hasManyThrough only).

    Examples:
        # User hasMany Order
        orders: List["Order"] = Relation(type="hasMany", foreign_key="user_id")

        # Order belongsTo User
        user: "User" = Relation(type="belongsTo", foreign_key="user_id")

        # User hasOne Profile
        profile: "Profile" = Relation(type="hasOne", foreign_key="user_id")

        # Product hasManyThrough Order via OrderItem
        orders: List["Order"] = Relation(
            type="hasManyThrough",
            through="OrderItem",
            foreign_key="product_id",
            other_key="order_id",
        )
    """

    def __init__(
        self,
        *,
        type:        str,
        foreign_key: str,
        through:     Optional[str] = None,
        other_key:   Optional[str] = None,
    ):
        try:
            self.relation_type = RelationType(type)
        except ValueError:
            valid = [r.value for r in RelationType]
            raise ConfigurationError(
                f"Invalid relation type '{type}'. Must be one of: {valid}"
            )

        if self.relation_type == RelationType.HAS_MANY_THROUGH:
            if not through:
                raise ConfigurationError("hasManyThrough requires a 'through' (junction model name).")
            if not other_key:
                raise ConfigurationError("hasManyThrough requires an 'other_key' (target FK in junction).")

        self.foreign_key = foreign_key
        self.through     = through
        self.other_key   = other_key

    def to_definition(self, name: str, target_model: str) -> RelationDefinition:
        return RelationDefinition(
            name           = name,
            relation_type  = self.relation_type,
            target_model   = target_model,
            foreign_key    = self.foreign_key,
            other_key      = self.other_key,
            through_model  = self.through,
        )

    def __repr__(self) -> str:
        return f"Relation(type={self.relation_type.value!r}, fk={self.foreign_key!r})"
