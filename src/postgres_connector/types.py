"""
Core type definitions for the PostgreSQL Connector.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Union, Dict, List, Type


class RelationType(str, Enum):
    HAS_ONE          = "hasOne"
    HAS_MANY         = "hasMany"
    BELONGS_TO       = "belongsTo"
    HAS_MANY_THROUGH = "hasManyThrough"


@dataclass
class FieldDefinition:
    """Metadata for a single column on a model."""
    name:             str
    python_type:      type
    primary_key:      bool          = False
    nullable:         bool          = True
    unique:           bool          = False
    default:          Any           = None
    has_default:      bool          = False
    max_length:       Optional[int] = None
    foreign_key:      Optional[str] = None   # e.g. "users.id"
    auto_increment:   bool          = False
    index:            bool          = False
    is_json:          bool          = False
    auto_now:         bool          = False
    auto_now_add:     bool          = False


@dataclass
class RelationDefinition:
    """Metadata for a relation between two models."""
    name:           str
    relation_type:  RelationType
    target_model:   str            # name of the target model class
    foreign_key:    str            # column name
    other_key:      Optional[str]  = None   # for hasManyThrough (target side)
    through_model:  Optional[str]  = None   # for hasManyThrough (junction model name)


@dataclass
class IncludeScope:
    """A single relation to include, optionally with a nested filter scope."""
    relation: str
    scope:    Optional["FilterOptions"] = None


@dataclass
class FilterOptions:
    """
    LoopBack-style filter DSL.

    Example:
        FilterOptions(
            where={"status": "active", "age": {"gt": 18}},
            order=["created_at DESC"],
            limit=10,
            skip=0,
            fields=["id", "name"],
            include=["orders.items"],
        )
    """
    where:   Optional[Dict[str, Any]]                        = None
    order:   Optional[List[str]]                             = None
    limit:   Optional[int]                                   = None
    skip:    Optional[int]                                   = None
    fields:  Optional[List[str]]                             = None
    include: Optional[List[Union[str, dict, IncludeScope]]]  = None


@dataclass
class ColumnInfo:
    """A column as read from PostgreSQL information_schema (used for schema diffing)."""
    name:                     str
    data_type:                str
    is_nullable:              bool
    column_default:           Optional[str]
    character_maximum_length: Optional[int]


# Type aliases
WhereClause  = Dict[str, Any]
OrderClause  = List[str]
IncludeItem  = Union[str, dict, IncludeScope]
