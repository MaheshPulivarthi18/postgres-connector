"""
PostgreSQLModel — the base class every table model must subclass.

Features:
  - Metaclass auto-registers models in the global registry
  - Declarative Field() and Relation() parsing from type annotations
  - Typed instance creation from raw DB rows
  - Relation access protection (RelationNotLoadedError if not included)
  - .to_dict() / .to_json() serialization with nested relations
  - Pydantic-compatible __json__ hook for FastAPI responses
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import (
    Any, ClassVar, Optional, Type, Union, Dict, List,
    get_args, get_origin,
)

from .fields import Field, Relation, _UNSET
from .types import FieldDefinition, RelationDefinition, RelationType
from .exceptions import RelationNotLoadedError, ConfigurationError

# `X | None` (PEP 604) has origin types.UnionType rather than typing.Union.
_UNION_ORIGINS: tuple = (Union,)
if sys.version_info >= (3, 10):
    import types as _types
    _UNION_ORIGINS += (_types.UnionType,)

# Sentinel object — stored on the instance for unloaded relations.
# Distinct from None so we can detect "never loaded" vs "loaded as None".
_UNLOADED = object()


class ModelMeta(type):
    """
    Metaclass that:
      1. Scans class annotations for Field() and Relation() descriptors
      2. Builds _fields and _relations dicts on the class
      3. Auto-registers the class in the global ModelRegistry
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Skip the abstract base itself
        if name == "PostgreSQLModel":
            return cls

        # ── Inherit parent definitions ────────────────────────────────────
        inherited_fields:    Dict[str, FieldDefinition]    = {}
        inherited_relations: Dict[str, RelationDefinition] = {}
        inherited_defaults:  Dict[str, Any]                = {}

        for base in reversed(bases):
            if hasattr(base, "_fields"):
                inherited_fields.update(base._fields)
            if hasattr(base, "_relations"):
                inherited_relations.update(base._relations)
            if hasattr(base, "_field_defaults"):
                inherited_defaults.update(base._field_defaults)

        cls._fields:         Dict[str, FieldDefinition]    = dict(inherited_fields)
        cls._relations:      Dict[str, RelationDefinition] = dict(inherited_relations)
        cls._field_defaults: Dict[str, Any]                = dict(inherited_defaults)

        # ── Collect all annotations up the MRO ───────────────────────────
        annotations: Dict[str, Any] = {}
        for klass in reversed(cls.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))

        # ── Process each annotation ───────────────────────────────────────
        for attr_name, attr_annotation in annotations.items():
            if attr_name.startswith("_"):
                continue

            raw_value = namespace.get(attr_name, _UNSET)

            if isinstance(raw_value, Field):
                # Explicit Field() descriptor
                python_type = mcs._unwrap_type(mcs._resolve_annotation(attr_annotation, cls))
                field_def   = raw_value.to_definition(attr_name, python_type)
                cls._fields[attr_name] = field_def

                if raw_value.has_default:
                    cls._field_defaults[attr_name] = raw_value.default

            elif isinstance(raw_value, Relation):
                # Explicit Relation() descriptor
                target_name = mcs._extract_model_name(attr_annotation)
                rel_def     = raw_value.to_definition(attr_name, target_name)
                cls._relations[attr_name] = rel_def

            elif raw_value is _UNSET:
                # Plain annotation without a descriptor — auto-detect
                attr_annotation = mcs._resolve_annotation(attr_annotation, cls)
                if mcs._looks_like_relation(attr_annotation):
                    pass
                else:
                    python_type = mcs._unwrap_type(attr_annotation)
                    nullable    = mcs._is_optional(attr_annotation)
                    cls._fields[attr_name] = FieldDefinition(
                        name        = attr_name,
                        python_type = python_type,
                        nullable    = nullable,
                    )

        # ── Remove raw Field/Relation descriptors from the class ─────────
        for attr_name in list(annotations.keys()):
            if attr_name.startswith("_"):
                continue
            raw = namespace.get(attr_name)
            if isinstance(raw, (Field, Relation)):
                try:
                    delattr(cls, attr_name)
                except AttributeError:
                    pass

        # ── Parse composite indexes ───────────────────────────────────────
        composite_indexes = getattr(cls, "__indexes__", [])
        if not isinstance(composite_indexes, list):
            composite_indexes = list(composite_indexes)
        cls._composite_indexes = composite_indexes

        # ── Validate table name ───────────────────────────────────────────
        if not getattr(cls, "__table__", ""):
            raise ConfigurationError(
                f"Model '{name}' must define __table__ = '<table_name>'."
            )

        # ── Validate at least one PK ──────────────────────────────────────
        has_pk = any(f.primary_key for f in cls._fields.values())
        if not has_pk:
            raise ConfigurationError(
                f"Model '{name}' has no primary key. "
                f"Add Field(primary_key=True) to one field."
            )

        # ── Auto-register ─────────────────────────────────────────────────
        from .registry import registry
        registry.register(cls)

        return cls

    # ── Type helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _resolve_annotation(annotation: Any, cls: type) -> Any:
        """
        Evaluate a string annotation (from `from __future__ import
        annotations`, or a quoted type) in the model's module, the same way
        typing.get_type_hints() does — so "datetime" and "Optional[int]"
        become real types instead of falling back to `str`. Unresolvable
        strings (forward refs to models not defined yet, names only visible
        in a function's locals) are returned unchanged.
        """
        if not isinstance(annotation, str):
            return annotation
        module = sys.modules.get(cls.__module__)
        globalns = dict(vars(module)) if module else {}
        try:
            # Module globals only: the class body would shadow a type with a
            # same-named field (`date: date = Field()` -> the Field object).
            return eval(annotation, globalns)  # noqa: S307 — the model's own source
        except Exception:
            return annotation

    @staticmethod
    def _unwrap_type(annotation: Any) -> type:
        """Unwrap Optional[X] / Union[X, None] → X. Falls back to str."""
        import typing
        origin = get_origin(annotation)
        args   = get_args(annotation)

        if origin in _UNION_ORIGINS:
            non_none = [a for a in args if a is not type(None)]
            if non_none:
                inner = non_none[0]
                if isinstance(inner, type):
                    return inner
                return str
        if isinstance(annotation, type):
            return annotation

        if isinstance(annotation, str):
            mapping = {
                "int":   int,
                "str":   str,
                "bool":  bool,
                "float": float,
                "dict":  dict,
                "list":  list,
            }
            return mapping.get(annotation.lower(), str)

        return str

    @staticmethod
    def _is_optional(annotation: Any) -> bool:
        import typing
        return (
            get_origin(annotation) in _UNION_ORIGINS
            and type(None) in get_args(annotation)
        )

    @staticmethod
    def _looks_like_relation(annotation: Any) -> bool:
        """Heuristic: list[...] annotations are typically relations."""
        if isinstance(annotation, str):  # unresolvable, e.g. list["Order"] on 3.8
            return annotation.replace(" ", "").startswith(("list[", "List["))
        return get_origin(annotation) is list

    @staticmethod
    def _extract_model_name(annotation: Any) -> str:
        """
        Extract model class name from:
          - list["Order"]
          - list[Order]
          - Optional["User"]
          - "User"
          - User
        """
        import typing
        if isinstance(annotation, str):
            text = annotation.strip().strip("'\"")
            for prefix in ("list[", "List[", "typing.List[", "Optional[", "typing.Optional["):
                if text.startswith(prefix) and text.endswith("]"):
                    return ModelMeta._extract_model_name(text[len(prefix):-1])
            return ModelMeta._name_from(text)

        origin = get_origin(annotation)
        args   = get_args(annotation)

        if origin is list:
            inner = args[0] if args else None
            return ModelMeta._name_from(inner)

        return ModelMeta._name_from(annotation)

    @staticmethod
    def _name_from(annotation: Any) -> str:
        if annotation is None:
            return "Unknown"
        if isinstance(annotation, str):
            return annotation.strip("'\"")
        if hasattr(annotation, "__forward_arg__"):
            return annotation.__forward_arg__.strip("'\"")
        if isinstance(annotation, type):
            return annotation.__name__
        return str(annotation).strip("'\"")


# ─────────────────────────────────────────────────────────────────────────────

class PostgreSQLModel(metaclass=ModelMeta):
    """
    Base class for all database models.

    Subclass it and define your columns and relations:

        class User(PostgreSQLModel):
            __table__ = "users"

            id:     int           = Field(primary_key=True)
            name:   str           = Field(max_length=100)
            email:  str           = Field(unique=True)
            active: bool          = Field(default=True)

            orders: list["Order"] = Relation(type="hasMany", foreign_key="user_id")
    """

    # ── Class-level metadata (set by ModelMeta) ───────────────────────────
    __table__:        ClassVar[str]                            = ""
    _fields:          ClassVar[Dict[str, FieldDefinition]]    = {}
    _relations:       ClassVar[Dict[str, RelationDefinition]] = {}
    _field_defaults:  ClassVar[Dict[str, Any]]                = {}
    _composite_indexes: ClassVar[List[tuple]]                 = []

    # ── Instance init ─────────────────────────────────────────────────────

    def __init__(self, **data: Any):
        object.__setattr__(self, "_loaded_relations", set())

        for field_name, field_def in self.__class__._fields.items():
            value = data.get(field_name, _UNSET)
            if value is not _UNSET:
                object.__setattr__(self, field_name, value)
            elif field_name in self.__class__._field_defaults:
                object.__setattr__(self, field_name, self.__class__._field_defaults[field_name])
            else:
                object.__setattr__(self, field_name, None)

        for rel_name in self.__class__._relations:
            object.__setattr__(self, f"__rel_{rel_name}", _UNLOADED)

    # ── Attribute access ──────────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        if name in self.__class__._relations:
            raw = self.__dict__.get(f"__rel_{name}", _UNLOADED)
            if raw is _UNLOADED:
                raise RelationNotLoadedError(name, self.__class__.__name__)
            return raw
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or name in self.__class__._fields:
            object.__setattr__(self, name, value)
        elif name in self.__class__._relations:
            object.__setattr__(self, f"__rel_{name}", value)
            self._loaded_relations.add(name)
        else:
            object.__setattr__(self, name, value)

    # ── Relation management (called by the connector) ─────────────────────

    def _set_relation(self, name: str, value: Any) -> None:
        """Populate a loaded relation. Called internally by RelationResolver."""
        object.__setattr__(self, f"__rel_{name}", value)
        self._loaded_relations.add(name)

    def _is_relation_loaded(self, name: str) -> bool:
        return name in self._loaded_relations

    # ── Class helpers ─────────────────────────────────────────────────────

    @classmethod
    def _get_primary_key(cls) -> Optional[str]:
        for name, f in cls._fields.items():
            if f.primary_key:
                return name
        return None

    @classmethod
    def _from_row(cls, row: Dict[str, Any]) -> "PostgreSQLModel":
        """
        Create a model instance from a raw dictionary returned by asyncpg.
        Does NOT call __init__ with keyword args for maximum speed.
        """
        instance: PostgreSQLModel = cls.__new__(cls)
        object.__setattr__(instance, "_loaded_relations", set())

        for name, field_def in cls._fields.items():
            val = row.get(name)
            # asyncpg with JSON codec returns parsed objects, but fallback for safety
            if field_def.is_json and isinstance(val, (str, bytes)):
                try:
                    val = json.loads(val)
                except (ValueError, TypeError):
                    pass
            object.__setattr__(instance, name, val)

        for rel_name in cls._relations:
            object.__setattr__(instance, f"__rel_{rel_name}", _UNLOADED)

        return instance

    # ── Hooks ─────────────────────────────────────────────────────────────

    async def _trigger_hook(self, name: str) -> None:
        """Helper to invoke an async hook if defined on the instance."""
        hook = getattr(self, name, None)
        if hook and callable(hook):
            if asyncio.iscoroutinefunction(hook):
                await hook()
            else:
                hook()

    async def before_create(self) -> None:
        """Override in subclass: called before the INSERT query."""
        pass

    async def after_create(self) -> None:
        """Override in subclass: called after the INSERT query."""
        pass

    async def before_update(self, data: Dict[str, Any]) -> None:
        """Override in subclass: called before the UPDATE query. Data can be mutated."""
        pass

    async def after_update(self) -> None:
        """Override in subclass: called after the UPDATE query."""
        pass

    async def before_delete(self) -> None:
        """Override in subclass: called before DELETE query."""
        pass

    # ── Serialization ─────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a plain dict.
        Loaded relations are included recursively.
        Unloaded relations are skipped (not included in output).
        """
        result: Dict[str, Any] = {}

        for field_name in self.__class__._fields:
            result[field_name] = self.__dict__.get(field_name)

        for rel_name in self.__class__._relations:
            if rel_name in self._loaded_relations:
                raw = self.__dict__.get(f"__rel_{rel_name}", _UNLOADED)
                if isinstance(raw, list):
                    result[rel_name] = [
                        item.to_dict() if isinstance(item, PostgreSQLModel) else item
                        for item in raw
                    ]
                elif isinstance(raw, PostgreSQLModel):
                    result[rel_name] = raw.to_dict()
                else:
                    result[rel_name] = raw

        return result

    def to_json(self, indent: int = 2) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=indent)

    # FastAPI / Pydantic compatibility hook
    def __json__(self) -> Dict[str, Any]:
        return self.to_dict()

    # ── Magic methods ─────────────────────────────────────────────────────

    def __repr__(self) -> str:
        pk = self.__class__._get_primary_key()
        pk_val = self.__dict__.get(pk, "?") if pk else "?"
        return f"<{self.__class__.__name__} {pk}={pk_val!r}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        pk = self.__class__._get_primary_key()
        if pk:
            return self.__dict__.get(pk) == other.__dict__.get(pk)
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        pk = self.__class__._get_primary_key()
        if pk:
            return hash((self.__class__.__name__, self.__dict__.get(pk)))
        return hash(id(self))
