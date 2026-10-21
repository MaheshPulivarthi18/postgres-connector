"""
Pydantic models generated from PostgreSQLModel classes, for FastAPI
`response_model`s and OpenAPI schemas without a hand-written duplicate class.

    UserOut        = User.pydantic_model()
    UserWithOrders = User.pydantic_model(include=["orders"])
    UserPublic     = User.pydantic_model(exclude=["password_hash"])

Built from what the model already declares (`_fields`, `_relations`).
Generated models read ORM instances directly (`from_attributes`), so a route
can return the instance itself. Pydantic is imported lazily: the library has
no hard dependency on it (FastAPI brings it).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, Type

from .exceptions import ConfigurationError
from .types import FieldDefinition, RelationType

if TYPE_CHECKING:
    from .model import PostgreSQLModel

_cache: Dict[Tuple[Any, ...], Any] = {}


def _field_annotation(field: FieldDefinition) -> Any:
    if field.is_json:
        return Any
    if field.auto_now or field.auto_now_add:
        return datetime  # stamped as a datetime whatever the declared annotation
    return field.python_type


def _split_includes(include: Iterable[str]) -> Dict[str, List[str]]:
    """["orders.items", "orders.payments", "profile"] -> {"orders": ["items", "payments"], "profile": []}"""
    tree: Dict[str, List[str]] = {}
    for path in include:
        head, _, rest = path.partition(".")
        tree.setdefault(head, [])
        if rest:
            tree[head].append(rest)
    return tree


def pydantic_model(
    model: Type[PostgreSQLModel],
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    name: Optional[str] = None,
) -> Any:
    """See PostgreSQLModel.pydantic_model()."""
    from pydantic import ConfigDict, create_model
    from pydantic import Field as PydanticField

    from .registry import registry

    include, exclude = tuple(include), tuple(exclude)
    key = (model, include, exclude, name)
    if key in _cache:
        return _cache[key]

    unknown = [f for f in exclude if f not in model._fields]
    if unknown:
        raise ConfigurationError(f"{model.__name__}.pydantic_model(): exclude has unknown fields {unknown}")

    fields: Dict[str, Any] = {}
    for fname, fdef in model._fields.items():
        if fname in exclude:
            continue
        annotation = _field_annotation(fdef)
        constraints: Dict[str, Any] = {}
        if fdef.max_length and fdef.python_type is str and not fdef.is_json:
            constraints["max_length"] = fdef.max_length
        if fdef.nullable and not fdef.primary_key:
            fields[fname] = (Optional[annotation], PydanticField(None, **constraints))
        else:
            fields[fname] = (annotation, PydanticField(..., **constraints))

    for rel_name, nested in _split_includes(include).items():
        rel = model._relations.get(rel_name)
        if rel is None:
            raise ConfigurationError(
                f"{model.__name__}.pydantic_model(): include has unknown relation '{rel_name}'"
            )
        target = registry.get(rel.target_model)
        sub = pydantic_model(target, include=nested)
        if rel.relation_type in (RelationType.HAS_MANY, RelationType.HAS_MANY_THROUGH):
            fields[rel_name] = (List[sub], PydanticField(default_factory=list))
        else:
            fields[rel_name] = (Optional[sub], PydanticField(None))

    model_name = name or model.__name__ + "".join(
        "With" + part.replace(".", "_").title().replace("_", "") for part in include
    ) + ("Public" if exclude else "") + "Out"
    generated = create_model(  # type: ignore[call-overload]
        model_name,
        __config__=ConfigDict(from_attributes=True),
        __module__=model.__module__,
        **fields,
    )
    _cache[key] = generated
    return generated
