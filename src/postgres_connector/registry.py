"""
Global model registry.

All PostgreSQLModel subclasses auto-register here the moment they are defined
(via the ModelMeta metaclass). You never call this directly.

Usage (internal):
    from .registry import registry
    UserClass = registry.get("User")
    all_models = registry.all()
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Type, Dict, List

if TYPE_CHECKING:
    from .model import PostgreSQLModel

from .exceptions import ModelNotRegisteredError


class ModelRegistry:
    """Thread-safe singleton that holds all registered PostgreSQLModel subclasses."""

    _instance: Optional["ModelRegistry"] = None

    def __new__(cls) -> "ModelRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._models: Dict[str, Type["PostgreSQLModel"]] = {}
        return cls._instance

    # ── Registration ─────────────────────────────────────────────────────

    def register(self, model_class: Type["PostgreSQLModel"]) -> None:
        """Called automatically by ModelMeta when a subclass is defined."""
        self._models[model_class.__name__] = model_class

    def unregister(self, name: str) -> None:
        """Mainly useful in tests for cleanup."""
        self._models.pop(name, None)

    # ── Lookup ───────────────────────────────────────────────────────────

    def get(self, name: str) -> Type["PostgreSQLModel"]:
        if name not in self._models:
            raise ModelNotRegisteredError(name)
        return self._models[name]

    def get_or_none(self, name: str) -> Optional[Type["PostgreSQLModel"]]:
        return self._models.get(name)

    def is_registered(self, name: str) -> bool:
        return name in self._models

    def all(self) -> Dict[str, Type["PostgreSQLModel"]]:
        return dict(self._models)

    def names(self) -> List[str]:
        return list(self._models.keys())

    # ── Debug ─────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"ModelRegistry(models={self.names()})"


# Global singleton instance — import this everywhere
registry = ModelRegistry()
