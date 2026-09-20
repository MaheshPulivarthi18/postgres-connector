"""
Stored procedure/function registration.

Declare a Postgres function or procedure once, call it by the same typed
`connector.call(...)` API everywhere — no hand-written raw SQL, no manual
result-row hydration.

Registering (creating it in the database) is a separate, opt-in step from
declaring how to call it: pass `body=` with your exact `CREATE OR REPLACE
FUNCTION/PROCEDURE` SQL, then run `connector.sync_procedures()` to actually
create/replace it. Without `body`, a Procedure is just a calling-only
binding for something that already exists in the database.

Usage:
    from postgres_connector import Procedure

    # Scalar function: SELECT add_numbers($1, $2) — with its own body,
    # so sync_procedures() will (re)create it in the database.
    add_numbers = Procedure(
        "add_numbers",
        params=["a", "b"],
        body='''
            CREATE OR REPLACE FUNCTION add_numbers(a INT, b INT) RETURNS INT AS $$
            BEGIN RETURN a + b; END;
            $$ LANGUAGE plpgsql
        ''',
    )

    # Table-returning function: SELECT * FROM get_active_users()
    get_active_users = Procedure("get_active_users", returns=User)

    # Procedure (no return value): CALL bump_counter($1)
    bump_counter = Procedure("bump_counter", params=["p_id"], kind="procedure")

    await connector.sync_procedures()          # creates/replaces add_numbers in the DB

    total = await connector.call(add_numbers, a=2, b=3)
    users = await connector.call(get_active_users)
    await connector.call(bump_counter, p_id=1)
    await connector.call("bump_counter", p_id=1)   # look up by registered name
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Type

if TYPE_CHECKING:
    from .model import PostgreSQLModel

from .exceptions import ConfigurationError, ProcedureNotRegisteredError

_VALID_KINDS = ("function", "procedure")


class Procedure:
    """
    Declares a callable Postgres function or procedure.

    Args:
        name:    The function/procedure name in the database.
        params:  Named parameters, in the order they'll be bound
                 positionally when the SQL is built. `connector.call(...)`
                 requires exactly these as keyword arguments — no more, no
                 fewer — so typos are caught before hitting the database.
        returns: For a table/set-returning function, the model class to
                 hydrate each result row into. Leave unset for a scalar
                 function (the raw scalar value is returned) or for
                 `kind="procedure"` (which has no return value).
        kind:    "function" (called via `SELECT` / `SELECT * FROM`) or
                 "procedure" (called via `CALL`, `kind="procedure"` always
                 returns None — OUT parameters are not supported; use
                 `connector.raw(...)` for those).
        body:    Optional. The exact `CREATE FUNCTION`/`CREATE PROCEDURE`
                 SQL for this procedure, written by you and executed
                 verbatim — nothing about it is parsed, generated, or
                 second-guessed. Write it as `CREATE OR REPLACE ...` so
                 it's safe to run repeatedly. If omitted (the default),
                 this Procedure is a calling-only binding for something
                 that already exists in the database — nothing tries to
                 create it. Run `connector.sync_procedures()` to actually
                 create/replace every Procedure that declared a `body`.
    """

    def __init__(
        self,
        name:    str,
        params:  Optional[List[str]] = None,
        returns: Optional[Type["PostgreSQLModel"]] = None,
        kind:    str = "function",
        body:    Optional[str] = None,
    ):
        if kind not in _VALID_KINDS:
            raise ConfigurationError(f"Invalid procedure kind '{kind}'. Must be one of: {_VALID_KINDS}")
        if kind == "procedure" and returns is not None:
            raise ConfigurationError("kind='procedure' cannot declare `returns` — CALL has no result set.")

        self.name    = name
        self.params  = list(params) if params else []
        self.returns = returns
        self.kind    = kind
        self.body    = body

        procedures.register(self)

    def __repr__(self) -> str:
        return f"Procedure({self.name!r}, kind={self.kind!r}, params={self.params!r})"


class ProcedureRegistry:
    """Singleton that holds all declared Procedure instances, by name."""

    _instance: Optional["ProcedureRegistry"] = None

    def __new__(cls) -> "ProcedureRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._procedures: Dict[str, Procedure] = {}
        return cls._instance

    def register(self, procedure: Procedure) -> None:
        """Called automatically by Procedure.__init__."""
        self._procedures[procedure.name] = procedure

    def unregister(self, name: str) -> None:
        """Mainly useful in tests for cleanup."""
        self._procedures.pop(name, None)

    def get(self, name: str) -> Procedure:
        if name not in self._procedures:
            raise ProcedureNotRegisteredError(name)
        return self._procedures[name]

    def get_or_none(self, name: str) -> Optional[Procedure]:
        return self._procedures.get(name)

    def is_registered(self, name: str) -> bool:
        return name in self._procedures

    def all(self) -> Dict[str, Procedure]:
        return dict(self._procedures)

    def names(self) -> List[str]:
        return list(self._procedures.keys())

    def __repr__(self) -> str:
        return f"ProcedureRegistry(procedures={self.names()})"


# Global singleton instance — import this everywhere
procedures = ProcedureRegistry()
