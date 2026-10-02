from collections.abc import Callable
from typing import Any, Generic, TypeVar, overload

from django.db.models import Manager, Q
from django.db.models.base import Model
from django.db.models.query import QuerySet
from django.db.models.sql import Query
from typing_extensions import Self, deprecated, override

from .meta import CTEColumns
from .raw import RawCTEQuerySet

__all__ = ["CTE", "with_cte"]

_M = TypeVar("_M", bound=Model)
_M_co = TypeVar("_M_co", bound=Model, covariant=True)
_R_co = TypeVar("_R_co", covariant=True)
# `object` rows: the row type of `QuerySet` is covariant, so
# `QuerySet[Model, object]` takes any queryset, no `Any` needed
_QS = TypeVar("_QS", bound=QuerySet[Model, object])
_QS_co = TypeVar("_QS_co", bound=QuerySet[Model, object], covariant=True)

@overload
def with_cte(*ctes: CTE[QuerySet[Model, object]], select: CTE[_QS]) -> _QS: ...
@overload
def with_cte(*ctes: CTE[QuerySet[Model, object]], select: _QS) -> _QS: ...
@overload
def with_cte(
    *ctes: CTE[QuerySet[Model, object]], select: type[_M]
) -> QuerySet[_M, _M]: ...

class CTE(Generic[_QS_co]):
    """Common Table Expression

    The type parameter is the type of the queryset making up the body
    of the CTE, e.g. `CTE[QuerySet[Order, dict[str, Any]]]`.
    """

    query: Query | None
    name: str
    col: CTEColumns
    materialized: bool
    @overload
    def __init__(
        self,
        queryset: _QS_co | None,
        name: str = "cte",
        materialized: bool = False,
    ) -> None: ...
    @overload
    def __init__(
        # `Any` values: the row types of raw SQL are unknown
        self: CTE[QuerySet[Model, dict[str, Any]]],  # pyright: ignore[reportExplicitAny]
        queryset: type[RawCTEQuerySet],
        name: str = "cte",
        materialized: bool = False,
    ) -> None: ...
    @classmethod
    def recursive(
        cls,
        make_cte_queryset: Callable[[CTE[_QS]], _QS],
        name: str = "cte",
        materialized: bool = False,
    ) -> CTE[_QS]: ...
    @overload
    def join(
        self,
        model_or_queryset: _QS,
        *filter_q: Q,
        _join_type: str = ...,
        **filter_kw: object,
    ) -> _QS: ...
    @overload
    def join(
        self,
        model_or_queryset: type[_M],
        *filter_q: Q,
        _join_type: str = ...,
        **filter_kw: object,
    ) -> QuerySet[_M, _M]: ...
    def queryset(self) -> _QS_co: ...
    def resolve_expression(self, *args: object, **kw: object) -> Self: ...

@deprecated("Use `django_cte.CTE` instead.")
class With(CTE[_QS_co]):
    @staticmethod
    @override
    @deprecated("Use `django_cte.CTE.recursive` instead.")
    def recursive(  # pyright: ignore[reportIncompatibleMethodOverride]
        make_cte_queryset: Callable[[CTE[_QS]], _QS],
        name: str = "cte",
        materialized: bool = False,
    ) -> CTE[_QS]: ...

# Message split as in cte.py, where the runtime message lives
@deprecated(
    "CTEQuerySet is deprecated. "  # pyright: ignore[reportImplicitStringConcatenation]
    "CTEs can now be applied to any queryset using `with_cte()`"
)
class CTEQuerySet(QuerySet[_M_co, _R_co]):
    """QuerySet with support for Common Table Expressions"""

    @deprecated("Use `django_cte.with_cte(cte, select=...)` instead.")
    def with_cte(self, cte: CTE[QuerySet[Model, object]]) -> Self: ...

# The deprecated manager returns the deprecated queryset, hence the
# `reportDeprecated` ignores
class _CTEManagerBase(Manager[_M_co]):
    @override
    def get_queryset(self) -> CTEQuerySet[_M_co, _M_co]: ...  # pyright: ignore[reportDeprecated]
    @override
    def all(self) -> CTEQuerySet[_M_co, _M_co]: ...  # pyright: ignore[reportDeprecated]
    @deprecated("Use `django_cte.with_cte(cte, select=...)` instead.")
    def with_cte(
        self, cte: CTE[QuerySet[Model, object]]
    ) -> CTEQuerySet[_M_co, _M_co]: ...  # pyright: ignore[reportDeprecated]

# Message split as in cte.py, where the runtime message lives
@deprecated(
    "CTEMAnager is deprecated. "  # pyright: ignore[reportImplicitStringConcatenation]
    "CTEs can now be applied to any queryset using `with_cte()`"
)
class CTEManager(_CTEManagerBase[_M_co]):
    """Manager for models that perform CTE queries"""
