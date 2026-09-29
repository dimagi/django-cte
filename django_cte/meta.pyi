from typing import Any

from django.db.models.base import Model
from django.db.models.expressions import Expression
from django.db.models.fields import Field
from django.db.models.query import QuerySet

from .cte import CTE

class CTEColumns:
    def __init__(self, cte: CTE[QuerySet[Model, object]]) -> None: ...
    def __getattr__(self, name: str) -> CTEColumn: ...

# `Field[Any, Any]` is django-stubs' "any field": `Field` is
# contravariant in its set type, so `object` does not fit there
class CTEColumn(Expression):
    table_alias: str | None
    name: str
    alias: str
    def __init__(
        self,
        cte: CTE[QuerySet[Model, object]],
        name: str,
        output_field: Field[Any, Any] | None = None,  # pyright: ignore[reportExplicitAny]
    ) -> None: ...
    @property
    def target(self) -> Field[Any, Any]: ...  # pyright: ignore[reportExplicitAny]

class CTEColumnRef(Expression):
    name: str
    cte_name: str
    def __init__(
        self,
        name: str,
        cte_name: str,
        output_field: Field[Any, Any],  # pyright: ignore[reportExplicitAny]
    ) -> None: ...
