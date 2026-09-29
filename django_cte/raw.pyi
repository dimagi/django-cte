from collections.abc import Mapping, Sequence
from typing import Any

from django.db.models.fields import Field

class RawCTEQuerySet:
    """Opaque result of `raw_cte_sql()`, only usable as a `CTE` body"""

def raw_cte_sql(
    sql: str,
    params: Sequence[object],
    # `Field[Any, Any]` is django-stubs' "any field"
    refs: Mapping[str, Field[Any, Any]],  # pyright: ignore[reportExplicitAny]
) -> type[RawCTEQuerySet]: ...
