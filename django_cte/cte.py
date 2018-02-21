from __future__ import absolute_import
from __future__ import unicode_literals

import django
from django.db.models import Manager
from django.db.models.query import QuerySet

from .meta import CTEColumns, CTEModel
from .query import CTEQuery

__all__ = ["With", "CTEManager", "CTEQuerySet"]


class With(object):
    """Common Table Expression query object: `WITH ...`

    :param queryset: A queryset to use as the body of the CTE.
    :param name: Optional name parameter for the CTE (default: "cte").
    This must be a unique name that does not conflict with other
    entities (tables, views, functions, other CTE(s), etc.) referenced
    in the given query as well any query to which this CTE will
    eventually be added.
    """

    def __init__(self, queryset, name="cte"):
        self._queryset = queryset
        self.name = name
        self.col = CTEColumns(self)

    def __repr__(self):
        return "<With {}>".format(self.name)

    @classmethod
    def recursive(cls, make_cte_queryset, name="cte"):
        """Recursive Common Table Expression: `WITH RECURSIVE ...`

        :param make_cte_queryset: Function taking a single argument (a
        not-yet-fully-constructed cte object) and returning a `QuerySet`
        object. The returned `QuerySet` normally consists of an initial
        statement unioned with a recursive statement.
        :param name: See `name` parameter of `__init__`.
        :returns: The fully constructed recursive cte object.
        """
        cte = cls(None, name)
        cte._queryset = make_cte_queryset(cte)
        return cte

    def join(self, model_or_queryset, *filter_q, **filter_kw):
        """Join this CTE to the given model or queryset

        This CTE will be refernced by the returned queryset, but the
        corresponding `WITH ...` statement will not be prepended to the
        queryset's SQL output; use `<CTEQuerySet>.with_cte(cte)` to
        achieve that outcome.

        :param model_or_queryset: Model class or queryset to which the
        CTE should be joined.
        :param *filter_q: Join condition Q expressions (optional).
        :param **filter_kw: Join conditions. All LHS fields (kwarg keys)
        are assumed to reference `model_or_queryset` fields. Use
        `cte.col.name` on the RHS to recursively reference CTE query
        columns. For example: `cte.join(Book, id=cte.col.id)`
        :returns: A queryset with the given model or queryset joined to
        this CTE.
        """
        if isinstance(model_or_queryset, QuerySet):
            queryset = model_or_queryset.all()
        else:
            queryset = model_or_queryset.objects.all()
        query = queryset.query
        self._add_to_query(query)
        return queryset.filter(*filter_q, **filter_kw)

    def queryset(self, model=None):
        """Get a queryset selecting from this CTE

        This CTE will be refernced by the returned queryset, but the
        corresponding `WITH ...` statement will not be prepended to the
        queryset's SQL output; use `<CTEQuerySet>.with_cte(cte)` to
        achieve that outcome.

        :param model: Optional model class to use as the queryset's
        primary model. If this is provided the CTE will be added to
        the queryset's table list and join conditions may be added
        with a subsequent `.filter(...)` call.
        :returns: A queryset.
        """
        query = CTEQuery(model)
        if model is None:
            model = query.model = CTEModel(self, query)
        else:
            self._add_to_query(query)
        return CTEQuerySet(model, query)

    def _add_to_query(self, query):
        django1 = django.VERSION < (2, 0)
        tables = query.tables if django1 else query.extra_tables
        if not tables:
            # prevent CTE becoming the initial alias
            query.get_initial_alias()
        name = self.name
        if name in tables:
            raise ValueError(
                "cannot add CTE with name '%s' because an entity with that "
                "name is already referenced in this query's FROM clause" % name
            )
        query.extra_tables += (name,)

    def _resolve_ref(self, name):
        return self._queryset.query.resolve_ref(name)


class CTEManager(Manager):
    """Manager for models that perform CTE queries"""

    def get_queryset(self):
        return CTEQuerySet(self.model, using=self._db)


class CTEQuerySet(QuerySet):
    """QuerySet with support for Common Table Expressions"""

    def __init__(self, model=None, query=None, using=None, hints=None):
        # Only create an instance of a Query if this is the first invocation in
        # a query chain.
        if query is None:
            query = CTEQuery(model)
        super(CTEQuerySet, self).__init__(model, query, using, hints)

    def with_cte(self, cte):
        """Add a Common Table Expression to this queryset

        The CTE `WITH ...` clause will be added to the queryset's SQL
        output (after other CTEs that have already been added) so it
        can be referenced in annotations, filters, etc.
        """
        qs = self._clone()
        qs.query._with_ctes.append(cte)
        return qs
