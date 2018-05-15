from __future__ import absolute_import
from __future__ import unicode_literals

from django.db.models import Manager
from django.db.models.query import Q, QuerySet, ValuesIterable
from django.db.models.sql.datastructures import BaseTable

from .join import QJoin, INNER
from .meta import CTEColumnRef, CTEColumns
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
        self.query = None if queryset is None else queryset.query
        self.name = name
        self.col = CTEColumns(self)

    def __getstate__(self):
        return (self.query, self.name)

    def __setstate__(self, state):
        self.query, self.name = state
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
        cte.query = make_cte_queryset(cte).query
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
            queryset = model_or_queryset._default_manager.all()
        join_type = filter_kw.pop("_join_type", INNER)
        query = queryset.query

        # based on Query.add_q: add necessary joins to query, but no filter
        q_object = Q(*filter_q, **filter_kw)
        map = query.alias_map
        existing_inner = set(a for a in map if map[a].join_type == INNER)
        on_clause, _ = query._add_q(q_object, query.used_aliases)
        query.demote_joins(existing_inner)

        parent = query.get_initial_alias()
        query.join(QJoin(parent, self.name, self.name, on_clause, join_type))
        return queryset

    def queryset(self):
        """Get a queryset selecting from this CTE

        This CTE will be refernced by the returned queryset, but the
        corresponding `WITH ...` statement will not be prepended to the
        queryset's SQL output; use `<CTEQuerySet>.with_cte(cte)` to
        achieve that outcome.

        :returns: A queryset.
        """
        cte_query = self.query
        qs = cte_query.model._default_manager.get_queryset()

        query = CTEQuery(cte_query.model)
        query.join(BaseTable(self.name, None))
        query.default_cols = cte_query.default_cols
        if cte_query._annotations:
            for alias, value in cte_query.annotations.items():
                col = CTEColumnRef(alias, value.output_field)
                query.add_annotation(col, alias)
        if cte_query.values_select:
            query.set_values(cte_query.values_select)
            qs._iterable_class = ValuesIterable
        query.annotation_select_mask = cte_query.annotation_select_mask

        qs.query = query
        return qs

    def _resolve_ref(self, name):
        return self.query.resolve_ref(name)


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
