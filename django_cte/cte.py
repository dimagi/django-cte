from django.db.models import Manager
from django.db.models.expressions import Ref
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
    :param materialized: Optional parameter (default: False) which enforce
    using of MATERIALIZED statement for supporting databases.
    """

    def __init__(self, queryset, name="cte", materialized=False):
        self.query = None if queryset is None else queryset.query
        self.name = name
        self.col = CTEColumns(self)
        self.materialized = materialized

    def __getstate__(self):
        return (self.query, self.name, self.materialized)

    def __setstate__(self, state):
        self.query, self.name, self.materialized = state
        self.col = CTEColumns(self)

    def __repr__(self):
        return "<With {}>".format(self.name)

    @classmethod
    def recursive(cls, make_cte_queryset, name="cte", materialized=False):
        """Recursive Common Table Expression: `WITH RECURSIVE ...`

        :param make_cte_queryset: Function taking a single argument (a
        not-yet-fully-constructed cte object) and returning a `QuerySet`
        object. The returned `QuerySet` normally consists of an initial
        statement unioned with a recursive statement.
        :param name: See `name` parameter of `__init__`.
        :param materialized: See `materialized` parameter of `__init__`.
        :returns: The fully constructed recursive cte object.
        """
        cte = cls(None, name, materialized)
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

        This CTE will be referenced by the returned queryset, but the
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
        query.deferred_loading = cte_query.deferred_loading
        if cte_query.values_select:
            query.set_values(cte_query.values_select)
            qs._iterable_class = ValuesIterable
        for alias in getattr(cte_query, "selected", None) or ():
            if alias not in cte_query.annotations:
                col = Ref(alias, cte_query.resolve_ref(alias))
                query.add_annotation(col, alias)
        if cte_query.annotations:
            for alias, value in cte_query.annotations.items():
                col = CTEColumnRef(alias, self.name, value.output_field)
                query.add_annotation(col, alias)
        query.annotation_select_mask = cte_query.annotation_select_mask

        qs.query = query
        return qs

    def _resolve_ref(self, name):
        selected = getattr(self.query, "selected", None)
        if selected and name in selected and name not in self.query.annotations:
            return Ref(name, self.query.resolve_ref(name))
        return self.query.resolve_ref(name)


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

    def as_manager(cls):
        # Address the circular dependency between
        # `CTEQuerySet` and `CTEManager`.
        manager = CTEManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)

    def _combinator_query(self, *args, **kw):
        clone = super()._combinator_query(*args, **kw)
        if clone.query.combinator:
            ctes = clone.query._with_ctes = []
            seen = {}
            for query in clone.query.combined_queries:
                for cte in getattr(query, "_with_ctes", []):
                    if seen.get(cte.name) is cte:
                        continue
                    if cte.name in seen:
                        raise ValueError(
                            f"Found two or more CTEs named '{cte.name}'. "
                            "Hint: assign a unique name to each CTE."
                        )
                    ctes.append(cte)
                    seen[cte.name] = cte
            if ctes:
                def without_ctes(query):
                    if getattr(query, "_with_ctes", None):
                        query = query.clone()
                        query._with_ctes = []
                    return query

                clone.query.combined_queries = [
                    without_ctes(query)
                    for query in clone.query.combined_queries
                ]
        return clone


class CTEManager(Manager.from_queryset(CTEQuerySet)):
    """Manager for models that perform CTE queries"""

    @classmethod
    def from_queryset(cls, queryset_class, class_name=None):
        if not issubclass(queryset_class, CTEQuerySet):
            raise TypeError(
                "models with CTE support need to use a CTEQuerySet")
        return super(CTEManager, cls).from_queryset(
            queryset_class, class_name=class_name)
