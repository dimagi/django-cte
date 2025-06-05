import django
from django.core.exceptions import EmptyResultSet
from django.db import connections
from django.db.models.sql import DeleteQuery, Query, UpdateQuery
from django.db.models.sql.constants import LOUTER

from .join import QJoin


class CTEQuery(Query):
    """A Query which processes SQL compilation through the CTE compiler"""

    def __init__(self, *args, **kwargs):
        super(CTEQuery, self).__init__(*args, **kwargs)
        self._with_ctes = []

    def combine(self, other, connector):
        if other._with_ctes:
            if self._with_ctes:
                raise TypeError("cannot merge queries with CTEs on both sides")
            self._with_ctes = other._with_ctes[:]
        return super(CTEQuery, self).combine(other, connector)

    def resolve_expression(self, *args, **kwargs):
        clone = super().resolve_expression(*args, **kwargs)
        clone._with_ctes = [
            cte.resolve_expression(*args, **kwargs)
            for cte in clone._with_ctes
        ]
        return clone

    def get_compiler(self, using=None, connection=None, *args, **kwargs):
        """ Overrides the Query method get_compiler in order to return
            a CTECompiler.
        """
        # Copy the body of this method from Django except the final
        # return statement. We will ignore code coverage for this.
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]

        compiler_class = connection.ops.compiler(self.compiler)
        cte_compiler = mixin_class(CTECompiler, compiler_class)
        return cte_compiler(self, connection, using, *args, **kwargs)

    def __chain(self, _name, klass=None, *args, **kwargs):
        klass = QUERY_TYPES.get(klass, self.__class__)
        clone = getattr(super(CTEQuery, self), _name)(klass, *args, **kwargs)
        clone._with_ctes = self._with_ctes[:]
        return clone

    def chain(self, klass=None):
        return self.__chain("chain", klass)


def generate_cte_sql(connection, query, as_sql):
    if not query._with_ctes:
        return as_sql()

    ctes = []
    params = []
    for cte in query._with_ctes:
        if django.VERSION > (4, 2):
            _ignore_with_col_aliases(cte.query)

        alias = query.alias_map.get(cte.name)
        should_elide_empty = (
                not isinstance(alias, QJoin) or alias.join_type != LOUTER
        )

        compiler = cte.query.get_compiler(
            connection=connection, elide_empty=should_elide_empty
        )

        qn = compiler.quote_name_unless_alias
        try:
            cte_sql, cte_params = compiler.as_sql()
        except EmptyResultSet:
            # If the CTE raises an EmptyResultSet the SqlCompiler still
            # needs to know the information about this base compiler
            # like, col_count and klass_info.
            as_sql()
            raise
        template = get_cte_query_template(cte)
        ctes.append(template.format(name=qn(cte.name), query=cte_sql))
        params.extend(cte_params)

    explain_attribute = "explain_info"
    explain_info = getattr(query, explain_attribute, None)
    explain_format = getattr(explain_info, "format", None)
    explain_options = getattr(explain_info, "options", {})

    explain_query_or_info = getattr(query, explain_attribute, None)
    sql = []
    if explain_query_or_info:
        sql.append(
            connection.ops.explain_query_prefix(
                explain_format,
                **explain_options
            )
        )
        # this needs to get set to None so that the base as_sql() doesn't
        # insert the EXPLAIN statement where it would end up between the
        # WITH ... clause and the final SELECT
        setattr(query, explain_attribute, None)

    if ctes:
        # Always use WITH RECURSIVE
        # https://www.postgresql.org/message-id/13122.1339829536%40sss.pgh.pa.us
        sql.extend(["WITH RECURSIVE", ", ".join(ctes)])
    base_sql, base_params = as_sql()

    if explain_query_or_info:
        setattr(query, explain_attribute, explain_query_or_info)

    sql.append(base_sql)
    params.extend(base_params)
    return " ".join(sql), tuple(params)


def get_cte_query_template(cte):
    if cte.materialized:
        return "{name} AS MATERIALIZED ({query})"
    return "{name} AS ({query})"


class CTEUpdateQuery(UpdateQuery, CTEQuery):
    pass


class CTEDeleteQuery(DeleteQuery, CTEQuery):
    # NOTE: it is currently not possible to execute delete queries that
    # reference CTEs without patching `QuerySet.delete` (Django method)
    # to call `self.query.chain(sql.DeleteQuery)` instead of
    # `sql.DeleteQuery(self.model)`
    pass


QUERY_TYPES = {
    Query: CTEQuery,
    UpdateQuery: CTEUpdateQuery,
    DeleteQuery: CTEDeleteQuery,
}


def _ignore_with_col_aliases(cte_query):
    if getattr(cte_query, "combined_queries", None):
        for query in cte_query.combined_queries:
            query.ignore_with_col_aliases = True


class CTECompiler:  # mixin for django.db.models.sql.compiler.SQLCompiler

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTECompiler, self).as_sql(*args, **kwargs)
        return generate_cte_sql(self.connection, self.query, _as_sql)

    def get_select(self, **kw):
        if kw.get("with_col_aliases") \
                and getattr(self.query, "ignore_with_col_aliases", False):
            kw.pop("with_col_aliases")
        return super().get_select(**kw)


def mixin_class(mixin, base_class):
    if issubclass(base_class, mixin):
        return base_class
    mixed = _mixin_cache.get(base_class)
    if mixed is None:
        name = f"{mixin.__name__}{base_class.__name__}"
        mixed = _mixin_cache[base_class] = type(name, (mixin, base_class), {})
    return mixed


_mixin_cache = {}
