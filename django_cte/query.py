from __future__ import absolute_import
from __future__ import unicode_literals

import django
from django.db import connections
from django.db.models.sql import DeleteQuery, Query, UpdateQuery
from django.db.models.sql.compiler import (
    SQLCompiler,
    SQLDeleteCompiler,
    SQLUpdateCompiler,
)

from .expressions import CTESubqueryResolver


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

    def get_compiler(self, using=None, connection=None, *args, **kwargs):
        """ Overrides the Query method get_compiler in order to return
            a CTECompiler.
        """
        # Copy the body of this method from Django except the final
        # return statement. We will ignore code coverage for this.
        if using is None and connection is None:  # pragma: no cover
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        # Check that the compiler will be able to execute the query
        for alias, aggregate in self.annotation_select.items():
            connection.ops.check_expression_support(aggregate)
        # Instantiate the custom compiler.
        klass = COMPILER_TYPES.get(self.__class__, CTEQueryCompiler)
        return klass(self, connection, using, *args, **kwargs)

    def add_annotation(self, annotation, *args, **kw):
        annotation = CTESubqueryResolver(annotation)
        super(CTEQuery, self).add_annotation(annotation, *args, **kw)

    def __chain(self, _name, klass=None, *args, **kwargs):
        klass = QUERY_TYPES.get(klass, self.__class__)
        clone = getattr(super(CTEQuery, self), _name)(klass, *args, **kwargs)
        clone._with_ctes = self._with_ctes[:]
        return clone

    if django.VERSION < (2, 0):
        def clone(self, klass=None, *args, **kwargs):
            return self.__chain("clone", klass, *args, **kwargs)

    else:
        def chain(self, klass=None):
            return self.__chain("chain", klass)


class CTECompiler(object):

    @classmethod
    def generate_sql(cls, connection, query, as_sql):
        if query.combinator:
            return as_sql()

        ctes = []
        params = []
        for cte in query._with_ctes:
            compiler = cte.query.get_compiler(connection=connection)
            qn = compiler.quote_name_unless_alias
            cte_sql, cte_params = compiler.as_sql()
            template = cls.get_cte_query_template(cte)
            ctes.append(template.format(name=qn(cte.name), query=cte_sql))
            params.extend(cte_params)

        explain_query = getattr(query, "explain_query", None)
        sql = []
        if explain_query:
            explain_format = getattr(query, "explain_format", None)
            explain_options = getattr(query, "explain_options", {})
            sql.append(
                connection.ops.explain_query_prefix(
                    explain_format,
                    **explain_options
                )
            )
            # this needs to get set to False so that the base as_sql() doesn't
            # insert the EXPLAIN statement where it would end up between the
            # WITH ... clause and the final SELECT
            query.explain_query = False

        if ctes:
            # Always use WITH RECURSIVE
            # https://www.postgresql.org/message-id/13122.1339829536%40sss.pgh.pa.us
            sql.extend(["WITH RECURSIVE", ", ".join(ctes)])
        base_sql, base_params = as_sql()

        if explain_query:
            query.explain_query = explain_query

        sql.append(base_sql)
        params.extend(base_params)
        return " ".join(sql), tuple(params)

    @classmethod
    def get_cte_query_template(cls, cte):
        if cte.materialized:
            return "{name} AS MATERIALIZED ({query})"
        return "{name} AS ({query})"


class CTEUpdateQuery(UpdateQuery, CTEQuery):
    pass


class CTEDeleteQuery(DeleteQuery, CTEQuery):
    pass


QUERY_TYPES = {
    UpdateQuery: CTEUpdateQuery,
    DeleteQuery: CTEDeleteQuery,
}


class CTEQueryCompiler(SQLCompiler):

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


class CTEUpdateQueryCompiler(SQLUpdateCompiler):

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEUpdateQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


class CTEDeleteQueryCompiler(SQLDeleteCompiler):

    # NOTE: it is currently not possible to execute delete queries that
    # reference CTEs without patching `QuerySet.delete` (Django method)
    # to call `self.query.chain(sql.DeleteQuery)` instead of
    # `sql.DeleteQuery(self.model)`

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEDeleteQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


COMPILER_TYPES = {
    CTEUpdateQuery: CTEUpdateQueryCompiler,
    CTEDeleteQuery: CTEDeleteQueryCompiler,
}
