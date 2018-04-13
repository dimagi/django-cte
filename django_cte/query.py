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

    def get_compiler(self, using=None, connection=None):
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
        return klass(self, connection, using)

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

    TEMPLATE = "{name} AS ({query})"

    @classmethod
    def generate_sql(cls, connection, query, as_sql):
        if query.combinator:
            return as_sql()

        ctes = []
        params = []
        for cte in query._with_ctes:
            compiler = cte.query.get_compiler(connection=connection)
            cte_sql, cte_params = compiler.as_sql()
            ctes.append(cls.TEMPLATE.format(name=cte.name, query=cte_sql))
            params.extend(cte_params)

        # Always use WITH RECURSIVE
        # https://www.postgresql.org/message-id/13122.1339829536%40sss.pgh.pa.us
        sql = ["WITH RECURSIVE", ", ".join(ctes)] if ctes else []
        base_sql, base_params = as_sql()
        sql.append(base_sql)
        params.extend(base_params)
        return " ".join(sql), tuple(params)


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
