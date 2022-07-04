from __future__ import absolute_import
from __future__ import unicode_literals

import django
from django.db import connections
from django.db.models.expressions import Col
from django.db.models.sql import DeleteQuery, Query, RawQuery, UpdateQuery
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

    def relabeled_clone(self, change_map):
        obj = super().relabeled_clone(change_map)
        for cte in self._with_ctes[:]:
            if cte.name in change_map:
                cte.name = change_map[cte.name]
        return obj

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
        # Should we clone the cte here?
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
    def generate_sql(cls, connection, query, with_col_aliases, as_sql):
        if query.combinator:
            return as_sql()

        ctes = []
        params = []
        if django.VERSION > (4, 2):
            named_ctes = {cte.name: cte for cte in query._with_ctes}
            for cte in named_ctes.values():
                if isinstance(cte.query, RawQuery):
                    pass
                if isinstance(cte.query, Query):
                    if cte.query.combinator:
                        for subquery in cte.query.combined_queries:
                            compiler = subquery.get_compiler(
                                connection=connection)
                            for idx, col in enumerate(
                                     compiler.get_select_renamed_cols(),
                                     start=1):
                                column = getattr(cte.col, col.target.name)
                                column.alias = column.name

        base_sql, base_params = as_sql(
            with_col_aliases=with_col_aliases | bool(query._with_ctes))
        for cte in query._with_ctes:
            compiler = cte.query.get_compiler(connection=connection)
            qn = compiler.quote_name_unless_alias
            cte_sql, cte_params = compiler.as_sql(
                with_col_aliases=with_col_aliases)
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
    Query: CTEQuery,
    UpdateQuery: CTEUpdateQuery,
    DeleteQuery: CTEDeleteQuery,
}


class CTEQueryCompiler(SQLCompiler):

    if django.VERSION > (4, 2):
        def get_select(self, *args, **kwargs):
            _columns, klass_info, annotations = super().get_select(
                *args, **kwargs)
            columns = []
            named_ctes = {cte.name: cte for cte in self.query._with_ctes}
            for idx, (expression, sql, calias) in enumerate(_columns, start=1):
                if (calias and calias == f'col{idx}'):
                    if isinstance(expression, Col):
                        if expression.alias in named_ctes.keys():
                            pass
                            # NOTE: Test needed to hit this condition
                            # columns.append([expression, sql,
                            #                'sss' + expression.target.column])
                            # continue
                        elif any(map(
                                lambda tables: expression.alias in tables,
                                self.query.table_map.values()
                        )):
                            columns.append([expression, sql,
                                            expression.target.column])
                            continue
                columns.append([expression, sql, calias])
            return columns, klass_info, annotations

    def get_select_renamed_cols(self):
        assert not (self.query.select and self.query.default_cols)
        select_mask = self.query.get_select_mask()
        if self.query.default_cols:
            cols = self.get_default_columns(select_mask)
        else:
            # self.query.select is a special case. These columns never go to
            # any model.
            cols = self.query.select
        if cols:
            return cols
        return []

    def as_sql(self, *args, **kwargs):
        with_col_aliases = kwargs.get('with_col_aliases', False)

        def _as_sql(with_col_aliases=False):
            _with_col_aliases = (
                kwargs.pop('with_col_aliases', False) or with_col_aliases)
            return super(CTEQueryCompiler, self).as_sql(
                *args, with_col_aliases=_with_col_aliases, **kwargs)

        # We need to ensure that all cols of the CTEs subqueries have aliases.
        sql = CTECompiler.generate_sql(
            self.connection, self.query, with_col_aliases, _as_sql)
        return sql


class CTEUpdateQueryCompiler(SQLUpdateCompiler):

    def get_select_renamed_cols(self,):
        assert not (self.query.select and self.query.default_cols)
        select_mask = self.query.get_select_mask()
        if self.query.default_cols:
            cols = self.get_default_columns(select_mask)
        else:
            # self.query.select is a special case. These columns never go to
            # any model.
            cols = self.query.select
        if cols:
            return cols
        return []

    def as_sql(self, *args, **kwargs):
        with_col_aliases = kwargs.get('with_col_aliases', False)

        def _as_sql(with_col_aliases=False):
            return super(CTEUpdateQueryCompiler, self).as_sql(
                *args,
                **kwargs)

        return CTECompiler.generate_sql(
            self.connection, self.query, with_col_aliases, _as_sql)


class CTEDeleteQueryCompiler(SQLDeleteCompiler):

    def get_select_renamed_cols(self):
        assert not (self.query.select and self.query.default_cols)
        select_mask = self.query.get_select_mask()
        if self.query.default_cols:
            cols = self.get_default_columns(select_mask)
        else:
            # self.query.select is a special case. These columns never go to
            # any model.
            cols = self.query.select
        if cols:
            return cols
        return []

    # NOTE: it is currently not possible to execute delete queries that
    # reference CTEs without patching `QuerySet.delete` (Django method)
    # to call `self.query.chain(sql.DeleteQuery)` instead of
    # `sql.DeleteQuery(self.model)`

    def as_sql(self, *args, **kwargs):
        with_col_aliases = kwargs.get('with_col_aliases', False)

        def _as_sql(with_col_aliases=False):
            return super(CTEDeleteQueryCompiler, self).as_sql(*args, **kwargs)

        return CTECompiler.generate_sql(
            self.connection, self.query, with_col_aliases, _as_sql)


COMPILER_TYPES = {
    CTEUpdateQuery: CTEUpdateQueryCompiler,
    CTEDeleteQuery: CTEDeleteQueryCompiler,
}
