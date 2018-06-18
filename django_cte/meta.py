from __future__ import absolute_import
from __future__ import unicode_literals

import weakref

from django.db.models.expressions import Col, Expression


class CTEColumns(object):

    def __init__(self, cte):
        self._cte = weakref.ref(cte)

    def __getattr__(self, name):
        return CTEColumn(self._cte(), name)


class CTEColumn(Expression):

    def __init__(self, cte, name, output_field=None):
        self._cte = cte
        self.table_alias = cte.name
        self.name = self.alias = name
        self._output_field = output_field

    def __repr__(self):
        return "<{} {}.{}>".format(
            self.__class__.__name__,
            self._cte.name,
            self.name,
        )

    @property
    def _ref(self):
        if self._cte.query is None:
            raise ValueError(
                "cannot resolve '{cte}.{name}' in recursive CTE setup. "
                "Hint: use ExpressionWrapper({cte}.col.{name}, "
                "output_field=...)".format(cte=self._cte.name, name=self.name)
            )
        ref = self._cte._resolve_ref(self.name)
        if ref is self or self in ref.get_source_expressions():
            raise ValueError("Circular reference: {} = {}".format(self, ref))
        return ref

    @property
    def target(self):
        return self._ref.target

    @property
    def output_field(self):
        if self._output_field is not None:
            return self._output_field
        return self._ref.output_field

    def as_sql(self, compiler, connection):
        qn = compiler.quote_name_unless_alias
        ref = self._ref
        if isinstance(ref, Col) and self.name == "pk":
            column = ref.target.column
        else:
            column = self.name
        return "%s.%s" % (qn(self.table_alias), qn(column)), []

    def relabeled_clone(self, relabels):
        if self.table_alias is not None and self.table_alias in relabels:
            clone = self.copy()
            clone.table_alias = relabels[self.table_alias]
            return clone
        return self


class CTEColumnRef(Expression):

    def __init__(self, name, output_field):
        self.name = name
        self.output_field = output_field
        self._alias = None

    def resolve_expression(self, query=None, allow_joins=True, reuse=None,
                           summarize=False, for_save=False):
        ref = self.copy()
        ref._alias = query.get_initial_alias()
        return ref

    def relabeled_clone(self, relabels):
        if self._alias is not None and self._alias in relabels:
            clone = self.copy()
            clone._alias = relabels[self._alias]
            return clone
        return self

    def as_sql(self, compiler, connection):
        qn = connection.ops.quote_name
        return "%s.%s" % (self._alias, qn(self.name)), []
