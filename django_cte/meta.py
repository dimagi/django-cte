from __future__ import absolute_import
from __future__ import unicode_literals

import weakref

from django.db.models.expressions import Col, Expression
from django.db.models.options import Options


class CTEModel(object):

    def __init__(self, cte, query):
        self._meta = CTEMeta(cte, query)

    def _copy_for_query(self, query):
        return type(self)(self._meta._cte(), query)


class CTEMeta(Options):

    def __init__(self, cte, query):
        super(CTEMeta, self).__init__(None)
        self.managed = False
        self.model = None
        self._cte = weakref.ref(cte)
        self._query = weakref.ref(query)

    @property
    def db_table(self):
        return self._cte().name

    @db_table.setter
    def db_table(self, value):
        if value != '':
            raise AttributeError("CTEMeta.db_table is read-only")

    @property
    def local_fields(self):
        cte = self._cte()
        query = cte._queryset.query
        opts = query.get_meta()
        fields = []
        if query.default_cols:
            assert not query.select, query.select
            fields.extend(opts.concrete_fields)
        else:
            fields.extend(
                CTEField(cte, col.target.column, col.output_field)
                for col in query.select
            )
        fields.extend(
            CTEField(cte, alias, annotation.output_field)
            for alias, annotation in query.annotation_select.items()
        )
        return fields

    @local_fields.setter
    def local_fields(self, value):
        if value != []:
            raise AttributeError("CTEMeta.local_fields is read-only")

    @property
    def _relation_tree(self):
        return []


class CTEColumns(object):

    def __init__(self, cte):
        self._cte = weakref.ref(cte)

    def __getattr__(self, name):
        return CTEColumn(self._cte(), name)


class CTERef(object):

    def __init__(self, cte, name, output_field=None):
        self._cte = cte
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
        if self._cte._queryset is None:
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


class CTEColumn(CTERef, Expression):

    def as_sql(self, compiler, connection):
        qn = compiler.quote_name_unless_alias
        ref = self._ref
        if isinstance(ref, Col) and self.name == "pk":
            column = ref.target.column
        else:
            column = self.name
        return "%s.%s" % (qn(self._cte.name), qn(column)), []


class CTEField(CTERef):

    concrete = False
    is_relation = False

    def get_col(self, alias, output_field=None):
        output_field = output_field or self.output_field
        return Col(alias, self, output_field)

    @property
    def model(self):
        query = self._cte._queryset.query
        model = query.model
        if isinstance(model, CTEModel) and model._meta._cte() is self._cte:
            return model
        return CTEModel(self._cte, query)

    @property
    def column(self):
        return self.name

    def __getattr__(self, name):
        return getattr(self.output_field, name)
