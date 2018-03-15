from __future__ import absolute_import
from __future__ import unicode_literals


def raw_cte_sql(sql, params, refs):
    """Raw CTE SQL

    :param sql: SQL query (string).
    :param params: List of bind parameters.
    :param refs: Dict of output fields: `{"name": <Field instance>}`.
    :returns: Object that can be passed to `With`.
    """

    class raw_cte_ref(object):
        def __init__(self, output_field):
            self.output_field = output_field

        def get_source_expressions(self):
            return []

    class raw_cte_compiler(object):
        @staticmethod
        def as_sql():
            return sql, params

    class raw_cte_queryset(object):
        class query(object):
            @staticmethod
            def get_compiler(connection):
                return raw_cte_compiler

            @staticmethod
            def resolve_ref(name):
                return raw_cte_ref(refs[name])

    return raw_cte_queryset
