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

        def __init__(self, connection):
            self.connection = connection

        def as_sql(self):
            return sql, params

        def quote_name_unless_alias(self, name):
            return self.connection.ops.quote_name(name)

    class raw_cte_queryset(object):
        class query(object):
            @staticmethod
            def get_compiler(connection, *, elide_empty=None):
                return raw_cte_compiler(connection)

            @staticmethod
            def resolve_ref(name):
                return raw_cte_ref(refs[name])

    return raw_cte_queryset
