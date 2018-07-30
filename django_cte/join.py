from __future__ import absolute_import
from __future__ import unicode_literals
from django.db.models.sql.constants import INNER


class QJoin(object):
    """Join clause with join condition from Q object clause

    :param parent_alias: Alias of parent table.
    :param table_name: Name of joined table.
    :param table_alias: Alias of joined table.
    :param on_clause: Query `where_class` instance represenging the ON clause.
    :param join_type: Join type (INNER or LOUTER).
    """

    filtered_relation = None

    def __init__(self, parent_alias, table_name, table_alias,
                 on_clause, join_type=INNER, nullable=None):
        self.parent_alias = parent_alias
        self.table_name = table_name
        self.table_alias = table_alias
        self.on_clause = on_clause
        self.join_type = join_type  # LOUTER or INNER
        self.nullable = join_type != INNER if nullable is None else nullable

    def as_sql(self, compiler, connection):
        """Generate join clause SQL"""
        on_clause_sql, params = self.on_clause.as_sql(compiler, connection)
        if self.table_alias == self.table_name:
            alias = ''
        else:
            alias = ' %s' % self.table_alias
        qn = compiler.quote_name_unless_alias
        sql = '%s %s%s ON %s' % (
            self.join_type,
            qn(self.table_name),
            alias,
            on_clause_sql
        )
        return sql, params

    def relabeled_clone(self, change_map):
        return self.__class__(
            parent_alias=change_map.get(self.parent_alias, self.parent_alias),
            table_name=self.table_name,
            table_alias=change_map.get(self.table_alias, self.table_alias),
            on_clause=self.on_clause.relabeled_clone(change_map),
            join_type=self.join_type,
            nullable=self.nullable,
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (
                self.parent_alias == other.parent_alias and
                self.table_name == other.table_name and
                self.on_clause == other.on_clause and
                self.join_type == other.join_type
            )
        return False
