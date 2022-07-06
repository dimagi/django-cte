import django
from django.db.models import Subquery


class CTESubqueryResolver(object):

    def __init__(self, annotation):
        self.annotation = annotation

    def resolve_expression(self, *args, **kw):
        # source: django.db.models.expressions.Subquery.resolve_expression
        # --- begin copied code (lightly adapted) --- #

        # Need to recursively resolve these.
        def resolve_all(child):
            if hasattr(child, 'children'):
                [resolve_all(_child) for _child in child.children]
            if hasattr(child, 'rhs'):
                child.rhs = resolve(child.rhs)

        def resolve(child):
            if hasattr(child, 'resolve_expression'):
                resolved = child.resolve_expression(*args, **kw)
                # Add table alias to the parent query's aliases to prevent
                # quoting.
                if hasattr(resolved, 'alias') and \
                        resolved.alias != resolved.target.model._meta.db_table:
                    get_query(clone).external_aliases.add(resolved.alias)
                return resolved
            return child

        # --- end copied code --- #

        if django.VERSION < (3, 0):
            def get_query(clone):
                return clone.queryset.query
        else:
            def get_query(clone):
                return clone.query

        # NOTE this uses the old (pre-Django 3) way of resolving.
        # Should a different technique should be used on Django 3+?
        clone = self.annotation.resolve_expression(*args, **kw)
        if isinstance(self.annotation, Subquery):
            for cte in getattr(get_query(clone), '_with_ctes', []):
                resolve_all(cte.query.where)
                for key, value in cte.query.annotations.items():
                    if isinstance(value, Subquery):
                        cte.query.annotations[key] = resolve(value)
        return clone
