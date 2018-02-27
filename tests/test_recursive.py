from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from unittest import SkipTest

from django.db.models import IntegerField, TextField
from django.db.models.expressions import (
    Case,
    ExpressionWrapper,
    F,
    Q,
    Value,
    When,
)
from django.db.models.functions import Concat
from django.db.utils import DatabaseError
from django.test import TestCase

from django_cte import With

from .models import Region

int_field = IntegerField()
text_field = TextField()


class TestRecursiveCTE(TestCase):

    def test_recursive_cte_query(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                parent__isnull=True
            ).values(
                "name",
                path=F("name"),
                depth=Value(0, output_field=int_field),
            ).union(
                cte.join(Region, parent=cte.col.name).values(
                    "name",
                    path=Concat(
                        cte.col.path, Value("\x01"), F("name"),
                        output_field=text_field,
                    ),
                    depth=cte.col.depth + 1,
                ),
                all=True,
            )
        cte = With.recursive(make_regions_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte).annotate(
            path=cte.col.path,
            depth=cte.col.depth,
        ).filter(depth=2).order_by("path")

        data = [(r.name, r.path.split("\x01"), r.depth) for r in regions]
        self.assertEqual(data, [
            ('moon', ['sun', 'earth', 'moon'], 2),
            ('deimos', ['sun', 'mars', 'deimos'], 2),
            ('phobos', ['sun', 'mars', 'phobos'], 2),
        ])

    def test_recursive_cte_reference_in_condition(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                parent__isnull=True
            ).values(
                "name",
                path=F("name"),
                depth=Value(0, output_field=int_field),
                is_planet=Value(0, output_field=int_field),
            ).union(
                cte.join(
                    Region, parent=cte.col.name
                ).annotate(
                    # annotations for filter and CASE/WHEN conditions
                    parent_name=ExpressionWrapper(
                        cte.col.name,
                        output_field=text_field,
                    ),
                    parent_depth=ExpressionWrapper(
                        cte.col.depth,
                        output_field=int_field,
                    ),
                ).filter(
                    ~Q(parent_name="mars"),
                ).values(
                    "name",
                    path=Concat(
                        cte.col.path, Value("\x01"), F("name"),
                        output_field=text_field,
                    ),
                    depth=cte.col.depth + 1,
                    is_planet=Case(
                        When(parent_depth=0, then=Value(1)),
                        default=Value(0),
                        output_field=int_field,
                    ),
                ),
                all=True,
            )
        cte = With.recursive(make_regions_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte).annotate(
            path=cte.col.path,
            depth=cte.col.depth,
            is_planet=cte.col.is_planet,
        ).order_by("path")

        data = [(r.path.split("\x01"), r.is_planet) for r in regions]
        print(data)
        self.assertEqual(data, [
            (["bernard's star"], 0),
            (['proxima centauri'], 0),
            (['proxima centauri', 'proxima centauri b'], 1),
            (['sun'], 0),
            (['sun', 'earth'], 1),
            (['sun', 'earth', 'moon'], 0),
            (['sun', 'mars'], 1),  # mars moons excluded: parent_name != 'mars'
            (['sun', 'mercury'], 1),
            (['sun', 'venus'], 1),
        ])

    def test_recursive_cte_with_empty_union_part(self):
        def make_regions_cte(cte):
            return Region.objects.none().union(
                cte.join(Region, parent=cte.col.name),
                all=True,
            )
        cte = With.recursive(make_regions_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte)

        print(regions.query)
        try:
            self.assertEqual(regions.count(), 0)
        except DatabaseError:
            raise SkipTest(
                "Expected failure: QuerySet omits `EmptyQuerySet` from "
                "UNION queries resulting in invalid CTE SQL"
            )

        # -- recursive query "cte" does not have the form
        # -- non-recursive-term UNION [ALL] recursive-term
        # WITH RECURSIVE cte AS (
        #   SELECT "tests_region"."name", "tests_region"."parent_id"
        #   FROM "tests_region", "cte"
        #   WHERE "tests_region"."parent_id" = ("cte"."name")
        # )
        # SELECT COUNT(*)
        # FROM "tests_region", "cte"
        # WHERE "tests_region"."name" = ("cte"."name")

    def test_circular_ref_error(self):
        def make_bad_cte(cte):
            # NOTE: not a valid recursive CTE query
            return cte.join(Region, parent=cte.col.name).values(
                depth=cte.col.depth + 1,
            )
        cte = With.recursive(make_bad_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte)
        with self.assertRaises(ValueError) as context:
            print(regions.query)
        self.assertIn("Circular reference:", str(context.exception))
