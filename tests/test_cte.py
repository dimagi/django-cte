from __future__ import absolute_import
from __future__ import unicode_literals

from django.db.models import IntegerField, TextField
from django.db.models.aggregates import Sum
from django.db.models.expressions import F, Value
from django.db.models.functions import Concat
from django.test import TestCase

from django_cte import With

from .models import Order, Region

int_field = IntegerField()
text_field = TextField()


class TestCTE(TestCase):

    def test_simple_cte_query(self):
        totals = With(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount"))
        )
        orders = (
            totals
            .join(Order, region=totals.col.region_id)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
            .order_by("amount")
        )

        data = [(o.amount, o.region_id, o.region_total) for o in orders]
        self.assertEqual(data, [
            (10, 'mercury', 33),
            (11, 'mercury', 33),
            (12, 'mercury', 33),
            (20, 'venus', 86),
            (21, 'venus', 86),
            (22, 'venus', 86),
            (23, 'venus', 86),
            (30, 'earth', 126),
            (31, 'earth', 126),
            (32, 'earth', 126),
            (33, 'earth', 126),
            (40, 'mars', 123),
            (41, 'mars', 123),
            (42, 'mars', 123),
        ])

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
                    depth=cte.col.depth + Value(1, output_field=int_field),
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
