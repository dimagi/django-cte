from __future__ import absolute_import
from __future__ import unicode_literals

from unittest import SkipTest

from django.db.models import IntegerField, TextField
from django.db.models.aggregates import Sum
from django.db.models.expressions import Exists, F, OuterRef, Subquery, Value
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

    def test_cte_queryset(self):
        sub_totals = With(
            Order.objects
            .values(region_parent=F("region__parent_id"))
            .annotate(total=Sum("amount")),
        )
        regions = (
            Region.objects.all()
            .with_cte(sub_totals)
            .annotate(
                child_regions_total=Subquery(
                    sub_totals.queryset()
                    .filter(region_parent=OuterRef("name"))
                    .values("total")
                ),
            )
            .order_by("name")
        )

        data = [(r.name, r.child_regions_total) for r in regions]
        self.assertEqual(data, [
            ("bernard's star", None),
            ('deimos', None),
            ('earth', 6),
            ('mars', None),
            ('mercury', None),
            ('moon', None),
            ('phobos', None),
            ('proxima centauri', 33),
            ('proxima centauri b', None),
            ('sun', 368),
            ('venus', None)
        ])

    def test_update_cte_query(self):
        cte = With(
            Order.objects
            .values(region_parent=F("region__parent_id"))
            .annotate(total=Sum("amount"))
            .filter(total__isnull=False)
        )
        # not the most efficient query, but it exercises CTEUpdateQuery
        Order.objects.all().with_cte(cte).filter(region_id__in=Subquery(
            cte.queryset()
            .filter(region_parent=OuterRef("region_id"))
            .values("region_parent")
        )).update(amount=Subquery(
            cte.queryset()
            .filter(region_parent=OuterRef("region_id"))
            .values("total")
        ))

        data = set((o.region_id, o.amount) for o in Order.objects.filter(
            region_id__in=["earth", "sun", "proxima centauri", "mars"]
        ))
        self.assertEqual(data, {
            ('earth', 6),
            ('mars', 40),
            ('mars', 41),
            ('mars', 42),
            ('proxima centauri', 33),
            ('sun', 368),
        })

    def test_delete_cte_query(self):
        raise SkipTest(
            "this test will not work until `QuerySet.delete` (Django method) "
            "calls `self.query.chain(sql.DeleteQuery)` instead of "
            "`sql.DeleteQuery(self.model)`"
        )
        cte = With(
            Order.objects
            .values(region_parent=F("region__parent_id"))
            .annotate(total=Sum("amount"))
            .filter(total__isnull=False)
        )
        Order.objects.all().with_cte(cte).annotate(
            cte_has_order=Exists(
                cte.queryset()
                .values("total")
                .filter(region_parent=OuterRef("region_id"))
            )
        ).filter(cte_has_order=False).delete()

        data = [(o.region_id, o.amount) for o in Order.objects.all()]
        self.assertEqual(data, [
            ('sun', 1000),
            ('earth', 30),
            ('earth', 31),
            ('earth', 32),
            ('earth', 33),
            ('proxima centauri', 2000),
        ])

    def test_insert_cte_query(self):
        raise SkipTest(
            "This test not implemented because `Query.bulk_create` calls "
            "`sql.InsertQuery(self.model)` rather than "
            "`self.query.chain(sql.InsertQuery)` to create the insert query. "
        )
        # additionally `QuerySet.bulk_create` seems to ignore any filters,
        # annotations, etc., so it appears there is no way to construct an
        # insert query that uses a CTE
