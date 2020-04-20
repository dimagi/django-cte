from __future__ import absolute_import
from __future__ import unicode_literals

from __future__ import print_function
from unittest import SkipTest

from django.db.models import IntegerField, TextField
from django.db.models.aggregates import Count, Max, Min, Sum
from django.db.models.expressions import Exists, F, OuterRef, Subquery, Value
from django.db.models.functions import Concat
from django.test import TestCase

from django_cte import CTESubquery, With
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
                    .values("total"),
                    output_field=int_field  # needed for Django 1.11, not 2.x
                ),
            )
            .order_by("name")
        )
        print(regions.query)

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

    def test_cte_queryset_with_model_result(self):
        cte = With(
            Order.objects
            .annotate(region_parent=F("region__parent_id")),
        )
        orders = cte.queryset().with_cte(cte).order_by("region_id", "amount")
        print(orders.query)

        data = [(x.region_id, x.amount, x.region_parent) for x in orders][:5]
        self.assertEqual(data, [
            ("earth", 30, "sun"),
            ("earth", 31, "sun"),
            ("earth", 32, "sun"),
            ("earth", 33, "sun"),
            ("mars", 40, "sun"),
        ])
        self.assertTrue(
            all(isinstance(x, Order) for x in orders),
            repr([x for x in orders]),
        )

    def test_cte_queryset_with_join(self):
        cte = With(
            Order.objects
            .annotate(region_parent=F("region__parent_id")),
        )
        orders = (
            cte.queryset()
            .with_cte(cte)
            .annotate(parent=F("region__parent_id"))
            .order_by("region_id", "amount")
        )
        print(orders.query)

        data = [(x.region_id, x.region_parent, x.parent) for x in orders][:5]
        self.assertEqual(data, [
            ("earth", "sun", "sun"),
            ("earth", "sun", "sun"),
            ("earth", "sun", "sun"),
            ("earth", "sun", "sun"),
            ("mars", "sun", "sun"),
        ])

    def test_cte_queryset_with_values_result(self):
        cte = With(
            Order.objects
            .values(
                "region_id",
                region_parent=F("region__parent_id"),
            )
            .distinct()
        )
        values = (
            cte.queryset()
            .with_cte(cte)
            .filter(region_parent__isnull=False)
            .order_by("region_parent", "region_id")
        )
        print(values.query)

        data = list(values)[:5]
        self.assertEqual(data, [
            {'region_id': 'moon', 'region_parent': 'earth'},
            {
                'region_id': 'proxima centauri b',
                'region_parent': 'proxima centauri',
            },
            {'region_id': 'earth', 'region_parent': 'sun'},
            {'region_id': 'mars', 'region_parent': 'sun'},
            {'region_id': 'mercury', 'region_parent': 'sun'},
        ])

    def test_cte_queryset_with_custom_queryset(self):
        cte = With(
            Order.objects
            .annotate(region_parent=F("region__parent_id"))
            .filter(region__parent_id="sun")
        )
        orders = (
            cte.queryset()
            .with_cte(cte)
            .lt40()  # custom queryset method
            .order_by("region_id", "amount")
        )
        print(orders.query)

        data = [(x.region_id, x.amount, x.region_parent) for x in orders]
        self.assertEqual(data, [
            ("earth", 30, "sun"),
            ("earth", 31, "sun"),
            ("earth", 32, "sun"),
            ("earth", 33, "sun"),
            ('mercury', 10, 'sun'),
            ('mercury', 11, 'sun'),
            ('mercury', 12, 'sun'),
            ('venus', 20, 'sun'),
            ('venus', 21, 'sun'),
            ('venus', 22, 'sun'),
            ('venus', 23, 'sun'),
        ])

    def test_named_ctes(self):
        def make_paths_cte(paths):
            return Region.objects.filter(
                parent__isnull=True
            ).values(
                "name",
                path=F("name"),
            ).union(
                paths.join(Region, parent=paths.col.name).values(
                    "name",
                    path=Concat(
                        paths.col.path, Value(" "), F("name"),
                        output_field=text_field,
                    ),
                ),
                all=True,
            )
        paths = With.recursive(make_paths_cte, name="region_paths")

        def make_groups_cte(groups):
            return paths.join(Region, name=paths.col.name).values(
                "name",
                parent_path=paths.col.path,
                parent_name=F("name"),
            ).union(
                groups.join(Region, parent=groups.col.name).values(
                    "name",
                    parent_path=groups.col.parent_path,
                    parent_name=groups.col.parent_name,
                ),
                all=True,
            )
        groups = With.recursive(make_groups_cte, name="region_groups")

        region_totals = With(
            groups.join(Order, region_id=groups.col.name)
            .values(
                name=groups.col.parent_name,
                path=groups.col.parent_path,
            ).annotate(
                orders_count=Count("id"),
                region_total=Sum("amount"),
            ),
            name="region_totals",
        )

        regions = (
            region_totals.join(Region, name=region_totals.col.name)
            .with_cte(paths)
            .with_cte(groups)
            .with_cte(region_totals)
            .annotate(
                path=region_totals.col.path,
                # count of orders in this region and all subregions
                orders_count=region_totals.col.orders_count,
                # sum of order amounts in this region and all subregions
                region_total=region_totals.col.region_total,
            )
            .order_by("path")
        )
        print(regions.query)

        data = [(r.name, r.orders_count, r.region_total) for r in regions]
        self.assertEqual(data, [
            ('proxima centauri', 4, 2033),
            ('proxima centauri b', 3, 33),
            ('sun', 18, 1374),
            ('earth', 7, 132),
            ('moon', 3, 6),
            ('mars', 3, 123),
            ('mercury', 3, 33),
            ('venus', 4, 86),
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

    def test_outerref_in_cte_query(self):
        # This query is meant to return the difference between min and max
        # order of each region, through a subquery
        min_and_max = With(
            Order.objects
            .filter(region=OuterRef("pk"))
            .values('region')  # This is to force group by region_id
            .annotate(
                amount_min=Min("amount"),
                amount_max=Max("amount"),
            )
            .values('amount_min', 'amount_max')
        )
        regions = (
            Region.objects
            .annotate(
                difference=CTESubquery(
                    min_and_max.queryset().with_cte(min_and_max).annotate(
                        difference=F('amount_max') - F('amount_min'),
                    ).values('difference')[:1],
                    output_field=IntegerField()
                )
            )
            .order_by("name")
        )

        data = [(r.name, r.difference) for r in regions]
        self.assertEqual(data, [
            ("bernard's star", None),
            ('deimos', None),
            ('earth', 3),
            ('mars', 2),
            ('mercury', 2),
            ('moon', 2),
            ('phobos', None),
            ('proxima centauri', 0),
            ('proxima centauri b', 2),
            ('sun', 0),
            ('venus', 3)
        ])
