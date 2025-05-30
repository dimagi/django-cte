from unittest import SkipTest

from django.db.models import IntegerField, TextField
from django.db.models.aggregates import Count, Max, Min, Sum
from django.db.models.expressions import (
    Exists, ExpressionWrapper, F, OuterRef, Subquery,
)
from django.db.models.sql.constants import LOUTER
from django.test import TestCase

from django_cte import CTE
from django_cte import CTEManager

from .models import Order, Region, User

int_field = IntegerField()
text_field = TextField()


class TestCTE(TestCase):

    def test_simple_cte_query(self):
        cte = CTE(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount"))
        )

        orders = (
            # FROM orders INNER JOIN cte ON orders.region_id = cte.region_id
            cte.join(Order, region=cte.col.region_id)

            # Add `WITH ...` before `SELECT ... FROM orders ...`
            .with_cte(cte)

            # Annotate each Order with a "region_total"
            .annotate(region_total=cte.col.total)
        )
        print(orders.query)

        data = sorted((o.amount, o.region_id, o.region_total) for o in orders)
        self.assertEqual(data, [
            (1, 'moon', 6),
            (2, 'moon', 6),
            (3, 'moon', 6),
            (10, 'mercury', 33),
            (10, 'proxima centauri b', 33),
            (11, 'mercury', 33),
            (11, 'proxima centauri b', 33),
            (12, 'mercury', 33),
            (12, 'proxima centauri b', 33),
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
            (1000, 'sun', 1000),
            (2000, 'proxima centauri', 2000),
        ])

    def test_cte_name_escape(self):
        totals = CTE(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="mixedCaseCTEName"
        )
        orders = (
            totals
            .join(Order, region=totals.col.region_id)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
            .order_by("amount")
        )
        self.assertTrue(
            str(orders.query).startswith('WITH RECURSIVE "mixedCaseCTEName"'))

    def test_cte_queryset(self):
        sub_totals = CTE(
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
        cte = CTE(
            Order.objects
            .annotate(region_parent=F("region__parent_id")),
        )
        orders = cte.queryset().with_cte(cte)
        print(orders.query)

        data = sorted(
            (x.region_id, x.amount, x.region_parent) for x in orders)[:5]
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
        cte = CTE(
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
        cte = CTE(
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
        )
        print(values.query)

        def key(item):
            return item["region_parent"], item["region_id"]

        data = sorted(values, key=key)[:5]
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

    def test_named_simple_ctes(self):
        totals = CTE(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="totals",
        )
        region_count = CTE(
            Region.objects
            .filter(parent="sun")
            .values("parent_id")
            .annotate(num=Count("name")),
            name="region_count",
        )
        orders = (
            region_count.join(
                totals.join(Order, region=totals.col.region_id),
                region__parent=region_count.col.parent_id
            )
            .with_cte(totals)
            .with_cte(region_count)
            .annotate(region_total=totals.col.total)
            .annotate(region_count=region_count.col.num)
            .order_by("amount")
        )
        print(orders.query)

        data = [(
            o.amount,
            o.region_id,
            o.region_count,
            o.region_total,
        ) for o in orders]
        self.assertEqual(data, [
            (10, 'mercury', 4, 33),
            (11, 'mercury', 4, 33),
            (12, 'mercury', 4, 33),
            (20, 'venus', 4, 86),
            (21, 'venus', 4, 86),
            (22, 'venus', 4, 86),
            (23, 'venus', 4, 86),
            (30, 'earth', 4, 126),
            (31, 'earth', 4, 126),
            (32, 'earth', 4, 126),
            (33, 'earth', 4, 126),
            (40, 'mars', 4, 123),
            (41, 'mars', 4, 123),
            (42, 'mars', 4, 123),
        ])

    def test_named_ctes(self):
        def make_root_mapping(rootmap):
            return Region.objects.filter(
                parent__isnull=True
            ).values(
                "name",
                root=F("name"),
            ).union(
                rootmap.join(Region, parent=rootmap.col.name).values(
                    "name",
                    root=rootmap.col.root,
                ),
                all=True,
            )
        rootmap = CTE.recursive(make_root_mapping, name="rootmap")

        totals = CTE(
            rootmap.join(Order, region_id=rootmap.col.name)
            .values(
                root=rootmap.col.root,
            ).annotate(
                orders_count=Count("id"),
                region_total=Sum("amount"),
            ),
            name="totals",
        )

        root_regions = (
            totals.join(Region, name=totals.col.root)
            .with_cte(rootmap)
            .with_cte(totals)
            .annotate(
                # count of orders in this region and all subregions
                orders_count=totals.col.orders_count,
                # sum of order amounts in this region and all subregions
                region_total=totals.col.region_total,
            )
        )
        print(root_regions.query)

        data = sorted(
            (r.name, r.orders_count, r.region_total) for r in root_regions
        )
        self.assertEqual(data, [
            ('proxima centauri', 4, 2033),
            ('sun', 18, 1374),
        ])

    def test_materialized_option(self):
        totals = CTE(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
            materialized=True
        )
        orders = (
            totals
            .join(Order, region=totals.col.region_id)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
            .order_by("amount")
        )
        self.assertTrue(
            str(orders.query).startswith(
                'WITH RECURSIVE "cte" AS MATERIALIZED'
            )
        )

    def test_update_cte_query(self):
        cte = CTE(
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

    def test_update_with_subquery(self):
        # Test for issue: https://github.com/dimagi/django-cte/issues/9
        # Issue is not reproduces on sqlite3 use postgres to run.
        # To reproduce the problem it's required to have some join
        # in the select-query so the compiler will turn it into a subquery.
        # To add a join use a filter over field of related model
        orders = Order.objects.filter(region__parent_id='sun')
        orders.update(amount=0)
        data = {(order.region_id, order.amount) for order in orders}
        self.assertEqual(data, {
            ('mercury', 0),
            ('venus', 0),
            ('earth', 0),
            ('mars', 0),
        })

    def test_delete_cte_query(self):
        raise SkipTest(
            "this test will not work until `QuerySet.delete` (Django method) "
            "calls `self.query.chain(sql.DeleteQuery)` instead of "
            "`sql.DeleteQuery(self.model)`"
        )
        cte = CTE(
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
        min_and_max = CTE(
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
                difference=Subquery(
                    min_and_max.queryset().with_cte(min_and_max).annotate(
                        difference=ExpressionWrapper(
                            F('amount_max') - F('amount_min'),
                            output_field=int_field,
                        ),
                    ).values('difference')[:1],
                    output_field=IntegerField()
                )
            )
            .order_by("name")
        )
        print(regions.query)

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

    def test_experimental_left_outer_join(self):
        totals = CTE(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount"))
            .filter(total__gt=100)
        )
        orders = (
            totals
            .join(Order, region=totals.col.region_id, _join_type=LOUTER)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
        )
        print(orders.query)
        self.assertIn("LEFT OUTER JOIN", str(orders.query))
        self.assertNotIn("INNER JOIN", str(orders.query))

        data = sorted((o.region_id, o.amount, o.region_total) for o in orders)
        self.assertEqual(data, [
            ('earth', 30, 126),
            ('earth', 31, 126),
            ('earth', 32, 126),
            ('earth', 33, 126),
            ('mars', 40, 123),
            ('mars', 41, 123),
            ('mars', 42, 123),
            ('mercury', 10, None),
            ('mercury', 11, None),
            ('mercury', 12, None),
            ('moon', 1, None),
            ('moon', 2, None),
            ('moon', 3, None),
            ('proxima centauri', 2000, 2000),
            ('proxima centauri b', 10, None),
            ('proxima centauri b', 11, None),
            ('proxima centauri b', 12, None),
            ('sun', 1000, 1000),
            ('venus', 20, None),
            ('venus', 21, None),
            ('venus', 22, None),
            ('venus', 23, None),
        ])

    def test_non_cte_subquery(self):
        """
        Verifies that subquery annotations are handled correctly when the
        subquery model doesn't use the CTE manager, and the query results
        match expected behavior
        """
        self.assertNotIsInstance(User.objects, CTEManager)

        sub_totals = CTE(
            Order.objects
            .values(region_parent=F("region__parent_id"))
            .annotate(
                total=Sum("amount"),
                # trivial subquery example testing existence of
                # a user for the order
                non_cte_subquery=Exists(
                    User.objects.filter(pk=OuterRef("user_id"))
                ),
            ),
        )
        regions = (
            Region.objects.all()
            .with_cte(sub_totals)
            .annotate(
                child_regions_total=Subquery(
                    sub_totals.queryset()
                    .filter(region_parent=OuterRef("name"))
                    .values("total"),
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

    def test_explain(self):
        """
        Verifies that using .explain() prepends the EXPLAIN clause in the
        correct position
        """

        totals = CTE(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="totals",
        )
        region_count = CTE(
            Region.objects
            .filter(parent="sun")
            .values("parent_id")
            .annotate(num=Count("name")),
            name="region_count",
        )
        orders = (
            region_count.join(
                totals.join(Order, region=totals.col.region_id),
                region__parent=region_count.col.parent_id
            )
            .with_cte(totals)
            .with_cte(region_count)
            .annotate(region_total=totals.col.total)
            .annotate(region_count=region_count.col.num)
            .order_by("amount")
        )
        print(orders.query)

        self.assertIsInstance(orders.explain(), str)

    def test_empty_result_set_cte(self):
        """
        Verifies that the CTEQueryCompiler can handle empty result sets in the
        related CTEs
        """
        totals = CTE(
            Order.objects
            .filter(id__in=[])
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="totals",
        )
        orders = (
            totals.join(Order, region=totals.col.region_id)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
            .order_by("amount")
        )

        self.assertEqual(len(orders), 0)

    def test_left_outer_join_on_empty_result_set_cte(self):
        totals = CTE(
            Order.objects
            .filter(id__in=[])
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="totals",
        )
        orders = (
            totals.join(Order, region=totals.col.region_id, _join_type=LOUTER)
            .with_cte(totals)
            .annotate(region_total=totals.col.total)
            .order_by("amount")
        )

        self.assertEqual(len(orders), 22)

    def test_union_query_with_cte(self):
        orders = (
            Order.objects
            .filter(region__parent="sun")
            .only("region", "amount")
        )
        orders_cte = CTE(orders, name="orders_cte")
        orders_cte_queryset = orders_cte.queryset()

        earth_orders = orders_cte_queryset.filter(region="earth")
        mars_orders = orders_cte_queryset.filter(region="mars")

        earth_mars = earth_orders.union(mars_orders, all=True)
        earth_mars_cte = (
            earth_mars
            .with_cte(orders_cte)
            .order_by("region", "amount")
            .values_list("region", "amount")
        )
        print(earth_mars_cte.query)

        self.assertEqual(list(earth_mars_cte), [
            ('earth', 30),
            ('earth', 31),
            ('earth', 32),
            ('earth', 33),
            ('mars', 40),
            ('mars', 41),
            ('mars', 42),
        ])

    def test_cte_select_pk(self):
        orders = Order.objects.filter(region="earth").values("pk")
        cte = CTE(orders)
        queryset = cte.join(orders, pk=cte.col.pk).with_cte(cte).order_by("pk")
        print(queryset.query)
        self.assertEqual(list(queryset), [
            {'pk': 9},
            {'pk': 10},
            {'pk': 11},
            {'pk': 12},
        ])

    def test_django52_resolve_ref_regression(self):
        cte = CTE(
            Order.objects.annotate(
                pnt_id=F("region__parent_id"),
                region_name=F("region__name"),
            ).values(
                # important: more than one query.select field
                "region_id",
                "amount",
                # important: more than one query.annotations field
                "pnt_id",
                "region_name",
            )
        )
        qs = (
            cte.queryset()
            .with_cte(cte)
            .values(
                amt=cte.col.amount,
                pnt_id=cte.col.pnt_id,
                region_name=cte.col.region_name,
            )
            .filter(region_id="earth")
            .order_by("amount")
        )
        print(qs.query)
        self.assertEqual(list(qs), [
            {'amt': 30, 'region_name': 'earth', 'pnt_id': 'sun'},
            {'amt': 31, 'region_name': 'earth', 'pnt_id': 'sun'},
            {'amt': 32, 'region_name': 'earth', 'pnt_id': 'sun'},
            {'amt': 33, 'region_name': 'earth', 'pnt_id': 'sun'},
        ])
