import pytest
from django.db.models import Value
from django.db.models.aggregates import Sum
from django.test import TestCase

from django_cte import With

from .models import Order, OrderPlainManager


class TestCTECombinators(TestCase):

    def test_cte_union_query(self):
        one = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
            name="one"
        )
        two = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount") * 2),
            name="two"
        )

        earths = (
            one.join(
                Order.objects.filter(region_id="earth"),
                region=one.col.region_id
            )
            .with_cte(one)
            .annotate(region_total=one.col.total)
            .values_list("amount", "region_id", "region_total")
        )
        mars = (
            two.join(
                Order.objects.filter(region_id="mars"),
                region=two.col.region_id
            )
            .with_cte(two)
            .annotate(region_total=two.col.total)
            .values_list("amount", "region_id", "region_total")
        )
        combined = earths.union(mars, all=True)
        print(combined.query)

        self.assertEqual(sorted(combined), [
            (30, 'earth', 126),
            (31, 'earth', 126),
            (32, 'earth', 126),
            (33, 'earth', 126),
            (40, 'mars', 246),
            (41, 'mars', 246),
            (42, 'mars', 246),
        ])

        # queries used in union should still work on their own
        print(earths.query)
        self.assertEqual(sorted(earths),[
            (30, 'earth', 126),
            (31, 'earth', 126),
            (32, 'earth', 126),
            (33, 'earth', 126),
        ])
        print(mars.query)
        self.assertEqual(sorted(mars),[
            (40, 'mars', 246),
            (41, 'mars', 246),
            (42, 'mars', 246),
        ])

    def test_cte_union_with_non_cte_query(self):
        one = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
        )

        earths = (
            one.join(
                Order.objects.filter(region_id="earth"),
                region=one.col.region_id
            )
            .with_cte(one)
            .annotate(region_total=one.col.total)
        )
        plain_mars = (
            OrderPlainManager.objects.filter(region_id="mars")
            .annotate(region_total=Value(0))
        )
        # Note: this does not work in the opposite order. A CTE query
        # must come first to invoke custom CTE combinator logic.
        combined = earths.union(plain_mars, all=True) \
            .values_list("amount", "region_id", "region_total")
        print(combined.query)

        self.assertEqual(sorted(combined), [
            (30, 'earth', 126),
            (31, 'earth', 126),
            (32, 'earth', 126),
            (33, 'earth', 126),
            (40, 'mars', 0),
            (41, 'mars', 0),
            (42, 'mars', 0),
        ])

    def test_cte_union_with_duplicate_names(self):
        cte_sun = With(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
        )
        cte_proxima = With(
            Order.objects
            .filter(region__parent="proxima centauri")
            .values("region_id")
            .annotate(total=2 * Sum("amount")),
        )

        orders_sun = (
            cte_sun.join(Order, region=cte_sun.col.region_id)
            .with_cte(cte_sun)
            .annotate(region_total=cte_sun.col.total)
        )
        orders_proxima = (
            cte_proxima.join(Order, region=cte_proxima.col.region_id)
            .with_cte(cte_proxima)
            .annotate(region_total=cte_proxima.col.total)
        )

        msg = "Found two or more CTEs named 'cte'"
        with pytest.raises(ValueError, match=msg):
            orders_sun.union(orders_proxima)

    def test_cte_union_of_same_cte(self):
        cte = With(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
        )

        orders_big = (
            cte.join(Order, region=cte.col.region_id)
            .with_cte(cte)
            .annotate(region_total=3 * cte.col.total)
        )
        orders_small = (
            cte.join(Order, region=cte.col.region_id)
            .with_cte(cte)
            .annotate(region_total=cte.col.total)
        )

        orders = orders_big.union(orders_small) \
            .values_list("amount", "region_id", "region_total")
        print(orders.query)

        self.assertEqual(sorted(orders), [
             (10, 'mercury', 33),
             (10, 'mercury', 99),
             (11, 'mercury', 33),
             (11, 'mercury', 99),
             (12, 'mercury', 33),
             (12, 'mercury', 99),
             (20, 'venus', 86),
             (20, 'venus', 258),
             (21, 'venus', 86),
             (21, 'venus', 258),
             (22, 'venus', 86),
             (22, 'venus', 258),
             (23, 'venus', 86),
             (23, 'venus', 258),
             (30, 'earth', 126),
             (30, 'earth', 378),
             (31, 'earth', 126),
             (31, 'earth', 378),
             (32, 'earth', 126),
             (32, 'earth', 378),
             (33, 'earth', 126),
             (33, 'earth', 378),
             (40, 'mars', 123),
             (40, 'mars', 369),
             (41, 'mars', 123),
             (41, 'mars', 369),
             (42, 'mars', 123),
             (42, 'mars', 369)
        ])

    def test_cte_intersection(self):
        cte_big = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
            name='big'
        )
        cte_small = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
            name='small'
        )
        orders_big = (
            cte_big.join(Order, region=cte_big.col.region_id)
            .with_cte(cte_big)
            .annotate(region_total=cte_big.col.total)
            .filter(region_total__gte=86)
        )
        orders_small = (
            cte_small.join(Order, region=cte_small.col.region_id)
            .with_cte(cte_small)
            .annotate(region_total=cte_small.col.total)
            .filter(region_total__lte=123)
        )

        orders = orders_small.intersection(orders_big) \
            .values_list("amount", "region_id", "region_total")
        print(orders.query)

        self.assertEqual(sorted(orders), [
            (20, 'venus', 86),
            (21, 'venus', 86),
            (22, 'venus', 86),
            (23, 'venus', 86),
            (40, 'mars', 123),
            (41, 'mars', 123),
            (42, 'mars', 123),
        ])

    def test_cte_difference(self):
        cte_big = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
            name='big'
        )
        cte_small = With(
            Order.objects
            .values("region_id")
            .annotate(total=Sum("amount")),
            name='small'
        )
        orders_big = (
            cte_big.join(Order, region=cte_big.col.region_id)
            .with_cte(cte_big)
            .annotate(region_total=cte_big.col.total)
            .filter(region_total__gte=86)
        )
        orders_small = (
            cte_small.join(Order, region=cte_small.col.region_id)
            .with_cte(cte_small)
            .annotate(region_total=cte_small.col.total)
            .filter(region_total__lte=123)
        )

        orders = orders_small.difference(orders_big) \
            .values_list("amount", "region_id", "region_total")
        print(orders.query)

        self.assertEqual(sorted(orders), [
            (1, 'moon', 6),
            (2, 'moon', 6),
            (3, 'moon', 6),
            (10, 'mercury', 33),
            (10, 'proxima centauri b', 33),
            (11, 'mercury', 33),
            (11, 'proxima centauri b', 33),
            (12, 'mercury', 33),
            (12, 'proxima centauri b', 33),
        ])
