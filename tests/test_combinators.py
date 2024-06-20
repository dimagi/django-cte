from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function

from django.db.models import IntegerField, TextField
from django.db.models.aggregates import Sum
from django.test import TestCase

from django_cte import With

from .models import Order

int_field = IntegerField()
text_field = TextField()


class TestCTECombinators(TestCase):

    def test_cte_basic_union(self):
        cte_sun = With(
            Order.objects
            .filter(region__parent="sun")
            .values("region_id")
            .annotate(total=Sum("amount")),
            name='rsun'
        )
        cte_proxima = With(
            Order.objects
            .filter(region__parent="proxima centauri")
            .values("region_id")
            .annotate(total=2 * Sum("amount")),
            name='rprox'
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

        orders = orders_sun.union(orders_proxima)
        print(orders.query)
        data = sorted((o.amount, o.region_id, o.region_total) for o in orders)
        self.assertEqual(data, [
            (10, 'mercury', 33),
            (10, 'proxima centauri b', 66),
            (11, 'mercury', 33),
            (11, 'proxima centauri b', 66),
            (12, 'mercury', 33),
            (12, 'proxima centauri b', 66),
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
            (42, 'mars', 123)
        ])

    def test_cte_basic_union_with_rename(self):
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

        orders = orders_sun.union(orders_proxima)
        print(orders.query)
        with self.subTest("Test union data"):
            data = sorted(
                (o.amount, o.region_id, o.region_total) for o in orders)
            self.assertEqual(data, [
                (10, 'mercury', 33),
                (10, 'proxima centauri b', 66),
                (11, 'mercury', 33),
                (11, 'proxima centauri b', 66),
                (12, 'mercury', 33),
                (12, 'proxima centauri b', 66),
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
                (42, 'mars', 123)
            ])

        print(orders_sun.query)
        with self.subTest("Preservation of original cte name"):
            # The original query is still using the un-renames CTE
            data = sorted(
                (o.amount, o.region_id, o.region_total) for o in orders_sun)
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
                (42, 'mars', 123)
            ])

            data = sorted(
                (o.amount, o.region_id, o.region_total)
                for o in orders_proxima)
            self.assertEqual(data, [
                (10, 'proxima centauri b', 66),
                (11, 'proxima centauri b', 66),
                (12, 'proxima centauri b', 66),
            ])

    def test_cte_basic_union_of_same_cte(self):
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

        orders = orders_big.union(orders_small)

        # Ensure we only include the CTE once
        self.assertEqual(len(orders.query._with_ctes), 1)

        print(orders.query)
        data = sorted(
            (o.amount, o.region_id, o.region_total) for o in orders)
        self.assertEqual(data, [
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

    def test_cte_basic_intersection(self):
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

        orders = orders_small.intersection(orders_big)
        print(orders.query)
        data = sorted(
            (o.amount, o.region_id, o.region_total) for o in orders)
        self.assertEqual(data, [
            (20, 'venus', 86),
            (21, 'venus', 86),
            (22, 'venus', 86),
            (23, 'venus', 86),
            (40, 'mars', 123),
            (41, 'mars', 123),
            (42, 'mars', 123),
        ])

    def test_cte_basic_difference(self):
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

        orders = orders_small.difference(orders_big)
        print(orders.query)
        data = sorted(
            (o.amount, o.region_id, o.region_total) for o in orders)
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
        ])
