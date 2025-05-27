from unittest import SkipTest

import django
from django.db import OperationalError, ProgrammingError
from django.db.models import F, Window
from django.db.models.functions import Rank, RowNumber
from django.test import TestCase, skipUnlessDBFeature

from django_cte import With

from .models import Order, Region, User


@skipUnlessDBFeature("supports_select_union")
class NonCteQueries(TestCase):
    """Test non-CTE queries

    These tests were adapted from the Django test suite. The models used
    here use CTEManager and CTEQuerySet to verify feature parity with
    their base classes Manager and QuerySet.
    """

    @classmethod
    def setUpTestData(cls):
        Order.objects.all().delete()

    def test_union_with_select_related_and_order(self):
        e1 = User.objects.create(name="e1")
        a1 = Order.objects.create(region_id="earth", user=e1)
        a2 = Order.objects.create(region_id="moon", user=e1)
        Order.objects.create(region_id="sun", user=e1)
        base_qs = Order.objects.select_related("user").order_by()
        qs1 = base_qs.filter(region_id="earth")
        qs2 = base_qs.filter(region_id="moon")
        print(qs1.union(qs2).order_by("pk").query)
        self.assertSequenceEqual(qs1.union(qs2).order_by("pk"), [a1, a2])

    @skipUnlessDBFeature("supports_slicing_ordering_in_compound")
    def test_union_with_select_related_and_first(self):
        e1 = User.objects.create(name="e1")
        a1 = Order.objects.create(region_id="earth", user=e1)
        Order.objects.create(region_id="moon", user=e1)
        base_qs = Order.objects.select_related("user")
        qs1 = base_qs.filter(region_id="earth")
        qs2 = base_qs.filter(region_id="moon")
        self.assertEqual(qs1.union(qs2).first(), a1)

    def test_union_with_first(self):
        e1 = User.objects.create(name="e1")
        a1 = Order.objects.create(region_id="earth", user=e1)
        base_qs = Order.objects.order_by()
        qs1 = base_qs.filter(region_id="earth")
        qs2 = base_qs.filter(region_id="moon")
        self.assertEqual(qs1.union(qs2).first(), a1)


class WindowFunctions(TestCase):

    @classmethod
    def setUpClass(cls):
        if django.VERSION < (4, 2):
            raise SkipTest("window functions were added in Django 4.2")
        super().setUpClass()

    def test_heterogeneous_filter_in_cte(self):
        cte = With(
            Order.objects.annotate(
                region_amount_rank=Window(
                    Rank(), partition_by="region_id", order_by="-amount"
                ),
            )
            .order_by("region_id")
            .values("region_id", "region_amount_rank")
            .filter(region_amount_rank=1, region_id__in=["sun", "moon"])
        )
        qs = cte.join(Region, name=cte.col.region_id).with_cte(cte)
        print(qs.query)
        # ProgrammingError: column cte.region_id does not exist
        # WITH RECURSIVE "cte" AS (SELECT * FROM (
        #   SELECT "orders"."region_id" AS "col1", ...
        # "region" INNER JOIN "cte" ON "region"."name" = ("cte"."region_id")
        try:
            self.assertSequenceEqual({r.name for r in qs}, {"moon", "sun"})
        except (OperationalError, ProgrammingError) as err:
            if "cte.region_id" in str(err):
                raise SkipTest(
                    "window function auto-aliasing breaks CTE "
                    "column references"
                )
            raise
        assert 0, "unexpected pass"

    def test_window_function_in_cte(self):
        cte = With(
            Region.objects.annotate(row_number=Window(RowNumber()))
        )
        qs = (
            cte.queryset()
            .annotate(rn2=Window(RowNumber(), order_by=[F("row_number")]))
            .filter(rn2=1)
            .with_cte(cte)
        )
        print(qs.query)
        self.assertEqual({r.name for r in qs}, {"sun"})
        self.assertEqual(str(qs.query).count("WITH "), 1)
