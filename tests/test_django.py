from unittest import SkipTest

import django
from django.db import OperationalError, ProgrammingError
from django.db.models import Window
from django.db.models.functions import Rank
from django.test import TestCase

from django_cte import CTE, with_cte

from .models import Order, Region


class WindowFunctions(TestCase):

    def test_heterogeneous_filter_in_cte(self):
        if django.VERSION < (4, 2):
            raise SkipTest("feature added in Django 4.2")
        cte = CTE(
            Order.objects.annotate(
                region_amount_rank=Window(
                    Rank(), partition_by="region_id", order_by="-amount"
                ),
            )
            .order_by("region_id")
            .values("region_id", "region_amount_rank")
            .filter(region_amount_rank=1, region_id__in=["sun", "moon"])
        )
        qs = with_cte(cte, select=cte.join(Region, name=cte.col.region_id))
        print(qs.query)
        # ProgrammingError: column cte.region_id does not exist
        # WITH RECURSIVE "cte" AS (SELECT * FROM (
        #   SELECT "orders"."region_id" AS "col1", ...
        # "region" INNER JOIN "cte" ON "region"."name" = ("cte"."region_id")
        try:
            self.assertEqual({r.name for r in qs}, {"moon", "sun"})
        except (OperationalError, ProgrammingError) as err:
            if "cte.region_id" in str(err):
                raise SkipTest(
                    "window function auto-aliasing breaks CTE "
                    "column references"
                )
            raise
        if django.VERSION < (5, 2):
            assert 0, "unexpected pass"
