from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from django.db.models import IntegerField, TextField
from django.test import TestCase

from django_cte import With
from django_cte.raw import raw_cte_sql

from .models import Region

int_field = IntegerField()
text_field = TextField()


class TestRawCTE(TestCase):

    def test_raw_cte_sql(self):
        cte = With(raw_cte_sql(
            """
            SELECT region_id, AVG(amount) AS avg_order
            FROM tests_order
            WHERE region_id = %s
            GROUP BY region_id
            """,
            ["moon"],
            {"region_id": text_field, "avg_order": int_field},
        ))
        moon_avg = (
            cte
            .join(Region, name=cte.col.region_id)
            .annotate(avg_order=cte.col.avg_order)
            .with_cte(cte)
        )

        data = [(r.name, r.parent.name, r.avg_order) for r in moon_avg]
        self.assertEqual(data, [('moon', 'earth', 2)])
