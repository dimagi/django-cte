from django.db.models import IntegerField, TextField
from django.test import TestCase

from django_cte import CTE, with_cte
from django_cte.raw import raw_cte_sql

from .models import Region

int_field = IntegerField()
text_field = TextField()


class TestRawCTE(TestCase):

    def test_raw_cte_sql(self):
        cte = CTE(raw_cte_sql(
            """
            SELECT region_id, AVG(amount) AS avg_order
            FROM orders
            WHERE region_id = %s
            GROUP BY region_id
            """,
            ["moon"],
            {"region_id": text_field, "avg_order": int_field},
        ))
        moon_avg = with_cte(
            cte, select=cte.join(Region, name=cte.col.region_id)
        ).annotate(avg_order=cte.col.avg_order)
        print(moon_avg.query)

        data = [(r.name, r.parent.name, r.avg_order) for r in moon_avg]
        self.assertEqual(data, [('moon', 'earth', 2)])

    def test_raw_cte_sql_name_escape(self):
        cte = CTE(
            raw_cte_sql(
                """
                SELECT region_id, AVG(amount) AS avg_order
                FROM orders
                WHERE region_id = %s
                GROUP BY region_id
                """,
                ["moon"],
                {"region_id": text_field, "avg_order": int_field},
            ),
            name="mixedCaseCTEName"
        )
        moon_avg = with_cte(
            cte, select=cte.join(Region, name=cte.col.region_id)
        ).annotate(avg_order=cte.col.avg_order)
        self.assertTrue(
            str(moon_avg.query).startswith(
                'WITH RECURSIVE "mixedCaseCTEName"')
        )
