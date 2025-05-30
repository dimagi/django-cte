from django.db.models.expressions import F
from django.db.models.query import QuerySet
from django.test import TestCase

from django_cte import CTE, CTEQuerySet, CTEManager, with_cte

from .models import (
    Order,
    OrderFromLT40,
    OrderLT40AsManager,
    OrderCustomManagerNQuery,
    OrderCustomManager,
    LT40QuerySet,
    LTManager,
    LT25QuerySet,
)


class TestCTE(TestCase):
    def test_cte_queryset_correct_defaultmanager(self):
        self.assertEqual(type(Order._default_manager), CTEManager)
        self.assertEqual(type(Order.objects.all()), CTEQuerySet)

    def test_cte_queryset_correct_from_queryset(self):
        self.assertEqual(type(OrderFromLT40.objects.all()), LT40QuerySet)

    def test_cte_queryset_correct_queryset_as_manager(self):
        self.assertEqual(type(OrderLT40AsManager.objects.all()), LT40QuerySet)

    def test_cte_queryset_correct_manager_n_from_queryset(self):
        self.assertIsInstance(
            OrderCustomManagerNQuery._default_manager, LTManager)
        self.assertEqual(type(
            OrderCustomManagerNQuery.objects.all()), LT25QuerySet)

    def test_cte_create_manager_from_non_cteQuery(self):
        class BrokenQuerySet(QuerySet):
            "This should be a CTEQuerySet if we want this to work"

        with self.assertRaises(TypeError):
            CTEManager.from_queryset(BrokenQuerySet)()

    def test_cte_queryset_correct_limitedmanager(self):
        self.assertEqual(type(OrderCustomManager._default_manager), LTManager)
        # Check the expected even if not ideal behavior occurs
        self.assertIsInstance(OrderCustomManager.objects.all(), CTEQuerySet)

    def test_cte_queryset_with_from_queryset(self):
        self.assertEqual(type(OrderFromLT40.objects.all()), LT40QuerySet)

        cte = CTE(
            OrderFromLT40.objects
            .annotate(region_parent=F("region__parent_id"))
            .filter(region__parent_id="sun")
        )
        orders = with_cte(
            cte,
            select=cte.queryset()
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

    def test_cte_queryset_with_custom_queryset(self):
        cte = CTE(
            OrderCustomManagerNQuery.objects
            .annotate(region_parent=F("region__parent_id"))
            .filter(region__parent_id="sun")
        )
        orders = with_cte(
            cte,
            select=cte.queryset()
            .lt25()  # custom queryset method
            .order_by("region_id", "amount")
        )
        print(orders.query)

        data = [(x.region_id, x.amount, x.region_parent) for x in orders]
        self.assertEqual(data, [
            ('mercury', 10, 'sun'),
            ('mercury', 11, 'sun'),
            ('mercury', 12, 'sun'),
            ('venus', 20, 'sun'),
            ('venus', 21, 'sun'),
            ('venus', 22, 'sun'),
            ('venus', 23, 'sun'),
        ])

    def test_cte_queryset_with_deferred_loading(self):
        cte = CTE(
            OrderCustomManagerNQuery.objects.order_by("id").only("id")[:1]
        )
        orders = with_cte(cte, select=cte)
        print(orders.query)

        self.assertEqual([x.id for x in orders], [1])
