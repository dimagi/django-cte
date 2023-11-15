from django.test import TestCase, skipUnlessDBFeature

from .models import Order, User


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
