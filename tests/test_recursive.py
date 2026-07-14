import pickle
from unittest import SkipTest

from django.db import connection
from django.db.models import IntegerField, TextField
from django.db.models.expressions import (
    Case,
    Exists,
    ExpressionWrapper,
    F,
    OuterRef,
    Q,
    Value,
    When,
)
from django.db.models.functions import Concat
from django.db.utils import DatabaseError
from django.test import TestCase

from django_cte import CTE, with_cte

from .models import KeyPair, Region

int_field = IntegerField()
text_field = TextField()


class TestRecursiveCTE(TestCase):

    def test_recursive_cte_query(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                # non-recursive: get root nodes
                parent__isnull=True
            ).values(
                "name",
                path=F("name"),
                depth=Value(0, output_field=int_field),
            ).union(
                # recursive union: get descendants
                cte.join(Region, parent=cte.col.name).values(
                    "name",
                    path=Concat(
                        cte.col.path, Value(" / "), F("name"),
                        output_field=text_field,
                    ),
                    depth=cte.col.depth + Value(1, output_field=int_field),
                ),
                all=True,
            )

        cte = CTE.recursive(make_regions_cte)

        regions = with_cte(
            cte,
            select=cte.join(Region, name=cte.col.name)
            .annotate(
                path=cte.col.path,
                depth=cte.col.depth,
            )
            .filter(depth=2)
            .order_by("path")
        )
        print(regions.query)

        data = [(r.name, r.path, r.depth) for r in regions]
        self.assertEqual(data, [
            ('moon', 'sun / earth / moon', 2),
            ('deimos', 'sun / mars / deimos', 2),
            ('phobos', 'sun / mars / phobos', 2),
        ])

    def test_recursive_cte_reference_in_condition(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                parent__isnull=True
            ).values(
                "name",
                path=F("name"),
                depth=Value(0, output_field=int_field),
                is_planet=Value(0, output_field=int_field),
            ).union(
                cte.join(
                    Region, parent=cte.col.name
                ).annotate(
                    # annotations for filter and CASE/WHEN conditions
                    parent_name=ExpressionWrapper(
                        cte.col.name,
                        output_field=text_field,
                    ),
                    parent_depth=ExpressionWrapper(
                        cte.col.depth,
                        output_field=int_field,
                    ),
                ).filter(
                    ~Q(parent_name="mars"),
                ).values(
                    "name",
                    path=Concat(
                        cte.col.path, Value("\x01"), F("name"),
                        output_field=text_field,
                    ),
                    depth=cte.col.depth + Value(1, output_field=int_field),
                    is_planet=Case(
                        When(parent_depth=0, then=Value(1)),
                        default=Value(0),
                        output_field=int_field,
                    ),
                ),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        regions = with_cte(
            cte, select=cte.join(Region, name=cte.col.name)
        ).annotate(
            path=cte.col.path,
            depth=cte.col.depth,
            is_planet=cte.col.is_planet,
        ).order_by("path")

        data = [(r.path.split("\x01"), r.is_planet) for r in regions]
        print(data)
        self.assertEqual(data, [
            (["bernard's star"], 0),
            (['proxima centauri'], 0),
            (['proxima centauri', 'proxima centauri b'], 1),
            (['sun'], 0),
            (['sun', 'earth'], 1),
            (['sun', 'earth', 'moon'], 0),
            (['sun', 'mars'], 1),  # mars moons excluded: parent_name != 'mars'
            (['sun', 'mercury'], 1),
            (['sun', 'venus'], 1),
        ])

    def test_recursive_cte_with_empty_union_part(self):
        def make_regions_cte(cte):
            return Region.objects.none().union(
                cte.join(Region, parent=cte.col.name),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        regions = with_cte(cte, select=cte.join(Region, name=cte.col.name))

        print(regions.query)
        try:
            self.assertEqual(regions.count(), 0)
        except DatabaseError:
            raise SkipTest(
                "Expected failure: QuerySet omits `EmptyQuerySet` from "
                "UNION queries resulting in invalid CTE SQL"
            )

        # -- recursive query "cte" does not have the form
        # -- non-recursive-term UNION [ALL] recursive-term
        # WITH RECURSIVE cte AS (
        #   SELECT "tests_region"."name", "tests_region"."parent_id"
        #   FROM "tests_region", "cte"
        #   WHERE "tests_region"."parent_id" = ("cte"."name")
        # )
        # SELECT COUNT(*)
        # FROM "tests_region", "cte"
        # WHERE "tests_region"."name" = ("cte"."name")

    def test_circular_ref_error(self):
        def make_bad_cte(cte):
            # NOTE: not a valid recursive CTE query
            return cte.join(Region, parent=cte.col.name).values(
                depth=cte.col.depth + 1,
            )
        cte = CTE.recursive(make_bad_cte)
        regions = with_cte(cte, select=cte.join(Region, name=cte.col.name))
        with self.assertRaises(ValueError) as context:
            print(regions.query)
        self.assertIn("Circular reference:", str(context.exception))

    def test_attname_should_not_mask_col_name(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                name="moon"
            ).values(
                "name",
                "parent_id",
            ).union(
                cte.join(Region, name=cte.col.parent_id).values(
                    "name",
                    "parent_id",
                ),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        regions = with_cte(
            cte,
            select=Region.objects.annotate(_ex=Exists(
                cte.queryset()
                .values(value=Value("1", output_field=int_field))
                .filter(name=OuterRef("name"))
            ))
            .filter(_ex=True)
            .order_by("name")
        )
        print(regions.query)

        data = [r.name for r in regions]
        self.assertEqual(data, ['earth', 'moon', 'sun'])

    def test_pickle_recursive_cte_queryset(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                parent__isnull=True
            ).annotate(
                depth=Value(0, output_field=int_field),
            ).union(
                cte.join(Region, parent=cte.col.name).annotate(
                    depth=cte.col.depth + Value(1, output_field=int_field),
                ),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        regions = with_cte(cte, select=cte).filter(depth=2).order_by("name")

        pickled_qs = pickle.loads(pickle.dumps(regions))

        data = [(r.name, r.depth) for r in pickled_qs]
        self.assertEqual(data, [(r.name, r.depth) for r in regions])
        self.assertEqual(data, [('deimos', 2), ('moon', 2), ('phobos', 2)])

    def test_alias_change_in_annotation(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                parent__name="sun",
            ).annotate(
                value=F('name'),
            ).union(
                cte.join(
                    Region.objects.annotate(value=F('name')),
                    parent_id=cte.col.name,
                ),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        query = with_cte(cte, select=cte)

        exclude_leaves = CTE(cte.queryset().filter(
            parent__name='sun',
        ).annotate(
            value=Concat(F('name'), F('name'))
        ), name='value_cte')

        query = with_cte(exclude_leaves, select=query.annotate(
            _exclude_leaves=Exists(
                exclude_leaves.queryset().filter(
                    name=OuterRef("name"),
                    value=OuterRef("value"),
                )
            )
        ).filter(_exclude_leaves=True))
        print(query.query)

        # Nothing should be returned.
        self.assertFalse(query)

    def test_alias_as_subquery(self):
        # This test covers CTEColumnRef.relabeled_clone
        def make_regions_cte(cte):
            return KeyPair.objects.filter(
                parent__key="level 1",
            ).annotate(
                rank=F('value'),
            ).union(
                cte.join(
                    KeyPair.objects.order_by(),
                    parent_id=cte.col.id,
                ).annotate(
                    rank=F('value'),
                ),
                all=True,
            )
        cte = CTE.recursive(make_regions_cte)
        children = with_cte(cte, select=cte)

        xdups = CTE(cte.queryset().filter(
            parent__key="level 1",
        ).annotate(
            rank=F('value')
        ).values('id', 'rank'), name='xdups')

        children = with_cte(xdups, select=children.annotate(
            _exclude=Exists(
                (
                    xdups.queryset().filter(
                        id=OuterRef("id"),
                        rank=OuterRef("rank"),
                    )
                )
            )
        ).filter(_exclude=True))

        print(children.query)
        query = KeyPair.objects.filter(parent__in=children)
        print(query.query)
        print(children.query)
        self.assertEqual(query.get().key, 'level 3')
        # Tests the case in which children's query was modified since it was
        # used in a subquery to define `query` above.
        self.assertEqual(
            list(c.key for c in children),
            ['level 2', 'level 2']
        )

    def test_materialized(self):
        # This test covers MATERIALIZED option in SQL query
        def make_regions_cte(cte):
            return KeyPair.objects.all()
        cte = CTE.recursive(make_regions_cte, materialized=True)

        query = with_cte(cte, select=KeyPair)
        print(query.query)
        self.assertTrue(
            str(query.query).startswith('WITH RECURSIVE "cte" AS MATERIALIZED')
        )

    def test_recursive_self_queryset(self):
        def make_regions_cte(cte):
            return Region.objects.filter(
                pk="earth"
            ).values("pk").union(
                cte.join(Region, parent=cte.col.pk).values("pk")
            )
        cte = CTE.recursive(make_regions_cte)
        queryset = with_cte(cte, select=cte).order_by("pk")
        print(queryset.query)
        self.assertEqual(list(queryset), [
            {'pk': 'earth'},
            {'pk': 'moon'},
        ])

    def test_cycle_with_list_of_columns(self):
        if connection.vendor == "sqlite":
            raise SkipTest("SQLite does not support CYCLE clause")

        cycle_a = Region.objects.create(name="cycle_a", parent=None)
        cycle_b = Region.objects.create(name="cycle_b", parent=cycle_a)
        cycle_c = Region.objects.create(name="cycle_c", parent=cycle_b)
        cycle_a.parent = cycle_c
        cycle_a.save()

        def make_regions_cte(cte):
            return Region.objects.filter(
                name="cycle_a"
            ).values(
                "name",
            ).union(
                cte.join(Region, parent=cte.col.name).values(
                    "name",
                ),
                all=True,
            )

        cte = CTE.recursive(
            make_regions_cte, cycle={"columns": ["name"], "using": "cycle_path"}
        )

        regions = with_cte(
            cte,
            select=cte.join(Region, name=cte.col.name)
            .order_by("name")
        )
        query_str = str(regions.query)
        print(query_str)

        self.assertIn("CYCLE name", query_str)
        self.assertIn("SET is_cycle", query_str)
        self.assertIn("TO true DEFAULT false", query_str)
        self.assertIn("USING cycle_path", query_str)

        data = list(regions.values_list("name", "is_cycle"))
        self.assertEqual(data, [
            ('cycle_a', False),
            ('cycle_a', True),
            ('cycle_b', False),
            ('cycle_c', False),
        ])

        cycle_a.delete()
        cycle_b.delete()
        cycle_c.delete()

    def test_cycle_with_dict_config(self):
        if connection.vendor == "sqlite":
            raise SkipTest("SQLite does not support CYCLE clause")

        alpha = Region.objects.create(name="alpha", parent=None)
        beta = Region.objects.create(name="beta", parent=alpha)
        gamma = Region.objects.create(name="gamma", parent=beta)
        delta = Region.objects.create(name="delta", parent=gamma)
        alpha.parent = delta
        alpha.save()

        def make_regions_cte(cte):
            return Region.objects.filter(name="alpha").values("name").union(
                cte.join(Region, parent=cte.col.name).values("name"),
                all=True,
            )

        cycle_config = {
            "columns": ["name"],
            "set": "cycle_detected",
            "to": "1",
            "default": "0",
            "using": "cycle_path",
        }
        cte = CTE.recursive(make_regions_cte, cycle=cycle_config)

        regions = with_cte(
            cte,
            select=cte.join(Region, name=cte.col.name)
            .order_by("name")
        )
        query_str = str(regions.query)
        print(query_str)

        self.assertIn("CYCLE name", query_str)
        self.assertIn("SET cycle_detected", query_str)
        self.assertIn("TO 1 DEFAULT 0", query_str)
        self.assertIn("USING cycle_path", query_str)

        data = list(regions.values_list("name", "cycle_detected", "cycle_path"))
        self.assertEqual(len(data), 5)
        
        # Build a dict for easier lookup: (name, cycle_detected) -> cycle_path
        path_lookup = {}
        for name, cycle_detected, cycle_path in data:
            path_lookup[(name, cycle_detected)] = cycle_path
        
        # First alpha (starting point): path contains only alpha
        self.assertIn("alpha", path_lookup[("alpha", 0)])
        self.assertNotIn("beta", path_lookup[("alpha", 0)])
        
        # Second alpha (cycle detected): path contains the full cycle
        self.assertIn("alpha", path_lookup[("alpha", 1)])
        self.assertIn("beta", path_lookup[("alpha", 1)])
        self.assertIn("gamma", path_lookup[("alpha", 1)])
        self.assertIn("delta", path_lookup[("alpha", 1)])
        
        # Beta: path contains alpha and beta
        self.assertIn("alpha", path_lookup[("beta", 0)])
        self.assertIn("beta", path_lookup[("beta", 0)])
        self.assertNotIn("gamma", path_lookup[("beta", 0)])
        
        # Gamma: path contains alpha, beta, and gamma
        self.assertIn("alpha", path_lookup[("gamma", 0)])
        self.assertIn("beta", path_lookup[("gamma", 0)])
        self.assertIn("gamma", path_lookup[("gamma", 0)])
        self.assertNotIn("delta", path_lookup[("gamma", 0)])
        
        # Delta: path contains alpha, beta, gamma, and delta
        self.assertIn("alpha", path_lookup[("delta", 0)])
        self.assertIn("beta", path_lookup[("delta", 0)])
        self.assertIn("gamma", path_lookup[("delta", 0)])
        self.assertIn("delta", path_lookup[("delta", 0)])

        alpha.delete()
        beta.delete()
        gamma.delete()
        delta.delete()

    def test_cycle_with_multiple_columns(self):
        if connection.vendor == "sqlite":
            raise SkipTest("SQLite does not support CYCLE clause")

        kp1 = KeyPair.objects.create(key="cyc_k1", value=100, parent=None)
        kp2 = KeyPair.objects.create(key="cyc_k2", value=200, parent=kp1)
        kp3 = KeyPair.objects.create(key="cyc_k3", value=300, parent=kp2)
        kp1.parent = kp3
        kp1.save()

        def make_keypair_cte(cte):
            return KeyPair.objects.filter(key="cyc_k1", value=100).values("id", "key", "value").union(
                cte.join(KeyPair, parent_id=cte.col.id).values("id", "key", "value"),
                all=True,
            )

        cte = CTE.recursive(
            make_keypair_cte, cycle={"columns": ["key", "value"], "using": "cycle_path"}
        )

        pairs = with_cte(
            cte,
            select=cte.join(KeyPair, key=cte.col.key, value=cte.col.value)
            .order_by("key", "value", "is_cycle")
        )
        query_str = str(pairs.query)
        print(query_str)

        self.assertIn("CYCLE key, value", query_str)
        self.assertIn("SET is_cycle", query_str)

        data = list(pairs.values_list("key", "value", "is_cycle", "cycle_path"))
        # if testing against psycopg3, cycle_path will be a list of string tuples instead of a string
        self.assertEqual(data, [
            ("cyc_k1", 100, False, '{"(cyc_k1,100)"}'),
            ("cyc_k1", 100, True, '{"(cyc_k1,100)","(cyc_k2,200)","(cyc_k3,300)","(cyc_k1,100)"}'),
            ("cyc_k2", 200, False, '{"(cyc_k1,100)","(cyc_k2,200)"}'),
            ("cyc_k3", 300, False, '{"(cyc_k1,100)","(cyc_k2,200)","(cyc_k3,300)"}'),
        ])

        kp1.delete()
        kp2.delete()
        kp3.delete()

    def test_cycle_with_materialized(self):
        if connection.vendor == "sqlite":
            raise SkipTest("SQLite does not support CYCLE clause")

        mat_a = Region.objects.create(name="mat_a", parent=None)
        mat_b = Region.objects.create(name="mat_b", parent=mat_a)
        mat_c = Region.objects.create(name="mat_c", parent=mat_b)
        mat_a.parent = mat_c
        mat_a.save()

        def make_regions_cte(cte):
            return Region.objects.filter(name="mat_a").values("name").union(
                cte.join(Region, parent=cte.col.name).values("name"),
                all=True,
            )

        cte = CTE.recursive(make_regions_cte, materialized=True, cycle=["name"])

        regions = with_cte(
            cte,
            select=cte.join(Region, name=cte.col.name)
            .order_by("name")
        )
        query_str = str(regions.query)
        print(query_str)

        self.assertIn("AS MATERIALIZED", query_str)
        self.assertIn("CYCLE name", query_str)
        self.assertIn("SET is_cycle", query_str)

        data = list(regions.values_list("name", "is_cycle"))
        self.assertEqual(data, [
            ("mat_a", False),
            ("mat_a", True),
            ("mat_b", False),
            ("mat_c", False),
        ])

        mat_a.delete()
        mat_b.delete()
        mat_c.delete()

    def test_cycle_hierarchical_traversal(self):
        if connection.vendor == "sqlite":
            raise SkipTest("SQLite does not support CYCLE clause")

        node1 = Region.objects.create(name="node1", parent=None)
        node2 = Region.objects.create(name="node2", parent=node1)
        node3 = Region.objects.create(name="node3", parent=node2)
        node4 = Region.objects.create(name="node4", parent=node3)
        node1.parent = node4
        node1.save()

        def make_regions_cte(cte):
            return Region.objects.filter(
                name="node1"
            ).values(
                "name",
            ).union(
                cte.join(Region, parent=cte.col.name).values(
                    "name",
                ),
                all=True,
            )

        cte = CTE.recursive(
            make_regions_cte, cycle={"columns": ["name"], "using": "cycle_path"}
        )
        regions = with_cte(
            cte,
            select=cte.join(Region, name=cte.col.name)
            .order_by("name")
        )
        query_str = str(regions.query)
        print(query_str)

        self.assertIn("CYCLE name", query_str)

        data = list(regions.values_list("name", "is_cycle"))
        self.assertEqual(data, [
            ("node1", False),
            ("node1", True),
            ("node2", False),
            ("node3", False),
            ("node4", False),
        ])

        non_cycle_rows = list(regions.filter(is_cycle=False).values_list("name", flat=True))
        self.assertEqual(sorted(non_cycle_rows), ["node1", "node2", "node3", "node4"])

        cycle_rows = list(regions.filter(is_cycle=True).values_list("name", flat=True))
        self.assertEqual(cycle_rows, ["node1"])

        node1.delete()
        node2.delete()
        node3.delete()
        node4.delete()
