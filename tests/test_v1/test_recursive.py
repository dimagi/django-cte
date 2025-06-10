import pickle
from unittest import SkipTest

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

from django_cte import With

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

        cte = With.recursive(make_regions_cte)

        regions = (
            cte.join(Region, name=cte.col.name)
            .with_cte(cte)
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
        cte = With.recursive(make_regions_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte).annotate(
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
        cte = With.recursive(make_regions_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte)

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
        cte = With.recursive(make_bad_cte)
        regions = cte.join(Region, name=cte.col.name).with_cte(cte)
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
        cte = With.recursive(make_regions_cte)
        regions = (
            Region.objects.all()
            .with_cte(cte)
            .annotate(_ex=Exists(
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
        cte = With.recursive(make_regions_cte)
        regions = cte.queryset().with_cte(cte).filter(depth=2).order_by("name")

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
                    Region.objects.all().annotate(
                        value=F('name'),
                    ),
                    parent_id=cte.col.name,
                ),
                all=True,
            )
        cte = With.recursive(make_regions_cte)
        query = cte.queryset().with_cte(cte)

        exclude_leaves = With(cte.queryset().filter(
            parent__name='sun',
        ).annotate(
            value=Concat(F('name'), F('name'))
        ), name='value_cte')

        query = query.annotate(
            _exclude_leaves=Exists(
                exclude_leaves.queryset().filter(
                    name=OuterRef("name"),
                    value=OuterRef("value"),
                )
            )
        ).filter(_exclude_leaves=True).with_cte(exclude_leaves)
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
                    KeyPair.objects.all().order_by(),
                    parent_id=cte.col.id,
                ).annotate(
                    rank=F('value'),
                ),
                all=True,
            )
        cte = With.recursive(make_regions_cte)
        children = cte.queryset().with_cte(cte)

        xdups = With(cte.queryset().filter(
            parent__key="level 1",
        ).annotate(
            rank=F('value')
        ).values('id', 'rank'), name='xdups')

        children = children.annotate(
            _exclude=Exists(
                (
                    xdups.queryset().filter(
                        id=OuterRef("id"),
                        rank=OuterRef("rank"),
                    )
                )
            )
        ).filter(_exclude=True).with_cte(xdups)

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
        cte = With.recursive(make_regions_cte, materialized=True)

        query = KeyPair.objects.with_cte(cte)
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
        cte = With.recursive(make_regions_cte)
        queryset = cte.queryset().with_cte(cte).order_by("pk")
        print(queryset.query)
        self.assertEqual(list(queryset), [
            {'pk': 'earth'},
            {'pk': 'moon'},
        ])
