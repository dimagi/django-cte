# Common Table Expressions with Django

* Table of contents (this line will not be displayed).
{:toc}

A Common Table Expression acts like a temporary table or view that exists only
for the duration of the query it is attached to. django-cte allows common table
expressions to be attached to normal Django ORM queries.


## Simple Common Table Expressions

See [Appendix A](#appendix-a-model-definitions-used-in-sample-code) for model
definitions used in sample code.

Simple CTEs are constructed using `CTE(...)`. A CTE is added to a queryset using
`with_cte(cte, select=queryset)`, which adds the `WITH` expression before the
main `SELECT` query. A CTE can be joined to a model or other `QuerySet` using
its `<CTE>.join(...)` method, which creates a new queryset with a `JOIN` and
`ON` condition.

```py
from django_cte import CTE, with_cte

cte = CTE(
    Order.objects
    .values("region_id")
    .annotate(total=Sum("amount"))
)

orders = with_cte(
    # WITH cte ...
    cte,

    # SELECT ... FROM orders INNER JOIN cte ON orders.region_id = cte.region_id
    select=cte.join(Order, region=cte.col.region_id)

    # Annotate each Order with a "region_total"
    .annotate(region_total=cte.col.total)
)

print(orders.query)  # print SQL
```

The `orders` SQL, after formatting for readability, would look something like
this:

```sql
WITH RECURSIVE "cte" AS (
    SELECT
        "orders"."region_id",
        SUM("orders"."amount") AS "total"
    FROM "orders"
    GROUP BY "orders"."region_id"
)
SELECT
    "orders"."id",
    "orders"."region_id",
    "orders"."amount",
    "cte"."total" AS "region_total"
FROM "orders"
INNER JOIN "cte" ON "orders"."region_id" = "cte"."region_id"
```

The `orders` query is a queryset containing annotated `Order` objects, just as
you would get from a query like `Order.objects.annotate(region_total=...)`. Each
`Order` object will be annotated with a `region_total` attribute, which is
populated with the value of the corresponding total from the joined CTE query.

You may have noticed the CTE in this query uses `WITH RECURSIVE` even though
this is not a [Recursive Common Table Expression](#recursive-common-table-expressions).
The `RECURSIVE` keyword is always used, even for non-recursive CTEs. On
databases such as PostgreSQL and SQLite this has no effect other than allowing
recursive CTEs to be included in the WITH block.


## Recursive Common Table Expressions

Recursive CTE queries allow fundamentally new types of queries that are
not otherwise possible.

Recursive CTEs are constructed using `CTE.recursive()`, which takes as its
first argument a function that constructs and returns a recursive query.
Recursive queries have two elements: first a non-recursive query element, and
second a recursive query element. The second is typically attached to the first
using `QuerySet.union()`.

```py
def make_regions_cte(cte):
    # non-recursive: get root nodes
    return Region.objects.filter(
        parent__isnull=True
    ).values(
        "name",
        path=F("name"),
        depth=Value(0, output_field=IntegerField()),
    ).union(
        # recursive union: get descendants
        cte.join(Region, parent=cte.col.name).values(
            "name",
            path=Concat(
                cte.col.path, Value(" / "), F("name"),
                output_field=TextField(),
            ),
            depth=cte.col.depth + Value(1, output_field=IntegerField()),
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
```

`Region` objects returned by this query will have `path` and `depth` attributes.
The results will be ordered by `path` (hierarchically by region name). The SQL
produced by this query looks something like this:

```sql
WITH RECURSIVE "cte" AS (
    SELECT
        "region"."name",
        "region"."name" AS "path",
        0 AS "depth"
    FROM "region"
    WHERE "region"."parent_id" IS NULL

    UNION ALL

    SELECT
        "region"."name",
        "cte"."path" || ' / ' || "region"."name" AS "path",
        "cte"."depth" + 1 AS "depth"
    FROM "region"
    INNER JOIN "cte" ON "region"."parent_id" = "cte"."name"
)
SELECT
    "region"."name",
    "region"."parent_id",
    "cte"."path" AS "path",
    "cte"."depth" AS "depth"
FROM "region"
INNER JOIN "cte" ON "region"."name" = "cte"."name"
WHERE "cte"."depth" = 2
ORDER BY "path" ASC
```


## Named Common Table Expressions

It is possible to add more than one CTE to a query. To do this, each CTE must
have a unique name. `CTE(queryset)` returns a CTE with the name `'cte'` by
default, but that can be overridden: `CTE(queryset, name='custom')` or
`CTE.recursive(make_queryset, name='custom')`. This allows each CTE to be
referenced uniquely within a single query.

Also note that a CTE may reference other CTEs in the same query.

Example query with two CTEs, and the second (`totals`) CTE references the first
(`rootmap`):

```py
def make_root_mapping(rootmap):
    return Region.objects.filter(
        parent__isnull=True
    ).values(
        "name",
        root=F("name"),
    ).union(
        rootmap.join(Region, parent=rootmap.col.name).values(
            "name",
            root=rootmap.col.root,
        ),
        all=True,
    )
rootmap = CTE.recursive(make_root_mapping, name="rootmap")

totals = CTE(
    rootmap.join(Order, region_id=rootmap.col.name)
    .values(
        root=rootmap.col.root,
    ).annotate(
        orders_count=Count("id"),
        region_total=Sum("amount"),
    ),
    name="totals",
)

root_regions = with_cte(
    # Important: add both CTEs to the query
    rootmap,
    totals,

    select=totals.join(Region, name=totals.col.root)
    .annotate(
        # count of orders in this region and all subregions
        orders_count=totals.col.orders_count,
        # sum of order amounts in this region and all subregions
        region_total=totals.col.region_total,
    )
)
```

And the resulting SQL.

```sql
WITH RECURSIVE "rootmap" AS (
    SELECT
        "region"."name",
        "region"."name" AS "root"
    FROM "region"
    WHERE "region"."parent_id" IS NULL

    UNION ALL

    SELECT
        "region"."name",
        "rootmap"."root" AS "root"
    FROM "region"
    INNER JOIN "rootmap" ON "region"."parent_id" = "rootmap"."name"
),
"totals" AS (
    SELECT
        "rootmap"."root" AS "root",
        COUNT("orders"."id") AS "orders_count",
        SUM("orders"."amount") AS "region_total"
    FROM "orders"
    INNER JOIN "rootmap" ON "orders"."region_id" = "rootmap"."name"
    GROUP BY "rootmap"."root"
)
SELECT
    "region"."name",
    "region"."parent_id",
    "totals"."orders_count" AS "orders_count",
    "totals"."region_total" AS "region_total"
FROM "region"
INNER JOIN "totals" ON "region"."name" = "totals"."root"
```


## Selecting FROM a Common Table Expression

Sometimes it is useful to construct queries where the final `FROM` clause
contains only common table expression(s). This is possible with
`CTE(...).queryset()`.

Each returned row may be a model object:

```py
cte = CTE(
    Order.objects
    .annotate(region_parent=F("region__parent_id")),
)
orders = with_cte(cte, select=cte.queryset())
```

And the resulting SQL:

```sql
WITH RECURSIVE "cte" AS (
    SELECT
        "orders"."id",
        "orders"."region_id",
        "orders"."amount",
        "region"."parent_id" AS "region_parent"
    FROM "orders"
    INNER JOIN "region" ON "orders"."region_id" = "region"."name"
)
SELECT
    "cte"."id",
    "cte"."region_id",
    "cte"."amount",
    "cte"."region_parent" AS "region_parent"
FROM "cte"
```

It is also possible to do the same with `values(...)` queries:

```py
cte = CTE(
    Order.objects
    .values(
        "region_id",
        region_parent=F("region__parent_id"),
    )
    .distinct()
)
values = with_cte(cte, select=cte).filter(region_parent__isnull=False)
```

Which produces this SQL:

```sql
WITH RECURSIVE "cte" AS (
    SELECT DISTINCT
        "orders"."region_id",
        "region"."parent_id" AS "region_parent"
    FROM "orders"
    INNER JOIN "region" ON "orders"."region_id" = "region"."name"
)
SELECT
    "cte"."region_id",
    "cte"."region_parent" AS "region_parent"
FROM "cte"
WHERE "cte"."region_parent" IS NOT NULL
```

You may have noticed that when a CTE is passed to the `select=...` argument as
in `with_cte(cte, select=cte)`, the `.queryset()` call is optional and may be
omitted.


## Experimental: Left Outer Join

Django does not provide precise control over joins, but there is an experimental
way to perform a `LEFT OUTER JOIN` with a CTE query using the `_join_type`
keyword argument of `CTE.join(...)`.

```py
from django.db.models.sql.constants import LOUTER

totals = CTE(
    Order.objects
    .values("region_id")
    .annotate(total=Sum("amount"))
    .filter(total__gt=100)
)
orders = with_cte(
    totals,
    select=totals
    .join(Order, region=totals.col.region_id, _join_type=LOUTER)
    .annotate(region_total=totals.col.total)
)
```

Which produces the following SQL

```sql
WITH RECURSIVE "cte" AS (
    SELECT
        "orders"."region_id",
        SUM("orders"."amount") AS "total"
    FROM "orders"
    GROUP BY "orders"."region_id"
    HAVING SUM("orders"."amount") > 100
)
SELECT
    "orders"."id",
    "orders"."region_id",
    "orders"."amount",
    "cte"."total" AS "region_total"
FROM "orders"
LEFT OUTER JOIN "cte" ON "orders"."region_id" = "cte"."region_id"
```

WARNING: as noted, this feature is experimental. There may be scenarios where
Django automatically converts a `LEFT OUTER JOIN` to an `INNER JOIN` in the
process of building the query. Be sure to test your queries to ensure they
produce the desired SQL.


## Materialized CTE

Both PostgreSQL 12+ and sqlite 3.35+ supports `MATERIALIZED` keyword for CTE
queries. To enforce usage of this keyword add `materialized` as a parameter of
`CTE(..., materialized=True)`.


```py
cte = CTE(
    Order.objects.values('id'),
    materialized=True
)
```

Which produces this SQL:

```sql
WITH RECURSIVE "cte" AS MATERIALIZED (
    SELECT 
        "orders"."id"
    FROM "orders"
)
...
```


## Raw CTE SQL

Some queries are easier to construct with raw SQL than with the Django ORM.
`raw_cte_sql()` is one solution for situations like that. The down-side is that
each result field in the raw query must be explicitly mapped to a field type.
The up-side is that there is no need to compromise result-set expressiveness
with the likes of `Manager.raw()`.

A short example:

```py
from django.db.models import IntegerField, TextField
from django_cte.raw import raw_cte_sql

cte = CTE(raw_cte_sql(
    """
    SELECT region_id, AVG(amount) AS avg_order
    FROM orders
    WHERE region_id = %s
    GROUP BY region_id
    """,
    ["moon"],
    {
        "region_id": TextField(),
        "avg_order": IntegerField(),
    },
))
moon_avg = with_cte(
    cte,
    select=cte
    .join(Region, name=cte.col.region_id)
    .annotate(avg_order=cte.col.avg_order)
)
```

Which produces this SQL:

```sql
WITH RECURSIVE "cte" AS (
    SELECT region_id, AVG(amount) AS avg_order
    FROM orders
    WHERE region_id = 'moon'
    GROUP BY region_id
)
SELECT
    "region"."name",
    "region"."parent_id",
    "cte"."avg_order" AS "avg_order"
FROM "region"
INNER JOIN "cte" ON "region"."name" = "cte"."region_id"
```

**WARNING**: Be very careful when writing raw SQL. Use bind parameters to
prevent SQL injection attacks.


## More Advanced Use Cases

A few more advanced techniques as well as example query results can be found
in the tests:

- [`test_cte.py`](https://github.com/dimagi/django-cte/blob/main/tests/test_cte.py)
- [`test_recursive.py`](https://github.com/dimagi/django-cte/blob/main/tests/test_recursive.py)
- [`test_raw.py`](https://github.com/dimagi/django-cte/blob/main/tests/test_raw.py)


## Appendix A: Model definitions used in sample code

```py
class Order(Model):
    id = AutoField(primary_key=True)
    region = ForeignKey("Region", on_delete=CASCADE)
    amount = IntegerField(default=0)

    class Meta:
        db_table = "orders"


class Region(Model):
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)

    class Meta:
        db_table = "region"
```


## Appendix B: django-cte v1 documentation (DEPRECATED)

The syntax for constructing CTE queries changed slightly in django-cte 2.0. The
most important change is that a custom model manager is no longer required on
models used to construct CTE queries. The documentation has been updated to use
v2 syntax, but the [documentation for v1](https://github.com/dimagi/django-cte/blob/v1.3.3/docs/index.md)
can be found on Github if needed.
