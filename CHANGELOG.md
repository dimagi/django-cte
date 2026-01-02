# Django CTE change log

## 3.0.0 - 2026-02-05

- **BREAKING:** on Django 5.2 and later when joining a CTE to a queryset with
  LEFT OUTER JOIN (`_join_type=LOUTER`) and the join condition implicitly joins
  a related table, the implicit join is now a LEFT OUTER JOIN instead of an
  INNER JOIN. This makes the join behavior match SQL database semantics: the
  left-side rows are always preserved. To restore the old behavior, either
  do not use `_join_type=LOUTER` or add extra WHERE conditions as needed.
  See [#130](https://github.com/dimagi/django-cte/pull/130) for more details.
- Fixed `CTE.queryset()` queries involving aggregates ([#128](https://github.com/dimagi/django-cte/issues/128)).
- Fixed Django 5.2 ambiguous column names bug ([#127](https://github.com/dimagi/django-cte/issues/127)).
- Fixed possible ambiguous column names in SQL ([#125](https://github.com/dimagi/django-cte/issues/125)).
- Fixed Django 5.2 `CTE.queryset()` regression ([#124](https://github.com/dimagi/django-cte/pull/124)).

## 2.0.0 - 2025-06-16

- **API overhaul**
  - `With` has been renamed to `CTE`. `With` is deprecated and will be removed
    in a future version of django-cte.
  - `with_cte` was moved from a `CTEQuerySet` method to a stand-alone function.
  - `CTEManager` and `CTEQuerySet` are deprecated and should be removed from
    code that uses them, as they are no longer necessary. They will be removed
    in a future version of django-cte.
  - Reference the [documentation](https://dimagi.github.io/django-cte/) for new
    usage patterns.
- **BREAKING:** On Django 5.2 and later, the name specified in
  `.values('fk_name')` must match the name of the same column referenced by
  `cte.col.fk_name`—for example, in a join condition. It may end with `_id` or
  not, but the references must be consistent. This change may require previously
  working CTE queries to be adjusted when migrating to Django 5.2
  ([example](https://github.com/dimagi/django-cte/commit/321d92cd8d1edd515c1f5000a3b12c35265aa4f8)).
- Django 5.0 is EOL and no longer supported.
- Fixed broken `UNION` and other "combined" queries.
- Internally, the library has been updated to simplify the code and remove
  workarounds for old and unsupported versions of Django.
- Modernized development tooling
  - Replaced _nosetests_ with _pytest_.
  - Replaced _setup.py_ with _pyproject.toml_
  - Replaced _flake8_ with _ruff_.
  - Replaced _venv/pip_ with _uv_.
  - Improved Github Actions automation, including automated releases.
  - Dev versions of django-cte are now published on PyPI, making them easier to
    test and use before an official release is cut.

## 1.3.3 - 2024-06-07

- Handle empty result sets in CTEs ([#92](https://github.com/dimagi/django-cte/pull/92)).
- Fix `.explain()` in Django >= 4.0 ([#91](https://github.com/dimagi/django-cte/pull/91)).
- Fixed bug in deferred loading ([#90](https://github.com/dimagi/django-cte/pull/90)).

## 1.3.2 - 2023-11-20

- Work around changes in Django 4.2 that broke CTE queries due to internally
  generated column aliases in the query compiler. The workaround is not always
  effective. Some queries will produce mal-formed SQL. For example, CTE queries
  with window functions.

## 1.3.1 - 2023-06-13

- Fix: `.update()` did not work when using CTE manager or when accessing nested
  tables.

## 1.3.0 - 2023-05-24

- Add support for Materialized CTEs.
- Fix: add EXPLAIN clause in correct position when using `.explain()` method.

## v1.2.1 - 2022-07-07

- Fix compatibility with non-CTE models.

## v1.2.0 - 2022-03-30

- Add support for Django 3.1, 3.2 and 4.0.
- Quote the CTE table name if needed.
- Resolve `OuterRef` in CTE `Subquery`.
- Fix default `CTEManager` so it can use `from_queryset` corectly.
- Fix for Django 3.0.5+.

## v1.1.5 - 2020-02-07

- Django 3 compatibility. Thank you @tim-schilling and @ryanhiebert!

## v1.1.4 - 2018-07-30

- Python 3 compatibility.

## v1.1.3 - 2018-06-19

- Fix CTE alias bug.

## v1.1.2 - 2018-05-22

- Use `_default_manager` instead of `objects`.

## v1.1.1 - 2018-04-13

- Fix recursive CTE pickling. Note: this is currently [broken on Django
  master](https://github.com/django/django/pull/9134#pullrequestreview-112057277).

## v1.1.0 - 2018-04-09

- `With.queryset()` now uses the CTE model's manager to create a new `QuerySet`,
  which makes it easier to work with custom `QuerySet` classes.

## v1.0.0 - 2018-04-04

- BACKWARD INCOMPATIBLE CHANGE: `With.queryset()` no longer accepts a `model`
  argument.
- Improve `With.queryset()` to select directly from the CTE rather than
  joining to anoter QuerySet.
- Refactor `With.join()` to use real JOIN clause.

## v0.1.4 - 2018-03-21

- Fix related field attname masking CTE column.

## v0.1.3 - 2018-03-15

- Add `django_cte.raw.raw_cte_sql` for constructing CTEs with raw SQL.

## v0.1.2 - 2018-02-21

- Improve error on bad recursive reference.
- Add more tests.
- Add change log.
- Improve README.
- PEP-8 style fixes.

## v0.1.1 - 2018-02-21

- Fix readme formatting on PyPI.

## v0.1 - 2018-02-21

- Initial implementation.
