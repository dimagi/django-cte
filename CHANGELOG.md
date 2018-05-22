# Django CTE change log

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
