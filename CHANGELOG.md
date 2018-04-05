# Django CTE change log

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
