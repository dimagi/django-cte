# Common Table Expressions with Django

[![Tests](https://github.com/dimagi/django-cte/actions/workflows/tests.yml/badge.svg)](https://github.com/dimagi/django-cte/actions/workflows/tests.yml)
[![PyPI version](https://badge.fury.io/py/django-cte.svg)](https://badge.fury.io/py/django-cte)

## Installation
```
pip install django-cte
```


## Documentation

The [django-cte documentation](https://dimagi.github.io/django-cte/) shows how
to use Common Table Expressions with the Django ORM.


## Running tests

```
cd django-cte
mkvirtualenv cte  # or however you choose to setup your environment
pip install django pytest-django flake8

python -m pytest
flake8 --config=setup.cfg
```

All feature and bug contributions are expected to be covered by tests.


## Uploading to PyPI

Package and upload the generated files.

```
pip install -r pkg-requires.txt

python setup.py sdist bdist_wheel
twine upload dist/*
```
