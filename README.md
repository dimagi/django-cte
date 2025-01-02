# Common Table Expressions with Django

[![Build Status](https://github.com/dimagi/django-cte/actions/workflows/tests.yml/badge.svg)](https://github.com/dimagi/django-cte/actions/workflows/tests.yml)
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
python -m venv .venv
source .venv/bin/activate
pip install django pytest-unmagic flake8

pytest
flake8 --config=setup.cfg
```

All feature and bug contributions are expected to be covered by tests.


## Uploading to PyPI

Package and upload the generated files. This assumes the `django-cte` repository
has been configured in `~/.pypirc`.

```
pip install -r pkg-requires.txt

python setup.py sdist bdist_wheel
twine upload --repository=django-cte dist/*
```
