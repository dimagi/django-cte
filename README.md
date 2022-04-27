# Common Table Expressions with Django

[![Build Status](https://travis-ci.com/dimagi/django-cte.png)](https://travis-ci.com/dimagi/django-cte)
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
pip install django nose flake8

nosetests
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
