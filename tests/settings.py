from __future__ import absolute_import
from __future__ import unicode_literals

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

INSTALLED_APPS = ["tests"]

SECRET_KEY = "test"
