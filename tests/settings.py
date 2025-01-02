from __future__ import absolute_import
from __future__ import unicode_literals

import os
import json

if "DB_SETTINGS" in os.environ:
    _db_settings = json.loads(os.environ["DB_SETTINGS"])
else:
    # sqlite3 by default
    # must be sqlite3 >= 3.8.3 supporting WITH clause
    # must be sqlite3 >= 3.35.0 supporting MATERIALIZED option
    _db_settings = {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }

DATABASES = {'default': _db_settings}

INSTALLED_APPS = ["tests"]

SECRET_KEY = "test"
USE_TZ = False
