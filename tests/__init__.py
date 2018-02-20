from __future__ import absolute_import
from __future__ import unicode_literals

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
django.setup()
