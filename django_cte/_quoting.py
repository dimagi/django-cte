import django

# `SQLCompiler.quote_name()` was added in Django 6.1, deprecating
# `quote_name_unless_alias()`, which did not quote table aliases. Use
# whichever one Django itself uses so generated SQL agrees on how aliases
# are spelled. Can be removed when the oldest supported Django is 6.1.
_QUOTE_NAME = (
    "quote_name" if django.VERSION >= (6, 1) else "quote_name_unless_alias"
)


def quote_name(compiler):
    """Get the compiler's name quoting function"""
    return getattr(compiler, _QUOTE_NAME)
