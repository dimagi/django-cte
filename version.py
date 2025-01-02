"""Check or update version in __init__.py"""
import re
import sys
from datetime import datetime
from pathlib import Path


def main(argv=sys.argv):
    if len(argv) < 2:
        sys.exit(f"usage: {argv[0]} (check|update) [...]")
    cmd, *args = sys.argv[1:]
    if cmd in COMMANDS:
        COMMANDS[cmd](*args)
    else:
        sys.exit(f"unknown arguments: {argv[1:]}")


def check(ref):
    import django_cte
    if not ref.startswith("refs/tags/v"):
        sys.exit(f"unexpected ref: {ref}")
    version = ref.removeprefix("refs/tags/v")
    if version != django_cte.__version__:
        sys.exit(f"version mismatch: {version} != {django_cte.__version__}")


def update():
    """Add a timestamped dev version qualifier to the current version"""
    path = Path(__file__).parent / "django_cte/__init__.py"
    vexpr = re.compile(r"""(?<=^__version__ = )['"](.+)['"]$""", flags=re.M)
    with open(path, "r+") as file:
        text = file.read()
        match = vexpr.search(text)
        if not match:
            sys.exit("django_cte.__version__ not found")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        version = f"{match.group(1)}.dev{timestamp}"
        print("new version:", version)
        file.seek(0)
        file.write(vexpr.sub(repr(version), text))
        file.truncate()


COMMANDS = {"check": check, "update": update}


if __name__ == "__main__":
    main()
