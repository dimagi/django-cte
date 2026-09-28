import datetime

from django.db.models import (
    BooleanField,
    DateField,
    DateTimeField,
    FloatField,
    IntegerField,
    TextField,
)
from django.utils import timezone

DICT_KEYS = frozenset([
    "columns", "set", "to", "default", "using", "using_output_field",
])


class CycleClause:
    """CYCLE clause of a recursive CTE

    The `output_columns` attribute maps the names of the columns added
    by the clause to their output fields.

    :param columns: Sequence of CTE column names to track for cycles.
    :param mark_column: Name of the generated cycle mark column
    (default: "is_cycle").
    :param cycle_value: Value of the mark column when a cycle is detected
    (default: True). A bool, int, float, str, date or datetime. Its type
    decides the output field of the mark column.
    :param default_value: Value of the mark column when no cycle is
    detected (default: False).
    :param path_column: Name of the generated path column (default:
    "path").
    :param path_output_field: Output field of the path column (default:
    `TextField()`). PostgreSQL generates the column as `ARRAY[RECORD]`,
    and `RECORD` is a pseudo-type for unspecified row types, which
    psycopg2 does not adapt to a list because it considers `RECORD`
    unknown. Pass an `ArrayField` and configure list adaptation to get
    anything other than text.

    See:
    * https://www.psycopg.org/docs/usage.html#lists-adaptation
    * https://www.psycopg.org/docs/extensions.html#cast-array-unknown
    * https://www.postgresql.org/docs/current/datatype-pseudo.html#DATATYPE-PSEUDO
    """

    def __init__(self, columns, mark_column="is_cycle", cycle_value=True,
                 default_value=False, path_column="path",
                 path_output_field=None):
        if isinstance(columns, str):
            raise ValueError(
                "CYCLE columns must be a sequence of column names, "
                "not a string"
            )
        if not columns:
            raise ValueError("CYCLE requires at least one column")
        self.columns = tuple(columns)
        self.mark_column = mark_column
        self.cycle_value = cycle_value
        self.default_value = default_value
        self.cycle_sql, mark_field = compile_mark_value(cycle_value)
        self.default_sql, _ = compile_mark_value(default_value)
        self.path_column = path_column
        self.path_output_field = path_output_field or TextField()
        self.output_columns = {
            self.mark_column: mark_field,
            self.path_column: self.path_output_field,
        }

    @classmethod
    def parse(cls, cycle):
        """Get a clause from the `cycle` argument of `CTE.recursive`

        :param cycle: A `CycleClause`, a sequence of column names, a dict
        of `CycleClause` keyword arguments by their public key names, or
        None.
        :returns: A `CycleClause` or None.
        :raises: `ValueError` for invalid/unknown configuration.
        """
        if cycle is None or isinstance(cycle, cls):
            return cycle
        if isinstance(cycle, (list, tuple)):
            return cls(cycle)
        if isinstance(cycle, dict):
            unknown = set(cycle) - DICT_KEYS
            if unknown:
                raise ValueError(
                    f"Unknown cycle option(s): {', '.join(sorted(unknown))}. "
                    f"Valid options are: {', '.join(sorted(DICT_KEYS))}"
                )
            return cls(
                cycle.get("columns", ()),
                mark_column=cycle.get("set", "is_cycle"),
                cycle_value=cycle.get("to", True),
                default_value=cycle.get("default", False),
                path_column=cycle.get("using", "path"),
                path_output_field=cycle.get("using_output_field"),
            )
        raise ValueError(
            "cycle must be a sequence of column names or a dict, "
            f"got {type(cycle).__name__}"
        )

    def as_sql(self, qn):
        """Get the CYCLE clause SQL

        :param qn: Name quoting function.
        :returns: The CYCLE clause SQL string.
        """
        return (
            f"CYCLE {', '.join(qn(c) for c in self.columns)} "
            f"SET {qn(self.mark_column)} "
            f"TO {self.cycle_sql} DEFAULT {self.default_sql} "
            f"USING {qn(self.path_column)}"
        )


def compile_mark_value(value):
    """Get the SQL and the output field of a mark column value

    PostgreSQL accepts only constants in TO and DEFAULT, not parameters
    or casts, so the value is written into the SQL.
    """
    if isinstance(value, bool):
        return ("true" if value else "false"), BooleanField()
    if isinstance(value, int):
        if value < 0:
            # a bare -1 is a syntax error, a typed literal is not
            return f"bigint '{int(value)}'", IntegerField()
        return str(int(value)), IntegerField()
    if isinstance(value, float):
        # a bare 1.5 is numeric, which the driver returns as Decimal
        return f"float8 '{float(value)!r}'", FloatField()
    if isinstance(value, str):
        return quote_string(value), TextField()
    if isinstance(value, datetime.datetime):
        type_name = "timestamptz" if timezone.is_aware(value) else "timestamp"
        return f"{type_name} '{value.isoformat()}'", DateTimeField()
    if isinstance(value, datetime.date):
        return f"date '{value.isoformat()}'", DateField()
    raise ValueError(
        "CYCLE mark values must be a bool, int, float, str, date or "
        f"datetime, got {value!r}"
    )


def quote_string(value):
    # doubled % survives the driver's parameter interpolation
    value = value.replace("'", "''").replace("%", "%%")
    if "\\" in value:
        # E'' reads backslashes as escapes whatever standard_conforming_strings is
        return "E'" + value.replace("\\", "\\\\") + "'"
    return "'" + value + "'"
