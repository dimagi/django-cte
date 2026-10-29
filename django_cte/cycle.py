import datetime

from django.db.models import TextField
from django.db.models.expressions import RawSQL, Value
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
    decides the output field of the mark column. A `Value` is taken like
    its value, but with its output field. A `RawSQL` without params is
    written into the query as is, for other constants.
    :param default_value: Value of the mark column when no cycle is
    detected (default: False). Its output field must be of the same type
    as the one of `cycle_value`.
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
        mark_field = get_mark_field(cycle_value)
        default_field = get_mark_field(default_value)
        if type(mark_field) is not type(default_field):
            raise ValueError(
                "CYCLE mark values must have the same type, got "
                f"{cycle_value!r} ({type(mark_field).__name__}) and "
                f"{default_value!r} ({type(default_field).__name__})"
            )
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

    def as_sql(self, qn, connection):
        """Get the CYCLE clause SQL

        :param qn: Name quoting function.
        :param connection: Connection the query is compiled for.
        :returns: The CYCLE clause SQL string.
        """
        sql = [
            "CYCLE", ", ".join(qn(c) for c in self.columns),
            "SET", qn(self.mark_column),
        ]
        if self.cycle_value is not True or self.default_value is not False:
            sql.extend([
                "TO", compile_mark_value(self.cycle_value, connection),
                "DEFAULT", compile_mark_value(self.default_value, connection),
            ])
        sql.extend(["USING", qn(self.path_column)])
        return " ".join(sql)


def get_mark_field(value):
    """Get the output field of a mark column value

    A plain value gets the output field `Value()` gives it, so "Y" and
    `Value("N")` match.
    """
    if isinstance(value, RawSQL):
        if value.params:
            raise ValueError("CYCLE mark RawSQL must not have params")
        return value.output_field
    if not isinstance(value, Value):
        value = Value(value)
    if not isinstance(value.value, (bool, int, float, str, datetime.date)):
        raise ValueError(
            "CYCLE mark values must be a bool, int, float, str, date, "
            f"datetime, Value or RawSQL, got {value.value!r}"
        )
    return value.output_field


def compile_mark_value(value, connection):
    """Get the SQL of a mark column value

    PostgreSQL accepts only constants in TO and DEFAULT, not parameters
    or casts, so the value is written into the SQL.
    """
    if isinstance(value, RawSQL):
        return value.sql
    if isinstance(value, Value):
        value = value.value
    # the driver writes these with a cast, a sign or as numeric, so they
    # are written as a typed literal of their text instead
    type_name = None
    if isinstance(value, datetime.datetime):
        type_name = "timestamptz" if timezone.is_aware(value) else "timestamp"
        value = value.isoformat()
    elif isinstance(value, datetime.date):
        type_name, value = "date", value.isoformat()
    elif isinstance(value, float):
        type_name, value = "float8", repr(value)
    elif isinstance(value, int) and value < 0:
        type_name, value = "bigint", str(value)
    # strip() drops the space psycopg writes before E'' and negative numbers
    sql = connection.ops.compose_sql("%s", [value]).strip()
    if type_name is not None:
        sql = f"{type_name} {sql}"
    # doubled % survives the driver's parameter interpolation
    return sql.replace("%", "%%")
