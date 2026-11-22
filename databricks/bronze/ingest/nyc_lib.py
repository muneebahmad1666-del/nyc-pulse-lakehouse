"""Small helpers for the bronze job. Plain Python, no Spark, so they are easy to test."""

import calendar
import re
from datetime import date


def clean_name(name):
    """A column name Delta accepts: letters, digits and underscores, lower case.

    'start station id' -> 'start_station_id'. Only the NAME changes, never the values.
    """
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return cleaned or "col"


def clean_names(names):
    """Clean a list of names and make them unique (a second 'id' becomes 'id_2')."""
    out, seen = [], {}
    for name in names:
        c = clean_name(name)
        seen[c] = seen.get(c, 0) + 1
        out.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return out


def month_bounds(month):
    """First and last day of '2025-11' as dates."""
    y, m = map(int, month.split("-"))
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def months_in_window(start, end):
    """['2025-11', '2025-12', ...] for every month the window touches."""
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def window_for_month(month, start, end):
    """The part of the window that falls inside this month: (first day, last day)."""
    first, last = month_bounds(month)
    return max(first, start), min(last, end)


def parse_day_dir(name):
    """'day=2025-11-05/' -> date(2025, 11, 5). Anything else (like '_manifests/') -> None."""
    m = re.match(r"^day=(\d{4}-\d{2}-\d{2})/?$", name)
    return date.fromisoformat(m.group(1)) if m else None


def stem_of(file_name):
    """'201904-citibike-tripdata_1.csv.gz' -> '201904-citibike-tripdata_1'."""
    base = file_name.rsplit("/", 1)[-1]
    for ending in (".csv.gz", ".gz", ".csv"):
        if base.endswith(ending):
            return base[: -len(ending)]
    return base


def group_by_stem(files):
    """files: [(day, path)]. Returns {stem: [paths]}.

    Day files that came from the same source CSV share one header, so they can be read
    together. Files from different source CSVs may have different headers (Citi Bike
    changed its columns), so each group is read on its own and the groups are united by name.
    """
    groups = {}
    for _day, path in files:
        groups.setdefault(stem_of(path), []).append(path)
    return groups


def day_predicate(lo, hi):
    """SQL for the rows of a day window, used to replace exactly those rows."""
    return f"_day >= DATE'{lo}' AND _day <= DATE'{hi}'"


def month_predicate(month):
    return f"_month = '{month}'"


# Columns of the bronze run log (state/bronze_run_log). Their types are spelled out because Spark
# cannot guess the type of a column whose value is None (error CANNOT_DETERMINE_TYPE).
LOG_FIELDS = [
    ("env", "string"),
    ("source", "string"),
    ("month", "string"),
    ("window_start", "string"),
    ("window_end", "string"),
    ("files", "long"),
    ("rows", "long"),
    ("status", "string"),
    ("message", "string"),
    ("started_at", "timestamp"),
    ("finished_at", "timestamp"),
]


def log_tuple(record):
    """One run-log row as a tuple in LOG_FIELDS order. None is allowed for any value."""
    names = [name for name, _ in LOG_FIELDS]
    extra, missing = set(record) - set(names), set(names) - set(record)
    if extra or missing:
        raise ValueError(
            f"run log record: unexpected {sorted(extra)}, missing {sorted(missing)}"
        )
    out = []
    for name, kind in LOG_FIELDS:
        value = record[name]
        out.append(str(value) if kind == "string" and value is not None else value)
    return tuple(out)
