"""Bronze job: copy the raw files in S3 into Delta tables, with nothing cleaned.

Bronze keeps every value exactly as it arrived (all text), adds four columns, and never drops a
row. Typing, de-duplicating and quarantine are silver's job.

  _source_file  the S3 file the row came from
  _day          the day folder it was in (empty for monthly sources: TLC and the rejected rows)
  _month        'YYYY-MM' (the partition)
  _ingested_at  when bronze wrote it

One run covers a window of days, month by month. The same code does a one-month dev run, the
full prod history, or a single day:

  --env dev  --start 2025-11-01 --end 2025-11-30
  --env prod --start 2019-02-01 --end 2026-10-31
  --env prod --start 2026-11-05 --end 2026-11-05          (daily mode)

Re-running a window is safe: it REPLACES exactly the rows of that window (Delta replaceWhere).

Where things go (bucket B, environment E):
  tables  s3://B/E/tables/bronze/<table>      external Delta tables
  state   s3://B/E/state/bronze_run_log       one row per source and month
  (E/managed/ is the catalog's managed location and is not used here.)

Run as a Databricks Python script task with the options above as parameters, or in a notebook:
  main(["--env", "dev", "--start", "2025-11-01", "--end", "2025-11-30"])
Add --dry-run to only list what would be read (no Spark reads, no writes).
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import reduce

try:  # a Python script task may not put its own folder on the path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:  # notebooks have no __file__
    pass

import nyc_lib as lib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEFAULT_BUCKET = os.environ.get(
    "NYC_BUCKET", "tb-scale-lakehouse-852824353987-us-east-1-an"
)
TLC_TYPES = ["yellow", "green", "fhv", "fhvhv"]
TABLE = {
    "311": "nyc311_raw",
    "citibike": "citibike_trips_raw",
    "citibike_rejected": "citibike_rejected_raw",
    "permits": "permits_changes_raw",
    **{f"tlc_{t}": f"tlc_{t}_raw" for t in TLC_TYPES},
}
CSV_OPTIONS = {"header": "true", "inferSchema": "false", "quote": '"', "escape": '"'}
_SPARK_TYPES = {
    "string": StringType(),
    "long": LongType(),
    "timestamp": TimestampType(),
}
LOG_SCHEMA = StructType(
    [StructField(n, _SPARK_TYPES[k], True) for n, k in lib.LOG_FIELDS]
)


# ----------------------------------------------------------------------------- context


@dataclass
class Ctx:
    spark: object
    dbutils: object
    env: str
    catalog: str
    bucket: str
    raw_prefix: str
    register: bool
    day_cache: dict = field(default_factory=dict)

    @property
    def raw(self):
        return f"s3://{self.bucket}/{self.raw_prefix}"

    def table_path(self, key):
        return f"s3://{self.bucket}/{self.env}/tables/bronze/{TABLE[key]}"

    @property
    def log_path(self):
        return f"s3://{self.bucket}/{self.env}/state/bronze_run_log"


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except ImportError:  # inside a notebook
        import IPython

        return IPython.get_ipython().user_ns["dbutils"]


# ----------------------------------------------------------------------------- listing


def safe_ls(dbutils, path):
    """dbutils.fs.ls, but a missing folder gives []. Other errors are raised."""
    try:
        return dbutils.fs.ls(path)
    except Exception as e:
        text = str(e)
        if "FileNotFound" in text or "does not exist" in text or "NOT_FOUND" in text:
            return []
        raise


def days_present(ctx, folder):
    """Every day that has a folder under raw/<folder>/ (one listing, cached)."""
    if folder not in ctx.day_cache:
        found = (
            lib.parse_day_dir(e.name)
            for e in safe_ls(ctx.dbutils, f"{ctx.raw}/{folder}/")
        )
        ctx.day_cache[folder] = sorted(d for d in found if d)
    return ctx.day_cache[folder]


def days_in_window(ctx, folder, lo, hi):
    return [d for d in days_present(ctx, folder) if lo <= d <= hi]


def list_day_files(ctx, folder, days, workers=16):
    """[(day, path)] for every .gz file in the given day folders."""

    def one(day):
        entries = safe_ls(ctx.dbutils, f"{ctx.raw}/{folder}/day={day}/")
        return [(day, e.path) for e in entries if e.name.endswith(".gz")]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return [item for files in pool.map(one, days) for item in files]


# ----------------------------------------------------------------------------- reading


def read_csv(spark, paths, multiline=False):
    """CSV files as all-text columns with clean names, plus _source_file."""
    reader = spark.read.options(**CSV_OPTIONS)
    if multiline:
        reader = reader.option("multiLine", "true")
    df = reader.csv(paths)
    names = lib.clean_names(df.columns)
    cols = [F.col(f"`{old}`").alias(new) for old, new in zip(df.columns, names)]
    return df.select(*cols, F.col("_metadata.file_path").alias("_source_file"))


def union_by_name(frames):
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def with_day_meta(df):
    """_day and _month come from the day=... folder in the file path."""
    return (
        df.withColumn(
            "_day",
            F.to_date(
                F.regexp_extract("_source_file", r"/day=(\d{4}-\d{2}-\d{2})/", 1)
            ),
        )
        .withColumn("_month", F.date_format("_day", "yyyy-MM"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def with_month_meta(df, month):
    return (
        df.withColumn("_day", F.lit(None).cast("date"))
        .withColumn("_month", F.lit(month))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ----------------------------------------------------------------------------- writing


def write_window(ctx, key, df, predicate):
    """Replace the rows matching `predicate` with df (first write creates the table)."""
    from delta.tables import DeltaTable

    path = ctx.table_path(key)
    writer = (
        df.write.format("delta").option("mergeSchema", "true").partitionBy("_month")
    )
    if DeltaTable.isDeltaTable(ctx.spark, path):
        writer.mode("overwrite").option("replaceWhere", predicate).save(path)
    else:
        writer.mode("append").save(path)
    if ctx.register:
        ctx.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {ctx.catalog}.bronze.{TABLE[key]} USING DELTA LOCATION '{path}'"
        )
    return ctx.spark.read.format("delta").load(path).where(predicate).count()


def log_run(ctx, record):
    """Append one line to the run log. The schema is explicit, so None values are fine."""
    row = lib.log_tuple(record)
    (
        ctx.spark.createDataFrame([row], schema=LOG_SCHEMA)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(ctx.log_path)
    )


def safe_log(ctx, record):
    """A problem with the run log must not undo or fail ingestion that already succeeded."""
    try:
        log_run(ctx, record)
        return True
    except Exception as e:  # noqa: BLE001 - the data is already written; report it, do not fail the run
        print(f"  WARNING: could not write the run log: {str(e)[:200]}", flush=True)
        return False


# ----------------------------------------------------------------------------- sources
# Each function returns a list of (table key, files read, rows written).


def ingest_311(ctx, month, lo, hi, dry):
    days = days_in_window(ctx, "nyc311", lo, hi)
    if not days or dry:
        return [("311", len(days), None)]
    paths = [f"{ctx.raw}/nyc311/day={d}/" for d in days]
    df = ctx.spark.read.text(paths).select(
        F.col("value").alias("raw_json"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    rows = write_window(ctx, "311", with_day_meta(df), lib.day_predicate(lo, hi))
    return [("311", len(days), rows)]


def ingest_citibike(ctx, month, lo, hi, dry):
    days = days_in_window(ctx, "citibike", lo, hi)
    files = list_day_files(ctx, "citibike", days)
    groups = lib.group_by_stem(files)
    results = []
    if files and not dry:
        frames = [read_csv(ctx.spark, paths) for paths in groups.values()]
        rows = write_window(
            ctx,
            "citibike",
            with_day_meta(union_by_name(frames)),
            lib.day_predicate(lo, hi),
        )
        results.append(("citibike", len(files), rows))
    else:
        results.append(("citibike", len(files), None))

    # rows the extractor could not place in a day (bad or far-out dates), kept for the quality report
    if lo.day == 1:  # monthly folder: handle it once, when the window starts on the 1st
        entries = [
            e
            for e in safe_ls(
                ctx.dbutils, f"{ctx.raw}/citibike/_rejected/month={month}/"
            )
            if e.name.endswith(".gz")
        ]
        if entries and not dry:
            by_stem = lib.group_by_stem([(None, e.path) for e in entries])
            frames = [read_csv(ctx.spark, paths) for paths in by_stem.values()]
            rows = write_window(
                ctx,
                "citibike_rejected",
                with_month_meta(union_by_name(frames), month),
                lib.month_predicate(month),
            )
            results.append(("citibike_rejected", len(entries), rows))
        else:
            results.append(("citibike_rejected", len(entries), None))
    return results


def ingest_permits(ctx, month, lo, hi, dry):
    days = days_in_window(ctx, "permits", lo, hi)
    if not days or dry:
        return [("permits", len(days), None)]
    paths = [f"{ctx.raw}/permits/day={d}/" for d in days]
    df = read_csv(ctx.spark, paths, multiline=True)
    rows = write_window(ctx, "permits", with_day_meta(df), lib.day_predicate(lo, hi))
    return [("permits", len(days), rows)]


def ingest_tlc(ctx, month, lo, hi, dry):
    """Monthly Parquet files. Run when the window starts on the 1st of the month."""
    if lo.day != 1:
        return []
    results = []
    for t in TLC_TYPES:
        entries = safe_ls(ctx.dbutils, f"{ctx.raw}/tlc/{t}/month={month}/")
        files = [e.path for e in entries if e.name.endswith(".parquet")]
        if not files:
            results.append((f"tlc_{t}", 0, None))  # not published for this month
            continue
        if dry:
            results.append((f"tlc_{t}", len(files), None))
            continue
        df = ctx.spark.read.parquet(*files)
        names = lib.clean_names(df.columns)
        cols = [
            F.col(f"`{old}`").cast("string").alias(new)
            for old, new in zip(df.columns, names)
        ]
        df = df.select(*cols, F.col("_metadata.file_path").alias("_source_file"))
        rows = write_window(
            ctx, f"tlc_{t}", with_month_meta(df, month), lib.month_predicate(month)
        )
        results.append((f"tlc_{t}", len(files), rows))
    return results


INGEST = {
    "311": ingest_311,
    "citibike": ingest_citibike,
    "permits": ingest_permits,
    "tlc": ingest_tlc,
}


# ----------------------------------------------------------------------------- main


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Bronze: raw files in S3 -> Delta tables")
    p.add_argument("--env", required=True, choices=["dev", "prod"])
    p.add_argument("--start", required=True, help="first day, YYYY-MM-DD")
    p.add_argument("--end", required=True, help="last day, YYYY-MM-DD")
    p.add_argument("--sources", default="311,citibike,tlc,permits")
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument(
        "--raw-prefix", default="raw", help="use raw/test to read the tiny test data"
    )
    p.add_argument(
        "--catalog", default=None, help="Unity Catalog name (default: same as --env)"
    )
    p.add_argument(
        "--no-register",
        action="store_true",
        help="write Delta files only, no catalog tables",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="list what would be read, write nothing"
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if start > end:
        raise ValueError(f"--start {start} is after --end {end}")
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in sources if s not in INGEST]
    if unknown:
        raise ValueError(
            f"unknown sources {unknown}; valid sources are {sorted(INGEST)}"
        )

    spark = SparkSession.builder.getOrCreate()
    ctx = Ctx(
        spark=spark,
        dbutils=get_dbutils(spark),
        env=args.env,
        catalog=args.catalog or args.env,
        bucket=args.bucket,
        raw_prefix=args.raw_prefix,
        register=not args.no_register,
    )
    print(
        f"bronze env={ctx.env} window={start}..{end} sources={sources} raw={ctx.raw} dry_run={args.dry_run}"
    )

    failed, log_failures = [], 0
    for month in lib.months_in_window(start, end):  # month by month, as planned
        lo, hi = lib.window_for_month(month, start, end)
        for source in sources:
            started = datetime.now(UTC)
            try:
                results = INGEST[source](ctx, month, lo, hi, args.dry_run)
                status, message = "OK", ""
            except Exception as e:  # noqa: BLE001 - record the failure, go on with the other sources
                results, status, message = [], "FAILED", str(e)[:500]
                failed.append(f"{source} {month}: {message}")
            if status == "FAILED":
                results = [(source, None, None)]
                print(f"  {month} {source}: FAILED {message}", flush=True)
            for key, files, rows in results:
                print(f"  {month} {key}: files={files} rows={rows}", flush=True)
                if args.dry_run:
                    continue
                row_status = "NO_FILES" if status == "OK" and rows is None else status
                ok = safe_log(
                    ctx,
                    {
                        "env": ctx.env,
                        "source": key,
                        "month": month,
                        "window_start": lo,
                        "window_end": hi,
                        "files": files,
                        "rows": rows,
                        "status": row_status,
                        "message": message,
                        "started_at": started,
                        "finished_at": datetime.now(UTC),
                    },
                )
                log_failures += 0 if ok else 1

    if failed:
        raise RuntimeError(f"{len(failed)} unit(s) failed: " + " | ".join(failed[:5]))
    print(
        "bronze finished"
        + (
            f" ({log_failures} run-log line(s) could not be written)"
            if log_failures
            else ""
        )
    )


if __name__ == "__main__":
    main(sys.argv[1:])
