"""Dry-run tests for nyc_ingest.py: the listing, the window and the month loop, without reading
any data. S3 and Spark are faked. Skipped when pyspark is not installed.

Run:  pip install pytest pyspark && pytest -q test_bronze_dry_run.py
"""

import contextlib
import io

import pytest

pytest.importorskip("pyspark")

import nyc_ingest as bi

B = "s3://test-bucket"


class Entry:
    def __init__(self, path):
        self.path = path
        self.name = path.rstrip("/").rsplit("/", 1)[-1] + (
            "/" if path.endswith("/") else ""
        )


class FakeFs:
    def __init__(self, tree, broken=()):
        self.tree, self.broken = tree, broken

    def ls(self, path):
        if any(b in path for b in self.broken):
            raise RuntimeError("Access denied")
        if path not in self.tree:
            raise RuntimeError("java.io.FileNotFoundException: " + path)
        return [Entry(p) for p in self.tree[path]]


class FakeDbutils:
    def __init__(self, tree, broken=()):
        self.fs = FakeFs(tree, broken)


class ExplodingSpark:
    """Any attempt to read or write data in a dry run is a bug."""

    @property
    def read(self):
        raise AssertionError("a dry run must not read data")

    def createDataFrame(self, *a, **k):
        raise AssertionError("a dry run must not write the run log")


def tree():
    t = {
        f"{B}/raw/nyc311/": [f"{B}/raw/nyc311/day=2025-11-0{d}/" for d in (1, 2, 3)]
        + [f"{B}/raw/nyc311/_manifests/"],
        f"{B}/raw/citibike/": [f"{B}/raw/citibike/day=2025-11-0{d}/" for d in (1, 2, 3)]
        + [f"{B}/raw/citibike/_rejected/"],
        f"{B}/raw/permits/": [
            f"{B}/raw/permits/day=2025-11-0{d}/" for d in (1, 3)
        ],  # no permits on the 2nd
        f"{B}/raw/citibike/_rejected/month=2025-11/": [
            f"{B}/raw/citibike/_rejected/month=2025-11/202511-citibike-tripdata.csv.gz"
        ],
        f"{B}/raw/tlc/yellow/month=2025-11/": [
            f"{B}/raw/tlc/yellow/month=2025-11/yellow_tripdata_2025-11.parquet"
        ],
        f"{B}/raw/tlc/fhvhv/month=2025-11/": [
            f"{B}/raw/tlc/fhvhv/month=2025-11/fhvhv_tripdata_2025-11.parquet"
        ],
    }
    for d in (1, 2, 3):
        t[f"{B}/raw/citibike/day=2025-11-0{d}/"] = [
            f"{B}/raw/citibike/day=2025-11-0{d}/202511-citibike-tripdata_1.csv.gz",
            f"{B}/raw/citibike/day=2025-11-0{d}/202511-citibike-tripdata_2.csv.gz",
        ]
    return t


def run(monkeypatch, argv, dbutils):
    class FakeBuilder:
        def getOrCreate(self):
            return ExplodingSpark()

    class FakeSparkSession:
        builder = FakeBuilder()

    monkeypatch.setattr(bi, "SparkSession", FakeSparkSession)
    monkeypatch.setattr(bi, "get_dbutils", lambda spark: dbutils)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        bi.main(["--bucket", "test-bucket", "--dry-run", *argv])
    return out.getvalue()


def test_dry_run_lists_files_per_source_and_never_touches_data(monkeypatch):
    text = run(
        monkeypatch,
        ["--env", "dev", "--start", "2025-11-01", "--end", "2025-11-03"],
        FakeDbutils(tree()),
    )
    assert "2025-11 311: files=3" in text
    assert "2025-11 citibike: files=6" in text  # 3 days x 2 parts
    assert "2025-11 citibike_rejected: files=1" in text
    assert "2025-11 permits: files=2" in text  # the 2nd has no folder, and that is fine
    assert (
        "tlc_yellow: files=1" in text and "tlc_green: files=0" in text
    )  # green not published


def test_a_window_that_starts_mid_month_skips_the_monthly_sources(monkeypatch):
    text = run(
        monkeypatch,
        ["--env", "dev", "--start", "2025-11-02", "--end", "2025-11-03"],
        FakeDbutils(tree()),
    )
    assert "2025-11 311: files=2" in text
    assert "tlc_" not in text and "citibike_rejected" not in text


def test_only_the_chosen_sources_run(monkeypatch):
    text = run(
        monkeypatch,
        [
            "--env",
            "dev",
            "--start",
            "2025-11-01",
            "--end",
            "2025-11-03",
            "--sources",
            "permits",
        ],
        FakeDbutils(tree()),
    )
    assert "permits" in text and "311" not in text and "citibike" not in text


def test_a_real_listing_error_fails_the_run_instead_of_being_swallowed(monkeypatch):
    with pytest.raises(RuntimeError, match="failed"):
        run(
            monkeypatch,
            [
                "--env",
                "dev",
                "--start",
                "2025-11-01",
                "--end",
                "2025-11-03",
                "--sources",
                "311",
            ],
            FakeDbutils(tree(), broken=("raw/nyc311",)),
        )


@pytest.mark.parametrize(
    "argv",
    [
        ["--env", "dev", "--start", "2025-11-05", "--end", "2025-11-01"],
        [
            "--env",
            "dev",
            "--start",
            "2025-11-01",
            "--end",
            "2025-11-03",
            "--sources",
            "gdelt",
        ],
    ],
)
def test_bad_arguments_are_rejected(monkeypatch, argv):
    with pytest.raises(ValueError):
        run(monkeypatch, argv, FakeDbutils(tree()))


class FakeFrame:
    def __init__(self, sink):
        self.sink = sink
        self.write = self

    def format(self, name):
        self.sink["format"] = name
        return self

    def mode(self, name):
        self.sink["mode"] = name
        return self

    def option(self, key, value):
        return self

    def save(self, path):
        self.sink["path"] = path


class RecordingSpark:
    def __init__(self, fail=False):
        self.sink, self.fail = {}, fail

    def createDataFrame(self, rows, schema=None):
        if self.fail:
            raise RuntimeError("boom")
        self.sink["rows"], self.sink["schema"] = rows, schema
        return FakeFrame(self.sink)


def make_ctx(spark):
    return bi.Ctx(
        spark=spark,
        dbutils=None,
        env="dev",
        catalog="dev",
        bucket="test-bucket",
        raw_prefix="raw",
        register=False,
    )


def log_record(**kw):
    base = {
        "env": "dev",
        "source": "tlc_fhv",
        "month": "2025-11",
        "window_start": "2025-11-01",
        "window_end": "2025-11-30",
        "files": 0,
        "rows": None,
        "status": "NO_FILES",
        "message": "",
        "started_at": None,
        "finished_at": None,
    }
    base.update(kw)
    return base


def test_run_log_is_written_with_an_explicit_schema_so_none_values_work():
    spark = RecordingSpark()
    bi.log_run(make_ctx(spark), log_record())
    assert (
        spark.sink["schema"] is bi.LOG_SCHEMA
    )  # Spark must not guess types from the None values
    assert spark.sink["path"] == "s3://test-bucket/dev/state/bronze_run_log"
    assert spark.sink["mode"] == "append" and spark.sink["format"] == "delta"


def test_a_run_log_problem_is_reported_but_does_not_fail_the_ingestion():
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        ok = bi.safe_log(make_ctx(RecordingSpark(fail=True)), log_record())
    assert ok is False and "could not write the run log" in out.getvalue()
