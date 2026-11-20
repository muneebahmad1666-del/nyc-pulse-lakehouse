"""Tests for nyc_bronze_plan.py (plain Python, no Airflow, no Databricks).

Run:  pip install pytest && pytest -q test_nyc_bronze_plan.py
"""

from datetime import date

import pytest

import nyc_bronze_plan as plan


def test_all_makes_one_piece():
    assert plan.make_chunks(date(2019, 2, 1), date(2026, 10, 31), "all") == [(date(2019, 2, 1), date(2026, 10, 31))]


def test_year_chunks_end_on_31_december_and_leave_no_gaps():
    pieces = plan.make_chunks(date(2019, 2, 1), date(2026, 10, 31), "year")
    assert len(pieces) == 8
    assert pieces[0] == (date(2019, 2, 1), date(2019, 12, 31))
    assert pieces[1] == (date(2020, 1, 1), date(2020, 12, 31))
    assert pieces[-1] == (date(2026, 1, 1), date(2026, 10, 31))
    for (_, prev_end), (next_start, _) in zip(pieces, pieces[1:], strict=False):
        assert (next_start - prev_end).days == 1  # no day is skipped or doubled


def test_quarter_and_month_chunks():
    q = plan.make_chunks(date(2025, 11, 15), date(2026, 4, 10), "quarter")
    assert q == [(date(2025, 11, 15), date(2025, 12, 31)), (date(2026, 1, 1), date(2026, 3, 31)),
                 (date(2026, 4, 1), date(2026, 4, 10))]
    m = plan.make_chunks(date(2024, 2, 1), date(2024, 3, 31), "month")
    assert m == [(date(2024, 2, 1), date(2024, 2, 29)), (date(2024, 3, 1), date(2024, 3, 31))]
    assert len(plan.make_chunks(date(2019, 2, 1), date(2026, 10, 31), "month")) == 93


def test_a_single_day_is_one_chunk_in_every_mode():
    day = date(2026, 11, 5)
    for chunk in plan.CHUNKS:
        assert plan.make_chunks(day, day, chunk) == [(day, day)]


def test_bad_chunk_or_reversed_window_is_rejected():
    with pytest.raises(ValueError):
        plan.make_chunks(date(2025, 1, 1), date(2025, 2, 1), "week")
    with pytest.raises(ValueError):
        plan.make_chunks(date(2025, 2, 1), date(2025, 1, 1), "all")


def test_form_is_validated_and_defaulted():
    p = plan.validate_params({"env": "prod", "start": "2019-02-01", "end": "2026-10-31", "chunk": "year"})
    assert p["env"] == "prod" and p["start"] == date(2019, 2, 1)
    assert p["sources"] == list(plan.SOURCES) and p["raw_prefix"] == "raw"
    assert p["dry_run"] is True  # the safe default


@pytest.mark.parametrize(
    "bad",
    [
        {"env": "staging", "start": "2025-11-01", "end": "2025-11-30"},
        {"env": "dev", "start": "2025-11-1", "end": "2025-11-30"},
        {"env": "dev", "start": "2025-12-01", "end": "2025-11-30"},
        {"env": "dev", "start": "2025-11-01", "end": "2025-11-30", "sources": ["gdelt"]},
        {"env": "dev", "start": "2025-11-01", "end": "2025-11-30", "chunk": "decade"},
        {"env": "dev", "end": "2025-11-30"},
    ],
)
def test_bad_forms_are_rejected(bad):
    with pytest.raises(ValueError):
        plan.validate_params(bad)


def spec(**kw):
    base = dict(env="dev", start=date(2025, 11, 1), end=date(2025, 11, 30), sources=["311", "permits"],
                raw_prefix="raw", dry_run=False, bucket="b", git_url="https://github.com/x/y",
                git_branch="main", python_file="databricks/bronze/ingest/nyc_ingest.py")
    base.update(kw)
    return plan.build_run_spec(**base)


def test_run_spec_passes_exactly_the_job_arguments():
    task = spec()["tasks"][0]
    args = task["spark_python_task"]["parameters"]
    assert args[:6] == ["--env", "dev", "--start", "2025-11-01", "--end", "2025-11-30"]
    assert args[args.index("--sources") + 1] == "311,permits"
    assert args[args.index("--bucket") + 1] == "b" and "--dry-run" not in args
    assert task["spark_python_task"]["source"] == "GIT"


def test_dry_run_flag_and_name():
    s = spec(dry_run=True)
    assert "--dry-run" in s["tasks"][0]["spark_python_task"]["parameters"]
    assert s["run_name"].endswith("(dry run)")


def test_each_environment_gets_its_own_cluster_and_the_driver_is_not_spot():
    dev, prod = spec(env="dev")["tasks"][0]["new_cluster"], spec(env="prod")["tasks"][0]["new_cluster"]
    assert dev["num_workers"] == 1 and "autoscale" not in dev
    assert prod["autoscale"] == {"min_workers": 2, "max_workers": 4}
    for c in (dev, prod):
        assert c["aws_attributes"]["first_on_demand"] == 1
        assert c["spark_version"].startswith("15.4")


def test_the_shared_cluster_presets_are_not_changed_by_building_a_spec():
    spec(run_as_user="me@example.com")
    assert "single_user_name" not in plan.CLUSTERS["dev"]
    assert spec(run_as_user="me@example.com")["tasks"][0]["new_cluster"]["single_user_name"] == "me@example.com"