"""Tests for nyc_lib.py (plain Python, no Spark).

Run:  pip install pytest && pytest -q test_nyc_lib.py
"""

from datetime import date

import pytest

import nyc_lib as lib


def test_column_names_become_delta_safe_without_changing_meaning():
    assert lib.clean_name("start station id") == "start_station_id"
    assert lib.clean_name("Trip Duration") == "trip_duration"
    assert lib.clean_name("PULocationID") == "pulocationid"
    assert lib.clean_name("birth year") == "birth_year"
    assert lib.clean_name("  (x) ") == "x"
    assert lib.clean_name("###") == "col"


def test_clean_names_makes_duplicates_unique():
    assert lib.clean_names(["id", "ID", "Id ", "name"]) == ["id", "id_2", "id_3", "name"]
    # Airport_fee and airport_fee in two TLC years must not collide inside one file
    assert lib.clean_names(["Airport_fee", "airport_fee"]) == ["airport_fee", "airport_fee_2"]


def test_months_in_window_covers_every_touched_month():
    assert lib.months_in_window(date(2025, 11, 15), date(2026, 1, 2)) == ["2025-11", "2025-12", "2026-01"]
    assert lib.months_in_window(date(2025, 11, 5), date(2025, 11, 5)) == ["2025-11"]
    assert len(lib.months_in_window(date(2019, 2, 1), date(2026, 10, 31))) == 93


@pytest.mark.parametrize(
    "month, first, last",
    [("2024-02", date(2024, 2, 1), date(2024, 2, 29)), ("2023-02", date(2023, 2, 1), date(2023, 2, 28)),
     ("2025-12", date(2025, 12, 1), date(2025, 12, 31))],
)
def test_month_bounds_use_the_calendar(month, first, last):
    assert lib.month_bounds(month) == (first, last)


def test_window_for_month_clips_to_the_window():
    start, end = date(2025, 11, 10), date(2025, 12, 5)
    assert lib.window_for_month("2025-11", start, end) == (date(2025, 11, 10), date(2025, 11, 30))
    assert lib.window_for_month("2025-12", start, end) == (date(2025, 12, 1), date(2025, 12, 5))


def test_day_folders_are_recognised_and_other_folders_ignored():
    assert lib.parse_day_dir("day=2025-11-05/") == date(2025, 11, 5)
    assert lib.parse_day_dir("day=2025-11-05") == date(2025, 11, 5)
    assert lib.parse_day_dir("_manifests/") is None
    assert lib.parse_day_dir("_rejected/") is None


def test_an_impossible_day_folder_name_raises_so_it_is_never_ingested_silently():
    with pytest.raises(ValueError):
        lib.parse_day_dir("day=2025-13-45/")


def test_files_group_by_source_csv_so_each_group_has_one_header():
    files = [
        (date(2020, 10, 1), "s3://b/raw/citibike/day=2020-10-01/202010-citibike-tripdata_1.csv.gz"),
        (date(2020, 10, 1), "s3://b/raw/citibike/day=2020-10-01/202010-citibike-tripdata_2.csv.gz"),
        (date(2020, 10, 2), "s3://b/raw/citibike/day=2020-10-02/202010-citibike-tripdata_1.csv.gz"),
        (date(2020, 11, 1), "s3://b/raw/citibike/day=2020-11-01/202010-citibike-tripdata_1.csv.gz"),
    ]
    groups = lib.group_by_stem(files)
    assert sorted(groups) == ["202010-citibike-tripdata_1", "202010-citibike-tripdata_2"]
    assert len(groups["202010-citibike-tripdata_1"]) == 3


def test_stem_handles_the_endings_the_extractor_writes():
    assert lib.stem_of("s3://b/x/201904-citibike-tripdata_1.csv.gz") == "201904-citibike-tripdata_1"
    assert lib.stem_of("202401-citibike-tripdata.csv.gz") == "202401-citibike-tripdata"
    assert lib.stem_of("plain.gz") == "plain"


def test_predicates_target_exactly_the_window():
    assert lib.day_predicate(date(2025, 11, 1), date(2025, 11, 30)) == (
        "_day >= DATE'2025-11-01' AND _day <= DATE'2025-11-30'"
    )
    assert lib.month_predicate("2025-11") == "_month = '2025-11'"