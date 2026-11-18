"""Helpers for the nyc_extract Airflow DAG. Plain Python, no Airflow imports, so it is easy to test.

Put this file in the ./src folder next to your docker-compose.yaml (it is on PYTHONPATH).
"""

import calendar
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

REGION = "us-east-1"
BUCKET = os.environ.get("BUCKET", "tb-scale-lakehouse-852824353987-us-east-1-an")
FUNCTION = "tb-scale-nyc-extract"
ALIAS = {"test": "dev", "raw": "prod"}  # only promoted code may write to raw/
PREFIX = {"raw": "raw", "test": "raw/test"}  # zone name -> S3 folder
FOLDER = {"311": "nyc311", "citibike": "citibike", "tlc": "tlc", "permits": "permits"}
SOURCES = list(FOLDER)
DAY_WORKERS = 5  # 311 days run at once inside one task; check your quota: aws lambda get-account-settings


# ----------------------------------------------------------------------------- dates


def months_between(first, last):
    """['2024-01', '2024-02', ...] from first to last, inclusive."""
    y, m = map(int, first.split("-"))
    ly, lm = map(int, last.split("-"))
    out = []
    while (y, m) <= (ly, lm):
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def days_of_month(month):
    """Every real day of the month: 28, 29, 30 or 31 of them, from the calendar."""
    y, m = map(int, month.split("-"))
    return [f"{month}-{d:02d}" for d in range(1, calendar.monthrange(y, m)[1] + 1)]


def validate_params(params):
    """Check the DAG's trigger form. Returns (zone, months, sources, simulate_from)."""
    zone = params.get("zone", "test")
    if zone not in ALIAS:
        raise ValueError(f"zone must be one of {sorted(ALIAS)}, got {zone!r}")
    first, last = params["first_month"], params["last_month"]
    for label, value in (("first_month", first), ("last_month", last)):
        try:
            datetime.strptime(value, "%Y-%m")
        except ValueError:
            raise ValueError(f"{label} must look like 2024-01, got {value!r}") from None
    if first > last:
        raise ValueError(f"first_month {first} is after last_month {last}")
    sources = params.get("sources") or SOURCES
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        raise ValueError(f"unknown sources {unknown}; valid sources are {SOURCES}")
    simulate_from = (params.get("simulate_from") or "").strip() or None
    if simulate_from:
        datetime.strptime(simulate_from, "%Y-%m")
    return zone, months_between(first, last), list(sources), simulate_from


# ----------------------------------------------------------------------------- Lambda


def lambda_client():
    import boto3
    from botocore.config import Config

    # Lambda's own limit is 900 s; give the client slightly longer so it never gives up first.
    return boto3.client(
        "lambda",
        region_name=REGION,
        config=Config(read_timeout=930, retries={"max_attempts": 0}),
    )


def invoke(zone, payload, client=None):
    """Call the extractor Lambda once. Raises if the function crashed or reported ok=False."""
    client = client or lambda_client()
    resp = client.invoke(
        FunctionName=f"{FUNCTION}:{ALIAS[zone]}",
        Payload=json.dumps({"zone": zone, **payload}),
    )
    out = json.loads(resp["Payload"].read())
    if "FunctionError" in resp:  # a Lambda that raises still answers HTTP 200
        raise RuntimeError(f"{payload}: function error: {out.get('errorMessage', out)}")
    if not out.get("ok"):
        raise RuntimeError(
            f"{payload}: extraction failed: {json.dumps(out.get('detail'))[:500]}"
        )
    return out


def short(out):
    """A small summary to store as the task result (not the whole Lambda answer)."""
    if out.get("skipped"):
        return {"unit": out["unit"], "already_done": True}
    d = out.get("detail") or {}
    if "rows" in d:  # 311 and Citi Bike
        return {
            "unit": out["unit"],
            "rows": d["rows"],
            "status": d.get("status", "done"),
        }
    return {
        "unit": out["unit"],
        "types": {t: v.get("status") for t, v in d.items()},
    }  # TLC


def run_311_month(zone, month, workers=DAY_WORKERS, call=None):
    """One Lambda call per day of the month, several at a time. Raises if any day failed."""
    call = call or (lambda day: invoke(zone, {"source": "311", "day": day}))
    days = days_of_month(month)
    failed, rows, done_before = [], 0, 0

    def one(day):
        try:
            return day, call(day), None
        except Exception as e:  # one bad day must not hide the others
            return day, None, str(e)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for day, out, err in pool.map(one, days):
            if err:
                failed.append(f"{day}: {err[:200]}")
            elif out.get("skipped"):
                done_before += 1
            else:
                rows += out["detail"]["rows"]
    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(days)} days failed in {month}: "
            + " | ".join(failed[:5])
        )
    return {
        "month": month,
        "days": len(days),
        "rows": rows,
        "already_done": done_before,
    }


# ----------------------------------------------------------------------------- check S3


def expected_units(source, months):
    if source == "311":
        return [d for m in months for d in days_of_month(m)]
    return list(months)


def found_units(zone, source, s3=None):
    """Units (a day or a month) that have a manifest in S3."""
    if s3 is None:
        import boto3

        s3 = boto3.client("s3", region_name=REGION)
    prefix = f"{PREFIX[zone]}/{FOLDER[source]}/_manifests/"
    found = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            m = re.search(r"(?:day|month)=([0-9-]+)\.json$", obj["Key"])
            if m:
                found.add(m.group(1))
    return found


def verify(zone, sources, months, s3=None):
    """{source: [units with no manifest]} for the chosen sources and months."""
    return {
        src: sorted(set(expected_units(src, months)) - found_units(zone, src, s3))
        for src in sources
    }
