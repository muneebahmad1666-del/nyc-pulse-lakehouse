"""Run the NYC Lambda for a range of months (from your computer).

Usage:  python invoke_nyc.py <raw|test> <311|citibike|tlc> 2024-01 [2024-03]

  311       one invocation per DAY of each month (28-31, from the calendar)
  citibike  one invocation per MONTH
  tlc       one invocation per MONTH (yellow, green, fhv and fhvhv together)

  test      tests the newest code (alias :dev) and writes to s3://BUCKET/raw/test/
  raw       runs the promoted code (alias :prod) and writes to s3://BUCKET/raw/.
            Promote first: bash deploy_nyc_lambda.sh promote

Months run in order, one after the other. Within a month, 311 days run in parallel.
Exit code is 1 if any unit failed. Re-running is safe: finished units are skipped.
"""

import calendar
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

FUNCTION = "tb-scale-nyc-extract"
ALIAS = {"test": "dev", "raw": "prod"}  # only promoted code may write to raw/
SOURCES = {"311", "citibike", "tlc"}
PARALLEL = {
    "311": 5,
    "citibike": 1,
    "tlc": 1,
}  # check your quota: aws lambda get-account-settings


def months_between(first, last):
    y, m = map(int, first.split("-"))
    ly, lm = map(int, last.split("-"))
    while (y, m) <= (ly, lm):
        yield f"{y}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def units(source, month):
    """The payload fields for each invocation of one month."""
    if source == "311":
        y, m = map(int, month.split("-"))
        return [
            {"day": f"{month}-{d:02d}"}
            for d in range(1, calendar.monthrange(y, m)[1] + 1)
        ]
    return [{"month": month}]


def short(out):
    if out.get("skipped"):
        return "already done"
    d = out.get("detail") or {}
    if "rows" in d:  # 311 and Citi Bike
        return f"rows={d['rows']}" + (f" status={d['status']}" if "status" in d else "")
    return {t: v.get("status") for t, v in d.items()}  # TLC


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    zone, source, first, *rest = sys.argv[1:]
    last = rest[0] if rest else first
    if zone not in ALIAS:
        sys.exit(f"zone must be one of {sorted(ALIAS)}, got {zone!r}")
    if source not in SOURCES:
        sys.exit(f"source must be one of {sorted(SOURCES)}, got {source!r}")

    # Lambda's own limit is 900 s; give the client slightly longer so it never gives up first.
    lam = boto3.client(
        "lambda",
        region_name="us-east-1",
        config=Config(read_timeout=930, retries={"max_attempts": 0}),
    )

    def run(unit):
        try:
            resp = lam.invoke(
                FunctionName=f"{FUNCTION}:{ALIAS[zone]}",
                Payload=json.dumps({"zone": zone, "source": source, **unit}),
            )
            out = json.loads(resp["Payload"].read())
            if "FunctionError" in resp:
                return unit, False, f"function error: {out.get('errorMessage', out)}"
            return unit, out["ok"], short(out)
        except Exception as e:  # one bad unit must not hide the others
            return unit, False, f"invoke error: {e}"

    failed, total = [], 0
    for month in months_between(first, last):
        todo = units(source, month)
        total += len(todo)
        with ThreadPoolExecutor(max_workers=PARALLEL[source]) as pool:
            for fut in as_completed([pool.submit(run, u) for u in todo]):
                unit, ok, info = fut.result()
                label = next(iter(unit.values()))
                print(label, "OK  " if ok else "FAIL", info, flush=True)
                if not ok:
                    failed.append(label)

    if failed:
        print(
            "\nRe-run to finish these (finished units are skipped):",
            " ".join(sorted(failed)),
        )
        sys.exit(1)
    print(
        f"\nAll {total} units extracted into {zone}/ for {source}, {first} to {last}."
    )


if __name__ == "__main__":
    main()
