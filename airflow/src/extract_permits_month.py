"""Extract one month of the permits table from Postgres into S3, one file per day.

Runs on YOUR computer (WSL), because Postgres is on your machine and a Lambda cannot reach it.
It writes the same raw/ layout as the Lambda, including a manifest written last.

Usage (from the folder that holds your .env with NYC_PG_PASSWORD):
  python extract_permits_month.py test 2019-04
  python extract_permits_month.py raw 2024-02 --simulate-from 2024-01
  python extract_permits_month.py raw 2019-04 --force

  --simulate-from YYYY-MM  for months on/after this, first simulate each day's changes
                           (500 updates + 20 soft deletes, skipping days already simulated)
  --force                  redo a month whose manifest already exists

Output (<zone> is raw/ or raw/test/):  s3://BUCKET/<zone>/permits/day=<D>/permits.csv.gz     (header row included)
         s3://BUCKET/<zone>/permits/_manifests/month=<M>.json

A row lands in the day of its updated_at. History rows carry their issue date as updated_at,
so one query by updated_at returns both the permits issued that day and the changes made
that day. Run months in ORDER and do not re-extract an old month after simulating later ones:
the table's state at extraction time decides what the month's files contain.
Needs: pip install psycopg2-binary boto3 ; AWS credentials from `aws configure`.

Settings come from environment variables, so the same file works on your computer and inside
the Airflow containers:  PG_HOST (default localhost; use nycpg inside Docker Compose),
NYC_PG_PASSWORD (falls back to the .env file in the current folder), BUCKET.
Airflow calls extract_month() directly (see nyc_extract_dag.py).
"""

import calendar
import gzip
import io
import json
import os
import sys
from datetime import UTC, date, datetime

import boto3
import psycopg2

BUCKET = os.environ.get("BUCKET", "tb-scale-lakehouse-852824353987-us-east-1-an")
PREFIX = {"raw": "raw", "test": "raw/test"}  # zone name -> S3 folder
ZONES = set(PREFIX)


def read_env(path=".env"):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def month_days(month):
    y, m = map(int, month.split("-"))
    return [date(y, m, d) for d in range(1, calendar.monthrange(y, m)[1] + 1)]


def object_exists(s3, key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def simulate_day(cur, day, n_updates=500, n_deletes=20):
    """Random status changes and soft deletes dated `day`. Skips a day already simulated."""
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM raw_permits WHERE updated_at::date = %s AND issued_on <> %s)",
        (day, day),
    )
    if cur.fetchone()[0]:
        return {"day": str(day), "skipped": True}
    cur.execute(
        """WITH pick AS (SELECT id FROM raw_permits
                         WHERE issued_on < %s AND deleted_at IS NULL ORDER BY random() LIMIT %s)
           UPDATE raw_permits r
              SET permit_status = (ARRAY['ISSUED','IN PROCESS','ISSUED','RE-ISSUED'])[1 + floor(random()*4)::int],
                  updated_at = %s::timestamptz
             FROM pick WHERE r.id = pick.id""",
        (day, n_updates, day),
    )
    updated = cur.rowcount
    cur.execute(
        """WITH pick AS (SELECT id FROM raw_permits
                         WHERE issued_on < %s AND deleted_at IS NULL ORDER BY random() LIMIT %s)
           UPDATE raw_permits r SET deleted_at = %s::timestamptz, updated_at = %s::timestamptz
             FROM pick WHERE r.id = pick.id""",
        (day, n_deletes, day, day),
    )
    return {"day": str(day), "updated": updated, "deleted": cur.rowcount}


def pg_password():
    return os.environ.get("NYC_PG_PASSWORD") or read_env()["NYC_PG_PASSWORD"]


def extract_month(zone, month, simulate_from=None, force=False, log=print):
    """Extract one month into S3 and return a small summary dict."""
    if zone not in ZONES:
        raise ValueError(f"zone must be one of {sorted(ZONES)}, got {zone!r}")
    datetime.strptime(month, "%Y-%m")

    s3 = boto3.client("s3", region_name="us-east-1")
    manifest_key = f"{PREFIX[zone]}/permits/_manifests/month={month}.json"
    if not force and object_exists(s3, manifest_key):
        log(f"{month}: manifest exists, nothing to do (use --force to redo)")
        return {"month": month, "skipped": True}

    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=5432,
        dbname="nyc",
        user="postgres",
        password=pg_password(),
        options="-c timezone=UTC",
    )
    days = month_days(month)
    simulated, per_day, total = [], {}, 0
    try:
        with (
            conn
        ):  # one transaction: if anything fails, the simulated changes roll back too
            with conn.cursor() as cur:
                if simulate_from and month >= simulate_from:
                    for d in (
                        days
                    ):  # in order: a later day's change must come after an earlier one
                        simulated.append(simulate_day(cur, d))

                for d in days:
                    key = f"{PREFIX[zone]}/permits/day={d}/permits.csv.gz"
                    where = "updated_at >= %s::date AND updated_at < %s::date + 1"
                    cur.execute(
                        f"SELECT count(*) FROM raw_permits WHERE {where}", (d, d)
                    )
                    n = cur.fetchone()[0]
                    per_day[str(d)] = n
                    if n == 0:
                        s3.delete_object(
                            Bucket=BUCKET, Key=key
                        )  # no stale file from an earlier run
                        continue
                    buf = io.BytesIO()
                    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=3) as gz:
                        cur.copy_expert(
                            cur.mogrify(
                                f"COPY (SELECT * FROM raw_permits WHERE {where} ORDER BY id) "
                                "TO STDOUT WITH (FORMAT csv, HEADER true)",
                                (d, d),
                            ).decode(),
                            gz,
                        )
                    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
                    total += n
                    log(f"  {d}: {n} rows")
    finally:
        conn.close()

    manifest = {
        "source": "permits",
        "zone": zone,
        "month": month,
        "rows": total,
        "rows_per_day": per_day,
        "simulated": simulated,
        "written_at": datetime.now(UTC).isoformat(),
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    day_files = sum(1 for v in per_day.values() if v)
    log(f"{month}: {total} rows in {day_files} day files -> {PREFIX[zone]}/permits/")
    return {"month": month, "skipped": False, "rows": total, "day_files": day_files}


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        sys.exit(__doc__)
    simulate_from = (
        args[args.index("--simulate-from") + 1] if "--simulate-from" in args else None
    )
    try:
        extract_month(
            args[0], args[1], simulate_from=simulate_from, force="--force" in args
        )
    except ValueError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
