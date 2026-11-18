"""
## nyc_extract: copy NYC data into S3 (the raw zone), month by month

Trigger it by hand (Trigger DAG w/ config) and fill in the form:

| field | meaning |
|---|---|
| zone | `test` writes to raw/test/ (tiny tests), `raw` writes the real data to raw/ |
| first_month, last_month | the months to extract, like 2024-01 |
| sources | which of tlc, citibike, 311, permits to run |
| simulate_from | permits only: months from here on get simulated daily changes first. Empty = none |

What runs:
* **tlc**, **citibike**: one Lambda call per month.
* **311**: one task per month; it calls the Lambda once for every real day of that month (28-31).
* **permits**: ONE task that goes through the months in order (the change simulation must run
  in date order). It reads Postgres, so it runs here in Airflow, not in Lambda.
* **verify_manifests**: at the end, checks that every expected "done" note exists in S3.

Everything can be re-run: finished units are skipped, so a failed run can simply be run again.
Files: put this in ./dags; nyc_extract_lib.py and extract_permits_month.py go in ./src.
"""

from datetime import datetime, timedelta

try:
    from airflow.sdk import Param, dag, task  # Airflow 3
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param

import nyc_extract_lib as lib


@dag(
    dag_id="nyc_extract",
    schedule=None,  # you start it yourself
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "zone": Param(
            "test",
            type="string",
            enum=["test", "raw"],
            description="test = raw/test/ (tiny tests), raw = the real data in raw/",
        ),
        "first_month": Param(
            "2024-01", type="string", description="First month, like 2019-04"
        ),
        "last_month": Param(
            "2024-01", type="string", description="Last month, like 2026-10"
        ),
        "sources": Param(
            lib.SOURCES,
            type="array",
            items={"type": "string", "enum": lib.SOURCES},
            description="Which sources to extract",
        ),
        "simulate_from": Param(
            None,
            type=["null", "string"],
            description="Permits: simulate daily changes from this month on (empty = never)",
        ),
    },
    doc_md=__doc__,
    tags=["nyc", "extract"],
)
def nyc_extract():
    @task
    def plan(params=None):
        """Checks the form and returns the list of months."""
        _, months, _, _ = lib.validate_params(params)
        return months

    @task(task_id="extract_tlc", max_active_tis_per_dagrun=1)
    def tlc(month, params=None):
        zone, _, sources, _ = lib.validate_params(params)
        if "tlc" not in sources:
            return {"unit": month, "not_selected": True}
        return lib.short(lib.invoke(zone, {"source": "tlc", "month": month}))

    @task(task_id="extract_citibike", max_active_tis_per_dagrun=1)
    def citibike(month, params=None):
        zone, _, sources, _ = lib.validate_params(params)
        if "citibike" not in sources:
            return {"unit": month, "not_selected": True}
        return lib.short(lib.invoke(zone, {"source": "citibike", "month": month}))

    @task(task_id="extract_311", max_active_tis_per_dagrun=1)
    def api_311(month, params=None):
        zone, _, sources, _ = lib.validate_params(params)
        if "311" not in sources:
            return {"month": month, "not_selected": True}
        return lib.run_311_month(zone, month)

    @task(task_id="extract_permits", execution_timeout=timedelta(hours=8))
    def permits(months, params=None):
        zone, _, sources, simulate_from = lib.validate_params(params)
        if "permits" not in sources:
            return {"not_selected": True}
        from extract_permits_month import extract_month

        results = [
            extract_month(zone, m, simulate_from=simulate_from) for m in months
        ]  # in order
        return {
            "months": len(results),
            "rows": sum(r.get("rows", 0) for r in results),
            "already_done": sum(1 for r in results if r["skipped"]),
        }

    @task(task_id="verify_manifests")
    def verify(params=None):
        zone, months, sources, _ = lib.validate_params(params)
        missing = lib.verify(zone, sources, months)
        summary = {
            s: {"expected": len(lib.expected_units(s, months)), "missing": len(m)}
            for s, m in missing.items()
        }
        bad = {s: m[:10] for s, m in missing.items() if m}
        if bad:
            raise RuntimeError(f"No 'done' note in S3 for: {bad}. Summary: {summary}")
        return summary

    months = plan()
    check = verify()
    for run in (
        tlc.expand(month=months),
        citibike.expand(month=months),
        api_311.expand(month=months),
        permits(months),
    ):
        run >> check


nyc_extract()
