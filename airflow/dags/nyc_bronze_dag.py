"""
## nyc_bronze: raw files in S3 -> Delta bronze tables (runs the bronze job on Databricks)

Trigger it by hand (Trigger DAG w/ config) and fill in the form:

| field | meaning |
|---|---|
| env | `dev` (small test) or `prod` (the full data). Picks the cluster, the tables folder and the catalog |
| start, end | first and last DAY to ingest, like 2025-11-01 |
| sources | which of 311, citibike, tlc, permits |
| chunk | how the window is cut into Databricks runs: `all` = one run, `year`, `quarter`, `month` |
| raw_prefix | `raw` (the real data) or `raw/test` (the tiny extraction tests) |
| dry_run | `true` only lists what would be read. Switch it off for the real run |

How it works:
1. `plan` checks the form and cuts the window into chunks (for example one per year).
2. `run_bronze` starts ONE Databricks run per chunk, one after the other. Each run starts a job
   cluster, runs nyc_ingest.py for that window, and the cluster stops when the run ends.
3. Inside a run the bronze job goes month by month and source by source, replacing exactly the
   rows of that window. Re-running a chunk is safe.

Needs: an Airflow connection `databricks_default` (host = your workspace URL, password = a
Databricks personal access token), and in the compose .env: BUCKET, and optionally
NYC_GIT_URL, NYC_GIT_BRANCH, NYC_BRONZE_PATH, DATABRICKS_RUN_AS_USER.
Files: this goes in ./dags; nyc_bronze_plan.py goes in ./src.
"""

import os
from datetime import datetime

try:
    from airflow.sdk import Param, dag, task  # Airflow 3
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

import nyc_bronze_plan as plan_lib

BUCKET = os.environ.get("BUCKET", "tb-scale-lakehouse-852824353987-us-east-1-an")
GIT_URL = os.environ.get("NYC_GIT_URL", "https://github.com/muneebahmad1666-del/tb-scale-lakehouse")
GIT_BRANCH = os.environ.get("NYC_GIT_BRANCH", "main")
PYTHON_FILE = os.environ.get("NYC_BRONZE_PATH", "databricks/bronze/ingest/nyc_ingest.py")  # path inside the repo
RUN_AS_USER = os.environ.get("DATABRICKS_RUN_AS_USER")  # your Databricks login email, if the cluster asks for it


@dag(
    dag_id="nyc_bronze",
    schedule=None,  # you start it yourself
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "env": Param("dev", type="string", enum=["dev", "prod"], description="dev = small test, prod = full data"),
        "start": Param("2025-11-01", type="string", description="First day, like 2025-11-01"),
        "end": Param("2025-11-30", type="string", description="Last day, like 2025-11-30"),
        "sources": Param(list(plan_lib.SOURCES), type="array",
                         items={"type": "string", "enum": list(plan_lib.SOURCES)}, description="Sources to ingest"),
        "chunk": Param("all", type="string", enum=list(plan_lib.CHUNKS),
                       description="How to cut the window into Databricks runs (one cluster start each)"),
        "raw_prefix": Param("raw", type="string", description="raw = real data, raw/test = tiny test data"),
        "dry_run": Param(True, type="boolean", description="true = only list what would be read"),
    },
    doc_md=__doc__,
    tags=["nyc", "bronze", "databricks"],
)
def nyc_bronze():
    @task
    def plan(params=None):
        """Checks the form and returns one run specification per chunk."""
        p = plan_lib.validate_params(params)
        specs = []
        for lo, hi in plan_lib.make_chunks(p["start"], p["end"], p["chunk"]):
            specs.append(
                plan_lib.build_run_spec(
                    env=p["env"], start=lo, end=hi, sources=p["sources"], raw_prefix=p["raw_prefix"],
                    dry_run=p["dry_run"], bucket=BUCKET, git_url=GIT_URL, git_branch=GIT_BRANCH,
                    python_file=PYTHON_FILE, run_as_user=RUN_AS_USER,
                )
            )
        return specs

    DatabricksSubmitRunOperator.partial(
        task_id="run_bronze",
        databricks_conn_id="databricks_default",
        databricks_retry_limit=20,
        databricks_retry_delay=30,
        polling_period_seconds=60,
        max_active_tis_per_dagrun=1,  # one chunk at a time: one cluster running, one bill
        retries=0,  # a failed chunk is fixed first, then triggered again (re-running is safe)
    ).expand(json=plan())


nyc_bronze()