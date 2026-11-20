"""Planning helpers for the nyc_bronze Airflow DAG. Plain Python, no Airflow imports, easy to test.

Put this file in ./src next to your docker-compose.yaml (it is on PYTHONPATH).
"""

import calendar
from datetime import date, timedelta

ENVS = ("dev", "prod")
SOURCES = ("311", "citibike", "tlc", "permits")
CHUNKS = ("all", "year", "quarter", "month")

# The cluster each environment runs on. Edit here: this is where the AWS cost is decided.
#  - the driver (first node) is on demand, the workers are spot with a fallback to on demand
#  - a job cluster stops by itself when the run ends, so nothing is left running
CLUSTERS = {
    "dev": {
        "spark_version": "15.4.x-scala2.12",  # Databricks Runtime 15.4 LTS (Python 3.11+)
        "node_type_id": "m5d.xlarge",
        "num_workers": 1,
        "data_security_mode": "SINGLE_USER",
        "aws_attributes": {"availability": "SPOT_WITH_FALLBACK", "first_on_demand": 1, "zone_id": "auto"},
        "custom_tags": {"project": "nyc-pulse", "env": "dev"},
    },
    "prod": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "m5d.xlarge",
        "autoscale": {"min_workers": 2, "max_workers": 4},
        "data_security_mode": "SINGLE_USER",
        "aws_attributes": {"availability": "SPOT_WITH_FALLBACK", "first_on_demand": 1, "zone_id": "auto"},
        "custom_tags": {"project": "nyc-pulse", "env": "prod"},
    },
}
TIMEOUT_SECONDS = {"dev": 2 * 3600, "prod": 8 * 3600}


def make_chunks(start, end, chunk):
    """Cut the window [start, end] into pieces that end on calendar boundaries.

    chunk 'all' -> one piece; 'year', 'quarter', 'month' -> one piece per year, quarter or month.
    Each piece becomes one Databricks run (one cluster start).
    """
    if chunk not in CHUNKS:
        raise ValueError(f"chunk must be one of {CHUNKS}, got {chunk!r}")
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    if chunk == "all":
        return [(start, end)]
    pieces, cur = [], start
    while cur <= end:
        if chunk == "year":
            boundary = date(cur.year, 12, 31)
        elif chunk == "quarter":
            last_month = ((cur.month - 1) // 3 + 1) * 3
            boundary = date(cur.year, last_month, calendar.monthrange(cur.year, last_month)[1])
        else:
            boundary = date(cur.year, cur.month, calendar.monthrange(cur.year, cur.month)[1])
        stop = min(boundary, end)
        pieces.append((cur, stop))
        cur = stop + timedelta(days=1)
    return pieces


def validate_params(params):
    """Check the DAG's trigger form. Returns a clean dict."""
    env = params.get("env", "dev")
    if env not in ENVS:
        raise ValueError(f"env must be one of {ENVS}, got {env!r}")
    try:
        start, end = date.fromisoformat(params["start"]), date.fromisoformat(params["end"])
    except (KeyError, ValueError):
        raise ValueError("start and end must look like 2025-11-01") from None
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    sources = list(params.get("sources") or SOURCES)
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        raise ValueError(f"unknown sources {unknown}; valid sources are {list(SOURCES)}")
    chunk = params.get("chunk", "all")
    if chunk not in CHUNKS:
        raise ValueError(f"chunk must be one of {CHUNKS}, got {chunk!r}")
    return {
        "env": env, "start": start, "end": end, "sources": sources, "chunk": chunk,
        "raw_prefix": (params.get("raw_prefix") or "raw").strip("/"),
        "dry_run": bool(params.get("dry_run", True)),
    }


def build_run_spec(*, env, start, end, sources, raw_prefix, dry_run, bucket, git_url, git_branch,
                   python_file, run_as_user=None):
    """The body of a Databricks 'runs/submit' call: one bronze run for one window."""
    arguments = [
        "--env", env, "--start", start.isoformat(), "--end", end.isoformat(),
        "--sources", ",".join(sources), "--bucket", bucket, "--raw-prefix", raw_prefix,
    ]
    if dry_run:
        arguments.append("--dry-run")
    cluster = dict(CLUSTERS[env])
    if run_as_user:
        cluster["single_user_name"] = run_as_user
    return {
        "run_name": f"nyc-bronze {env} {start}..{end}" + (" (dry run)" if dry_run else ""),
        "timeout_seconds": TIMEOUT_SECONDS[env],
        "git_source": {"git_url": git_url, "git_provider": "gitHub", "git_branch": git_branch},
        "tasks": [
            {
                "task_key": "bronze",
                "spark_python_task": {"python_file": python_file, "source": "GIT", "parameters": arguments},
                "new_cluster": cluster,
            }
        ],
    }