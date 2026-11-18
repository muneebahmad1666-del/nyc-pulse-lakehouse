"""AWS Lambda: extract ONE unit of NYC data into S3 (the raw zone).

One invocation = one unit. Events:

  {"zone": "test", "source": "311",      "day":   "2024-01-05"}
  {"zone": "raw", "source": "citibike", "month": "2019-04"}
  {"zone": "raw", "source": "tlc",      "month": "2024-01", "types": ["yellow", "green", "fhv", "fhvhv"]}

Add "force": true to redo a unit whose manifest already exists.
Tiny Citi Bike test (zone "test" only): add "max_rows": 20000. It reads only the first rows of
one file straight from the server (a few MB, no big download) and writes NO manifest.
Env vars: BUCKET (required), SODA_APP_TOKEN (optional, avoids NYC Open Data throttling).

Zones:   raw   -> s3://BUCKET/raw/        the real data; only promoted code writes it.
         test  -> s3://BUCKET/raw/test/   scratch copy for tiny test runs. Safe to delete.
         In the paths below, <zone> means the folder of the zone you chose.

Output (everything is day-partitioned except the TLC Parquet files):
  311       s3://BUCKET/<zone>/nyc311/day=<D>/requests.jsonl.gz
            s3://BUCKET/<zone>/nyc311/_manifests/day=<D>.json
  citibike  s3://BUCKET/<zone>/citibike/day=<D>/<source-file-name>.csv.gz   (header kept in every file)
            s3://BUCKET/<zone>/citibike/_rejected/month=<M>/<file>.csv.gz   (unparsed / out-of-range dates)
            s3://BUCKET/<zone>/citibike/_manifests/month=<M>.json
  tlc       s3://BUCKET/<zone>/tlc/<type>/month=<M>/<type>_tripdata_<M>.parquet
            s3://BUCKET/<zone>/tlc/_manifests/month=<M>.json

The manifest is written LAST and only if nothing failed, so "manifest exists" means "this
unit is fully extracted". A source file that does not exist (for example FHVHV before 2019)
is recorded as `missing`, which is how ingestion tells a gap in the source from a failed
extraction. Re-running a unit is safe: finished work is skipped or overwritten with the same
keys, and S3 never shows a half-written object.

Citi Bike: 2013-2023 are YEARLY zips (we pick the month's files out of the year; in some years
each month is itself a zip inside the yearly zip, which we open too); 2024 on are monthly zips. The zip is downloaded to /tmp, so give the function 10 GB of ephemeral storage.
"""

import csv
import gzip
import http.client
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]

PREFIX = {"raw": "raw", "test": "raw/test"}  # zone name -> S3 folder
ZONES = set(PREFIX)
SOURCES = {"311", "citibike", "tlc"}
FOLDER = {"311": "nyc311", "citibike": "citibike", "tlc": "tlc"}

# NYC split the 311 dataset in December 2025: 2010-2019 and 2020-to-present are two datasets.
SODA_URL_2010_2019 = "https://data.cityofnewyork.us/resource/76ig-c548.json"
SODA_URL_2020_ON = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
SODA_PAGE = 50000
CITIBIKE_URL = "https://s3.amazonaws.com/tripdata/{name}"
CITIBIKE_YEARLY_UNTIL = 2023  # 2013..2023 are yearly zips, 2024+ monthly
START_COLUMNS = ("started_at", "starttime", "start_time")  # drifted over the years
WINDOW_DAYS = 31  # rows dated further than this from the month are rejected
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{t}_tripdata_{month}.parquet"
TLC_TYPES = ["yellow", "green", "fhv", "fhvhv"]
ATTEMPTS = 3


class NotFound(Exception):
    """The remote file does not exist."""


def object_size(key):
    """Size in bytes if the object exists, None if it does not. Other errors are raised.

    Needs s3:ListBucket on the bucket: without it S3 answers 403 (not 404) for a missing key.
    """
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def open_url(url, method="GET", headers=None, missing=(403, 404)):
    """Open a URL with retries. Raises NotFound for the status codes in `missing`."""
    req = urllib.request.Request(
        url, method=method, headers={"User-Agent": "nyc-lakehouse", **(headers or {})}
    )
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return urllib.request.urlopen(req, timeout=60)
        except urllib.error.HTTPError as e:
            if e.code in missing:
                raise NotFound(url) from e
            if attempt == ATTEMPTS:
                raise
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.HTTPException,
        ):
            if attempt == ATTEMPTS:
                raise
        time.sleep(2**attempt)


# ----------------------------------------------------------------------------- 311 API


def soda_url(day):
    """Which 311 dataset holds this day."""
    return SODA_URL_2010_2019 if day.year < 2020 else SODA_URL_2020_ON


def extract_311(zone, day_str):
    """One day of 311 requests, paged with $limit/$offset, written as one JSONL.gz file."""
    day = datetime.strptime(day_str, "%Y-%m-%d").date()
    nxt = day + timedelta(days=1)
    where = f"created_date >= '{day}T00:00:00' AND created_date < '{nxt}T00:00:00'"
    headers = {}
    if os.environ.get("SODA_APP_TOKEN"):
        headers["X-App-Token"] = os.environ["SODA_APP_TOKEN"]

    rows, pages, offset = [], 0, 0
    while True:
        query = urllib.parse.urlencode(
            {"$where": where, "$order": ":id", "$limit": SODA_PAGE, "$offset": offset}
        )
        # 403 here means a bad app token or throttling, so only 404 counts as "missing"
        with open_url(
            f"{soda_url(day)}?{query}", headers=headers, missing=(404,)
        ) as resp:
            page = json.loads(resp.read())
        pages += 1
        rows.extend(page)
        if len(page) < SODA_PAGE:
            break
        offset += SODA_PAGE

    detail = {"rows": len(rows), "pages": pages, "key": None, "bytes": 0}
    if not rows:
        # 311 gets thousands of requests every day, so an empty day means a wrong dataset or an
        # API problem. Fail loudly: an empty "done" note would hide missing data.
        detail["error"] = f"0 rows for {day_str} from {soda_url(day)}"
        return detail, [day_str]
    key = f"{PREFIX[zone]}/nyc311/day={day_str}/requests.jsonl.gz"
    body = gzip.compress(
        "\n".join(json.dumps(r) for r in rows).encode(), compresslevel=3
    )
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    detail.update(key=key, bytes=len(body))
    return detail, []


# ----------------------------------------------------------------------------- Citi Bike


def to_day(text):
    """Calendar date of a start-time string, or None. Handles both formats seen in the files:
    '2019-04-01 00:00:03.3830' and '4/1/2019 00:00:03'."""
    s = (text or "").strip()
    try:
        if len(s) >= 10 and s[4] == "-":
            return date.fromisoformat(s[:10])
        if "/" in s:
            m, d, rest = s.split("/", 2)
            y = int(rest.split(" ")[0])
            return date(y + 2000 if y < 100 else y, int(m), int(d))
    except ValueError:
        return None
    return None


def citibike_zip_name(month):
    if int(month[:4]) <= CITIBIKE_YEARLY_UNTIL:
        return f"{month[:4]}-citibike-tripdata.zip"
    return f"{month.replace('-', '')}-citibike-tripdata.zip"


def usable(zf, endings):
    """Files in a zip with one of the given endings. Skips folders, .DS_Store and __MACOSX."""
    return [
        i
        for i in zf.infolist()
        if not i.is_dir()
        and i.filename.lower().endswith(endings)
        and "__MACOSX" not in i.filename
        and not os.path.basename(i.filename).startswith(".")
    ]


def pick_members(zf, month):
    """The CSV files, or zips of CSV files, that belong to `month`.

    Some yearly zips hold one CSV per month (2019), others hold one ZIP per month (2020).
    """
    found = usable(zf, (".csv", ".zip"))
    if int(month[:4]) > CITIBIKE_YEARLY_UNTIL:
        return found  # a monthly zip holds only this month
    y, m = month.split("-")
    tokens = (f"{y}{m}", f"{y}-{m}", f"{y}_{m}")  # file naming differs between years
    return [i for i in found if any(t in os.path.basename(i.filename) for t in tokens)]


def process_member(zf, member, month, workdir, max_rows=None):
    """Split one CSV into per-day gz files in workdir; returns (stats, [(path, key_suffix)])."""
    y, m = map(int, month.split("-"))
    first = date(y, m, 1)
    last = (first + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    lo, hi = first - timedelta(days=WINDOW_DAYS), last + timedelta(days=WINDOW_DAYS)
    stem = re.sub(
        r"[^A-Za-z0-9_.-]+", "_", os.path.splitext(os.path.basename(member.filename))[0]
    )

    stats = {
        "file": member.filename,
        "rows": 0,
        "unparsed": 0,
        "out_of_range": 0,
        "days": 0,
    }
    files = []  # (local path, S3 key suffix)
    with ExitStack() as stack:
        reader = csv.reader(
            stack.enter_context(
                io.TextIOWrapper(zf.open(member), encoding="utf-8-sig", newline="")
            )
        )
        header = next(reader, None)
        if not header:
            raise ValueError(f"{member.filename} is empty")
        lowered = [h.strip().lower() for h in header]
        idx = next((lowered.index(c) for c in START_COLUMNS if c in lowered), None)
        if idx is None:
            raise ValueError(f"{member.filename}: no start-time column in {header}")

        writers = {}  # day string -> csv writer

        def writer_for(label, path, head):
            fh = stack.enter_context(gzip.open(path, "wt", newline="", compresslevel=3))
            w = csv.writer(fh)
            w.writerow(head)
            writers[label] = w
            return w

        for row in reader:
            if max_rows and stats["rows"] >= max_rows:
                stats["stopped_early"] = (
                    True  # test mode: only the first rows were read
                )
                break
            stats["rows"] += 1
            d = to_day(row[idx]) if len(row) > idx else None
            if d is None:
                reason = "unparsed"
            elif not lo <= d <= hi:
                reason = "out_of_range"
            else:
                reason = None
            if reason:
                stats[reason] += 1
                w = writers.get("_rejected") or writer_for(
                    "_rejected",
                    os.path.join(workdir, f"{stem}.rejected.csv.gz"),
                    header + ["_reject_reason"],
                )
                w.writerow(row + [reason])
                continue
            label = d.isoformat()
            w = writers.get(label) or writer_for(
                label, os.path.join(workdir, f"{stem}.{label}.csv.gz"), header
            )
            w.writerow(row)

    for label in writers:
        if label == "_rejected":
            files.append(
                (
                    os.path.join(workdir, f"{stem}.rejected.csv.gz"),
                    f"_rejected/month={month}/{stem}.csv.gz",
                )
            )
        else:
            files.append(
                (
                    os.path.join(workdir, f"{stem}.{label}.csv.gz"),
                    f"day={label}/{stem}.csv.gz",
                )
            )
            stats["days"] += 1
    return stats, files


class RangeFile(io.RawIOBase):
    """A remote file that zipfile can open WITHOUT downloading all of it.

    It asks the server for 1 MB pieces (HTTP Range requests) only when zipfile reads them.
    Used by the tiny test mode: the zip's table of contents is at the end of the file, and
    the first rows of a CSV are near the start, so a few MB are enough.
    """

    BLOCK = 1024 * 1024
    KEEP = 8  # pieces kept in memory

    def __init__(self, url):
        with open_url(url, method="HEAD") as head:
            self.size = int(head.headers["Content-Length"])
        self.url, self.pos, self.blocks = url, 0, {}

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=0):
        base = {0: 0, 1: self.pos, 2: self.size}[whence]
        self.pos = base + offset
        return self.pos

    def _block(self, i):
        if i not in self.blocks:
            start = i * self.BLOCK
            end = min(start + self.BLOCK, self.size) - 1
            with open_url(self.url, headers={"Range": f"bytes={start}-{end}"}) as resp:
                self.blocks[i] = resp.read()
            while len(self.blocks) > self.KEEP:
                self.blocks.pop(next(iter(self.blocks)))  # forget the oldest piece
        return self.blocks[i]

    def read(self, n=-1):
        end = self.size if n is None or n < 0 else min(self.pos + n, self.size)
        out = bytearray()
        while self.pos < end:
            i, offset = divmod(self.pos, self.BLOCK)
            chunk = self._block(i)[offset : offset + (end - self.pos)]
            if not chunk:
                break
            out += chunk
            self.pos += len(chunk)
        return bytes(out)


def download_zip(url, path):
    """Download the whole zip to a file, retrying on network trouble. Raises NotFound."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            with open_url(url) as resp, open(path, "wb") as out:
                shutil.copyfileobj(resp, out, 8 * 1024 * 1024)
            return
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.HTTPException,
        ):
            if attempt == ATTEMPTS:
                raise
            time.sleep(2**attempt)


def extract_citibike(zone, month, max_rows=None):
    """One month of Citi Bike rides. With max_rows (test mode) only the first rows of the first
    file are read, straight from the server, and nothing big is downloaded."""
    workdir = tempfile.mkdtemp(dir="/tmp")
    detail = {"zip": citibike_zip_name(month), "files": [], "rows": 0, "bytes": 0}
    failed = []
    try:
        url = CITIBIKE_URL.format(name=detail["zip"])
        try:
            if max_rows:
                source = RangeFile(url)
                detail["partial"] = True
            else:
                source = os.path.join(workdir, "source.zip")
                download_zip(url, source)
        except NotFound:
            detail["status"] = "missing"  # that month is not published (yet)
            return detail, []

        with zipfile.ZipFile(source) as zf:
            members = pick_members(zf, month)
            if not members:
                sample = [i.filename for i in zf.infolist()][:10]
                raise ValueError(
                    f"no CSV files for {month} in {detail['zip']}; first entries: {sample}"
                )
            if max_rows:
                if any(m.filename.lower().endswith(".zip") for m in members):
                    raise ValueError(
                        "a tiny test cannot read zips inside zips; try a month from 2019 or from 2024 on"
                    )
                members = members[:1]  # a test needs one file only

            def store(stats, files):
                for path, suffix in files:
                    s3.upload_file(path, BUCKET, f"{PREFIX[zone]}/citibike/{suffix}")
                    detail["bytes"] += os.path.getsize(path)
                    os.remove(path)
                detail["files"].append(stats)
                detail["rows"] += stats["rows"]

            for member in members:
                try:
                    if member.filename.lower().endswith(".zip"):  # a zip inside the zip
                        inner_path = os.path.join(workdir, "inner.zip")
                        with zf.open(member) as src, open(inner_path, "wb") as dst:
                            shutil.copyfileobj(src, dst, 8 * 1024 * 1024)
                        with zipfile.ZipFile(inner_path) as inner:
                            inner_csvs = usable(inner, (".csv",))
                            if not inner_csvs:
                                raise ValueError(
                                    f"{member.filename} holds no CSV files"
                                )
                            for csv_member in inner_csvs:
                                store(
                                    *process_member(inner, csv_member, month, workdir)
                                )
                        os.remove(inner_path)
                    else:
                        store(*process_member(zf, member, month, workdir, max_rows))
                except Exception as e:  # noqa: BLE001 - mark this file failed, go on
                    log.error("failed %s: %s", member.filename, e)
                    failed.append(member.filename)
                    detail["files"].append({"file": member.filename, "error": str(e)})
        detail["status"] = "failed" if failed else "done"
        return detail, failed
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ----------------------------------------------------------------------------- TLC Parquet


def extract_tlc(zone, month, types):
    """Stream each monthly Parquet file straight into S3 (nothing is held in memory)."""
    detail, failed = {}, []
    for t in types:
        key = f"{PREFIX[zone]}/tlc/{t}/month={month}/{t}_tripdata_{month}.parquet"
        url = TLC_URL.format(t=t, month=month)
        try:
            with open_url(url, method="HEAD") as head:
                size = int(head.headers["Content-Length"])
            if object_size(key) == size:
                detail[t] = {"status": "skipped", "bytes": size}
                continue
            with open_url(url) as resp:
                s3.upload_fileobj(resp, BUCKET, key)
            got = object_size(key)
            if got != size:
                raise ValueError(f"size mismatch: S3 has {got}, source has {size}")
            detail[t] = {"status": "done", "bytes": size}
        except NotFound:
            detail[t] = {"status": "missing"}  # not published for this month
        except Exception as e:  # noqa: BLE001 - mark this type failed, go on
            log.error("failed %s %s: %s", t, month, e)
            detail[t] = {"status": "failed", "error": str(e)}
            failed.append(t)
    return detail, failed


# ----------------------------------------------------------------------------- handler


def handler(event, context):
    zone, source = event["zone"], event["source"]
    if zone not in ZONES:
        raise ValueError(f"zone must be one of {sorted(ZONES)}, got {zone!r}")
    if source not in SOURCES:
        raise ValueError(f"source must be one of {sorted(SOURCES)}, got {source!r}")

    if source == "311":
        unit, label = event["day"], "day"
        datetime.strptime(unit, "%Y-%m-%d")
    else:
        unit, label = event["month"], "month"
        datetime.strptime(unit, "%Y-%m")
    max_rows = event.get("max_rows")
    if max_rows is not None and not (
        source == "citibike"
        and zone == "test"
        and isinstance(max_rows, int)
        and max_rows > 0
    ):
        raise ValueError(
            "max_rows must be a positive whole number, and only works for source "
            "'citibike' in zone 'test'"
        )
    types = event.get("types", TLC_TYPES)
    unknown = [t for t in types if t not in TLC_TYPES]
    if source == "tlc" and unknown:
        raise ValueError(f"unknown TLC types {unknown}; valid types are {TLC_TYPES}")

    manifest_key = f"{PREFIX[zone]}/{FOLDER[source]}/_manifests/{label}={unit}.json"
    if (
        max_rows is None
        and not event.get("force")
        and object_size(manifest_key) is not None
    ):
        return {
            "zone": zone,
            "source": source,
            "unit": unit,
            "ok": True,
            "skipped": True,
            "manifest": manifest_key,
            "detail": None,
        }

    if source == "311":
        detail, failed = extract_311(zone, unit)
    elif source == "citibike":
        detail, failed = extract_citibike(zone, unit, max_rows)
    else:
        detail, failed = extract_tlc(zone, unit, types)

    ok = not failed
    if (
        ok and max_rows is None
    ):  # a partial test run must never look like a finished month
        manifest = {
            "source": source,
            "zone": zone,
            label: unit,
            "detail": detail,
            "written_at": datetime.now(UTC).isoformat(),
        }
        s3.put_object(
            Bucket=BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest).encode(),
            ContentType="application/json",
        )
    log.info("%s %s zone %s ok=%s", source, unit, zone, ok)
    return {
        "zone": zone,
        "source": source,
        "unit": unit,
        "ok": ok,
        "skipped": False,
        "manifest": manifest_key if ok and max_rows is None else None,
        "detail": detail,
    }
