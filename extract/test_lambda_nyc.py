"""Tests for lambda_nyc.py. No AWS and no network: S3 and the NYC servers are faked.

Run:  pip install boto3 pytest && pytest -q test_lambda_nyc.py
"""

import gzip
import io
import json
import os
import urllib.error
import urllib.parse
import zipfile

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault("BUCKET", "test-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import lambda_nyc as ln  # noqa: E402


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return {"ContentLength": len(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

    def upload_fileobj(self, Fileobj, Bucket, Key):
        self.objects[Key] = Fileobj.read()

    def upload_file(self, Filename, Bucket, Key):
        with open(Filename, "rb") as f:
            self.objects[Key] = f.read()


class Resp(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers or {}


class FakeWeb:
    """Stands in for urllib.request.urlopen for SODA (311), the Citi Bike bucket and TLC."""

    def __init__(self):
        self.soda = {}  # "2024-01-05" -> list of row dicts
        self.zips = {}  # "2019-citibike-tripdata.zip" -> bytes
        self.tlc = {}  # ("yellow", "2024-01") -> bytes
        self.tlc_missing_code = {}  # ("green", "2024-01") -> 403 or 404
        self.tlc_fail = set()  # (type, month) that always reset the connection
        self.calls = []
        self.full_zip_downloads = 0  # Citi Bike zips fetched WITHOUT a Range header

    def __call__(self, req, timeout=None):
        url, method = req.full_url, req.get_method()
        self.calls.append((method, url))
        if "data.cityofnewyork.us" in url:
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            day = q["$where"][0].split("'")[1][:10]
            lo, n = int(q["$offset"][0]), int(q["$limit"][0])
            return Resp(json.dumps(self.soda.get(day, [])[lo : lo + n]).encode())
        if "s3.amazonaws.com/tripdata" in url:
            name = url.rsplit("/", 1)[-1]
            if name not in self.zips:
                raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
            body = self.zips[name]
            if method == "HEAD":
                return Resp(b"", {"Content-Length": str(len(body))})
            rng = req.get_header("Range")
            if rng:  # "bytes=0-99"
                start, end = map(int, rng.split("=")[1].split("-"))
                return Resp(body[start : end + 1])
            self.full_zip_downloads += 1
            return Resp(body)
        if "cloudfront" in url:
            name = url.rsplit("/", 1)[-1]  # yellow_tripdata_2024-01.parquet
            t, month = name.split("_tripdata_")[0], name.split("_tripdata_")[1][:7]
            if (t, month) in self.tlc_fail:
                raise urllib.error.URLError("connection reset")
            if (t, month) in self.tlc_missing_code:
                raise urllib.error.HTTPError(
                    url, self.tlc_missing_code[(t, month)], "x", {}, None
                )
            if (t, month) not in self.tlc:
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            body = self.tlc[(t, month)]
            return Resp(
                b"" if method == "HEAD" else body, {"Content-Length": str(len(body))}
            )
        raise AssertionError(url)


def make_zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def read_gz(raw):
    return gzip.decompress(raw).decode()


@pytest.fixture
def world(monkeypatch):
    fake_s3, fake_web = FakeS3(), FakeWeb()
    monkeypatch.setattr(ln, "s3", fake_s3)
    monkeypatch.setattr(ln.urllib.request, "urlopen", fake_web)
    monkeypatch.setattr(ln.time, "sleep", lambda s: None)
    monkeypatch.setattr(ln, "SODA_PAGE", 2)  # small pages so paging is exercised
    return fake_s3, fake_web


def event(source, **kw):
    return {"zone": "test", "source": source, **kw}


# ------------------------------------------------------------------------------ 311


def test_311_pages_and_writes_day_file_and_manifest(world):
    fake_s3, fake_web = world
    fake_web.soda["2024-01-05"] = [
        {"unique_key": str(i), "borough": "BRONX"} for i in range(5)
    ]

    out = ln.handler(event("311", day="2024-01-05"), None)

    assert out["ok"] is True
    assert out["detail"]["rows"] == 5 and out["detail"]["pages"] == 3  # 2 + 2 + 1
    lines = read_gz(
        fake_s3.objects["raw/test/nyc311/day=2024-01-05/requests.jsonl.gz"]
    ).splitlines()
    assert [json.loads(x)["unique_key"] for x in lines] == ["0", "1", "2", "3", "4"]
    manifest = json.loads(
        fake_s3.objects["raw/test/nyc311/_manifests/day=2024-01-05.json"]
    )
    assert manifest["detail"]["rows"] == 5
    # the query is for exactly that one day
    assert any(
        "2024-01-05T00%3A00%3A00" in u and "2024-01-06T00%3A00%3A00" in u
        for _, u in fake_web.calls
    )


def test_311_day_with_no_rows_fails_and_writes_nothing(world):
    fake_s3, _ = world
    out = ln.handler(event("311", day="2019-06-01"), None)
    assert out["ok"] is False and out["manifest"] is None
    assert "0 rows" in out["detail"]["error"]
    assert not any(
        k.startswith("raw/test/nyc311") for k in fake_s3.objects
    )  # no empty "done" note


def test_311_uses_the_dataset_that_holds_the_year(world):
    _, fake_web = world
    fake_web.soda["2019-12-31"] = [{"unique_key": "1"}]
    fake_web.soda["2020-01-01"] = [{"unique_key": "2"}]
    ln.handler(event("311", day="2019-12-31"), None)
    ln.handler(event("311", day="2020-01-01"), None)
    urls = [u for _, u in fake_web.calls if "data.cityofnewyork.us" in u]
    assert "/76ig-c548.json" in urls[0]  # 2010 to 2019
    assert "/erm2-nwe9.json" in urls[1]  # 2020 to present


def test_finished_unit_is_skipped_unless_forced(world):
    _, fake_web = world
    fake_web.soda["2024-01-05"] = [{"unique_key": "1"}]
    ln.handler(event("311", day="2024-01-05"), None)
    fake_web.calls.clear()

    again = ln.handler(event("311", day="2024-01-05"), None)
    assert again["skipped"] is True and fake_web.calls == []

    forced = ln.handler(event("311", day="2024-01-05", force=True), None)
    assert forced["skipped"] is False and fake_web.calls != []


# ------------------------------------------------------------------------------ Citi Bike

OLD_HEADER = "tripduration,starttime,stoptime,start station id\n"
NEW_HEADER = "ride_id,started_at,ended_at,start_station_id\n"


def test_citibike_yearly_zip_picks_the_month_and_splits_by_day(world):
    fake_s3, fake_web = world
    april = (
        OLD_HEADER
        + "100,2019-04-01 00:00:03.3830,x,1\n"
        + "200,2019-04-01 10:00:00,x,2\n"
        + "300,4/3/2019 10:00:00,x,3\n"  # the other date format
        + "400,not a date,x,4\n"  # unparsed
        + "500,2001-01-01 00:00:00,x,5\n"  # far outside the month
    )
    may = OLD_HEADER + "600,2019-05-01 00:00:00,x,6\n"
    fake_web.zips["2019-citibike-tripdata.zip"] = make_zip(
        {
            "2019-citibike-tripdata/4_April/201904-citibike-tripdata_1.csv": april,
            "2019-citibike-tripdata/5_May/201905-citibike-tripdata_1.csv": may,
            "2019-citibike-tripdata/.DS_Store": "junk",
            "__MACOSX/2019-citibike-tripdata/._201904-citibike-tripdata_1.csv": "junk",
        }
    )

    out = ln.handler(event("citibike", month="2019-04"), None)

    assert out["ok"] is True
    day1 = read_gz(
        fake_s3.objects[
            "raw/test/citibike/day=2019-04-01/201904-citibike-tripdata_1.csv.gz"
        ]
    )
    assert day1.splitlines()[0].startswith("tripduration,starttime")  # header kept
    assert len(day1.splitlines()) == 3  # header + 2 rides
    assert (
        "raw/test/citibike/day=2019-04-03/201904-citibike-tripdata_1.csv.gz"
        in fake_s3.objects
    )
    rejected = read_gz(
        fake_s3.objects[
            "raw/test/citibike/_rejected/month=2019-04/201904-citibike-tripdata_1.csv.gz"
        ]
    )
    assert "unparsed" in rejected and "out_of_range" in rejected
    stats = out["detail"]["files"][0]
    assert (stats["rows"], stats["unparsed"], stats["out_of_range"], stats["days"]) == (
        5,
        1,
        1,
        2,
    )
    assert not any(
        "May" in k or "05-01" in k for k in fake_s3.objects
    )  # other months untouched
    assert "raw/test/citibike/_manifests/month=2019-04.json" in fake_s3.objects


def test_citibike_yearly_zip_with_a_zip_per_month(world):
    fake_s3, fake_web = world
    inner_oct = make_zip(
        {
            "202010-citibike-tripdata_1.csv": NEW_HEADER
            + "a,2020-10-03 08:00:00,x,1\nb,2020-10-04 09:00:00,x,2\n",
            "__MACOSX/._202010-citibike-tripdata_1.csv": "junk",
        }
    )
    inner_nov = make_zip(
        {"202011-citibike-tripdata_1.csv": NEW_HEADER + "c,2020-11-01 08:00:00,x,3\n"}
    )
    fake_web.zips["2020-citibike-tripdata.zip"] = make_zip(
        {
            "2020-citibike-tripdata/": "",
            "2020-citibike-tripdata/202010-citibike-tripdata.zip": inner_oct,
            "2020-citibike-tripdata/202011-citibike-tripdata.zip": inner_nov,
        }
    )

    out = ln.handler(event("citibike", month="2020-10"), None)

    assert out["ok"] is True and out["detail"]["rows"] == 2
    assert (
        "raw/test/citibike/day=2020-10-03/202010-citibike-tripdata_1.csv.gz"
        in fake_s3.objects
    )
    assert (
        "raw/test/citibike/day=2020-10-04/202010-citibike-tripdata_1.csv.gz"
        in fake_s3.objects
    )
    assert not any(
        "2020-11" in k for k in fake_s3.objects
    )  # November's zip was not opened
    assert "raw/test/citibike/_manifests/month=2020-10.json" in fake_s3.objects


def test_tiny_test_refuses_zips_inside_zips(world):
    _, fake_web = world
    fake_web.zips["2020-citibike-tripdata.zip"] = make_zip(
        {
            "2020-citibike-tripdata/202010-citibike-tripdata.zip": make_zip(
                {"a.csv": NEW_HEADER}
            )
        }
    )
    with pytest.raises(ValueError, match="zips inside zips"):
        ln.handler(event("citibike", month="2020-10", max_rows=5), None)


def test_citibike_monthly_zip_from_2024(world):
    fake_s3, fake_web = world
    fake_web.zips["202401-citibike-tripdata.zip"] = make_zip(
        {"202401-citibike-tripdata.csv": NEW_HEADER + "a,2024-01-02 08:00:00.123,x,9\n"}
    )
    out = ln.handler(event("citibike", month="2024-01"), None)
    assert out["ok"] is True
    assert (
        "raw/test/citibike/day=2024-01-02/202401-citibike-tripdata.csv.gz"
        in fake_s3.objects
    )


def test_citibike_tiny_test_reads_a_few_rows_without_downloading_the_zip(
    world, monkeypatch
):
    fake_s3, fake_web = world
    monkeypatch.setattr(
        ln.RangeFile, "BLOCK", 256
    )  # tiny pieces, so many separate requests
    rows = "".join(f"{i},2019-04-0{1 + i % 3} 10:00:00,x,{i}\n" for i in range(200))
    fake_web.zips["2019-citibike-tripdata.zip"] = make_zip(
        {
            "2019-citibike-tripdata/4_April/201904-citibike-tripdata_1.csv": OLD_HEADER
            + rows,
            "2019-citibike-tripdata/4_April/201904-citibike-tripdata_2.csv": OLD_HEADER
            + rows,
        }
    )

    out = ln.handler(event("citibike", month="2019-04", max_rows=10), None)

    assert out["ok"] is True and out["manifest"] is None
    assert out["detail"]["partial"] is True and out["detail"]["rows"] == 10
    assert len(out["detail"]["files"]) == 1  # only the first file was read
    assert out["detail"]["files"][0]["stopped_early"] is True
    assert fake_web.full_zip_downloads == 0  # never fetched the whole zip
    assert not any(
        "_manifests" in k for k in fake_s3.objects
    )  # a test must not look like a finished month
    total = sum(
        len(read_gz(v).splitlines()) - 1
        for k, v in fake_s3.objects.items()
        if "/day=" in k
    )
    assert total == 10


def test_tiny_test_ignores_an_existing_manifest(world):
    fake_s3, fake_web = world
    fake_web.zips["202401-citibike-tripdata.zip"] = make_zip(
        {"202401-citibike-tripdata.csv": NEW_HEADER + "a,2024-01-02 08:00:00,x,9\n"}
    )
    ln.handler(event("citibike", month="2024-01"), None)  # a full run leaves a manifest
    again = ln.handler(event("citibike", month="2024-01", max_rows=5), None)
    assert again["skipped"] is False and again["detail"]["partial"] is True


def test_citibike_month_not_published_is_missing_not_failed(world):
    fake_s3, _ = world
    out = ln.handler(event("citibike", month="2026-12"), None)
    assert out["ok"] is True and out["detail"]["status"] == "missing"
    assert "raw/test/citibike/_manifests/month=2026-12.json" in fake_s3.objects


def test_citibike_file_without_start_column_fails_and_has_no_manifest(world):
    fake_s3, fake_web = world
    fake_web.zips["202401-citibike-tripdata.zip"] = make_zip(
        {"202401-citibike-tripdata.csv": "a,b,c\n1,2,3\n"}
    )
    out = ln.handler(event("citibike", month="2024-01"), None)
    assert out["ok"] is False and out["manifest"] is None
    assert "raw/test/citibike/_manifests/month=2024-01.json" not in fake_s3.objects


def test_citibike_yearly_zip_without_that_month_fails_clearly(world):
    fake_s3, fake_web = world
    fake_web.zips["2019-citibike-tripdata.zip"] = make_zip(
        {"2019-citibike-tripdata/5_May/201905-citibike-tripdata_1.csv": OLD_HEADER}
    )
    with pytest.raises(ValueError, match="no CSV files for 2019-04"):
        ln.handler(event("citibike", month="2019-04"), None)


# ------------------------------------------------------------------------------ TLC


def test_tlc_streams_files_and_marks_unpublished_ones_missing(world):
    fake_s3, fake_web = world
    fake_web.tlc[("yellow", "2024-01")] = b"PAR1-yellow"
    fake_web.tlc_missing_code[("green", "2024-01")] = (
        403  # CloudFront answers 403 or 404
    )
    # fhv and fhvhv are not in fake_web.tlc, so they answer 404

    out = ln.handler(event("tlc", month="2024-01"), None)

    assert out["ok"] is True
    status = {t: d["status"] for t, d in out["detail"].items()}
    assert status == {
        "yellow": "done",
        "green": "missing",
        "fhv": "missing",
        "fhvhv": "missing",
    }
    key = "raw/test/tlc/yellow/month=2024-01/yellow_tripdata_2024-01.parquet"
    assert fake_s3.objects[key] == b"PAR1-yellow"


def test_tlc_failed_type_means_no_manifest_then_rerun_finishes(world):
    fake_s3, fake_web = world
    fake_web.tlc[("yellow", "2024-01")] = b"Y"
    fake_web.tlc[("fhvhv", "2024-01")] = b"H"
    fake_web.tlc_fail = {("fhvhv", "2024-01")}

    out = ln.handler(event("tlc", month="2024-01"), None)
    assert out["ok"] is False and out["manifest"] is None
    assert out["detail"]["fhvhv"]["status"] == "failed"

    fake_web.tlc_fail = set()  # the source recovered
    out = ln.handler(event("tlc", month="2024-01"), None)
    assert out["ok"] is True
    assert (
        out["detail"]["yellow"]["status"] == "skipped"
    )  # already in S3 with the same size
    assert out["detail"]["fhvhv"]["status"] == "done"


# ------------------------------------------------------------------------------ input checks


@pytest.mark.parametrize(
    "ev",
    [
        {"zone": "dev", "source": "311", "day": "2024-01-05"},
        {"zone": "raw", "source": "gdelt", "day": "2024-01-05"},
        {"zone": "raw", "source": "311", "day": "05-01-2024"},
        {"zone": "raw", "source": "citibike", "month": "2019-4-1"},
        {"zone": "raw", "source": "tlc", "month": "2024-01", "types": ["taxi"]},
        {
            "zone": "raw",
            "source": "citibike",
            "month": "2024-01",
            "max_rows": 5,
        },  # test zone only
        {
            "zone": "test",
            "source": "311",
            "day": "2024-01-05",
            "max_rows": 5,
        },  # citibike only
        {"zone": "test", "source": "citibike", "month": "2024-01", "max_rows": 0},
    ],
)
def test_bad_input_is_rejected(world, ev):
    with pytest.raises(ValueError):
        ln.handler(ev, None)
