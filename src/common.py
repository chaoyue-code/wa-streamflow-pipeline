"""Shared helpers for the wa-streamflow-pipeline ingestion scripts.

Used by ingest_bom.py and ingest_dwer.py. Keep source-specific logic (API parameters,
response checks, S3 key layout) in each script; only generic plumbing lives here.
"""

import csv
import json
import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "wa-streamflow-pipeline (portfolio project; chaoyue0224@gmail.com)"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "stations.csv"
LOCAL_OUT = PROJECT_ROOT / "data"  # dry-run output; data/ is gitignored


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def load_stations(path=CONFIG_PATH, only=None):
    """Read the station config as a list of dicts. Optionally keep only station numbers in `only`."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(f)]
    if only:
        wanted = set(only)
        rows = [r for r in rows if r["station_no"] in wanted]
        missing = wanted - {r["station_no"] for r in rows}
        if missing:
            raise SystemExit(f"Station(s) not in config: {sorted(missing)}")
    return rows


def make_session():
    """HTTP session that retries transient failures with exponential backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,  # waits roughly 2, 4, 8, 16 seconds between attempts
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,  # return the last response so raise_for_status reports it
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


# ---------------------------------------------------------------------------
# Storage: the same functions work against S3 (s3 client given) or the local
# data/ folder (s3=None), so dry runs mirror the real layout.
# ---------------------------------------------------------------------------

def write_output(body, key, bucket=None, s3=None):
    """Write bytes to S3 if an s3 client is given, otherwise to data/<key>. Returns the location."""
    if s3 is None:
        path = LOCAL_OUT / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return str(path)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{bucket}/{key}"


def list_objects(prefix, bucket=None, s3=None):
    """Return [(key, last_modified_timestamp)] under a prefix, newest first."""
    items = []
    if s3 is None:
        base = LOCAL_OUT / prefix
        if base.exists():
            for p in base.rglob("*.json"):
                items.append((p.relative_to(LOCAL_OUT).as_posix(), p.stat().st_mtime))
    else:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    items.append((obj["Key"], obj["LastModified"].timestamp()))
    return sorted(items, key=lambda kv: kv[1], reverse=True)


def read_json(key, bucket=None, s3=None):
    """Read and parse a JSON object from S3 or data/<key>."""
    if s3 is None:
        return json.loads((LOCAL_OUT / key).read_text(encoding="utf-8"))
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())
