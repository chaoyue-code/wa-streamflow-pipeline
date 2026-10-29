"""Ingest BoM daily discharge (KiWIS) for configured stations into the S3 bronze layer.

Usage examples (run from the project root):
    python src/ingest_bom.py --dry-run --stations 613002   # test one station locally
    python src/ingest_bom.py --dry-run                     # all stations, local only
    python src/ingest_bom.py --bucket chaoyue-wa-streamflow  # all stations to S3

The raw KiWIS response is stored unmodified inside a small metadata envelope.
Cleaning, flattening and type casting happen later in dbt (staging layer).
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "http://www.bom.gov.au/waterdata/services"
USER_AGENT = "wa-streamflow-pipeline (portfolio project; chaoyue0224@gmail.com)"
TS_NAME = "DMQaQc.Merged.DailyMean.24HR"
RETURN_FIELDS = "Timestamp,Value,Quality Code"
DEFAULT_FROM = "1975-01-01"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "stations.csv"
LOCAL_OUT = PROJECT_ROOT / "data"  # dry-run output; data/ is gitignored

log = logging.getLogger("ingest_bom")


def load_stations(path, only=None):
    """Read the station config. Optionally keep only the station numbers in `only`."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
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


def fetch_values(session, ts_id, date_from, date_to):
    """Call KiWIS getTimeseriesValues. Returns (request_params, parsed_json)."""
    params = {
        "service": "kisters",
        "type": "queryServices",
        "datasource": 0,
        "format": "json",
        "request": "getTimeseriesValues",
        "ts_id": ts_id,
        "from": date_from,
        "to": date_to,
        "returnfields": RETURN_FIELDS,
    }
    r = session.get(BASE_URL, params=params, timeout=120)
    r.raise_for_status()
    payload = r.json()
    # KiWIS can return HTTP 200 with an error object in the body
    if isinstance(payload, dict) and payload.get("type") == "error":
        raise RuntimeError(f"KiWIS error: {payload.get('message', payload)}")
    return params, payload


def count_records(payload):
    """Number of value rows in a getTimeseriesValues response, or None if the shape is unexpected."""
    try:
        return len(payload[0]["data"])
    except (KeyError, IndexError, TypeError):
        return None


def build_key(station_no, ingest_date):
    return (
        f"bronze/bom/discharge/station={station_no}/"
        f"ingest_date={ingest_date}/discharge.json"
    )


def write_output(body, key, bucket, dry_run, s3):
    if dry_run:
        path = LOCAL_OUT / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return str(path)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    return f"s3://{bucket}/{key}"


def parse_args():
    p = argparse.ArgumentParser(description="Ingest BoM daily discharge into S3 bronze.")
    p.add_argument("--stations", nargs="+", help="Station numbers to run (default: all in config)")
    p.add_argument("--from", dest="date_from", default=DEFAULT_FROM, help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", default=date.today().isoformat(), help="End date YYYY-MM-DD")
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="S3 bucket (or set S3_BUCKET)")
    p.add_argument("--dry-run", action="store_true", help="Write to ./data instead of S3")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()
    if not args.dry_run and not args.bucket:
        raise SystemExit("No bucket given. Use --bucket or set S3_BUCKET, or run with --dry-run.")

    stations = load_stations(CONFIG_PATH, args.stations)
    session = make_session()
    s3 = None if args.dry_run else boto3.client("s3")

    ingested_at = datetime.now(timezone.utc)
    ingest_date = ingested_at.date().isoformat()

    log.info(
        "Starting ingestion: %d station(s), %s to %s, target=%s",
        len(stations), args.date_from, args.date_to,
        "local (dry run)" if args.dry_run else f"s3://{args.bucket}",
    )

    succeeded, failed = [], []
    for st in stations:
        station_no = st["station_no"]
        try:
            params, payload = fetch_values(session, st["ts_id"], args.date_from, args.date_to)
            n = count_records(payload)
            envelope = {
                "source": "bom_kiwis",
                "station_no": station_no,
                "station_name": st["station_name"],
                "ts_id": st["ts_id"],
                "ts_name": TS_NAME,
                "request_params": params,
                "ingested_at": ingested_at.isoformat(),
                "record_count": n,
                "payload": payload,  # raw API response, unmodified
            }
            body = json.dumps(envelope).encode("utf-8")
            target = write_output(body, build_key(station_no, ingest_date), args.bucket, args.dry_run, s3)
            if not n:
                log.warning("%s %s: 0 or unreadable records, check the response", station_no, st["station_name"])
            log.info("%s %s: %s records -> %s", station_no, st["station_name"], n, target)
            succeeded.append(station_no)
        except Exception as exc:  # keep going so one bad station doesn't stop the run
            log.error("%s %s: FAILED (%s)", station_no, st["station_name"], exc)
            failed.append(station_no)

    log.info("Done: %d succeeded, %d failed %s", len(succeeded), len(failed), failed or "")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
