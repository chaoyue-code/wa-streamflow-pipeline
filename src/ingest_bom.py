"""Ingest BoM daily discharge (KiWIS) for configured stations into the S3 bronze layer, incrementally.

Usage examples (run from the project root):
    python src/ingest_bom.py --bucket chaoyue-wa-streamflow                  # incremental, all stations
    python src/ingest_bom.py --dry-run --bucket chaoyue-wa-streamflow        # read state from S3, write locally
    python src/ingest_bom.py --dry-run --stations 613002                     # local state and output only
    python src/ingest_bom.py --bucket chaoyue-wa-streamflow --full-refresh   # reload everything from 1975
    python src/ingest_bom.py --bucket chaoyue-wa-streamflow --from 2020-01-01 --stations 613002

How the incremental window is chosen (per station):
    1. --from given        -> use it (manual backfill).
    2. --full-refresh      -> DEFAULT_FROM.
    3. Otherwise           -> watermark = latest day WITH A VALUE already in bronze,
                              then go back LOOKBACK_DAYS to pick up late corrections.
                              No bronze data yet -> DEFAULT_FROM (first load).

Days with no data come back as rows with a null value, so the watermark ignores them;
otherwise days BoM fills in later would never be fetched.

The overlap means the same day can appear in several bronze files. That is intended:
bronze is append-only, and dbt staging keeps the row from the latest ingested_at.

The raw KiWIS response is stored unmodified inside a small metadata envelope.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3

from common import (
    CONFIG_PATH,
    list_objects,
    load_stations,
    make_session,
    read_json,
    setup_logging,
    write_output,
)

BASE_URL = "http://www.bom.gov.au/waterdata/services"
TS_NAME = "DMQaQc.Merged.DailyMean.24HR"
RETURN_FIELDS = "Timestamp,Value,Quality Code"
DEFAULT_FROM = "1975-01-01"
LOOKBACK_DAYS = 7          # re-fetch this many days before the watermark
MAX_FILES_TO_SCAN = 10     # stop looking for a watermark after this many files
LOCAL_TZ = ZoneInfo("Australia/Perth")

log = logging.getLogger("ingest_bom")


# --------------------------------------------------------------------------- API

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


def get_rows(payload):
    """Return (timestamp_index, value_index, rows) from a getTimeseriesValues response."""
    block = payload[0]
    cols = [c.strip() for c in block.get("columns", RETURN_FIELDS).split(",")]
    return cols.index("Timestamp"), cols.index("Value"), block["data"]


def count_records(payload):
    """(total rows, rows with a value), or (None, None) if the shape is unexpected."""
    try:
        _, vi, rows = get_rows(payload)
    except (KeyError, IndexError, TypeError, ValueError):
        return None, None
    return len(rows), sum(1 for r in rows if r[vi] is not None)


def to_local_date(ts):
    """KiWIS timestamp string -> calendar date in Perth."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ).date()


def latest_valued_date(payload):
    """Latest local date that has a non-null value, or None."""
    try:
        ti, vi, rows = get_rows(payload)
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    dates = [to_local_date(r[ti]) for r in rows if r[vi] is not None]
    return max(dates) if dates else None


# ------------------------------------------------------------------- watermark

def station_prefix(station_no):
    return f"bronze/bom/discharge/station={station_no}/"


KEY_PATTERN = re.compile(
    r"ingest_date=(?P<ingest>\d{4}-\d{2}-\d{2})/discharge(?:_(?P<from>[\d-]+)_(?P<to>[\d-]+))?\.json$"
)


def window_end(key):
    """End date of the window a file covers, from its key. Legacy discharge.json -> its ingest_date."""
    m = KEY_PATTERN.search(key)
    if not m:
        return ""
    return m.group("to") or m.group("ingest")


def find_watermark(station_no, bucket, s3):
    """Latest day with a value, checking files whose window ends latest first.

    Ordering by window end (not upload time) means a manual backfill of old dates
    can't drag the watermark backwards.
    """
    keys = [k for k, _ in list_objects(station_prefix(station_no), bucket, s3)]
    keys.sort(key=window_end, reverse=True)
    for key in keys[:MAX_FILES_TO_SCAN]:
        found = latest_valued_date(read_json(key, bucket, s3).get("payload"))
        if found:
            return found
    return None


def choose_window_start(args, station_no, bucket, state_s3):
    """Return (date_from, reason)."""
    if args.date_from:
        return args.date_from, "manual --from"
    if args.full_refresh:
        return DEFAULT_FROM, "full refresh"
    wm = find_watermark(station_no, bucket, state_s3)
    if wm is None:
        return DEFAULT_FROM, "no bronze data yet, first load"
    start = wm - timedelta(days=LOOKBACK_DAYS)
    return start.isoformat(), f"watermark {wm.isoformat()} minus {LOOKBACK_DAYS} days"


# --------------------------------------------------------------------- output

def build_key(station_no, ingest_date, date_from, date_to):
    # Window in the file name: a same-day incremental run can't overwrite a full load.
    return (
        f"{station_prefix(station_no)}ingest_date={ingest_date}/"
        f"discharge_{date_from}_{date_to}.json"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Ingest BoM daily discharge into S3 bronze (incremental).")
    p.add_argument("--stations", nargs="+", help="Station numbers to run (default: all in config)")
    p.add_argument("--from", dest="date_from", default=None, help="Override start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", default=date.today().isoformat(), help="End date YYYY-MM-DD")
    p.add_argument("--full-refresh", action="store_true", help=f"Reload from {DEFAULT_FROM}")
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="S3 bucket (or set S3_BUCKET)")
    p.add_argument("--dry-run", action="store_true",
                   help="Write to ./data instead of S3 (state is still read from S3 if a bucket is given)")
    return p.parse_args()


def main():
    setup_logging()
    args = parse_args()
    if not args.dry_run and not args.bucket:
        raise SystemExit("No bucket given. Use --bucket or set S3_BUCKET, or run with --dry-run.")

    stations = load_stations(CONFIG_PATH, args.stations)
    session = make_session()
    state_s3 = boto3.client("s3") if args.bucket else None   # where we read the watermark from
    write_s3 = None if args.dry_run else state_s3             # where we write the new file

    ingested_at = datetime.now(timezone.utc)
    ingest_date = ingested_at.date().isoformat()

    log.info(
        "Starting ingestion: %d station(s), to %s, state=%s, target=%s",
        len(stations), args.date_to,
        f"s3://{args.bucket}" if state_s3 else "local",
        "local (dry run)" if write_s3 is None else f"s3://{args.bucket}",
    )

    succeeded, failed = [], []
    for st in stations:
        station_no, name = st["station_no"], st["station_name"]
        try:
            date_from, reason = choose_window_start(args, station_no, args.bucket, state_s3)
            params, payload = fetch_values(session, st["ts_id"], date_from, args.date_to)
            total, valued = count_records(payload)
            envelope = {
                "source": "bom_kiwis",
                "station_no": station_no,
                "station_name": name,
                "ts_id": st["ts_id"],
                "ts_name": TS_NAME,
                "load_type": "full" if date_from == DEFAULT_FROM else "incremental",
                "window_reason": reason,
                "request_params": params,
                "ingested_at": ingested_at.isoformat(),
                "record_count": total,
                "valued_record_count": valued,
                "payload": payload,  # raw API response, unmodified
            }
            body = json.dumps(envelope).encode("utf-8")
            key = build_key(station_no, ingest_date, date_from, args.date_to)
            target = write_output(body, key, args.bucket, write_s3)
            if total is None:
                log.warning("%s %s: unreadable response, check it", station_no, name)
            log.info("%s %s: %s to %s (%s): %s rows, %s with values -> %s",
                     station_no, name, date_from, args.date_to, reason, total, valued, target)
            succeeded.append(station_no)
        except Exception as exc:  # keep going so one bad station doesn't stop the run
            log.error("%s %s: FAILED (%s)", station_no, name, exc)
            failed.append(station_no)

    log.info("Done: %d succeeded, %d failed %s", len(succeeded), len(failed), failed or "")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
