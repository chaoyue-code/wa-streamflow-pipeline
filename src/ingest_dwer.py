"""Ingest DWER Water Information Sites (DWER-145) metadata for configured stations into the S3 bronze layer.

Source: public SLIP ArcGIS REST service (no login needed)
    SLIP_Public_Services/Water/MapServer/61

Usage examples (run from the project root):
    python src/ingest_dwer.py --dry-run --stations 613002   # test one station locally
    python src/ingest_dwer.py --dry-run                     # all stations, local only
    python src/ingest_dwer.py --bucket chaoyue-wa-streamflow  # all stations to S3

The raw GeoJSON response is stored unmodified inside a small metadata envelope.
Cleaning (type casting, trimming, splitting river_basin, epoch-ms dates) happens later in dbt.

Licence note: DWER-145 is covered by the DWER Active Acceptance Licence Agreement.
Keep the data itself out of the public repo (data/ is gitignored); only this code is committed.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3

from common import CONFIG_PATH, load_stations, make_session, setup_logging, write_output

LAYER_URL = (
    "https://public-services.slip.wa.gov.au/public/rest/services/"
    "SLIP_Public_Services/Water/MapServer/61"
)
QUERY_URL = f"{LAYER_URL}/query"

# Only the fields relevant to stream gauges. The ~15 groundwater-only fields are always "N/A".
OUT_FIELDS = [
    "site_ref",
    "site_name",
    "site_shortname",
    "all_site_references",
    "site_type",
    "latitude",
    "longitude",
    "spheroid",
    "position_accuracy",
    "catch_area",
    "infra_status_current",
    "currently_monitored",
    "ts_first_avail",
    "ts_last_avail",
    "river_basin",
    "catchment",
    "surfacewater_area",
    "surfacewater_subarea",
    "department_region",
    "local_govt_authority",
    "pub_url",
]

log = logging.getLogger("ingest_dwer")


def build_params(station_ids):
    """Query parameters for the ArcGIS REST /query endpoint."""
    id_list = ",".join(f"'{s}'" for s in station_ids)  # site_ref is a string field
    return {
        "where": f"site_ref IN ({id_list})",
        "outFields": ",".join(OUT_FIELDS),
        "returnGeometry": "true",
        "outSR": "4326",  # WGS84 lon/lat, the GeoJSON standard
        "f": "geojson",
    }


def fetch_sites(session, station_ids):
    """Call the layer query. Returns (request_params, parsed_geojson)."""
    params = build_params(station_ids)
    resp = session.get(QUERY_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    # ArcGIS often returns HTTP 200 with an error object in the body.
    if "error" in payload:
        raise RuntimeError(f"ArcGIS error: {payload['error']}")
    if payload.get("type") != "FeatureCollection":
        raise RuntimeError(f"Unexpected response type: {payload.get('type')!r}")
    if payload.get("exceededTransferLimit") or payload.get("properties", {}).get("exceededTransferLimit"):
        raise RuntimeError("Response truncated by the server (exceededTransferLimit)")
    return params, payload


def validate(station_ids, payload):
    """Every configured station must appear exactly once. Returns a list of problems."""
    returned = [f["properties"].get("site_ref") for f in payload.get("features", [])]
    problems = []
    missing = sorted(set(station_ids) - set(returned))
    extra = sorted(set(returned) - set(station_ids))
    dupes = sorted({r for r in returned if returned.count(r) > 1})
    if missing:
        problems.append(f"missing from DWER: {missing}")
    if extra:
        problems.append(f"unexpected site_ref returned: {extra}")
    if dupes:
        problems.append(f"duplicate site_ref returned: {dupes}")
    return problems


def build_envelope(station_ids, params, payload, ingested_at):
    """Wrap the raw response with lineage metadata. The payload itself is not modified."""
    return {
        "source": "dwer_slip_mapserver_61",
        "dataset": "DWER-145 Water Information Sites",
        "endpoint": QUERY_URL,
        "request_params": params,
        "station_ids_requested": station_ids,
        "feature_count": len(payload.get("features", [])),
        "ingested_at": ingested_at.isoformat(),
        "payload": payload,
    }


def s3_key(ingest_date):
    return f"bronze/dwer/sites/ingest_date={ingest_date.isoformat()}/sites.json"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--stations", nargs="+", help="Only these station numbers (default: all in config)")
    p.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="S3 bucket (default: $S3_BUCKET)")
    p.add_argument("--dry-run", action="store_true", help="Write to local data/ instead of S3")
    return p.parse_args()


def main():
    setup_logging()
    args = parse_args()
    if not args.dry_run and not args.bucket:
        raise SystemExit("No bucket given. Use --bucket, set S3_BUCKET, or add --dry-run.")

    station_ids = [s["station_no"] for s in load_stations(CONFIG_PATH, args.stations)]
    log.info("Requesting DWER metadata for %d station(s): %s", len(station_ids), ", ".join(station_ids))

    session = make_session()
    try:
        params, payload = fetch_sites(session, station_ids)
    except Exception as exc:  # noqa: BLE001 - log any failure and exit non-zero
        log.error("Request failed: %s", exc)
        sys.exit(1)

    problems = validate(station_ids, payload)
    if problems:
        for msg in problems:
            log.error("Validation failed: %s", msg)
        log.error("Nothing written. Check the site_ref values in config/stations.csv.")
        sys.exit(1)

    ingested_at = datetime.now(timezone.utc)
    envelope = build_envelope(station_ids, params, payload, ingested_at)
    key = s3_key(ingested_at.date())
    body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    s3 = None if args.dry_run else boto3.client("s3")
    location = write_output(body, key, args.bucket, s3)
    log.info("Wrote %d feature(s) to %s", envelope["feature_count"], location)


if __name__ == "__main__":
    main()
