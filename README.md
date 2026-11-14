# WA Streamflow Pipeline

An end-to-end data pipeline analysing long-term streamflow change in south-west Western Australia.
Author: Chaoyue Zhao

## Question
How has streamflow in south-west WA catchments changed since 1975, and which catchments show the steepest decline?

## Data sources
- **Bureau of Meteorology Water Data Online (KiWIS API):** daily mean discharge, time series `DMQaQc.Merged.DailyMean.24HR`
- **DWER Water Information Sites (DWER-145):** station metadata (catchment area, river basin, location, operational status), queried from the public SLIP ArcGIS REST service (`SLIP_Public_Services/Water/MapServer/61`). No login is required.

BoM station numbers match DWER `site_ref` values exactly, so `station_no` is the join key between the two sources.

## Project status
| Milestone | Scope | Status |
|---|---|---|
| 1. Ingestion | BoM discharge and DWER metadata into the S3 bronze layer, incremental loads | Done |
| 2. Warehouse and modelling | Snowflake, dbt staging and marts | In Progress |
| 3. Orchestration and data quality | GitHub Actions schedule, dbt tests | Planned |
| 4. Reporting | Power BI report and write-up | Planned |

## Station selection
1. Daily discharge record starting before 1975 and still current
2. At least 90% of days coded Quality A (code 10) since 1975.
   Completeness alone was misleading: some "100% complete" stations were mostly estimated values.
3. Listed as a BoM Hydrologic Reference Station (unregulated, minimal land-use change)
4. One station per basin

Exceptions:
- 613002 Dingo Rd accepted at 89% Quality A because it is an HRS station
- Basin 602 excluded: no candidate station was an HRS station
- 613007 Waterous ranked highest in basin 613 but is not an HRS station, so 613002 Dingo Rd was used instead
- HRS status was checked manually at bom.gov.au/water/hrs, so the final list differs from the notebook's automated shortlist

Final stations: see `config/stations.csv` (9 stations).

## Ingestion
```
src/
  common.py        shared helpers: HTTP session with retries, config loading, S3/local storage
  ingest_bom.py    BoM daily discharge, incremental, one file per station per run
  ingest_dwer.py   DWER-145 metadata for the configured stations, one file per run
config/
  stations.csv     station list, the single source of truth for both scripts
```

Both scripts read the station list from `config/stations.csv`, retry transient HTTP failures with exponential backoff, and store the raw API response unmodified inside a small metadata envelope (source, request parameters, `ingested_at`, record counts). Cleaning and type casting happen later in dbt.

### Bronze layout (S3)
```
bronze/bom/discharge/station=<station_no>/ingest_date=<YYYY-MM-DD>/discharge_<from>_<to>.json
bronze/dwer/sites/ingest_date=<YYYY-MM-DD>/sites.json
```
`ingest_date` is the UTC date of the run. Including the request window in the BoM file name means a same-day incremental run cannot overwrite a full load.

### Incremental loads (BoM)
For each station, the script finds the latest day **with a value** already in bronze, then re-requests from 7 days before it:
- **Missing days arrive as rows with a null value**, not as absent rows. They are ignored when finding the watermark, so days BoM fills in late are fetched on the next run.
- **The 7-day overlap** picks up late corrections. The same day can therefore appear in more than one bronze file; dbt staging keeps the row with the latest `ingested_at` per station and day.
- **Files are ranked by the end of their request window**, not upload time, so a manual backfill of old dates cannot move the watermark backwards.
- **A station with no bronze data** gets a full load from 1975.

### Data quality checks at ingestion
- **BoM:** KiWIS and ArcGIS can return HTTP 200 with an error in the body; both are detected and treated as failures. A failed station is logged and the run continues, then exits non-zero.
- **DWER:** the run fails, writing nothing, if any configured station is missing, duplicated or unexpected in the response.

### Running
```bash
source .venv/bin/activate
python src/ingest_bom.py --bucket <bucket>                    # incremental, all stations
python src/ingest_bom.py --dry-run --bucket <bucket>          # read state from S3, write to ./data
python src/ingest_bom.py --bucket <bucket> --full-refresh     # reload from 1975
python src/ingest_bom.py --bucket <bucket> --from 2020-01-01 --stations 613002   # manual backfill
python src/ingest_dwer.py --bucket <bucket>
```

## Warehouse (Snowflake)

`snowflake/`
- `aws_iam_setup.sh` read-only IAM policy and role that let Snowflake read the `bronze/` prefix only
- `01_setup.sql` database, RAW schema, and a 20-credit monthly resource monitor on `COMPUTE_WH` (X-Small, 60-second auto-suspend)
- `02_integration.sql` storage integration and external stage over the bronze layer
- `03_raw_load.sql` COPY INTO the RAW tables, plus a row-count check

Run in that order. All SQL scripts are safe to re-run, and COPY INTO skips files it has already loaded.

Access is a two-step handshake: the IAM role is created with a placeholder trust policy, then updated with the IAM user and external ID that Snowflake generates (`DESC INTEGRATION`). Never use `CREATE OR REPLACE` on the integration, as it generates a new external ID and breaks the trust policy.

RAW tables hold one row per bronze file: the JSON document whole in a `VARIANT` column, plus `source_file` and `loaded_at` for lineage. Parsing, deduplication and Perth time conversion happen in dbt staging.

## Notes
- Timestamps are returned in UTC (16:00 UTC = 00:00 AWST) and converted to Australia/Perth. The `DailyMean` series is already daily, so no aggregation is needed.
- Annual analysis will use the HRS water year, which BoM defines per station as starting in the month with the lowest average monthly flow. Water-year start months will be recorded per station in `config/stations.csv`.
- DWER dates (`ts_first_avail`, `ts_last_avail`) are epoch milliseconds in UTC, and `site_ref` is text while `station_no` is numeric in the config; both are handled in dbt staging.

## Data licensing and attribution
- **No data is stored in this repository.** Raw data is kept in a private S3 bucket; `data/` is gitignored.
- **DWER-145** is provided under the DWER Active Acceptance Licence Agreement. Information sourced from the Department of Water and Environmental Regulation (DWER), Government of Western Australia, which is the source and copyright owner of that information.
- **Streamflow data:** Bureau of Meteorology, Water Data Online. © Commonwealth of Australia.
