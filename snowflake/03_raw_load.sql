-- 03_raw_load.sql
-- Load bronze JSON files from S3 into RAW tables, one row per source file.
-- Safe to re-run: COPY INTO skips files it has already loaded (load history kept for 64 days).

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA WA_STREAMFLOW.RAW;

-- 1. File format: keep each JSON document whole
CREATE FILE FORMAT IF NOT EXISTS ff_json
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

-- 2. RAW tables: raw document, source file and load time
CREATE TABLE IF NOT EXISTS raw_bom_discharge (
  raw         VARIANT,
  source_file VARCHAR,
  loaded_at   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw_dwer_sites (
  raw         VARIANT,
  source_file VARCHAR,
  loaded_at   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Load BoM discharge files
COPY INTO raw_bom_discharge (raw, source_file)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @bronze_stage/bom/discharge/
)
FILE_FORMAT = (FORMAT_NAME = ff_json)
PATTERN = '.*[.]json';

-- 4. Load DWER sites file
COPY INTO raw_dwer_sites (raw, source_file)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @bronze_stage/dwer/sites/
)
FILE_FORMAT = (FORMAT_NAME = ff_json)
PATTERN = '.*[.]json';

-- 5. Check: both tables should have rows
SELECT 'raw_bom_discharge' AS table_name, COUNT(*) AS row_count, MAX(loaded_at) AS last_loaded FROM raw_bom_discharge
UNION ALL
SELECT 'raw_dwer_sites', COUNT(*), MAX(loaded_at) FROM raw_dwer_sites;
