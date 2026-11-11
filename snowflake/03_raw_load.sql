USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
USE SCHEMA wa_streamflow.raw;

-- File format: keep each JSON document whole
CREATE FILE FORMAT ff_json
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

-- RAW tables: one row per source file
CREATE TABLE raw_bom_discharge (
  raw          VARIANT,
  source_file  VARCHAR,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE raw_dwer_sites (
  raw          VARIANT,
  source_file  VARCHAR,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load BoM discharge files
COPY INTO raw_bom_discharge (raw, source_file)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @bronze_stage/bom/discharge/
)
FILE_FORMAT = (FORMAT_NAME = ff_json)
PATTERN = '.*[.]json';

-- Load DWER sites file
COPY INTO raw_dwer_sites (raw, source_file)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @bronze_stage/dwer/sites/
)
FILE_FORMAT = (FORMAT_NAME = ff_json)
PATTERN = '.*[.]json';