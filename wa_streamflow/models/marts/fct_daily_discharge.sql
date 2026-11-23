-- One row per station per day.
-- Grain key: (station_no, reading_date). Missing days are kept as rows with
-- is_missing = true so completeness can be measured downstream.
-- quality_code is a foreign key to the bom_quality_codes seed.

select
    station_no,
    reading_date,
    discharge_value        as discharge_cumecs,   -- assumed m3/s (daily mean); confirm units
    discharge_value * 86.4 as daily_volume_ml,    -- 1 m3/s for 24 h = 86,400 m3 = 86.4 ML
    quality_code,
    is_missing,
    ingested_at
from {{ ref('stg_bom__discharge') }}
