with source as (
    select * from {{ source('raw', 'raw_bom_discharge') }}
),

files as (
    select
        raw:station_no::string                         as station_no,
        raw:station_name::string                       as station_name,
        raw:ts_id::string                              as ts_id,
        raw:ts_name::string                            as ts_name,
        coalesce(raw:load_type::string, 'backfill')    as load_type,
        try_to_timestamp_tz(raw:ingested_at::string)   as ingested_at,
        source_file,
        loaded_at,
        raw:payload                                    as payload
    from source
),

flattened as (
    select
        f.* exclude (payload),
        to_timestamp_tz(d.value[0]::string)            as reading_ts,
        d.value[1]::float                              as discharge_value,
        d.value[2]::int                                as quality_code
    from files f,
         lateral flatten(input => f.payload)           p,
         lateral flatten(input => p.value:data)        d
),

typed as (
    select
        station_no,
        station_name,
        ts_id,
        ts_name,
        convert_timezone('Australia/Perth', reading_ts)::date as reading_date,
        reading_ts,
        discharge_value,
        quality_code,
        quality_code = -1                              as is_missing,
        load_type,
        ingested_at,
        source_file,
        loaded_at
    from flattened
)

select *
from typed
qualify row_number() over (
    partition by ts_id, reading_date
    order by ingested_at desc, loaded_at desc, source_file desc
) = 1