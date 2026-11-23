-- One row per BoM station in the pipeline.
-- Starts from the BoM stations (not DWER, which lists every WA site) and left-joins
-- DWER metadata, so a station without a DWER match still appears and can be caught by tests.

with bom_stations as (

    select
        station_no,
        any_value(station_name) as bom_station_name,
        any_value(ts_id)        as ts_id,
        min(reading_date)       as first_reading_date,
        max(reading_date)       as last_reading_date
    from {{ ref('stg_bom__discharge') }}
    group by station_no

),

dwer as (

    select * from {{ ref('stg_dwer__stations') }}

),

water_year as (

    select * from {{ ref('int_station_water_year') }}

)

select
    b.station_no,
    coalesce(d.site_name, b.bom_station_name) as station_name,
    b.ts_id,
    d.river_basin,
    d.catchment,
    d.surfacewater_area,
    d.department_region,
    d.catchment_area_km2,
    d.latitude,
    d.longitude,
    d.location,                    -- GEOGRAPHY point; Power BI maps will use latitude/longitude
    d.is_currently_monitored,
    b.first_reading_date,
    b.last_reading_date,
    w.water_year_start_month,
    d.report_url
from bom_stations b
left join dwer d
    on d.site_ref = b.station_no
left join water_year w
    on w.station_no = b.station_no
