-- One row per station per water year.
-- water_year is labelled by the calendar year it STARTS in; start and end dates are
-- included so the label is never ambiguous.
-- Incomplete years (partial first/last years, or years with gaps) are kept but flagged
-- with is_complete_year; filter on it for trend analysis.

with daily as (

    select
        f.station_no,
        f.reading_date,
        f.discharge_cumecs,
        f.daily_volume_ml,
        f.is_missing,
        s.water_year_start_month,
        s.catchment_area_km2
    from {{ ref('fct_daily_discharge') }} f
    inner join {{ ref('dim_station') }} s
        on s.station_no = f.station_no

),

assigned as (

    select
        *,
        case
            when month(reading_date) >= water_year_start_month then year(reading_date)
            else year(reading_date) - 1
        end as water_year
    from daily

),

annual as (

    select
        station_no,
        water_year,
        any_value(water_year_start_month)                           as water_year_start_month,
        any_value(catchment_area_km2)                               as catchment_area_km2,
        count_if(not is_missing and discharge_cumecs is not null)   as days_with_data,
        sum(daily_volume_ml)                                        as annual_volume_ml,
        avg(discharge_cumecs)                                       as mean_discharge_cumecs,
        max(discharge_cumecs)                                       as max_daily_discharge_cumecs
    from assigned
    group by station_no, water_year

),

dated as (

    select
        *,
        date_from_parts(water_year, water_year_start_month, 1) as water_year_start_date
    from annual

)

select
    station_no,
    water_year,
    water_year_start_date,
    dateadd(day, -1, dateadd(year, 1, water_year_start_date))                  as water_year_end_date,
    datediff(day, water_year_start_date, dateadd(year, 1, water_year_start_date)) as days_in_water_year,
    days_with_data,
    days_with_data
        / datediff(day, water_year_start_date, dateadd(year, 1, water_year_start_date)) as completeness_pct,
    annual_volume_ml,
    annual_volume_ml / nullif(catchment_area_km2, 0)                           as runoff_depth_mm,  -- 1 ML/km2 = 1 mm
    mean_discharge_cumecs,
    max_daily_discharge_cumecs,
    days_with_data
        / datediff(day, water_year_start_date, dateadd(year, 1, water_year_start_date))
        >= {{ var('min_completeness_pct', 0.95) }}                             as is_complete_year
from dated
