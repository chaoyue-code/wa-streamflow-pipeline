-- One row per station: the month each station's water year starts.
-- Rule (BoM Hydrologic Reference Stations): the water year begins with the
-- calendar month that has the lowest average monthly flow over the period of record.

with daily as (

    select
        station_no,
        reading_date,
        discharge_value
    from {{ ref('stg_bom__discharge') }}
    where not is_missing
      and discharge_value is not null

),

-- Mean flow for each individual month of record (e.g. March 1998)
monthly as (

    select
        station_no,
        date_trunc('month', reading_date) as month_start,
        avg(discharge_value)              as monthly_mean_discharge
    from daily
    group by station_no, date_trunc('month', reading_date)

),

-- Long-term average of those monthly means, per calendar month (Jan to Dec)
by_calendar_month as (

    select
        station_no,
        month(month_start)          as calendar_month,
        avg(monthly_mean_discharge) as avg_monthly_discharge,
        count(*)                    as months_in_record
    from monthly
    group by station_no, month(month_start)

)

select
    station_no,
    calendar_month        as water_year_start_month,
    avg_monthly_discharge as min_avg_monthly_discharge,
    months_in_record
from by_calendar_month
-- Lowest average month wins; ties go to the earlier month so the result is deterministic
qualify row_number() over (
    partition by station_no
    order by avg_monthly_discharge, calendar_month
) = 1
