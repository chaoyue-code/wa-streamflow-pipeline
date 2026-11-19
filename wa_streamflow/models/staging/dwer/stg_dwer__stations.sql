with source as (
    select * from {{ source('raw', 'raw_dwer_sites') }}
),

features as (
    select
        f.value                                      as feature,
        try_to_timestamp_tz(s.raw:ingested_at::string) as ingested_at,
        s.source_file,
        s.loaded_at
    from source s,
         lateral flatten(input => s.raw:payload:features) f
),

renamed as (
    select
        feature:properties:site_ref::string                      as site_ref,
        feature:properties:site_name::string                     as site_name,
        feature:properties:site_shortname::string                as site_shortname,
        feature:properties:site_type::string                     as site_type,
        feature:properties:river_basin::string                   as river_basin,
        feature:properties:catchment::string                     as catchment,
        feature:properties:surfacewater_area::string             as surfacewater_area,
        feature:properties:surfacewater_subarea::string          as surfacewater_subarea,
        feature:properties:department_region::string             as department_region,
        trim(feature:properties:local_govt_authority::string)    as local_govt_authority,
        try_to_double(feature:properties:catch_area::string)     as catchment_area_km2,
        feature:properties:currently_monitored::string = 'Yes'   as is_currently_monitored,
        feature:properties:infra_status_current::string          as infra_status,
        feature:properties:latitude::float                       as latitude,
        feature:properties:longitude::float                      as longitude,
        st_makepoint(feature:geometry:coordinates[0]::float,
                     feature:geometry:coordinates[1]::float)     as location,
        feature:properties:spheroid::string                      as spheroid,
        feature:properties:position_accuracy::string             as position_accuracy,
        to_timestamp_ntz(feature:properties:ts_first_avail::number, 3)::date as record_start_date,
        to_timestamp_ntz(feature:properties:ts_last_avail::number, 3)::date  as record_end_date,
        feature:properties:pub_url::string                       as report_url,
        ingested_at,
        source_file,
        loaded_at
    from features
)

select *
from renamed
qualify row_number() over (
    partition by site_ref
    order by ingested_at desc, loaded_at desc
) = 1