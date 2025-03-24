{{
    config(
        materialized = 'view'
    )
}}


with raw_data as (
     select
        station_id::varchar as station_id,
        station_name::varchar as station_name,
        full_station_name::varchar as full_station_name,
        station_number::varchar as station_number,
        station_type::varchar as station_type,
        latitude::float as latitude,
        longitude::float as longitude,
        county_name::varchar as county_name,
        sample_count::int as sample_count,
        sample_date_min::timestamp as sample_date_min,
        sample_date_max::timestamp as sample_date_max
    from {{ source('ca_water_quality', 'raw_water_quality_stations') }}
)
select 
    station_id,
    station_name,
    full_station_name,
    station_number,
    station_type,
    latitude,
    longitude,
    county_name,
    sample_count,
    sample_date_min,
    sample_date_max
from raw_data