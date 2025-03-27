with src as (
    select 
        station_id::varchar as station_id,
        station_name::varchar as station_name,
        full_station_name::varchar as full_station_name,
        station_number::varchar as station_number,
        station_type::varchar as station_type,
        latitude::float as latitude,
        longitude::float as longitude,
        county_name::varchar  as county_name,
        sample_code::varchar as sample_code,
        sample_date::timestamp as sample_date,
        sample_depth::varchar as sample_depth, -- a mix between decimal and whole numbers
        sample_depth_units::varchar as sample_depth_units,
        parameter::varchar as parameter,
        result as result,
        reporting_limit::varchar as reporting_limit,
        units::varchar as units,
        method_name::varchar as method_name
    from {{ source('ca_water_quality', 'raw_water_quality_lab_results') }}
)
select 
    *
from src