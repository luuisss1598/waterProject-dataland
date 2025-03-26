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
        sample_depth::float as sample_depth, -- a mix between decimal and whole numbers
        sample_depth_units::varchar as sample_depth_units,
        parameter::varchar as parameter,
        case
            when result <> '< R.L.' then result::float
            else 0 end as result_number, -- there might be values with 0, but are not '< R.L', however, we can filter those out with a condition (case when)
        case
            when result = '< R.L.' then result::varchar
            else 'N/A' end as result_RL, 
        reporting_limit::float as reporting_limit,
        units::varchar as units,
        method_name::varchar as method_name
    from {{ source('ca_water_quality', 'raw_water_quality_lab_results') }}
)
select 
    *
from src