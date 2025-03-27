{{
    config(
        alias='water_quality_lab_results'
    )
}}

select 
    station_id, 
    station_name, 
    full_station_name, 
    station_number, 
    station_type, 
    latitude, 
    longitude, 
    county_name, 
    sample_code, 
    sample_date, 
    sample_depth, 
    sample_depth_units, 
    parameter, 
    result,
    reporting_limit, 
    units, 
    method_name
from {{ ref('stg_water_quality_water_quality_lab_results') }}