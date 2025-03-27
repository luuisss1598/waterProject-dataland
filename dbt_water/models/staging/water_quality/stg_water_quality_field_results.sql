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
        anl_data_type::varchar as anl_data_type,
        parameter::varchar as parameter,
        fdr_result::float as fdr_result,
        fdr_text_result::varchar as fdr_text_result,
        fdr_date_result::timestamp as fdr_date_result,
        fdr_reporting_limit::float as fdr_reporting_limit,
        uns_name::varchar as uns_name,
        mth_name::varchar as mth_name,
        fdr_footnote::text as fdr_footnote
    from {{ source('ca_water_quality', 'raw_water_quality_field_results') }}
)
select 
    *
from src