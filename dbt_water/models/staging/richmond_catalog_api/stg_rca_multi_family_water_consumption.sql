with src as (
    select
        month::varchar as month,
        month_number_::smallint as month_number,
        year::smallint as year,
        avg_mm_gal_water_day::float as avg_mm_gal_water_day,
        full_date::date as full_date
    from {{ source('richmond_water', 'raw_multi_family_water_consumption') }}
)
select 
    *
from src