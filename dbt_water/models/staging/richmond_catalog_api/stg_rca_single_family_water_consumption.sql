with src as (
    select
        month::varchar as month,
        month_number::smallint as month_number,
        year::smallint as year,
        quantity_avg_mm_gal_day::float as quantity_avg_mm_gal_day,
        full_date::date as full_date
    from {{ source('richmond_water', 'raw_single_family_water_consumption') }}
)
select 
    *
from src