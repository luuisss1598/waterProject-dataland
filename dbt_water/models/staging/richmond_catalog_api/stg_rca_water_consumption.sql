with src as (
    select
        sector::varchar as sector,
        month::varchar as month,
        month_transform::smallint as month_transform,
        year::smallint as  year,
        water_consumption_mgd::float as water_consumption_mgd,
        month_and_year_date::date as month_and_year_date
    from {{ source('richmond_water', 'raw_water_consumption') }}
)
select 
    *
from src