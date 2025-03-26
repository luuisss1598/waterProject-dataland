with src as (
    select
        sector::varchar as sector,
        month::varchar as month,
        year::date as year_date,
        water_consumption_mgd::float as water_consumption_mgd
    from {{ source('richmond_water', 'raw_richmond_water_consumption_data') }}
)
select 
    *
from src 