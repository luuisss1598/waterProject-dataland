with src as (
    select
        sector::varchar as sector,
        month::varchar as month,
        year::date as year_date,
        water_consumption_mgd::float as water_consumption_mgd
    from {{ source('richmond_water', 'raw_richmond_water_consumption_data') }} as src 
)
, fix_year_date as (
    select
        distinct month as month_name,
        year_date,
        case
            when month = 'January' then (extract(year from year_date) || '-01' || '-01')::date
            when month = 'February' then (extract(year from year_date) || '-02' || '-01')::date
            when month = 'March' then (extract(year from year_date) || '-03' || '-01')::date
            when month = 'April' then (extract(year from year_date) || '-04' || '-01')::date
            when month = 'May' then (extract(year from year_date) || '-05' || '-01')::date
            when month = 'June' then (extract(year from year_date) || '-06' || '-01')::date
            when month = 'July' then (extract(year from year_date) || '-07' || '-01')::date
            when month = 'August' then (extract(year from year_date) || '-08' || '-01')::date
            when month = 'September' then (extract(year from year_date) || '-09' || '-01')::date
            when month = 'October' then (extract(year from year_date) || '-10' || '-01')::date
            when month = 'November' then (extract(year from year_date) || '-11' || '-01')::date
            when month = 'December' then (extract(year from year_date) || '-12' || '-01')::date
            end as month_date
    from src
)
select
    fyd.month_date,
    s.*
from  src as s
left join fix_year_date as fyd on s.month=fyd.month_name and s.year_date=fyd.year_date