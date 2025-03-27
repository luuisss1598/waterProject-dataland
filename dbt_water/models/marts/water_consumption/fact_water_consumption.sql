{{
    config(
        alias='general_water_consumption'
    )
}}

with combine_water_consumption as (
    select
        sector,
        month,
        water_consumption_mgd,
        month_date as month_date
    from {{ ref("stg_rca_richmond_water_consumption_data") }}

    union all

    select
        sector,
        month,
        water_consumption_mgd,
        month_and_year_date as month_date
    from {{ ref('stg_rca_water_consumption') }}
)
select
    distinct * -- remove duplicated rows
from combine_water_consumption