{{
    config(
        alias='municipal_family_utility_usage'
    )
}}

with staging_data as (
    select
        *
    from {{ ref("stg_rca_municipal_family_utility_usage") }}
)
select 
    *
from staging_data