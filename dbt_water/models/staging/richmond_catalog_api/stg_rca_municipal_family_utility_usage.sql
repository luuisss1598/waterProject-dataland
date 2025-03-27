with src as (
    select
        month::date as month_date,
        electricity_use_kwh::int as electricity_use_kwh,
        electricity_cost::int as electricity_cost,
        natural_gas_use_therms::int as natural_gas_use_therms,
        natural_gas_cost::int as natural_gas_cost,
        water_use_ccf::int as water_use_ccf,
        water_cost::int as water_cost,
        total_utility_cost::int as total_utility_cost
    from {{ source('richmond_water', 'raw_municipal_family_utility_usage') }}
)
select 
    *
from src