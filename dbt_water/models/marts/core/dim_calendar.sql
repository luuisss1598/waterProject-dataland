{{
    config(
        unique_key='_pk',
        alias='calendar'
    )
}}

with recursive date_back_spine as (
    select '2010-01-01'::date as calendar_date

    union all

    select
        calendar_date + 1 -- add one more date
    from date_back_spine
    where true
        and calendar_date < current_date
)
, calendar_data as (
    select
        {{dbt_utils.generate_surrogate_key(
            ['calendar_date']
        )}} as _pk,
        calendar_date as calendar_date,
        extract(month from calendar_date) as month,
        extract(year from calendar_date ) as year,
        extract(day from calendar_date) as day,
        date_trunc('month', calendar_date)::date as month_date,
        to_char(calendar_date, 'Month') as month_name,
        extract(doy from calendar_date) as day_of_year,
        extract(dow from calendar_date) as day_of_week,
        to_char(calendar_date, 'Day') as day_name,
        extract(quarter from calendar_date) as quarter,
        date_trunc('week', calendar_date)::date as week_start,
        (date_trunc('week', calendar_date) + '6 days'::interval)::date as week_end
    from date_back_spine
)
select
    *
from calendar_data