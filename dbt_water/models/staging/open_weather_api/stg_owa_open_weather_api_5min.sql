with src as (
    select
        _pk::varchar as _pk,
        extracted_at::timestamp as extracted_at_local,
        id::varchar as id,
        name::varchar as city_name,
        'Ca'::varchar as state_name,
        timezone::varchar as timezone,
        cod::int as cod,
        coord::jsonb as coordinates,
        weather::jsonb as weather,
        base::varchar as base,
        main::jsonb as main,
        visibility::varchar as visibility,
        wind::jsonb as wind,
        clouds::jsonb as clouds,
        dt::varchar as dt,
        sys::jsonb as sys
    from {{ source('open_weather', 'open_weather_api_5min') }}
)
select
    *
from src