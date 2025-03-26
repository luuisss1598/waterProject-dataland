with src as (
    select
        dt::varchar as _pk,
        trim(left(dt_iso, length(dt_iso)-3))::timestamp as date_utc,
        timezone::varchar as timezone,
        'Richmond'::varchar as city_name,
        'Ca'::varchar as state_name,
        lat::varchar as latitude,
        lon::varchar as longitude,
        temp::float(2) as temperature_fahrenheit,
        visibility::varchar as visibility,
        dew_point::float(2) as dew_point,
        feels_like::float(2) as feels_like_f,
        temp_min::float(2) as temp_min_f,
        temp_max::float(2) as temp_max_f,
        pressure::int as pressure,
        sea_level,
        grnd_level,
        humidity::int as humidity,
        wind_speed::float(2) as wind_speed,
        wind_deg::int as wind_deg,
        wind_gust::float(2) as wind_gust,
        rain_1h::float(2) as rain_1h,
        rain_3h::float(2) as rain_3h,
        snow_1h::float(2) as snow_1h,
        snow_3h::float(2) as snow_3h,
        clouds_all as clouds_all,
        weather_id::varchar as weather_id,
        weather_main::varchar as weather_main,
        weather_description::varchar as weather_description,
        weather_icon::varchar as weather_icon
    from {{ source('open_weather', 'open_weather_api_historical_hourly') }}
)
select
    *
from src