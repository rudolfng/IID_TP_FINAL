with celestial as (
    select
        observation_timestamp,
        -- Alineamos la hora de observación al bloque de 3 horas de OpenWeather
        date_trunc('day', observation_timestamp) + 
            interval (floor(extract(hour from observation_timestamp) / 3) * 3) hour as forecast_window_hour,
        location_name,
        planet_id,
        altitude,
        azimuth,
        constellation_name,
        moon_phase_fraction,
        moon_phase_name
    from {{ ref('stg_celestial_bodies') }}
),

moon_info as (
    select 
        forecast_window_hour,
        location_name,
        moon_phase_fraction,
        moon_phase_name
    from celestial
    where planet_id = 'moon'
)

select
    c.*,
    m.moon_phase_fraction as current_moon_phase,
    m.moon_phase_name as current_moon_phase_name
from celestial c
left join moon_info m
    on c.forecast_window_hour = m.forecast_window_hour
    and c.location_name = m.location_name
