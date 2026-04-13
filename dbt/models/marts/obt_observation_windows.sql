{{ config(materialized='table') }}

with weather as (
    select
        forecast_hour,
        location_name,
        cloud_coverage,
        visibility_meters,
        temperature,
        humidity
    from {{ ref('int_weather_hourly') }}
),

celestial as (
    select * from {{ ref('int_celestial_combined') }}
),

-- CTE para detectar si el Sol está arriba en cada locación/hora
sun_status as (
    select 
        forecast_window_hour, 
        location_name, 
        altitude as sun_altitude
    from celestial
    where planet_id = 'sun'
),

final_scores as (
    select
        c.observation_timestamp as observation_time,
        c.location_name,
        c.planet_id,
        c.constellation_name,
        c.altitude,
        c.azimuth,
        w.cloud_coverage,
        w.visibility_meters,
        w.temperature,
        w.humidity,
        c.current_moon_phase,
        c.current_moon_phase_name,
        s.sun_altitude,
        
        -- Score de Nubes (40%)
        (100 - w.cloud_coverage) as cloud_score,
        
        -- Score de Visibilidad (30%) - Normalizado (10,000m / 100 = 100)
        (w.visibility_meters / 100.0) as visibility_score,
        
        -- Score de Altitud (30%) - Normalizado (90° = 100, pero usamos la escala de 100)
        (c.altitude / 90.0) * 100 as altitude_score
        
    from celestial c
    inner join weather w 
        on c.forecast_window_hour = w.forecast_hour 
        and c.location_name = w.location_name
    left join sun_status s
        on c.forecast_window_hour = s.forecast_window_hour
        and c.location_name = s.location_name
),

calculated_score as (
    select
        *,
        -- Lógica de Score 2.0
        case 
            -- 1. Si la altitud del cuerpo es < 10 grados, score 0
            when altitude < 10 then 0
            -- 2. Penalización Multiplicativa de Nubes: Si > 80%, score 0
            when cloud_coverage > 80 then 0
            -- 3. Filtro Solar: Si el Sol está arriba de -12 (Crepúsculo Náutico), score 0 para otros objetos
            when planet_id != 'sun' and sun_altitude > -12 then 0
            -- 4. Cálculo ponderado
            else (altitude_score * 0.3) + (visibility_score * 0.3) + (cloud_score * 0.4)
        end as final_observation_score
    from final_scores
)

select
    observation_time,
    location_name,
    planet_id,
    constellation_name,
    altitude,
    azimuth,
    cloud_coverage,
    visibility_meters,
    temperature,
    humidity,
    current_moon_phase,
    current_moon_phase_name,
    sun_altitude,
    final_observation_score as observation_score,
    case 
        when final_observation_score > 75 then 'Excelente'
        when final_observation_score > 50 then 'Bueno'
        when final_observation_score > 25 then 'Pobre'
        else 'No Recomendado'
    end as visibility_category
from calculated_score
order by observation_time, observation_score desc
