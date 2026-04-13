with raw_forecast as (
    select * from {{ source('astronomy_raw', 'forecast') }}
),

refined as (
    select
        (CAST(dt_txt AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Asuncion')::TIMESTAMP as forecast_timestamp,
        case 
            when contains(lower(trim(replace(ubicacion, '_', ' '))), 'muci') then 'planetario san cosmos muci'
            when contains(lower(trim(replace(ubicacion, '_', ' '))), 'mbaracayu') then 'reserva natural mbaracayu'
            when contains(lower(trim(replace(ubicacion, '_', ' '))), 'san cosme') then 'planetario san cosme y damian'
            else lower(trim(replace(ubicacion, '_', ' ')))
        end as location_name,
        cast(main->>'$.temp' as float) as temperature,
        cast(main->>'$.humidity' as float) as humidity,
        cast(main->>'$.pressure' as float) as pressure,
        cast(clouds->>'$.all' as integer) as cloud_coverage,
        cast(visibility as float) as visibility_meters,
        current_timestamp as loaded_at
    from raw_forecast
)

select * from refined
