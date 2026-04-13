with raw_celestial as (
    select 
        planet_id,
        lower(trim(replace(ubicacion, '_', ' '))) as loc_raw,
        unnest(cast(cells as JSON[])) as cell
    from {{ source('astronomy_raw', 'celestial_bodies') }}
),

renamed as (
    select
        planet_id,
        case 
            when contains(loc_raw, 'muci') then 'planetario san cosmos muci'
            when contains(loc_raw, 'mbaracayu') then 'reserva natural mbaracayu'
            when contains(loc_raw, 'san cosme') then 'planetario san cosme y damian'
            else loc_raw
        end as location_name,
        (cast(cell->>'$.date' AS TIMESTAMPTZ) AT TIME ZONE 'America/Asuncion')::TIMESTAMP as observation_timestamp,
        cast(cell->'$.position'->'$.horizontal'->'$.altitude'->>'$.degrees' as float) as altitude,
        cast(cell->'$.position'->'$.horizontal'->'$.azimuth'->>'$.degrees' as float) as azimuth,
        upper(cell->'$.position'->'$.constellation'->>'$.name') as constellation_name,
        cast(cell->'$.extraInfo'->'$.phase'->>'$.fraction' as float) as moon_phase_fraction,
        cell->'$.extraInfo'->'$.phase'->>'$.string' as moon_phase_name
    from raw_celestial
    where planet_id != 'earth'
)

select
    *,
    current_timestamp as loaded_at
from renamed
