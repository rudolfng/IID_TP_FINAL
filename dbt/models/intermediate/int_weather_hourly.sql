with weather as (
    select
        date_trunc('hour', forecast_timestamp) as forecast_hour,
        location_name,
        cloud_coverage,
        visibility_meters,
        temperature,
        humidity
    from {{ ref('stg_weather') }}
)

select * from weather
