{{ config(materialized='table', schema='delivery') }}

with ranked as (
  select
    station_id,
    station_name,
    longitude,
    latitude,
    dt,
    ingested_at,
    row_number() over (
      partition by station_id
      order by dt desc, ingested_at desc
    ) as rn
  from {{ ref('stg_ipma_stations') }}
)

select
  station_id,
  station_name,
  longitude,
  latitude,
  dt as valid_from_dt,
  cast(null as date) as valid_to_dt,
  true as is_current
from ranked
where rn = 1
