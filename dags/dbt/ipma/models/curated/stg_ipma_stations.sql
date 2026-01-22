{{ config(materialized='table') }}

with raw as (
  select
    ingested_at,
    dt,
    payload
  from {{ source('raw', 'ipma_stations_raw') }}
),

-- 1) dedup por dt: pega o payload mais recente do dia
dedup_dt as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by dt
        order by ingested_at desc
      ) as rn
    from raw
  )
  where rn = 1
),

-- 2) payload é um array de Features (json string)
expanded as (
  select
    d.ingested_at,
    d.dt,
    f
  from dedup_dt d
  cross join unnest(
    cast(json_parse(d.payload) as array(json))
  ) as t(f)
),

typed as (
  select
    ingested_at,
    dt,

    cast(json_extract_scalar(f, '$.properties.idEstacao') as integer) as station_id,
    json_extract_scalar(f, '$.properties.localEstacao') as station_name,

    cast(json_extract(f, '$.geometry.coordinates[0]') as double) as longitude,
    cast(json_extract(f, '$.geometry.coordinates[1]') as double) as latitude
  from expanded
  where json_extract_scalar(f, '$.properties.idEstacao') is not null
),

-- 3) dedup final por station_id (se vier repetido no mesmo payload/dia): mantém o mais recente
dedup_station as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by station_id
        order by ingested_at desc, dt desc
      ) as rn
    from typed
  )
  where rn = 1
)

select
  station_id,
  station_name,
  longitude,
  latitude,
  dt,
  ingested_at
from dedup_station
