{{ config(materialized='table') }}

with raw as (
  select
    ingested_at,
    payload
  from {{ source('raw', 'ipma_observations_raw') }}
),

-- json -> map(varchar, json)
payload_map as (
  select
    ingested_at,
    cast(json_parse(payload) as map(varchar, json)) as m
  from raw
),

-- explode timestamps (key = '2026-01-21T18:00', value = json do mapa de estações)
expanded_ts as (
  select
    p.ingested_at,
    e.key   as ts_key,
    e.value as ts_obj
  from payload_map p
  cross join unnest(map_entries(p.m)) as u(e)
),

typed_ts as (
  select
    ingested_at,
    from_iso8601_timestamp(ts_key) as observation_ts,
    cast(date(from_iso8601_timestamp(ts_key)) as date) as dt,
    hour(from_iso8601_timestamp(ts_key)) as hour_of_day,
    cast(ts_obj as map(varchar, json)) as stations_map
  from expanded_ts
),

expanded_station as (
  select
    t.ingested_at,
    t.observation_ts,
    t.dt,
    t.hour_of_day,
    s.key   as station_key,
    s.value as station_obj
  from typed_ts t
  cross join unnest(map_entries(t.stations_map)) as u(s)
  where station_obj is not null
),

final_typed as (
  select
    ingested_at,
    dt,
    hour_of_day,
    observation_ts,
    cast(station_key as bigint) as station_id,

    cast(json_extract_scalar(station_obj, '$.intensidadeVentoKM') as double) as intensidade_vento_km,
    cast(json_extract_scalar(station_obj, '$.temperatura') as double)          as temperatura,
    cast(json_extract_scalar(station_obj, '$.radiacao') as double)             as radiacao,
    cast(json_extract_scalar(station_obj, '$.idDireccVento') as integer)       as id_direcc_vento,
    cast(json_extract_scalar(station_obj, '$.precAcumulada') as double)        as prec_acumulada,
    cast(json_extract_scalar(station_obj, '$.intensidadeVento') as double)     as intensidade_vento,
    cast(json_extract_scalar(station_obj, '$.humidade') as double)             as humidade,
    cast(json_extract_scalar(station_obj, '$.pressao') as double)              as pressao
  from expanded_station
),

dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by station_id, observation_ts
        order by ingested_at desc
      ) as rn
    from final_typed
  )
  where rn = 1
)

select
  station_id,
  dt,
  hour_of_day,
  observation_ts,
  intensidade_vento_km,
  temperatura,
  radiacao,
  id_direcc_vento,
  prec_acumulada,
  intensidade_vento,
  humidade,
  pressao,
  ingested_at
from dedup
