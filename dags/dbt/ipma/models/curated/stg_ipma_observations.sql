{{ config(materialized='table') }}

with src as (
  select
    ingested_at,
    cast(dt as date) as dt,
    payload
  from {{ source('raw', 'ipma_observations_raw') }}
),

-- payload = {"2026-01-21T18:00": {"1210881": {...}, "1210883": null, ...}, ...}
payload_map as (
  select
    ingested_at,
    cast(json_parse(payload) as map(varchar, json)) as ts_map
  from src
),

-- 1) explode timestamps (só 1 coluna: ts_key)
ts_rows as (
  select
    p.ingested_at,
    ts_key,
    element_at(p.ts_map, ts_key) as stations_json
  from payload_map p
  cross join unnest(map_keys(p.ts_map)) as u(ts_key)
),

typed_ts as (
  select
    ingested_at,
    from_iso8601_timestamp(ts_key) as observation_ts,
    cast(date(from_iso8601_timestamp(ts_key)) as date) as obs_dt,
    hour(from_iso8601_timestamp(ts_key)) as hour_of_day,
    cast(stations_json as map(varchar, json)) as stations_map
  from ts_rows
),

-- 2) explode stations (só 1 coluna: station_key)
station_rows as (
  select
    t.ingested_at,
    t.observation_ts,
    t.obs_dt as dt,
    t.hour_of_day,
    cast(station_key as bigint) as station_id,
    element_at(t.stations_map, station_key) as station_obj
  from typed_ts t
  cross join unnest(map_keys(t.stations_map)) as u(station_key)
  where element_at(t.stations_map, station_key) is not null
),

typed as (
  select
    ingested_at,
    dt,
    hour_of_day,
    observation_ts,
    station_id,

    cast(json_extract_scalar(station_obj, '$.intensidadeVentoKM') as double) as intensidade_vento_km,
    cast(json_extract_scalar(station_obj, '$.temperatura') as double)          as temperatura,
    cast(json_extract_scalar(station_obj, '$.radiacao') as double)             as radiacao,
    cast(json_extract_scalar(station_obj, '$.idDireccVento') as integer)       as id_direcc_vento,
    cast(json_extract_scalar(station_obj, '$.precAcumulada') as double)        as prec_acumulada,
    cast(json_extract_scalar(station_obj, '$.intensidadeVento') as double)     as intensidade_vento,
    cast(json_extract_scalar(station_obj, '$.humidade') as double)             as humidade,
    cast(json_extract_scalar(station_obj, '$.pressao') as double)              as pressao
  from station_rows
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
    from typed
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
;
