{{ config(materialized='table') }}

with raw as (
  select
    ingested_at,
    dt,
    hour,
    payload
  from {{ source('raw', 'ipma_observations_raw') }}
),

-- 1) se a duplicação é no nível dt+hour (mesma hora chega mais de 1x),
--    pegue só o payload mais recente
dedup_dt_hour as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by dt, hour
        order by ingested_at desc
      ) as rn
    from raw
  )
  where rn = 1
),

-- 2) abre o payload:
-- payload é JSON string tipo: {"2026-01-21T18:00": {"1210881": {...}, "1210883": null, ...}}
expanded as (
  select
    d.ingested_at,
    d.dt,
    d.hour,
    ts_key as observed_at_str,
    station_id_str,
    station_obj
  from dedup_dt_hour d
  cross join unnest(
    cast(json_parse(d.payload) as map(varchar, json))
  ) as t(ts_key, stations_json)
  cross join unnest(
    cast(stations_json as map(varchar, json))
  ) as s(station_id_str, station_obj)
  where station_obj is not null
),

typed as (
  select
    ingested_at,
    dt,
    hour,
    from_iso8601_timestamp(observed_at_str) as observed_at,
    cast(station_id_str as integer) as station_id,

    cast(json_extract_scalar(station_obj, '$.temperatura') as double)          as temperatura,
    cast(json_extract_scalar(station_obj, '$.humidade') as double)             as humidade,
    cast(json_extract_scalar(station_obj, '$.pressao') as double)              as pressao,
    cast(json_extract_scalar(station_obj, '$.radiacao') as double)             as radiacao,
    cast(json_extract_scalar(station_obj, '$.precAcumulada') as double)        as prec_acumulada,
    cast(json_extract_scalar(station_obj, '$.intensidadeVento') as double)     as intensidade_vento,
    cast(json_extract_scalar(station_obj, '$.intensidadeVentoKM') as double)   as intensidade_vento_km,
    cast(json_extract_scalar(station_obj, '$.idDireccVento') as integer)       as id_direcc_vento
  from expanded
),

-- 3) garantia final: 1 linha por station_id + observed_at (+dt/hour se quiser)
dedup_station_hour as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by station_id, observed_at
        order by ingested_at desc
      ) as rn
    from typed
  )
  where rn = 1
)

select
  station_id,
  observed_at,
  dt,
  hour,
  temperatura,
  humidade,
  pressao,
  radiacao,
  prec_acumulada,
  intensidade_vento,
  intensidade_vento_km,
  id_direcc_vento,
  ingested_at
from dedup_station_hour
;
