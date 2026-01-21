{{ config(materialized='table') }}

with src as (
    select
        ingested_at,
        dt,
        hour,
        observations
    from {{ source('raw', 'ipma_observations_raw') }}
),

-- observations é VARCHAR/JSON: {"2026-01-19T00:00": {"1210881": {...}, "1210883": {...}}}
parsed as (
    select
        ingested_at,
        dt,
        hour,
        cast(json_parse(observations) as map(varchar, json)) as by_ts
    from src
),

by_timestamp as (
    select
        ingested_at,
        dt,
        hour,
        ts_key,
        cast(ts_val as map(varchar, json)) as by_station
    from parsed
    cross join unnest(map_entries(by_ts)) as t(ts_key, ts_val)
),

by_station as (
    select
        ingested_at,
        dt,
        hour,
        ts_key,
        station_id,
        station_json
    from by_timestamp
    cross join unnest(map_entries(by_station)) as s(station_id, station_json)
)

select
    ingested_at,
    dt,
    hour,
    ts_key as observation_ts,
    cast(station_id as varchar) as station_id,

    try_cast(json_extract_scalar(station_json, '$.temperatura') as double)            as temperatura,
    try_cast(json_extract_scalar(station_json, '$.humidade') as double)               as humidade,
    try_cast(json_extract_scalar(station_json, '$.pressao') as double)                as pressao,
    try_cast(json_extract_scalar(station_json, '$.radiacao') as double)               as radiacao,
    try_cast(json_extract_scalar(station_json, '$.intensidadeVento') as double)       as intensidade_vento,
    try_cast(json_extract_scalar(station_json, '$.intensidadeVentoKM') as double)     as intensidade_vento_km,
    try_cast(json_extract_scalar(station_json, '$.idDireccVento') as integer)         as id_direcc_vento,
    try_cast(json_extract_scalar(station_json, '$.precAcumulada') as double)          as prec_acumulada

from by_station
;
