{{ config(materialized='view') }}

with src as (

    select
        ingested_at,
        dt,
        hour,
        payload as payload_json_str
    from {{ source('ipma', 'observations') }}

),

by_ts as (

    select
        ingested_at,
        dt,
        hour,
        cast(json_parse(payload_json_str) as map(varchar, json)) as ts_map
    from src

),

ts_rows as (

    select
        ingested_at,
        dt,
        hour,
        ts_key,
        ts_value
    from by_ts
    cross join unnest(map_entries(ts_map)) as t(ts_key, ts_value)

),

by_station as (

    select
        ingested_at,
        dt,
        hour,
        ts_key,
        cast(ts_value as map(varchar, json)) as station_map
    from ts_rows

),

station_rows as (

    select
        ingested_at,
        dt,
        hour,
        ts_key,
        station_id,
        measures
    from by_station
    cross join unnest(map_entries(station_map)) as s(station_id, measures)

)

select
    ingested_at,
    dt,
    hour,

    -- timestamp da observação vindo da chave do JSON (ex: "2026-01-19T00:00" ou "2026-01-18T07:00")
    coalesce(
        try(from_iso8601_timestamp(ts_key)),
        try(date_parse(ts_key, '%Y-%m-%dT%H:%i'))
    ) as observation_ts,

    cast(station_id as bigint) as station_id,

    -- campos (ajuste/adicione conforme seu JSON)
    cast(json_extract_scalar(measures, '$.temperatura')          as double) as temperatura,
    cast(json_extract_scalar(measures, '$.humidade')             as double) as humidade,
    cast(json_extract_scalar(measures, '$.pressao')              as double) as pressao,
    cast(json_extract_scalar(measures, '$.radiacao')             as double) as radiacao,
    cast(json_extract_scalar(measures, '$.precAcumulada')        as double) as prec_acumulada,
    cast(json_extract_scalar(measures, '$.intensidadeVento')      as double) as intensidade_vento,
    cast(json_extract_scalar(measures, '$.intensidadeVentoKM')    ass double) as intensidade_vento_km,
    cast(json_extract_scalar(measures, '$.idDireccVento')         as integer) as id_direcc_vento,

    measures as measures_json

from station_rows
;
