with src as (

    select
        ingested_at,
        dt,
        hour,
        payload
    from {{ source('raw', 'ipma_observations_raw') }}

),

-- transforma o payload (varchar JSON) num map(varchar, json)
t1 as (

    select
        ingested_at,
        dt,
        hour,
        cast(json_parse(payload) as map(varchar, json)) as ts_map
    from src

),

-- explode timestamps (YYYY-mm-ddThh:mm)
t2 as (

    select
        ingested_at,
        dt,
        hour,
        ts_key as observed_at_str,
        ts_val as stations_json
    from t1
    cross join unnest(map_entries(ts_map)) as u(ts_key, ts_val)

),

-- stations_json é um objeto { "1210881": {...}, "1210883": null, ... }
t3 as (

    select
        ingested_at,
        dt,
        hour,
        from_iso8601_timestamp(observed_at_str) as observed_at,
        cast(stations_json as map(varchar, json)) as station_map
    from t2

),

-- explode stations
t4 as (

    select
        ingested_at,
        dt,
        hour,
        observed_at,
        cast(station_id as integer) as idEstacao,
        station_payload
    from t3
    cross join unnest(map_entries(station_map)) as s(station_id, station_payload)
    where station_payload is not null

)

select
    ingested_at,
    dt,
    hour,
    observed_at,

    idEstacao,

    try_cast(json_extract_scalar(station_payload, '$.intensidadeVentoKM') as double) as intensidadeVentoKM,
    try_cast(json_extract_scalar(station_payload, '$.temperatura') as double)          as temperatura,
    try_cast(json_extract_scalar(station_payload, '$.radiacao') as double)             as radiacao,

    try_cast(json_extract_scalar(station_payload, '$.idDireccVento') as integer)       as idDireccVento,
    try_cast(json_extract_scalar(station_payload, '$.precAcumulada') as double)        as precAcumulada,
    try_cast(json_extract_scalar(station_payload, '$.intensidadeVento') as double)     as intensidadeVento,
    try_cast(json_extract_scalar(station_payload, '$.humidade') as double)             as humidade,
    try_cast(json_extract_scalar(station_payload, '$.pressao') as double)              as pressao

from t4
;
