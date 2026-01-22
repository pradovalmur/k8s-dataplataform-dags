{{ config(materialized='table', schema='delivery') }}

select
  ingested_at,
  observation_ts as ts_hour,
  dt,
  hour_of_day as hour,
  station_id,

  nullif(temperatura, -99.0) as temperatura,
  nullif(humidade, -99.0) as humidade,
  nullif(pressao, -99.0) as pressao,
  nullif(radiacao, -99.0) as radiacao,
  nullif(prec_acumulada, -99.0) as prec_acumulada,
  nullif(intensidade_vento, -99.0) as intensidade_vento,
  nullif(intensidade_vento_km, -99.0) as intensidade_vento_km,
  nullif(id_direcc_vento, -99) as id_direcc_vento

from {{ ref('stg_ipma_observations') }}
