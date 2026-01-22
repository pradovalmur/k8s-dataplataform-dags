{{ config(materialized='table', schema='delivery') }}

select
  dt,
  station_id,
  count(*) as hours_present,

  avg(temperatura) as temp_avg,
  min(temperatura) as temp_min,
  max(temperatura) as temp_max,

  sum(prec_acumulada) as prec_sum,
  avg(intensidade_vento) as wind_avg,
  avg(humidade) as hum_avg,
  avg(pressao) as pressure_avg,
  sum(radiacao) as radiation_sum

from {{ ref('fact_observation_hourly') }}
group by 1,2
