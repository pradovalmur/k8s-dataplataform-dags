{{ config(materialized='view', schema='delivery') }}

with f as (
  select * from {{ ref('fact_observation_hourly') }}
),
d as (
  select * from {{ ref('dim_station') }}
)

select
  f.*,
  d.station_name,
  d.longitude,
  d.latitude,

  case f.id_direcc_vento
    when 0 then null
    when 1 then 'N'
    when 2 then 'NE'
    when 3 then 'E'
    when 4 then 'SE'
    when 5 then 'S'
    when 6 then 'SW'
    when 7 then 'W'
    when 8 then 'NW'
    when 9 then 'N'
    else null
  end as wind_dir_label,

  case when f.prec_acumulada is not null and f.prec_acumulada > 0 then true else false end as has_precip
from f
left join d
  on f.station_id = d.station_id
