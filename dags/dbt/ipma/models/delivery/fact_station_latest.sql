{{ config(materialized='table', schema='delivery') }}

with ranked as (
  select
    *,
    row_number() over (
      partition by station_id
      order by ts_hour desc, ingested_at desc
    ) as rn
  from {{ ref('fact_observation_enriched') }}
)

select
  *
from ranked
where rn = 1
