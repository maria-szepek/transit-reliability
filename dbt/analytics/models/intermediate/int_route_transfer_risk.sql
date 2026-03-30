{{ config(materialized='table') }}

select
    route_id,
    avg(risk_score) as avg_transfer_risk,
    count(*) as transfer_count
from {{ ref('int_transfer_risk') }}
group by route_id