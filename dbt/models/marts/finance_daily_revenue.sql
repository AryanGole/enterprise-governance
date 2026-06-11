-- dbt Model: mart.daily_revenue_summary (Gold layer)
{{ config(materialized="table", schema="mart", alias="daily_revenue_summary",
    tags=["mart","finance","gold","daily"],
    meta={"owner":"Finance Analytics","classification":"CONFIDENTIAL"}) }}
with enriched as (select * from {{ ref("int_revenue_enriched") }})
select transaction_date as revenue_date,
    sum(gross_revenue_usd) as gross_revenue_usd,
    sum(gross_revenue_usd) * 0.96 as net_revenue_usd,
    sum(transaction_count) as transaction_count,
    current_timestamp as dbt_updated_at
from enriched group by 1