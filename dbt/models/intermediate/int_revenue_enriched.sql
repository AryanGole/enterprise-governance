-- dbt Model: intermediate.int_revenue_enriched
{{ config(materialized="ephemeral", tags=["intermediate","finance"]) }}
with txns as (select * from {{ ref("stg_transactions") }}),
products as (select * from {{ source("reference_dim", "dim_products") }}),
enriched as (
    select t.transaction_date, t.currency_code, p.product_category,
        sum(t.amount_usd) as gross_revenue_usd, count(t.transaction_id) as transaction_count
    from txns t left join products p on t.product_id = p.product_id
    where t.transaction_type = "CREDIT" group by 1,2,3
)
select * from enriched