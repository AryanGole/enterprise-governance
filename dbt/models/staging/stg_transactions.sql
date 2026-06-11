-- dbt Model: staging.stg_transactions
-- Source: finance.raw.transaction_ledger | Applies FX conversion + PII masking
{{ config(materialized="view", schema="staging", tags=["staging","finance","daily"]) }}
with source as (select * from {{ source("finance_raw", "transaction_ledger") }} where transaction_date >= current_date - interval "2 years"),
fx_rates as (select currency_pair, rate_to_usd from {{ source("market_data_raw", "raw_fx_rates") }} where rate_date = current_date - 1),
cleaned as (
    select t.transaction_id, md5(t.account_id::text) as account_id_masked,
        t.amount_usd, coalesce(t.amount_usd / nullif(fx.rate_to_usd,0), t.amount_usd) as amount_local_currency,
        coalesce(t.currency_code,"USD") as currency_code,
        t.transaction_date::date as transaction_date, t.transaction_type,
        current_timestamp as dbt_loaded_at
    from source t left join fx_rates fx on t.currency_code = split_part(fx.currency_pair,"/",1)
    where t.transaction_id is not null and t.amount_usd is not null
)
select * from cleaned