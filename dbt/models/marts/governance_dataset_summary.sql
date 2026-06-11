-- ============================================================
-- dbt Model: governance_mart.governance_dataset_summary
-- Layer: Gold / Mart
-- Materialization: Table (refreshed daily)
-- Owner: Data Governance Platform Team
-- ============================================================
-- Business Context:
--   Central reporting model for the governance dashboard.
--   Joins dataset metadata, schema versions, lineage edges,
--   and column profiles to produce a single denormalized
--   governance view used by the React frontend and BI tools.
-- ============================================================

{{ config(
    materialized='table',
    schema='governance_mart',
    alias='governance_dataset_summary',
    tags=['governance', 'daily', 'gold'],
    meta={
        'owner': 'data-governance-team',
        'classification': 'INTERNAL',
        'sla': 'by 06:00 UTC',
        'downstream': ['governance_dashboard', 'data_catalog_api']
    }
) }}

with datasets as (
    select
        d.id                                                    as dataset_id,
        d.qualified_name,
        d.display_name,
        d.source_system,
        d.database_name,
        d.schema_name,
        d.table_name,
        d.domain,
        d.classification,
        d.governance_status,
        d.owner_team,
        d.owner_email,
        d.update_frequency,
        d.node_type,
        d.is_pii,
        d.tags,
        d.dbt_model_path,
        d.airflow_dag_id,
        d.row_count,
        d.created_at                                            as dataset_registered_at,
        d.last_profiled_at
    from {{ source('governance_raw', 'datasets') }} d
    where d.is_active = true
),

column_stats as (
    select
        dc.dataset_id,
        count(dc.id)                                            as total_columns,
        sum(case when dc.business_definition is not null then 1 else 0 end)
                                                                as documented_columns,
        sum(case when dc.is_pii = true then 1 else 0 end)      as pii_columns,
        sum(case when dc.is_primary_key = true then 1 else 0 end) as pk_columns,
        sum(case when dc.is_nullable = false then 1 else 0 end) as non_nullable_columns,
        round(
            sum(case when dc.business_definition is not null then 1.0 else 0 end)
            / nullif(count(dc.id), 0) * 100, 1
        )                                                       as documentation_coverage_pct
    from {{ source('governance_raw', 'dataset_columns') }} dc
    group by dc.dataset_id
),

schema_version_stats as (
    select
        sv.dataset_id,
        max(sv.version)                                         as latest_version,
        count(sv.id)                                            as total_versions,
        sum(case when sv.is_breaking = true then 1 else 0 end) as breaking_version_count,
        max(sv.created_at)                                      as last_schema_change_at
    from {{ source('governance_raw', 'schema_versions') }} sv
    group by sv.dataset_id
),

lineage_stats as (
    select
        dataset_id,
        sum(upstream_count)     as upstream_sources,
        sum(downstream_count)   as downstream_consumers
    from (
        select target_dataset_id as dataset_id, count(*) as upstream_count, 0 as downstream_count
        from {{ source('governance_raw', 'lineage_edges') }} where is_active = true
        group by target_dataset_id

        union all

        select source_dataset_id as dataset_id, 0 as upstream_count, count(*) as downstream_count
        from {{ source('governance_raw', 'lineage_edges') }} where is_active = true
        group by source_dataset_id
    ) t
    group by dataset_id
),

-- ── Governance Score Computation ─────────────────────────────────────────────
-- Scoring rubric (100 pts total):
--   Documentation coverage : up to 30 pts
--   Owner assigned          : 20 pts
--   Classification set      : 20 pts
--   Lineage registered      : up to 20 pts
--   Profiled in last 7 days : 10 pts

governance_scores as (
    select
        d.dataset_id,
        -- Documentation
        least(coalesce(cs.documentation_coverage_pct, 0) * 0.30, 30) as doc_score,
        -- Ownership
        case when d.owner_email is not null then 20.0 else 0 end      as owner_score,
        -- Classification
        case when d.classification not in ('DRAFT', '') then 20.0 else 0 end as class_score,
        -- Lineage
        least(coalesce(ls.upstream_sources, 0) + coalesce(ls.downstream_consumers, 0), 4) * 5.0
                                                                       as lineage_score,
        -- Recency
        case
            when d.last_profiled_at >= current_timestamp - interval '7 days' then 10.0
            when d.last_profiled_at >= current_timestamp - interval '30 days' then 5.0
            else 0
        end                                                            as recency_score
    from datasets d
    left join column_stats cs on cs.dataset_id = d.dataset_id
    left join lineage_stats ls on ls.dataset_id = d.dataset_id
),

final as (
    select
        d.dataset_id,
        d.qualified_name,
        d.display_name,
        d.source_system,
        d.database_name,
        d.schema_name,
        d.table_name,
        d.domain,
        d.classification,
        d.governance_status,
        d.owner_team,
        d.owner_email,
        d.update_frequency,
        d.node_type,
        d.is_pii,
        d.tags,
        d.dbt_model_path,
        d.airflow_dag_id,
        d.row_count,
        d.dataset_registered_at,
        d.last_profiled_at,

        -- Column stats
        coalesce(cs.total_columns, 0)            as total_columns,
        coalesce(cs.documented_columns, 0)       as documented_columns,
        coalesce(cs.pii_columns, 0)              as pii_columns,
        coalesce(cs.pk_columns, 0)               as pk_columns,
        coalesce(cs.documentation_coverage_pct, 0) as documentation_coverage_pct,

        -- Schema evolution
        coalesce(sv.latest_version, 1)           as schema_version,
        coalesce(sv.total_versions, 0)           as schema_version_count,
        coalesce(sv.breaking_version_count, 0)   as breaking_schema_changes,
        sv.last_schema_change_at,

        -- Lineage
        coalesce(ls.upstream_sources, 0)         as upstream_source_count,
        coalesce(ls.downstream_consumers, 0)     as downstream_consumer_count,
        case
            when coalesce(ls.upstream_sources, 0) + coalesce(ls.downstream_consumers, 0) = 0
            then true else false
        end                                      as is_lineage_orphan,

        -- Governance score
        round(
            gs.doc_score + gs.owner_score + gs.class_score +
            gs.lineage_score + gs.recency_score, 1
        )                                        as governance_score,
        case
            when (gs.doc_score + gs.owner_score + gs.class_score + gs.lineage_score + gs.recency_score) >= 80
                 then 'PLATINUM'
            when (gs.doc_score + gs.owner_score + gs.class_score + gs.lineage_score + gs.recency_score) >= 60
                 then 'GOLD'
            when (gs.doc_score + gs.owner_score + gs.class_score + gs.lineage_score + gs.recency_score) >= 40
                 then 'SILVER'
            else 'BRONZE'
        end                                      as governance_tier,

        -- Audit metadata
        current_timestamp                        as dbt_updated_at,
        '{{ run_started_at }}'                   as dbt_run_started_at

    from datasets d
    left join column_stats cs        on cs.dataset_id  = d.dataset_id
    left join schema_version_stats sv on sv.dataset_id = d.dataset_id
    left join lineage_stats ls        on ls.dataset_id = d.dataset_id
    left join governance_scores gs    on gs.dataset_id = d.dataset_id
)

select * from final
