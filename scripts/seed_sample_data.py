"""
Sample Data Seeding Script
Populates the governance platform with realistic enterprise datasets,
lineage edges, schema versions, and dictionary terms for development/demo.

Run: python scripts/seed_sample_data.py
"""

import asyncio
import httpx
import uuid
from datetime import datetime, timedelta
import random

API_BASE = "http://localhost:8000/api/v1"
HEADERS = {
    "X-User-Email": "seeder@enterprise.com",
    "X-User-Role": "platform_admin",
    "Content-Type": "application/json"
}

# ── Sample Dataset Definitions ────────────────────────────────────────────────

DATASETS = [
    # Finance domain
    {
        "name": "transaction_ledger",
        "qualified_name": "finance.raw.transaction_ledger",
        "display_name": "Transaction Ledger",
        "description": "Raw financial transaction records from core banking system. Includes all debit/credit entries.",
        "source_system": "Kafka",
        "database_name": "finance",
        "schema_name": "raw",
        "table_name": "transaction_ledger",
        "node_type": "TABLE",
        "classification": "RESTRICTED",
        "owner_team": "Finance Engineering",
        "owner_email": "d.kim@enterprise.com",
        "domain": "Finance",
        "update_frequency": "REAL_TIME",
        "is_pii": False,
        "tags": ["finance", "raw", "transactions", "core-banking"],
        "dbt_model_path": None,
        "airflow_dag_id": "finance_kafka_consumer",
        "columns": [
            {"column_name": "transaction_id", "data_type": "VARCHAR(36)", "is_nullable": False, "is_primary_key": True, "business_definition": "Unique identifier for each financial transaction.", "ordinal_position": 1},
            {"column_name": "account_id", "data_type": "VARCHAR(20)", "is_nullable": False, "is_pii": True, "business_definition": "Account number of the originating account.", "ordinal_position": 2},
            {"column_name": "amount_usd", "data_type": "FLOAT", "is_nullable": False, "business_definition": "Transaction amount in US dollars.", "ordinal_position": 3},
            {"column_name": "transaction_date", "data_type": "DATE", "is_nullable": False, "business_definition": "Calendar date the transaction was posted.", "ordinal_position": 4},
            {"column_name": "transaction_type", "data_type": "VARCHAR(50)", "is_nullable": False, "business_definition": "Type: DEBIT, CREDIT, TRANSFER, FEE.", "ordinal_position": 5},
        ]
    },
    {
        "name": "daily_revenue_summary",
        "qualified_name": "finance.mart.daily_revenue_summary",
        "display_name": "Daily Revenue Summary",
        "description": "Aggregated daily revenue mart used for executive reporting and Finance P&L dashboard.",
        "source_system": "Snowflake",
        "database_name": "finance",
        "schema_name": "mart",
        "table_name": "daily_revenue_summary",
        "node_type": "TABLE",
        "classification": "CONFIDENTIAL",
        "owner_team": "Finance Analytics",
        "owner_email": "r.chen@enterprise.com",
        "domain": "Finance",
        "update_frequency": "DAILY",
        "is_pii": False,
        "tags": ["finance", "mart", "revenue", "certified", "executive"],
        "dbt_model_path": "models/marts/finance/daily_revenue_summary.sql",
        "airflow_dag_id": "finance_etl_daily",
        "columns": [
            {"column_name": "revenue_date", "data_type": "DATE", "is_nullable": False, "is_primary_key": True, "business_definition": "Calendar date for which revenue figures are aggregated.", "ordinal_position": 1},
            {"column_name": "gross_revenue_usd", "data_type": "FLOAT", "is_nullable": False, "business_definition": "Total revenue before deductions, in US dollars.", "ordinal_position": 2},
            {"column_name": "net_revenue_usd", "data_type": "FLOAT", "is_nullable": False, "business_definition": "Net revenue after refunds and chargebacks.", "ordinal_position": 3},
            {"column_name": "transaction_count", "data_type": "INTEGER", "is_nullable": False, "business_definition": "Number of transactions contributing to this day's revenue.", "ordinal_position": 4},
        ]
    },
    # Risk domain
    {
        "name": "risk_exposure_agg",
        "qualified_name": "risk.staging.risk_exposure_agg",
        "display_name": "Risk Exposure Aggregated",
        "description": "Aggregated counterparty risk exposure by entity and product type. Used in regulatory reporting.",
        "source_system": "Spark",
        "database_name": "risk",
        "schema_name": "staging",
        "table_name": "risk_exposure_agg",
        "node_type": "TABLE",
        "classification": "RESTRICTED",
        "owner_team": "Risk Analytics",
        "owner_email": "r.singh@enterprise.com",
        "domain": "Risk",
        "update_frequency": "DAILY",
        "is_pii": False,
        "tags": ["risk", "regulatory", "exposure", "basel-iii"],
        "dbt_model_path": "models/staging/risk/risk_exposure_agg.sql",
        "airflow_dag_id": "risk_batch_daily",
    },
    # Marketing domain
    {
        "name": "customer_360_profile",
        "qualified_name": "marketing.gold.customer_360_profile",
        "display_name": "Customer 360 Profile",
        "description": "Unified customer profile combining CRM, transactional, behavioral and preference data. Contains PII.",
        "source_system": "dbt",
        "database_name": "marketing",
        "schema_name": "gold",
        "table_name": "customer_360_profile",
        "node_type": "TABLE",
        "classification": "RESTRICTED",
        "owner_team": "Marketing Analytics",
        "owner_email": "s.agarwal@enterprise.com",
        "domain": "Marketing",
        "update_frequency": "DAILY",
        "is_pii": True,
        "tags": ["marketing", "gold", "customer", "pii", "crm"],
        "dbt_model_path": "models/gold/marketing/customer_360_profile.sql",
    },
    # ML domain
    {
        "name": "credit_scoring_features_v3",
        "qualified_name": "ml.features.credit_scoring_features_v3",
        "display_name": "Credit Scoring Features v3",
        "description": "Feature store dataset for the credit scoring ML model. Includes 48 engineered features.",
        "source_system": "Python",
        "database_name": "ml",
        "schema_name": "features",
        "table_name": "credit_scoring_features_v3",
        "node_type": "TABLE",
        "classification": "CONFIDENTIAL",
        "owner_team": "ML Engineering",
        "owner_email": "k.murphy@enterprise.com",
        "domain": "Credit",
        "update_frequency": "DAILY",
        "is_pii": False,
        "tags": ["ml", "features", "credit", "model-v3"],
    },
    # Operations
    {
        "name": "ops_pipeline_metrics",
        "qualified_name": "operations.mart.ops_pipeline_metrics",
        "display_name": "Ops Pipeline Metrics",
        "description": "Pipeline execution metrics: run times, SLA adherence, failure rates across all Airflow DAGs.",
        "source_system": "Airflow",
        "database_name": "operations",
        "schema_name": "mart",
        "table_name": "ops_pipeline_metrics",
        "node_type": "TABLE",
        "classification": "INTERNAL",
        "owner_team": "Data Platform",
        "owner_email": "platform@enterprise.com",
        "domain": "Operations",
        "update_frequency": "HOURLY",
        "is_pii": False,
        "tags": ["ops", "monitoring", "sla", "pipelines"],
        "airflow_dag_id": "ops_metrics_hourly",
    },
    # Reference
    {
        "name": "dim_products",
        "qualified_name": "reference.dim.dim_products",
        "display_name": "Product Dimension",
        "description": "Slowly changing dimension table for all financial products offered by the enterprise.",
        "source_system": "S3",
        "database_name": "reference",
        "schema_name": "dim",
        "table_name": "dim_products",
        "node_type": "TABLE",
        "classification": "INTERNAL",
        "owner_team": "Reference Data",
        "owner_email": "refdata@enterprise.com",
        "domain": "Reference",
        "update_frequency": "WEEKLY",
        "is_pii": False,
        "tags": ["dimension", "reference", "products", "scd"],
    },
    # Market Data
    {
        "name": "raw_fx_rates",
        "qualified_name": "market_data.raw.raw_fx_rates",
        "display_name": "Raw FX Rates",
        "description": "End-of-day FX exchange rates from Bloomberg. Used for currency conversion in downstream models.",
        "source_system": "API",
        "database_name": "market_data",
        "schema_name": "raw",
        "table_name": "raw_fx_rates",
        "node_type": "EXTERNAL",
        "classification": "INTERNAL",
        "owner_team": "Market Data",
        "owner_email": "marketdata@enterprise.com",
        "domain": "Market Data",
        "update_frequency": "DAILY",
        "is_pii": False,
        "tags": ["fx", "market-data", "bloomberg", "raw"],
    },
]

DICTIONARY_TERMS = [
    {
        "term": "Net Revenue",
        "business_definition": "Gross revenue minus refunds, chargebacks, and promotional discounts for a given period. The primary top-line metric for financial reporting.",
        "technical_definition": "SUM(amount_usd) WHERE transaction_type IN ('CREDIT') MINUS SUM(amount_usd) WHERE transaction_type IN ('REFUND', 'CHARGEBACK')",
        "domain": "Finance",
        "synonyms": ["Net Sales", "Revenue Net of Adjustments"],
        "steward": "r.chen@enterprise.com"
    },
    {
        "term": "Risk Exposure",
        "business_definition": "Maximum potential financial loss from a counterparty or portfolio position under stress scenario assumptions. Reported to regulators under Basel III framework.",
        "technical_definition": "SUM(notional_amount * risk_weight * probability_of_default) grouped by counterparty",
        "domain": "Risk",
        "synonyms": ["Credit Exposure", "Market Risk Exposure"],
        "steward": "r.singh@enterprise.com"
    },
    {
        "term": "Customer 360",
        "business_definition": "A unified view of a customer that combines their identity, transactional history, behavioral signals, and stated preferences into a single profile used for personalization and risk assessment.",
        "domain": "Marketing",
        "synonyms": ["Unified Customer Profile", "Golden Record"],
        "steward": "s.agarwal@enterprise.com"
    },
    {
        "term": "FX Rate",
        "business_definition": "The exchange rate between two currency pairs sourced from Bloomberg at end-of-day fixing. Used for converting multi-currency transactions to USD for consolidated reporting.",
        "domain": "Market Data",
        "synonyms": ["Exchange Rate", "Forex Rate"],
        "steward": "marketdata@enterprise.com"
    },
    {
        "term": "Governance Score",
        "business_definition": "A 0-100 composite score measuring the governance maturity of a data asset across five dimensions: documentation, ownership, classification, lineage registration, and profiling recency.",
        "domain": "Data Governance",
        "steward": "platform@enterprise.com"
    }
]


async def seed():
    async with httpx.AsyncClient(base_url=API_BASE, headers=HEADERS, timeout=30) as client:
        print("🌱 Seeding Enterprise Governance Platform...")
        print()

        # Register datasets
        dataset_ids = {}
        print("📦 Registering datasets...")
        for ds in DATASETS:
            try:
                r = await client.post("/datasets", json=ds)
                if r.status_code == 201:
                    data = r.json()
                    dataset_ids[ds["qualified_name"]] = data["id"]
                    print(f"  ✓ {ds['qualified_name']}")
                elif r.status_code == 409:
                    print(f"  ↷ {ds['qualified_name']} (already exists)")
                else:
                    print(f"  ✗ {ds['qualified_name']}: {r.text}")
            except Exception as e:
                print(f"  ✗ {ds['qualified_name']}: {e}")

        print()
        print("🔗 Registering lineage edges...")
        # Define lineage topology
        lineage_edges = [
            ("finance.raw.transaction_ledger", "finance.mart.daily_revenue_summary", "AGGREGATE"),
            ("market_data.raw.raw_fx_rates", "finance.mart.daily_revenue_summary", "JOIN"),
            ("reference.dim.dim_products", "finance.mart.daily_revenue_summary", "ENRICH"),
            ("finance.raw.transaction_ledger", "risk.staging.risk_exposure_agg", "AGGREGATE"),
            ("finance.raw.transaction_ledger", "ml.features.credit_scoring_features_v3", "FEATURE_ENGINEERING"),
            ("finance.mart.daily_revenue_summary", "operations.mart.ops_pipeline_metrics", "MONITOR"),
        ]

        for src_qname, tgt_qname, transform in lineage_edges:
            src_id = dataset_ids.get(src_qname)
            tgt_id = dataset_ids.get(tgt_qname)
            if src_id and tgt_id:
                r = await client.post("/lineage", json={
                    "source_dataset_id": src_id,
                    "target_dataset_id": tgt_id,
                    "transformation_type": transform
                })
                if r.status_code in (200, 201):
                    print(f"  ✓ {src_qname.split('.')[-1]} → {tgt_qname.split('.')[-1]}")
                else:
                    print(f"  ✗ Edge failed: {r.text[:80]}")

        print()
        print("📖 Seeding data dictionary...")
        for term in DICTIONARY_TERMS:
            r = await client.post("/dictionary", json=term)
            if r.status_code in (200, 201):
                print(f"  ✓ {term['term']}")

        print()
        print("✅ Seeding complete.")
        print()
        print("  → Platform UI:   http://localhost:3000")
        print("  → API Docs:      http://localhost:8000/api/docs")
        print("  → Neo4j Browser: http://localhost:7474")
        print("  → Airflow:       http://localhost:8080")


if __name__ == "__main__":
    asyncio.run(seed())
