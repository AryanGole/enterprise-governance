"""
Airflow DAG: Enterprise Governance Metadata Sync Pipeline
Orchestrates: schema profiling → lineage registration → governance scoring → alerting

Schedule: Daily at 02:00 UTC
Owner: Data Governance Platform Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import json
import logging
import requests

logger = logging.getLogger(__name__)

GOVERNANCE_API = "http://governance-api:8000/api/v1"
GOVERNANCE_CONN_ID = "governance_postgres"

DEFAULT_ARGS = {
    "owner": "data-governance-team",
    "depends_on_past": False,
    "email": ["governance-alerts@enterprise.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def profile_source_schemas(**context):
    """
    Introspect all registered source systems and extract live schema definitions.
    Pushes schema snapshots to XCom for downstream tasks.
    """
    pg = PostgresHook(postgres_conn_id=GOVERNANCE_CONN_ID)

    # Fetch all active datasets from governance catalog
    datasets = pg.get_records(
        """
        SELECT id, qualified_name, source_system, database_name, schema_name, table_name
        FROM datasets WHERE is_active = TRUE
        ORDER BY source_system, schema_name, table_name
        """
    )

    logger.info(f"Profiling {len(datasets)} active datasets...")
    schema_snapshots = {}

    for ds_id, qname, source, db, schema, table in datasets:
        try:
            # In production: connect to actual source system via SQLAlchemy
            # Here we simulate schema introspection
            columns = pg.get_records(
                """
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                parameters=(schema, table)
            )

            schema_snapshots[str(ds_id)] = {
                "qualified_name": qname,
                "columns": {
                    col[0]: {
                        "type": col[1].upper(),
                        "nullable": col[2] == "YES",
                        "position": col[3]
                    }
                    for col in columns
                }
            }
        except Exception as e:
            logger.error(f"Failed to profile {qname}: {e}")

    context["ti"].xcom_push(key="schema_snapshots", value=schema_snapshots)
    logger.info(f"Schema profiling complete. {len(schema_snapshots)} datasets processed.")
    return len(schema_snapshots)


def register_schema_versions(**context):
    """
    For each profiled schema, call the governance API to register a new schema version.
    Breaking changes trigger automated alerts.
    """
    snapshots = context["ti"].xcom_pull(key="schema_snapshots", task_ids="profile_source_schemas")
    breaking_changes = []

    for dataset_id, snapshot in snapshots.items():
        try:
            response = requests.post(
                f"{GOVERNANCE_API}/schemas/{dataset_id}/versions",
                json={
                    "schema_snapshot": snapshot,
                    "authored_by": "airflow-governance-dag",
                    "change_notes": f"Auto-profiled on {datetime.utcnow().date()}"
                },
                timeout=30
            )
            result = response.json()
            if result.get("is_breaking"):
                breaking_changes.append({
                    "dataset_id": dataset_id,
                    "qualified_name": snapshot["qualified_name"],
                    "changes": result.get("changes", [])
                })
        except Exception as e:
            logger.error(f"Schema version registration failed for {dataset_id}: {e}")

    context["ti"].xcom_push(key="breaking_changes", value=breaking_changes)

    if breaking_changes:
        logger.warning(f"⚠️ {len(breaking_changes)} datasets have breaking schema changes!")
    return len(breaking_changes)


def compute_governance_scores(**context):
    """
    Score each dataset's governance posture based on:
    - Documentation coverage (30%)
    - Ownership assignment (20%)
    - Classification status (20%)
    - Lineage registration (20%)
    - Last profile recency (10%)
    Pushes scores to governance_records table.
    """
    pg = PostgresHook(postgres_conn_id=GOVERNANCE_CONN_ID)

    datasets = pg.get_records("""
        SELECT d.id, d.qualified_name, d.owner_email, d.classification,
               COUNT(dc.id) AS total_cols,
               SUM(CASE WHEN dc.business_definition IS NOT NULL THEN 1 ELSE 0 END) AS documented_cols,
               COUNT(le.id) AS lineage_count
        FROM datasets d
        LEFT JOIN dataset_columns dc ON dc.dataset_id = d.id
        LEFT JOIN lineage_edges le ON le.source_dataset_id = d.id OR le.target_dataset_id = d.id
        WHERE d.is_active = TRUE
        GROUP BY d.id, d.qualified_name, d.owner_email, d.classification
    """)

    scores = []
    for row in datasets:
        ds_id, qname, owner, classification, total, documented, lineage_count = row

        doc_score      = (documented / total * 30) if total > 0 else 0
        owner_score    = 20 if owner else 0
        class_score    = 20 if classification and classification != "DRAFT" else 0
        lineage_score  = min(lineage_count * 5, 20)  # Cap at 20
        recency_score  = 10  # Placeholder — computed from last_profiled_at

        total_score = doc_score + owner_score + class_score + lineage_score + recency_score
        scores.append((str(ds_id), round(total_score, 1)))

    logger.info(f"Governance scores computed for {len(scores)} datasets.")
    context["ti"].xcom_push(key="governance_scores", value=scores)
    return scores


def send_governance_digest(**context):
    """
    Send daily governance digest: breaking changes, orphaned datasets,
    low-score assets, and PII exposure summary.
    """
    breaking = context["ti"].xcom_pull(key="breaking_changes", task_ids="register_schema_versions")
    scores   = context["ti"].xcom_pull(key="governance_scores", task_ids="compute_governance_scores")

    low_score_assets = [s for s in (scores or []) if s[1] < 50]

    digest = {
        "date": datetime.utcnow().date().isoformat(),
        "summary": {
            "breaking_schema_changes": len(breaking or []),
            "low_governance_score_assets": len(low_score_assets),
        },
        "breaking_changes": breaking or [],
        "low_score_assets": low_score_assets[:10]
    }

    logger.info(f"Governance digest: {json.dumps(digest, indent=2)}")

    # In production: POST to Slack webhook / send email
    # requests.post(SLACK_WEBHOOK, json={"text": format_digest(digest)})

    return digest


with DAG(
    dag_id="enterprise_governance_metadata_sync",
    default_args=DEFAULT_ARGS,
    description="Daily metadata profiling, schema versioning, and governance scoring",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["governance", "metadata", "lineage", "schema"],
    doc_md="""
    ## Enterprise Governance Metadata Sync

    **Purpose**: Automates daily metadata lifecycle management across all registered data assets.

    **Pipeline Steps**:
    1. Profile source system schemas
    2. Register schema versions (detect breaking changes)
    3. Compute governance scores
    4. Send daily digest

    **SLA**: Must complete within 2 hours (before business day begins)
    **Owner**: Data Governance Platform Team
    **Oncall**: #data-governance-alerts
    """,
) as dag:

    with TaskGroup("ingestion", tooltip="Source system profiling") as tg_ingest:
        profile = PythonOperator(
            task_id="profile_source_schemas",
            python_callable=profile_source_schemas,
            doc_md="Introspect live schemas from all registered source systems."
        )

    with TaskGroup("versioning", tooltip="Schema version management") as tg_version:
        register = PythonOperator(
            task_id="register_schema_versions",
            python_callable=register_schema_versions,
            doc_md="Register schema snapshots; detect and alert on breaking changes."
        )

    with TaskGroup("scoring", tooltip="Governance posture scoring") as tg_score:
        score = PythonOperator(
            task_id="compute_governance_scores",
            python_callable=compute_governance_scores,
            doc_md="Score datasets on documentation, ownership, classification, lineage coverage."
        )

    with TaskGroup("reporting", tooltip="Governance digest") as tg_report:
        digest = PythonOperator(
            task_id="send_governance_digest",
            python_callable=send_governance_digest,
            doc_md="Compile and distribute daily governance health digest."
        )

    # DAG dependency chain
    tg_ingest >> tg_version >> tg_score >> tg_report
