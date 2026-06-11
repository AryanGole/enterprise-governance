"""
Airflow DAG: OpenLineage Event Ingestion
Polls Marquez API for new lineage events and syncs them into the
governance platform's PostgreSQL + Neo4j stores.

Schedule: Every 15 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

logger = logging.getLogger(__name__)

MARQUEZ_URL = "http://marquez:5000"
GOVERNANCE_API = "http://governance-api:8000/api/v1"

DEFAULT_ARGS = {
    "owner": "data-governance-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def poll_openlineage_events(**context):
    """
    Fetch recent OpenLineage events from Marquez.
    Translate job runs to governance lineage edges.
    """
    try:
        # Fetch recent job runs from Marquez
        resp = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/enterprise-governance/runs",
            params={"limit": 50},
            timeout=10
        )
        runs = resp.json().get("runs", [])
        logger.info(f"Fetched {len(runs)} OpenLineage runs from Marquez.")

        # Push to XCom for next task
        context["ti"].xcom_push(key="runs", value=runs)
        return len(runs)
    except Exception as e:
        logger.warning(f"Marquez poll failed (expected in local dev): {e}")
        return 0


def sync_lineage_to_governance(**context):
    """
    For each OpenLineage run, register lineage edges
    between input and output datasets in the governance API.
    """
    runs = context["ti"].xcom_pull(key="runs", task_ids="poll_openlineage_events") or []
    registered = 0

    for run in runs:
        inputs  = run.get("inputVersions", [])
        outputs = run.get("outputVersions", [])

        for inp in inputs:
            for out in outputs:
                try:
                    resp = requests.post(
                        f"{GOVERNANCE_API}/lineage",
                        json={
                            "source_qualified_name": f"{inp['namespaceName']}.{inp['datasetName']}",
                            "target_qualified_name": f"{out['namespaceName']}.{out['datasetName']}",
                            "transformation_type": "OPENLINEAGE",
                            "openlineage_run_id": run.get("id")
                        },
                        headers={"X-User-Email": "airflow-openlineage-dag@enterprise.com"},
                        timeout=10
                    )
                    if resp.status_code in (200, 201, 409):
                        registered += 1
                except Exception as e:
                    logger.error(f"Failed to sync lineage edge: {e}")

    logger.info(f"Synced {registered} lineage edges from OpenLineage events.")
    return registered


with DAG(
    dag_id="openlineage_event_ingestion",
    default_args=DEFAULT_ARGS,
    description="Polls Marquez for OpenLineage events and syncs to governance platform",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["governance", "lineage", "openlineage", "marquez"],
    max_active_runs=1,
) as dag:

    poll = PythonOperator(
        task_id="poll_openlineage_events",
        python_callable=poll_openlineage_events,
        doc_md="Poll Marquez API for recent OpenLineage job run events."
    )

    sync = PythonOperator(
        task_id="sync_lineage_to_governance",
        python_callable=sync_lineage_to_governance,
        doc_md="Translate OpenLineage events to governance platform lineage edges."
    )

    poll >> sync
