"""Impact Analysis Router — Downstream dependency assessment."""

from fastapi import APIRouter, Query
from core.neo4j_client import get_impact_assessment

router = APIRouter()


@router.get("/{dataset_id}", summary="Downstream impact assessment")
async def impact_analysis(
    dataset_id: str,
    include_dashboards: bool = Query(True),
    include_ml_models: bool = Query(True)
):
    """
    Assess the blast radius of a schema change or pipeline failure.
    Returns all downstream datasets, their domains, and risk classification.
    """
    impact = await get_impact_assessment(dataset_id)

    # Classify risk based on affected assets
    affected_count = impact["affected_count"]
    risk_level = (
        "CRITICAL" if affected_count > 50 else
        "HIGH"     if affected_count > 20 else
        "MEDIUM"   if affected_count > 5  else
        "LOW"
    )

    try:
        recommendation = _recommendation(risk_level)
    except KeyError:
        recommendation = f"Assess {affected_count} affected downstream assets and coordinate with owners."

    return {
        **impact,
        "risk_level": risk_level,
        "recommendation": recommendation
    }


def _recommendation(risk: str) -> str:
    """Return governance action for a risk level. Raises KeyError on unknown levels."""
    return {
        "CRITICAL": "Freeze deployments. Notify all downstream owners. Convene incident bridge.",
        "HIGH":     "Notify downstream team leads. Stage rollout with validation gates.",
        "MEDIUM":   "Coordinate with downstream owners. Run regression tests before deploy.",
        "LOW":      "Standard deployment process. Monitor downstream pipeline SLAs post-deploy."
    }[risk]
