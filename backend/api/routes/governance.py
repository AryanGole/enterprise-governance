"""Governance Router"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from core.database import get_db
from models.models import GovernanceRecord, Dataset, GovernanceStatus

router = APIRouter()


@router.get("/summary", summary="Governance posture summary")
async def governance_summary(db: AsyncSession = Depends(get_db)):
    stmt = select(Dataset.governance_status, func.count(Dataset.id)).group_by(Dataset.governance_status)
    result = await db.execute(stmt)
    rows = result.all()
    status_counts = {
        (row[0].value if hasattr(row[0], "value") else str(row[0])): row[1]
        for row in rows
    }
    total = sum(status_counts.values())
    approved = status_counts.get("APPROVED", 0)
    return {
        "total_datasets":    total,
        "approved":          approved,
        "pending_review":    status_counts.get("PENDING_REVIEW", 0),
        "draft":             status_counts.get("DRAFT", 0),
        "rejected":          status_counts.get("REJECTED", 0),      # FIX: was missing
        "deprecated":        status_counts.get("DEPRECATED", 0),
        "approval_rate_pct": round(approved / total * 100, 1) if total else 0,
        "status_breakdown":  status_counts,
    }


@router.get("/pii-exposure", summary="PII dataset exposure report")
async def pii_exposure(db: AsyncSession = Depends(get_db)):
    stmt = select(Dataset).where(Dataset.is_pii == True, Dataset.is_active == True)
    result = await db.execute(stmt)
    pii_datasets = result.scalars().all()
    return {
        "pii_dataset_count": len(pii_datasets),
        "datasets": [
            {
                "id":             str(d.id),
                "name":           d.qualified_name,
                "domain":         d.domain,
                # FIX: guard against None classification
                "classification": d.classification.value if d.classification else None,
                "owner":          d.owner_email,
            }
            for d in pii_datasets
        ],
    }
