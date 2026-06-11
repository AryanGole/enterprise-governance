"""Schema Evolution Router"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from core.database import get_db
from models.models import Dataset, SchemaVersion
from services.schema_evolution import record_schema_version, get_schema_diff
from pydantic import BaseModel
from typing import Optional, Dict
import uuid

router = APIRouter()


def _parse_uuid(val: str) -> uuid.UUID:
    try:
        return uuid.UUID(val)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail=f"Invalid UUID: '{val}'")


class SchemaVersionCreate(BaseModel):
    schema_snapshot: Dict
    authored_by: str
    change_notes: Optional[str] = None


@router.post("/{dataset_id}/versions", status_code=201, summary="Record schema version")
async def create_schema_version(
    dataset_id: str, payload: SchemaVersionCreate, db: AsyncSession = Depends(get_db)
):
    dataset = (await db.execute(
        select(Dataset).where(Dataset.id == _parse_uuid(dataset_id))
    )).scalar_one_or_none()
    if not dataset:
        raise HTTPException(404, "Dataset not found.")
    sv = await record_schema_version(
        db, dataset, payload.schema_snapshot, payload.authored_by, payload.change_notes
    )
    return {"version": sv.version, "is_breaking": sv.is_breaking, "changes": sv.changes}


@router.get("/{dataset_id}/versions", summary="List schema versions")
async def list_schema_versions(dataset_id: str, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(SchemaVersion)
        .where(SchemaVersion.dataset_id == _parse_uuid(dataset_id))
        .order_by(desc(SchemaVersion.version))
    )
    versions = (await db.execute(stmt)).scalars().all()
    return [
        {
            "version": v.version,
            "is_breaking": v.is_breaking,
            "change_count": len(v.changes) if v.changes else 0,
            "authored_by": v.authored_by,
            "created_at": v.created_at.isoformat() if v.created_at else None,
        }
        for v in versions
    ]


@router.get("/{dataset_id}/diff", summary="Schema version diff")
async def schema_diff(
    dataset_id: str,
    from_version: int = Query(..., ge=1),
    to_version: int = Query(..., ge=1),
    db: AsyncSession = Depends(get_db),
):
    if from_version == to_version:
        raise HTTPException(400, "from_version and to_version must be different.")
    try:
        return await get_schema_diff(db, _parse_uuid(dataset_id), from_version, to_version)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
