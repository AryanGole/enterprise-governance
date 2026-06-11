"""Pipelines Router"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from core.database import get_db
from models.models import Pipeline
from pydantic import BaseModel
from typing import Optional, List
import uuid

router = APIRouter()


class PipelineCreate(BaseModel):
    name:          str
    pipeline_type: Optional[str] = None
    dag_id:        Optional[str] = None
    schedule:      Optional[str] = None
    owner_team:    Optional[str] = None
    owner_email:   Optional[str] = None
    description:   Optional[str] = None
    sla_minutes:   Optional[int] = None
    tags:          Optional[List[str]] = []


@router.get("/", summary="List all pipelines")
async def list_pipelines(
    owner_team: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Pipeline).where(Pipeline.is_active == True)
    if owner_team:
        stmt = stmt.where(Pipeline.owner_team == owner_team)
    pipelines = (await db.execute(stmt)).scalars().all()
    return [
        {
            "id":               str(p.id),
            "name":             p.name,
            "type":             p.pipeline_type,
            "last_run_status":  p.last_run_status,
            "schedule":         p.schedule,
            "owner_team":       p.owner_team,
            "owner_email":      p.owner_email,
            "sla_minutes":      p.sla_minutes,
        }
        for p in pipelines
    ]


@router.post("/", status_code=201, summary="Register a pipeline")
async def create_pipeline(
    payload: PipelineCreate, db: AsyncSession = Depends(get_db)
):
    pipeline = Pipeline(
        id=uuid.uuid4(),       # set eagerly — avoid MissingGreenlet
        **payload.model_dump()
    )
    db.add(pipeline)
    await db.commit()
    return {"id": str(pipeline.id), "name": pipeline.name, "status": "registered"}


@router.get("/{pipeline_id}", summary="Get pipeline details")
async def get_pipeline(pipeline_id: str, db: AsyncSession = Depends(get_db)):
    try:
        pid = uuid.UUID(pipeline_id)
    except (ValueError, AttributeError):
        raise HTTPException(422, f"Invalid UUID: '{pipeline_id}'")
    pipeline = (await db.execute(
        select(Pipeline).where(Pipeline.id == pid)
    )).scalar_one_or_none()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found.")
    return {
        "id":              str(pipeline.id),
        "name":            pipeline.name,
        "type":            pipeline.pipeline_type,
        "dag_id":          pipeline.dag_id,
        "schedule":        pipeline.schedule,
        "owner_team":      pipeline.owner_team,
        "owner_email":     pipeline.owner_email,
        "last_run_status": pipeline.last_run_status,
        "sla_minutes":     pipeline.sla_minutes,
        "is_active":       pipeline.is_active,
    }
