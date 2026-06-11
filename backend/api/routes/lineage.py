"""
Lineage API Router — Source-to-target graph traversal via Neo4j.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, Dict, List
import uuid

from models.models import Dataset, LineageEdge, Pipeline
from core.neo4j_client import (
    get_full_lineage_graph, get_upstream_lineage, get_downstream_lineage,
    upsert_lineage_edge, find_shortest_path, get_impact_assessment,
    get_orphaned_datasets, get_cross_domain_flows
)
from core.database import get_db
from pydantic import BaseModel

router = APIRouter()


def _parse_uuid(val: str, field: str = "id") -> uuid.UUID:
    try:
        return uuid.UUID(val)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail=f"Invalid UUID for '{field}': '{val}'")


class LineageEdgeCreate(BaseModel):
    source_dataset_id:   str
    target_dataset_id:   str
    pipeline_id:         Optional[str] = None        # FIX: was missing = None
    transformation_type: str = "TRANSFORM"
    transformation_sql:  Optional[str] = None        # FIX: was missing = None
    column_mappings:     Optional[Dict] = {}


@router.get("/{dataset_id}/graph", summary="Full bidirectional lineage graph")
async def get_lineage_graph(
    dataset_id: str,
    depth: int = Query(5, ge=1, le=10),
    direction: str = Query("both", pattern="^(upstream|downstream|both)$")
):
    if direction == "upstream":
        graph = await get_upstream_lineage(dataset_id, depth)
    elif direction == "downstream":
        graph = await get_downstream_lineage(dataset_id, depth)
    else:
        graph = await get_full_lineage_graph(dataset_id, depth)

    if not graph["nodes"]:
        return {
            "root_id": dataset_id, "nodes": [], "edges": [],
            "message": "No lineage found. Register lineage edges to populate the graph."
        }
    return graph


@router.post("/", status_code=201, summary="Register a lineage edge")
async def create_lineage_edge(
    payload: LineageEdgeCreate,
    db: AsyncSession = Depends(get_db)
):
    src_uuid = _parse_uuid(payload.source_dataset_id, "source_dataset_id")
    tgt_uuid = _parse_uuid(payload.target_dataset_id, "target_dataset_id")

    source = (await db.execute(select(Dataset).where(Dataset.id == src_uuid))).scalar_one_or_none()
    if not source:
        raise HTTPException(404, f"Source dataset '{payload.source_dataset_id}' not found.")

    target = (await db.execute(select(Dataset).where(Dataset.id == tgt_uuid))).scalar_one_or_none()
    if not target:
        raise HTTPException(404, f"Target dataset '{payload.target_dataset_id}' not found.")

    # FIX: self-referential edge guard
    if src_uuid == tgt_uuid:
        raise HTTPException(400, "Source and target dataset cannot be the same.")

    edge = LineageEdge(
        id=uuid.uuid4(),                             # FIX: set id eagerly
        source_dataset_id=src_uuid,
        target_dataset_id=tgt_uuid,
        pipeline_id=_parse_uuid(payload.pipeline_id, "pipeline_id") if payload.pipeline_id else None,
        transformation_type=payload.transformation_type,
        transformation_sql=payload.transformation_sql,
        column_mappings=payload.column_mappings or {}
    )
    db.add(edge)
    await db.commit()

    await upsert_lineage_edge(
        source_id=payload.source_dataset_id,
        target_id=payload.target_dataset_id,
        pipeline_id=payload.pipeline_id,
        transformation_type=payload.transformation_type,
        column_mappings=payload.column_mappings
    )

    return {
        "id": str(edge.id),
        "source": source.qualified_name,
        "target": target.qualified_name,
        "transformation_type": payload.transformation_type,
        "status": "registered"
    }


@router.get("/path/shortest", summary="Shortest path between two datasets")
async def shortest_path(source_id: str = Query(...), target_id: str = Query(...)):
    result = await find_shortest_path(source_id, target_id)
    if result["hops"] == -1:
        raise HTTPException(404, "No lineage path found between these datasets.")
    return result


@router.get("/analytics/orphaned", summary="Datasets with no lineage")
async def orphaned_datasets():
    orphans = await get_orphaned_datasets()
    return {
        "count": len(orphans),
        "datasets": orphans,
        "governance_action": "Register lineage edges or deprecate these assets."
    }


@router.get("/analytics/cross-domain", summary="Cross-domain data flows")
async def cross_domain_flows():
    flows = await get_cross_domain_flows()
    return {"total_cross_domain_flows": len(flows), "flows": flows}
