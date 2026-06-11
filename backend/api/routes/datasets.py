"""
Datasets API Router
Full CRUD for dataset metadata with governance hooks, audit logging,
and Neo4j synchronization on every write.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload
from typing import Optional, List
from datetime import datetime
import uuid

from models.models import Dataset, DatasetColumn, AuditLog, AuditAction, GovernanceStatus
from core.neo4j_client import upsert_dataset_node, upsert_column_node
from core.database import get_db
from pydantic import BaseModel, EmailStr
from enum import Enum

router = APIRouter()

def _parse_uuid(dataset_id: str) -> uuid.UUID:
    """Parse UUID string, raising 422 on invalid format."""
    try:
        return uuid.UUID(dataset_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail=f"Invalid UUID format: '{dataset_id}'")


# ── Pydantic Schemas (Request/Response) ──────────────────────────────────────

class DataClassificationEnum(str, Enum):
    PUBLIC       = "PUBLIC"
    INTERNAL     = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED   = "RESTRICTED"


class NodeTypeEnum(str, Enum):
    TABLE     = "TABLE"
    VIEW      = "VIEW"
    STREAM    = "STREAM"
    API       = "API"
    REPORT    = "REPORT"
    DASHBOARD = "DASHBOARD"
    ML_MODEL  = "ML_MODEL"
    EXTERNAL  = "EXTERNAL"


class ColumnCreate(BaseModel):
    column_name:          str
    display_name:         Optional[str] = None
    data_type:            str
    is_nullable:          bool = True
    is_primary_key:       bool = False
    is_foreign_key:       bool = False          # FIX: was missing
    is_pii:               bool = False
    business_definition:  Optional[str] = None
    technical_definition: Optional[str] = None
    transformation_logic: Optional[str] = None
    ordinal_position:     Optional[int] = None


class DatasetCreate(BaseModel):
    name:             str
    qualified_name:   str              # e.g. "analytics.finance.daily_revenue"
    display_name:     Optional[str] = None
    description:      Optional[str] = None
    source_system:    str              # Snowflake, Postgres, S3, BigQuery, etc.
    database_name:    Optional[str] = None
    schema_name:      Optional[str] = None
    table_name:       Optional[str] = None
    node_type:        NodeTypeEnum = NodeTypeEnum.TABLE
    classification:   DataClassificationEnum = DataClassificationEnum.INTERNAL
    owner_team:       str
    owner_email:      str
    domain:           str              # Finance, Risk, Marketing, Operations
    update_frequency: Optional[str] = None   # DAILY, HOURLY, WEEKLY, REAL_TIME
    tags:             List[str] = []
    is_pii:           bool = False
    dbt_model_path:   Optional[str] = None
    airflow_dag_id:   Optional[str] = None
    columns:          List[ColumnCreate] = []


class DatasetUpdate(BaseModel):
    display_name:     Optional[str] = None
    description:      Optional[str] = None
    classification:   Optional[DataClassificationEnum] = None
    owner_team:       Optional[str] = None
    owner_email:      Optional[str] = None
    update_frequency: Optional[str] = None
    tags:             Optional[List[str]] = None
    is_pii:           Optional[bool] = None


class DatasetResponse(BaseModel):
    id:               str
    name:             str
    qualified_name:   str
    display_name:     Optional[str] = None
    description:      Optional[str] = None
    source_system:    str
    domain:           str
    classification:   str
    governance_status: str
    owner_team:       Optional[str]
    owner_email:      Optional[str]
    update_frequency: Optional[str]
    node_type:        str
    is_pii:           bool
    tags:             List[str]
    column_count:     int
    created_at:       datetime
    updated_at:       datetime

    model_config = {"from_attributes": True}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/", response_model=dict, summary="List all datasets")
async def list_datasets(
    domain: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    classification: Optional[str] = Query(None),
    owner_email: Optional[str] = Query(None),
    is_pii: Optional[bool] = Query(None),
    governance_status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Paginated dataset catalog with multi-dimensional filtering.
    Supports full-text search across name, description, and qualified_name.
    """
    stmt = select(Dataset).where(Dataset.is_active == True).options(selectinload(Dataset.columns))

    if domain:         stmt = stmt.where(Dataset.domain == domain)
    if source_system:  stmt = stmt.where(Dataset.source_system == source_system)
    if classification: stmt = stmt.where(Dataset.classification == classification)
    if owner_email:    stmt = stmt.where(Dataset.owner_email == owner_email)
    if is_pii is not None: stmt = stmt.where(Dataset.is_pii == is_pii)
    if governance_status:  stmt = stmt.where(Dataset.governance_status == governance_status)
    if search:
        term = f"%{search}%"
        stmt = stmt.where(or_(
            Dataset.name.ilike(term),
            Dataset.display_name.ilike(term),
            Dataset.description.ilike(term),
            Dataset.qualified_name.ilike(term)
        ))

    # Total count
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar()

    # Paginate
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    datasets = result.scalars().all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "items": [_serialize_dataset(d) for d in datasets]
    }


@router.post("/", status_code=status.HTTP_201_CREATED, summary="Register a new dataset")
async def create_dataset(
    payload: DatasetCreate,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Register a new data asset in the governance catalog.
    Automatically syncs to Neo4j lineage graph and emits audit log.
    """
    # Check for duplicate qualified name
    existing = await db.execute(
        select(Dataset).where(Dataset.qualified_name == payload.qualified_name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"Dataset '{payload.qualified_name}' already registered."
        )

    import uuid as _uuid
    dataset = Dataset(**payload.model_dump(exclude={"columns"}))
    if dataset.id is None:
        dataset.id = _uuid.uuid4()
    db.add(dataset)

    # Add columns
    for idx, col in enumerate(payload.columns):
        col_data = col.model_dump(exclude={"ordinal_position"})
        column = DatasetColumn(
            dataset_id=dataset.id,
            ordinal_position=col.ordinal_position if col.ordinal_position is not None else idx,
            **col_data
        )
        db.add(column)

    # Audit log
    db.add(AuditLog(
        entity_type="Dataset",
        entity_id=str(dataset.id),
        action=AuditAction.CREATE,
        actor=request.headers.get("X-User-Email", "system"),
        actor_role=request.headers.get("X-User-Role", "analyst"),
        after_state=payload.model_dump(exclude={"columns"}),
        change_summary=f"Registered dataset '{payload.qualified_name}' in governance catalog.",
        correlation_id=getattr(request.state, "correlation_id", None)
    ))

    await db.commit()

    # Sync to Neo4j
    await upsert_dataset_node({
        "id": str(dataset.id),
        "qualified_name": dataset.qualified_name,
        "display_name": dataset.display_name or dataset.name,
        "source_system": dataset.source_system,
        "domain": dataset.domain,
        "classification": dataset.classification.value,
        "owner_team": dataset.owner_team,
        "node_type": dataset.node_type.value,
        "is_pii": dataset.is_pii
    })

    return {"id": str(dataset.id), "qualified_name": dataset.qualified_name, "status": "registered"}


@router.get("/{dataset_id}", summary="Get dataset details")
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Full dataset detail including all columns and metadata."""
    stmt = (select(Dataset)
            .where(Dataset.id == _parse_uuid(dataset_id))
            .options(selectinload(Dataset.columns)))
    result = await db.execute(stmt)
    dataset = result.scalar_one_or_none()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    # columns already loaded via selectinload
    cols = dataset.columns

    return {
        **_serialize_dataset(dataset),
        "columns": [_serialize_column(c) for c in cols]
    }


@router.patch("/{dataset_id}", summary="Update dataset metadata")
async def update_dataset(
    dataset_id: str,
    payload: DatasetUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Partial update of dataset metadata with full audit trail."""
    stmt = (select(Dataset)
            .where(Dataset.id == _parse_uuid(dataset_id))
            .options(selectinload(Dataset.columns)))
    dataset = (await db.execute(stmt)).scalar_one_or_none()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    before_state = _serialize_dataset(dataset)
    updates = payload.model_dump(exclude_none=True)

    for field, value in updates.items():
        setattr(dataset, field, value)
    from datetime import timezone as _tz
    dataset.updated_at = datetime.now(_tz.utc).replace(tzinfo=None)

    db.add(AuditLog(
        entity_type="Dataset",
        entity_id=str(dataset.id),
        action=AuditAction.UPDATE,
        actor=request.headers.get("X-User-Email", "system"),
        before_state=before_state,
        after_state=updates,
        change_summary=f"Updated fields: {', '.join(updates.keys())}",
        correlation_id=getattr(request.state, "correlation_id", None)
    ))

    await db.commit()

    # Re-sync to Neo4j
    # FIX: safely coerce enum or string classification/node_type for Neo4j
    def _enum_val(v):
        return v.value if hasattr(v, "value") else str(v) if v else None

    await upsert_dataset_node({
        "id": str(dataset.id),
        "qualified_name": dataset.qualified_name,
        "display_name": dataset.display_name or dataset.name,
        "source_system": dataset.source_system,
        "domain": dataset.domain,
        "classification": _enum_val(dataset.classification),
        "owner_team": dataset.owner_team,
        "node_type": _enum_val(dataset.node_type),
        "is_pii": dataset.is_pii
    })

    return {"id": dataset_id, "updated_fields": list(updates.keys())}


@router.get("/{dataset_id}/statistics", summary="Dataset statistics summary")
async def get_dataset_statistics(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Column count, PII columns, classification breakdown."""
    parsed_id = _parse_uuid(dataset_id)
    # FIX: verify dataset exists first
    dataset = (await db.execute(
        select(Dataset).where(Dataset.id == parsed_id)
    )).scalar_one_or_none()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    col_stmt = select(DatasetColumn).where(DatasetColumn.dataset_id == parsed_id)
    cols = (await db.execute(col_stmt)).scalars().all()

    total          = len(cols)
    documented     = sum(1 for c in cols if c.business_definition)
    nullable_count = sum(1 for c in cols if c.is_nullable)
    return {
        "total_columns":        total,
        "pii_columns":          sum(1 for c in cols if c.is_pii),
        "pk_columns":           sum(1 for c in cols if c.is_primary_key),
        "fk_columns":           sum(1 for c in cols if c.is_foreign_key),
        "nullable_columns":     nullable_count,
        "non_nullable_columns": total - nullable_count,        # FIX: added
        "documented_columns":   documented,
        "documentation_coverage": round(documented / total * 100, 1) if total else 0,
    }


# ── Serializers ───────────────────────────────────────────────────────────────

def _serialize_dataset(d: Dataset) -> dict:
    return {
        "id": str(d.id),
        "name": d.name,
        "qualified_name": d.qualified_name,
        "display_name": d.display_name,
        "description": d.description,
        "source_system": d.source_system,
        "database_name": d.database_name,
        "schema_name": d.schema_name,
        "table_name": d.table_name,
        "domain": d.domain,
        "classification": d.classification.value if d.classification else None,
        "governance_status": d.governance_status.value if d.governance_status else None,
        "owner_team": d.owner_team,
        "owner_email": d.owner_email,
        "update_frequency": d.update_frequency,
        "node_type": d.node_type.value if d.node_type else None,
        "is_pii": d.is_pii,
        "tags": d.tags or [],
        "column_count": len(d.columns) if d.columns else 0,
        "dbt_model_path": d.dbt_model_path,
        "airflow_dag_id": d.airflow_dag_id,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
        "last_profiled_at": d.last_profiled_at.isoformat() if d.last_profiled_at else None,
    }


def _serialize_column(c: DatasetColumn) -> dict:
    return {
        "id": str(c.id),
        "column_name": c.column_name,
        "display_name": c.display_name,
        "data_type": c.data_type,
        "is_nullable": c.is_nullable,
        "is_primary_key": c.is_primary_key,
        "is_foreign_key": c.is_foreign_key,
        "is_pii": c.is_pii,
        "classification": c.classification.value if c.classification else None,
        "business_definition": c.business_definition,
        "technical_definition": c.technical_definition,
        "transformation_logic": c.transformation_logic,
        "ordinal_position": c.ordinal_position,
    }
