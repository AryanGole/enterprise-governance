"""Audit Log Router"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from core.database import get_db
from models.models import AuditLog
from typing import Optional

router = APIRouter()


@router.get("/", summary="Query audit logs")
async def get_audit_logs(
    entity_type: Optional[str] = Query(None),
    actor:       Optional[str] = Query(None),
    action:      Optional[str] = Query(None),
    page:        int = Query(1,  ge=1),
    page_size:   int = Query(50, ge=1, le=500),    # FIX: added ge=1
    db: AsyncSession = Depends(get_db),
):
    stmt = select(AuditLog).order_by(desc(AuditLog.created_at))
    if entity_type: stmt = stmt.where(AuditLog.entity_type == entity_type)
    if actor:       stmt = stmt.where(AuditLog.actor == actor)
    if action:      stmt = stmt.where(AuditLog.action == action)
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    logs = (await db.execute(stmt)).scalars().all()
    return {
        "logs": [
            {
                "id":             str(l.id),
                "entity_type":    l.entity_type,
                "entity_id":      l.entity_id,
                "action":         l.action.value,
                "actor":          l.actor,
                "change_summary": l.change_summary,
                "created_at":     l.created_at.isoformat(),
            }
            for l in logs
        ]
    }
