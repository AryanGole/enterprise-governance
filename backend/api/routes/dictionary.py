"""Data Dictionary Router"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from core.database import get_db
from models.models import DataDictionary
from pydantic import BaseModel
from typing import Optional, List
import uuid

router = APIRouter()


class DictionaryCreate(BaseModel):
    term:                 str
    business_definition:  str
    technical_definition: Optional[str] = None
    domain:               Optional[str] = None
    synonyms:             Optional[List[str]] = []
    steward:              Optional[str] = None


@router.get("/", summary="List dictionary terms")
async def list_terms(domain: Optional[str] = Query(None), db: AsyncSession = Depends(get_db)):
    stmt = select(DataDictionary)
    if domain:
        stmt = stmt.where(DataDictionary.domain == domain)
    terms = (await db.execute(stmt)).scalars().all()
    return [
        {
            "id":                  str(t.id),
            "term":                t.term,
            "business_definition": t.business_definition,
            "domain":              t.domain,
            "is_certified":        t.is_certified,
        }
        for t in terms
    ]


@router.post("/", status_code=201, summary="Add dictionary term")
async def create_term(payload: DictionaryCreate, db: AsyncSession = Depends(get_db)):
    term_obj = DataDictionary(
        id=uuid.uuid4(),         # FIX: set id eagerly before commit
        **payload.model_dump()
    )
    db.add(term_obj)
    await db.commit()
    # FIX: no need for refresh since id was set eagerly
    return {"id": str(term_obj.id), "term": term_obj.term}
