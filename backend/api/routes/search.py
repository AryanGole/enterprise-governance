"""Global Search Router"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
from core.database import get_db
from models.models import Dataset, DataDictionary

router = APIRouter()


@router.get("/", summary="Global metadata search")
async def global_search(
    q: str = Query(..., min_length=2),
    db: AsyncSession = Depends(get_db),
):
    term = f"%{q}%"

    # FIX: filter by is_active=True; FIX: add display_name to search
    datasets = (
        await db.execute(
            select(Dataset)
            .where(
                Dataset.is_active == True,
                or_(
                    Dataset.name.ilike(term),
                    Dataset.display_name.ilike(term),
                    Dataset.qualified_name.ilike(term),
                    Dataset.description.ilike(term),
                ),
            )
            .limit(20)
        )
    ).scalars().all()

    terms = (
        await db.execute(
            select(DataDictionary)
            .where(
                or_(
                    DataDictionary.term.ilike(term),
                    DataDictionary.business_definition.ilike(term),
                )
            )
            .limit(10)
        )
    ).scalars().all()

    return {
        "query":            q,
        "datasets":         [
            {
                "id":            str(d.id),
                "name":          d.name,
                "qualified_name": d.qualified_name,
                "display_name":  d.display_name,
                "domain":        d.domain,
                "source_system": d.source_system,
            }
            for d in datasets
        ],
        "dictionary_terms": [
            {"id": str(t.id), "term": t.term, "domain": t.domain}
            for t in terms
        ],
        "total_results": len(datasets) + len(terms),
    }
