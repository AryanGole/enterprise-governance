"""
Integration Tests — /api/v1/datasets
Every endpoint, every branch, every error path.
"""
import pytest, sys, os, uuid
from unittest.mock import AsyncMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from models.models import Base, Dataset, DatasetColumn, DataClassification
from core.database import get_db


def build_app(engine):
    import api.routes.datasets as _ds_mod
    _ds_mod.upsert_dataset_node = AsyncMock(return_value=None)   # permanent mock
    
    Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    app = FastAPI()
    async def override_db():
        async with Session() as s: yield s
    app.dependency_overrides[get_db] = override_db
    from api.routes.datasets import router
    app.include_router(router, prefix="/api/v1/datasets")
    return app


@pytest.fixture
def neo4j_mock():
    with patch("api.routes.datasets.upsert_dataset_node", new=AsyncMock(return_value=None)):
        yield


async def _ac(engine):
    return AsyncClient(transport=ASGITransport(app=build_app(engine)), base_url="http://test")


def payload(**ov):
    base = {
        "name": "daily_revenue",
        "qualified_name": "finance.mart.daily_revenue",
        "source_system": "Snowflake",
        "owner_team": "Finance",
        "owner_email": "fin@e.com",
        "domain": "Finance",
        "classification": "INTERNAL",
        "is_pii": False,
        "tags": [],
        "columns": [],
    }
    base.update(ov)
    return base


# ════════════════════════════════════════
# POST /datasets/
# ════════════════════════════════════════
class TestCreateDataset:
    @pytest.mark.asyncio
    async def test_201_on_valid(self, engine):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/", json=payload())
        assert r.status_code == 201

    @pytest.mark.asyncio
    async def test_response_has_id_and_status(self, engine):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/", json=payload())
        b = r.json()
        assert "id" in b and b["status"] == "registered"
        assert b["qualified_name"] == "finance.mart.daily_revenue"

    @pytest.mark.asyncio
    async def test_409_on_duplicate_qualified_name(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload())
            r = await c.post("/api/v1/datasets/", json=payload())
        assert r.status_code == 409

    @pytest.mark.asyncio
    async def test_422_missing_required_field(self, engine):
        async with await _ac(engine) as c:
            p = payload(); del p["qualified_name"]
            r = await c.post("/api/v1/datasets/", json=p)
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_422_invalid_classification(self, engine):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/", json=payload(classification="TOP_SECRET"))
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_422_invalid_node_type(self, engine):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/", json=payload(node_type="FAKE"))
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_columns_stored(self, engine, db_session):
        cols = [
            {"column_name": "dt", "data_type": "DATE", "is_nullable": False,
             "is_primary_key": True, "is_foreign_key": False,
             "business_definition": "Revenue date", "ordinal_position": 1},
            {"column_name": "rev", "data_type": "FLOAT", "is_nullable": False,
             "is_primary_key": False, "is_foreign_key": False,
             "business_definition": "Total revenue", "ordinal_position": 2},
        ]
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/", json=payload(columns=cols))
        ds_id = r.json()["id"]
        from sqlalchemy import select
        stored = (await db_session.execute(
            select(DatasetColumn).where(DatasetColumn.dataset_id == ds_id)
        )).scalars().all()
        assert len(stored) == 2
        assert {s.column_name for s in stored} == {"dt", "rev"}

    @pytest.mark.asyncio
    async def test_is_foreign_key_stored(self, engine, db_session):
        cols = [{"column_name": "fk_col", "data_type": "INTEGER",
                 "is_nullable": True, "is_primary_key": False, "is_foreign_key": True}]
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/",
                             json=payload(qualified_name="f.t.fk_test", columns=cols))
        ds_id = r.json()["id"]
        from sqlalchemy import select
        col = (await db_session.execute(
            select(DatasetColumn).where(DatasetColumn.dataset_id == ds_id)
        )).scalar_one()
        assert col.is_foreign_key is True

    @pytest.mark.asyncio
    async def test_pii_flag_stored(self, engine, db_session):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/",
                             json=payload(qualified_name="f.t.pii", is_pii=True))
        ds_id = r.json()["id"]
        from sqlalchemy import select
        ds = (await db_session.execute(
            select(Dataset).where(Dataset.id == ds_id)
        )).scalar_one()
        assert ds.is_pii is True

    @pytest.mark.asyncio
    async def test_restricted_classification_stored(self, engine, db_session):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/",
                             json=payload(qualified_name="f.t.restricted", classification="RESTRICTED"))
        ds_id = r.json()["id"]
        from sqlalchemy import select
        ds = (await db_session.execute(
            select(Dataset).where(Dataset.id == ds_id)
        )).scalar_one()
        assert ds.classification == DataClassification.RESTRICTED

    @pytest.mark.asyncio
    async def test_no_columns_ok(self, engine):
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/",
                             json=payload(qualified_name="f.t.nocols", columns=[]))
        assert r.status_code == 201

    @pytest.mark.asyncio
    async def test_ordinal_position_respected(self, engine, db_session):
        cols = [
            {"column_name": "first",  "data_type": "INT", "ordinal_position": 1},
            {"column_name": "second", "data_type": "INT", "ordinal_position": 2},
        ]
        async with await _ac(engine) as c:
            r = await c.post("/api/v1/datasets/",
                             json=payload(qualified_name="f.t.ord", columns=cols))
        from sqlalchemy import select
        stored = (await db_session.execute(
            select(DatasetColumn).where(DatasetColumn.dataset_id == r.json()["id"])
                .order_by(DatasetColumn.ordinal_position)
        )).scalars().all()
        assert stored[0].column_name == "first"
        assert stored[1].column_name == "second"


# ════════════════════════════════════════
# GET /datasets/
# ════════════════════════════════════════
class TestListDatasets:
    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, engine):
        async with await _ac(engine) as c:
            r = await c.get("/api/v1/datasets/")
        b = r.json()
        assert b["total"] == 0 and b["items"] == []

    @pytest.mark.asyncio
    async def test_after_create_total_one(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload())
            r = await c.get("/api/v1/datasets/")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_filter_by_domain(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.a", domain="Finance"))
            await c.post("/api/v1/datasets/", json=payload(qualified_name="r.b", domain="Risk"))
            r = await c.get("/api/v1/datasets/?domain=Finance")
        b = r.json()
        assert b["total"] == 1 and b["items"][0]["domain"] == "Finance"

    @pytest.mark.asyncio
    async def test_filter_by_source_system(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.s1", source_system="Snowflake"))
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.s2", source_system="Spark"))
            r = await c.get("/api/v1/datasets/?source_system=Spark")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_filter_by_classification(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.c1", classification="INTERNAL"))
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.c2", classification="RESTRICTED"))
            r = await c.get("/api/v1/datasets/?classification=RESTRICTED")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_filter_by_pii(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.p1", is_pii=True))
            await c.post("/api/v1/datasets/", json=payload(qualified_name="f.p2", is_pii=False))
            r = await c.get("/api/v1/datasets/?is_pii=true")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_search_by_name(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(name="alpha_ds", qualified_name="f.alpha"))
            await c.post("/api/v1/datasets/", json=payload(name="beta_ds",  qualified_name="f.beta"))
            r = await c.get("/api/v1/datasets/?search=alpha")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_search_by_qualified_name(self, engine):
        async with await _ac(engine) as c:
            await c.post("/api/v1/datasets/", json=payload(qualified_name="risk.raw.transactions"))
            await c.post("/api/v1/datasets/", json=payload(qualified_name="finance.mart.revenue"))
            r = await c.get("/api/v1/datasets/?search=risk")
        assert r.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_pagination_page_size(self, engine):
        async with await _ac(engine) as c:
            for i in range(5):
                await c.post("/api/v1/datasets/", json=payload(
                    qualified_name=f"f.p.ds{i}", name=f"ds{i}"))
            r = await c.get("/api/v1/datasets/?page=1&page_size=2")
        b = r.json()
        assert b["total"] == 5 and len(b["items"]) == 2 and b["pages"] == 3

    @pytest.mark.asyncio
    async def test_pagination_page_2(self, engine):
        async with await _ac(engine) as c:
            for i in range(4):
                await c.post("/api/v1/datasets/", json=payload(
                    qualified_name=f"f.pg2.{i}", name=f"pg{i}"))
            r = await c.get("/api/v1/datasets/?page=2&page_size=2")
        assert len(r.json()["items"]) == 2

    @pytest.mark.asyncio
    async def test_response_structure(self, engine):
        async with await _ac(engine) as c:
            r = await c.get("/api/v1/datasets/")
        for k in ["total","page","page_size","pages","items"]:
            assert k in r.json()

    @pytest.mark.asyncio
    async def test_inactive_excluded(self, engine, db_session):
        from tests.conftest import make_dataset
        ds = make_dataset(qualified_name="f.t.inactive", is_active=False)
        db_session.add(ds); await db_session.commit()
        async with await _ac(engine) as c:
            r = await c.get("/api/v1/datasets/")
        assert r.json()["total"] == 0


# ════════════════════════════════════════
# GET /datasets/{id}
# ════════════════════════════════════════
class TestGetDataset:
    @pytest.mark.asyncio
    async def test_get_existing(self, engine):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}")
        assert r.status_code == 200 and r.json()["id"] == ds_id

    @pytest.mark.asyncio
    async def test_get_includes_columns(self, engine):
        cols = [{"column_name":"x","data_type":"INT","is_nullable":True,
                 "is_primary_key":False,"is_foreign_key":False}]
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.gc", columns=cols))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}")
        assert len(r.json()["columns"]) == 1

    @pytest.mark.asyncio
    async def test_404_on_missing_uuid(self, engine):
        async with await _ac(engine) as c:
            r = await c.get(f"/api/v1/datasets/{uuid.uuid4()}")
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_422_on_invalid_uuid(self, engine):
        async with await _ac(engine) as c:
            r = await c.get("/api/v1/datasets/not-a-uuid")
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_qualified_name_in_response(self, engine):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}")
        assert r.json()["qualified_name"] == "finance.mart.daily_revenue"


# ════════════════════════════════════════
# PATCH /datasets/{id}
# ════════════════════════════════════════
class TestUpdateDataset:
    @pytest.mark.asyncio
    async def test_update_description(self, engine):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            r = await c.patch(f"/api/v1/datasets/{ds_id}", json={"description": "Updated."})
        assert r.status_code == 200 and "description" in r.json()["updated_fields"]

    @pytest.mark.asyncio
    async def test_update_classification(self, engine, db_session):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            r = await c.patch(f"/api/v1/datasets/{ds_id}", json={"classification": "RESTRICTED"})
        assert r.status_code == 200
        from sqlalchemy import select
        ds = (await db_session.execute(
            select(Dataset).where(Dataset.id == ds_id)
        )).scalar_one()
        assert ds.classification == DataClassification.RESTRICTED

    @pytest.mark.asyncio
    async def test_update_pii_flag(self, engine, db_session):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            await c.patch(f"/api/v1/datasets/{ds_id}", json={"is_pii": True})
        from sqlalchemy import select
        ds = (await db_session.execute(
            select(Dataset).where(Dataset.id == ds_id)
        )).scalar_one()
        assert ds.is_pii is True

    @pytest.mark.asyncio
    async def test_update_owner_email(self, engine, db_session):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            await c.patch(f"/api/v1/datasets/{ds_id}", json={"owner_email":"new@e.com"})
        from sqlalchemy import select
        ds = (await db_session.execute(
            select(Dataset).where(Dataset.id == ds_id)
        )).scalar_one()
        assert ds.owner_email == "new@e.com"

    @pytest.mark.asyncio
    async def test_404_on_missing(self, engine):
        async with await _ac(engine) as c:
            r = await c.patch(f"/api/v1/datasets/{uuid.uuid4()}", json={"description":"x"})
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_422_on_invalid_uuid(self, engine):
        async with await _ac(engine) as c:
            r = await c.patch("/api/v1/datasets/bad-uuid", json={"description":"x"})
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_audit_log_written_on_update(self, engine, db_session):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            await c.patch(f"/api/v1/datasets/{ds_id}", json={"description":"Changed"})
        from sqlalchemy import select
        from models.models import AuditLog, AuditAction
        logs = (await db_session.execute(
            select(AuditLog).where(AuditLog.action == AuditAction.UPDATE)
        )).scalars().all()
        assert len(logs) >= 1

    @pytest.mark.asyncio
    async def test_neo4j_sync_after_classification_update(self, engine):
        """FIXED: updating classification (string) must not crash Neo4j sync via .value."""
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/", json=payload())).json()["id"]
            r = await c.patch(f"/api/v1/datasets/{ds_id}", json={"classification": "CONFIDENTIAL"})
        assert r.status_code == 200  # would be 500 before fix


# ════════════════════════════════════════
# GET /datasets/{id}/statistics
# ════════════════════════════════════════
class TestDatasetStatistics:
    @pytest.mark.asyncio
    async def test_statistics_with_two_cols(self, engine):
        cols = [
            {"column_name":"dt","data_type":"DATE","is_nullable":False,
             "is_primary_key":True,"is_foreign_key":False,"business_definition":"Date","ordinal_position":1},
            {"column_name":"rev","data_type":"FLOAT","is_nullable":False,
             "is_primary_key":False,"is_foreign_key":False,"business_definition":"Revenue","ordinal_position":2},
        ]
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.stat2", columns=cols))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}/statistics")
        b = r.json()
        assert b["total_columns"] == 2
        assert b["pk_columns"] == 1
        assert b["documented_columns"] == 2
        assert b["documentation_coverage"] == 100.0

    @pytest.mark.asyncio
    async def test_statistics_zero_columns(self, engine):
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.stat0", columns=[]))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}/statistics")
        b = r.json()
        assert b["total_columns"] == 0 and b["documentation_coverage"] == 0

    @pytest.mark.asyncio
    async def test_statistics_pii_count(self, engine):
        cols = [
            {"column_name":"email","data_type":"VARCHAR","is_pii":True,"is_nullable":True,
             "is_primary_key":False,"is_foreign_key":False},
            {"column_name":"amount","data_type":"FLOAT","is_pii":False,"is_nullable":True,
             "is_primary_key":False,"is_foreign_key":False},
        ]
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.pii_stat", columns=cols))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}/statistics")
        assert r.json()["pii_columns"] == 1

    @pytest.mark.asyncio
    async def test_statistics_404_on_missing_dataset(self, engine):
        """FIXED: statistics returns 404 when dataset_id is valid UUID but not found."""
        async with await _ac(engine) as c:
            r = await c.get(f"/api/v1/datasets/{uuid.uuid4()}/statistics")
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_statistics_422_on_invalid_uuid(self, engine):
        async with await _ac(engine) as c:
            r = await c.get("/api/v1/datasets/not-a-uuid/statistics")
        assert r.status_code == 422

    @pytest.mark.asyncio
    async def test_statistics_non_nullable_count(self, engine):
        cols = [
            {"column_name":"c1","data_type":"INT","is_nullable":False,
             "is_primary_key":False,"is_foreign_key":False},
            {"column_name":"c2","data_type":"INT","is_nullable":True,
             "is_primary_key":False,"is_foreign_key":False},
        ]
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.nn", columns=cols))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}/statistics")
        assert r.json()["non_nullable_columns"] == 1

    @pytest.mark.asyncio
    async def test_statistics_partial_documentation(self, engine):
        cols = [
            {"column_name":"doc","data_type":"INT","is_nullable":True,
             "is_primary_key":False,"is_foreign_key":False,"business_definition":"Documented"},
            {"column_name":"nodoc","data_type":"INT","is_nullable":True,
             "is_primary_key":False,"is_foreign_key":False},
        ]
        async with await _ac(engine) as c:
            ds_id = (await c.post("/api/v1/datasets/",
                                  json=payload(qualified_name="f.t.partial", columns=cols))).json()["id"]
            r = await c.get(f"/api/v1/datasets/{ds_id}/statistics")
        b = r.json()
        assert b["documented_columns"] == 1
        assert b["documentation_coverage"] == 50.0
