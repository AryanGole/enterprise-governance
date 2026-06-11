"""
Unit Tests — models/models.py + serializers + impact helpers
"""
import pytest, sys, os, uuid
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.models import (
    Dataset, DatasetColumn, SchemaVersion, LineageEdge, AuditLog,
    GovernanceRecord, DataDictionary, Pipeline,
    DataClassification, GovernanceStatus, LineageNodeType,
    ChangeType, AuditAction,
)
from api.routes.datasets import _serialize_dataset, _serialize_column
from api.routes.impact import _recommendation


# ════════════════════════════════════════
# Enums
# ════════════════════════════════════════
class TestEnums:
    def test_classification_values(self):
        assert DataClassification.PUBLIC.value      == "PUBLIC"
        assert DataClassification.INTERNAL.value    == "INTERNAL"
        assert DataClassification.CONFIDENTIAL.value== "CONFIDENTIAL"
        assert DataClassification.RESTRICTED.value  == "RESTRICTED"
    def test_governance_status_values(self):
        assert GovernanceStatus.DRAFT.value     == "DRAFT"
        assert GovernanceStatus.PENDING.value   == "PENDING_REVIEW"
        assert GovernanceStatus.APPROVED.value  == "APPROVED"
        assert GovernanceStatus.REJECTED.value  == "REJECTED"
        assert GovernanceStatus.DEPRECATED.value== "DEPRECATED"
    def test_change_type_values(self):
        assert ChangeType.COLUMN_ADDED.value    == "COLUMN_ADDED"
        assert ChangeType.COLUMN_REMOVED.value  == "COLUMN_REMOVED"
        assert ChangeType.TYPE_CHANGED.value    == "TYPE_CHANGED"
        assert ChangeType.NULLABLE_CHANGED.value== "NULLABLE_CHANGED"
    def test_audit_action_values(self):
        assert AuditAction.CREATE.value == "CREATE"
        assert AuditAction.UPDATE.value == "UPDATE"
        assert AuditAction.APPROVE.value== "APPROVE"
        assert AuditAction.REJECT.value == "REJECT"
    def test_node_type_values(self):
        assert LineageNodeType.TABLE.value    == "TABLE"
        assert LineageNodeType.VIEW.value     == "VIEW"
        assert LineageNodeType.DASHBOARD.value== "DASHBOARD"
        assert LineageNodeType.ML_MODEL.value == "ML_MODEL"
    def test_all_enums_are_str_subclasses(self):
        for e in [DataClassification.INTERNAL, GovernanceStatus.DRAFT,
                  ChangeType.COLUMN_ADDED, AuditAction.CREATE, LineageNodeType.TABLE]:
            assert isinstance(e, str), f"{e} not a str subclass"


# ════════════════════════════════════════
# Model tablenames
# ════════════════════════════════════════
class TestModelTablenames:
    def test_dataset(self):          assert Dataset.__tablename__      == "datasets"
    def test_column(self):           assert DatasetColumn.__tablename__ == "dataset_columns"
    def test_schema_version(self):   assert SchemaVersion.__tablename__ == "schema_versions"
    def test_lineage_edge(self):     assert LineageEdge.__tablename__   == "lineage_edges"
    def test_audit_log(self):        assert AuditLog.__tablename__      == "audit_logs"
    def test_governance_record(self):assert GovernanceRecord.__tablename__== "governance_records"
    def test_data_dictionary(self):  assert DataDictionary.__tablename__ == "data_dictionary"
    def test_pipeline(self):         assert Pipeline.__tablename__       == "pipelines"


# ════════════════════════════════════════
# _serialize_dataset
# ════════════════════════════════════════
class TestSerializeDataset:
    def _ds(self, **kw):
        from datetime import datetime
        d = Dataset(
            id=uuid.uuid4(), name="ds", qualified_name="a.b.c",
            source_system="S", domain="F", owner_team="T", owner_email="e@e.com",
            is_pii=False, is_active=True, tags=[],
            classification=DataClassification.INTERNAL,
            governance_status=GovernanceStatus.DRAFT,
            node_type=LineageNodeType.TABLE,
        )
        d.columns = []
        d.created_at = datetime(2024, 1, 15)
        d.updated_at = datetime(2024, 1, 16)
        d.last_profiled_at = None
        for k,v in kw.items(): setattr(d, k, v)
        return d

    def test_returns_dict(self):              assert isinstance(_serialize_dataset(self._ds()), dict)
    def test_id_is_string(self):              assert isinstance(_serialize_dataset(self._ds())["id"], str)
    def test_classification_as_string(self):  assert _serialize_dataset(self._ds())["classification"] == "INTERNAL"
    def test_governance_status_string(self):  assert _serialize_dataset(self._ds())["governance_status"] == "DRAFT"
    def test_node_type_string(self):          assert _serialize_dataset(self._ds())["node_type"] == "TABLE"
    def test_tags_none_becomes_list(self):
        d = self._ds(); d.tags = None
        assert _serialize_dataset(d)["tags"] == []
    def test_column_count_from_columns(self):
        d = self._ds(); d.columns = [DatasetColumn(column_name="x", data_type="INT")]
        assert _serialize_dataset(d)["column_count"] == 1
    def test_timestamps_iso(self):
        r = _serialize_dataset(self._ds())
        assert "2024-01-15" in r["created_at"]
        assert "2024-01-16" in r["updated_at"]
    def test_last_profiled_none(self):
        assert _serialize_dataset(self._ds())["last_profiled_at"] is None
    def test_required_keys_present(self):
        r = _serialize_dataset(self._ds())
        for k in ["id","name","qualified_name","domain","source_system",
                  "classification","governance_status","is_pii","tags","column_count"]:
            assert k in r, f"Missing: {k}"
    def test_pii_true_reflected(self):
        assert _serialize_dataset(self._ds(is_pii=True))["is_pii"] is True
    def test_restricted_classification(self):
        d = self._ds(classification=DataClassification.RESTRICTED)
        assert _serialize_dataset(d)["classification"] == "RESTRICTED"


# ════════════════════════════════════════
# _serialize_column
# ════════════════════════════════════════
class TestSerializeColumn:
    def _col(self, **kw):
        c = DatasetColumn(column_name="amount", data_type="FLOAT",
                          is_nullable=True, is_primary_key=False,
                          is_foreign_key=False, is_pii=False, ordinal_position=1)
        c.id = uuid.uuid4(); c.classification = None
        for k,v in kw.items(): setattr(c,k,v)
        return c

    def test_returns_dict(self):       assert isinstance(_serialize_column(self._col()), dict)
    def test_id_is_string(self):       assert isinstance(_serialize_column(self._col())["id"], str)
    def test_pii_false(self):          assert _serialize_column(self._col())["is_pii"] is False
    def test_pii_true(self):           assert _serialize_column(self._col(is_pii=True))["is_pii"] is True
    def test_classification_none(self):assert _serialize_column(self._col())["classification"] is None
    def test_classification_value(self):
        c = self._col(); c.classification = DataClassification.RESTRICTED
        assert _serialize_column(c)["classification"] == "RESTRICTED"
    def test_required_keys(self):
        r = _serialize_column(self._col())
        for k in ["id","column_name","data_type","is_nullable","is_primary_key","is_pii","ordinal_position"]:
            assert k in r


# ════════════════════════════════════════
# _recommendation
# ════════════════════════════════════════
class TestRecommendation:
    def test_critical(self): assert "Freeze" in _recommendation("CRITICAL")
    def test_high(self):     assert "Notify" in _recommendation("HIGH")
    def test_medium(self):   assert "Coordinate" in _recommendation("MEDIUM")
    def test_low(self):      assert "Standard" in _recommendation("LOW")
    def test_unknown_raises_key_error(self):
        with pytest.raises(KeyError): _recommendation("UNKNOWN")
    def test_all_return_non_empty_string(self):
        for lvl in ["CRITICAL","HIGH","MEDIUM","LOW"]:
            r = _recommendation(lvl)
            assert isinstance(r, str) and len(r) > 10


# ════════════════════════════════════════
# Risk threshold classification
# ════════════════════════════════════════
class TestRiskClassification:
    def _cls(self, n):
        return ("CRITICAL" if n>50 else "HIGH" if n>20 else "MEDIUM" if n>5 else "LOW")
    def test_zero_is_low(self):      assert self._cls(0)    == "LOW"
    def test_five_is_low(self):      assert self._cls(5)    == "LOW"
    def test_six_is_medium(self):    assert self._cls(6)    == "MEDIUM"
    def test_twenty_is_medium(self): assert self._cls(20)   == "MEDIUM"
    def test_twentyone_is_high(self):assert self._cls(21)   == "HIGH"
    def test_fifty_is_high(self):    assert self._cls(50)   == "HIGH"
    def test_fiftyone_critical(self):assert self._cls(51)   == "CRITICAL"
    def test_large_critical(self):   assert self._cls(1000) == "CRITICAL"


# ════════════════════════════════════════
# DB-level model tests
# ════════════════════════════════════════
class TestDatabaseModels:
    @pytest.mark.asyncio
    async def test_dataset_persisted_and_retrieved(self, db_session):
        from tests.conftest import make_dataset
        ds = make_dataset(qualified_name="f.t.db1")
        db_session.add(ds); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(
            select(Dataset).where(Dataset.qualified_name == "f.t.db1")
        )).scalar_one()
        assert fetched.name == "test_dataset"
        assert fetched.domain == "Finance"

    @pytest.mark.asyncio
    async def test_column_fk_to_dataset(self, db_session):
        from tests.conftest import make_dataset
        ds = make_dataset(qualified_name="f.t.db2")
        db_session.add(ds); await db_session.commit()
        col = DatasetColumn(
            id=uuid.uuid4(), dataset_id=ds.id,
            column_name="revenue", data_type="FLOAT",
            is_nullable=False, is_primary_key=False,
            is_foreign_key=False, is_pii=False
        )
        db_session.add(col); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(
            select(DatasetColumn).where(DatasetColumn.dataset_id == ds.id)
        )).scalars().all()
        assert len(fetched) == 1 and fetched[0].column_name == "revenue"

    @pytest.mark.asyncio
    async def test_audit_log_persisted(self, db_session):
        log = AuditLog(
            id=uuid.uuid4(), entity_type="Dataset", entity_id="abc",
            action=AuditAction.CREATE, actor="test@e.com",
            change_summary="Created dataset"
        )
        db_session.add(log); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(select(AuditLog))).scalars().all()
        assert len(fetched) == 1 and fetched[0].actor == "test@e.com"

    @pytest.mark.asyncio
    async def test_pipeline_persisted(self, db_session):
        p = Pipeline(
            id=uuid.uuid4(), name="finance_etl",
            pipeline_type="AIRFLOW_DAG", is_active=True, tags=[]
        )
        db_session.add(p); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(select(Pipeline))).scalars().all()
        assert len(fetched) == 1 and fetched[0].name == "finance_etl"

    @pytest.mark.asyncio
    async def test_data_dictionary_persisted(self, db_session):
        d = DataDictionary(
            id=uuid.uuid4(), term="Net Revenue",
            business_definition="Revenue minus deductions.",
            domain="Finance", is_certified=False, synonyms=[]
        )
        db_session.add(d); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(select(DataDictionary))).scalars().all()
        assert len(fetched) == 1 and fetched[0].term == "Net Revenue"

    @pytest.mark.asyncio
    async def test_unique_constraint_qualified_name(self, db_session):
        from tests.conftest import make_dataset
        from sqlalchemy.exc import IntegrityError
        ds1 = make_dataset(qualified_name="f.t.unique")
        ds2 = make_dataset(qualified_name="f.t.unique")
        db_session.add(ds1); await db_session.commit()
        db_session.add(ds2)
        with pytest.raises(IntegrityError):
            await db_session.commit()

    @pytest.mark.asyncio
    async def test_lineage_edge_persisted(self, db_session):
        from tests.conftest import make_dataset
        src = make_dataset(qualified_name="f.t.src")
        tgt = make_dataset(qualified_name="f.t.tgt")
        db_session.add_all([src, tgt]); await db_session.commit()
        edge = LineageEdge(
            id=uuid.uuid4(), source_dataset_id=src.id, target_dataset_id=tgt.id,
            transformation_type="AGGREGATE", is_active=True, column_mappings={}
        )
        db_session.add(edge); await db_session.commit()
        from sqlalchemy import select
        fetched = (await db_session.execute(
            select(LineageEdge).where(LineageEdge.source_dataset_id == src.id)
        )).scalars().all()
        assert len(fetched) == 1
