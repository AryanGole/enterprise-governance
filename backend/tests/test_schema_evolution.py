"""
Unit Tests — services/schema_evolution.py
Every branch of detect_changes, _is_type_breaking, record_schema_version,
get_schema_diff, and _raise_breaking_change_alert.
"""
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.schema_evolution import detect_changes, _is_type_breaking, TYPE_COMPATIBILITY
from models.models import ChangeType

SCHEMA_V1 = {"columns": {
    "id":     {"type": "INTEGER", "nullable": False},
    "name":   {"type": "VARCHAR", "nullable": True},
    "amount": {"type": "FLOAT",   "nullable": True},
}}
SCHEMA_V2_SAFE = {"columns": {
    "id":     {"type": "INTEGER", "nullable": False},
    "name":   {"type": "TEXT",    "nullable": True},
    "amount": {"type": "FLOAT",   "nullable": True},
    "status": {"type": "VARCHAR", "nullable": True},
}}
SCHEMA_V2_BREAKING = {"columns": {
    "id": {"type": "INTEGER", "nullable": False},
}}


class TestIsTypeBreaking:
    def test_integer_to_bigint_safe(self):    assert _is_type_breaking("INTEGER","BIGINT")  is False
    def test_integer_to_float_safe(self):     assert _is_type_breaking("INTEGER","FLOAT")   is False
    def test_varchar_to_text_safe(self):      assert _is_type_breaking("VARCHAR","TEXT")    is False
    def test_date_to_timestamp_safe(self):    assert _is_type_breaking("DATE","TIMESTAMP")  is False
    def test_boolean_to_integer_safe(self):   assert _is_type_breaking("BOOLEAN","INTEGER") is False
    def test_float_to_integer_breaking(self): assert _is_type_breaking("FLOAT","INTEGER")   is True
    def test_text_to_varchar_breaking(self):  assert _is_type_breaking("TEXT","VARCHAR")     is True
    def test_timestamp_to_date_breaking(self):assert _is_type_breaking("TIMESTAMP","DATE")  is True
    def test_integer_to_boolean_breaking(self):assert _is_type_breaking("INTEGER","BOOLEAN") is True
    def test_unknown_defaults_breaking(self): assert _is_type_breaking("JSON","BLOB")        is True
    def test_lowercase_normalised(self):      assert _is_type_breaking("integer","bigint")   is False
    def test_mixed_case_normalised(self):     assert _is_type_breaking("Float","Integer")    is True
    def test_full_matrix(self):
        for (f,t), expected in TYPE_COMPATIBILITY.items():
            assert _is_type_breaking(f,t) == expected, f"{f}->{t}"


class TestDetectChangesIdentity:
    def test_both_empty(self):
        c,b = detect_changes({},{})
        assert c==[] and b is False
    def test_no_columns_key(self):
        c,b = detect_changes({"x":1},{"x":2})
        assert c==[] and b is False
    def test_identical_schema(self):
        c,b = detect_changes(SCHEMA_V1, SCHEMA_V1)
        assert c==[] and b is False
    def test_empty_columns_dict(self):
        c,b = detect_changes({"columns":{}},{"columns":{}})
        assert c==[] and b is False


class TestDetectChangesRemoval:
    def test_one_column_removed_breaking(self):
        old={"columns":{"id":{"type":"INTEGER"},"amt":{"type":"FLOAT"}}}
        new={"columns":{"id":{"type":"INTEGER"}}}
        c,b = detect_changes(old,new)
        assert b is True
        rm = [x for x in c if x["type"]==ChangeType.COLUMN_REMOVED]
        assert len(rm)==1 and rm[0]["column"]=="amt" and rm[0]["is_breaking"] is True
    def test_multiple_removed(self):
        old={"columns":{f"c{i}":{"type":"INTEGER"} for i in range(5)}}
        new={"columns":{"c0":{"type":"INTEGER"}}}
        c,b = detect_changes(old,new)
        assert b is True
        assert sum(1 for x in c if x["type"]==ChangeType.COLUMN_REMOVED)==4
    def test_all_removed(self):
        old={"columns":{"a":{"type":"INTEGER"},"b":{"type":"VARCHAR"}}}
        c,b = detect_changes(old,{"columns":{}})
        assert b is True and len(c)==2
    def test_description_non_empty(self):
        old={"columns":{"x":{"type":"FLOAT"}}}
        c,_ = detect_changes(old,{"columns":{}})
        assert len(c[0]["description"])>5


class TestDetectChangesAddition:
    def test_nullable_added_safe(self):
        old={"columns":{"id":{"type":"INTEGER"}}}
        new={"columns":{"id":{"type":"INTEGER"},"e":{"type":"VARCHAR","nullable":True}}}
        c,b = detect_changes(old,new)
        assert b is False
        assert [x for x in c if x["type"]==ChangeType.COLUMN_ADDED][0]["is_breaking"] is False
    def test_not_null_added_breaking(self):
        old={"columns":{"id":{"type":"INTEGER"}}}
        new={"columns":{"id":{"type":"INTEGER"},"r":{"type":"VARCHAR","nullable":False}}}
        c,b = detect_changes(old,new)
        assert b is True
    def test_no_nullable_key_defaults_safe(self):
        c,b = detect_changes({"columns":{}},{"columns":{"c":{"type":"VARCHAR"}}})
        assert b is False
    def test_mixed_additions(self):
        new={"columns":{"safe":{"type":"VARCHAR","nullable":True},"brk":{"type":"VARCHAR","nullable":False}}}
        c,b = detect_changes({"columns":{}},new)
        assert b is True
        assert sum(1 for x in c if x["type"]==ChangeType.COLUMN_ADDED)==2


class TestDetectChangesTypeChange:
    def test_safe_type_change(self):
        old={"columns":{"n":{"type":"VARCHAR","nullable":True}}}
        new={"columns":{"n":{"type":"TEXT","nullable":True}}}
        c,b = detect_changes(old,new)
        assert b is False
        t=[x for x in c if x["type"]==ChangeType.TYPE_CHANGED]
        assert t[0]["old_type"]=="VARCHAR" and t[0]["new_type"]=="TEXT"
    def test_breaking_type_change(self):
        old={"columns":{"p":{"type":"FLOAT","nullable":True}}}
        new={"columns":{"p":{"type":"INTEGER","nullable":True}}}
        _,b = detect_changes(old,new)
        assert b is True
    def test_unknown_type_change_breaking(self):
        old={"columns":{"d":{"type":"JSON","nullable":True}}}
        new={"columns":{"d":{"type":"BLOB","nullable":True}}}
        _,b = detect_changes(old,new)
        assert b is True
    def test_no_change_no_type_changed_event(self):
        s={"columns":{"x":{"type":"INTEGER","nullable":True}}}
        c,_ = detect_changes(s,s)
        assert not any(x["type"]==ChangeType.TYPE_CHANGED for x in c)


class TestDetectChangesNullable:
    def test_nullable_to_not_null_breaking(self):
        old={"columns":{"c":{"type":"VARCHAR","nullable":True}}}
        new={"columns":{"c":{"type":"VARCHAR","nullable":False}}}
        c,b = detect_changes(old,new)
        assert b is True
        assert [x for x in c if x["type"]==ChangeType.NULLABLE_CHANGED][0]["is_breaking"] is True
    def test_not_null_to_nullable_safe(self):
        old={"columns":{"c":{"type":"VARCHAR","nullable":False}}}
        new={"columns":{"c":{"type":"VARCHAR","nullable":True}}}
        c,b = detect_changes(old,new)
        assert b is False
        assert [x for x in c if x["type"]==ChangeType.NULLABLE_CHANGED][0]["is_breaking"] is False
    def test_both_missing_nullable_no_event(self):
        s={"columns":{"c":{"type":"INTEGER"}}}
        c,_ = detect_changes(s,s)
        assert not any(x["type"]==ChangeType.NULLABLE_CHANGED for x in c)
    def test_missing_to_not_null_breaking(self):
        old={"columns":{"c":{"type":"VARCHAR"}}}
        new={"columns":{"c":{"type":"VARCHAR","nullable":False}}}
        _,b = detect_changes(old,new)
        assert b is True


class TestDetectChangesCombinedEdge:
    def test_type_and_nullable_both_change(self):
        old={"columns":{"p":{"type":"FLOAT","nullable":True}}}
        new={"columns":{"p":{"type":"INTEGER","nullable":False}}}
        c,b = detect_changes(old,new)
        assert b is True
        types={x["type"] for x in c}
        assert ChangeType.TYPE_CHANGED in types and ChangeType.NULLABLE_CHANGED in types
    def test_none_col_def_in_old_handled(self):
        c,b = detect_changes({"columns":{"col":None}},{"columns":{"col":{"type":"INTEGER"}}})
        assert b is True and len(c)>=1
    def test_none_col_def_in_new_handled(self):
        c,b = detect_changes({"columns":{"col":{"type":"INTEGER"}}},{"columns":{"col":None}})
        assert b is True
    def test_all_changes_have_required_keys(self):
        old={"columns":{"kept":{"type":"INTEGER","nullable":True},"rm":{"type":"FLOAT","nullable":True}}}
        new={"columns":{"kept":{"type":"BIGINT","nullable":False},"new":{"type":"VARCHAR","nullable":True}}}
        c,_ = detect_changes(old,new)
        for ch in c:
            for k in ("type","column","is_breaking","description"):
                assert k in ch, f"Missing key '{k}' in change: {ch}"
    def test_large_schema_no_crash(self):
        old={"columns":{f"c{i}":{"type":"INTEGER","nullable":True} for i in range(100)}}
        new={"columns":{f"c{i}":{"type":"INTEGER","nullable":True} for i in range(50,150)}}
        c,b = detect_changes(old,new)
        assert b is True and len(c)>=50


class TestRecordSchemaVersion:
    @pytest.mark.asyncio
    async def test_first_version_v1_no_changes(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rv1")
        db_session.add(ds); await db_session.commit()
        sv=await record_schema_version(db_session,ds,SCHEMA_V1,"tester@e.com")
        assert sv.version==1 and sv.is_breaking is False and sv.changes==[]

    @pytest.mark.asyncio
    async def test_version_increments(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvinc")
        db_session.add(ds); await db_session.commit()
        for v in [1,2,3]:
            sv=await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
            assert sv.version==v

    @pytest.mark.asyncio
    async def test_safe_change_not_breaking(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvsafe")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        sv2=await record_schema_version(db_session,ds,SCHEMA_V2_SAFE,"sys")
        assert sv2.is_breaking is False and len(sv2.changes)>=1

    @pytest.mark.asyncio
    async def test_breaking_change_sets_flag(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvbreak")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        sv2=await record_schema_version(db_session,ds,SCHEMA_V2_BREAKING,"sys")
        assert sv2.is_breaking is True

    @pytest.mark.asyncio
    async def test_change_notes_stored(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvnotes")
        db_session.add(ds); await db_session.commit()
        sv=await record_schema_version(db_session,ds,SCHEMA_V1,"sys","My note")
        assert sv.change_notes=="My note"

    @pytest.mark.asyncio
    async def test_snapshot_stored(self, db_session):
        from services.schema_evolution import record_schema_version
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvsnap")
        db_session.add(ds); await db_session.commit()
        sv=await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        assert sv.schema_snapshot==SCHEMA_V1

    @pytest.mark.asyncio
    async def test_audit_log_written(self, db_session):
        from services.schema_evolution import record_schema_version
        from models.models import AuditLog
        from sqlalchemy import select
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.rvaudit")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"auditor@e.com")
        logs=(await db_session.execute(
            select(AuditLog).where(AuditLog.entity_type=="SchemaVersion")
        )).scalars().all()
        assert len(logs)>=1 and logs[0].actor=="auditor@e.com"


class TestGetSchemaDiff:
    @pytest.mark.asyncio
    async def test_diff_structure(self, db_session):
        from services.schema_evolution import record_schema_version, get_schema_diff
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.diff1")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        await record_schema_version(db_session,ds,SCHEMA_V2_BREAKING,"sys")
        d=await get_schema_diff(db_session,ds.id,1,2)
        assert d["from_version"]==1 and d["to_version"]==2
        assert d["is_breaking"] is True and d["change_count"]>=1
        assert "changes" in d and "from_snapshot" in d and "to_snapshot" in d

    @pytest.mark.asyncio
    async def test_missing_version_raises_value_error(self, db_session):
        from services.schema_evolution import record_schema_version, get_schema_diff
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.diff_miss")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        with pytest.raises(ValueError, match="not found"):
            await get_schema_diff(db_session,ds.id,1,99)

    @pytest.mark.asyncio
    async def test_safe_diff_not_breaking(self, db_session):
        from services.schema_evolution import record_schema_version, get_schema_diff
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.diff_safe")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        await record_schema_version(db_session,ds,SCHEMA_V2_SAFE,"sys")
        d=await get_schema_diff(db_session,ds.id,1,2)
        assert d["is_breaking"] is False


class TestBreakingChangeAlert:
    @pytest.mark.asyncio
    async def test_alert_with_enum_type(self, db_session):
        from services.schema_evolution import record_schema_version, _raise_breaking_change_alert
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.ale1")
        db_session.add(ds); await db_session.commit()
        await record_schema_version(db_session,ds,SCHEMA_V1,"sys")
        sv=await record_schema_version(db_session,ds,SCHEMA_V2_BREAKING,"sys")
        await _raise_breaking_change_alert(ds,sv,sv.changes)  # must not raise

    @pytest.mark.asyncio
    async def test_alert_with_plain_string_type(self, db_session):
        """FIXED: plain string change types (from JSON column) handled without AttributeError."""
        from services.schema_evolution import _raise_breaking_change_alert
        from models.models import SchemaVersion
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.ale2")
        db_session.add(ds); await db_session.commit()
        plain=[{"type":"COLUMN_REMOVED","column":"x","is_breaking":True,"description":"Removed."}]
        sv=SchemaVersion(dataset_id=ds.id,version=1,schema_snapshot={},changes=plain,is_breaking=True)
        await _raise_breaking_change_alert(ds,sv,plain)  # must not raise AttributeError

    @pytest.mark.asyncio
    async def test_alert_empty_changes(self, db_session):
        from services.schema_evolution import _raise_breaking_change_alert
        from models.models import SchemaVersion
        from tests.conftest import make_dataset
        ds=make_dataset(qualified_name="f.t.ale3")
        db_session.add(ds); await db_session.commit()
        sv=SchemaVersion(dataset_id=ds.id,version=1,schema_snapshot={},changes=[],is_breaking=False)
        await _raise_breaking_change_alert(ds,sv,[])
