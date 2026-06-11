"""
Shared test fixtures — SQLite-compatible engine via column-type patching.
UUID → SQLiteUUID (String 36), ARRAY → SQLiteArray (JSON Text).
All fixtures are function-scoped for full test isolation.
"""
import pytest
import pytest_asyncio
import sys, os, uuid, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sqlalchemy as sa
from sqlalchemy import TypeDecorator, String, Text


class SQLiteUUID(TypeDecorator):
    impl = String(36)
    cache_ok = True
    def process_bind_param(self, v, d): return str(v) if v is not None else None
    def process_result_value(self, v, d):
        if v is None: return None
        try: return uuid.UUID(v)
        except: return v


class SQLiteArray(TypeDecorator):
    impl = Text
    cache_ok = True
    def process_bind_param(self, v, d): return json.dumps(v) if v is not None else "[]"
    def process_result_value(self, v, d):
        if v is None: return []
        if isinstance(v, list): return v
        try: return json.loads(v)
        except: return []


def _patch_metadata_for_sqlite(metadata):
    from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY, UUID as PG_UUID
    for table in metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, PG_UUID):
                col.type = SQLiteUUID(); col.type._set_parent(col)
            elif isinstance(col.type, PG_ARRAY):
                col.type = SQLiteArray(); col.type._set_parent(col)


from models.models import (
    Base, Dataset, DatasetColumn, SchemaVersion, LineageEdge,
    AuditLog, GovernanceRecord, DataDictionary, Pipeline,
    DataClassification, GovernanceStatus, LineageNodeType,
    ChangeType, AuditAction,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker


@pytest_asyncio.fixture(scope="function")
async def engine():
    _patch_metadata_for_sqlite(Base.metadata)
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture(scope="function")
async def db_session(engine):
    Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with Session() as session:
        yield session


def make_dataset(**kwargs) -> Dataset:
    defaults = dict(
        name="test_dataset",
        qualified_name="finance.raw.test_dataset",
        source_system="Snowflake", domain="Finance",
        owner_team="Finance Engineering", owner_email="test@enterprise.com",
        classification=DataClassification.INTERNAL,
        governance_status=GovernanceStatus.DRAFT,
        node_type=LineageNodeType.TABLE,
        is_pii=False, is_active=True, tags=[],
    )
    defaults.update(kwargs)
    d = Dataset(**defaults)
    if d.id is None: d.id = uuid.uuid4()
    return d


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
    "id":     {"type": "INTEGER", "nullable": False},
}}
