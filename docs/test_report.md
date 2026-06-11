# Test & Bug Report
## Enterprise Metadata & Data Lineage Management System
**Date:** 2026-05-26 | **Platform:** Python 3.12 / FastAPI / SQLAlchemy 2.0 / Pydantic v2  
**Test Runner:** pytest-asyncio | **DB Backend (tests):** SQLite in-memory (aiosqlite)

---

## Executive Summary

| Metric | Result |
|--------|--------|
| Total tests | **143** |
| Passed | **143** |
| Failed | **0** |
| Bugs found | **9** |
| Bugs fixed | **9** |
| Overall coverage | **78%** |
| models/models.py coverage | **100%** |
| services/schema_evolution.py coverage | **91%** |

---

## Test Suite Breakdown

### File 1 — `test_schema_evolution.py` (Unit, 37 tests)

| Class | Count | What's tested |
|-------|-------|----------------|
| `TestIsTypeBreaking` | 12 | Full type-compat matrix, case normalisation, unknown pairs |
| `TestDetectChanges` | 20 | Column add/remove/modify, nullable changes, edge cases, None-def guard |
| `TestRecordSchemaVersion` | 5 | Version sequencing, breaking flag persistence, audit log creation |

### File 2 — `test_models_and_helpers.py` (Unit, 46 tests)

| Class | Count | What's tested |
|-------|-------|----------------|
| `TestEnums` | 7 | All enum values, str-subclass contract for JSON safety |
| `TestDatasetModel` | 10 | Construction, tablenames, required field assertions |
| `TestSerializeDataset` | 9 | Enum→string, None-safe tags, ISO timestamps, column_count |
| `TestSerializeColumn` | 5 | PII flag, classification serialisation, id→str |
| `TestRecommendation` | 6 | All risk levels, KeyError on unknown risk (by design) |
| `TestRiskClassification` | 9 | All threshold boundaries: 0, 5, 6, 20, 21, 50, 51, 1000 |

### File 3 — `test_datasets_api.py` (Integration, 29 tests)

| Class | Count | What's tested |
|-------|-------|----------------|
| `TestCreateDataset` | 9 | 201 creation, duplicate 409, 422 on bad payload, columns stored, PII/classification flags |
| `TestListDatasets` | 7 | Empty state, domain/PII filters, search OR logic, pagination, page 2 |
| `TestGetDataset` | 4 | Detail + columns, 404 on missing, **422 on invalid UUID** |
| `TestUpdateDataset` | 4 | Partial update, audit trail, 404 on missing, **422 on bad UUID** |
| `TestDatasetStatistics` | 5 | Total/doc/pk/pii column counts, 0% coverage on zero-col dataset |

### File 4 — `test_schema_governance_api.py` (Integration, 31 tests)

| Class | Count | What's tested |
|-------|-------|----------------|
| `TestCreateSchemaVersion` | 7 | v1 init, increment, safe/breaking detection, 404 dataset, 422 missing author |
| `TestListSchemaVersions` | 3 | Empty, descending order, all metadata fields present |
| `TestSchemaDiff` | 2 | Breaking diff content, **404 on missing version** |
| `TestGovernanceSummary` | 3 | Zero-total state, status counts, full response structure |
| `TestPiiExposure` | 3 | No PII, PII detected, response field completeness |
| `TestGlobalSearch` | 5 | Name match, empty result, min-length 422, structure, dictionary match |
| `TestDataDictionary` | 4 | Create, list, domain filter, missing definition 422 |
| `TestAuditLog` | 4 | Populated on create, on schema version, entity_type filter, field structure |

---

## Coverage Report

```
Name                           Stmts   Miss  Cover   Missing lines
--------------------------------------------------------------------
models/models.py                 203      0   100%
services/schema_evolution.py      76      7    91%   219-228
api/routes/audit.py               15      1    93%   23
api/routes/schemas.py             33      7    79%   25-30, 38, 50-51
api/routes/search.py              12      2    83%   19-24
api/routes/datasets.py           162     36    78%   161-165, 188-237, 247-254, 272-309
api/routes/dictionary.py          26      2    92%   25, 32
api/routes/governance.py          21      7    67%   14-18, 32-33
api/routes/impact.py              15      8    47%   19-35 (Neo4j-dependent)
api/routes/lineage.py             57     57     0%   Fully Neo4j-dependent
api/routes/pipelines.py           10     10     0%   No pipeline test data
--------------------------------------------------------------------
TOTAL                            630    137    78%
```

> **Note:** `lineage.py` (0%) and the Neo4j-dependent sections of `impact.py` are excluded from unit/integration tests by design — they require a live Neo4j driver. These are covered by manual smoke tests and are prime candidates for `testcontainers` integration tests.

---

## Bugs Found & Fixed

### BUG-01 · CRITICAL — Invalid UUID in path returns 500, not 422

**File:** `api/routes/datasets.py` — `get_dataset()`, `update_dataset()`, `get_dataset_statistics()`

**Root cause:** Bare `uuid.UUID(dataset_id)` call in route handlers raised `ValueError` on non-UUID path strings (e.g. `/datasets/not-a-uuid`), propagating as an unhandled HTTP 500.

```python
# BEFORE — crashes
stmt = select(Dataset).where(Dataset.id == uuid.UUID(dataset_id))

# AFTER — raises proper 422
def _parse_uuid(dataset_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(dataset_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422,
                            detail=f"Invalid UUID format: '{dataset_id}'")

stmt = select(Dataset).where(Dataset.id == _parse_uuid(dataset_id))
```

---

### BUG-02 · HIGH — `_recommendation()` raises unhandled `KeyError` on unknown risk level

**File:** `api/routes/impact.py`

**Root cause:** Dict key lookup `{}[risk]` with no default. If `affected_count` boundary logic ever produced an unlisted tier (e.g., due to future refactoring), the router would crash.

```python
# BEFORE — KeyError on unknown risk
return {"CRITICAL": "...", "HIGH": "...", "MEDIUM": "...", "LOW": "..."}[risk]

# AFTER — router catches and returns safe fallback
try:
    recommendation = _recommendation(risk_level)
except KeyError:
    recommendation = f"Assess {affected_count} affected downstream assets..."
```

---

### BUG-03 · HIGH — Pydantic v2: `Optional[T]` without `= None` is treated as required

**Files:** `datasets.py`, `schemas.py`, `dictionary.py` — 6 Pydantic models, ~18 fields

**Root cause:** Pydantic v2 changed behaviour: `Optional[str]` without a default is a required field. Every partial API payload returned 422.

```python
# BEFORE — all optional fields required in Pydantic v2
class DatasetCreate(BaseModel):
    display_name:     Optional[str]   # required!
    description:      Optional[str]   # required!

# AFTER — explicit defaults
class DatasetCreate(BaseModel):
    display_name:     Optional[str] = None
    description:      Optional[str] = None
```

---

### BUG-04 · HIGH — `detect_changes()` crashes with `AttributeError` on `None` column definitions

**File:** `services/schema_evolution.py`

**Root cause:** `col_def.get("type")` called on `None` when a schema snapshot had a `None` column value instead of a `dict`.

```python
# BEFORE — AttributeError crash
if old_def.get("type") != new_def.get("type"):  # crashes if old_def is None

# AFTER — guarded
if not isinstance(old_def, dict) or not isinstance(new_def, dict):
    changes.append({"type": ChangeType.TYPE_CHANGED, "column": col,
                    "is_breaking": True, "description": "Malformed definition."})
    is_breaking = True
    continue
```

---

### BUG-05 · MEDIUM — `ordinal_position` passed twice to `DatasetColumn()` constructor

**File:** `api/routes/datasets.py` — `create_dataset()`

**Root cause:** `ordinal_position=idx` passed explicitly, then `**col.model_dump()` also included `ordinal_position`, causing `TypeError: got multiple values for keyword argument`.

```python
# BEFORE — TypeError crash
column = DatasetColumn(ordinal_position=idx, **col.model_dump())

# AFTER — excluded from model_dump
col_data = col.model_dump(exclude={"ordinal_position"})
column = DatasetColumn(
    ordinal_position=col.ordinal_position if col.ordinal_position is not None else idx,
    **col_data
)
```

---

### BUG-06 · MEDIUM — `Dataset.id` is `None` when columns are created (UUID not set at `__init__`)

**File:** `api/routes/datasets.py` — `create_dataset()`

**Root cause:** SQLAlchemy `Column(default=uuid.uuid4)` fires at SQL `INSERT` time, not at Python `__init__` time. `dataset.id` was `None` when `DatasetColumn(dataset_id=dataset.id)` ran, causing `NOT NULL constraint failed`.

```python
# BEFORE — dataset.id is None before flush
dataset = Dataset(**payload.model_dump(exclude={"columns"}))
db.add(dataset)
column = DatasetColumn(dataset_id=dataset.id, ...)  # None → IntegrityError

# AFTER — eagerly set id before columns reference it
dataset = Dataset(**payload.model_dump(exclude={"columns"}))
if dataset.id is None:
    dataset.id = uuid.uuid4()
db.add(dataset)
```

---

### BUG-07 · MEDIUM — Lazy relationship load outside async context causes `MissingGreenlet`

**File:** `api/routes/datasets.py` — `list_datasets()`, `get_dataset()`, `update_dataset()`

**Root cause:** `_serialize_dataset()` accessed `dataset.columns` (lazy-loaded by default). In async SQLAlchemy, lazy loading outside an `await` raises `MissingGreenlet`.

```python
# BEFORE — lazy load crashes async context
stmt = select(Dataset).where(Dataset.is_active == True)
# accessing dataset.columns → MissingGreenlet

# AFTER — eager load in same query
from sqlalchemy.orm import selectinload
stmt = (select(Dataset)
        .where(Dataset.is_active == True)
        .options(selectinload(Dataset.columns)))
```

---

### BUG-08 · LOW — `get_schema_diff()` ValueError propagates as unhandled crash

**File:** `api/routes/schemas.py` — `schema_diff()`

**Root cause:** Service raises `ValueError` for missing version numbers; the route handler did not catch it.

```python
# BEFORE — ValueError crashes as 500
return await get_schema_diff(db, dataset_id, from_version, to_version)

# AFTER — clean 404
try:
    return await get_schema_diff(db, dataset_id, from_version, to_version)
except ValueError as e:
    raise HTTPException(status_code=404, detail=str(e))
```

---

### BUG-09 · LOW — `datetime.utcnow()` deprecated in Python 3.12

**Files:** `models/models.py`, `api/routes/datasets.py`

**Root cause:** `datetime.utcnow()` is deprecated in Python 3.12. Running with `-W error` breaks CI.

```python
# BEFORE — DeprecationWarning
Column(DateTime, default=datetime.utcnow)
dataset.updated_at = datetime.utcnow()

# AFTER — timezone-aware, warning-free
def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)

Column(DateTime, default=_utcnow)
dataset.updated_at = datetime.now(_tz.utc).replace(tzinfo=None)
```

---

## Known Limitations

| Item | Detail |
|------|--------|
| `lineage.py` — 0% coverage | Requires live Neo4j. All Cypher logic is tested manually. |
| `impact.py` — 47% coverage | Neo4j-dependent sections excluded. Risk classification threshold logic is 100% tested separately. |
| `pipelines.py` — 0% coverage | No pipeline test data seeded. Add `TestPipelines` class in follow-up. |
| SQLAlchemy Column `default=` at `__init__` | Python-level defaults don't fire at object construction in legacy `Column` style. This is documented SQLAlchemy behaviour, not a bug. Callers must pass explicit values. |

---

## Recommended Follow-up

1. **Neo4j integration tests** — Use `testcontainers-python` (`Neo4jContainer`) to run lineage traversal, impact analysis, shortest-path, and orphan detection against a real graph in CI.
2. **Lineage endpoint tests** — Cover `POST /lineage`, `GET /lineage/{id}/graph`, `GET /lineage/analytics/orphaned` once Neo4j container is available.
3. **Pipeline registry tests** — Add `TestPipelines` class: list empty, create, filter by type/owner.
4. **Airflow DAG unit tests** — Mock `PostgresHook` and Marquez HTTP calls to unit-test `profile_source_schemas`, `register_schema_versions`, `compute_governance_scores`, `send_governance_digest`.
5. **Migrate to `mapped_column()` (SQLAlchemy 2.0 style)** — Gives proper Python-level defaults at `__init__` time, eliminating the `dataset.id = uuid.uuid4()` workaround.
6. **Fuzz `detect_changes()`** — Run `hypothesis` strategies over arbitrary schema dicts to surface any remaining edge-case crashes.
