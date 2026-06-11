# Architecture Documentation
## Enterprise Data Governance Platform v2.1

---

## System Architecture

```
                        ┌──────────────────────────────────────┐
                        │         React Frontend (3000)         │
                        │  Dashboard · Catalog · Lineage Graph  │
                        │  Schema Diff · Impact · Audit · Dict  │
                        └──────────────┬───────────────────────┘
                                       │ REST / JSON
                        ┌──────────────▼───────────────────────┐
                        │         FastAPI Backend (8000)        │
                        │  /datasets  /lineage  /schemas        │
                        │  /impact    /audit    /governance     │
                        │  /search    /dictionary /pipelines    │
                        └──────┬────────────────┬──────────────┘
                               │                │
              ┌────────────────▼──┐    ┌────────▼──────────────┐
              │  PostgreSQL (5432) │    │    Neo4j (7687/7474)  │
              │  - datasets        │    │  - Dataset nodes       │
              │  - dataset_columns │    │  - Column nodes        │
              │  - lineage_edges   │    │  - FEEDS_INTO edges    │
              │  - schema_versions │    │  - COLUMN_MAPS_TO      │
              │  - governance_recs │    │  - Impact traversal    │
              │  - audit_logs      │    │  - Shortest path       │
              │  - data_dictionary │    │  - Orphan detection    │
              └───────────────────┘    └───────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         │              │              │
┌────────▼──────┐ ┌─────▼──────┐ ┌────▼──────────┐
│ Airflow (8080) │ │  dbt Core  │ │ Marquez (5000) │
│  governance_   │ │  staging   │ │  OpenLineage   │
│  metadata_sync │ │  mart      │ │  events ingest │
│  Daily 02:00   │ │  gold      │ │  API consumers │
└───────────────┘ └────────────┘ └───────────────┘
         │
┌────────▼──────┐
│  Redis (6379)  │
│  API cache     │
│  TTL: 300s     │
└───────────────┘
```

---

## Data Flow

### Metadata Registration Flow
```
Data Asset Created
      ↓
POST /api/v1/datasets
      ↓
Validate (owner, classification, qualified_name)
      ↓
Persist → PostgreSQL (datasets table)
      ↓
Sync → Neo4j (upsert Dataset node)
      ↓
Write → AuditLog (CREATE action)
      ↓
Return 201 {id, qualified_name}
```

### Lineage Registration Flow
```
Pipeline Executes
      ↓
POST /api/v1/lineage  OR  OpenLineage event → Marquez
      ↓
Validate source + target datasets exist
      ↓
Persist → PostgreSQL (lineage_edges table)
      ↓
Create → Neo4j FEEDS_INTO relationship
      ↓
If column_mappings: create COLUMN_MAPS_TO edges
      ↓
AuditLog entry
```

### Schema Evolution Flow
```
Airflow DAG: profile_source_schemas
      ↓
Introspect live schema from source system
      ↓
POST /api/v1/schemas/{id}/versions
      ↓
detect_changes(old_snapshot, new_snapshot)
      ↓
Classify each change (ADDED/REMOVED/TYPE_CHANGED/NULLABLE_CHANGED)
      ↓
Determine is_breaking (type compatibility matrix)
      ↓
Persist → SchemaVersion (immutable snapshot)
      ↓
Write → AuditLog
      ↓
If breaking: trigger alert (Slack / PagerDuty / Email)
```

### Impact Analysis Flow
```
GET /api/v1/impact/{dataset_id}
      ↓
Neo4j Cypher:
  MATCH (src {id})-[:FEEDS_INTO*]->(affected)
  RETURN COUNT(affected), COLLECT(affected)
      ↓
Classify risk (CRITICAL >50 / HIGH >20 / MEDIUM >5 / LOW)
      ↓
Return blast radius with recommendation
```

---

## Neo4j Graph Schema

### Node Types
```
(:Dataset {
    id, qualified_name, display_name,
    source_system, domain, classification,
    owner_team, node_type, is_pii
})

(:Column {
    id, name, data_type, is_pii, is_pk
})

(:Pipeline {
    id, name, type, schedule
})
```

### Relationship Types
```
(Dataset)-[:HAS_COLUMN]->(Column)
(Dataset)-[:FEEDS_INTO {
    pipeline_id, transformation_type
}]->(Dataset)
(Column)-[:COLUMN_MAPS_TO {
    transformation
}]->(Column)
```

---

## Governance Score Model

```
Score = doc_score + owner_score + class_score + lineage_score + recency_score

doc_score     = min(documented_columns / total_columns * 100 * 0.30, 30)
owner_score   = 20 if owner_email is set else 0
class_score   = 20 if classification != DRAFT else 0  
lineage_score = min((upstream + downstream edges) * 5, 20)
recency_score = 10 if profiled ≤7d | 5 if ≤30d | 0 otherwise

Tiers:
  PLATINUM: score ≥ 80  →  Fully governed
  GOLD:     score 60–79  →  Well governed
  SILVER:   score 40–59  →  In progress
  BRONZE:   score < 40   →  Needs attention
```

---

## Breaking Change Detection Matrix

```
Type Transition          Breaking?  Reason
─────────────────────────────────────────────────
INTEGER → BIGINT         No         Widening
FLOAT → INTEGER          Yes        Precision loss
VARCHAR → TEXT           No         Widening
TEXT → VARCHAR           Yes        Truncation risk
DATE → TIMESTAMP         No         Widening
TIMESTAMP → DATE         Yes        Data loss
NOT NULL column added    Yes        INSERT fails downstream
Nullable column added    No         Safe addition
Column removed           Yes (always) Downstream breaks
Column renamed           Yes        Downstream breaks
NOT NULL → NULLABLE      No         Relaxation (safe)
NULLABLE → NOT NULL      Yes        Existing NULLs fail
```

---

## API Authentication (Production)

In production, add JWT middleware:

```python
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

security = HTTPBearer()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
        return {"email": payload["sub"], "role": payload["role"]}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Apply to all routes:
app.include_router(datasets.router, dependencies=[Depends(get_current_user)])
```

Roles:
- `platform_admin` — full access
- `data_steward` — approve governance records, manage dictionary
- `data_analyst` — read catalog, register datasets
- `pipeline_engineer` — register lineage, manage pipelines
- `readonly` — search and browse only

---

## Deployment (Kubernetes)

```yaml
# governance-api deployment excerpt
apiVersion: apps/v1
kind: Deployment
metadata:
  name: governance-api
  namespace: data-platform
spec:
  replicas: 3
  selector:
    matchLabels:
      app: governance-api
  template:
    spec:
      containers:
      - name: api
        image: enterprise/governance-api:2.1.0
        ports:
        - containerPort: 8000
        env:
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: governance-secrets
              key: postgres-password
        resources:
          requests: { cpu: "250m", memory: "512Mi" }
          limits:   { cpu: "1000m", memory: "2Gi" }
        livenessProbe:
          httpGet: { path: /health, port: 8000 }
          initialDelaySeconds: 30
```

---

## Performance Considerations

| Concern | Approach |
|---------|----------|
| Lineage graph queries | Neo4j native traversal (sub-100ms for depth ≤5) |
| Catalog search | PostgreSQL GIN index on `tsvector` for FTS |
| Schema version diffs | Computed on-demand; cached in Redis (TTL 5min) |
| API response caching | Redis cache for read-heavy endpoints |
| Audit log writes | Async fire-and-forget; non-blocking |
| Impact analysis | Neo4j Cypher aggregation; indexed by dataset_id |

---

## Monitoring

Key metrics to track in Prometheus / Datadog:

```
governance_api_request_duration_seconds (p50, p95, p99)
governance_api_error_rate
governance_datasets_registered_total
governance_breaking_changes_detected_total
governance_neo4j_query_duration_seconds
governance_schema_versions_per_day
governance_orphaned_datasets_count
governance_approval_rate_pct
```
