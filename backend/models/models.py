"""
SQLAlchemy ORM Models — Enterprise Governance Platform
Covers: Datasets, Columns, Lineage Edges, Schema Versions,
        Governance Records, Audit Logs, Pipelines, Ownership
"""

from datetime import datetime, timezone

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)
from typing import Optional, List
from sqlalchemy import (
    Column, String, Text, Integer, Float, Boolean, DateTime,
    ForeignKey, JSON, Enum, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship, DeclarativeBase
from sqlalchemy.dialects.postgresql import UUID, ARRAY
import uuid
import enum


class Base(DeclarativeBase):
    pass


# ── Enums ─────────────────────────────────────────────────────────────────────

class DataClassification(str, enum.Enum):
    PUBLIC       = "PUBLIC"
    INTERNAL     = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED   = "RESTRICTED"   # PII / PCI / HIPAA


class GovernanceStatus(str, enum.Enum):
    DRAFT     = "DRAFT"
    PENDING   = "PENDING_REVIEW"
    APPROVED  = "APPROVED"
    REJECTED  = "REJECTED"
    DEPRECATED = "DEPRECATED"


class ChangeType(str, enum.Enum):
    COLUMN_ADDED    = "COLUMN_ADDED"
    COLUMN_REMOVED  = "COLUMN_REMOVED"
    TYPE_CHANGED    = "TYPE_CHANGED"
    NULLABLE_CHANGED= "NULLABLE_CHANGED"
    RENAMED         = "RENAMED"
    TABLE_ADDED     = "TABLE_ADDED"
    TABLE_REMOVED   = "TABLE_REMOVED"


class LineageNodeType(str, enum.Enum):
    TABLE       = "TABLE"
    VIEW        = "VIEW"
    STREAM      = "STREAM"
    API         = "API"
    REPORT      = "REPORT"
    DASHBOARD   = "DASHBOARD"
    ML_MODEL    = "ML_MODEL"
    EXTERNAL    = "EXTERNAL"


class AuditAction(str, enum.Enum):
    CREATE   = "CREATE"
    UPDATE   = "UPDATE"
    DELETE   = "DELETE"
    APPROVE  = "APPROVE"
    REJECT   = "REJECT"
    ACCESS   = "ACCESS"
    PUBLISH  = "PUBLISH"


# ── Core Models ───────────────────────────────────────────────────────────────

class Dataset(Base):
    """Central registry entry for every tracked data asset."""
    __tablename__ = "datasets"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name            = Column(String(255), nullable=False)
    qualified_name  = Column(String(512), unique=True, nullable=False)  # schema.table
    display_name    = Column(String(255))
    description     = Column(Text)
    source_system   = Column(String(100))       # Snowflake, Postgres, S3, etc.
    database_name   = Column(String(100))
    schema_name     = Column(String(100))
    table_name      = Column(String(100))
    node_type       = Column(Enum(LineageNodeType), default=LineageNodeType.TABLE)
    classification  = Column(Enum(DataClassification), default=DataClassification.INTERNAL)
    governance_status = Column(Enum(GovernanceStatus), default=GovernanceStatus.DRAFT)
    owner_team      = Column(String(100))
    owner_email     = Column(String(255))
    domain          = Column(String(100))       # Finance, Risk, Marketing, etc.
    update_frequency = Column(String(50))       # DAILY, HOURLY, REAL_TIME
    row_count       = Column(Integer)
    size_bytes      = Column(Float)
    tags            = Column(ARRAY(String), default=list)
    custom_metadata = Column(JSON, default=dict)
    is_pii          = Column(Boolean, default=False, server_default='false')
    is_active       = Column(Boolean, default=True, server_default='true')
    created_at      = Column(DateTime, default=_utcnow)
    updated_at      = Column(DateTime, default=_utcnow, onupdate=_utcnow)
    last_profiled_at = Column(DateTime)
    dbt_model_path  = Column(String(512))
    airflow_dag_id  = Column(String(255))

    # Relationships
    columns         = relationship("DatasetColumn", back_populates="dataset", cascade="all, delete-orphan")
    schema_versions = relationship("SchemaVersion", back_populates="dataset", cascade="all, delete-orphan")
    governance_records = relationship("GovernanceRecord", back_populates="dataset")
    outbound_lineage = relationship("LineageEdge", foreign_keys="LineageEdge.source_dataset_id", back_populates="source")
    inbound_lineage  = relationship("LineageEdge", foreign_keys="LineageEdge.target_dataset_id", back_populates="target")

    __table_args__ = (
        Index("idx_dataset_source_system", "source_system"),
        Index("idx_dataset_domain", "domain"),
        Index("idx_dataset_classification", "classification"),
        Index("idx_dataset_owner", "owner_email"),
    )


class DatasetColumn(Base):
    """Column-level metadata including type, description, lineage hooks."""
    __tablename__ = "dataset_columns"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id      = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    column_name     = Column(String(255), nullable=False)
    display_name    = Column(String(255))
    data_type       = Column(String(100))
    is_nullable     = Column(Boolean, default=True)
    is_primary_key  = Column(Boolean, default=False)
    is_foreign_key  = Column(Boolean, default=False)
    is_pii          = Column(Boolean, default=False)
    classification  = Column(Enum(DataClassification))
    business_definition = Column(Text)
    technical_definition = Column(Text)
    example_values  = Column(ARRAY(String))
    validation_rules = Column(JSON, default=dict)   # {min, max, regex, allowed_values}
    source_column   = Column(String(255))            # upstream column reference
    transformation_logic = Column(Text)
    ordinal_position = Column(Integer)
    created_at      = Column(DateTime, default=_utcnow)
    updated_at      = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    dataset = relationship("Dataset", back_populates="columns")

    __table_args__ = (
        UniqueConstraint("dataset_id", "column_name", name="uq_dataset_column"),
    )


class LineageEdge(Base):
    """
    Directed edge in the lineage graph (also mirrored in Neo4j).
    Represents a data flow: source → target via a pipeline/transformation.
    """
    __tablename__ = "lineage_edges"

    id                 = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_dataset_id  = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    target_dataset_id  = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    pipeline_id        = Column(UUID(as_uuid=True), ForeignKey("pipelines.id"))
    transformation_type = Column(String(100))   # JOIN, AGGREGATE, FILTER, ENRICH
    transformation_sql  = Column(Text)
    column_mappings    = Column(JSON, default=dict)   # {target_col: source_col}
    openlineage_run_id = Column(String(255))
    is_active          = Column(Boolean, default=True)
    created_at         = Column(DateTime, default=_utcnow)
    updated_at         = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    source   = relationship("Dataset", foreign_keys=[source_dataset_id], back_populates="outbound_lineage")
    target   = relationship("Dataset", foreign_keys=[target_dataset_id], back_populates="inbound_lineage")
    pipeline = relationship("Pipeline", back_populates="lineage_edges")

    __table_args__ = (
        UniqueConstraint("source_dataset_id", "target_dataset_id", "pipeline_id",
                         name="uq_lineage_edge"),
        Index("idx_lineage_source", "source_dataset_id"),
        Index("idx_lineage_target", "target_dataset_id"),
    )


class SchemaVersion(Base):
    """
    Immutable schema snapshot at a point in time.
    Enables version diffing and breaking change detection.
    """
    __tablename__ = "schema_versions"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id   = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    version      = Column(Integer, nullable=False)
    schema_snapshot = Column(JSON, nullable=False)   # Full column definitions at this version
    changes      = Column(JSON, default=list)        # List of ChangeEvent dicts
    is_breaking  = Column(Boolean, default=False)
    authored_by  = Column(String(255))
    change_notes = Column(Text)
    approved_by  = Column(String(255))
    created_at   = Column(DateTime, default=_utcnow)

    dataset = relationship("Dataset", back_populates="schema_versions")

    __table_args__ = (
        UniqueConstraint("dataset_id", "version", name="uq_schema_version"),
        Index("idx_schema_version_dataset", "dataset_id"),
    )


class Pipeline(Base):
    """ETL/ELT pipeline registry — Airflow DAGs, dbt runs, custom jobs."""
    __tablename__ = "pipelines"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name            = Column(String(255), nullable=False)
    pipeline_type   = Column(String(50))   # AIRFLOW_DAG, DBT_MODEL, SPARK_JOB, CUSTOM
    dag_id          = Column(String(255))
    schedule        = Column(String(100))  # Cron expression
    owner_team      = Column(String(100))
    owner_email     = Column(String(255))
    description     = Column(Text)
    sla_minutes     = Column(Integer)
    last_run_at     = Column(DateTime)
    last_run_status = Column(String(50))   # SUCCESS, FAILED, RUNNING
    last_run_duration_seconds = Column(Integer)
    is_active       = Column(Boolean, default=True)
    tags            = Column(ARRAY(String), default=list)
    created_at      = Column(DateTime, default=_utcnow)
    updated_at      = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    lineage_edges = relationship("LineageEdge", back_populates="pipeline")


class GovernanceRecord(Base):
    """Tracks ownership assignment, review cycles, and approval status per dataset."""
    __tablename__ = "governance_records"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id      = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    status          = Column(Enum(GovernanceStatus), default=GovernanceStatus.DRAFT)
    assigned_steward = Column(String(255))
    reviewed_by     = Column(String(255))
    review_notes    = Column(Text)
    certification_date = Column(DateTime)
    next_review_date   = Column(DateTime)
    compliance_tags  = Column(ARRAY(String), default=list)  # GDPR, SOX, PCI, HIPAA
    data_retention_days = Column(Integer)
    access_control_group = Column(String(255))
    created_at      = Column(DateTime, default=_utcnow)
    updated_at      = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    dataset = relationship("Dataset", back_populates="governance_records")


class AuditLog(Base):
    """
    Immutable audit trail — every write operation on governed entities
    is recorded here for SOX / compliance purposes.
    """
    __tablename__ = "audit_logs"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type     = Column(String(100), nullable=False)   # Dataset, Column, Lineage, etc.
    entity_id       = Column(String(255), nullable=False)
    action          = Column(Enum(AuditAction), nullable=False)
    actor           = Column(String(255), nullable=False)   # user@enterprise.com
    actor_role      = Column(String(100))
    before_state    = Column(JSON)
    after_state     = Column(JSON)
    change_summary  = Column(Text)
    ip_address      = Column(String(45))
    correlation_id  = Column(String(255))
    session_id      = Column(String(255))
    created_at      = Column(DateTime, default=_utcnow, nullable=False)

    __table_args__ = (
        Index("idx_audit_entity", "entity_type", "entity_id"),
        Index("idx_audit_actor", "actor"),
        Index("idx_audit_created_at", "created_at"),
    )


class DataDictionary(Base):
    """Auto-generated and curated business glossary entries."""
    __tablename__ = "data_dictionary"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    term            = Column(String(255), unique=True, nullable=False)
    business_definition = Column(Text, nullable=False)
    technical_definition = Column(Text)
    domain          = Column(String(100))
    synonyms        = Column(ARRAY(String), default=list)
    related_datasets = Column(ARRAY(String), default=list)  # qualified_names
    related_columns  = Column(ARRAY(String), default=list)
    steward         = Column(String(255))
    is_certified    = Column(Boolean, default=False)
    source_system   = Column(String(100))
    created_at      = Column(DateTime, default=_utcnow)
    updated_at      = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index("idx_dictionary_domain", "domain"),
        Index("idx_dictionary_certified", "is_certified"),
    )
