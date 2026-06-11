"""
Schema Evolution Service
Tracks schema changes over time, detects breaking changes,
and triggers governance alerts when critical changes occur.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging
from models.models import SchemaVersion, Dataset, ChangeType, AuditLog, AuditAction
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

logger = logging.getLogger(__name__)

# Column type compatibility matrix — determines if a type change is breaking
TYPE_COMPATIBILITY = {
    # (from_type, to_type) -> is_breaking
    ("INTEGER", "BIGINT"):      False,  # Widening — safe
    ("INTEGER", "FLOAT"):       False,
    ("FLOAT",   "INTEGER"):     True,   # Narrowing — breaking
    ("VARCHAR", "TEXT"):        False,
    ("TEXT",    "VARCHAR"):     True,   # Truncation risk — breaking
    ("DATE",    "TIMESTAMP"):   False,
    ("TIMESTAMP", "DATE"):      True,
    ("BOOLEAN", "INTEGER"):     False,
    ("INTEGER", "BOOLEAN"):     True,
}


def _is_type_breaking(from_type: str, to_type: str) -> bool:
    """Determine if a datatype transition is backward-incompatible."""
    key = (from_type.upper(), to_type.upper())
    return TYPE_COMPATIBILITY.get(key, True)   # Unknown transitions default to breaking


def detect_changes(
    old_schema: Dict,
    new_schema: Dict
) -> Tuple[List[Dict], bool]:
    """
    Diff two schema snapshots and classify each change.
    
    Args:
        old_schema: {"columns": {col_name: {type, nullable, ...}}}
        new_schema:  same format
    
    Returns:
        (changes: List[ChangeEvent], has_breaking_change: bool)
    """
    old_cols = old_schema.get("columns", {})
    new_cols = new_schema.get("columns", {})

    changes: List[Dict] = []
    is_breaking = False

    # Removed columns (always breaking)
    for col in set(old_cols) - set(new_cols):
        changes.append({
            "type": ChangeType.COLUMN_REMOVED,
            "column": col,
            "old_definition": old_cols[col],
            "is_breaking": True,
            "description": f"Column '{col}' was removed — downstream consumers will fail."
        })
        is_breaking = True

    # Added columns
    for col in set(new_cols) - set(old_cols):
        col_def = new_cols[col]
        breaking = not col_def.get("nullable", True)  # NOT NULL without default = breaking
        changes.append({
            "type": ChangeType.COLUMN_ADDED,
            "column": col,
            "new_definition": col_def,
            "is_breaking": breaking,
            "description": (
                f"Column '{col}' added as NOT NULL without default — "
                "INSERT statements on downstream tables may fail."
            ) if breaking else f"Column '{col}' added (nullable — non-breaking)."
        })
        if breaking:
            is_breaking = True

    # Modified columns
    for col in set(old_cols) & set(new_cols):
        old_def = old_cols[col]
        new_def = new_cols[col]

        # Guard against malformed (non-dict) column definitions
        if not isinstance(old_def, dict) or not isinstance(new_def, dict):
            changes.append({
                "type": ChangeType.TYPE_CHANGED,
                "column": col,
                "is_breaking": True,
                "description": f"Column '{col}' has a malformed definition (not a dict). Treating as breaking change."
            })
            is_breaking = True
            continue

        # Type change
        if old_def.get("type") != new_def.get("type"):
            breaking = _is_type_breaking(
                old_def.get("type", "UNKNOWN"),
                new_def.get("type", "UNKNOWN")
            )
            changes.append({
                "type": ChangeType.TYPE_CHANGED,
                "column": col,
                "old_type": old_def.get("type"),
                "new_type": new_def.get("type"),
                "is_breaking": breaking,
                "description": (
                    f"Column '{col}' type changed from {old_def.get('type')} → "
                    f"{new_def.get('type')} ({'BREAKING' if breaking else 'compatible'})."
                )
            })
            if breaking:
                is_breaking = True

        # Nullable change
        if old_def.get("nullable") != new_def.get("nullable"):
            breaking = not new_def.get("nullable", True)  # Becoming NOT NULL is breaking
            changes.append({
                "type": ChangeType.NULLABLE_CHANGED,
                "column": col,
                "old_nullable": old_def.get("nullable"),
                "new_nullable": new_def.get("nullable"),
                "is_breaking": breaking,
                "description": (
                    f"Column '{col}' changed to NOT NULL — existing NULLs will cause constraint violations."
                ) if breaking else f"Column '{col}' relaxed to nullable."
            })
            if breaking:
                is_breaking = True

    return changes, is_breaking


async def record_schema_version(
    session: AsyncSession,
    dataset: Dataset,
    new_schema: Dict,
    authored_by: str,
    change_notes: Optional[str] = None
) -> SchemaVersion:
    """
    Compare new schema against the latest version, detect changes,
    and persist a new immutable SchemaVersion record.
    """
    # Fetch latest version
    stmt = (
        select(SchemaVersion)
        .where(SchemaVersion.dataset_id == dataset.id)
        .order_by(desc(SchemaVersion.version))
        .limit(1)
    )
    result = await session.execute(stmt)
    latest = result.scalar_one_or_none()

    version_number = (latest.version + 1) if latest else 1
    changes = []
    is_breaking = False

    if latest:
        changes, is_breaking = detect_changes(
            old_schema=latest.schema_snapshot,
            new_schema=new_schema
        )
        logger.info(
            f"Schema diff for '{dataset.qualified_name}': "
            f"{len(changes)} changes, breaking={is_breaking}"
        )

    sv = SchemaVersion(
        dataset_id=dataset.id,
        version=version_number,
        schema_snapshot=new_schema,
        changes=changes,
        is_breaking=is_breaking,
        authored_by=authored_by,
        change_notes=change_notes
    )
    session.add(sv)

    # Audit log
    audit = AuditLog(
        entity_type="SchemaVersion",
        entity_id=str(dataset.id),
        action=AuditAction.CREATE,
        actor=authored_by,
        change_summary=(
            f"Schema version {version_number} recorded. "
            f"{len(changes)} change(s), breaking={is_breaking}"
        ),
        after_state={"version": version_number, "is_breaking": is_breaking, "changes": changes}
    )
    session.add(audit)
    await session.commit()

    if is_breaking:
        await _raise_breaking_change_alert(dataset, sv, changes)

    return sv


async def get_schema_diff(
    session: AsyncSession,
    dataset_id: str,
    from_version: int,
    to_version: int
) -> Dict:
    """Returns a structured diff between two schema versions."""
    stmt = select(SchemaVersion).where(
        SchemaVersion.dataset_id == dataset_id,
        SchemaVersion.version.in_([from_version, to_version])
    )
    result = await session.execute(stmt)
    versions = {sv.version: sv for sv in result.scalars()}

    if from_version not in versions or to_version not in versions:
        raise ValueError(f"Versions {from_version} or {to_version} not found.")

    old_sv = versions[from_version]
    new_sv = versions[to_version]
    changes, is_breaking = detect_changes(old_sv.schema_snapshot, new_sv.schema_snapshot)

    return {
        "dataset_id":   str(dataset_id),
        "from_version": from_version,
        "to_version":   to_version,
        "is_breaking":  is_breaking,
        "change_count": len(changes),
        "changes":      changes,
        "from_snapshot": old_sv.schema_snapshot,
        "to_snapshot":   new_sv.schema_snapshot
    }


async def _raise_breaking_change_alert(
    dataset: Dataset,
    schema_version: SchemaVersion,
    changes: List[Dict]
):
    """
    Emit alerts for breaking schema changes.
    In production: integrates with PagerDuty / Slack / Email.
    """
    breaking_changes = [c for c in changes if c.get("is_breaking")]
    logger.warning(
        f"🚨 BREAKING SCHEMA CHANGE DETECTED | "
        f"Dataset: {dataset.qualified_name} | "
        f"Version: {schema_version.version} | "
        f"Breaking changes: {len(breaking_changes)}"
    )
    for change in breaking_changes:
        # FIX: change['type'] may be a ChangeType enum OR a plain string (from JSON column)
        ctype = change["type"].value if hasattr(change["type"], "value") else str(change["type"])
        logger.warning(f"  ↳ {ctype}: {change['description']}")

    # TODO: Post to Slack webhook, send email alert, create PagerDuty incident
    # await slack_client.post_alert(channel="#data-governance-alerts", ...)
    # await email_client.send(to=dataset.owner_email, subject="Breaking Schema Change", ...)
