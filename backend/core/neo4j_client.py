"""
Neo4j Lineage Graph Engine
Manages graph-based lineage storage and traversal using Cypher queries.

Node Types:   Dataset, Column, Pipeline, Report, Dashboard
Edge Types:   DERIVED_FROM, FEEDS_INTO, TRANSFORMS, COLUMN_MAPS_TO
"""

from neo4j import AsyncGraphDatabase
from typing import Optional, List, Dict, Any
import logging
from core.config import settings

logger = logging.getLogger(__name__)

_driver = None


async def init_neo4j():
    global _driver
    _driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    await _ensure_constraints()
    logger.info("Neo4j lineage graph initialized.")


async def get_driver():
    return _driver


async def _ensure_constraints():
    """Idempotent schema constraints for graph integrity."""
    constraints = [
        "CREATE CONSTRAINT dataset_id IF NOT EXISTS FOR (d:Dataset) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT column_id  IF NOT EXISTS FOR (c:Column)  REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT pipeline_id IF NOT EXISTS FOR (p:Pipeline) REQUIRE p.id IS UNIQUE",
        "CREATE INDEX dataset_qname IF NOT EXISTS FOR (d:Dataset) ON (d.qualified_name)",
        "CREATE INDEX dataset_domain IF NOT EXISTS FOR (d:Dataset) ON (d.domain)",
    ]
    async with _driver.session() as session:
        for cql in constraints:
            try:
                await session.run(cql)
            except Exception as e:
                logger.warning(f"Constraint already exists or failed: {e}")


# ── Graph Write Operations ────────────────────────────────────────────────────

async def upsert_dataset_node(dataset: Dict[str, Any]):
    """Merge a dataset node into the lineage graph."""
    cql = """
    MERGE (d:Dataset {id: $id})
    SET d.qualified_name  = $qualified_name,
        d.display_name    = $display_name,
        d.source_system   = $source_system,
        d.domain          = $domain,
        d.classification  = $classification,
        d.owner_team      = $owner_team,
        d.node_type       = $node_type,
        d.is_pii          = $is_pii,
        d.updated_at      = timestamp()
    RETURN d
    """
    async with _driver.session() as session:
        result = await session.run(cql, **dataset)
        return await result.single()


async def upsert_column_node(column: Dict[str, Any]):
    """Merge a column node and link it to its parent dataset."""
    cql = """
    MATCH (d:Dataset {id: $dataset_id})
    MERGE (c:Column {id: $id})
    SET c.name         = $column_name,
        c.data_type    = $data_type,
        c.is_pii       = $is_pii,
        c.is_pk        = $is_primary_key
    MERGE (d)-[:HAS_COLUMN]->(c)
    RETURN c
    """
    async with _driver.session() as session:
        result = await session.run(cql, **column)
        return await result.single()


async def upsert_lineage_edge(
    source_id: str,
    target_id: str,
    pipeline_id: Optional[str],
    transformation_type: str,
    column_mappings: Optional[Dict] = None
):
    """
    Create or update a FEEDS_INTO relationship in the graph.
    Also creates COLUMN_MAPS_TO edges for column-level lineage.
    """
    # Dataset-level edge
    cql = """
    MATCH (src:Dataset {id: $source_id})
    MATCH (tgt:Dataset {id: $target_id})
    MERGE (src)-[e:FEEDS_INTO]->(tgt)
    SET e.pipeline_id        = $pipeline_id,
        e.transformation_type = $transformation_type,
        e.updated_at         = timestamp()
    RETURN e
    """
    async with _driver.session() as session:
        await session.run(
            cql,
            source_id=source_id,
            target_id=target_id,
            pipeline_id=pipeline_id,
            transformation_type=transformation_type
        )

        # Column-level lineage edges
        if column_mappings:
            col_cql = """
            MATCH (src_col:Column {id: $src_col_id})
            MATCH (tgt_col:Column {id: $tgt_col_id})
            MERGE (src_col)-[m:COLUMN_MAPS_TO]->(tgt_col)
            SET m.transformation = $transformation
            """
            for tgt_col, mapping in column_mappings.items():
                await session.run(
                    col_cql,
                    src_col_id=mapping.get("source_column_id"),
                    tgt_col_id=mapping.get("target_column_id"),
                    transformation=mapping.get("transformation", "DIRECT")
                )


# ── Graph Read Operations (Lineage Traversal) ─────────────────────────────────

async def get_upstream_lineage(dataset_id: str, depth: int = 5) -> Dict:
    """
    Traverse upstream (ancestors) of a dataset up to `depth` hops.
    Returns nodes and edges for graph visualization.
    """
    cql = """
    MATCH path = (src:Dataset)-[:FEEDS_INTO*1..$depth]->(tgt:Dataset {id: $dataset_id})
    UNWIND nodes(path) AS n
    UNWIND relationships(path) AS r
    RETURN 
        COLLECT(DISTINCT {
            id: n.id, label: n.qualified_name, display: n.display_name,
            domain: n.domain, source_system: n.source_system,
            classification: n.classification, node_type: n.node_type
        }) AS nodes,
        COLLECT(DISTINCT {
            source: startNode(r).id, target: endNode(r).id,
            type: r.transformation_type
        }) AS edges
    """
    async with _driver.session() as session:
        result = await session.run(cql, dataset_id=dataset_id, depth=depth)
        record = await result.single()
        return {
            "nodes": record["nodes"] if record else [],
            "edges": record["edges"] if record else []
        }


async def get_downstream_lineage(dataset_id: str, depth: int = 5) -> Dict:
    """
    Traverse downstream (descendants) — used for impact analysis.
    """
    cql = """
    MATCH path = (src:Dataset {id: $dataset_id})-[:FEEDS_INTO*1..$depth]->(tgt:Dataset)
    UNWIND nodes(path) AS n
    UNWIND relationships(path) AS r
    RETURN
        COLLECT(DISTINCT {
            id: n.id, label: n.qualified_name, display: n.display_name,
            domain: n.domain, source_system: n.source_system,
            classification: n.classification, node_type: n.node_type,
            is_pii: n.is_pii
        }) AS nodes,
        COLLECT(DISTINCT {
            source: startNode(r).id, target: endNode(r).id,
            type: r.transformation_type
        }) AS edges
    """
    async with _driver.session() as session:
        result = await session.run(cql, dataset_id=dataset_id, depth=depth)
        record = await result.single()
        return {
            "nodes": record["nodes"] if record else [],
            "edges": record["edges"] if record else []
        }


async def get_full_lineage_graph(dataset_id: str, depth: int = 5) -> Dict:
    """
    Full bidirectional lineage: upstream + downstream combined.
    Powers the interactive lineage visualization.
    """
    upstream   = await get_upstream_lineage(dataset_id, depth)
    downstream = await get_downstream_lineage(dataset_id, depth)

    # Merge and deduplicate
    all_nodes = {n["id"]: n for n in upstream["nodes"] + downstream["nodes"]}
    all_edges = {
        f"{e['source']}->{e['target']}": e
        for e in upstream["edges"] + downstream["edges"]
    }

    # Mark root node
    if dataset_id in all_nodes:
        all_nodes[dataset_id]["is_root"] = True

    return {
        "root_id": dataset_id,
        "nodes": list(all_nodes.values()),
        "edges": list(all_edges.values()),
        "upstream_depth": len(upstream["nodes"]),
        "downstream_depth": len(downstream["nodes"])
    }


async def find_shortest_path(source_id: str, target_id: str) -> Dict:
    """Shortest lineage path between any two datasets."""
    cql = """
    MATCH path = shortestPath(
        (src:Dataset {id: $source_id})-[:FEEDS_INTO*]-(tgt:Dataset {id: $target_id})
    )
    RETURN [n IN nodes(path) | n.qualified_name] AS path_names,
           length(path) AS hops
    """
    async with _driver.session() as session:
        result = await session.run(cql, source_id=source_id, target_id=target_id)
        record = await result.single()
        return {
            "path": record["path_names"] if record else [],
            "hops": record["hops"] if record else -1
        }


async def get_impact_assessment(dataset_id: str) -> Dict:
    """
    Impact analysis: how many downstream assets would be affected
    if this dataset fails or changes schema.
    """
    cql = """
    MATCH (src:Dataset {id: $dataset_id})-[:FEEDS_INTO*]->(affected:Dataset)
    RETURN
        COUNT(DISTINCT affected) AS affected_count,
        COLLECT(DISTINCT affected.domain) AS affected_domains,
        COLLECT(DISTINCT {
            id: affected.id,
            name: affected.qualified_name,
            domain: affected.domain,
            node_type: affected.node_type,
            classification: affected.classification
        }) AS affected_assets
    """
    async with _driver.session() as session:
        result = await session.run(cql, dataset_id=dataset_id)
        record = await result.single()
        return {
            "affected_count":   record["affected_count"] if record else 0,
            "affected_domains": record["affected_domains"] if record else [],
            "affected_assets":  record["affected_assets"] if record else []
        }


async def get_orphaned_datasets() -> List[Dict]:
    """Find datasets with no lineage connections (governance alert)."""
    cql = """
    MATCH (d:Dataset)
    WHERE NOT (d)-[:FEEDS_INTO]-() AND NOT ()-[:FEEDS_INTO]->(d)
    RETURN d.id AS id, d.qualified_name AS name, d.domain AS domain, d.owner_team AS owner
    """
    async with _driver.session() as session:
        result = await session.run(cql)
        return [dict(r) async for r in result]


async def get_cross_domain_flows() -> List[Dict]:
    """
    Detect data flows crossing domain boundaries — key for data mesh governance.
    """
    cql = """
    MATCH (src:Dataset)-[e:FEEDS_INTO]->(tgt:Dataset)
    WHERE src.domain <> tgt.domain
    RETURN src.domain AS from_domain, tgt.domain AS to_domain,
           COUNT(e) AS edge_count,
           COLLECT(DISTINCT src.qualified_name)[..5] AS sample_sources
    ORDER BY edge_count DESC
    LIMIT 20
    """
    async with _driver.session() as session:
        result = await session.run(cql)
        return [dict(r) async for r in result]
