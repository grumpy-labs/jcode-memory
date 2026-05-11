#!/usr/bin/env python3
"""Probe DuckDB against jcode broker graph/storage access patterns.

This is intentionally a proof harness, not a production adapter. It answers the
Phase 5 question: can DuckDB model the broker/Vault graph well enough to stay
in the operational-store race, and what caveats show up immediately?
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable


OPERATIONAL_WARNINGS = [
    "DuckDB has a single writer process constraint for native database writes; a broker service would need to own writes.",
    "DuckDB is optimized for analytical/bulk workloads, so frequent tiny agent transactions need explicit benchmarking.",
    "DuckDB FTS indexes do not refresh automatically after table changes; ingestion must rebuild or refresh indexes deterministically.",
    "DuckDB VSS persistent HNSW indexes are still guarded by experimental persistence settings; treat vector indexes as rebuildable.",
    "DuckPGQ is a community extension under active development; keep a recursive-CTE fallback for graph traversal proofs.",
]


@dataclasses.dataclass(frozen=True)
class GraphProofRecords:
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    chunks: list[dict[str, Any]]


@dataclasses.dataclass
class ProbeCheck:
    name: str
    status: str
    elapsed_ms: int
    detail: str
    rows: list[Any] = dataclasses.field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def sample_records() -> GraphProofRecords:
    """Return a tiny corpus that models broker memories plus Vault evidence."""

    nodes = [
        {
            "id": "mem_prov_transcript_1",
            "kind": "memory",
            "title": "hidden transcript provenance",
            "content": "External transcript synced from hermes:session_end. Rob prefers focused broker tests.",
            "tags_json": json.dumps(["broker-provenance", "broker-transcript-sync"]),
            "source_uri": "hermes:session_end:phase5-proof",
            "checksum": "sha256:prov-1",
        },
        {
            "id": "mem_derived_broker_tests",
            "kind": "memory",
            "title": "derived memory",
            "content": "Rob prefers focused broker tests before broad Rust test runs.",
            "tags_json": json.dumps(["broker-derived", "focused-checks"]),
            "source_uri": "derived:hermes:session_end:phase5-proof",
            "checksum": "sha256:derived-1",
        },
        {
            "id": "vault_file_jcode_plan",
            "kind": "vault_file",
            "title": "jcode Nervous-System Broker Parity Plan",
            "content": "Vault source note for jcode broker parity planning.",
            "tags_json": json.dumps(["jcode", "broker", "vault"]),
            "source_uri": "vault://Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md",
            "checksum": "sha256:file-1",
        },
        {
            "id": "vault_chunk_jcode_plan_1",
            "kind": "vault_chunk",
            "title": "Phase 5 graph foundation chunk",
            "content": "Phase 5 evaluates DuckDB first, keeps SurrealDB as baseline, and plans whole-Vault ingestion.",
            "tags_json": json.dumps(["phase5", "duckdb", "surrealdb"]),
            "source_uri": "vault://Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md#phase-5",
            "checksum": "sha256:chunk-1",
        },
        {
            "id": "vault_task_sidecar_live_proof",
            "kind": "vault_task",
            "title": "Close live sidecar extraction caveat",
            "content": "Use an isolated Hermes profile and throwaway jcode home to prove live extraction.",
            "tags_json": json.dumps(["sidecar", "proof"]),
            "source_uri": "vault://Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md#7.6",
            "checksum": "sha256:task-1",
        },
    ]
    edges = [
        {
            "source_id": "mem_derived_broker_tests",
            "target_id": "mem_prov_transcript_1",
            "kind": "DerivedFrom",
            "weight": 0.7,
        },
        {
            "source_id": "vault_chunk_jcode_plan_1",
            "target_id": "vault_file_jcode_plan",
            "kind": "ChunkOf",
            "weight": 1.0,
        },
        {
            "source_id": "vault_task_sidecar_live_proof",
            "target_id": "vault_file_jcode_plan",
            "kind": "TaskIn",
            "weight": 1.0,
        },
        {
            "source_id": "vault_chunk_jcode_plan_1",
            "target_id": "mem_derived_broker_tests",
            "kind": "Mentions",
            "weight": 0.6,
        },
        {
            "source_id": "vault_task_sidecar_live_proof",
            "target_id": "mem_prov_transcript_1",
            "kind": "RequiresProofOf",
            "weight": 0.8,
        },
    ]
    chunks = [
        {
            "id": "vault_chunk_jcode_plan_1",
            "file_id": "vault_file_jcode_plan",
            "heading": "7.2 Graph DB candidate spike",
            "content": nodes[3]["content"],
            "embedding": [0.10, 0.20, 0.30],
            "checksum": "sha256:chunk-1",
        },
        {
            "id": "mem_derived_broker_tests",
            "file_id": "broker_memory_graph",
            "heading": "Derived broker memory",
            "content": nodes[1]["content"],
            "embedding": [0.11, 0.19, 0.31],
            "checksum": "sha256:derived-1",
        },
        {
            "id": "vault_task_sidecar_live_proof",
            "file_id": "vault_file_jcode_plan",
            "heading": "7.6 Live sidecar extraction proof",
            "content": nodes[4]["content"],
            "embedding": [0.90, 0.10, 0.05],
            "checksum": "sha256:task-1",
        },
    ]
    return GraphProofRecords(nodes=nodes, edges=edges, chunks=chunks)


def duckdb_probe_queries() -> dict[str, str]:
    return {
        "recursive_neighborhood": """
            WITH RECURSIVE walk(node_id, depth, path) AS (
                SELECT 'mem_derived_broker_tests', 0, 'mem_derived_broker_tests'
                UNION ALL
                SELECT e.target_id, w.depth + 1, w.path || '->' || e.target_id
                FROM walk w
                JOIN edges e ON e.source_id = w.node_id
                WHERE w.depth < 3 AND instr(w.path, e.target_id) = 0
            )
            SELECT node_id, depth, path
            FROM walk
            ORDER BY depth, node_id
        """,
        "link_neighborhood": """
            SELECT n.id, n.kind, n.title, e.kind AS edge_kind
            FROM nodes n
            JOIN edges e ON e.source_id = n.id
            WHERE e.target_id = 'mem_derived_broker_tests'
            ORDER BY n.id
        """,
        "fts_search": """
            SELECT id, score
            FROM (
                SELECT id, fts_main_chunks.match_bm25(id, 'focused broker tests') AS score
                FROM chunks
            ) sq
            WHERE score IS NOT NULL
            ORDER BY score DESC, id
            LIMIT 3
        """,
        "vector_search": """
            SELECT id, array_cosine_distance(embedding, [0.10, 0.20, 0.30]::FLOAT[3]) AS distance
            FROM chunks
            ORDER BY distance ASC, id
            LIMIT 3
        """,
        "duckpgq_property_graph": """
            FROM GRAPH_TABLE (memory_pg
                MATCH (a:nodes)-[e:edges]->(b:nodes)
                COLUMNS (a.id AS source_id, e.kind AS kind, b.id AS target_id)
            )
            ORDER BY source_id, target_id
        """,
    }


def run_probe(require_duckdb: bool = False, require_extensions: bool = False) -> dict[str, Any]:
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised by command-line use.
        status = "fail" if require_duckdb else "blocked"
        return result_payload(
            duckdb_available=False,
            checks=[
                ProbeCheck(
                    name="duckdb_import",
                    status=status,
                    elapsed_ms=0,
                    detail=f"DuckDB Python package unavailable: {exc}",
                )
            ],
            duckdb_version=None,
        )

    started = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    try:
        records = sample_records()
        load_records(con, records)
        checks = [
            timed_check("recursive_neighborhood", lambda: check_recursive_neighborhood(con)),
            timed_check("link_neighborhood", lambda: check_link_neighborhood(con)),
            timed_check("fts_search", lambda: check_fts(con)),
            timed_check("vector_search", lambda: check_vector(con)),
            timed_check("duckpgq_property_graph", lambda: check_duckpgq(con)),
            timed_check("parquet_export", lambda: check_parquet_export(con)),
        ]
    finally:
        con.close()

    payload = result_payload(
        duckdb_available=True,
        checks=checks,
        duckdb_version=getattr(duckdb, "__version__", None),
        elapsed_ms=int((time.perf_counter() - started) * 1000),
    )
    payload["exit_ok"] = should_exit_ok(payload, require_extensions=require_extensions)
    return payload


def load_records(con: Any, records: GraphProofRecords) -> None:
    con.execute(
        """
        CREATE TABLE nodes (
            id VARCHAR PRIMARY KEY,
            kind VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            tags_json VARCHAR NOT NULL,
            source_uri VARCHAR NOT NULL,
            checksum VARCHAR NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE edges (
            source_id VARCHAR NOT NULL,
            target_id VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            weight FLOAT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE chunks (
            id VARCHAR PRIMARY KEY,
            file_id VARCHAR NOT NULL,
            heading VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            embedding FLOAT[3] NOT NULL,
            checksum VARCHAR NOT NULL
        )
        """
    )

    con.executemany(
        """
        INSERT INTO nodes
        (id, kind, title, content, tags_json, source_uri, checksum)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                node["id"],
                node["kind"],
                node["title"],
                node["content"],
                node["tags_json"],
                node["source_uri"],
                node["checksum"],
            )
            for node in records.nodes
        ],
    )
    con.executemany(
        "INSERT INTO edges (source_id, target_id, kind, weight) VALUES (?, ?, ?, ?)",
        [
            (edge["source_id"], edge["target_id"], edge["kind"], edge["weight"])
            for edge in records.edges
        ],
    )
    con.executemany(
        """
        INSERT INTO chunks
        (id, file_id, heading, content, embedding, checksum)
        VALUES (?, ?, ?, ?, array_value(?, ?, ?)::FLOAT[3], ?)
        """,
        [
            (
                chunk["id"],
                chunk["file_id"],
                chunk["heading"],
                chunk["content"],
                chunk["embedding"][0],
                chunk["embedding"][1],
                chunk["embedding"][2],
                chunk["checksum"],
            )
            for chunk in records.chunks
        ],
    )


def timed_check(name: str, fn: Callable[[], tuple[bool, str, list[Any]]]) -> ProbeCheck:
    start = time.perf_counter()
    try:
        passed, detail, rows = fn()
        status = "pass" if passed else "fail"
    except Exception as exc:
        status = "blocked"
        detail = str(exc)
        rows = []
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return ProbeCheck(name=name, status=status, elapsed_ms=elapsed_ms, detail=detail, rows=rows)


def check_recursive_neighborhood(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(duckdb_probe_queries()["recursive_neighborhood"]).fetchall()
    found = any(row[0] == "mem_prov_transcript_1" for row in rows)
    return found, "Derived memory can traverse to hidden provenance via edges", rows


def check_link_neighborhood(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(duckdb_probe_queries()["link_neighborhood"]).fetchall()
    found = any(row[0] == "vault_chunk_jcode_plan_1" and row[3] == "Mentions" for row in rows)
    return found, "Vault chunk can be retrieved as note evidence near derived memory", rows


def check_fts(con: Any) -> tuple[bool, str, list[Any]]:
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    con.execute("PRAGMA create_fts_index('chunks', 'id', 'content', overwrite = 1)")
    rows = con.execute(duckdb_probe_queries()["fts_search"]).fetchall()
    found = any(row[0] == "mem_derived_broker_tests" for row in rows)
    return found, "DuckDB FTS can retrieve broker/Vault chunks with BM25", rows


def check_vector(con: Any) -> tuple[bool, str, list[Any]]:
    con.execute("INSTALL vss")
    con.execute("LOAD vss")
    con.execute("CREATE INDEX chunks_embedding_hnsw ON chunks USING HNSW (embedding)")
    rows = con.execute(duckdb_probe_queries()["vector_search"]).fetchall()
    found = bool(rows) and rows[0][0] == "vault_chunk_jcode_plan_1"
    return found, "DuckDB VSS can retrieve nearest chunks with fixed-size ARRAY embeddings", rows


def check_duckpgq(con: Any) -> tuple[bool, str, list[Any]]:
    con.execute("INSTALL duckpgq FROM community")
    con.execute("LOAD duckpgq")
    con.execute(
        """
        CREATE PROPERTY GRAPH memory_pg
        VERTEX TABLES (
            nodes
        )
        EDGE TABLES (
            edges
                SOURCE KEY (source_id) REFERENCES nodes (id)
                DESTINATION KEY (target_id) REFERENCES nodes (id)
        )
        """
    )
    rows = con.execute(duckdb_probe_queries()["duckpgq_property_graph"]).fetchall()
    found = any(row[0] == "mem_derived_broker_tests" and row[2] == "mem_prov_transcript_1" for row in rows)
    return found, "DuckPGQ property graph can represent jcode memory/Vault edges", rows


def check_parquet_export(con: Any) -> tuple[bool, str, list[Any]]:
    with tempfile.TemporaryDirectory(prefix="jcode-duckdb-proof-") as tmp:
        target = Path(tmp) / "chunks.parquet"
        con.execute(f"COPY chunks TO '{target.as_posix()}' (FORMAT parquet)")
        exists = target.exists() and target.stat().st_size > 0
        rows = [(target.name, target.stat().st_size if target.exists() else 0)]
    return exists, "DuckDB can export chunks to Parquet for data-science workflows", rows


def result_payload(
    duckdb_available: bool,
    checks: list[ProbeCheck],
    duckdb_version: str | None,
    elapsed_ms: int = 0,
) -> dict[str, Any]:
    check_json = [check.to_json() for check in checks]
    passed = [check.name for check in checks if check.status == "pass"]
    blocked = [check.name for check in checks if check.status == "blocked"]
    failed = [check.name for check in checks if check.status == "fail"]
    return {
        "duckdb_available": duckdb_available,
        "duckdb_version": duckdb_version,
        "elapsed_ms": elapsed_ms,
        "checks": check_json,
        "passed": passed,
        "blocked": blocked,
        "failed": failed,
        "operational_warnings": OPERATIONAL_WARNINGS,
        "recommendation": recommendation(check_json),
        "exit_ok": not failed and duckdb_available,
    }


def recommendation(checks: list[dict[str, Any]]) -> dict[str, str]:
    by_name = {check["name"]: check["status"] for check in checks}
    core_passed = by_name.get("recursive_neighborhood") == "pass" and by_name.get("link_neighborhood") == "pass"
    fts_passed = by_name.get("fts_search") == "pass"
    vector_passed = by_name.get("vector_search") == "pass"
    graph_extension_passed = by_name.get("duckpgq_property_graph") == "pass"

    if core_passed and fts_passed and vector_passed and graph_extension_passed:
        operational = "DuckDB remains a serious operational candidate if jcode owns a single-writer broker service."
    elif core_passed and (fts_passed or vector_passed):
        operational = "DuckDB is promising, but extension or graph-query gaps still need follow-up before it can displace SurrealDB."
    elif core_passed:
        operational = "DuckDB can model the relational graph core, but search/vector proof is incomplete."
    else:
        operational = "DuckDB has not yet proven the minimum broker graph traversal shape."

    return {
        "operational_role": operational,
        "analytics_role": "Keep DuckDB/Parquet as a first-class analytical mirror unless a later proof rejects it.",
        "baseline_role": "Keep SurrealDB as the validated operational baseline until DuckDB passes jcode-shaped write, restore, and sidecar/Vault proofs.",
    }


def should_exit_ok(payload: dict[str, Any], require_extensions: bool) -> bool:
    if not payload["duckdb_available"] or payload["failed"]:
        return False
    required = {"recursive_neighborhood", "link_neighborhood"}
    if require_extensions:
        required.update({"fts_search", "vector_search", "duckpgq_property_graph"})
    passed = set(payload["passed"])
    return required.issubset(passed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument("--require-duckdb", action="store_true", help="Fail if duckdb is unavailable")
    parser.add_argument(
        "--require-extensions",
        action="store_true",
        help="Fail if FTS, VSS, or DuckPGQ capability checks are blocked",
    )
    args = parser.parse_args(argv)

    payload = run_probe(
        require_duckdb=args.require_duckdb,
        require_extensions=args.require_extensions,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"duckdb_available: {payload['duckdb_available']}")
        print(f"duckdb_version: {payload['duckdb_version']}")
        for check in payload["checks"]:
            print(f"{check['name']}: {check['status']} ({check['elapsed_ms']}ms) - {check['detail']}")
        print(f"recommendation: {payload['recommendation']['operational_role']}")
    return 0 if payload.get("exit_ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
