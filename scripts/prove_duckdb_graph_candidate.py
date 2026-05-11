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
import queue
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable


OPERATIONAL_WARNINGS = [
    "DuckDB has a single writer process constraint for native database writes; a broker service would need to own writes.",
    "DuckDB is optimized for analytical/bulk workloads, so frequent tiny agent transactions need explicit benchmarking.",
    "DuckDB FTS indexes do not refresh automatically after table changes; ingestion must rebuild or refresh indexes deterministically.",
    "DuckDB VSS persistent HNSW indexes are still guarded by experimental persistence settings; treat vector indexes as rebuildable.",
    "DuckPGQ is a community extension under active development; keep a recursive-CTE SQL graph fallback for graph traversal proofs.",
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
        "sql_property_graph_fallback": """
            WITH RECURSIVE paths(start_id, node_id, depth, path, edge_path) AS (
                SELECT
                    'mem_derived_broker_tests',
                    'mem_derived_broker_tests',
                    0,
                    'mem_derived_broker_tests',
                    ''
                UNION ALL
                SELECT
                    p.start_id,
                    e.target_id,
                    p.depth + 1,
                    p.path || '->' || e.target_id,
                    CASE
                        WHEN p.edge_path = '' THEN e.kind
                        ELSE p.edge_path || '->' || e.kind
                    END
                FROM paths p
                JOIN edges e ON e.source_id = p.node_id
                WHERE p.depth < 3
                  AND instr(p.path, e.target_id) = 0
            )
            SELECT p.start_id, p.node_id, p.depth, p.path, p.edge_path, n.kind
            FROM paths p
            JOIN nodes n ON n.id = p.node_id
            WHERE p.node_id = 'mem_prov_transcript_1'
            ORDER BY p.depth, p.path
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
        "vault_reconcile_search": """
            SELECT id, score
            FROM (
                SELECT
                    id,
                    fts_main_chunks.match_bm25(id, 'operational reconciliation') AS score
                FROM chunks
                WHERE deleted_at IS NULL
            ) sq
            WHERE score IS NOT NULL
            ORDER BY score DESC, id
            LIMIT 3
        """,
        "duckpgq_property_graph": """
            FROM GRAPH_TABLE (memory_pg
                MATCH (a:nodes)-[e:edges]->(b:nodes)
                COLUMNS (a.id AS source_id, e.kind AS kind, b.id AS target_id)
            )
            ORDER BY source_id, target_id
        """,
        "onager_pagerank": """
            WITH node_ids AS (
                SELECT id, row_number() OVER (ORDER BY id)::BIGINT AS node_id
                FROM nodes
            ),
            edge_ids AS (
                SELECT source.node_id AS src, target.node_id AS dst
                FROM edges
                JOIN node_ids source ON source.id = edges.source_id
                JOIN node_ids target ON target.id = edges.target_id
            )
            SELECT node_id, rank
            FROM onager_ctr_pagerank((SELECT src, dst FROM edge_ids))
            ORDER BY rank DESC, node_id
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
            timed_check("sql_property_graph_fallback", lambda: check_sql_graph_fallback(con)),
            timed_check("link_neighborhood", lambda: check_link_neighborhood(con)),
            timed_check("fts_search", lambda: check_fts(con)),
            timed_check("vector_search", lambda: check_vector(con)),
            timed_check(
                "single_writer_broker_service",
                lambda: check_single_writer_broker_service(duckdb),
            ),
            timed_check(
                "vault_update_delete_reconciliation",
                lambda: check_vault_update_delete_reconciliation(con),
            ),
            timed_check("backup_restore", lambda: check_backup_restore(duckdb)),
            timed_check("duckpgq_property_graph", lambda: check_duckpgq(con)),
            timed_check("onager_graph_analytics", lambda: check_onager(con)),
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
            checksum VARCHAR NOT NULL,
            deleted_at VARCHAR
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
            checksum VARCHAR NOT NULL,
            deleted_at VARCHAR
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


def check_sql_graph_fallback(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(duckdb_probe_queries()["sql_property_graph_fallback"]).fetchall()
    found = any(
        row[1] == "mem_prov_transcript_1" and "DerivedFrom" in str(row[4])
        for row in rows
    )
    return (
        found,
        "Pure SQL recursive CTE path query can replace DuckPGQ for core traversal",
        rows,
    )


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


_STOP_WRITER = object()


class SingleWriterDuckDbService:
    """Tiny proof service: one thread owns the DuckDB write connection."""

    def __init__(self, duckdb_module: Any, db_path: Path) -> None:
        self._duckdb = duckdb_module
        self._db_path = db_path
        self._queue: queue.Queue[Any] = queue.Queue()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="duckdb-proof-writer")
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("single-writer service did not start")

    def execute(self, sql: str, params: list[Any] | tuple[Any, ...] = ()) -> list[Any]:
        response: queue.Queue[Any] = queue.Queue(maxsize=1)
        self._queue.put((sql, params, response))
        ok, payload = response.get(timeout=10)
        if not ok:
            raise payload
        return payload

    def close(self) -> None:
        self._queue.put(_STOP_WRITER)
        self._thread.join(timeout=5)

    def _run(self) -> None:
        con = self._duckdb.connect(database=str(self._db_path))
        self._ready.set()
        try:
            while True:
                item = self._queue.get()
                if item is _STOP_WRITER:
                    break
                sql, params, response = item
                try:
                    cursor = con.execute(sql, params)
                    rows = cursor.fetchall() if cursor.description else []
                    response.put((True, rows))
                except Exception as exc:  # pragma: no cover - failure path is reported in check.
                    response.put((False, exc))
        finally:
            con.close()


def check_single_writer_broker_service(duckdb_module: Any) -> tuple[bool, str, list[Any]]:
    with tempfile.TemporaryDirectory(prefix="jcode-duckdb-writer-") as tmp:
        service = SingleWriterDuckDbService(duckdb_module, Path(tmp) / "broker.duckdb")
        try:
            service.execute(
                """
                CREATE TABLE broker_writes (
                    id VARCHAR PRIMARY KEY,
                    client_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    content VARCHAR NOT NULL
                )
                """
            )
            errors: list[BaseException] = []

            def client(client_id: int) -> None:
                try:
                    for sequence in range(8):
                        service.execute(
                            """
                            INSERT INTO broker_writes
                            (id, client_id, sequence, content)
                            VALUES (?, ?, ?, ?)
                            """,
                            [
                                f"client-{client_id}-{sequence}",
                                client_id,
                                sequence,
                                f"broker write {client_id}/{sequence}",
                            ],
                        )
                except BaseException as exc:  # pragma: no cover - failure path is asserted below.
                    errors.append(exc)

            threads = [threading.Thread(target=client, args=(idx,)) for idx in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            if errors:
                raise errors[0]

            rows = service.execute(
                """
                SELECT count(*) AS total, count(DISTINCT client_id) AS clients, max(sequence) AS max_sequence
                FROM broker_writes
                """
            )
        finally:
            service.close()

    found = bool(rows) and rows[0] == (32, 4, 7)
    return (
        found,
        "Single-writer broker service can serialize concurrent client writes into DuckDB",
        rows,
    )


def check_vault_update_delete_reconciliation(con: Any) -> tuple[bool, str, list[Any]]:
    updated_content = (
        "Phase 5 proves DuckDB operational reconciliation for updated Vault chunks."
    )
    deleted_at = "2026-05-11T00:00:00Z"
    con.execute(
        """
        UPDATE nodes
        SET content = ?, checksum = ?
        WHERE id = 'vault_chunk_jcode_plan_1'
        """,
        [updated_content, "sha256:chunk-2"],
    )
    con.execute(
        """
        UPDATE chunks
        SET content = ?, checksum = ?
        WHERE id = 'vault_chunk_jcode_plan_1'
        """,
        [updated_content, "sha256:chunk-2"],
    )
    con.execute(
        """
        UPDATE nodes
        SET deleted_at = ?
        WHERE id = 'vault_task_sidecar_live_proof'
        """,
        [deleted_at],
    )
    con.execute(
        """
        UPDATE chunks
        SET deleted_at = ?
        WHERE id = 'vault_task_sidecar_live_proof'
        """,
        [deleted_at],
    )
    con.execute(
        """
        DELETE FROM edges
        WHERE source_id = 'vault_task_sidecar_live_proof'
           OR target_id = 'vault_task_sidecar_live_proof'
        """
    )
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    con.execute("PRAGMA create_fts_index('chunks', 'id', 'content', overwrite = 1)")
    rows = con.execute(duckdb_probe_queries()["vault_reconcile_search"]).fetchall()
    tombstones = con.execute(
        """
        SELECT id, deleted_at
        FROM nodes
        WHERE deleted_at IS NOT NULL
        ORDER BY id
        """
    ).fetchall()
    active_edges = con.execute(
        """
        SELECT count(*)
        FROM edges
        WHERE source_id = 'vault_task_sidecar_live_proof'
           OR target_id = 'vault_task_sidecar_live_proof'
        """
    ).fetchone()[0]

    found_update = any(row[0] == "vault_chunk_jcode_plan_1" for row in rows)
    found_delete = ("vault_task_sidecar_live_proof", deleted_at) in tombstones
    found_edges_removed = active_edges == 0
    return (
        found_update and found_delete and found_edges_removed,
        "Vault chunk update refreshes FTS and deleted records are tombstoned/unlinked",
        [*rows, *tombstones, ("active_deleted_edges", active_edges)],
    )


def check_backup_restore(duckdb_module: Any) -> tuple[bool, str, list[Any]]:
    with tempfile.TemporaryDirectory(prefix="jcode-duckdb-backup-") as tmp:
        db_path = Path(tmp) / "broker.duckdb"
        backup_path = Path(tmp) / "broker.backup.duckdb"
        con = duckdb_module.connect(database=str(db_path))
        try:
            load_records(con, sample_records())
        finally:
            con.close()
        shutil.copy2(db_path, backup_path)
        restored = duckdb_module.connect(database=str(backup_path), read_only=True)
        try:
            rows = restored.execute(
                """
                SELECT
                    (SELECT count(*) FROM nodes) AS node_count,
                    (SELECT count(*) FROM edges) AS edge_count,
                    (SELECT count(*) FROM chunks) AS chunk_count
                """
            ).fetchall()
        finally:
            restored.close()

    found = bool(rows) and rows[0] == (5, 5, 3)
    return found, "DuckDB file backup can be restored and queried read-only", rows


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


def check_onager(con: Any) -> tuple[bool, str, list[Any]]:
    con.execute("INSTALL onager FROM community")
    con.execute("LOAD onager")
    rows = con.execute(duckdb_probe_queries()["onager_pagerank"]).fetchall()
    found = bool(rows) and all(len(row) == 2 for row in rows)
    return (
        found,
        "Onager can run DuckDB-native graph analytics over broker edge tables",
        rows,
    )


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
    graph_fallback_passed = by_name.get("sql_property_graph_fallback") == "pass"
    fts_passed = by_name.get("fts_search") == "pass"
    vector_passed = by_name.get("vector_search") == "pass"
    writer_passed = by_name.get("single_writer_broker_service") == "pass"
    reconcile_passed = by_name.get("vault_update_delete_reconciliation") == "pass"
    backup_passed = by_name.get("backup_restore") == "pass"
    graph_extension_passed = by_name.get("duckpgq_property_graph") == "pass"

    if core_passed and graph_fallback_passed and fts_passed and vector_passed and writer_passed and reconcile_passed and backup_passed:
        operational = "DuckDB's no-DuckPGQ path is viable enough for a whole-Vault operational proof behind a single-writer broker service."
    elif core_passed and fts_passed and vector_passed and graph_extension_passed:
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
    required = {"recursive_neighborhood", "link_neighborhood", "sql_property_graph_fallback"}
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
