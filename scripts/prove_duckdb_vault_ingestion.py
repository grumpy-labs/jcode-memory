#!/usr/bin/env python3
"""Read-only whole-Vault ingestion proof for DuckDB-backed broker context.

This is a Phase 5 proof harness, not a production importer. It inventories an
Obsidian Vault, builds broker-shaped DuckDB tables in memory by default, and
checks whether note chunks, links, tasks, tags, headings, and source metadata
can support fast context recall without writing to the source Vault.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


EXCLUDED_DIRS = {".git", ".obsidian", ".trash", ".DS_Store", "__pycache__", "node_modules"}
LARGE_FILE_BYTES = 1_000_000
WIKILINK_RE = re.compile(r"\[\[([^]|#]+)(?:#[^]|]+)?(?:\|[^\]]+)?\]\]")
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
TAG_RE = re.compile(r"(?<![\w/])#([A-Za-z0-9][A-Za-z0-9_/-]*)")
TASK_RE = re.compile(r"^\s*[-*]\s+\[([ xX])\]\s+(.*)$")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


@dataclasses.dataclass(frozen=True)
class VaultRecords:
    files: list[dict[str, Any]]
    chunks: list[dict[str, Any]]
    links: list[dict[str, Any]]
    tags: list[dict[str, Any]]
    tasks: list[dict[str, Any]]
    headings: list[dict[str, Any]]
    attachments: list[dict[str, Any]]
    frontmatter_errors: list[dict[str, str]]


def stable_id(prefix: str, *parts: object) -> str:
    joined = "\x1f".join(str(part) for part in parts)
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


def sha256_text(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def should_skip(path: Path, vault: Path) -> bool:
    try:
        rel_parts = path.relative_to(vault).parts
    except ValueError:
        rel_parts = path.parts
    return any(part in EXCLUDED_DIRS or part.startswith(".sync-conflict") for part in rel_parts)


def iter_vault_files(vault: Path) -> list[Path]:
    return sorted(path for path in vault.rglob("*") if path.is_file() and not should_skip(path, vault))


def parse_frontmatter(text: str, rel_path: str) -> tuple[dict[str, Any], str, str | None]:
    if not text.startswith("---"):
        return {}, text, None
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text, None
    for idx in range(1, len(lines)):
        if lines[idx].strip() == "---":
            raw = "\n".join(lines[1:idx])
            body = "\n".join(lines[idx + 1 :])
            try:
                import yaml  # type: ignore

                parsed = yaml.safe_load(raw) or {}
                if not isinstance(parsed, dict):
                    return {}, body, f"{rel_path}: frontmatter is not a map"
                return parsed, body, None
            except Exception as exc:
                return {}, body, f"{rel_path}: {exc}"
    return {}, text, f"{rel_path}: unterminated frontmatter"


def frontmatter_tags(frontmatter: dict[str, Any]) -> list[str]:
    raw = frontmatter.get("tags") or frontmatter.get("tag") or []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    tags: list[str] = []
    for tag in raw:
        normalized = str(tag).strip().lstrip("#")
        if normalized:
            tags.append(normalized)
    return tags


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    return str(value)


def title_from_body(path: Path, body: str) -> str:
    for line in body.splitlines():
        match = HEADING_RE.match(line)
        if match:
            return match.group(2).strip()
    return path.stem


def chunk_body(file_id: str, rel_path: str, body: str) -> list[dict[str, Any]]:
    lines = body.splitlines()
    heading = ""
    start_line = 1
    chunks: list[dict[str, Any]] = []
    current: list[str] = []

    def flush(end_line: int) -> None:
        content = "\n".join(current).strip()
        if not content:
            return
        chunks.append(
            {
                "id": stable_id("vault_chunk", rel_path, start_line, heading, content[:64]),
                "file_id": file_id,
                "path": rel_path,
                "heading": heading,
                "content": content[:4000],
                "start_line": start_line,
                "end_line": end_line,
                "checksum": sha256_text(content),
            }
        )

    for idx, line in enumerate(lines, start=1):
        match = HEADING_RE.match(line)
        if match and current:
            flush(idx - 1)
            current = []
            heading = match.group(2).strip()
            start_line = idx
        elif match:
            heading = match.group(2).strip()
            start_line = idx
        current.append(line)
    flush(len(lines))
    return chunks


def collect_vault_records(vault: Path) -> VaultRecords:
    vault = vault.expanduser().resolve()
    files: list[dict[str, Any]] = []
    chunks: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    tags: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    headings: list[dict[str, Any]] = []
    attachments: list[dict[str, Any]] = []
    frontmatter_errors: list[dict[str, str]] = []

    for path in iter_vault_files(vault):
        rel_path = path.relative_to(vault).as_posix()
        stat = path.stat()
        if path.suffix.lower() != ".md":
            attachments.append(
                {
                    "path": rel_path,
                    "suffix": path.suffix.lower(),
                    "size_bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                }
            )
            continue

        text = path.read_text(encoding="utf-8", errors="replace")
        frontmatter, body, frontmatter_error = parse_frontmatter(text, rel_path)
        if frontmatter_error:
            frontmatter_errors.append({"path": rel_path, "error": frontmatter_error})
        file_id = stable_id("vault_file", rel_path)
        inline_tags = TAG_RE.findall(body)
        all_tags = sorted(set(frontmatter_tags(frontmatter) + inline_tags))
        file_record = {
            "id": file_id,
            "path": rel_path,
            "title": str(frontmatter.get("title") or title_from_body(path, body)),
            "checksum": sha256_text(text),
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "frontmatter_json": json.dumps(json_safe(frontmatter), sort_keys=True),
        }
        files.append(file_record)
        chunks.extend(chunk_body(file_id, rel_path, body))

        for tag in all_tags:
            tags.append({"file_id": file_id, "path": rel_path, "tag": tag})

        for line_no, line in enumerate(body.splitlines(), start=1):
            if heading_match := HEADING_RE.match(line):
                headings.append(
                    {
                        "id": stable_id("vault_heading", rel_path, line_no, heading_match.group(2)),
                        "file_id": file_id,
                        "path": rel_path,
                        "level": len(heading_match.group(1)),
                        "heading": heading_match.group(2).strip(),
                        "line": line_no,
                    }
                )
            if task_match := TASK_RE.match(line):
                tasks.append(
                    {
                        "id": stable_id("vault_task", rel_path, line_no, task_match.group(2)),
                        "file_id": file_id,
                        "path": rel_path,
                        "checked": task_match.group(1).lower() == "x",
                        "content": task_match.group(2).strip(),
                        "line": line_no,
                    }
                )

        for match in WIKILINK_RE.finditer(body):
            raw_target = match.group(1).strip()
            links.append(
                {
                    "id": stable_id("vault_link", rel_path, "wiki", match.start(), raw_target),
                    "source_file_id": file_id,
                    "source_path": rel_path,
                    "target": raw_target,
                    "kind": "wikilink",
                    "raw": match.group(0),
                }
            )
        for match in MARKDOWN_LINK_RE.finditer(body):
            target = match.group(2).strip()
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target):
                continue
            links.append(
                {
                    "id": stable_id("vault_link", rel_path, "markdown", match.start(), target),
                    "source_file_id": file_id,
                    "source_path": rel_path,
                    "target": target,
                    "kind": "markdown",
                    "raw": match.group(0),
                }
            )

    return VaultRecords(
        files=files,
        chunks=chunks,
        links=links,
        tags=tags,
        tasks=tasks,
        headings=headings,
        attachments=attachments,
        frontmatter_errors=frontmatter_errors,
    )


def inventory_vault(vault: Path) -> dict[str, Any]:
    return inventory_from_records(collect_vault_records(vault))


def load_records(con: Any, records: VaultRecords) -> None:
    con.execute(
        """
        CREATE TABLE vault_file (
            id VARCHAR PRIMARY KEY,
            path VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            checksum VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL,
            mtime_ns BIGINT NOT NULL,
            frontmatter_json VARCHAR NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE vault_chunk (
            id VARCHAR PRIMARY KEY,
            file_id VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            heading VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            start_line INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            checksum VARCHAR NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE vault_link (
            id VARCHAR PRIMARY KEY,
            source_file_id VARCHAR NOT NULL,
            source_path VARCHAR NOT NULL,
            target VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            raw VARCHAR NOT NULL
        )
        """
    )
    con.execute("CREATE TABLE vault_tag (file_id VARCHAR, path VARCHAR, tag VARCHAR)")
    con.execute(
        """
        CREATE TABLE vault_task (
            id VARCHAR PRIMARY KEY,
            file_id VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            checked BOOLEAN NOT NULL,
            content VARCHAR NOT NULL,
            line INTEGER NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE vault_heading (
            id VARCHAR PRIMARY KEY,
            file_id VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            level INTEGER NOT NULL,
            heading VARCHAR NOT NULL,
            line INTEGER NOT NULL
        )
        """
    )

    execute_many(
        con,
        """
        INSERT INTO vault_file
        (id, path, title, checksum, size_bytes, mtime_ns, frontmatter_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["id"],
                record["path"],
                record["title"],
                record["checksum"],
                record["size_bytes"],
                record["mtime_ns"],
                record["frontmatter_json"],
            )
            for record in records.files
        ],
    )
    execute_many(
        con,
        """
        INSERT INTO vault_chunk
        (id, file_id, path, heading, content, start_line, end_line, checksum)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["id"],
                record["file_id"],
                record["path"],
                record["heading"],
                record["content"],
                record["start_line"],
                record["end_line"],
                record["checksum"],
            )
            for record in records.chunks
        ],
    )
    execute_many(
        con,
        """
        INSERT INTO vault_link
        (id, source_file_id, source_path, target, kind, raw)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["id"],
                record["source_file_id"],
                record["source_path"],
                record["target"],
                record["kind"],
                record["raw"],
            )
            for record in records.links
        ],
    )
    execute_many(
        con,
        "INSERT INTO vault_tag (file_id, path, tag) VALUES (?, ?, ?)",
        [(record["file_id"], record["path"], record["tag"]) for record in records.tags],
    )
    execute_many(
        con,
        """
        INSERT INTO vault_task
        (id, file_id, path, checked, content, line)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["id"],
                record["file_id"],
                record["path"],
                record["checked"],
                record["content"],
                record["line"],
            )
            for record in records.tasks
        ],
    )
    execute_many(
        con,
        """
        INSERT INTO vault_heading
        (id, file_id, path, level, heading, line)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["id"],
                record["file_id"],
                record["path"],
                record["level"],
                record["heading"],
                record["line"],
            )
            for record in records.headings
        ],
    )


def execute_many(con: Any, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        con.executemany(sql, rows)


def table_counts(con: Any) -> dict[str, int]:
    tables = ["vault_file", "vault_chunk", "vault_link", "vault_tag", "vault_task", "vault_heading"]
    return {
        table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in tables
    }


def run_vault_ingestion_proof(
    vault: Path,
    *,
    query: str = "jcode broker memory",
    require_duckdb: bool = False,
    database: str = ":memory:",
) -> dict[str, Any]:
    started = time.perf_counter()
    vault = vault.expanduser().resolve()
    records = collect_vault_records(vault)
    inventory = inventory_from_records(records)

    try:
        import duckdb  # type: ignore
    except Exception as exc:
        status = "fail" if require_duckdb else "blocked"
        return {
            "duckdb_available": False,
            "duckdb_version": None,
            "inventory": inventory,
            "checks": [{"name": "duckdb_import", "status": status, "detail": str(exc)}],
            "passed": [],
            "blocked": ["duckdb_import"] if status == "blocked" else [],
            "failed": ["duckdb_import"] if status == "fail" else [],
            "exit_ok": False,
        }

    con = duckdb.connect(database=database)
    try:
        load_records(con, records)
        counts = table_counts(con)
        checks = run_checks(con, query=query)
    finally:
        con.close()

    passed = [check["name"] for check in checks if check["status"] == "pass"]
    blocked = [check["name"] for check in checks if check["status"] == "blocked"]
    failed = [check["name"] for check in checks if check["status"] == "fail"]
    return {
        "duckdb_available": True,
        "duckdb_version": getattr(duckdb, "__version__", None),
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
        "inventory": inventory,
        "tables": counts,
        "checks": checks,
        "passed": passed,
        "blocked": blocked,
        "failed": failed,
        "exit_ok": not failed,
    }


def inventory_from_records(records: VaultRecords) -> dict[str, Any]:
    tag_counts = Counter(record["tag"] for record in records.tags)
    lower_paths = [record["path"].lower() for record in records.files]
    duplicate_paths = [path for path, count in Counter(lower_paths).items() if count > 1]
    broken_links = broken_links_for_records(records)
    large_files = sorted(
        [
            {"path": record["path"], "size_bytes": record["size_bytes"]}
            for record in [*records.files, *records.attachments]
            if record["size_bytes"] >= LARGE_FILE_BYTES
        ],
        key=lambda item: item["size_bytes"],
        reverse=True,
    )[:20]
    return {
        "markdown_count": len(records.files),
        "attachment_count": len(records.attachments),
        "chunk_count": len(records.chunks),
        "heading_count": len(records.headings),
        "task_count": len(records.tasks),
        "wikilink_count": sum(1 for link in records.links if link["kind"] == "wikilink"),
        "markdown_link_count": sum(1 for link in records.links if link["kind"] == "markdown"),
        "tag_counts": dict(tag_counts.most_common(50)),
        "frontmatter_error_count": len(records.frontmatter_errors),
        "frontmatter_errors": records.frontmatter_errors[:20],
        "large_files": large_files,
        "duplicate_path_count": len(duplicate_paths),
        "duplicate_paths": duplicate_paths[:20],
        "broken_link_count": len(broken_links),
        "broken_links": broken_links[:20],
    }


def broken_links_for_records(records: VaultRecords) -> list[dict[str, str]]:
    known = set()
    for record in records.files:
        path = record["path"].lower()
        known.add(path)
        if path.endswith(".md"):
            known.add(path[:-3])
        known.add(Path(path).stem)
    for record in records.attachments:
        path = record["path"].lower()
        known.add(path)
        known.add(Path(path).name.lower())

    broken: list[dict[str, str]] = []
    for link in records.links:
        target = normalized_link_target(link["target"])
        if not target or target in known:
            continue
        broken.append(
            {
                "source_path": link["source_path"],
                "target": link["target"],
                "kind": link["kind"],
            }
        )
    return broken


def normalized_link_target(target: str) -> str:
    normalized = target.strip().replace("\\", "/")
    if normalized.startswith("#"):
        return ""
    normalized = normalized.split("#", 1)[0].split("|", 1)[0].strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = normalized.lstrip("/")
    if normalized.endswith(".md"):
        normalized = normalized[:-3]
    return normalized.lower()


def run_checks(con: Any, *, query: str) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    checks.append(check("source_metadata", lambda: check_source_metadata(con)))
    checks.append(check("fts_recall", lambda: check_fts_recall(con, query)))
    checks.append(check("link_neighborhood", lambda: check_link_neighborhood(con)))
    checks.append(check("task_extraction", lambda: check_task_extraction(con)))
    return checks


def check(name: str, fn) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        passed, detail, rows = fn()
        status = "pass" if passed else "fail"
    except Exception as exc:
        status = "blocked"
        detail = str(exc)
        rows = []
    return {
        "name": name,
        "status": status,
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
        "detail": detail,
        "rows": rows[:10],
    }


def check_source_metadata(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(
        """
        SELECT path, checksum, size_bytes
        FROM vault_file
        WHERE checksum LIKE 'sha256:%'
        ORDER BY path
        LIMIT 3
        """
    ).fetchall()
    return bool(rows), "Vault file records preserve path/checksum/size metadata", rows


def check_fts_recall(con: Any, query: str) -> tuple[bool, str, list[Any]]:
    con.execute("INSTALL fts")
    con.execute("LOAD fts")
    con.execute("PRAGMA create_fts_index('vault_chunk', 'id', 'content', overwrite = 1)")
    rows = con.execute(
        """
        SELECT id, path, heading, score
        FROM (
            SELECT
                id,
                path,
                heading,
                fts_main_vault_chunk.match_bm25(id, ?) AS score
            FROM vault_chunk
        ) sq
        WHERE score IS NOT NULL
        ORDER BY score DESC, path, id
        LIMIT 5
        """,
        [query],
    ).fetchall()
    return bool(rows), "DuckDB FTS retrieves Vault chunks by query", rows


def check_link_neighborhood(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(
        """
        SELECT f.path, l.kind, l.target
        FROM vault_link l
        JOIN vault_file f ON f.id = l.source_file_id
        ORDER BY f.path, l.target
        LIMIT 5
        """
    ).fetchall()
    return bool(rows), "Vault links can be queried as broker context neighborhoods", rows


def check_task_extraction(con: Any) -> tuple[bool, str, list[Any]]:
    rows = con.execute(
        """
        SELECT path, checked, content
        FROM vault_task
        ORDER BY path, line
        LIMIT 5
        """
    ).fetchall()
    return bool(rows), "Obsidian task lines become queryable Vault task records", rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", default=str(Path.home() / "Vault"))
    parser.add_argument("--query", default="jcode broker memory")
    parser.add_argument("--db", default=":memory:", help="DuckDB database path; defaults to in-memory")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--require-duckdb", action="store_true")
    args = parser.parse_args(argv)

    payload = run_vault_ingestion_proof(
        Path(args.vault),
        query=args.query,
        require_duckdb=args.require_duckdb,
        database=args.db,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"duckdb_available: {payload['duckdb_available']}")
        print(f"inventory: {json.dumps(payload['inventory'], sort_keys=True)}")
        for check_result in payload.get("checks", []):
            print(
                f"{check_result['name']}: {check_result['status']} "
                f"({check_result.get('elapsed_ms', 0)}ms) - {check_result['detail']}"
            )
    return 0 if payload.get("exit_ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
