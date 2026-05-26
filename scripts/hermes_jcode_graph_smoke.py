#!/usr/bin/env python3
"""Smoke-test Hermes loading the jcode_graph memory provider.

Run this with Hermes' Python environment and a test HERMES_HOME that has
plugins/jcode_graph installed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _add_default_hermes_repo_to_path() -> None:
    configured = os.environ.get("HERMES_AGENT_REPO")
    candidates = (
        [Path(configured)]
        if configured
        else [
            Path.home() / ".hermes" / "hermes-agent-v0.13.0",
            Path.home() / ".hermes" / "hermes-agent",
        ]
    )
    repo = next((candidate for candidate in candidates if candidate.exists()), candidates[-1])
    if repo.exists():
        sys.path.insert(0, str(repo))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", default="broker memory")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--socket", help="jcode broker socket path")
    parser.add_argument("--working-dir", default=os.getcwd())
    parser.add_argument("--session-id", default="hermes_jcode_graph_smoke")
    parser.add_argument("--sync-user", help="Optional user turn to sync before prefetch")
    parser.add_argument("--sync-assistant", help="Optional assistant turn to sync before prefetch")
    parser.add_argument("--transcript-user", help="Optional user message for transcript hooks")
    parser.add_argument("--transcript-assistant", help="Optional assistant message for transcript hooks")
    parser.add_argument(
        "--require-derived-store-proof",
        action="store_true",
        help=(
            "Require durable JCODE_HOME memory-store evidence that live sidecar "
            "extraction created broker-derived memories linked to hidden provenance."
        ),
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.socket:
        os.environ["JCODE_BROKER_SOCKET"] = args.socket

    _add_default_hermes_repo_to_path()

    from plugins.memory import load_memory_provider

    provider = load_memory_provider("jcode_graph")
    if provider is None:
        print(
            "jcode_graph provider not found. Install adapters/hermes/jcode_graph "
            "to $HERMES_HOME/plugins/jcode_graph first.",
            file=sys.stderr,
        )
        return 2

    provider.initialize(
        session_id=args.session_id,
        working_dir=args.working_dir,
        platform="smoke",
        agent_context="primary",
    )
    try:
        if args.sync_user or args.sync_assistant:
            provider.sync_turn(
                args.sync_user or "",
                args.sync_assistant or "",
                session_id=args.session_id,
            )
        transcript_messages = _messages(args.transcript_user, args.transcript_assistant)
        if transcript_messages:
            provider.on_pre_compress(transcript_messages)
            provider.on_session_end(transcript_messages)

        text = provider.prefetch(args.query, session_id=args.session_id)
        tool_payload = provider.handle_tool_call(
            "jcode_broker_context",
            {"query": args.query, "limit": args.limit},
        )
        raw_terms = [
            term
            for term in (
                args.sync_user,
                args.sync_assistant,
                args.transcript_user,
                args.transcript_assistant,
            )
            if term
        ]
        provenance_query = _provenance_query(args.query, raw_terms=raw_terms)
        provenance_payload = provider.handle_tool_call(
            "jcode_broker_context",
            {"query": provenance_query, "limit": args.limit, "include_provenance": True},
        )
        diagnostics = _diagnostics(provider)
    finally:
        provider.shutdown()

    tool = json.loads(tool_payload)
    items = tool.get("items") or []
    provenance_tool = json.loads(provenance_payload)
    provenance_items = [
        item
        for item in (provenance_tool.get("items") or [])
        if isinstance(item, dict) and _is_provenance_item(item)
    ]
    provenance_blob = json.dumps(provenance_items, sort_keys=True)
    result = {
        "provider": provider.name,
        "prefetch_has_context": bool(text.strip()),
        "prefetch_chars": len(text),
        "prefetch": text,
        "default_prefetch_has_raw_provenance": any(term in text for term in raw_terms),
        "tool_event_type": tool.get("type"),
        "tool_item_kinds": [item.get("kind") for item in items if isinstance(item, dict)],
        "tool_memory_contents": [
            item.get("content")
            for item in items
            if isinstance(item, dict) and item.get("kind") == "memory"
        ],
        "tool_item_count": len(items),
        "provenance_tool_event_type": provenance_tool.get("type"),
        "provenance_tool_query": provenance_query,
        "provenance_tool_item_count": len(provenance_tool.get("items") or []),
        "provenance_tool_provenance_count": len(provenance_items),
        "provenance_tool_memory_contents": [
            item.get("content") or item.get("summary")
            for item in provenance_items
            if isinstance(item, dict) and item.get("kind") == "memory"
        ],
        "provenance_tool_contains_synced_text": any(term in provenance_blob for term in raw_terms),
        "diagnostics": diagnostics,
        "extraction_status": diagnostics.get("last_extraction_status"),
    }
    if raw_terms:
        result["raw_terms_checked"] = raw_terms
    if args.require_derived_store_proof:
        result["derived_store_proof"] = _derived_store_proof(
            args.session_id,
            raw_terms=raw_terms,
        )

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"provider: {result['provider']}")
        print(f"prefetch_has_context: {result['prefetch_has_context']}")
        print(f"prefetch_chars: {result['prefetch_chars']}")
        print(
            "default_prefetch_has_raw_provenance: "
            f"{result['default_prefetch_has_raw_provenance']}"
        )
        print(f"tool_event_type: {result['tool_event_type']}")
        print(f"tool_item_kinds: {', '.join(result['tool_item_kinds'])}")
        print(f"provenance_tool_event_type: {result['provenance_tool_event_type']}")
        print(f"provenance_tool_item_count: {result['provenance_tool_item_count']}")
        print(
            "provenance_tool_contains_synced_text: "
            f"{result['provenance_tool_contains_synced_text']}"
        )
        print(f"extraction_status: {result['extraction_status']}")
        print(f"diagnostics: {json.dumps(result['diagnostics'], sort_keys=True)}")
        if result["tool_memory_contents"]:
            print(f"tool_memory_contents: {result['tool_memory_contents']}")
        if result["provenance_tool_memory_contents"]:
            print(
                "provenance_tool_memory_contents: "
                f"{result['provenance_tool_memory_contents']}"
            )
        if text:
            print()
            print(text)

    success = bool(result["prefetch_has_context"] and result["tool_item_count"])
    if args.sync_user or args.sync_assistant:
        success = success and diagnostics.get("turn_sync_count", 0) >= 1
    if args.transcript_user or args.transcript_assistant:
        success = success and diagnostics.get("transcript_sync_count", 0) >= 2
    if args.require_derived_store_proof:
        proof = result["derived_store_proof"]
        success = (
            success
            and proof["hidden_provenance_count"] > 0
            and proof["derived_memory_count"] > 0
            and proof["derived_from_edge_count"] > 0
            and proof["hidden_provenance_contains_synced_text"]
        )
    if raw_terms:
        provenance_proved = result["provenance_tool_contains_synced_text"]
        if args.require_derived_store_proof:
            provenance_proved = (
                provenance_proved
                or result["derived_store_proof"]["hidden_provenance_contains_synced_text"]
            )
        success = (
            success
            and not result["default_prefetch_has_raw_provenance"]
            and provenance_proved
        )
    return 0 if success else 1


def _messages(user: str | None, assistant: str | None) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if user:
        messages.append({"role": "user", "content": user})
    if assistant:
        messages.append({"role": "assistant", "content": assistant})
    return messages


def _provenance_query(default_query: str, *, raw_terms: list[str]) -> str:
    """Use the synced marker itself when proving hidden provenance retrieval."""
    for term in raw_terms:
        if term.strip():
            return term.strip()
    return default_query


def _diagnostics(provider) -> dict:
    diagnostics = getattr(provider, "diagnostics", None)
    if callable(diagnostics):
        return diagnostics()
    return {}


def _is_provenance_item(item: dict) -> bool:
    tags = item.get("tags") or []
    metadata = item.get("metadata") or {}
    return (
        (isinstance(tags, list) and "broker-provenance" in tags)
        or (isinstance(metadata, dict) and metadata.get("provenance") is True)
        or item.get("category") == "provenance"
        or item.get("title") == "provenance"
    )


def _derived_store_proof(session_id: str, *, raw_terms: list[str]) -> dict[str, Any]:
    """Inspect isolated JCODE_HOME memory JSON for live sidecar extraction evidence."""
    jcode_home = Path(os.environ.get("JCODE_HOME", Path.home() / ".jcode"))
    graph_paths = sorted((jcode_home / "memory" / "projects").glob("*.json"))
    provenance_ids: set[str] = set()
    derived_ids: set[str] = set()
    derived_from_edges: set[tuple[str, str]] = set()
    hidden_provenance_contains_synced_text = False
    derived_memory_contents: list[str] = []

    for graph_path in graph_paths:
        try:
            graph = json.loads(graph_path.read_text())
        except Exception:
            continue
        memories = graph.get("memories") or {}
        edges = graph.get("edges") or {}
        for memory_id, memory in memories.items():
            if not isinstance(memory, dict):
                continue
            tags = memory.get("tags") or []
            content = str(memory.get("content") or "")
            source = str(memory.get("source") or "")
            content_matches_raw_terms = any(term in content for term in raw_terms)
            if "broker-provenance" in tags and (
                session_id in source or session_id in content or content_matches_raw_terms
            ):
                provenance_ids.add(memory_id)
                if content_matches_raw_terms:
                    hidden_provenance_contains_synced_text = True

        for source_id, source_edges in edges.items():
            source_memory = memories.get(source_id) or {}
            if not isinstance(source_memory, dict):
                continue
            source_tags = source_memory.get("tags") or []
            if "broker-derived" not in source_tags or not isinstance(source_edges, list):
                continue
            for edge in source_edges:
                if not isinstance(edge, dict):
                    continue
                target_id = edge.get("target")
                if edge.get("kind") == "derived_from" and target_id in provenance_ids:
                    derived_ids.add(source_id)
                    derived_memory_contents.append(str(source_memory.get("content") or ""))
                    derived_from_edges.add((source_id, target_id))

    return {
        "graph_file_count": len(graph_paths),
        "hidden_provenance_count": len(provenance_ids),
        "derived_memory_count": len(derived_ids),
        "derived_from_edge_count": len(derived_from_edges),
        "hidden_provenance_contains_synced_text": hidden_provenance_contains_synced_text,
        "derived_memory_contents": derived_memory_contents,
    }


if __name__ == "__main__":
    raise SystemExit(main())
