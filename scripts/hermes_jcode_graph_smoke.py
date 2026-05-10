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
        session_id="hermes_jcode_graph_smoke",
        working_dir=args.working_dir,
        platform="smoke",
        agent_context="primary",
    )
    try:
        text = provider.prefetch(args.query, session_id="hermes_jcode_graph_smoke")
        tool_payload = provider.handle_tool_call(
            "jcode_broker_context",
            {"query": args.query, "limit": args.limit},
        )
    finally:
        provider.shutdown()

    tool = json.loads(tool_payload)
    items = tool.get("items") or []
    result = {
        "provider": provider.name,
        "prefetch_has_context": bool(text.strip()),
        "prefetch": text,
        "tool_event_type": tool.get("type"),
        "tool_item_kinds": [item.get("kind") for item in items if isinstance(item, dict)],
        "tool_item_count": len(items),
    }

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"provider: {result['provider']}")
        print(f"prefetch_has_context: {result['prefetch_has_context']}")
        print(f"tool_event_type: {result['tool_event_type']}")
        print(f"tool_item_kinds: {', '.join(result['tool_item_kinds'])}")
        if text:
            print()
            print(text)

    return 0 if result["prefetch_has_context"] and result["tool_item_count"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
