from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE_SCRIPT = REPO_ROOT / "scripts" / "hermes_jcode_graph_smoke.py"


def load_smoke_module():
    spec = importlib.util.spec_from_file_location("hermes_jcode_graph_smoke", SMOKE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load hermes_jcode_graph_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HermesJcodeGraphSmokeTests(unittest.TestCase):
    def test_success_accepts_no_prefetch_restraint_when_requested(self) -> None:
        smoke = load_smoke_module()
        args = SimpleNamespace(
            expect_no_prefetch=True,
            require_derived_store_proof=False,
            sync_user=None,
            sync_assistant=None,
            transcript_user=None,
            transcript_assistant=None,
        )
        result = {
            "prefetch_has_context": False,
            "prefetch_chars": 0,
            "tool_event_type": "broker_context",
            "tool_item_count": 2,
            "default_prefetch_has_raw_provenance": False,
            "provenance_tool_contains_synced_text": False,
            "diagnostics": {"last_prefetch_item_count": 0},
        }

        self.assertTrue(smoke._smoke_success(result, args))

    def test_success_rejects_no_prefetch_restraint_when_context_was_injected(self) -> None:
        smoke = load_smoke_module()
        args = SimpleNamespace(
            expect_no_prefetch=True,
            require_derived_store_proof=False,
            sync_user=None,
            sync_assistant=None,
            transcript_user=None,
            transcript_assistant=None,
        )
        result = {
            "prefetch_has_context": True,
            "prefetch_chars": 42,
            "tool_event_type": "broker_context",
            "tool_item_count": 2,
            "default_prefetch_has_raw_provenance": False,
            "provenance_tool_contains_synced_text": False,
            "diagnostics": {"last_prefetch_item_count": 1},
        }

        self.assertFalse(smoke._smoke_success(result, args))

    def test_success_uses_prefetch_diagnostics_before_explicit_tool_calls(self) -> None:
        smoke = load_smoke_module()
        args = SimpleNamespace(
            expect_no_prefetch=True,
            require_derived_store_proof=False,
            sync_user=None,
            sync_assistant=None,
            transcript_user=None,
            transcript_assistant=None,
        )
        result = {
            "prefetch_has_context": False,
            "prefetch_chars": 0,
            "tool_event_type": "broker_context",
            "tool_item_count": 2,
            "default_prefetch_has_raw_provenance": False,
            "provenance_tool_contains_synced_text": False,
            "prefetch_diagnostics": {"last_prefetch_item_count": 0},
            "diagnostics": {"last_prefetch_item_count": 2},
        }

        self.assertTrue(smoke._smoke_success(result, args))

    def test_provenance_query_prefers_synced_raw_term(self) -> None:
        smoke = load_smoke_module()

        query = smoke._provenance_query(
            "Clio current context packet provider gate",
            raw_terms=["unique synced smoke marker"],
        )

        self.assertEqual(query, "unique synced smoke marker")

    def test_compact_tool_items_keep_source_metadata_without_full_content(self) -> None:
        smoke = load_smoke_module()

        compact = smoke._compact_tool_items(
            [
                {
                    "kind": "vault_chunk",
                    "title": "Ghostty setup",
                    "summary": "Advanced Tips: Make Ghostty Even Better",
                    "content": "long body should stay out of eval JSON",
                    "metadata": {
                        "target_path": "Projects/Hermes-Honcho-LangGraph-Second-Brain/CURRENT.md",
                        "uri": "vault://TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md#Advanced Tips",
                        "start_line": 199,
                        "end_line": 200,
                    },
                    "relevance": {"rank": 1, "retrieval_mode": "duckdb_broker_store"},
                }
            ]
        )

        self.assertEqual(compact[0]["kind"], "vault_chunk")
        self.assertIn("Ghostty Terminal", compact[0]["source_path"])
        self.assertEqual(compact[0]["line_start"], 199)
        self.assertEqual(compact[0]["metadata"]["target_path"], "Projects/Hermes-Honcho-LangGraph-Second-Brain/CURRENT.md")
        self.assertEqual(compact[0]["relevance"]["retrieval_mode"], "duckdb_broker_store")
        self.assertNotIn("content", compact[0])

    def test_derived_store_proof_finds_live_sidecar_memories(self) -> None:
        smoke = load_smoke_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            jcode_home = Path(temp_dir) / "jcode-home"
            project_dir = jcode_home / "memory" / "projects"
            project_dir.mkdir(parents=True)
            session_id = "session_sidecar_live"
            provenance_id = "mem_provenance"
            graph = {
                "memories": {
                    provenance_id: {
                        "id": provenance_id,
                        "category": {"custom": "provenance"},
                        "content": (
                            "External transcript synced from hermes:pre_compress.\n\n"
                            "user: remember isolated sidecar auth path"
                        ),
                        "tags": ["broker-provenance", "broker-transcript-sync"],
                        "source": f"hermes:pre_compress:{session_id}",
                    },
                    "mem_derived": {
                        "id": "mem_derived",
                        "category": "fact",
                        "content": "isolated sidecar auth path lives under JCODE_HOME",
                        "tags": ["broker-derived", f"derived-from:{provenance_id}"],
                        "source": f"derived:hermes:pre_compress:{session_id}",
                    },
                },
                "edges": {
                    "mem_derived": [
                        {"target": provenance_id, "kind": "derived_from"},
                    ],
                },
            }
            (project_dir / "proof.json").write_text(json.dumps(graph))

            old_home = os.environ.get("JCODE_HOME")
            os.environ["JCODE_HOME"] = str(jcode_home)
            try:
                proof = smoke._derived_store_proof(
                    session_id,
                    raw_terms=["remember isolated sidecar auth path"],
                )
            finally:
                if old_home is None:
                    os.environ.pop("JCODE_HOME", None)
                else:
                    os.environ["JCODE_HOME"] = old_home

        self.assertEqual(proof["hidden_provenance_count"], 1)
        self.assertEqual(proof["derived_memory_count"], 1)
        self.assertEqual(proof["derived_from_edge_count"], 1)
        self.assertTrue(proof["hidden_provenance_contains_synced_text"])


if __name__ == "__main__":
    unittest.main()
