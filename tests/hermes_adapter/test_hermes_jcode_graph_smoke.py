from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


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
    def test_provenance_query_prefers_synced_raw_term(self) -> None:
        smoke = load_smoke_module()

        query = smoke._provenance_query(
            "Clio current context packet provider gate",
            raw_terms=["unique synced smoke marker"],
        )

        self.assertEqual(query, "unique synced smoke marker")

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
