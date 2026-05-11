import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "prove_duckdb_graph_candidate.py"


def load_probe_module():
    spec = importlib.util.spec_from_file_location("prove_duckdb_graph_candidate", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class DuckDbCandidateProbeTests(unittest.TestCase):
    def test_sample_records_cover_broker_and_vault_access_patterns(self):
        probe = load_probe_module()

        records = probe.sample_records()
        node_ids = {node["id"] for node in records.nodes}
        edge_keys = {(edge["source_id"], edge["target_id"], edge["kind"]) for edge in records.edges}

        self.assertIn("mem_prov_transcript_1", node_ids)
        self.assertIn("mem_derived_broker_tests", node_ids)
        self.assertIn("vault_chunk_jcode_plan_1", node_ids)
        self.assertIn(
            ("mem_derived_broker_tests", "mem_prov_transcript_1", "DerivedFrom"),
            edge_keys,
        )
        self.assertIn(("vault_chunk_jcode_plan_1", "vault_file_jcode_plan", "ChunkOf"), edge_keys)
        self.assertTrue(
            any(edge["kind"] == "Mentions" for edge in records.edges),
            "probe should model note-to-memory neighborhood traversal",
        )

    def test_queries_include_required_duckdb_capabilities_and_operational_warning(self):
        probe = load_probe_module()

        queries = probe.duckdb_probe_queries()

        self.assertIn("recursive_neighborhood", queries)
        self.assertIn("sql_property_graph_fallback", queries)
        self.assertIn("fts_search", queries)
        self.assertIn("vector_search", queries)
        self.assertIn("duckpgq_property_graph", queries)
        self.assertIn("single writer", probe.OPERATIONAL_WARNINGS[0].lower())

    def test_duckdb_probe_reports_operational_store_checks(self):
        probe = load_probe_module()

        payload = probe.run_probe(require_duckdb=True)
        check_names = {check["name"] for check in payload["checks"]}

        self.assertIn("sql_property_graph_fallback", check_names)
        self.assertIn("single_writer_broker_service", check_names)
        self.assertIn("vault_update_delete_reconciliation", check_names)
        self.assertIn("backup_restore", check_names)
        self.assertIn("sql_property_graph_fallback", payload["passed"])


if __name__ == "__main__":
    unittest.main()
