import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "prove_duckdb_vault_ingestion.py"


def load_ingestion_module():
    spec = importlib.util.spec_from_file_location("prove_duckdb_vault_ingestion", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class DuckDbVaultIngestionTests(unittest.TestCase):
    def test_inventory_reads_obsidian_markdown_without_writing_source(self):
        ingestion = load_ingestion_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            vault = Path(temp_dir)
            (vault / "Project").mkdir()
            (vault / "Project" / "Alpha.md").write_text(
                "---\n"
                "project: jcode\n"
                "created: 2026-05-11\n"
                "tags: [duckdb, broker]\n"
                "---\n"
                "# Alpha\n"
                "This note links to [[Beta]], [[Missing]], and has #inline-tag.\n"
                "- [ ] prove whole-vault ingestion\n",
                encoding="utf-8",
            )
            (vault / "Beta.md").write_text("# Beta\nBacklink target.\n", encoding="utf-8")
            (vault / "diagram.png").write_bytes(b"not really an image")

            inventory = ingestion.inventory_vault(vault)

        self.assertEqual(inventory["markdown_count"], 2)
        self.assertEqual(inventory["attachment_count"], 1)
        self.assertEqual(inventory["frontmatter_error_count"], 0)
        self.assertGreaterEqual(inventory["wikilink_count"], 1)
        self.assertEqual(inventory["broken_link_count"], 1)
        self.assertIn("inline-tag", inventory["tag_counts"])

    def test_ingestion_proof_builds_duckdb_tables_and_fts(self):
        ingestion = load_ingestion_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            vault = Path(temp_dir)
            (vault / "Alpha.md").write_text(
                "# Alpha\n"
                "DuckDB operational vault retrieval should find this alpha evidence.\n"
                "[[Beta]]\n"
                "- [x] checked task\n",
                encoding="utf-8",
            )
            (vault / "Beta.md").write_text("# Beta\nRelated target.\n", encoding="utf-8")

            payload = ingestion.run_vault_ingestion_proof(
                vault,
                query="operational vault retrieval",
                require_duckdb=True,
            )

        self.assertTrue(payload["duckdb_available"])
        self.assertEqual(payload["inventory"]["markdown_count"], 2)
        self.assertGreater(payload["tables"]["vault_file"], 0)
        self.assertGreater(payload["tables"]["vault_chunk"], 0)
        self.assertGreater(payload["tables"]["vault_link"], 0)
        self.assertIn("fts_recall", payload["passed"])
        self.assertIn("link_neighborhood", payload["passed"])


if __name__ == "__main__":
    unittest.main()
