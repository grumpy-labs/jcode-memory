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


class DuckDbOperationalStoreTests(unittest.TestCase):
    def test_durable_broker_service_formats_vault_context_items(self):
        ingestion = load_ingestion_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            vault = root / "Vault"
            vault.mkdir()
            (vault / "Alpha.md").write_text(
                "# Alpha\n"
                "DuckDB broker context should cite this vault evidence.\n"
                "[[Beta]]\n"
                "- [ ] wire vault context formatting\n",
                encoding="utf-8",
            )
            (vault / "Beta.md").write_text(
                "# Beta\n"
                "Related DuckDB graph note.\n",
                encoding="utf-8",
            )
            db_path = root / "broker.duckdb"

            payload = ingestion.run_durable_broker_service_proof(
                vault,
                db_path,
                query="DuckDB broker context",
                require_duckdb=True,
            )

        self.assertTrue(payload["duckdb_available"])
        self.assertEqual(payload["database_path"], str(db_path))
        self.assertIn("durable_import", payload["passed"])
        self.assertIn("vault_context_formatting", payload["passed"])
        self.assertIn("embedding_backfill", payload["passed"])

        context_items = payload["context_items"]
        self.assertTrue(
            any(item["kind"] == "vault_chunk" for item in context_items),
            context_items,
        )
        chunk = next(item for item in context_items if item["kind"] == "vault_chunk")
        self.assertEqual(chunk["scope"], "vault")
        self.assertEqual(chunk["content_format"], "markdown")
        self.assertEqual(chunk["origin"]["tool"], "vault_ingestion")
        self.assertEqual(chunk["origin"]["path"], "Alpha.md")
        self.assertEqual(chunk["metadata"]["durable_memory"], False)
        self.assertIn("source_checksum", chunk["metadata"])
        self.assertTrue(chunk["source"].startswith("vault://"))

    def test_durable_broker_service_reconciles_updated_and_deleted_vault_files(self):
        ingestion = load_ingestion_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            vault = root / "Vault"
            vault.mkdir()
            alpha = vault / "Alpha.md"
            alpha.write_text(
                "# Alpha\n"
                "Initial operational reconciliation evidence.\n",
                encoding="utf-8",
            )
            stale = vault / "Stale.md"
            stale.write_text("# Stale\nThis note will be removed.\n", encoding="utf-8")
            db_path = root / "broker.duckdb"

            service = ingestion.DurableDuckDbBrokerService.start(db_path)
            try:
                initial = ingestion.collect_vault_records(vault)
                service.replace_vault_records(initial)

                alpha.write_text(
                    "# Alpha\n"
                    "Updated operational reconciliation evidence for DuckDB.\n",
                    encoding="utf-8",
                )
                stale.unlink()
                updated = ingestion.collect_vault_records(vault)
                reconcile = service.reconcile_vault_records(updated)
                rows = service.execute(
                    """
                    SELECT path, deleted_at
                    FROM vault_file
                    ORDER BY path
                    """
                )
                context_items = service.query_vault_context(
                    "Updated operational reconciliation",
                    limit=5,
                )
            finally:
                service.close()

        self.assertGreater(reconcile["updated_files"], 0)
        self.assertEqual(reconcile["tombstoned_files"], 1)
        self.assertIn(("Stale.md", reconcile["deleted_at"]), rows)
        self.assertTrue(
            any(
                item["kind"] == "vault_chunk"
                and item["origin"]["path"] == "Alpha.md"
                and "Updated operational reconciliation" in item["content"]
                for item in context_items
            ),
            context_items,
        )


if __name__ == "__main__":
    unittest.main()
