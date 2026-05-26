from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "deploy_jcode_memory.py"


def load_deploy_module():
    spec = importlib.util.spec_from_file_location("deploy_jcode_memory", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class DeployJcodeMemoryTests(unittest.TestCase):
    def test_default_plan_is_dry_run_and_targets_both_live_copies(self):
        deploy = load_deploy_module()
        sha = "589b2a3144d5b18a3044af3f103cad9b6112a629"

        plan = deploy.build_plan(target="all", sha=sha, apply=False)
        rendered = deploy.render_plan(plan)

        self.assertFalse(plan.apply)
        self.assertIn("DRY RUN", rendered)
        self.assertIn("ct1103", plan.target_names)
        self.assertIn("ct1150", plan.target_names)
        self.assertIn(sha, rendered)

    def test_rejects_shell_like_sha_values(self):
        deploy = load_deploy_module()

        with self.assertRaises(ValueError):
            deploy.validate_sha("HEAD; rm -rf /")

        with self.assertRaises(ValueError):
            deploy.validate_sha("589b2a31")

    def test_ct1103_plan_stages_builds_installs_and_restarts_broker_only_on_apply(self):
        deploy = load_deploy_module()
        sha = "589b2a3144d5b18a3044af3f103cad9b6112a629"

        dry_plan = deploy.build_plan(target="ct1103", sha=sha, apply=False)
        apply_plan = deploy.build_plan(target="ct1103", sha=sha, apply=True)
        dry_rendered = deploy.render_plan(dry_plan)
        apply_rendered = deploy.render_plan(apply_plan)

        self.assertIn("/srv/hermes-jcode/releases/jcode-memory/" + sha, dry_rendered)
        self.assertIn("/srv/hermes-jcode/build-cache/jcode-memory-target", dry_rendered)
        self.assertIn("export CARGO_TARGET_DIR=", dry_rendered)
        self.assertIn("cargo test -q -p jcode-protocol", dry_rendered)
        self.assertIn("--features duckdb-storage-bundled,embeddings", dry_rendered)
        self.assertIn("cargo build -q --release --bin jcode", dry_rendered)
        self.assertIn("/srv/hermes-jcode/build-cache/jcode-memory-target/release/jcode", apply_rendered)
        self.assertIn("/usr/local/bin/jcode-memory-broker", apply_rendered)
        self.assertIn("systemctl restart hermes-jcode-broker.service", apply_rendered)
        self.assertNotIn("systemctl restart hermes-jcode-broker.service", dry_rendered)

    def test_ct1103_prebuilt_binary_plan_skips_target_cargo_build(self):
        deploy = load_deploy_module()
        sha = "589b2a3144d5b18a3044af3f103cad9b6112a629"
        prebuilt = "/tmp/jcode-linux-x86_64"

        dry_plan = deploy.build_plan(
            target="ct1103",
            sha=sha,
            apply=False,
            ct1103_prebuilt_binary=prebuilt,
        )
        apply_plan = deploy.build_plan(
            target="ct1103",
            sha=sha,
            apply=True,
            ct1103_prebuilt_binary=prebuilt,
        )
        dry_rendered = deploy.render_plan(dry_plan)
        apply_rendered = deploy.render_plan(apply_plan)

        self.assertIn("Upload prebuilt broker binary to CT1103", dry_rendered)
        self.assertIn(prebuilt, dry_rendered)
        self.assertIn("/srv/hermes-jcode/releases/jcode-memory/" + sha + "/jcode-prebuilt", dry_rendered)
        self.assertIn("Verify prebuilt broker binary on CT1103", dry_rendered)
        self.assertIn("grep -F 589b2a3", dry_rendered)
        self.assertNotIn("cargo build -q --release --bin jcode", dry_rendered)
        self.assertNotIn("cargo test -q -p jcode-storage", dry_rendered)
        self.assertIn("/srv/hermes-jcode/releases/jcode-memory/" + sha + "/jcode-prebuilt", apply_rendered)
        self.assertIn("/usr/local/bin/jcode-memory-broker", apply_rendered)
        self.assertIn("systemctl restart hermes-jcode-broker.service", apply_rendered)

    def test_ct1150_plan_stages_compiles_installs_and_restarts_gateway_only_on_apply(self):
        deploy = load_deploy_module()
        sha = "589b2a3144d5b18a3044af3f103cad9b6112a629"

        dry_plan = deploy.build_plan(target="ct1150", sha=sha, apply=False)
        apply_plan = deploy.build_plan(target="ct1150", sha=sha, apply=True)
        dry_rendered = deploy.render_plan(dry_plan)
        apply_rendered = deploy.render_plan(apply_plan)

        self.assertIn("/home/claw/.hermes/releases/jcode_graph/" + sha, dry_rendered)
        self.assertIn("python -m py_compile", dry_rendered)
        self.assertIn("/home/claw/.hermes/plugins/jcode_graph", apply_rendered)
        self.assertIn("systemctl restart hermes-clio-gateway.service", apply_rendered)
        self.assertIn("curl -fsS http://127.0.0.1:8642/health", apply_rendered)
        self.assertIn("for attempt in", apply_rendered)
        self.assertIn("sleep 2", apply_rendered)
        self.assertNotIn("systemctl restart hermes-clio-gateway.service", dry_rendered)


if __name__ == "__main__":
    unittest.main()
