#!/usr/bin/env python3
"""Manual, dry-run-first deployment helper for Rob's live jcode-memory copies.

This script intentionally does not install timers, webhooks, or automatic
pullers. It builds a versioned deployment plan for the two current live targets
and only executes it when called with --apply.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FULL_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")

DEFAULT_CT1103_HOST = "jcode@10.1.10.103"
DEFAULT_CT1150_HOST = "claw@10.1.10.150"
DEFAULT_PROXMOX_HOST = "root@10.1.10.19"

CT1103_RELEASE_ROOT = "/srv/hermes-jcode/releases/jcode-memory"
CT1103_CARGO_TARGET_DIR = "/srv/hermes-jcode/build-cache/jcode-memory-target"
CT1103_SOCKET = "/srv/hermes-jcode/runtime/jcode-broker.sock"
CT1103_BINARY = "/usr/local/bin/jcode-memory-broker"
CT1103_SERVICE = "hermes-jcode-broker.service"
CT1103_BROKER_FEATURES = "duckdb-storage-bundled,embeddings"

CT1150_RELEASE_ROOT = "/home/claw/.hermes/releases/jcode_graph"
CT1150_PLUGIN_DIR = "/home/claw/.hermes/plugins/jcode_graph"
CT1150_HERMES_PYTHON = "/home/claw/.hermes/hermes-agent-v0.14.0/.venv/bin/python"
CT1150_HEALTH_URL = "http://127.0.0.1:8642/health"
CT1150_SERVICE = "hermes-clio-gateway.service"


@dataclasses.dataclass(frozen=True)
class PlanStep:
    target: str
    label: str
    command: str


@dataclasses.dataclass(frozen=True)
class DeployPlan:
    sha: str
    apply: bool
    target_names: tuple[str, ...]
    steps: tuple[PlanStep, ...]


def validate_sha(value: str) -> str:
    """Return a normalized full commit SHA or raise for unsafe input."""
    if not FULL_SHA_RE.fullmatch(value):
        raise ValueError("expected a full 40-character hexadecimal commit SHA")
    return value.lower()


def _quote(value: str | os.PathLike[str]) -> str:
    return shlex.quote(os.fspath(value))


def _run_git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def resolve_head_sha() -> str:
    return validate_sha(_run_git(["rev-parse", "HEAD"]))


def upstream_name() -> str | None:
    try:
        return _run_git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    except subprocess.CalledProcessError:
        return None


def sha_is_in_upstream(sha: str) -> bool:
    upstream = upstream_name()
    if upstream is None:
        return False
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", sha, upstream],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.returncode == 0


def ssh_command(host: str, remote_command: str) -> str:
    return f"ssh {_quote(host)} {_quote('bash -lc ' + _quote(remote_command))}"


def retrying_curl_health_command(url: str, *, attempts: int = 15, delay_seconds: int = 2) -> str:
    return (
        "set -euo pipefail; "
        f"for attempt in $(seq 1 {attempts}); do "
        f"if curl -fsS {_quote(url)}; then exit 0; fi; "
        f'if [ "$attempt" -lt {attempts} ]; then sleep {delay_seconds}; fi; '
        "done; "
        "exit 1"
    )


def git_archive_to_remote_command(
    *,
    sha: str,
    host: str,
    release_dir: str,
    pathspec: str | None = None,
    strip_components: int | None = None,
) -> str:
    archive_parts = ["git", "archive", "--format=tar", sha]
    if pathspec is not None:
        archive_parts.append(pathspec)
    archive_command = " ".join(_quote(part) for part in archive_parts)

    tar_flags = "tar"
    if strip_components is not None:
        tar_flags += f" --strip-components={strip_components}"
    remote = (
        "set -euo pipefail; "
        f"release={_quote(release_dir)}; "
        'rm -rf "$release"; '
        'mkdir -p "$release"; '
        f'{tar_flags} -C "$release" -xf -'
    )
    return f"{archive_command} | {ssh_command(host, remote)}"


def _ct1103_steps(sha: str, *, apply: bool, ct1103_host: str, proxmox_host: str) -> list[PlanStep]:
    release_dir = f"{CT1103_RELEASE_ROOT}/{sha}"
    steps = [
        PlanStep(
            "ct1103",
            "Stage exact repo commit on CT1103",
            git_archive_to_remote_command(
                sha=sha,
                host=ct1103_host,
                release_dir=release_dir,
            ),
        ),
        PlanStep(
            "ct1103",
            "Build and test broker release on CT1103",
            ssh_command(
                ct1103_host,
                "set -euo pipefail; "
                'export PATH="$HOME/.cargo/bin:$PATH"; '
                f"mkdir -p {_quote(CT1103_CARGO_TARGET_DIR)}; "
                f"export CARGO_TARGET_DIR={_quote(CT1103_CARGO_TARGET_DIR)}; "
                f"cd {_quote(release_dir)}; "
                "cargo test -q -p jcode-protocol; "
                "cargo test -q -p jcode-storage --features duckdb-storage-bundled; "
                f"cargo test -q --features {CT1103_BROKER_FEATURES} --test e2e broker_runtime; "
                f"cargo build -q --release --bin jcode --features {CT1103_BROKER_FEATURES}",
            ),
        ),
    ]
    if apply:
        steps.extend(
            [
                PlanStep(
                    "ct1103",
                    "Install broker binary and restart CT1103 service",
                    ssh_command(
                        proxmox_host,
                        f"pct exec 1103 -- install -m 0755 {_quote(CT1103_CARGO_TARGET_DIR + '/release/jcode')} "
                        f"{_quote(CT1103_BINARY)} && "
                        f"pct exec 1103 -- systemctl restart {CT1103_SERVICE} && "
                        f"pct exec 1103 -- systemctl is-active {CT1103_SERVICE}",
                    ),
                ),
                PlanStep(
                    "ct1103",
                    "Verify broker socket is present",
                    ssh_command(ct1103_host, f"test -S {_quote(CT1103_SOCKET)}"),
                ),
            ]
        )
    return steps


def _ct1150_steps(sha: str, *, apply: bool, ct1150_host: str, proxmox_host: str) -> list[PlanStep]:
    release_dir = f"{CT1150_RELEASE_ROOT}/{sha}"
    steps = [
        PlanStep(
            "ct1150",
            "Stage exact Hermes jcode_graph plugin commit on CT1150",
            git_archive_to_remote_command(
                sha=sha,
                host=ct1150_host,
                release_dir=release_dir,
                pathspec="adapters/hermes/jcode_graph",
                strip_components=3,
            ),
        ),
        PlanStep(
            "ct1150",
            "Compile staged Hermes plugin on CT1150",
            ssh_command(
                ct1150_host,
                f"{_quote(CT1150_HERMES_PYTHON)} -m py_compile {_quote(release_dir + '/__init__.py')}",
            ),
        ),
    ]
    if apply:
        install_remote = (
            "set -euo pipefail; "
            f"release={_quote(release_dir)}; "
            f"plugin={_quote(CT1150_PLUGIN_DIR)}; "
            'backup="${plugin}.backup-$(date +%Y%m%d%H%M%S)"; '
            'cp -a "$plugin" "$backup"; '
            'install -m 0644 "$release/__init__.py" "$plugin/__init__.py"; '
            'if [ -f "$release/plugin.yaml" ]; then install -m 0644 "$release/plugin.yaml" "$plugin/plugin.yaml"; fi; '
            'if [ -f "$release/README.md" ]; then install -m 0644 "$release/README.md" "$plugin/README.md"; fi; '
            'printf "backup=%s\\n" "$backup"'
        )
        steps.extend(
            [
                PlanStep(
                    "ct1150",
                    "Back up and install staged Hermes plugin",
                    ssh_command(ct1150_host, install_remote),
                ),
                PlanStep(
                    "ct1150",
                    "Restart CT1150 Hermes/Clio gateway",
                    ssh_command(
                        proxmox_host,
                        f"pct exec 1150 -- systemctl restart {CT1150_SERVICE} && "
                        f"pct exec 1150 -- systemctl is-active {CT1150_SERVICE}",
                    ),
                ),
                PlanStep(
                    "ct1150",
                    "Verify CT1150 gateway health",
                    ssh_command(ct1150_host, retrying_curl_health_command(CT1150_HEALTH_URL)),
                ),
            ]
        )
    return steps


def build_plan(
    *,
    target: str,
    sha: str,
    apply: bool,
    ct1103_host: str = DEFAULT_CT1103_HOST,
    ct1150_host: str = DEFAULT_CT1150_HOST,
    proxmox_host: str = DEFAULT_PROXMOX_HOST,
) -> DeployPlan:
    sha = validate_sha(sha)
    if target not in {"ct1103", "ct1150", "all"}:
        raise ValueError("target must be one of: ct1103, ct1150, all")

    target_names: list[str]
    if target == "all":
        target_names = ["ct1103", "ct1150"]
    else:
        target_names = [target]

    steps: list[PlanStep] = []
    if "ct1103" in target_names:
        steps.extend(_ct1103_steps(sha, apply=apply, ct1103_host=ct1103_host, proxmox_host=proxmox_host))
    if "ct1150" in target_names:
        steps.extend(_ct1150_steps(sha, apply=apply, ct1150_host=ct1150_host, proxmox_host=proxmox_host))

    return DeployPlan(sha=sha, apply=apply, target_names=tuple(target_names), steps=tuple(steps))


def render_plan(plan: DeployPlan) -> str:
    mode = "APPLY" if plan.apply else "DRY RUN"
    lines = [
        f"jcode-memory deploy plan ({mode})",
        f"sha: {plan.sha}",
        f"targets: {', '.join(plan.target_names)}",
        "",
    ]
    for index, step in enumerate(plan.steps, start=1):
        lines.append(f"{index}. [{step.target}] {step.label}")
        lines.append(f"   $ {step.command}")
    if not plan.apply:
        lines.extend(
            [
                "",
                "No commands were executed. Re-run with --apply to run this plan.",
            ]
        )
    return "\n".join(lines)


def execute_plan(plan: DeployPlan) -> None:
    if not plan.apply:
        print(render_plan(plan))
        return
    for index, step in enumerate(plan.steps, start=1):
        print(f"\n[{index}/{len(plan.steps)}] {step.target}: {step.label}", flush=True)
        print(f"$ {step.command}", flush=True)
        subprocess.run(step.command, cwd=ROOT, shell=True, check=True)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=("ct1103", "ct1150", "all"),
        default="all",
        help="Live target to prepare. Default: all.",
    )
    parser.add_argument(
        "--sha",
        help="Full 40-character commit SHA to deploy. Default: current HEAD.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the deployment plan. Without this flag the script only prints the plan.",
    )
    parser.add_argument(
        "--allow-unpushed",
        action="store_true",
        help="Allow deploying a SHA that is not reachable from the configured upstream branch.",
    )
    parser.add_argument("--ct1103-host", default=DEFAULT_CT1103_HOST)
    parser.add_argument("--ct1150-host", default=DEFAULT_CT1150_HOST)
    parser.add_argument("--proxmox-host", default=DEFAULT_PROXMOX_HOST)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        sha = validate_sha(args.sha) if args.sha else resolve_head_sha()
        if not args.allow_unpushed and not sha_is_in_upstream(sha):
            print(
                "error: SHA is not reachable from the configured upstream branch; "
                "push first or pass --allow-unpushed for an intentional local-only deploy",
                file=sys.stderr,
            )
            return 2
        plan = build_plan(
            target=args.target,
            sha=sha,
            apply=args.apply,
            ct1103_host=args.ct1103_host,
            ct1150_host=args.ct1150_host,
            proxmox_host=args.proxmox_host,
        )
    except (subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(render_plan(plan))
    if args.apply:
        execute_plan(plan)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
