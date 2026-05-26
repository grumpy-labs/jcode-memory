# Clio jcode Operator Guide

This guide is for Rob's live Clio/Hermes memory and context path.

Use `docs/CLIO_CONTEXT_PROVIDER_GATES.md` for gate policy and
`docs/JCODE_MEMORY_DEPLOYMENT.md` for release deployment mechanics. This file
is the day-to-day operator map.

## Names

- `jcode_graph`: Hermes memory-provider adapter. It is the provider edge that
  Hermes loads.
- jcode broker API: headless context/memory service behind the provider.
- DuckDB broker store: operational index used by the broker.
- Obsidian Vault: human source-of-truth and review surface.
- CT `1103`: broker/index host.
- CT `1150`: server-facing Hermes/Clio surface host.
- VM `1111`: canonical Obsidian Vault host.

Do not call the whole system "the jcode provider." The clearer split is:

```text
jcode_graph provider -> jcode broker API -> DuckDB/Vault index
```

## Local Mac Paths

```text
/Users/rob/.hermes/plugins/jcode_graph
/Users/rob/.hermes/config.yaml
/Users/rob/.jcode-memory/runtime/jcode-broker.sock
/Users/rob/.jcode-memory/bin/start-gateway-broker-tunnel.sh
/Users/rob/.jcode-memory/bin/refresh-gateway-vault.sh
/Users/rob/Code/grumpy-labs/jcode-memory
/Users/rob/Vault/Projects/Hermes-Honcho-LangGraph-Second-Brain
```

Current local Hermes config points `jcode_graph` at:

```yaml
working_dir: /Users/rob/Vault/Projects/Hermes-Honcho-LangGraph-Second-Brain
jcode_binary: /Users/rob/.local/bin/jcode-memory-broker
socket_path: /Users/rob/.jcode-memory/runtime/jcode-broker.sock
duckdb_path: /srv/hermes-jcode/broker/hermes-vault.duckdb
gateway_host: 10.1.10.103
gateway_remote_socket: /srv/hermes-jcode/runtime/jcode-broker.sock
auto_start: false
sync_turns: true
sync_transcripts: true
include_provenance: false
context_limit: 8
max_chars: 4000
item_max_chars: 500
timeout_seconds: 45
turn_sync_timeout_seconds: 2
transcript_sync_timeout_seconds: 5
startup_timeout_seconds: 5
```

## Gateway Paths

CT `1103` broker target paths:

```text
/srv/hermes-jcode/runtime/jcode-broker.sock
/srv/hermes-jcode/broker/hermes-vault.duckdb
/srv/hermes-jcode/vault
/usr/local/bin/jcode-memory-broker
hermes-jcode-broker.service
```

CT `1150` Hermes plugin target paths:

```text
/home/claw/.hermes/plugins/jcode_graph
/home/claw/.hermes/releases/jcode_graph/<sha>
hermes-clio-gateway.service
```

VM `1111` Vault source:

```text
/srv/obsidian/Vault
```

## Refresh Vault Context

After meaningful Vault-note edits that Clio should retrieve:

```bash
/Users/rob/.jcode-memory/bin/refresh-gateway-vault.sh
```

Expected successful shape:

```json
{
  "type": "broker_vault_refreshed",
  "vault": "/srv/hermes-jcode/vault",
  "db": "/srv/hermes-jcode/broker/hermes-vault.duckdb",
  "updated_files": 1
}
```

`updated_files` may be `0` when no indexed note changed.

## Verify Provider Health

From the repo root:

```bash
cargo run -q --bin jcode -- broker eval-context \
  --suite clio-super-session-v1 \
  --mode fixture \
  --json
```

```bash
JCODE_BROKER_SOCKET=/Users/rob/.jcode-memory/runtime/jcode-broker.sock \
cargo run -q --bin jcode -- broker eval-context \
  --suite clio-super-session-v1 \
  --mode live-broker \
  --json
```

```bash
HERMES_HOME=/Users/rob/.hermes \
HERMES_AGENT_REPO=/Users/rob/.hermes/hermes-agent \
HERMES_JCODE_GRAPH_SMOKE_PYTHON=/Users/rob/.hermes/hermes-agent/.venv/bin/python \
JCODE_BROKER_SOCKET=/Users/rob/.jcode-memory/runtime/jcode-broker.sock \
cargo run -q --bin jcode -- broker eval-context \
  --suite clio-super-session-v1 \
  --mode installed-provider \
  --json
```

Current expected gate counts:

```text
fixture:            15/15
live-broker:         5/5
installed-provider:  7/7
```

The harmless local warning currently seen on some runs is:

```text
jcode logger cleanup failed: Operation not permitted (os error 1)
```

Treat a non-empty `review.top_failures` field as the important signal.

## Deploy Code

Do not manually copy random files into CT `1103` or CT `1150` as a normal
workflow. Use the dry-run-first deployment lane:

```bash
scripts/deploy_jcode_memory.py --target all
```

Only apply after reading the plan:

```bash
scripts/deploy_jcode_memory.py --target all --apply
```

The script intentionally keeps CT `1103` broker rollout and CT `1150`
`jcode_graph` plugin rollout explicit. It does not enable automatic Git
pulls, timers, or webhooks.

CT `1103` deploys reuse a persistent Cargo target cache at:

```text
/srv/hermes-jcode/build-cache/jcode-memory-target
```

This keeps the full target-side checks and release build, but avoids cold
rebuilding bundled DuckDB and embedding dependencies for every small SHA.
Do not deploy CT `1103` for docs, fixture-only, local-script, formatting, or
analysis-only changes.

## Roll Back

Plan rollback:

1. Restore the pre-adoption Vault plan backup or source-history version.
2. Preserve the super-session plan as rejected/deferred evidence.
3. Record which gate or design concern caused rollback.
4. Keep useful locked probes.

Provider rollback:

1. Change Hermes `memory.provider` away from `jcode_graph` only through a
   reviewed config change.
2. Preserve `/Users/rob/.hermes/plugins/jcode_graph` and CT `1103` broker
   state until Rob explicitly approves cleanup.
3. Stop or disable CT services only with a rollback bundle and service-specific
   notes.
4. Keep Honcho and SurrealDB data as historical/fallback context unless Rob
   approves archival or deletion.

Plan adoption does not equal provider replacement. Provider replacement needs a
separate reviewed implementation/deployment plan.

## Honcho Transition Status

For Rob's current Clio use case, jcode replaces Honcho as the active
memory/context provider for:

- current Vault-backed context retrieval;
- source-spanned evidence;
- currentness/conflict ranking;
- logical super-session lineage;
- Hermes turn/transcript hidden provenance sync;
- locked context-contract gates.

Honcho remains historical/rollback context. Do not revive Honcho as current
provider truth unless Rob explicitly reopens that provider decision.

## Common Failure Handling

- If the Mac socket is missing, check the gateway tunnel before changing
  provider config.
- If Vault context is stale, run `refresh-gateway-vault.sh` before debugging
  retrieval code.
- If exact source recall fails, run live-broker mode first, then
  installed-provider mode.
- If installed-provider mode fails while live-broker passes, suspect adapter
  rendering/config/plugin deployment.
- If live-broker fails, suspect broker retrieval/index/currentness logic or
  stale CT `1103` Vault state.
- If a general non-Vault prompt injects Vault context, treat it as a
  non-Vault restraint regression.
