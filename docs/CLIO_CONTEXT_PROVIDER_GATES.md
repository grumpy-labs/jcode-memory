# Clio Context Provider Gates

This is the operational gate note for Rob's Clio/Hermes `jcode_graph`
deployment.

The current posture is **jcode with gates**:

- `jcode_graph` is the Hermes memory-provider adapter.
- The jcode broker API is the durable context/memory service behind that
  adapter.
- DuckDB is the active broker index/store on CT `1103`.
- Obsidian remains the human source-of-truth and review surface.
- Alternative providers are not promoted by novelty. They must beat jcode on
  the locked context contract while preserving Obsidian UX and provenance.

## Current Live Topology

Rob's live path is:

```text
Hermes / Clio surface
  -> jcode_graph Hermes provider
  -> /Users/rob/.jcode-memory/runtime/jcode-broker.sock
  -> SSH Unix-socket tunnel
  -> CT 1103 jcode broker
  -> /srv/hermes-jcode/broker/hermes-vault.duckdb
  -> VM 1111 /srv/obsidian/Vault source snapshot
```

CT `1150` runs server-facing Hermes/Clio surfaces and the installed
`jcode_graph` plugin. CT `1103` owns the broker service, DuckDB store, Vault
snapshot, embeddings, relationship rows, and refresh API.

## Context Contract

The provider is considered healthy only when it produces or exposes a compact
Clio Context Packet v1 with these layers:

1. Active task
2. Authority
3. Conflicts
4. Lineage
5. Vault evidence
6. Durable memory
7. Session evidence
8. Artifact refs
9. Skill hints
10. Tool hints

Normal prefetch renders budgeted Markdown. Explicit `jcode_broker_context`
tool calls may expose richer JSON for debugging and locked evals. Raw turn and
transcript provenance stays hidden from default prefetch unless deliberately
requested.

## Locked Eval Commands

Run from the repo root:

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

Use `--no-log` for local scratch runs. Omit it when the run should append to:

```text
/Users/rob/.local/state/clio/context-evals/clio-super-session-v1/runs.jsonl
```

## Current Gates

`jcode_graph` remains the incumbent while all of these stay true:

- Authority/conflict probes pass at 100%.
- Source/provenance structure probes pass at 100%.
- Hidden turn/transcript sync remains nonblocking and hidden from normal
  prefetch.
- Ordinary non-Vault prompts inject no unrelated Vault context.
- Live broker exact Ghostty and broad Ghostty retrieval stay rank-1/source
  backed.
- Live broker CURRENT relationship-neighborhood retrieval exposes relationship
  rows and conflict notes.
- Installed-provider mode proves current-plan authority, current-correction
  precedence over checkpoint evidence, exact Ghostty source span, CURRENT
  relationship rows, structured session-end handoffs, ambiguous Vault/current
  uncertainty guidance, legacy-provider restraint, non-Vault restraint, and
  hidden sync.
- Fixture mode proves generated helper-session demotion, stale prior-session
  demotion for current-state prompts, and open Vault task/checklist routing
  into `active_task`.
- The full initial suite passes at least 90%.
- Probe duration and packet budget caps pass.

As of the current context-gate suite, the latest expected results are:

```text
fixture:            20/20
live-broker:         5/5
installed-provider:  9/9
```

Latest production broker: `2ada547e1894eba738dbfb0209718f32ac82d9d9`. That
deployment captures true Hermes runtime compression summaries through
`broker_transcript_sync.runtime_summary`. Latest Mac/CT `1150` adapter/plugin:
`f3078c60673e5b6fcd4a354ee936370943099eec`. That adapter/eval slice adds
ambiguity guidance plus legacy-provider restraint gates without rebuilding CT
`1103`.

## Failure Policy

If a gate fails:

1. Classify the failure from the eval `review` field.
2. Allow one focused jcode remediation pass.
3. Re-run the same locked mode plus any affected lower-level tests.
4. If the same gate still fails, pause deeper jcode investment and start the
   read-only provider bake-off.
5. If the probe is wrong, update it only with a progress-log note explaining
   why.

Authority, conflict, provenance, and source-trace failures are blocking even
when aggregate pass rate is high. Latency and packet-budget failures are
blocking when they make Clio feel stuck, bloated, or unreliable.

## Bake-Off Trigger

Run a read-only alternative-provider bake-off only when one of these happens:

- jcode fails an authority/conflict/source-trace gate after one remediation
  pass.
- jcode cannot represent logical super-session lineage without unreasonable
  complexity.
- jcode cannot provide source-traceable packets without prompt bloat.
- DuckDB operational/index constraints become the main blocker.
- Rob explicitly asks to compare providers before a failure.

Candidate lanes should include the incumbent jcode path, a simple
Obsidian/Vault retrieval baseline, and any alternative provider that can run
isolated and read-only. Honcho and SurrealDB remain fallback/reference lanes,
not default candidates.

## Adoption And Rollback

Plan adoption and provider replacement are separate decisions.

- Adopting the super-session plan does not automatically switch providers.
- Switching providers requires a separate reviewed implementation/deployment
  plan.
- Honcho, SurrealDB, and preserved backups stay available as historical or
  rollback context until Rob explicitly approves cleanup.
- Keep useful locked probes even if a provider is replaced.

For day-to-day operation, use `docs/CLIO_JCODE_OPERATOR_GUIDE.md`. For live
deploys, use the manual dry-run-first lane in
`docs/JCODE_MEMORY_DEPLOYMENT.md`. Do not enable automatic Git pulls, timers,
or webhooks until a manual apply and rollback rehearsal are boring.

If rolling back the plan, restore the pre-adoption plan backup from the Vault
or source history, preserve the super-session plan as rejected/deferred, and
record which gate or design concern caused the rollback.
