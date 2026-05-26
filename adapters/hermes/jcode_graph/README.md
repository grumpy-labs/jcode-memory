# jcode_graph Hermes Memory Provider

This is the Hermes memory-provider adapter for the jcode broker runtime.
Hermes owns the provider lifecycle, prompt injection, and compression hooks;
jcode owns memory ingestion, derived extraction, typed context assembly,
retrieval metadata, and provenance.

Install target for local testing:

```bash
hermes profile create jcodegraphsmoke --clone --no-alias
PROFILE_HOME="$HOME/.hermes/profiles/jcodegraphsmoke"
mkdir -p "$PROFILE_HOME/plugins/jcode_graph"
cp -R adapters/hermes/jcode_graph/. "$PROFILE_HOME/plugins/jcode_graph/"
hermes -p jcodegraphsmoke config set memory.provider jcode_graph
hermes -p jcodegraphsmoke config set plugins.jcode_graph.working_dir "$PWD"
hermes -p jcodegraphsmoke config set plugins.jcode_graph.jcode_binary "$PWD/target/debug/jcode"
```

The adapter connects to a jcode broker socket. If the socket is unavailable,
it starts `jcode broker serve --socket ... --quiet` automatically when a jcode
binary is available. By default it looks for:

```text
$JCODE_RUNTIME_DIR/jcode-broker.sock, otherwise $TMPDIR/jcode-broker.sock on macOS
```

Override the socket with either `JCODE_BROKER_SOCKET` or
`plugins.jcode_graph.socket_path` in the active Hermes `config.yaml`.

Useful config keys:

- `auto_start`: defaults to `true`; set `false` to require an already-running broker.
- `jcode_binary`: path to `jcode`/`jcode-memory`; also accepts `JCODE_BINARY`.
- `startup_timeout_seconds`: wait time for the broker socket after auto-start.
- `working_dir`: project directory for broker-scoped context.
- `source`: source label for synced Hermes turns; defaults to `hermes`.
- `sync_turns`: defaults to `true`; writes completed Hermes turns as hidden broker provenance.
- `sync_transcripts`: defaults to `true`; sends `on_pre_compress` and `on_session_end` transcripts for broker extraction.
- `include_provenance`: defaults to `false`; keep raw provenance out of normal prefetch.
- `context_limit`: broker items requested for normal prefetch.
- `max_chars`: total formatted context budget for normal prefetch.
- `item_max_chars`: per-item summary/content budget.
- `transcript_max_chars`: maximum transcript size sent through lifecycle hooks.
- `turn_buffer_limit`: recent completed turns retained as fallback when Hermes does not pass messages to `on_session_end`.
- `turn_buffer_max_chars`: per-user/per-assistant field budget for the fallback turn buffer.
- `tool_inventory_limit`: maximum broker tool names shown in the compact tool inventory section.

Current capabilities:

- Implements the Hermes `MemoryProvider` lifecycle.
- Injects grouped `broker_context.items` via `prefetch()`.
- Keeps hidden raw provenance out of normal prefetch unless explicitly configured.
- Exposes `jcode_broker_context` as an explicit memory-provider tool.
- Lets the model request provenance deliberately with `include_provenance: true`.
- Syncs Hermes turns back into jcode memory via `broker_turn_sync`.
- Syncs compression/session-end transcripts via `broker_transcript_sync`.
- Reports lightweight diagnostics through `provider.diagnostics()` for smoke checks.

Normal prefetch is formatted in this order:

1. Memories
2. Goals and Todos
3. Evidence (`session_search_hit` and `conversation_search_hit`)
4. Skill Candidates
5. Artifacts
6. Broker Tools
7. Other Context

Smoke test after installing into a test Hermes profile:

```bash
hermes -p jcodegraphsmoke memory status

HERMES_HOME="$HOME/.hermes/profiles/jcodegraphsmoke" \
HERMES_AGENT_REPO="$HOME/.hermes/hermes-agent-v0.13.0" \
JCODE_BINARY="$PWD/target/debug/jcode" \
JCODE_RUNTIME_DIR="$(mktemp -d /tmp/jcode-smoke-runtime.XXXXXX)" \
JCODE_HOME="$(mktemp -d /tmp/jcode-smoke-home.XXXXXX)" \
"$HOME/.hermes/hermes-agent-v0.13.0/venv311/bin/python" scripts/hermes_jcode_graph_smoke.py \
  --working-dir "$PWD" \
  --query "phase four live smoke provenance" \
  --sync-user "phase four live smoke provenance user turn" \
  --sync-assistant "phase four live smoke provenance assistant turn" \
  --transcript-user "phase four live smoke transcript extraction" \
  --transcript-assistant "phase four live smoke transcript response" \
  --json
```

The smoke output should show:

- `prefetch_has_context: true`
- `default_prefetch_has_raw_provenance: false`
- `provenance_tool_contains_synced_text: true`
- non-zero `turn_sync_count` and `transcript_sync_count` diagnostics

For the non-Vault restraint gate, use an ordinary prompt for `--query`, a
context-bearing prompt for `--tool-query`, and `--expect-no-prefetch`. This
passes only when normal prefetch injects no context while the explicit
`jcode_broker_context` tool still reaches the broker:

```bash
HERMES_HOME="$HOME/.hermes" \
HERMES_AGENT_REPO="$HOME/.hermes/hermes-agent" \
JCODE_BROKER_SOCKET="$HOME/.jcode-memory/runtime/jcode-broker.sock" \
"$HOME/.hermes/hermes-agent/.venv/bin/python" scripts/hermes_jcode_graph_smoke.py \
  --working-dir "$HOME/Vault/Projects/Hermes-Honcho-LangGraph-Second-Brain" \
  --query "Answer in one concise sentence: what makes a cup of tea relaxing?" \
  --tool-query "Clio current context packet provider gate" \
  --expect-no-prefetch \
  --json
```

Not implemented yet:

- Graph database persistence.
- Promotion/consolidation policies for search hits.
