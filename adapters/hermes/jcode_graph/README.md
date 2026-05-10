# jcode_graph Hermes Memory Provider

This is a thin Hermes memory-provider adapter for the jcode broker runtime.
Hermes owns the provider lifecycle; jcode owns the broker context snapshot.

Install target for local testing:

```bash
hermes profile create jcodegraphsmoke --clone --no-alias
PROFILE_HOME="$HOME/.hermes/profiles/jcodegraphsmoke"
mkdir -p "$PROFILE_HOME/plugins/jcode_graph"
cp -R adapters/hermes/jcode_graph/. "$PROFILE_HOME/plugins/jcode_graph/"
hermes -p jcodegraphsmoke config set memory.provider jcode_graph
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

Current capabilities:

- Implements the Hermes `MemoryProvider` lifecycle.
- Injects `broker_context.items` via `prefetch()`.
- Exposes `jcode_broker_context` as an explicit memory-provider tool.

Smoke test after installing into a test Hermes profile:

```bash
hermes -p jcodegraphsmoke memory status

HERMES_HOME="$HOME/.hermes/profiles/jcodegraphsmoke" \
python scripts/hermes_jcode_graph_smoke.py --working-dir "$PWD" --json
```

Not implemented yet:

- Direct turn-sync writes from Hermes into jcode.
- Graph database persistence.
- Promotion/consolidation policies for search hits.
