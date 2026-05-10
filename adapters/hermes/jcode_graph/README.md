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

The adapter expects a running jcode broker socket. By default it looks for:

```text
$JCODE_RUNTIME_DIR/jcode-broker.sock, otherwise $TMPDIR/jcode-broker.sock on macOS
```

Override with either `JCODE_BROKER_SOCKET` or `plugins.jcode_graph.socket_path`
in the active Hermes `config.yaml`.

Current capabilities:

- Implements the Hermes `MemoryProvider` lifecycle.
- Injects `broker_context.items` via `prefetch()`.
- Exposes `jcode_broker_context` as an explicit memory-provider tool.

Smoke test after installing into a test Hermes profile:

```bash
JCODE_BROKER_SOCKET="$TMPDIR/jcode-broker.sock" \
hermes -p jcodegraphsmoke memory status

HERMES_HOME="$HOME/.hermes/profiles/jcodegraphsmoke" \
JCODE_BROKER_SOCKET="$TMPDIR/jcode-broker.sock" \
python scripts/hermes_jcode_graph_smoke.py --working-dir "$PWD" --json
```

Not implemented yet:

- Starting or supervising `jcode broker serve`.
- Direct turn-sync writes from Hermes into jcode.
- Graph database persistence.
- Promotion/consolidation policies for search hits.
