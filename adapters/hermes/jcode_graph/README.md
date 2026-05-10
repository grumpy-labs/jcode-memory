# jcode_graph Hermes Memory Provider

This is a thin Hermes memory-provider adapter for the jcode broker runtime.
Hermes owns the provider lifecycle; jcode owns the broker context snapshot.

Install target for local testing:

```bash
mkdir -p "$HERMES_HOME/plugins/jcode_graph"
cp adapters/hermes/jcode_graph/* "$HERMES_HOME/plugins/jcode_graph/"
hermes config set memory.provider jcode_graph
```

The adapter expects a running jcode broker socket. By default it looks for:

```text
~/.local/share/jcode/jcode-broker.sock
```

Override with either `JCODE_BROKER_SOCKET` or `plugins.jcode_graph.socket_path`
in the active Hermes `config.yaml`.

Current capabilities:

- Implements the Hermes `MemoryProvider` lifecycle.
- Injects `broker_context.items` via `prefetch()`.
- Exposes `jcode_broker_context` as an explicit memory-provider tool.

Not implemented yet:

- Starting or supervising `jcode broker serve`.
- Direct turn-sync writes from Hermes into jcode.
- Graph database persistence.
- Promotion/consolidation policies for search hits.
