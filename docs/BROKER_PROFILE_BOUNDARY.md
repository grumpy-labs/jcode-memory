# Broker Profile Boundary

Status: Active stripdown guidance

The broker profile should run headless and API-first. The TUI remains useful, but it is an operator/debugging surface that can attach to, inspect, and steer the runtime rather than a required part of broker execution.

## Keep In Broker Core

- Context broker tools: `memory`, `goal`, `todo`, `session_search`, `conversation_search`, `swarm`.
- Read/search tools needed to ground context decisions: `read`, `ls`, `glob`, `grep`.
- Side-panel context artifacts produced by broker-safe tools such as `goal`.
- Server/client state events that expose context artifacts to observers.
- Ambient architecture in the retained harness/server stack.

## Keep As Operator Presentation

- The manual `side_panel` tool.
- Ratatui rendering, markdown/mermaid/image side-panel display, and keyboard/mouse affordances.
- TUI commands that open or focus panes for a human operator.

## Current Code Boundary

- `RegistryProfile::Broker` does not expose the manual `side_panel` tool.
- `RegistryProfile::Full` exposes operator presentation tools.
- Broker-safe tools may still write side-panel snapshots as durable context artifacts. This preserves inspectability without making the TUI load-bearing.
- The typed `broker_context` protocol request exposes broker-safe tool inventory, scoped memory items, working directory, and side-panel context artifacts without requiring debug command strings.
- `broker_context.items` is the normalized context stream for provider adapters. It currently maps broker tools, memories, goal/side-panel artifacts, and session todos while keeping the legacy `tool_names`, `memories`, and `side_panel` fields available during the transition.
- Each broker context item carries explicit `origin`, optional `relevance`, and optional `fragments` fields so future session-search, conversation-search, and graph-database records can keep provenance separate from durable memory semantics.
- `jcode broker serve` starts the headless broker runtime with the broker tool profile and a distinct default socket, `jcode-broker.sock`.

Regression coverage:

- `broker_profile_keeps_exact_context_broker_tool_set`
- `registry_profile_marks_operator_presentation_boundary`
- `broker_goal_writes_context_artifact_without_side_panel_tool`
- `broker_serve_subcommand_parses`
- `broker_server_mode_forces_broker_profile_and_default_socket`
- `daemon_lock_path_follows_socket_name`
- `broker_headless_session_exposes_context_artifacts_over_api`
- `typed_broker_context_api_returns_memory_tools_and_artifacts`

## Future Product Split

The current branch uses command boundaries, tool profiles, and binary gates as a transition path. The intended long-term split is still product-level:

- `jcode-harness`: standalone coding-agent harness, TUI/server/client runtime, local sessions, provider execution, swarm coordination, self-dev, and operator UX.
- `jcode-memory-broker`: headless context/memory broker, API-first runtime, memory/goal/todo/session-search/conversation-search/swarm context surfaces, retrieval/extraction/consolidation, and context artifact publication.

The shared core should stay small and boring: provider contracts, message/session/protocol DTOs, tool execution contracts, storage/path primitives, and context-artifact DTOs. The TUI, keybindings, rendering, browser/Gmail/schedule/product tools, desktop/mobile packaging, and release plumbing belong on the harness side. Broker APIs, retrieval policies, extraction/consolidation policies, graph/database adapters, and the Hermes memory-provider adapter belong on the broker side.

Do not split crates or repositories until the broker API and artifact contract have survived a few more cuts. Premature physical separation would freeze unstable boundaries and make the stripdown harder to learn from.

## Next Pruning Rule

When removing presentation code, do not remove `src/side_panel.rs`, `crates/jcode-side-panel-types`, bus/server side-panel events, or `goal` artifact writes unless a replacement context-artifact store exists first.
