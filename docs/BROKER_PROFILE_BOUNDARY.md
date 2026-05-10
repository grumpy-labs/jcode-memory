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

Regression coverage:

- `broker_profile_keeps_exact_context_broker_tool_set`
- `registry_profile_marks_operator_presentation_boundary`
- `broker_goal_writes_context_artifact_without_side_panel_tool`

## Next Pruning Rule

When removing presentation code, do not remove `src/side_panel.rs`, `crates/jcode-side-panel-types`, bus/server side-panel events, or `goal` artifact writes unless a replacement context-artifact store exists first.
