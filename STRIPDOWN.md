# jcode-memory stripdown contract

This fork is an experimental reduction of jcode into a standalone agent harness
focused on durable situational awareness. The near-term goal is to keep the
parts that help a future Hermes adapter use jcode-style context brokerage, and
remove product surfaces that do not serve that goal.

## Keep

- Agent runtime and turn loop needed to run as a standalone harness.
- Model provider plumbing needed for local smoke tests and future adapter work.
- Memory manager, memory agent, memory graph, embeddings, sidecar relevance, and
  explicit memory tools.
- Session search and conversation search when they provide context retrieval.
- Server architecture where it supports long-lived broker/runtime behavior.
- Storage abstractions that can become graph-database adapters.
- Swarm, ambient mode, and self-dev only while they demonstrate reusable context
  hooks or coordination patterns.

## Remove First

- Mobile app and simulator surfaces.
- Desktop/app shell surfaces.
- Marketing/demo media and mockups.
- Release/distribution packaging not needed for local broker experimentation.
- Telemetry worker and analytics surfaces.
- UI-only TUI widgets once equivalent headless status/diagnostics exist.

## Non-goals

- Do not preserve jcode as a polished end-user product.
- Do not keep every provider, UI, or platform integration just because it works.
- Do not introduce Hermes adapter code until the stripped standalone harness has
  a stable internal broker boundary.

## Verification Ladder

Use the highest available rung after each reduction:

1. `cargo check -p jcode-memory-types`
2. `cargo test -p jcode-memory-types`
3. `cargo check -p jcode-agent-runtime -p jcode-storage -p jcode-provider-core`
4. `cargo check --no-default-features`
5. `cargo check`
6. A local `jcode`/broker smoke test once a binary still exists.

If the local machine lacks Rust, record that explicitly and defer code-removal
commits until a Rust toolchain or remote build path is available.
