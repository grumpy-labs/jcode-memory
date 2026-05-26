# jcode-memory Deployment Lane

This repo is now the source-controlled home for Rob's custom `jcode-memory`
broker and Hermes `jcode_graph` adapter work. Live deployments stay as separate
copies/services:

- CT `1103` runs the broker, DuckDB store, and Vault index.
- CT `1150` runs Hermes/Clio gateway surfaces and the installed `jcode_graph`
  Hermes plugin.

The first deployment lane is intentionally manual and dry-run-first. It does not
install systemd timers, webhooks, or automatic Git pullers.

## Current Manual Flow

1. Commit and push the intended repo state to GitHub.
2. Print the deploy plan:

   ```bash
   scripts/deploy_jcode_memory.py --target all
   ```

3. Read the plan. Confirm the SHA, release directories, tests, binaries, plugin
   paths, and service names are the ones expected.
4. Apply only when ready:

   ```bash
   scripts/deploy_jcode_memory.py --target all --apply
   ```

By default the script refuses to deploy a commit that is not reachable from the
configured upstream branch. Use `--allow-unpushed` only for an intentional
local-only emergency deploy.

## Inner Loop Policy

Do not deploy CT `1103` for changes that do not affect the production broker
binary, such as docs, eval fixture text, local-only scripts, comments, report
formatting, or analysis artifacts. Run the relevant local checks instead.

For small broker-code changes, run fast local tests first and batch related
fixes into one CT `1103` deploy when safe. Keep live-broker and
installed-provider gates meaningful after the deploy, but do not pay the full
remote build cost for every tiny iteration.

When a Linux-compatible release binary has already been built and tested on a
stronger or warmer build host, CT `1103` can skip its target-side Cargo build:

```bash
scripts/deploy_jcode_memory.py \
  --target ct1103 \
  --use-local-binary /absolute/path/to/jcode-linux-x86_64 \
  --allow-unpushed
```

The helper still stages the exact repo commit, uploads the binary into that
release directory, verifies `jcode --version` contains the target commit hash
on CT `1103`, and only installs it with `--apply`. Use this lane only after
focused local/remote build checks have already proven the binary for the commit
being deployed.

`--use-local-binary` is an operator-friendly alias for
`--ct1103-prebuilt-binary`; both flags feed the same guarded prebuilt-binary
deploy path.

Production embeddings remain `mxbai-embed-large:latest`. Do not change the
embedding model or mix embeddings from different models in the same production
index as part of deploy-loop speed work.

## Target Details

### CT1103 Broker

The CT `1103` target stages the exact Git commit under:

```text
/srv/hermes-jcode/releases/jcode-memory/<sha>
```

It then runs the focused broker/storage checks and builds the release binary:

```bash
CARGO_TARGET_DIR=/srv/hermes-jcode/build-cache/jcode-memory-target
cargo test -q -p jcode-protocol
cargo test -q -p jcode-storage --features duckdb-storage-bundled
cargo test -q --features duckdb-storage-bundled,embeddings --test e2e broker_runtime
cargo build -q --release --bin jcode --features duckdb-storage-bundled,embeddings
```

The shared `CARGO_TARGET_DIR` is intentionally outside the per-SHA release
directory so CT `1103` can reuse Cargo/DuckDB/embedding build artifacts across
deploys while still staging and building the exact requested SHA.

For the prebuilt-binary lane, the target-side Cargo step above is replaced with:

```bash
scp /absolute/path/to/jcode-linux-x86_64 \
  jcode@10.1.10.103:/srv/hermes-jcode/releases/jcode-memory/<sha>/jcode-prebuilt
ssh jcode@10.1.10.103 \
  '/srv/hermes-jcode/releases/jcode-memory/<sha>/jcode-prebuilt --version | grep -F <short-sha>'
```

The final install path and service restart are unchanged.

Only with `--apply`, it installs:

```text
/usr/local/bin/jcode-memory-broker
```

and restarts:

```text
hermes-jcode-broker.service
```

The post-restart check verifies that the broker socket exists at:

```text
/srv/hermes-jcode/runtime/jcode-broker.sock
```

### CT1150 Hermes Plugin

The CT `1150` target stages only the Hermes adapter subtree under:

```text
/home/claw/.hermes/releases/jcode_graph/<sha>
```

It compiles the staged plugin with the live Hermes v0.14 virtual environment.

Only with `--apply`, it backs up and updates:

```text
/home/claw/.hermes/plugins/jcode_graph
```

Then it restarts:

```text
hermes-clio-gateway.service
```

and verifies:

```text
http://127.0.0.1:8642/health
```

## Future Automation Gate

Do not enable automatic CT updates yet. The safe next step is to run this manual
lane for several normal repo changes and record the evidence in the Master Plan.

When that is boring, add a second phase that converts the release staging into a
Git-backed pull/fetch workflow on CT `1103` and CT `1150`, with:

- separate service-owned checkouts or release directories;
- pinned commit SHAs, not floating branch deploys;
- rollback to the previous release path;
- systemd timers or webhook-triggered pulls disabled by default during rehearsal;
- health checks before and after service restarts.
