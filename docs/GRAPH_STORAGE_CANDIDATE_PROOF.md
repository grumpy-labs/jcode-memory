# Graph Storage Candidate Proof

Status date: 2026-05-11

This note records the first Phase 5 storage proof for the jcode nervous-system
broker. It is intentionally evidence-oriented: the winner should be chosen by
broker-shaped access patterns, not by generic database enthusiasm.

## Current Direction

DuckDB should be evaluated first because it matches Rob's preferred ecosystem:
SQL, Parquet, data science tooling, visualization integrations, and a large
extension surface. SurrealDB remains the validated operational baseline until a
jcode-shaped proof displaces it.

The likely architecture is still hybrid:

- Operational graph/search store behind the jcode broker storage boundary.
- DuckDB/Parquet analytical mirror for data science, reporting, evaluation, and
  bulk graph/corpus analysis.
- Source-of-truth raw Vault files on the gateway Vault, with graph/index records
  storing metadata, chunks, embeddings, summaries, tasks, links, and provenance.

## References Used

- DuckDB graph queries / DuckPGQ: https://duckdb.org/docs/current/guides/sql_features/graph_queries
- DuckDB recursive CTEs / `WITH RECURSIVE`: https://duckdb.org/docs/current/sql/query_syntax/with
- DuckDB full-text search: https://duckdb.org/docs/current/core_extensions/full_text_search
- DuckDB vector similarity search: https://duckdb.org/docs/current/core_extensions/vss
- DuckDB concurrency: https://duckdb.org/docs/current/connect/concurrency
- awesome-duckdb: https://github.com/davidgasquez/awesome-duckdb
- Hugr: https://hugr-lab.github.io/
- Kuzu: https://github.com/kuzudb/kuzu
- Kuzu DuckDB extension docs: https://docs.kuzudb.com/extensions/attach/duckdb/
- Onager DuckDB graph analytics extension: https://github.com/CogitatorTech/onager
- SurrealDB graph model: https://surrealdb.com/docs/surrealdb/models/graph
- SurrealDB vector model: https://surrealdb.com/docs/surrealdb/models/vector
- SurrealDB full-text model: https://surrealdb.com/docs/surrealdb/models/full-text-search
- Neo4j vector indexes: https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/
- Neo4j Graph Data Science: https://neo4j.com/docs/graph-data-science/current/
- ArangoDB overview: https://docs.arangodb.com/3.13/about-arangodb/
- pgvector: https://github.com/pgvector/pgvector
- Redis vector search: https://redis.io/docs/latest/develop/ai/search-and-query/vectors/

## Probe Artifact

Runnable proof:

```bash
/Users/rob/.local/bin/uv run --with duckdb \
  python scripts/prove_duckdb_graph_candidate.py --json
```

Durable whole-Vault broker-service proof:

```bash
tmpdir=$(mktemp -d /tmp/jcode-duckdb-durable.XXXXXX)
/Users/rob/.local/bin/uv run --with duckdb --with pyyaml \
  python scripts/prove_duckdb_vault_ingestion.py \
  --vault /Users/rob/Vault \
  --query "jcode broker memory DuckDB graph" \
  --durable-db "$tmpdir/broker.duckdb" \
  --json --require-duckdb
rm -rf "$tmpdir"
```

Unit test:

```bash
/Users/rob/.local/bin/uv run --with duckdb --with pyyaml \
  python -m unittest \
  tests/storage_candidate/test_duckdb_candidate_probe.py \
  tests/storage_candidate/test_duckdb_vault_ingestion.py \
  tests/storage_candidate/test_duckdb_operational_store.py
```

Rust storage-boundary checks:

```bash
cargo test -q -p jcode-storage memory_graph_store --no-default-features
DUCKDB_DOWNLOAD_LIB=1 cargo test -q -p jcode-storage \
  --features duckdb-storage memory_graph_store
DUCKDB_DOWNLOAD_LIB=1 cargo test -q -p jcode-storage \
  --features duckdb-storage duckdb_broker_store
cargo test -q -p jcode-storage \
  --features duckdb-storage-bundled duckdb_broker_store
DUCKDB_DOWNLOAD_LIB=1 cargo test -q \
  duckdb_graph_backend_writes_through_to_json_fallback \
  --features duckdb-storage
cargo test -q --no-default-features --features duckdb-storage-bundled \
  broker_ingest_vault_writes_normalized_duckdb_store
```

The probe uses a tiny corpus that models the broker/Vault records this project
actually needs:

- hidden transcript provenance memory
- derived broker memory
- `DerivedFrom` edge
- Vault file record
- Vault chunk record
- Vault task record
- note-to-memory `Mentions` edge
- task-to-provenance proof edge
- chunk text and fixed-size vector embedding

## Results

Fresh core run on 2026-05-11 with DuckDB Python `1.5.2`:

| Check | Result | Evidence |
| --- | --- | --- |
| Recursive graph traversal | Pass | Derived memory traversed to hidden transcript provenance through edges. |
| SQL graph fallback | Pass | Pure SQL recursive CTE path query traversed `DerivedFrom` without DuckPGQ. |
| Link-neighborhood recall | Pass | Vault chunk was retrieved as note evidence near derived memory. |
| FTS/BM25 | Pass | `fts` retrieved the focused broker-test chunk. |
| Vector search | Pass | `vss` retrieved nearest fixed-size array embeddings. |
| Single-writer broker service | Pass | Four concurrent client threads serialized 32 writes through one DuckDB-owning writer service. |
| Update/delete reconciliation | Pass | A changed Vault chunk refreshed FTS, and a deleted task was tombstoned and unlinked. |
| Backup/restore | Pass | A DuckDB database file copy was reopened read-only with expected tables intact. |
| Onager graph analytics | Pass | `onager_ctr_pagerank` ran over broker edge tables after loading the DuckDB community extension. |
| Parquet export | Pass | Chunk table exported to Parquet. |
| DuckPGQ property graph | Blocked | DuckDB tried to fetch `duckpgq` for `v1.5.2/osx_arm64`, but the community extension URL returned 404. |

Whole-Vault read-only proof on `/Users/rob/Vault`:

| Check | Result | Evidence |
| --- | --- | --- |
| Inventory | Pass | 450 Markdown files, 95 attachments, 4,889 chunks, 4,783 headings, 1,596 tasks, 1,093 links, and 1,755 tag records. |
| Source metadata | Pass | File records preserve path, checksum, size, and mtime metadata. |
| FTS recall | Pass | Query `jcode broker memory DuckDB graph` returned the jcode plan and related TaskNotes chunks. |
| Link neighborhoods | Pass | Wiki/Markdown links were imported into queryable `vault_link` records. |
| Task extraction | Pass | Obsidian task lines were imported into queryable `vault_task` records. |
| Vault hygiene findings | Review | 5 frontmatter parse issues, 48 broken/local unresolved links, and 0 duplicate case-insensitive paths were detected. |

Durable single-writer proof on a temporary DuckDB file using `/Users/rob/Vault`:

| Check | Result | Evidence |
| --- | --- | --- |
| Durable import | Pass | Single writer imported 450 `vault_file`, 4,889 `vault_chunk`, 1,093 `vault_link`, 1,755 `vault_tag`, 1,596 `vault_task`, and 4,783 `vault_heading` records into a real DuckDB file. |
| Broker context formatting | Pass | FTS hits formatted as `vault_chunk` broker context items with `vault://` source URIs, source checksums, line spans, relevance metadata, and source fragments. |
| Embedding backfill | Pass | 4,889 deterministic local `vault_embedding` records were written and ranked by vector distance. |
| Reconciliation unit proof | Pass | Updated notes refresh active chunk context, while deleted notes are tombstoned with `deleted_at`. |

Rust storage-boundary proof:

| Check | Result | Evidence |
| --- | --- | --- |
| Default JSON boundary | Pass | `jcode-storage` round-trips opaque memory graph records through the existing JSON path without enabling DuckDB. |
| Optional DuckDB boundary | Pass | `jcode-storage` stores and reloads opaque graph records in a real DuckDB file behind the opt-in `duckdb-storage` feature. |
| MemoryManager write-through | Pass | `MemoryManager::with_duckdb_graph_store` can remember through DuckDB, reopen from DuckDB, and read the same graph through JSON fallback. |
| Normalized Rust broker tables | Pass | `jcode-storage` creates normalized `vault_file`, `vault_chunk`, `vault_link`, `vault_task`, and `graph_edge` tables behind the opt-in `duckdb-storage` feature. |
| Rust single-writer service | Pass | Concurrent Rust clients serialize writes through one DuckDB-owning worker service. |
| Rust context/tombstone boundary | Pass | `query_vault_chunks` returns chunk context metadata from normalized tables and hides tombstoned file records from active context. |
| Broker context integration | Pass | With `duckdb-storage` and `JCODE_BROKER_DUCKDB_PATH`, `broker_context.items` can include `vault_chunk`, `vault_task`, and `vault_link` items from normalized DuckDB rows. |
| Rust backup/restore proof | Pass | The normalized broker service checkpoints and copies a DuckDB file, then reopens a restored copy with context rows intact. |
| Rust Vault ingestion/reconciliation writer | Pass | `jcode broker ingest-vault` reconciles a Vault into the normalized DuckDB broker store, with update/delete tombstones and checksum-based rename identity preservation covered by focused tests. A whole-Vault command smoke imported `/Users/rob/Vault` into a temp DuckDB file with 450 files, 4,976 chunks, 1,091 links, 1,648 tasks, and 7,715 graph edges. |
| Rust Vault embedding boundary | Pass | The normalized broker service stores `vault_embedding` records, lists chunks missing current embeddings by model label/checksum, ranks stored vectors by cosine similarity, and deletes stale vectors when file records are replaced. `jcode broker embed-vault` now backfills missing chunks through the jcode embedding facade; a live model-enabled whole-Vault run is still pending. |

## What This Means

DuckDB can represent the broker graph core today using normal relational tables
and recursive CTE traversal. It can also support chunk search with FTS, vector
nearest-neighbor retrieval through VSS, and Parquet export for data science.

Onager strengthens the DuckDB analytics story: it is not a replacement for
broker graph querying, but it can run graph algorithms such as PageRank directly
over DuckDB edge tables. Kuzu remains useful as an adjacent graph engine to watch
or bridge to, especially because it can attach DuckDB data, but the original
Kuzu repo is archived and its DuckDB extension is a Kuzu-side scanner rather
than a DuckDB-native graph store.

The important DuckPGQ finding is that DuckPGQ is now optional for the first
operational path. We can keep graph records in plain DuckDB tables and use
recursive SQL path queries for core broker traversal while DuckPGQ remains a
future ergonomics/performance enhancement.

DuckDB is not yet fully proven as the operational broker store:

- Native DuckDB writes are single-writer-process oriented.
- DuckDB is optimized for analytical and bulk workloads, not many tiny
  cross-process writes, so the broker should own writes through a single-writer
  service.
- FTS indexes do not auto-refresh when source tables change.
- Persistent VSS/HNSW indexes still carry experimental persistence caveats.
- DuckPGQ is not available in this local DuckDB/osx_arm64 proof, so graph
  extension ergonomics remain unproven here.
- The first normalized Rust DuckDB broker store now exists for Vault files,
  chunks, links, tasks, and graph edges. The live broker can read `vault_chunk`,
  `vault_task`, and `vault_link` context from it when `duckdb-storage` is enabled and
  `JCODE_BROKER_DUCKDB_PATH` points at the broker database.
- The Rust CLI production writer can reconcile a Vault into that normalized
  store with `jcode broker ingest-vault --vault ... --db ...`, and `--watch`
  enables polling reconciliation for a long-running broker-side process.
- The Rust store now has checksum-aware `vault_embedding` records, semantic
  query over stored vectors, and a `jcode broker embed-vault` backfill command
  that uses the jcode embedding facade when the embedding stack is available.
- A live model-enabled whole-Vault embedding smoke, native filesystem-event
  watching, real FTS/vector index acceleration, summaries/entities, and JSON
  graph migration are still pending.

## Recommendation

Keep DuckDB in first position. The no-DuckPGQ path plus durable-file service
proof was strong enough to start the Rust storage boundary work, and the first
opt-in Rust boundary now exists. The broker now uses a Rust single-writer service
with normalized Vault/context tables for the proof path; do not make DuckDB the
canonical operational DB until the remaining live-service concerns below are
closed, but stop treating either DuckPGQ availability or lack of a separate graph
server as a blocker.

Next DuckDB proof requirements:

- repeated small writes under broker-like concurrency on a larger corpus
- native filesystem-event watch/reconciliation, if polling is not enough
- index refresh timing for FTS/vector
- rebuild-from-source behavior from the source Vault
- live model-enabled embedding backfill for whole-Vault chunks
- DB-native vector-index acceleration only if benchmarks justify it
- migration of JSON memory graph reads behind the same storage boundary

Keep SurrealDB as the current operational baseline until DuckDB passes those
Rust integration tests. Even if DuckDB does not become the operational graph
store, keep DuckDB/Parquet as a first-class analytical mirror.
