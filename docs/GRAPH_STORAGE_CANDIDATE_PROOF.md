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

Unit test:

```bash
python3 -m unittest tests/storage_candidate/test_duckdb_candidate_probe.py
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
| Parquet export | Pass | Chunk table exported to Parquet. |
| DuckPGQ property graph | Blocked | DuckDB tried to fetch `duckpgq` for `v1.5.2/osx_arm64`, but the community extension URL returned 404. |

Whole-Vault read-only proof on `/Users/rob/Vault`:

| Check | Result | Evidence |
| --- | --- | --- |
| Inventory | Pass | 450 Markdown files, 95 attachments, 4,889 chunks, 4,783 headings, 1,560 tasks, 1,093 links, and 1,755 tag records. |
| Source metadata | Pass | File records preserve path, checksum, size, and mtime metadata. |
| FTS recall | Pass | Query `jcode broker memory DuckDB graph` returned the jcode plan and related TaskNotes chunks. |
| Link neighborhoods | Pass | Wiki/Markdown links were imported into queryable `vault_link` records. |
| Task extraction | Pass | Obsidian task lines were imported into queryable `vault_task` records. |
| Vault hygiene findings | Review | 5 frontmatter parse issues, 48 broken/local unresolved links, and 0 duplicate case-insensitive paths were detected. |

## What This Means

DuckDB can represent the broker graph core today using normal relational tables
and recursive CTE traversal. It can also support chunk search with FTS, vector
nearest-neighbor retrieval through VSS, and Parquet export for data science.

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
- The whole-Vault proof is read-only and in-memory; durable service integration,
  incremental watches, and vector embedding backfill still need proof.

## Recommendation

Keep DuckDB in first position. The no-DuckPGQ path is viable enough to justify a
whole-Vault operational proof behind a single-writer broker service. Do not make
it the canonical operational DB yet, but stop treating DuckPGQ availability as a
blocker.

Next DuckDB proof requirements:

- durable single-writer broker service module/API, not only this proof harness
- repeated small writes under broker-like concurrency on a larger corpus
- incremental update/delete reconciliation for Vault chunks
- index refresh timing for FTS/vector
- backup/restore and rebuild-from-source behavior
- vector embedding backfill for whole-Vault chunks
- broker context formatting from `vault_chunk`, `vault_link`, and `vault_task`
  records without prompt bloat

Keep SurrealDB as the current operational baseline until DuckDB passes those
write, restore, and whole-Vault tests. Even if DuckDB does not become the
operational graph store, keep DuckDB/Parquet as a first-class analytical mirror.
