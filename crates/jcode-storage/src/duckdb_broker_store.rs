use anyhow::{Context, Result, anyhow};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultFileRecord {
    pub id: String,
    pub path: String,
    pub title: String,
    pub checksum: String,
    pub size_bytes: i64,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultChunkRecord {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub heading: String,
    pub content: String,
    pub start_line: i64,
    pub end_line: i64,
    pub checksum: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultLinkRecord {
    pub id: String,
    pub source_file_id: String,
    pub source_path: String,
    pub target: String,
    pub kind: String,
    pub raw: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultTaskRecord {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub checked: bool,
    pub content: String,
    pub line: i64,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultSummaryRecord {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub summary: String,
    pub checksum: String,
    pub source_checksum: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultEntityRecord {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub name: String,
    pub kind: String,
    pub source: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphEdgeRecord {
    pub id: String,
    pub source_id: String,
    pub target_id: String,
    pub kind: String,
    pub weight: f64,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultEmbeddingRecord {
    pub id: String,
    pub record_id: String,
    pub record_kind: String,
    pub embedding_model: String,
    pub embedding: Vec<f32>,
    pub content_checksum: String,
    pub source_checksum: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct VaultRecordBatch {
    pub files: Vec<VaultFileRecord>,
    pub chunks: Vec<VaultChunkRecord>,
    pub links: Vec<VaultLinkRecord>,
    pub tasks: Vec<VaultTaskRecord>,
    pub summaries: Vec<VaultSummaryRecord>,
    pub entities: Vec<VaultEntityRecord>,
    pub edges: Vec<GraphEdgeRecord>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BrokerStoreCounts {
    pub vault_file: i64,
    pub active_vault_file: i64,
    pub vault_chunk: i64,
    pub active_vault_chunk: i64,
    pub vault_link: i64,
    pub active_vault_link: i64,
    pub vault_task: i64,
    pub active_vault_task: i64,
    pub vault_summary: i64,
    pub active_vault_summary: i64,
    pub vault_entity: i64,
    pub active_vault_entity: i64,
    pub vault_embedding: i64,
    pub active_vault_embedding: i64,
    pub graph_edge: i64,
    pub active_graph_edge: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultChunkContextRow {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub heading: String,
    pub content: String,
    pub start_line: i64,
    pub end_line: i64,
    pub checksum: String,
    pub source_checksum: String,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub score: f64,
    pub matched_terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultTaskContextRow {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub checked: bool,
    pub content: String,
    pub line: i64,
    pub source_checksum: String,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub score: f64,
    pub matched_terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultLinkContextRow {
    pub id: String,
    pub source_file_id: String,
    pub source_path: String,
    pub title: String,
    pub target: String,
    pub kind: String,
    pub raw: String,
    pub source_checksum: String,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub score: f64,
    pub matched_terms: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultRelationshipContextRow {
    pub id: String,
    pub relationship: String,
    pub source_file_id: String,
    pub source_path: String,
    pub source_title: String,
    pub target: String,
    pub target_path: Option<String>,
    pub target_title: Option<String>,
    pub link_kind: Option<String>,
    pub raw: Option<String>,
    pub source_checksum: String,
    pub target_checksum: Option<String>,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub score: f64,
    pub retrieval_mode: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultChunkEmbeddingCandidate {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub heading: String,
    pub content: String,
    pub start_line: i64,
    pub end_line: i64,
    pub checksum: String,
    pub source_checksum: String,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VaultChunkEmbeddingHit {
    pub id: String,
    pub file_id: String,
    pub path: String,
    pub title: String,
    pub heading: String,
    pub content: String,
    pub start_line: i64,
    pub end_line: i64,
    pub checksum: String,
    pub source_checksum: String,
    pub mtime_ns: i64,
    pub frontmatter_json: String,
    pub embedding_model: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultDuplicateEntityCandidate {
    pub kind: String,
    pub name: String,
    pub active_file_count: i64,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultStaleSummaryCandidate {
    pub path: String,
    pub active_summary_count: i64,
}

pub struct DuckDbBrokerStore {
    db_path: PathBuf,
    connection: duckdb::Connection,
}

#[derive(Debug, Clone)]
struct ActiveVaultFile {
    id: String,
    path: String,
    title: String,
    checksum: String,
    mtime_ns: i64,
    frontmatter_json: String,
}

#[derive(Debug, Clone)]
struct ActiveVaultLink {
    id: String,
    source_file_id: String,
    source_path: String,
    target: String,
    kind: String,
    raw: String,
}

impl DuckDbBrokerStore {
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().expand_homeish();
        if let Some(parent) = db_path.parent() {
            crate::ensure_dir(parent)?;
        }
        let connection = duckdb::Connection::open(&db_path)?;
        let store = Self {
            db_path,
            connection,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn replace_vault_records(&mut self, batch: VaultRecordBatch) -> Result<()> {
        self.connection.execute_batch(
            r#"
            DELETE FROM vault_embedding;
            DELETE FROM graph_edge;
            DELETE FROM vault_entity;
            DELETE FROM vault_summary;
            DELETE FROM vault_task;
            DELETE FROM vault_link;
            DELETE FROM vault_chunk;
            DELETE FROM vault_file;
            "#,
        )?;
        self.upsert_vault_records(batch)
    }

    pub fn upsert_vault_records(&mut self, batch: VaultRecordBatch) -> Result<()> {
        for record in batch.files {
            self.upsert_vault_file(&record)?;
        }
        for record in batch.chunks {
            self.upsert_vault_chunk(&record)?;
        }
        for record in batch.links {
            self.upsert_vault_link(&record)?;
        }
        for record in batch.tasks {
            self.upsert_vault_task(&record)?;
        }
        for record in batch.summaries {
            self.upsert_vault_summary(&record)?;
        }
        for record in batch.entities {
            self.upsert_vault_entity(&record)?;
        }
        for record in batch.edges {
            self.upsert_graph_edge(&record)?;
        }
        Ok(())
    }

    pub fn upsert_vault_embeddings(&mut self, records: Vec<VaultEmbeddingRecord>) -> Result<()> {
        for record in records {
            self.upsert_vault_embedding(&record)?;
        }
        Ok(())
    }

    pub fn replace_file_records(&mut self, file_id: &str, batch: VaultRecordBatch) -> Result<()> {
        self.delete_file_records(file_id)?;
        self.upsert_vault_records(batch)
    }

    pub fn list_vault_files(&self) -> Result<Vec<VaultFileRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT id, path, title, checksum, size_bytes, mtime_ns, frontmatter_json, deleted_at
            FROM vault_file
            ORDER BY path
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut files = Vec::new();
        while let Some(row) = rows.next()? {
            files.push(VaultFileRecord {
                id: row.get(0)?,
                path: row.get(1)?,
                title: row.get(2)?,
                checksum: row.get(3)?,
                size_bytes: row.get(4)?,
                mtime_ns: row.get(5)?,
                frontmatter_json: row.get(6)?,
                deleted_at: row.get(7)?,
            });
        }
        Ok(files)
    }

    pub fn tombstone_file_records(&mut self, file_id: &str, deleted_at: &str) -> Result<()> {
        self.connection.execute(
            r#"
            UPDATE vault_embedding
            SET deleted_at = ?
            WHERE deleted_at IS NULL
              AND record_kind = 'vault_chunk'
              AND record_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
            "#,
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            r#"
            UPDATE graph_edge
            SET deleted_at = ?
            WHERE deleted_at IS NULL
              AND (
                source_id = ?
                OR target_id = ?
                OR source_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
                OR target_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
                OR source_id IN (SELECT id FROM vault_task WHERE file_id = ?)
                OR target_id IN (SELECT id FROM vault_task WHERE file_id = ?)
                OR source_id IN (SELECT id FROM vault_summary WHERE file_id = ?)
                OR target_id IN (SELECT id FROM vault_summary WHERE file_id = ?)
                OR source_id IN (SELECT id FROM vault_entity WHERE file_id = ?)
                OR target_id IN (SELECT id FROM vault_entity WHERE file_id = ?)
              )
            "#,
            duckdb::params![
                deleted_at, file_id, file_id, file_id, file_id, file_id, file_id, file_id, file_id,
                file_id, file_id
            ],
        )?;
        self.connection.execute(
            "UPDATE vault_task SET deleted_at = ? WHERE file_id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            "UPDATE vault_summary SET deleted_at = ? WHERE file_id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            "UPDATE vault_entity SET deleted_at = ? WHERE file_id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            "UPDATE vault_link SET deleted_at = ? WHERE source_file_id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            "UPDATE vault_chunk SET deleted_at = ? WHERE file_id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        self.connection.execute(
            "UPDATE vault_file SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL",
            duckdb::params![deleted_at, file_id],
        )?;
        Ok(())
    }

    fn delete_file_records(&mut self, file_id: &str) -> Result<()> {
        self.connection.execute(
            r#"
            DELETE FROM vault_embedding
            WHERE record_kind = 'vault_chunk'
              AND record_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
            "#,
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            r#"
            DELETE FROM graph_edge
            WHERE source_id = ?
               OR target_id = ?
               OR source_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
               OR target_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
               OR source_id IN (SELECT id FROM vault_task WHERE file_id = ?)
               OR target_id IN (SELECT id FROM vault_task WHERE file_id = ?)
               OR source_id IN (SELECT id FROM vault_link WHERE source_file_id = ?)
               OR target_id IN (SELECT id FROM vault_link WHERE source_file_id = ?)
               OR source_id IN (SELECT id FROM vault_summary WHERE file_id = ?)
               OR target_id IN (SELECT id FROM vault_summary WHERE file_id = ?)
               OR source_id IN (SELECT id FROM vault_entity WHERE file_id = ?)
               OR target_id IN (SELECT id FROM vault_entity WHERE file_id = ?)
            "#,
            duckdb::params![
                file_id, file_id, file_id, file_id, file_id, file_id, file_id, file_id, file_id,
                file_id, file_id, file_id
            ],
        )?;
        self.connection.execute(
            "DELETE FROM vault_task WHERE file_id = ?",
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            "DELETE FROM vault_summary WHERE file_id = ?",
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            "DELETE FROM vault_entity WHERE file_id = ?",
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            "DELETE FROM vault_link WHERE source_file_id = ?",
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            "DELETE FROM vault_chunk WHERE file_id = ?",
            duckdb::params![file_id],
        )?;
        self.connection.execute(
            "DELETE FROM vault_file WHERE id = ?",
            duckdb::params![file_id],
        )?;
        Ok(())
    }

    pub fn table_counts(&self) -> Result<BrokerStoreCounts> {
        Ok(BrokerStoreCounts {
            vault_file: self.count_table("vault_file", "")?,
            active_vault_file: self.count_table("vault_file", "WHERE deleted_at IS NULL")?,
            vault_chunk: self.count_table("vault_chunk", "")?,
            active_vault_chunk: self.count_table("vault_chunk", "WHERE deleted_at IS NULL")?,
            vault_link: self.count_table("vault_link", "")?,
            active_vault_link: self.count_table("vault_link", "WHERE deleted_at IS NULL")?,
            vault_task: self.count_table("vault_task", "")?,
            active_vault_task: self.count_table("vault_task", "WHERE deleted_at IS NULL")?,
            vault_summary: self.count_table("vault_summary", "")?,
            active_vault_summary: self.count_table("vault_summary", "WHERE deleted_at IS NULL")?,
            vault_entity: self.count_table("vault_entity", "")?,
            active_vault_entity: self.count_table("vault_entity", "WHERE deleted_at IS NULL")?,
            vault_embedding: self.count_table("vault_embedding", "")?,
            active_vault_embedding: self
                .count_table("vault_embedding", "WHERE deleted_at IS NULL")?,
            graph_edge: self.count_table("graph_edge", "")?,
            active_graph_edge: self.count_table("graph_edge", "WHERE deleted_at IS NULL")?,
        })
    }

    pub fn list_missing_vault_chunk_embeddings(
        &self,
        embedding_model: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                c.id,
                c.file_id,
                c.path,
                f.title,
                c.heading,
                c.content,
                c.start_line,
                c.end_line,
                c.checksum,
                f.checksum,
                f.mtime_ns
            FROM vault_chunk c
            JOIN vault_file f ON f.id = c.file_id
            LEFT JOIN vault_embedding e
                ON e.record_id = c.id
               AND e.record_kind = 'vault_chunk'
               AND e.embedding_model = ?
               AND e.content_checksum = c.checksum
               AND e.source_checksum = f.checksum
               AND e.deleted_at IS NULL
            WHERE c.deleted_at IS NULL
              AND f.deleted_at IS NULL
              AND e.id IS NULL
            ORDER BY c.path, c.start_line, c.id
            LIMIT ?
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![embedding_model, limit as i64])?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next()? {
            candidates.push(VaultChunkEmbeddingCandidate {
                id: row.get(0)?,
                file_id: row.get(1)?,
                path: row.get(2)?,
                title: row.get(3)?,
                heading: row.get(4)?,
                content: row.get(5)?,
                start_line: row.get(6)?,
                end_line: row.get(7)?,
                checksum: row.get(8)?,
                source_checksum: row.get(9)?,
                mtime_ns: row.get(10)?,
            });
        }
        Ok(candidates)
    }

    pub fn count_missing_vault_chunk_embeddings(&self, embedding_model: &str) -> Result<i64> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT count(*)
            FROM vault_chunk c
            JOIN vault_file f ON f.id = c.file_id
            LEFT JOIN vault_embedding e
                ON e.record_id = c.id
               AND e.record_kind = 'vault_chunk'
               AND e.embedding_model = ?
               AND e.content_checksum = c.checksum
               AND e.source_checksum = f.checksum
               AND e.deleted_at IS NULL
            WHERE c.deleted_at IS NULL
              AND f.deleted_at IS NULL
              AND e.id IS NULL
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![embedding_model])?;
        let Some(row) = rows.next()? else {
            return Err(anyhow!("missing embedding count query returned no rows"));
        };
        row.get(0).map_err(Into::into)
    }

    pub fn list_duplicate_vault_entities(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultDuplicateEntityCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                e.kind,
                e.name,
                count(DISTINCT e.file_id) AS active_file_count,
                string_agg(DISTINCT e.path, chr(10) ORDER BY e.path) AS paths
            FROM vault_entity e
            JOIN vault_file f ON f.id = e.file_id
            WHERE e.deleted_at IS NULL
              AND f.deleted_at IS NULL
            GROUP BY e.kind, e.name
            HAVING count(DISTINCT e.file_id) > 1
            ORDER BY active_file_count DESC, e.kind, e.name
            LIMIT ?
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![limit as i64])?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next()? {
            let paths: String = row.get(3)?;
            candidates.push(VaultDuplicateEntityCandidate {
                kind: row.get(0)?,
                name: row.get(1)?,
                active_file_count: row.get(2)?,
                paths: paths
                    .lines()
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(ToOwned::to_owned)
                    .collect(),
            });
        }
        Ok(candidates)
    }

    pub fn count_stale_vault_summaries(&self) -> Result<i64> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT count(*)
            FROM vault_summary s
            JOIN vault_file f ON f.id = s.file_id
            WHERE s.deleted_at IS NULL
              AND f.deleted_at IS NULL
              AND s.source_checksum <> f.checksum
            "#,
        )?;
        let mut rows = statement.query([])?;
        let Some(row) = rows.next()? else {
            return Err(anyhow!("stale summary count query returned no rows"));
        };
        row.get(0).map_err(Into::into)
    }

    pub fn list_stale_vault_summaries(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultStaleSummaryCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT s.path, count(*) AS active_summary_count
            FROM vault_summary s
            JOIN vault_file f ON f.id = s.file_id
            WHERE s.deleted_at IS NULL
              AND f.deleted_at IS NULL
              AND s.source_checksum <> f.checksum
            GROUP BY s.path
            ORDER BY active_summary_count DESC, s.path
            LIMIT ?
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![limit as i64])?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next()? {
            candidates.push(VaultStaleSummaryCandidate {
                path: row.get(0)?,
                active_summary_count: row.get(1)?,
            });
        }
        Ok(candidates)
    }

    pub fn consolidate_duplicate_vault_entities(&mut self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                e.kind,
                e.name,
                string_agg(e.id, chr(10) ORDER BY e.path, e.id) AS entity_ids
            FROM vault_entity e
            JOIN vault_file f ON f.id = e.file_id
            WHERE e.deleted_at IS NULL
              AND f.deleted_at IS NULL
            GROUP BY e.kind, e.name
            HAVING count(DISTINCT e.file_id) > 1
            ORDER BY count(DISTINCT e.file_id) DESC, e.kind, e.name
            LIMIT ?
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![limit as i64])?;
        let mut edges = Vec::new();
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let name: String = row.get(1)?;
            let entity_ids: String = row.get(2)?;
            let ids: Vec<String> = entity_ids
                .lines()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            let Some(canonical_id) = ids.first() else {
                continue;
            };
            for duplicate_id in ids.iter().skip(1) {
                edges.push(GraphEdgeRecord {
                    id: format!("graph_edge:duplicate_entity:{duplicate_id}:{canonical_id}"),
                    source_id: duplicate_id.clone(),
                    target_id: canonical_id.clone(),
                    kind: format!("DuplicateEntityOf:{kind}:{name}"),
                    weight: 0.75,
                    deleted_at: None,
                });
            }
        }
        drop(rows);
        drop(statement);

        let edge_count = edges.len();
        for edge in edges {
            self.upsert_graph_edge(&edge)?;
        }
        Ok(edge_count)
    }

    pub fn purge_tombstoned_vault_records(&mut self, older_than_deleted_at: &str) -> Result<usize> {
        let mut deleted = 0usize;
        for table in [
            "vault_embedding",
            "graph_edge",
            "vault_task",
            "vault_summary",
            "vault_entity",
            "vault_link",
            "vault_chunk",
            "vault_file",
        ] {
            deleted += self.connection.execute(
                &format!("DELETE FROM {table} WHERE deleted_at IS NOT NULL AND deleted_at <= ?"),
                duckdb::params![older_than_deleted_at],
            )?;
        }
        Ok(deleted)
    }

    pub fn query_vault_chunks_by_embedding(
        &self,
        embedding_model: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingHit>> {
        if limit == 0 || query_embedding.is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                c.id,
                c.file_id,
                c.path,
                f.title,
                c.heading,
                c.content,
                c.start_line,
                c.end_line,
                c.checksum,
                f.checksum,
                f.mtime_ns,
                f.frontmatter_json,
                e.embedding_model,
                e.embedding_json
            FROM vault_embedding e
            JOIN vault_chunk c ON c.id = e.record_id
            JOIN vault_file f ON f.id = c.file_id
            WHERE e.record_kind = 'vault_chunk'
              AND e.embedding_model = ?
              AND e.embedding_dim = ?
              AND e.content_checksum = c.checksum
              AND e.source_checksum = f.checksum
              AND e.deleted_at IS NULL
              AND c.deleted_at IS NULL
              AND f.deleted_at IS NULL
            ORDER BY c.path, c.start_line, c.id
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![
            embedding_model,
            i64::try_from(query_embedding.len()).unwrap_or(i64::MAX)
        ])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let embedding_json: String = row.get(13)?;
            let embedding = parse_embedding_json(&embedding_json)
                .with_context(|| format!("invalid stored embedding for row {id}"))?;
            let score = cosine_similarity(query_embedding, &embedding);
            hits.push(VaultChunkEmbeddingHit {
                id,
                file_id: row.get(1)?,
                path: row.get(2)?,
                title: row.get(3)?,
                heading: row.get(4)?,
                content: row.get(5)?,
                start_line: row.get(6)?,
                end_line: row.get(7)?,
                checksum: row.get(8)?,
                source_checksum: row.get(9)?,
                mtime_ns: row.get(10)?,
                frontmatter_json: row.get(11)?,
                embedding_model: row.get(12)?,
                score,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.start_line.cmp(&right.start_line))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn query_vault_chunks(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkContextRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let terms = query_terms(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                c.id,
                c.file_id,
                c.path,
                f.title,
                c.heading,
                c.content,
                c.start_line,
                c.end_line,
                c.checksum,
                f.checksum,
                f.mtime_ns,
                f.frontmatter_json
            FROM vault_chunk c
            JOIN vault_file f ON f.id = c.file_id
            WHERE c.deleted_at IS NULL
              AND f.deleted_at IS NULL
            ORDER BY c.path, c.start_line
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let title: String = row.get(3)?;
            let heading: String = row.get(4)?;
            let content: String = row.get(5)?;
            let path: String = row.get(2)?;
            let Some((score, matched_terms)) =
                score_vault_chunk_hit(query, &terms, &path, &title, &heading, &content)
            else {
                continue;
            };
            hits.push(VaultChunkContextRow {
                id: row.get(0)?,
                file_id: row.get(1)?,
                path,
                title,
                heading,
                content,
                start_line: row.get(6)?,
                end_line: row.get(7)?,
                checksum: row.get(8)?,
                source_checksum: row.get(9)?,
                mtime_ns: row.get(10)?,
                frontmatter_json: row.get(11)?,
                score,
                matched_terms,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.start_line.cmp(&right.start_line))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn query_vault_tasks(&self, query: &str, limit: usize) -> Result<Vec<VaultTaskContextRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let terms = query_terms(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                t.id,
                t.file_id,
                t.path,
                f.title,
                t.checked,
                t.content,
                t.line,
                f.checksum,
                f.mtime_ns,
                f.frontmatter_json
            FROM vault_task t
            JOIN vault_file f ON f.id = t.file_id
            WHERE t.deleted_at IS NULL
              AND f.deleted_at IS NULL
            ORDER BY t.path, t.line
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let path: String = row.get(2)?;
            let title: String = row.get(3)?;
            let content: String = row.get(5)?;
            let haystack = format!("{path}\n{title}\n{content}");
            let Some((score, matched_terms)) = score_query_hit(query, &terms, &haystack) else {
                continue;
            };
            hits.push(VaultTaskContextRow {
                id: row.get(0)?,
                file_id: row.get(1)?,
                path,
                title,
                checked: row.get(4)?,
                content,
                line: row.get(6)?,
                source_checksum: row.get(7)?,
                mtime_ns: row.get(8)?,
                frontmatter_json: row.get(9)?,
                score,
                matched_terms,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.line.cmp(&right.line))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn query_vault_links(&self, query: &str, limit: usize) -> Result<Vec<VaultLinkContextRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let terms = query_terms(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            r#"
            SELECT
                l.id,
                l.source_file_id,
                l.source_path,
                f.title,
                l.target,
                l.kind,
                l.raw,
                f.checksum,
                f.mtime_ns,
                f.frontmatter_json
            FROM vault_link l
            JOIN vault_file f ON f.id = l.source_file_id
            WHERE l.deleted_at IS NULL
              AND f.deleted_at IS NULL
            ORDER BY l.source_path, l.target, l.kind
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let source_path: String = row.get(2)?;
            let title: String = row.get(3)?;
            let target: String = row.get(4)?;
            let kind: String = row.get(5)?;
            let raw: String = row.get(6)?;
            let haystack = format!("{source_path}\n{title}\n{target}\n{kind}\n{raw}");
            let Some((score, matched_terms)) = score_query_hit(query, &terms, &haystack) else {
                continue;
            };
            hits.push(VaultLinkContextRow {
                id: row.get(0)?,
                source_file_id: row.get(1)?,
                source_path,
                title,
                target,
                kind,
                raw,
                source_checksum: row.get(7)?,
                mtime_ns: row.get(8)?,
                frontmatter_json: row.get(9)?,
                score,
                matched_terms,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_path.cmp(&right.source_path))
                .then_with(|| left.target.cmp(&right.target))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    pub fn query_vault_relationships_for_path(
        &self,
        requested_path: &str,
        limit: usize,
    ) -> Result<Vec<VaultRelationshipContextRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let requested_path = requested_path.trim();
        if requested_path.is_empty() {
            return Ok(Vec::new());
        }

        let files = self.active_vault_files_for_relationships()?;
        let Some(target_file) = files
            .iter()
            .find(|file| vault_path_matches(&file.path, requested_path))
            .or_else(|| {
                let requested_stem = vault_path_stem(requested_path);
                files.iter().find(|file| {
                    !requested_stem.is_empty()
                        && vault_path_stem(&file.path).eq_ignore_ascii_case(&requested_stem)
                })
            })
        else {
            return Ok(Vec::new());
        };
        let links = self.active_vault_links_for_relationships()?;
        let mut rows = Vec::new();

        for link in links
            .iter()
            .filter(|link| link.source_path == target_file.path)
        {
            let linked_file = resolve_vault_link_target(&link.target, &link.source_path, &files);
            rows.push(VaultRelationshipContextRow {
                id: format!("vault_relationship:outlink:{}", link.id),
                relationship: "outlink".to_string(),
                source_file_id: link.source_file_id.clone(),
                source_path: link.source_path.clone(),
                source_title: target_file.title.clone(),
                target: link.target.clone(),
                target_path: linked_file.map(|file| file.path.clone()),
                target_title: linked_file.map(|file| file.title.clone()),
                link_kind: Some(link.kind.clone()),
                raw: Some(link.raw.clone()),
                source_checksum: target_file.checksum.clone(),
                target_checksum: linked_file.map(|file| file.checksum.clone()),
                mtime_ns: target_file.mtime_ns,
                frontmatter_json: target_file.frontmatter_json.clone(),
                score: 100.0,
                retrieval_mode: "duckdb_broker_store_relationship".to_string(),
            });
        }

        for link in links
            .iter()
            .filter(|link| link.source_path != target_file.path)
            .filter(|link| {
                resolve_vault_link_target(&link.target, &link.source_path, &files)
                    .map(|file| file.path == target_file.path)
                    .unwrap_or(false)
            })
        {
            let Some(source_file) = files.iter().find(|file| file.path == link.source_path) else {
                continue;
            };
            rows.push(VaultRelationshipContextRow {
                id: format!("vault_relationship:backlink:{}", link.id),
                relationship: "backlink".to_string(),
                source_file_id: link.source_file_id.clone(),
                source_path: link.source_path.clone(),
                source_title: source_file.title.clone(),
                target: link.target.clone(),
                target_path: Some(target_file.path.clone()),
                target_title: Some(target_file.title.clone()),
                link_kind: Some(link.kind.clone()),
                raw: Some(link.raw.clone()),
                source_checksum: source_file.checksum.clone(),
                target_checksum: Some(target_file.checksum.clone()),
                mtime_ns: source_file.mtime_ns,
                frontmatter_json: source_file.frontmatter_json.clone(),
                score: 95.0,
                retrieval_mode: "duckdb_broker_store_relationship".to_string(),
            });
        }

        let parent = vault_parent_path(&target_file.path);
        for file in files
            .iter()
            .filter(|file| file.path != target_file.path)
            .filter(|file| vault_parent_path(&file.path) == parent)
        {
            rows.push(VaultRelationshipContextRow {
                id: format!(
                    "vault_relationship:folder_neighbor:{}:{}",
                    target_file.id, file.id
                ),
                relationship: "folder_neighbor".to_string(),
                source_file_id: target_file.id.clone(),
                source_path: target_file.path.clone(),
                source_title: target_file.title.clone(),
                target: file.path.clone(),
                target_path: Some(file.path.clone()),
                target_title: Some(file.title.clone()),
                link_kind: None,
                raw: None,
                source_checksum: target_file.checksum.clone(),
                target_checksum: Some(file.checksum.clone()),
                mtime_ns: file.mtime_ns,
                frontmatter_json: target_file.frontmatter_json.clone(),
                score: 25.0,
                retrieval_mode: "duckdb_broker_store_relationship".to_string(),
            });
        }

        rows.sort_by(|left, right| {
            relationship_priority(&left.relationship)
                .cmp(&relationship_priority(&right.relationship))
                .then_with(|| {
                    right
                        .score
                        .partial_cmp(&left.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.source_path.cmp(&right.source_path))
                .then_with(|| left.target_path.cmp(&right.target_path))
        });
        rows.truncate(limit);
        Ok(rows)
    }

    fn active_vault_files_for_relationships(&self) -> Result<Vec<ActiveVaultFile>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT id, path, title, checksum, mtime_ns, frontmatter_json
            FROM vault_file
            WHERE deleted_at IS NULL
            ORDER BY path
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut files = Vec::new();
        while let Some(row) = rows.next()? {
            files.push(ActiveVaultFile {
                id: row.get(0)?,
                path: row.get(1)?,
                title: row.get(2)?,
                checksum: row.get(3)?,
                mtime_ns: row.get(4)?,
                frontmatter_json: row.get(5)?,
            });
        }
        Ok(files)
    }

    fn active_vault_links_for_relationships(&self) -> Result<Vec<ActiveVaultLink>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT id, source_file_id, source_path, target, kind, raw
            FROM vault_link
            WHERE deleted_at IS NULL
            ORDER BY source_path, target, kind
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut links = Vec::new();
        while let Some(row) = rows.next()? {
            links.push(ActiveVaultLink {
                id: row.get(0)?,
                source_file_id: row.get(1)?,
                source_path: row.get(2)?,
                target: row.get(3)?,
                kind: row.get(4)?,
                raw: row.get(5)?,
            });
        }
        Ok(links)
    }

    pub fn backup_to(&self, backup_path: impl AsRef<Path>) -> Result<PathBuf> {
        let backup_path = backup_path.as_ref().expand_homeish();
        if let Some(parent) = backup_path.parent() {
            crate::ensure_dir(parent)?;
        }
        self.connection.execute_batch("CHECKPOINT;")?;
        std::fs::copy(&self.db_path, &backup_path).with_context(|| {
            format!(
                "failed to copy DuckDB broker store backup from {} to {}",
                self.db_path.display(),
                backup_path.display()
            )
        })?;
        Ok(backup_path)
    }

    fn initialize_schema(&self) -> Result<()> {
        self.connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS vault_file (
                id VARCHAR PRIMARY KEY,
                path VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                checksum VARCHAR NOT NULL,
                size_bytes BIGINT NOT NULL,
                mtime_ns BIGINT NOT NULL,
                frontmatter_json VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_chunk (
                id VARCHAR PRIMARY KEY,
                file_id VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                heading VARCHAR NOT NULL,
                content VARCHAR NOT NULL,
                start_line BIGINT NOT NULL,
                end_line BIGINT NOT NULL,
                checksum VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_link (
                id VARCHAR PRIMARY KEY,
                source_file_id VARCHAR NOT NULL,
                source_path VARCHAR NOT NULL,
                target VARCHAR NOT NULL,
                kind VARCHAR NOT NULL,
                raw VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_task (
                id VARCHAR PRIMARY KEY,
                file_id VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                checked BOOLEAN NOT NULL,
                content VARCHAR NOT NULL,
                line BIGINT NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_summary (
                id VARCHAR PRIMARY KEY,
                file_id VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                summary VARCHAR NOT NULL,
                checksum VARCHAR NOT NULL,
                source_checksum VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_entity (
                id VARCHAR PRIMARY KEY,
                file_id VARCHAR NOT NULL,
                path VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                kind VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS vault_embedding (
                id VARCHAR PRIMARY KEY,
                record_id VARCHAR NOT NULL,
                record_kind VARCHAR NOT NULL,
                embedding_model VARCHAR NOT NULL,
                embedding_dim BIGINT NOT NULL,
                embedding_json VARCHAR NOT NULL,
                content_checksum VARCHAR NOT NULL,
                source_checksum VARCHAR NOT NULL,
                updated_at VARCHAR NOT NULL,
                deleted_at VARCHAR
            );

            CREATE TABLE IF NOT EXISTS graph_edge (
                id VARCHAR PRIMARY KEY,
                source_id VARCHAR NOT NULL,
                target_id VARCHAR NOT NULL,
                kind VARCHAR NOT NULL,
                weight DOUBLE NOT NULL,
                deleted_at VARCHAR
            );
            "#,
        )?;
        self.ensure_vault_embedding_columns()?;
        Ok(())
    }

    fn upsert_vault_file(&self, record: &VaultFileRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_file
                (id, path, title, checksum, size_bytes, mtime_ns, frontmatter_json, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                path = excluded.path,
                title = excluded.title,
                checksum = excluded.checksum,
                size_bytes = excluded.size_bytes,
                mtime_ns = excluded.mtime_ns,
                frontmatter_json = excluded.frontmatter_json,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.path,
                record.title,
                record.checksum,
                record.size_bytes,
                record.mtime_ns,
                record.frontmatter_json,
                record.deleted_at.as_deref()
            ],
        )?;
        self.connection.execute(
            r#"
            DELETE FROM vault_embedding
            WHERE record_kind = 'vault_chunk'
              AND source_checksum <> ?
              AND record_id IN (SELECT id FROM vault_chunk WHERE file_id = ?)
            "#,
            duckdb::params![record.checksum, record.id],
        )?;
        Ok(())
    }

    fn upsert_vault_chunk(&self, record: &VaultChunkRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_chunk
                (id, file_id, path, heading, content, start_line, end_line, checksum, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                file_id = excluded.file_id,
                path = excluded.path,
                heading = excluded.heading,
                content = excluded.content,
                start_line = excluded.start_line,
                end_line = excluded.end_line,
                checksum = excluded.checksum,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.file_id,
                record.path,
                record.heading,
                record.content,
                record.start_line,
                record.end_line,
                record.checksum,
                record.deleted_at.as_deref()
            ],
        )?;
        if record.deleted_at.is_some() {
            self.connection.execute(
                "DELETE FROM vault_embedding WHERE record_kind = 'vault_chunk' AND record_id = ?",
                duckdb::params![record.id],
            )?;
        } else {
            self.connection.execute(
                r#"
                DELETE FROM vault_embedding
                WHERE record_kind = 'vault_chunk'
                  AND record_id = ?
                  AND content_checksum <> ?
                "#,
                duckdb::params![record.id, record.checksum],
            )?;
        }
        Ok(())
    }

    fn upsert_vault_link(&self, record: &VaultLinkRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_link
                (id, source_file_id, source_path, target, kind, raw, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                source_file_id = excluded.source_file_id,
                source_path = excluded.source_path,
                target = excluded.target,
                kind = excluded.kind,
                raw = excluded.raw,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.source_file_id,
                record.source_path,
                record.target,
                record.kind,
                record.raw,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn upsert_vault_task(&self, record: &VaultTaskRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_task
                (id, file_id, path, checked, content, line, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                file_id = excluded.file_id,
                path = excluded.path,
                checked = excluded.checked,
                content = excluded.content,
                line = excluded.line,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.file_id,
                record.path,
                record.checked,
                record.content,
                record.line,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn upsert_vault_summary(&self, record: &VaultSummaryRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_summary
                (id, file_id, path, summary, checksum, source_checksum, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                file_id = excluded.file_id,
                path = excluded.path,
                summary = excluded.summary,
                checksum = excluded.checksum,
                source_checksum = excluded.source_checksum,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.file_id,
                record.path,
                record.summary,
                record.checksum,
                record.source_checksum,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn upsert_vault_entity(&self, record: &VaultEntityRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO vault_entity
                (id, file_id, path, name, kind, source, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                file_id = excluded.file_id,
                path = excluded.path,
                name = excluded.name,
                kind = excluded.kind,
                source = excluded.source,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.file_id,
                record.path,
                record.name,
                record.kind,
                record.source,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn upsert_graph_edge(&self, record: &GraphEdgeRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO graph_edge
                (id, source_id, target_id, kind, weight, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                source_id = excluded.source_id,
                target_id = excluded.target_id,
                kind = excluded.kind,
                weight = excluded.weight,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.source_id,
                record.target_id,
                record.kind,
                record.weight,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn upsert_vault_embedding(&self, record: &VaultEmbeddingRecord) -> Result<()> {
        if record.embedding.is_empty() {
            return Err(anyhow!("vault embedding {} cannot be empty", record.id));
        }
        let embedding_dim = i64::try_from(record.embedding.len())
            .map_err(|_| anyhow!("vault embedding {} dimension is too large", record.id))?;
        let embedding_json = serde_json::to_string(&record.embedding)
            .with_context(|| format!("failed to serialize vault embedding {}", record.id))?;

        self.connection.execute(
            r#"
            DELETE FROM vault_embedding
            WHERE record_id = ?
              AND record_kind = ?
              AND embedding_model = ?
              AND id <> ?
            "#,
            duckdb::params![
                record.record_id,
                record.record_kind,
                record.embedding_model,
                record.id
            ],
        )?;

        self.connection.execute(
            r#"
            INSERT INTO vault_embedding
                (
                    id,
                    record_id,
                    record_kind,
                    embedding_model,
                    embedding_dim,
                    embedding_json,
                    content_checksum,
                    source_checksum,
                    updated_at,
                    deleted_at
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                record_id = excluded.record_id,
                record_kind = excluded.record_kind,
                embedding_model = excluded.embedding_model,
                embedding_dim = excluded.embedding_dim,
                embedding_json = excluded.embedding_json,
                content_checksum = excluded.content_checksum,
                source_checksum = excluded.source_checksum,
                updated_at = excluded.updated_at,
                deleted_at = excluded.deleted_at
            "#,
            duckdb::params![
                record.id,
                record.record_id,
                record.record_kind,
                record.embedding_model,
                embedding_dim,
                embedding_json,
                record.content_checksum,
                record.source_checksum,
                record.updated_at,
                record.deleted_at.as_deref()
            ],
        )?;
        Ok(())
    }

    fn ensure_vault_embedding_columns(&self) -> Result<()> {
        let columns = [
            ("record_id", "VARCHAR"),
            ("record_kind", "VARCHAR"),
            ("embedding_model", "VARCHAR"),
            ("embedding_dim", "BIGINT"),
            ("embedding_json", "VARCHAR"),
            ("content_checksum", "VARCHAR"),
            ("source_checksum", "VARCHAR"),
            ("updated_at", "VARCHAR"),
            ("deleted_at", "VARCHAR"),
        ];
        for (column, column_type) in columns {
            if !self.table_has_column("vault_embedding", column)? {
                self.connection.execute(
                    &format!("ALTER TABLE vault_embedding ADD COLUMN {column} {column_type}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn table_has_column(&self, table: &str, column: &str) -> Result<bool> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_name = ?
              AND column_name = ?
            "#,
        )?;
        let mut rows = statement.query(duckdb::params![table, column])?;
        let Some(row) = rows.next()? else {
            return Err(anyhow!(
                "information_schema query returned no rows for {table}.{column}"
            ));
        };
        let count: i64 = row.get(0)?;
        Ok(count > 0)
    }

    fn count_table(&self, table: &str, clause: &str) -> Result<i64> {
        let mut statement = self
            .connection
            .prepare(&format!("SELECT count(*) FROM {table} {clause}"))?;
        let mut rows = statement.query([])?;
        let Some(row) = rows.next()? else {
            return Err(anyhow!("count query returned no rows for {table}"));
        };
        row.get(0).map_err(Into::into)
    }
}

pub struct DuckDbBrokerStoreService {
    client: DuckDbBrokerStoreClient,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl DuckDbBrokerStoreService {
    pub fn start(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().expand_homeish();
        let (request_sender, request_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel();

        let join_handle = thread::spawn(move || {
            let mut store = match DuckDbBrokerStore::open(&db_path) {
                Ok(store) => {
                    let _ = ready_sender.send(Ok(()));
                    store
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                    return;
                }
            };

            while let Ok(request) = request_receiver.recv() {
                match request {
                    BrokerStoreRequest::ReplaceVaultRecords(batch, response) => {
                        let _ = response.send(store.replace_vault_records(batch));
                    }
                    BrokerStoreRequest::UpsertVaultRecords(batch, response) => {
                        let _ = response.send(store.upsert_vault_records(batch));
                    }
                    BrokerStoreRequest::UpsertVaultEmbeddings(records, response) => {
                        let _ = response.send(store.upsert_vault_embeddings(records));
                    }
                    BrokerStoreRequest::ReplaceFileRecords {
                        file_id,
                        batch,
                        response,
                    } => {
                        let _ = response.send(store.replace_file_records(&file_id, batch));
                    }
                    BrokerStoreRequest::ListVaultFiles(response) => {
                        let _ = response.send(store.list_vault_files());
                    }
                    BrokerStoreRequest::TombstoneFileRecords {
                        file_id,
                        deleted_at,
                        response,
                    } => {
                        let _ = response.send(store.tombstone_file_records(&file_id, &deleted_at));
                    }
                    BrokerStoreRequest::TableCounts(response) => {
                        let _ = response.send(store.table_counts());
                    }
                    BrokerStoreRequest::QueryVaultChunks {
                        query,
                        limit,
                        response,
                    } => {
                        let _ = response.send(store.query_vault_chunks(&query, limit));
                    }
                    BrokerStoreRequest::QueryVaultTasks {
                        query,
                        limit,
                        response,
                    } => {
                        let _ = response.send(store.query_vault_tasks(&query, limit));
                    }
                    BrokerStoreRequest::QueryVaultLinks {
                        query,
                        limit,
                        response,
                    } => {
                        let _ = response.send(store.query_vault_links(&query, limit));
                    }
                    BrokerStoreRequest::QueryVaultRelationshipsForPath {
                        path,
                        limit,
                        response,
                    } => {
                        let _ =
                            response.send(store.query_vault_relationships_for_path(&path, limit));
                    }
                    BrokerStoreRequest::ListMissingVaultChunkEmbeddings {
                        embedding_model,
                        limit,
                        response,
                    } => {
                        let _ = response.send(
                            store.list_missing_vault_chunk_embeddings(&embedding_model, limit),
                        );
                    }
                    BrokerStoreRequest::CountMissingVaultChunkEmbeddings {
                        embedding_model,
                        response,
                    } => {
                        let _ = response
                            .send(store.count_missing_vault_chunk_embeddings(&embedding_model));
                    }
                    BrokerStoreRequest::ListDuplicateVaultEntities { limit, response } => {
                        let _ = response.send(store.list_duplicate_vault_entities(limit));
                    }
                    BrokerStoreRequest::CountStaleVaultSummaries { response } => {
                        let _ = response.send(store.count_stale_vault_summaries());
                    }
                    BrokerStoreRequest::ListStaleVaultSummaries { limit, response } => {
                        let _ = response.send(store.list_stale_vault_summaries(limit));
                    }
                    BrokerStoreRequest::ConsolidateDuplicateVaultEntities { limit, response } => {
                        let _ = response.send(store.consolidate_duplicate_vault_entities(limit));
                    }
                    BrokerStoreRequest::PurgeTombstonedVaultRecords {
                        older_than_deleted_at,
                        response,
                    } => {
                        let _ = response
                            .send(store.purge_tombstoned_vault_records(&older_than_deleted_at));
                    }
                    BrokerStoreRequest::QueryVaultChunksByEmbedding {
                        embedding_model,
                        query_embedding,
                        limit,
                        response,
                    } => {
                        let _ = response.send(store.query_vault_chunks_by_embedding(
                            &embedding_model,
                            &query_embedding,
                            limit,
                        ));
                    }
                    BrokerStoreRequest::BackupTo {
                        backup_path,
                        response,
                    } => {
                        let _ = response.send(store.backup_to(backup_path));
                    }
                    BrokerStoreRequest::Shutdown => break,
                }
            }
        });

        ready_receiver
            .recv()
            .map_err(|_| anyhow!("DuckDB broker store worker stopped before startup"))??;

        Ok(Self {
            client: DuckDbBrokerStoreClient {
                sender: request_sender,
            },
            join_handle: Some(join_handle),
        })
    }

    pub fn client(&self) -> DuckDbBrokerStoreClient {
        self.client.clone()
    }

    pub fn replace_vault_records(&self, batch: VaultRecordBatch) -> Result<()> {
        self.client.replace_vault_records(batch)
    }

    pub fn upsert_vault_records(&self, batch: VaultRecordBatch) -> Result<()> {
        self.client.upsert_vault_records(batch)
    }

    pub fn upsert_vault_embeddings(&self, records: Vec<VaultEmbeddingRecord>) -> Result<()> {
        self.client.upsert_vault_embeddings(records)
    }

    pub fn tombstone_file_records(&self, file_id: &str, deleted_at: &str) -> Result<()> {
        self.client.tombstone_file_records(file_id, deleted_at)
    }

    pub fn replace_file_records(&self, file_id: &str, batch: VaultRecordBatch) -> Result<()> {
        self.client.replace_file_records(file_id, batch)
    }

    pub fn list_vault_files(&self) -> Result<Vec<VaultFileRecord>> {
        self.client.list_vault_files()
    }

    pub fn table_counts(&self) -> Result<BrokerStoreCounts> {
        self.client.table_counts()
    }

    pub fn query_vault_chunks(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkContextRow>> {
        self.client.query_vault_chunks(query, limit)
    }

    pub fn query_vault_tasks(&self, query: &str, limit: usize) -> Result<Vec<VaultTaskContextRow>> {
        self.client.query_vault_tasks(query, limit)
    }

    pub fn query_vault_links(&self, query: &str, limit: usize) -> Result<Vec<VaultLinkContextRow>> {
        self.client.query_vault_links(query, limit)
    }

    pub fn query_vault_relationships_for_path(
        &self,
        path: &str,
        limit: usize,
    ) -> Result<Vec<VaultRelationshipContextRow>> {
        self.client.query_vault_relationships_for_path(path, limit)
    }

    pub fn list_missing_vault_chunk_embeddings(
        &self,
        embedding_model: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingCandidate>> {
        self.client
            .list_missing_vault_chunk_embeddings(embedding_model, limit)
    }

    pub fn count_missing_vault_chunk_embeddings(&self, embedding_model: &str) -> Result<i64> {
        self.client
            .count_missing_vault_chunk_embeddings(embedding_model)
    }

    pub fn list_duplicate_vault_entities(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultDuplicateEntityCandidate>> {
        self.client.list_duplicate_vault_entities(limit)
    }

    pub fn count_stale_vault_summaries(&self) -> Result<i64> {
        self.client.count_stale_vault_summaries()
    }

    pub fn list_stale_vault_summaries(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultStaleSummaryCandidate>> {
        self.client.list_stale_vault_summaries(limit)
    }

    pub fn query_vault_chunks_by_embedding(
        &self,
        embedding_model: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingHit>> {
        self.client
            .query_vault_chunks_by_embedding(embedding_model, query_embedding, limit)
    }

    pub fn consolidate_duplicate_vault_entities(&self, limit: usize) -> Result<usize> {
        self.client.consolidate_duplicate_vault_entities(limit)
    }

    pub fn purge_tombstoned_vault_records(&self, older_than_deleted_at: &str) -> Result<usize> {
        self.client
            .purge_tombstoned_vault_records(older_than_deleted_at)
    }

    pub fn backup_to(&self, backup_path: impl AsRef<Path>) -> Result<PathBuf> {
        self.client.backup_to(backup_path)
    }

    pub fn restore_from_backup(
        backup_path: impl AsRef<Path>,
        db_path: impl AsRef<Path>,
    ) -> Result<PathBuf> {
        let backup_path = backup_path.as_ref().expand_homeish();
        let db_path = db_path.as_ref().expand_homeish();
        if let Some(parent) = db_path.parent() {
            crate::ensure_dir(parent)?;
        }
        std::fs::copy(&backup_path, &db_path).with_context(|| {
            format!(
                "failed to restore DuckDB broker store backup from {} to {}",
                backup_path.display(),
                db_path.display()
            )
        })?;
        Ok(db_path)
    }
}

impl Drop for DuckDbBrokerStoreService {
    fn drop(&mut self) {
        let _ = self.client.sender.send(BrokerStoreRequest::Shutdown);
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

#[derive(Clone)]
pub struct DuckDbBrokerStoreClient {
    sender: mpsc::Sender<BrokerStoreRequest>,
}

impl DuckDbBrokerStoreClient {
    pub fn replace_vault_records(&self, batch: VaultRecordBatch) -> Result<()> {
        self.request(|response| BrokerStoreRequest::ReplaceVaultRecords(batch, response))
    }

    pub fn upsert_vault_records(&self, batch: VaultRecordBatch) -> Result<()> {
        self.request(|response| BrokerStoreRequest::UpsertVaultRecords(batch, response))
    }

    pub fn upsert_vault_embeddings(&self, records: Vec<VaultEmbeddingRecord>) -> Result<()> {
        self.request(|response| BrokerStoreRequest::UpsertVaultEmbeddings(records, response))
    }

    pub fn tombstone_file_records(&self, file_id: &str, deleted_at: &str) -> Result<()> {
        self.request(|response| BrokerStoreRequest::TombstoneFileRecords {
            file_id: file_id.to_string(),
            deleted_at: deleted_at.to_string(),
            response,
        })
    }

    pub fn replace_file_records(&self, file_id: &str, batch: VaultRecordBatch) -> Result<()> {
        self.request(|response| BrokerStoreRequest::ReplaceFileRecords {
            file_id: file_id.to_string(),
            batch,
            response,
        })
    }

    pub fn list_vault_files(&self) -> Result<Vec<VaultFileRecord>> {
        self.request(BrokerStoreRequest::ListVaultFiles)
    }

    pub fn table_counts(&self) -> Result<BrokerStoreCounts> {
        self.request(BrokerStoreRequest::TableCounts)
    }

    pub fn query_vault_chunks(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkContextRow>> {
        self.request(|response| BrokerStoreRequest::QueryVaultChunks {
            query: query.to_string(),
            limit,
            response,
        })
    }

    pub fn query_vault_tasks(&self, query: &str, limit: usize) -> Result<Vec<VaultTaskContextRow>> {
        self.request(|response| BrokerStoreRequest::QueryVaultTasks {
            query: query.to_string(),
            limit,
            response,
        })
    }

    pub fn query_vault_links(&self, query: &str, limit: usize) -> Result<Vec<VaultLinkContextRow>> {
        self.request(|response| BrokerStoreRequest::QueryVaultLinks {
            query: query.to_string(),
            limit,
            response,
        })
    }

    pub fn query_vault_relationships_for_path(
        &self,
        path: &str,
        limit: usize,
    ) -> Result<Vec<VaultRelationshipContextRow>> {
        self.request(
            |response| BrokerStoreRequest::QueryVaultRelationshipsForPath {
                path: path.to_string(),
                limit,
                response,
            },
        )
    }

    pub fn list_missing_vault_chunk_embeddings(
        &self,
        embedding_model: &str,
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingCandidate>> {
        self.request(
            |response| BrokerStoreRequest::ListMissingVaultChunkEmbeddings {
                embedding_model: embedding_model.to_string(),
                limit,
                response,
            },
        )
    }

    pub fn count_missing_vault_chunk_embeddings(&self, embedding_model: &str) -> Result<i64> {
        self.request(
            |response| BrokerStoreRequest::CountMissingVaultChunkEmbeddings {
                embedding_model: embedding_model.to_string(),
                response,
            },
        )
    }

    pub fn list_duplicate_vault_entities(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultDuplicateEntityCandidate>> {
        self.request(|response| BrokerStoreRequest::ListDuplicateVaultEntities { limit, response })
    }

    pub fn count_stale_vault_summaries(&self) -> Result<i64> {
        self.request(|response| BrokerStoreRequest::CountStaleVaultSummaries { response })
    }

    pub fn list_stale_vault_summaries(
        &self,
        limit: usize,
    ) -> Result<Vec<VaultStaleSummaryCandidate>> {
        self.request(|response| BrokerStoreRequest::ListStaleVaultSummaries { limit, response })
    }

    pub fn query_vault_chunks_by_embedding(
        &self,
        embedding_model: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<VaultChunkEmbeddingHit>> {
        self.request(|response| BrokerStoreRequest::QueryVaultChunksByEmbedding {
            embedding_model: embedding_model.to_string(),
            query_embedding: query_embedding.to_vec(),
            limit,
            response,
        })
    }

    pub fn consolidate_duplicate_vault_entities(&self, limit: usize) -> Result<usize> {
        self.request(
            |response| BrokerStoreRequest::ConsolidateDuplicateVaultEntities { limit, response },
        )
    }

    pub fn purge_tombstoned_vault_records(&self, older_than_deleted_at: &str) -> Result<usize> {
        self.request(|response| BrokerStoreRequest::PurgeTombstonedVaultRecords {
            older_than_deleted_at: older_than_deleted_at.to_string(),
            response,
        })
    }

    pub fn backup_to(&self, backup_path: impl AsRef<Path>) -> Result<PathBuf> {
        let backup_path = backup_path.as_ref().expand_homeish();
        self.request(|response| BrokerStoreRequest::BackupTo {
            backup_path,
            response,
        })
    }

    fn request<T>(
        &self,
        make_request: impl FnOnce(mpsc::Sender<Result<T>>) -> BrokerStoreRequest,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (response_sender, response_receiver) = mpsc::channel();
        self.sender
            .send(make_request(response_sender))
            .map_err(|_| anyhow!("DuckDB broker store worker stopped"))?;
        response_receiver
            .recv()
            .map_err(|_| anyhow!("DuckDB broker store worker dropped the response"))?
    }
}

enum BrokerStoreRequest {
    ReplaceVaultRecords(VaultRecordBatch, mpsc::Sender<Result<()>>),
    UpsertVaultRecords(VaultRecordBatch, mpsc::Sender<Result<()>>),
    UpsertVaultEmbeddings(Vec<VaultEmbeddingRecord>, mpsc::Sender<Result<()>>),
    ReplaceFileRecords {
        file_id: String,
        batch: VaultRecordBatch,
        response: mpsc::Sender<Result<()>>,
    },
    ListVaultFiles(mpsc::Sender<Result<Vec<VaultFileRecord>>>),
    TombstoneFileRecords {
        file_id: String,
        deleted_at: String,
        response: mpsc::Sender<Result<()>>,
    },
    TableCounts(mpsc::Sender<Result<BrokerStoreCounts>>),
    QueryVaultChunks {
        query: String,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultChunkContextRow>>>,
    },
    QueryVaultTasks {
        query: String,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultTaskContextRow>>>,
    },
    QueryVaultLinks {
        query: String,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultLinkContextRow>>>,
    },
    QueryVaultRelationshipsForPath {
        path: String,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultRelationshipContextRow>>>,
    },
    ListMissingVaultChunkEmbeddings {
        embedding_model: String,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultChunkEmbeddingCandidate>>>,
    },
    CountMissingVaultChunkEmbeddings {
        embedding_model: String,
        response: mpsc::Sender<Result<i64>>,
    },
    ListDuplicateVaultEntities {
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultDuplicateEntityCandidate>>>,
    },
    CountStaleVaultSummaries {
        response: mpsc::Sender<Result<i64>>,
    },
    ListStaleVaultSummaries {
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultStaleSummaryCandidate>>>,
    },
    ConsolidateDuplicateVaultEntities {
        limit: usize,
        response: mpsc::Sender<Result<usize>>,
    },
    PurgeTombstonedVaultRecords {
        older_than_deleted_at: String,
        response: mpsc::Sender<Result<usize>>,
    },
    QueryVaultChunksByEmbedding {
        embedding_model: String,
        query_embedding: Vec<f32>,
        limit: usize,
        response: mpsc::Sender<Result<Vec<VaultChunkEmbeddingHit>>>,
    },
    BackupTo {
        backup_path: PathBuf,
        response: mpsc::Sender<Result<PathBuf>>,
    },
    Shutdown,
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|term| {
            let term = term.trim().to_lowercase();
            if term.is_empty()
                || (term.len() == 1 && term.chars().all(|ch| ch.is_ascii_alphabetic()))
                || is_query_stopword(&term)
            {
                None
            } else {
                Some(term)
            }
        })
        .collect()
}

fn is_query_stopword(term: &str) -> bool {
    matches!(
        term,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "by"
            | "contains"
            | "contain"
            | "file"
            | "files"
            | "for"
            | "from"
            | "has"
            | "have"
            | "how"
            | "in"
            | "into"
            | "is"
            | "it"
            | "its"
            | "mentions"
            | "my"
            | "note"
            | "notes"
            | "of"
            | "on"
            | "or"
            | "our"
            | "that"
            | "the"
            | "this"
            | "to"
            | "vault"
            | "what"
            | "when"
            | "where"
            | "which"
            | "who"
            | "why"
            | "with"
            | "your"
    )
}

fn quoted_query_phrases(query: &str) -> Vec<String> {
    let mut phrases = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in query.chars() {
        if matches!(ch, '"' | '“' | '”') {
            if in_quote {
                let phrase = current.trim();
                if !phrase.is_empty() {
                    phrases.push(phrase.to_string());
                }
                current.clear();
                in_quote = false;
            } else {
                current.clear();
                in_quote = true;
            }
        } else if in_quote {
            current.push(ch);
        }
    }

    phrases
}

fn normalize_match_text(value: &str) -> String {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|part| {
            let part = part.trim().to_lowercase();
            if part.is_empty() { None } else { Some(part) }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_embedding_json(value: &str) -> Result<Vec<f32>> {
    let embedding: Vec<f32> = serde_json::from_str(value)?;
    if embedding.is_empty() {
        return Err(anyhow!("stored embedding vector is empty"));
    }
    Ok(embedding)
}

fn cosine_similarity(query: &[f32], candidate: &[f32]) -> f64 {
    if query.len() != candidate.len() || query.is_empty() {
        return 0.0;
    }

    let dot: f64 = query
        .iter()
        .zip(candidate.iter())
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum();
    let query_norm: f64 = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    let candidate_norm: f64 = candidate
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();

    if query_norm == 0.0 || candidate_norm == 0.0 {
        return 0.0;
    }
    dot / (query_norm * candidate_norm)
}

fn score_query_hit(query: &str, terms: &[String], haystack: &str) -> Option<(f64, Vec<String>)> {
    let haystack = haystack.to_lowercase();
    let normalized_haystack = normalize_match_text(&haystack);
    let matched_terms: Vec<String> = terms
        .iter()
        .filter(|term| haystack.contains(term.as_str()))
        .cloned()
        .collect();
    if matched_terms.is_empty() {
        return None;
    }
    let mut score = matched_terms.len() as f64;
    if haystack.contains(&query.to_lowercase()) {
        score += terms.len() as f64;
    }
    for phrase in quoted_query_phrases(query) {
        let normalized_phrase = normalize_match_text(&phrase);
        if !normalized_phrase.is_empty() && normalized_haystack.contains(&normalized_phrase) {
            score += 100.0 + (query_terms(&phrase).len() as f64 * 5.0);
        }
    }
    Some((score, matched_terms))
}

fn score_vault_chunk_hit(
    query: &str,
    terms: &[String],
    path: &str,
    title: &str,
    heading: &str,
    content: &str,
) -> Option<(f64, Vec<String>)> {
    let haystack = format!("{path}\n{title}\n{heading}\n{content}");
    let (mut score, matched_terms) = score_query_hit(query, terms, &haystack)?;
    let source_label = format!("{title}\n{heading}");
    if let Some((label_score, _)) = score_query_hit(query, terms, &source_label) {
        score += 50.0 + (label_score * 2.0);
        if source_label.to_lowercase().contains(&query.to_lowercase()) {
            score += terms.len() as f64 * 4.0;
        }
    }
    Some((score, matched_terms))
}

fn vault_parent_path(path: &str) -> String {
    let path = normalize_vault_path_key(path);
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn relationship_priority(relationship: &str) -> usize {
    match relationship {
        "backlink" => 0,
        "outlink" => 1,
        "folder_neighbor" => 2,
        _ => 3,
    }
}

fn resolve_vault_link_target<'a>(
    target: &str,
    source_path: &str,
    files: &'a [ActiveVaultFile],
) -> Option<&'a ActiveVaultFile> {
    let target = normalize_vault_path_key(target);
    if target.is_empty() {
        return None;
    }
    let target_no_ext = strip_md_extension(&target);

    if let Some(file) = files.iter().find(|file| {
        let path = normalize_vault_path_key(&file.path);
        let path_no_ext = strip_md_extension(&path);
        target == path || target_no_ext == path_no_ext
    }) {
        return Some(file);
    }

    if target.contains('/') {
        return None;
    }

    let source_parent = vault_parent_path(source_path);
    if let Some(file) = files.iter().find(|file| {
        vault_parent_path(&file.path) == source_parent
            && vault_path_stem(&file.path).eq_ignore_ascii_case(target_no_ext)
    }) {
        return Some(file);
    }

    let mut matching_files = files
        .iter()
        .filter(|file| vault_path_stem(&file.path).eq_ignore_ascii_case(target_no_ext));
    let first = matching_files.next()?;
    if matching_files.next().is_some() {
        None
    } else {
        Some(first)
    }
}

fn vault_path_matches(path: &str, requested_path: &str) -> bool {
    let path = normalize_vault_path_key(path);
    let requested_path = normalize_vault_path_key(requested_path);
    if path.is_empty() || requested_path.is_empty() {
        return false;
    }
    let path_no_ext = strip_md_extension(&path);
    let requested_no_ext = strip_md_extension(&requested_path);
    path == requested_path
        || path_no_ext == requested_no_ext
        || requested_path.ends_with(&format!("/{path}"))
        || requested_no_ext.ends_with(&format!("/{path_no_ext}"))
        || path.ends_with(&format!("/{requested_path}"))
        || path_no_ext.ends_with(&format!("/{requested_no_ext}"))
}

fn vault_path_stem(path: &str) -> String {
    let path = normalize_vault_path_key(path);
    let name = path.rsplit('/').next().unwrap_or(path.as_str());
    strip_md_extension(name).to_string()
}

fn normalize_vault_path_key(value: &str) -> String {
    let mut value = value
        .trim()
        .trim_matches(['`', '"', '\'', '<', '>', '(', ')']);
    if let Some(rest) = value.strip_prefix("vault://") {
        value = rest;
    }
    value = value.split('#').next().unwrap_or(value);
    value = value.split('|').next().unwrap_or(value);
    let value = value.replace('\\', "/");
    let value = value.trim_start_matches("./").trim_start_matches('/');
    value.to_lowercase()
}

fn strip_md_extension(value: &str) -> &str {
    value.strip_suffix(".md").unwrap_or(value)
}

trait ExpandHomeish {
    fn expand_homeish(&self) -> PathBuf;
}

impl ExpandHomeish for Path {
    fn expand_homeish(&self) -> PathBuf {
        if self.is_absolute() {
            self.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(self)
        }
    }
}
