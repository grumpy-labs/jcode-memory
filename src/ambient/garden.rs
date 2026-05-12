use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const DEFAULT_GARDEN_ITEM_LIMIT: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AmbientGardenReport {
    pub mode: String,
    pub read_only: bool,
    pub autonomous_actions_allowed: bool,
    pub system_changes_allowed: bool,
    pub db_path: Option<String>,
    pub embedding_model: String,
    pub counts: AmbientGardenCounts,
    pub work_items: Vec<AmbientGardenWorkItem>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AmbientGardenCounts {
    pub active_vault_file: i64,
    pub active_vault_chunk: i64,
    pub active_vault_task: i64,
    pub active_vault_link: i64,
    pub active_vault_summary: i64,
    pub active_vault_entity: i64,
    pub active_vault_embedding: i64,
    pub missing_vault_chunk_embeddings: i64,
    pub tombstoned_vault_file: i64,
    pub tombstoned_vault_chunk: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AmbientGardenWorkItem {
    pub kind: String,
    pub summary: String,
    pub count: i64,
    pub source: String,
    pub command: Option<String>,
    pub paths: Vec<String>,
}

pub fn gather_ambient_garden_report_from_env() -> Result<AmbientGardenReport> {
    let db_path = std::env::var_os("JCODE_BROKER_DUCKDB_PATH").map(PathBuf::from);
    let embedding_model = std::env::var("JCODE_BROKER_VAULT_EMBEDDING_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "jcode-local-embedding".to_string());
    gather_ambient_garden_report(db_path, &embedding_model, DEFAULT_GARDEN_ITEM_LIMIT)
}

#[cfg(feature = "duckdb-storage")]
pub fn gather_ambient_garden_report(
    db_path: Option<PathBuf>,
    embedding_model: &str,
    limit: usize,
) -> Result<AmbientGardenReport> {
    let Some(db_path) = db_path else {
        return Ok(empty_report(None, embedding_model));
    };
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)?;
    let counts = service.table_counts()?;
    let missing_embeddings = service.count_missing_vault_chunk_embeddings(embedding_model)?;
    let duplicate_entities = service.list_duplicate_vault_entities(limit)?;

    let mut work_items = Vec::new();
    if missing_embeddings > 0 {
        work_items.push(AmbientGardenWorkItem {
            kind: "embedding_backfill".to_string(),
            summary: format!(
                "{missing_embeddings} active Vault chunk(s) are missing current {embedding_model} embeddings"
            ),
            count: missing_embeddings,
            source: "duckdb_broker_store".to_string(),
            command: Some(format!(
                "jcode broker embed-vault --db {} --model {} --limit {}",
                shell_quote_hint(&db_path.display().to_string()),
                shell_quote_hint(embedding_model),
                missing_embeddings
            )),
            paths: Vec::new(),
        });
    }

    let tombstoned_files = counts.vault_file - counts.active_vault_file;
    let tombstoned_chunks = counts.vault_chunk - counts.active_vault_chunk;
    if tombstoned_files > 0 || tombstoned_chunks > 0 {
        work_items.push(AmbientGardenWorkItem {
            kind: "stale_tombstone_review".to_string(),
            summary: format!(
                "{tombstoned_files} tombstoned Vault file(s) and {tombstoned_chunks} tombstoned chunk(s) are retained for rollback/provenance review"
            ),
            count: tombstoned_files + tombstoned_chunks,
            source: "duckdb_broker_store".to_string(),
            command: None,
            paths: Vec::new(),
        });
    }

    for duplicate in duplicate_entities {
        work_items.push(AmbientGardenWorkItem {
            kind: "duplicate_entity_candidate".to_string(),
            summary: format!(
                "{} entity {:?} appears in {} active Vault file(s)",
                duplicate.kind, duplicate.name, duplicate.active_file_count
            ),
            count: duplicate.active_file_count,
            source: "duckdb_broker_store".to_string(),
            command: None,
            paths: duplicate.paths,
        });
    }

    Ok(AmbientGardenReport {
        mode: "garden_only".to_string(),
        read_only: true,
        autonomous_actions_allowed: false,
        system_changes_allowed: false,
        db_path: Some(db_path.display().to_string()),
        embedding_model: embedding_model.to_string(),
        counts: AmbientGardenCounts {
            active_vault_file: counts.active_vault_file,
            active_vault_chunk: counts.active_vault_chunk,
            active_vault_task: counts.active_vault_task,
            active_vault_link: counts.active_vault_link,
            active_vault_summary: counts.active_vault_summary,
            active_vault_entity: counts.active_vault_entity,
            active_vault_embedding: counts.active_vault_embedding,
            missing_vault_chunk_embeddings: missing_embeddings,
            tombstoned_vault_file: tombstoned_files,
            tombstoned_vault_chunk: tombstoned_chunks,
        },
        work_items,
    })
}

#[cfg(not(feature = "duckdb-storage"))]
pub fn gather_ambient_garden_report(
    db_path: Option<PathBuf>,
    embedding_model: &str,
    _limit: usize,
) -> Result<AmbientGardenReport> {
    Ok(empty_report(db_path, embedding_model))
}

fn empty_report(db_path: Option<PathBuf>, embedding_model: &str) -> AmbientGardenReport {
    AmbientGardenReport {
        mode: "garden_only".to_string(),
        read_only: true,
        autonomous_actions_allowed: false,
        system_changes_allowed: false,
        db_path: db_path.map(|path| path.display().to_string()),
        embedding_model: embedding_model.to_string(),
        counts: AmbientGardenCounts::default(),
        work_items: vec![AmbientGardenWorkItem {
            kind: "broker_index_unavailable".to_string(),
            summary: "No DuckDB broker index is configured for ambient garden review".to_string(),
            count: 0,
            source: "ambient_garden".to_string(),
            command: None,
            paths: Vec::new(),
        }],
    }
}

#[cfg(feature = "duckdb-storage")]
fn shell_quote_hint(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/'))
    {
        value.to_string()
    } else {
        format!("{value:?}")
    }
}
