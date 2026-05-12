use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

const DEFAULT_GARDEN_ITEM_LIMIT: usize = 8;
const DEFAULT_SESSION_SCAN_LIMIT: usize = 100;

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
    pub stale_vault_summary_facts: i64,
    pub missed_extraction_sessions: i64,
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
        return Ok(empty_report(None, embedding_model, limit));
    };
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)?;
    let counts = service.table_counts()?;
    let missing_embeddings = service.count_missing_vault_chunk_embeddings(embedding_model)?;
    let duplicate_entities = service.list_duplicate_vault_entities(limit)?;
    let stale_summary_count = service.count_stale_vault_summaries()?;
    let stale_summaries = service.list_stale_vault_summaries(limit)?;
    let missed_sessions = list_missed_extraction_session_paths(limit);

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

    if stale_summary_count > 0 {
        work_items.push(AmbientGardenWorkItem {
            kind: "stale_fact_verification".to_string(),
            summary: format!(
                "{stale_summary_count} derived Vault summary/fact record(s) were generated from outdated Vault source checksums"
            ),
            count: stale_summary_count,
            source: "duckdb_broker_store".to_string(),
            command: None,
            paths: stale_summaries
                .into_iter()
                .map(|candidate| candidate.path)
                .collect(),
        });
    }

    if !missed_sessions.is_empty() {
        work_items.push(AmbientGardenWorkItem {
            kind: "retroactive_extraction_candidate".to_string(),
            summary: format!(
                "{} recent crashed/error session(s) may need retroactive memory extraction",
                missed_sessions.len()
            ),
            count: missed_sessions.len() as i64,
            source: "jcode_sessions".to_string(),
            command: None,
            paths: missed_sessions.clone(),
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
            stale_vault_summary_facts: stale_summary_count,
            missed_extraction_sessions: missed_sessions.len() as i64,
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
    Ok(empty_report(db_path, embedding_model, _limit))
}

fn empty_report(
    db_path: Option<PathBuf>,
    embedding_model: &str,
    limit: usize,
) -> AmbientGardenReport {
    let missed_sessions = list_missed_extraction_session_paths(limit);
    let mut work_items = vec![AmbientGardenWorkItem {
        kind: "broker_index_unavailable".to_string(),
        summary: "No DuckDB broker index is configured for ambient garden review".to_string(),
        count: 0,
        source: "ambient_garden".to_string(),
        command: None,
        paths: Vec::new(),
    }];
    if !missed_sessions.is_empty() {
        work_items.push(AmbientGardenWorkItem {
            kind: "retroactive_extraction_candidate".to_string(),
            summary: format!(
                "{} recent crashed/error session(s) may need retroactive memory extraction",
                missed_sessions.len()
            ),
            count: missed_sessions.len() as i64,
            source: "jcode_sessions".to_string(),
            command: None,
            paths: missed_sessions.clone(),
        });
    }
    AmbientGardenReport {
        mode: "garden_only".to_string(),
        read_only: true,
        autonomous_actions_allowed: false,
        system_changes_allowed: false,
        db_path: db_path.map(|path| path.display().to_string()),
        embedding_model: embedding_model.to_string(),
        counts: AmbientGardenCounts {
            missed_extraction_sessions: missed_sessions.len() as i64,
            ..Default::default()
        },
        work_items,
    }
}

#[derive(Debug, Deserialize)]
struct GardenSessionHeader {
    #[serde(default)]
    status: crate::session::SessionStatus,
    #[serde(default)]
    is_debug: bool,
    #[serde(default)]
    messages: Vec<serde_json::Value>,
}

fn list_missed_extraction_session_paths(limit: usize) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }
    let sessions_dir = match crate::storage::jcode_dir() {
        Ok(dir) => dir.join("sessions"),
        Err(_) => return Vec::new(),
    };
    if !sessions_dir.exists() {
        return Vec::new();
    }

    let mut entries = match std::fs::read_dir(&sessions_dir) {
        Ok(entries) => entries
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                (path.extension().is_some_and(|ext| ext == "json")).then_some(path)
            })
            .filter_map(|path| {
                let modified = path
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                Some((modified, path))
            })
            .collect::<Vec<_>>(),
        Err(_) => return Vec::new(),
    };
    entries.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified));
    entries.truncate(DEFAULT_SESSION_SCAN_LIMIT);

    let mut candidates = Vec::new();
    for (_, path) in entries {
        if candidates.len() >= limit {
            break;
        }
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(header) = serde_json::from_str::<GardenSessionHeader>(&content) else {
            continue;
        };
        if header.is_debug || header.messages.is_empty() {
            continue;
        }
        let needs_extraction = matches!(
            header.status,
            crate::session::SessionStatus::Crashed { .. }
                | crate::session::SessionStatus::Error { .. }
        );
        if needs_extraction {
            candidates.push(path.display().to_string());
        }
    }
    candidates
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
