#[cfg(feature = "duckdb-storage")]
use anyhow::Context;
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[cfg(feature = "duckdb-storage")]
use std::io::Write;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AmbientGardenActionKind {
    EmbeddingBackfill,
    ConsolidateDuplicates,
    PruneTombstones,
    VerifyStaleFacts,
    RetroactiveExtraction,
}

impl AmbientGardenActionKind {
    pub fn all() -> Vec<Self> {
        vec![
            Self::EmbeddingBackfill,
            Self::ConsolidateDuplicates,
            Self::PruneTombstones,
            Self::VerifyStaleFacts,
            Self::RetroactiveExtraction,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbientGardenApplyOptions {
    pub db_path: Option<PathBuf>,
    pub vault_path: Option<PathBuf>,
    pub embedding_model: String,
    pub kinds: Vec<AmbientGardenActionKind>,
    pub limit: usize,
    pub tombstone_retention_days: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AmbientGardenApplyReport {
    pub mode: String,
    pub read_only: bool,
    pub autonomous_actions_allowed: bool,
    pub system_changes_allowed: bool,
    pub db_path: Option<String>,
    pub vault_path: Option<String>,
    pub embedding_model: String,
    pub counts_before: AmbientGardenCounts,
    pub counts_after: AmbientGardenCounts,
    pub actions: Vec<AmbientGardenActionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AmbientGardenActionResult {
    pub kind: String,
    pub status: String,
    pub summary: String,
    pub count: i64,
    pub source: String,
    pub paths: Vec<String>,
}

pub fn gather_ambient_garden_report_from_env() -> Result<AmbientGardenReport> {
    let db_path = std::env::var_os("JCODE_BROKER_DUCKDB_PATH").map(PathBuf::from);
    let embedding_model = embedding_model_from_env();
    gather_ambient_garden_report(db_path, &embedding_model, DEFAULT_GARDEN_ITEM_LIMIT)
}

pub fn apply_ambient_garden_from_env(
    kinds: Vec<AmbientGardenActionKind>,
) -> Result<AmbientGardenApplyReport> {
    let db_path = std::env::var_os("JCODE_BROKER_DUCKDB_PATH").map(PathBuf::from);
    let vault_path = std::env::var_os("JCODE_BROKER_VAULT_PATH")
        .or_else(|| std::env::var_os("JCODE_VAULT_PATH"))
        .map(PathBuf::from);
    let limit = std::env::var("JCODE_AMBIENT_GARDEN_LIMIT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_GARDEN_ITEM_LIMIT);
    let tombstone_retention_days = std::env::var("JCODE_AMBIENT_TOMBSTONE_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(30);
    apply_ambient_garden_actions(AmbientGardenApplyOptions {
        db_path,
        vault_path,
        embedding_model: embedding_model_from_env(),
        kinds,
        limit,
        tombstone_retention_days,
    })
}

fn embedding_model_from_env() -> String {
    std::env::var("JCODE_BROKER_VAULT_EMBEDDING_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "jcode-local-embedding".to_string())
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

#[cfg(feature = "duckdb-storage")]
pub fn apply_ambient_garden_actions(
    options: AmbientGardenApplyOptions,
) -> Result<AmbientGardenApplyReport> {
    let kinds = if options.kinds.is_empty() {
        AmbientGardenActionKind::all()
    } else {
        options.kinds.clone()
    };
    let service = match options.db_path.as_ref() {
        Some(db_path) => {
            Some(jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(db_path)?)
        }
        None => None,
    };
    let missed_before = list_missed_extraction_session_paths(options.limit);
    let counts_before = match service.as_ref() {
        Some(service) => garden_counts_for_service(
            service,
            &options.embedding_model,
            missed_before.len() as i64,
        )?,
        None => AmbientGardenCounts {
            missed_extraction_sessions: missed_before.len() as i64,
            ..Default::default()
        },
    };

    let mut actions = Vec::new();
    for kind in kinds {
        let action_kind_name = ambient_garden_action_name(&kind).to_string();
        let result = match kind {
            AmbientGardenActionKind::EmbeddingBackfill => {
                apply_embedding_backfill(service.as_ref(), &options.embedding_model, options.limit)
            }
            AmbientGardenActionKind::ConsolidateDuplicates => {
                apply_duplicate_consolidation(service.as_ref(), options.limit)
            }
            AmbientGardenActionKind::PruneTombstones => {
                apply_tombstone_prune(service.as_ref(), options.tombstone_retention_days)
            }
            AmbientGardenActionKind::VerifyStaleFacts => apply_stale_fact_verification(
                service.as_ref(),
                options.vault_path.as_ref(),
                options.limit,
            ),
            AmbientGardenActionKind::RetroactiveExtraction => {
                apply_retroactive_extraction(options.limit)
            }
        };
        actions.push(result.unwrap_or_else(|error| failed_action(&action_kind_name, error)));
    }

    let missed_after = list_missed_extraction_session_paths(options.limit);
    let counts_after = match service.as_ref() {
        Some(service) => {
            garden_counts_for_service(service, &options.embedding_model, missed_after.len() as i64)?
        }
        None => AmbientGardenCounts {
            missed_extraction_sessions: missed_after.len() as i64,
            ..Default::default()
        },
    };

    Ok(AmbientGardenApplyReport {
        mode: "garden_apply".to_string(),
        read_only: false,
        autonomous_actions_allowed: false,
        system_changes_allowed: false,
        db_path: options.db_path.map(|path| path.display().to_string()),
        vault_path: options.vault_path.map(|path| path.display().to_string()),
        embedding_model: options.embedding_model,
        counts_before,
        counts_after,
        actions,
    })
}

#[cfg(feature = "duckdb-storage")]
fn garden_counts_for_service(
    service: &jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService,
    embedding_model: &str,
    missed_sessions: i64,
) -> Result<AmbientGardenCounts> {
    let counts = service.table_counts()?;
    let missing_embeddings = service.count_missing_vault_chunk_embeddings(embedding_model)?;
    let stale_summary_count = service.count_stale_vault_summaries()?;
    Ok(AmbientGardenCounts {
        active_vault_file: counts.active_vault_file,
        active_vault_chunk: counts.active_vault_chunk,
        active_vault_task: counts.active_vault_task,
        active_vault_link: counts.active_vault_link,
        active_vault_summary: counts.active_vault_summary,
        active_vault_entity: counts.active_vault_entity,
        active_vault_embedding: counts.active_vault_embedding,
        missing_vault_chunk_embeddings: missing_embeddings,
        stale_vault_summary_facts: stale_summary_count,
        missed_extraction_sessions: missed_sessions,
        tombstoned_vault_file: counts.vault_file - counts.active_vault_file,
        tombstoned_vault_chunk: counts.vault_chunk - counts.active_vault_chunk,
    })
}

#[cfg(feature = "duckdb-storage")]
fn ambient_garden_action_name(kind: &AmbientGardenActionKind) -> &'static str {
    match kind {
        AmbientGardenActionKind::EmbeddingBackfill => "embedding_backfill",
        AmbientGardenActionKind::ConsolidateDuplicates => "duplicate_entity_consolidation",
        AmbientGardenActionKind::PruneTombstones => "stale_tombstone_prune",
        AmbientGardenActionKind::VerifyStaleFacts => "stale_fact_verification",
        AmbientGardenActionKind::RetroactiveExtraction => "retroactive_extraction",
    }
}

#[cfg(feature = "duckdb-storage")]
fn failed_action(kind: &str, error: anyhow::Error) -> AmbientGardenActionResult {
    AmbientGardenActionResult {
        kind: kind.to_string(),
        status: "failed".to_string(),
        summary: error.to_string(),
        count: 0,
        source: "ambient_garden".to_string(),
        paths: Vec::new(),
    }
}

#[cfg(feature = "duckdb-storage")]
fn skipped_action(kind: &str, status: &str, summary: &str) -> AmbientGardenActionResult {
    AmbientGardenActionResult {
        kind: kind.to_string(),
        status: status.to_string(),
        summary: summary.to_string(),
        count: 0,
        source: "ambient_garden".to_string(),
        paths: Vec::new(),
    }
}

#[cfg(feature = "duckdb-storage")]
fn apply_embedding_backfill(
    service: Option<&jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService>,
    embedding_model: &str,
    limit: usize,
) -> Result<AmbientGardenActionResult> {
    let Some(service) = service else {
        return Ok(skipped_action(
            "embedding_backfill",
            "skipped_no_broker_db",
            "No DuckDB broker index is configured for Vault embedding backfill",
        ));
    };
    let candidates = service.list_missing_vault_chunk_embeddings(embedding_model, limit)?;
    if candidates.is_empty() {
        return Ok(AmbientGardenActionResult {
            kind: "embedding_backfill".to_string(),
            status: "noop".to_string(),
            summary: "No active Vault chunks are missing current embeddings".to_string(),
            count: 0,
            source: "duckdb_broker_store".to_string(),
            paths: Vec::new(),
        });
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut records = Vec::with_capacity(candidates.len());
    let mut paths = Vec::new();
    for candidate in &candidates {
        let embedding_text = format!(
            "{}\n{}\n{}",
            candidate.title, candidate.heading, candidate.content
        );
        let embedding = crate::embedding::embed(&embedding_text).with_context(|| {
            format!(
                "failed to embed Vault chunk {} from {}",
                candidate.id, candidate.path
            )
        })?;
        paths.push(candidate.path.clone());
        records.push(jcode_storage::duckdb_broker_store::VaultEmbeddingRecord {
            id: format!("vault_embedding:{embedding_model}:{}", candidate.id),
            record_id: candidate.id.clone(),
            record_kind: "vault_chunk".to_string(),
            embedding_model: embedding_model.to_string(),
            embedding,
            content_checksum: candidate.checksum.clone(),
            source_checksum: candidate.source_checksum.clone(),
            updated_at: now.clone(),
            deleted_at: None,
        });
    }
    let count = records.len() as i64;
    service.upsert_vault_embeddings(records)?;
    Ok(AmbientGardenActionResult {
        kind: "embedding_backfill".to_string(),
        status: "applied".to_string(),
        summary: format!("Backfilled {count} Vault chunk embedding(s)"),
        count,
        source: "duckdb_broker_store".to_string(),
        paths,
    })
}

#[cfg(feature = "duckdb-storage")]
fn apply_duplicate_consolidation(
    service: Option<&jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService>,
    limit: usize,
) -> Result<AmbientGardenActionResult> {
    let Some(service) = service else {
        return Ok(skipped_action(
            "duplicate_entity_consolidation",
            "skipped_no_broker_db",
            "No DuckDB broker index is configured for duplicate entity consolidation",
        ));
    };
    let count = service.consolidate_duplicate_vault_entities(limit)? as i64;
    Ok(AmbientGardenActionResult {
        kind: "duplicate_entity_consolidation".to_string(),
        status: if count > 0 { "applied" } else { "noop" }.to_string(),
        summary: if count > 0 {
            format!("Reinforced {count} duplicate Vault entity relationship(s)")
        } else {
            "No duplicate Vault entity relationships needed reinforcement".to_string()
        },
        count,
        source: "duckdb_broker_store".to_string(),
        paths: Vec::new(),
    })
}

#[cfg(feature = "duckdb-storage")]
fn apply_tombstone_prune(
    service: Option<&jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService>,
    retention_days: i64,
) -> Result<AmbientGardenActionResult> {
    let Some(service) = service else {
        return Ok(skipped_action(
            "stale_tombstone_prune",
            "skipped_no_broker_db",
            "No DuckDB broker index is configured for tombstone pruning",
        ));
    };
    let retention_days = retention_days.max(0);
    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days);
    let cutoff = cutoff.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let count = service.purge_tombstoned_vault_records(&cutoff)? as i64;
    Ok(AmbientGardenActionResult {
        kind: "stale_tombstone_prune".to_string(),
        status: if count > 0 { "applied" } else { "noop" }.to_string(),
        summary: if count > 0 {
            format!(
                "Pruned {count} tombstoned broker index record(s) older than {retention_days} day(s)"
            )
        } else {
            format!("No tombstoned broker index records were older than {retention_days} day(s)")
        },
        count,
        source: "duckdb_broker_store".to_string(),
        paths: Vec::new(),
    })
}

#[cfg(feature = "duckdb-storage")]
fn apply_stale_fact_verification(
    service: Option<&jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService>,
    vault_path: Option<&PathBuf>,
    limit: usize,
) -> Result<AmbientGardenActionResult> {
    let Some(service) = service else {
        return Ok(skipped_action(
            "stale_fact_verification",
            "skipped_no_broker_db",
            "No DuckDB broker index is configured for stale fact verification",
        ));
    };
    let Some(vault_path) = vault_path else {
        return Ok(skipped_action(
            "stale_fact_verification",
            "skipped_missing_vault_path",
            "Set JCODE_BROKER_VAULT_PATH or JCODE_VAULT_PATH to reconcile stale Vault facts",
        ));
    };
    let stale_paths = service
        .list_stale_vault_summaries(limit)?
        .into_iter()
        .map(|candidate| candidate.path)
        .collect::<Vec<_>>();
    let report = service.reconcile_vault_path(vault_path)?;
    let count = report.new_files
        + report.updated_files
        + report.tombstoned_files
        + report.renamed_files.len();
    Ok(AmbientGardenActionResult {
        kind: "stale_fact_verification".to_string(),
        status: if count > 0 { "applied" } else { "noop" }.to_string(),
        summary: format!(
            "reconciled Vault source into broker index: new={}, updated={}, tombstoned={}, renamed={}",
            report.new_files,
            report.updated_files,
            report.tombstoned_files,
            report.renamed_files.len()
        ),
        count: count as i64,
        source: "duckdb_broker_store".to_string(),
        paths: stale_paths,
    })
}

#[cfg(feature = "duckdb-storage")]
fn apply_retroactive_extraction(limit: usize) -> Result<AmbientGardenActionResult> {
    let paths = list_missed_extraction_session_paths(limit);
    if paths.is_empty() {
        return Ok(AmbientGardenActionResult {
            kind: "retroactive_extraction".to_string(),
            status: "noop".to_string(),
            summary: "No recent crashed/error sessions need retroactive extraction".to_string(),
            count: 0,
            source: "jcode_sessions".to_string(),
            paths: Vec::new(),
        });
    }

    let sidecar_enabled = crate::memory::memory_sidecar_enabled();
    let mut extracted_sessions = 0i64;
    let mut failed_sessions = 0i64;
    for path in &paths {
        let extraction_result = if sidecar_enabled {
            retroactively_extract_session(path)
        } else {
            Ok(("skipped_sidecar_disabled".to_string(), Vec::new()))
        };
        match extraction_result {
            Ok((status, memory_ids)) => {
                if status == "extracted" {
                    extracted_sessions += 1;
                }
                append_retroactive_extraction_audit(path, &status, &memory_ids, None)?;
            }
            Err(error) => {
                failed_sessions += 1;
                append_retroactive_extraction_audit(path, "failed", &[], Some(error.to_string()))?;
            }
        }
    }

    let status = if failed_sessions > 0 {
        "partial_failure"
    } else if !sidecar_enabled {
        "skipped_sidecar_disabled"
    } else if extracted_sessions > 0 {
        "applied"
    } else {
        "noop"
    };
    Ok(AmbientGardenActionResult {
        kind: "retroactive_extraction".to_string(),
        status: status.to_string(),
        summary: if sidecar_enabled {
            format!(
                "Processed {} missed session(s) for retroactive extraction; extracted={}, failed={}",
                paths.len(),
                extracted_sessions,
                failed_sessions
            )
        } else {
            format!(
                "Audited {} missed session(s); sidecar extraction is disabled",
                paths.len()
            )
        },
        count: paths.len() as i64,
        source: "jcode_sessions".to_string(),
        paths,
    })
}

#[cfg(feature = "duckdb-storage")]
fn retroactively_extract_session(path: &str) -> Result<(String, Vec<String>)> {
    let session = crate::session::Session::load_from_path(std::path::Path::new(path))
        .with_context(|| format!("failed to load session {path}"))?;
    let transcript = transcript_for_session(&session);
    if transcript.trim().is_empty() {
        return Ok(("noop_empty_transcript".to_string(), Vec::new()));
    }
    let manager = match session.working_dir.as_deref() {
        Some(dir) if !dir.trim().is_empty() => {
            crate::memory::MemoryManager::new().with_project_dir(PathBuf::from(dir))
        }
        _ => crate::memory::MemoryManager::new(),
    };
    let memory_ids =
        futures::executor::block_on(manager.extract_from_transcript(&transcript, &session.id))?;
    let status = if memory_ids.is_empty() {
        "stored_provenance".to_string()
    } else {
        "extracted".to_string()
    };
    Ok((status, memory_ids))
}

#[cfg(feature = "duckdb-storage")]
fn transcript_for_session(session: &crate::session::Session) -> String {
    let mut transcript = String::new();
    for msg in &session.messages {
        let role = match msg.role {
            crate::message::Role::User => "User",
            crate::message::Role::Assistant => "Assistant",
        };
        transcript.push_str(&format!("**{}:**\n", role));
        for block in &msg.content {
            match block {
                crate::message::ContentBlock::Text { text, .. } => {
                    transcript.push_str(text);
                    transcript.push('\n');
                }
                crate::message::ContentBlock::ToolUse { name, .. } => {
                    transcript.push_str(&format!("[Used tool: {}]\n", name));
                }
                crate::message::ContentBlock::ToolResult { content, .. } => {
                    let preview = if content.len() > 200 {
                        format!("{}...", crate::util::truncate_str(content, 200))
                    } else {
                        content.clone()
                    };
                    transcript.push_str(&format!("[Result: {}]\n", preview));
                }
                crate::message::ContentBlock::Reasoning { .. } => {}
                crate::message::ContentBlock::Image { .. } => {
                    transcript.push_str("[Image]\n");
                }
                crate::message::ContentBlock::OpenAICompaction { .. } => {
                    transcript.push_str("[OpenAI native compaction]\n");
                }
            }
        }
        transcript.push('\n');
    }
    transcript
}

#[cfg(feature = "duckdb-storage")]
fn append_retroactive_extraction_audit(
    session_path: &str,
    status: &str,
    memory_ids: &[String],
    error: Option<String>,
) -> Result<()> {
    let audit_path = crate::storage::jcode_dir()?
        .join("ambient")
        .join("retroactive_extraction.jsonl");
    if let Some(parent) = audit_path.parent() {
        crate::storage::ensure_dir(parent)?;
    }
    let payload = serde_json::json!({
        "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "session_path": session_path,
        "status": status,
        "memory_ids": memory_ids,
        "error": error,
    });
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&audit_path)
        .with_context(|| format!("failed to open {}", audit_path.display()))?;
    writeln!(file, "{}", serde_json::to_string(&payload)?)?;
    Ok(())
}

#[cfg(not(feature = "duckdb-storage"))]
pub fn gather_ambient_garden_report(
    db_path: Option<PathBuf>,
    embedding_model: &str,
    _limit: usize,
) -> Result<AmbientGardenReport> {
    Ok(empty_report(db_path, embedding_model, _limit))
}

#[cfg(not(feature = "duckdb-storage"))]
pub fn apply_ambient_garden_actions(
    options: AmbientGardenApplyOptions,
) -> Result<AmbientGardenApplyReport> {
    let missed_sessions = list_missed_extraction_session_paths(options.limit);
    let counts = AmbientGardenCounts {
        missed_extraction_sessions: missed_sessions.len() as i64,
        ..Default::default()
    };
    Ok(AmbientGardenApplyReport {
        mode: "garden_apply".to_string(),
        read_only: false,
        autonomous_actions_allowed: false,
        system_changes_allowed: false,
        db_path: options.db_path.map(|path| path.display().to_string()),
        vault_path: options.vault_path.map(|path| path.display().to_string()),
        embedding_model: options.embedding_model,
        counts_before: counts.clone(),
        counts_after: counts,
        actions: vec![AmbientGardenActionResult {
            kind: "broker_index_unavailable".to_string(),
            status: "skipped_no_duckdb_storage_feature".to_string(),
            summary: "DuckDB broker-store garden apply actions require the duckdb-storage feature"
                .to_string(),
            count: 0,
            source: "ambient_garden".to_string(),
            paths: missed_sessions,
        }],
    })
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
