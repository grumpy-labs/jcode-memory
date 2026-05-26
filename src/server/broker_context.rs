use super::SessionAgents;
use crate::memory::TrustLevel;
use crate::memory::{MemoryCategory, MemoryEntry, MemoryManager, MemoryScope};
use crate::memory_graph::EdgeKind;
#[cfg(feature = "duckdb-storage")]
use crate::protocol::BrokerVaultRefreshCounts;
use crate::protocol::{
    BrokerContextFragment, BrokerContextItem, BrokerContextOrigin, BrokerContextRelevance,
    BrokerMemoryContextItem, BrokerMemoryExtractionStatus, ClioContextPacketItem,
    ClioContextPacketV1, ServerEvent,
};
use crate::todo::TodoItem;
use anyhow::{Context, Result};
use jcode_session_types::SessionSearchResult;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
#[cfg(feature = "duckdb-storage")]
use std::sync::{Mutex as StdMutex, OnceLock};
use tokio::sync::mpsc;

#[cfg(feature = "duckdb-storage")]
use jcode_storage::duckdb_broker_store::{
    BrokerStoreCounts, DuckDbBrokerStoreClient, DuckDbBrokerStoreService, VaultChunkContextRow,
    VaultChunkEmbeddingHit, VaultEmbeddingRecord, VaultLinkContextRow, VaultRelationshipContextRow,
    VaultTaskContextRow,
};

type TranscriptExtractionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<crate::sidecar::ExtractedMemory>>> + Send + 'a>>;

const BROKER_SEMANTIC_THRESHOLD: f32 = crate::memory::EMBEDDING_SIMILARITY_THRESHOLD;
const BROKER_SEARCH_HIT_LIMIT: usize = 3;
const BROKER_SKILL_SUMMARY_LIMIT: usize = 8;
const BROKER_LINEAGE_CONTEXT_LIMIT: usize = 3;
const BROKER_LINEAGE_TAG: &str = "broker-lineage";
const BROKER_CHECKPOINT_TAG: &str = "broker-checkpoint";
#[cfg(feature = "duckdb-storage")]
const BROKER_DUCKDB_PATH_ENV: &str = "JCODE_BROKER_DUCKDB_PATH";
#[cfg(feature = "duckdb-storage")]
const BROKER_VAULT_EMBEDDING_MODEL_ENV: &str = "JCODE_BROKER_VAULT_EMBEDDING_MODEL";

trait TranscriptMemoryExtractor {
    fn extract<'a>(
        &'a self,
        transcript: &'a str,
        existing: &'a [String],
    ) -> TranscriptExtractionFuture<'a>;
}

struct SidecarTranscriptMemoryExtractor;

impl TranscriptMemoryExtractor for SidecarTranscriptMemoryExtractor {
    fn extract<'a>(
        &'a self,
        transcript: &'a str,
        existing: &'a [String],
    ) -> TranscriptExtractionFuture<'a> {
        Box::pin(async move {
            crate::sidecar::Sidecar::new()
                .extract_memories_with_existing(transcript, existing)
                .await
        })
    }
}

pub(super) async fn handle_broker_turn_sync(
    id: u64,
    requested_session_id: Option<String>,
    user_content: String,
    assistant_content: String,
    source: Option<String>,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let event = match broker_turn_sync_event(
        id,
        requested_session_id,
        &user_content,
        &assistant_content,
        source.as_deref(),
        fallback_session_id,
        sessions,
    )
    .await
    {
        Ok(event) => event,
        Err(error) => ServerEvent::Error {
            id,
            message: error.to_string(),
            retry_after_secs: None,
        },
    };

    let _ = client_event_tx.send(event);
}

pub(super) async fn handle_broker_transcript_sync(
    id: u64,
    requested_session_id: Option<String>,
    transcript: String,
    source: Option<String>,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let event = match broker_transcript_sync_event(
        id,
        requested_session_id,
        &transcript,
        source.as_deref(),
        fallback_session_id,
        sessions,
    )
    .await
    {
        Ok(event) => event,
        Err(error) => ServerEvent::Error {
            id,
            message: error.to_string(),
            retry_after_secs: None,
        },
    };

    let _ = client_event_tx.send(event);
}

pub(super) async fn handle_broker_vault_refresh(
    id: u64,
    vault: String,
    embed_missing: bool,
    embedding_model: String,
    embedding_limit: usize,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let event = match tokio::task::spawn_blocking(move || {
        broker_vault_refresh_event(id, vault, embed_missing, embedding_model, embedding_limit)
    })
    .await
    {
        Ok(Ok(event)) => event,
        Ok(Err(error)) => ServerEvent::Error {
            id,
            message: format!("{error:#}"),
            retry_after_secs: None,
        },
        Err(error) => ServerEvent::Error {
            id,
            message: format!("broker Vault refresh task failed: {error}"),
            retry_after_secs: None,
        },
    };

    let _ = client_event_tx.send(event);
}

pub(super) async fn handle_broker_context(
    id: u64,
    requested_session_id: Option<String>,
    query: Option<String>,
    limit: usize,
    include_provenance: bool,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let event = match broker_context_event(
        id,
        requested_session_id,
        query.as_deref(),
        limit,
        include_provenance,
        fallback_session_id,
        sessions,
    )
    .await
    {
        Ok(event) => event,
        Err(error) => ServerEvent::Error {
            id,
            message: error.to_string(),
            retry_after_secs: None,
        },
    };

    let _ = client_event_tx.send(event);
}

#[cfg(feature = "duckdb-storage")]
fn broker_vault_refresh_event(
    id: u64,
    vault: String,
    embed_missing: bool,
    embedding_model: String,
    embedding_limit: usize,
) -> Result<ServerEvent> {
    let vault = vault.trim();
    if vault.is_empty() {
        anyhow::bail!("broker Vault refresh requires a vault path");
    }
    let vault_path = PathBuf::from(vault);
    if !vault_path.is_dir() {
        anyhow::bail!(
            "broker Vault refresh path is not a directory: {}",
            vault_path.display()
        );
    }

    let db_path = broker_duckdb_path();
    let client = broker_duckdb_store_client(db_path.clone())?;
    let report = client.reconcile_vault_path(&vault_path)?;
    let mut embedded_chunks = 0;
    let counts = if embed_missing {
        embedded_chunks =
            backfill_missing_vault_chunk_embeddings(&client, &embedding_model, embedding_limit)?;
        client.table_counts()?
    } else {
        report.counts.clone()
    };

    Ok(ServerEvent::BrokerVaultRefreshed {
        id,
        vault: vault_path.to_string_lossy().to_string(),
        db: Some(db_path.to_string_lossy().to_string()),
        new_files: report.new_files,
        updated_files: report.updated_files,
        unchanged_files: report.unchanged_files,
        tombstoned_files: report.tombstoned_files,
        renamed_files: report.renamed_files.len(),
        embedded_chunks,
        counts: broker_vault_refresh_counts(&counts),
    })
}

#[cfg(not(feature = "duckdb-storage"))]
fn broker_vault_refresh_event(
    _id: u64,
    _vault: String,
    _embed_missing: bool,
    _embedding_model: String,
    _embedding_limit: usize,
) -> Result<ServerEvent> {
    anyhow::bail!("broker Vault refresh requires the duckdb-storage feature");
}

#[cfg(feature = "duckdb-storage")]
fn broker_duckdb_path() -> PathBuf {
    std::env::var_os(BROKER_DUCKDB_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| jcode_storage::runtime_dir().join("jcode-broker.duckdb"))
}

#[cfg(feature = "duckdb-storage")]
fn backfill_missing_vault_chunk_embeddings(
    client: &DuckDbBrokerStoreClient,
    embedding_model: &str,
    embedding_limit: usize,
) -> Result<usize> {
    let candidates =
        client.list_missing_vault_chunk_embeddings(embedding_model, embedding_limit)?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut records = Vec::with_capacity(candidates.len());
    for candidate in candidates {
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
        records.push(VaultEmbeddingRecord {
            id: format!("vault_embedding:{embedding_model}:{}", candidate.id),
            record_id: candidate.id,
            record_kind: "vault_chunk".to_string(),
            embedding_model: embedding_model.to_string(),
            embedding,
            content_checksum: candidate.checksum,
            source_checksum: candidate.source_checksum,
            updated_at: now.clone(),
            deleted_at: None,
        });
    }

    let embedded_chunks = records.len();
    client.upsert_vault_embeddings(records)?;
    Ok(embedded_chunks)
}

#[cfg(feature = "duckdb-storage")]
fn broker_vault_refresh_counts(counts: &BrokerStoreCounts) -> BrokerVaultRefreshCounts {
    BrokerVaultRefreshCounts {
        active_vault_file: counts.active_vault_file,
        active_vault_chunk: counts.active_vault_chunk,
        active_vault_embedding: counts.active_vault_embedding,
        active_vault_task: counts.active_vault_task,
        active_graph_edge: counts.active_graph_edge,
    }
}

async fn broker_turn_sync_event(
    id: u64,
    requested_session_id: Option<String>,
    user_content: &str,
    assistant_content: &str,
    source: Option<&str>,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
) -> Result<ServerEvent> {
    if user_content.trim().is_empty() && assistant_content.trim().is_empty() {
        anyhow::bail!("broker turn sync requires user_content or assistant_content");
    }

    let session_id = requested_session_id
        .or_else(|| fallback_session_id.map(str::to_string))
        .context("broker turn sync requires a session_id")?;

    let agent = {
        let sessions_guard = sessions.read().await;
        sessions_guard
            .get(&session_id)
            .cloned()
            .with_context(|| format!("session not found: {session_id}"))?
    };

    let working_dir = {
        let agent_guard = agent.lock().await;
        agent_guard.working_dir().map(str::to_string)
    };

    let source = normalized_source(source, "hermes");
    let content = format!(
        "External turn synced from {source}.\n\nUser: {}\n\nAssistant: {}",
        user_content.trim(),
        assistant_content.trim()
    );
    let manager = manager_for_working_dir(working_dir.as_deref());
    let memory_id = store_provenance_memory(
        &manager,
        &session_id,
        source,
        content,
        vec!["broker-turn-sync".to_string(), format!("{source}-turn")],
    )?;

    Ok(ServerEvent::BrokerTurnSynced {
        id,
        session_id,
        memory_ids: vec![memory_id.clone()],
        provenance_memory_ids: vec![memory_id],
        derived_memory_ids: Vec::new(),
        extraction_status: BrokerMemoryExtractionStatus::StoredProvenance,
    })
}

async fn broker_transcript_sync_event(
    id: u64,
    requested_session_id: Option<String>,
    transcript: &str,
    source: Option<&str>,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
) -> Result<ServerEvent> {
    if transcript.trim().is_empty() {
        anyhow::bail!("broker transcript sync requires transcript content");
    }

    let session_id = requested_session_id
        .or_else(|| fallback_session_id.map(str::to_string))
        .context("broker transcript sync requires a session_id")?;

    let agent = {
        let sessions_guard = sessions.read().await;
        sessions_guard
            .get(&session_id)
            .cloned()
            .with_context(|| format!("session not found: {session_id}"))?
    };

    let working_dir = {
        let agent_guard = agent.lock().await;
        agent_guard.working_dir().map(str::to_string)
    };

    let manager = manager_for_working_dir(working_dir.as_deref());
    let source = normalized_source(source, "hermes:transcript");
    let extractor = SidecarTranscriptMemoryExtractor;
    broker_transcript_sync_event_for_manager_with_gate(
        id,
        session_id,
        &manager,
        transcript,
        source,
        working_dir.as_deref(),
        crate::memory::memory_sidecar_enabled(),
        &extractor,
    )
    .await
}

#[cfg(test)]
async fn broker_transcript_sync_event_for_manager<E>(
    id: u64,
    session_id: String,
    manager: &MemoryManager,
    transcript: &str,
    source: &str,
    extractor: &E,
) -> Result<ServerEvent>
where
    E: TranscriptMemoryExtractor + ?Sized,
{
    broker_transcript_sync_event_for_manager_with_gate(
        id, session_id, manager, transcript, source, None, true, extractor,
    )
    .await
}

async fn broker_transcript_sync_event_for_manager_with_gate<E>(
    id: u64,
    session_id: String,
    manager: &MemoryManager,
    transcript: &str,
    source: &str,
    working_dir: Option<&str>,
    extraction_enabled: bool,
    extractor: &E,
) -> Result<ServerEvent>
where
    E: TranscriptMemoryExtractor + ?Sized,
{
    if transcript.trim().is_empty() {
        anyhow::bail!("broker transcript sync requires transcript content");
    }

    let content = format!(
        "External transcript synced from {source}.\n\n{}",
        transcript.trim()
    );
    let provenance_id = store_provenance_memory(
        &manager,
        &session_id,
        source,
        content,
        vec![
            "broker-transcript-sync".to_string(),
            "hermes-transcript".to_string(),
        ],
    )?;
    let lineage_checkpoint_id = store_lineage_checkpoint_memory(
        manager,
        &session_id,
        source,
        working_dir,
        transcript,
        &provenance_id,
    )?;

    let (derived_memory_ids, extraction_status) = extract_derived_memories(
        manager,
        transcript,
        &session_id,
        source,
        &provenance_id,
        extraction_enabled,
        extractor,
    )
    .await?;
    let mut memory_ids = vec![provenance_id.clone()];
    if let Some(lineage_checkpoint_id) = lineage_checkpoint_id {
        memory_ids.push(lineage_checkpoint_id);
    }
    memory_ids.extend(derived_memory_ids.iter().cloned());

    Ok(ServerEvent::BrokerTranscriptSynced {
        id,
        session_id,
        memory_ids,
        provenance_memory_ids: vec![provenance_id],
        derived_memory_ids,
        extraction_status,
    })
}

async fn broker_context_event(
    id: u64,
    requested_session_id: Option<String>,
    query: Option<&str>,
    limit: usize,
    include_provenance: bool,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
) -> Result<ServerEvent> {
    let session_id = requested_session_id
        .or_else(|| fallback_session_id.map(str::to_string))
        .context("broker context requires a session_id")?;

    let agent = {
        let sessions_guard = sessions.read().await;
        sessions_guard
            .get(&session_id)
            .cloned()
            .with_context(|| format!("session not found: {session_id}"))?
    };

    let (working_dir, mut tool_names, session_snapshot) = {
        let agent_guard = agent.lock().await;
        (
            agent_guard.working_dir().map(str::to_string),
            agent_guard.tool_names().await,
            agent_guard.session_snapshot(),
        )
    };
    tool_names.sort();

    #[cfg(feature = "duckdb-storage")]
    let relationship_query = vault_relationship_query_requested(query);
    #[cfg(not(feature = "duckdb-storage"))]
    let relationship_query = false;

    let memory_results = if relationship_query {
        Vec::new()
    } else {
        let memory_working_dir = working_dir.clone();
        let memory_query = query.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            collect_broker_memory_results(
                memory_working_dir.as_deref(),
                memory_query.as_deref(),
                limit,
                include_provenance,
            )
        })
        .await
        .context("broker memory context task failed")??
    };
    let memories: Vec<BrokerMemoryContextItem> = memory_results
        .iter()
        .map(|result| result.memory.clone())
        .collect();
    let side_panel = crate::side_panel::snapshot_for_session(&session_id).unwrap_or_default();
    let todos = crate::todo::load_todos(&session_id).unwrap_or_default();
    let skill_summaries = collect_skill_context_summaries(working_dir.as_deref(), query);
    let session_search_hits =
        collect_session_search_hits(&session_id, working_dir.as_deref(), query, limit)?;
    let conversation_search_hits =
        collect_conversation_search_hits(&session_snapshot, query, limit);
    let context_items = collect_context_items(
        &session_id,
        working_dir.as_deref(),
        &tool_names,
        &memory_results,
        &side_panel,
        &todos,
        &skill_summaries,
        &session_search_hits,
        &conversation_search_hits,
    );
    let vault_working_dir = working_dir.clone();
    let vault_query = query.map(str::to_string);
    let vault_items = tokio::task::spawn_blocking(move || {
        collect_vault_context_items(vault_working_dir.as_deref(), vault_query.as_deref(), limit)
    })
    .await
    .context("broker Vault context task failed")??;
    let items = broker_context_items_with_vault_priority(context_items, vault_items);
    let packet = clio_context_packet_from_items(&items);
    let packet = if clio_context_packet_is_empty(&packet) {
        None
    } else {
        Some(packet)
    };

    Ok(ServerEvent::BrokerContext {
        id,
        session_id,
        working_dir,
        tool_names,
        items,
        memories,
        side_panel,
        packet,
    })
}

fn manager_for_working_dir(working_dir: Option<&str>) -> MemoryManager {
    match working_dir {
        Some(dir) if !dir.trim().is_empty() => {
            MemoryManager::new().with_project_dir(PathBuf::from(dir))
        }
        _ => MemoryManager::new(),
    }
}

fn normalized_source<'a>(source: Option<&'a str>, default: &'a str) -> &'a str {
    source
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
}

fn store_provenance_memory(
    manager: &MemoryManager,
    session_id: &str,
    source: &str,
    content: String,
    mut tags: Vec<String>,
) -> Result<String> {
    tags.push("broker-provenance".to_string());
    tags.sort();
    tags.dedup();

    let entry = MemoryEntry::new(MemoryCategory::Custom("provenance".to_string()), content)
        .with_source(format!("{source}:{session_id}"))
        .with_tags(tags);
    manager.remember_project(entry)
}

fn store_lineage_checkpoint_memory(
    manager: &MemoryManager,
    session_id: &str,
    source: &str,
    working_dir: Option<&str>,
    transcript: &str,
    provenance_id: &str,
) -> Result<Option<String>> {
    let Some(checkpoint_kind) = checkpoint_kind_from_source(source) else {
        return Ok(None);
    };

    let logical_super_session_id = logical_super_session_id(working_dir, session_id);
    let title = checkpoint_title(checkpoint_kind);
    let summary = compact_transcript_checkpoint(transcript);
    let content = format!(
        "{title}\n\
         Logical super-session: {logical_super_session_id}\n\
         Session segment: {session_id}\n\
         Source: {source}\n\
         Provenance memory: {provenance_id}\n\n\
         Checkpoint summary:\n{summary}"
    );
    let entry = MemoryEntry::new(MemoryCategory::Custom("checkpoint".to_string()), content)
        .with_source(format!("broker-lineage:{source}:{session_id}"))
        .with_tags(vec![
            BROKER_LINEAGE_TAG.to_string(),
            BROKER_CHECKPOINT_TAG.to_string(),
            format!("checkpoint-kind:{checkpoint_kind}"),
            format!("logical-super-session:{logical_super_session_id}"),
            format!("session-segment:{session_id}"),
            format!("derived-from:{provenance_id}"),
        ])
        .with_trust(TrustLevel::Medium);
    let checkpoint_id = manager.remember_project(entry)?;
    link_derived_memories(manager, provenance_id, std::slice::from_ref(&checkpoint_id))?;
    Ok(Some(checkpoint_id))
}

fn checkpoint_kind_from_source(source: &str) -> Option<&'static str> {
    match source {
        "hermes:pre_compress" => Some("compression"),
        "hermes:session_end" => Some("session_end"),
        _ => None,
    }
}

fn checkpoint_title(checkpoint_kind: &str) -> &'static str {
    match checkpoint_kind {
        "compression" => "Hermes compression checkpoint",
        "session_end" => "Hermes session-end checkpoint",
        _ => "Hermes checkpoint",
    }
}

fn logical_super_session_id(working_dir: Option<&str>, session_id: &str) -> String {
    if working_dir
        .map(|dir| dir.contains("Hermes-Honcho-LangGraph-Second-Brain"))
        .unwrap_or(false)
    {
        return "clio-super-session".to_string();
    }

    let seed = working_dir
        .map(str::trim)
        .filter(|dir| !dir.is_empty())
        .unwrap_or(session_id);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    use std::hash::{Hash, Hasher};
    seed.hash(&mut hasher);
    format!("jcode-super-session-{:016x}", hasher.finish())
}

fn compact_transcript_checkpoint(transcript: &str) -> String {
    let mut lines: Vec<String> = transcript
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| checkpoint_line_has_artifact_signal(line))
        .map(|line| line.chars().take(220).collect::<String>())
        .collect();
    if lines.is_empty() {
        return "No artifact-trail lines detected; full transcript remains hidden behind provenance memory.".to_string();
    }
    if lines.len() > 8 {
        lines = lines.split_off(lines.len() - 8);
    }
    let joined = lines.join("\n");
    if joined.chars().count() <= 1_200 {
        joined
    } else {
        joined.chars().take(1_200).collect()
    }
}

fn checkpoint_line_has_artifact_signal(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.contains("next action")
        || lower.contains("decision")
        || lower.contains("decided")
        || lower.contains("failure")
        || lower.contains("failed")
        || lower.contains("error")
        || lower.contains("blocked")
        || lower.contains("commit")
        || lower.contains("deployed")
        || lower.contains("modified")
        || lower.contains("created")
        || lower.contains("updated")
        || lower.contains("verified")
        || lower.contains("passed")
        || lower.contains("risk")
        || line.contains("scripts/")
        || line.contains("Cargo.toml")
        || line.contains(".md")
        || line.contains(".rs")
        || line.contains(".py")
        || line.trim_start().starts_with('$')
}

async fn extract_derived_memories(
    manager: &MemoryManager,
    transcript: &str,
    session_id: &str,
    source: &str,
    provenance_id: &str,
    extraction_enabled: bool,
    extractor: &(impl TranscriptMemoryExtractor + ?Sized),
) -> Result<(Vec<String>, BrokerMemoryExtractionStatus)> {
    if !extraction_enabled {
        return Ok((
            Vec::new(),
            BrokerMemoryExtractionStatus::SkippedSidecarDisabled,
        ));
    }

    let existing: Vec<String> = manager
        .list_all()
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.active && !is_provenance_memory(entry) && !is_lineage_memory(entry))
        .map(|entry| entry.content)
        .collect();

    let extracted = match extractor.extract(transcript, &existing).await {
        Ok(extracted) => extracted,
        Err(error) => {
            crate::logging::info(&format!("Broker transcript extraction failed: {error}"));
            return Ok((Vec::new(), BrokerMemoryExtractionStatus::Failed));
        }
    };

    let derived_ids =
        store_derived_memories(manager, extracted, session_id, source, provenance_id)?;
    let status = if derived_ids.is_empty() {
        BrokerMemoryExtractionStatus::StoredProvenance
    } else {
        BrokerMemoryExtractionStatus::Extracted
    };
    Ok((derived_ids, status))
}

fn store_derived_memories(
    manager: &MemoryManager,
    extracted: Vec<crate::sidecar::ExtractedMemory>,
    session_id: &str,
    source: &str,
    provenance_id: &str,
) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for memory in extracted {
        let category = MemoryCategory::from_extracted(&memory.category);
        let trust = match memory.trust.as_str() {
            "high" => TrustLevel::High,
            "low" => TrustLevel::Low,
            _ => TrustLevel::Medium,
        };
        let entry = MemoryEntry::new(category, memory.content)
            .with_source(format!("derived:{source}:{session_id}"))
            .with_tags(vec![
                "broker-derived".to_string(),
                format!("derived-from:{provenance_id}"),
            ])
            .with_trust(trust);
        if let Some(existing_id) = find_exact_duplicate_derived_memory(manager, &entry)? {
            reinforce_memory(manager, &existing_id, session_id)?;
            ids.push(existing_id);
        } else {
            ids.push(manager.remember_project(entry)?);
        }
    }

    link_derived_memories(manager, provenance_id, &ids)?;
    Ok(ids)
}

fn find_exact_duplicate_derived_memory(
    manager: &MemoryManager,
    entry: &MemoryEntry,
) -> Result<Option<String>> {
    let target_content = crate::memory_types::normalize_memory_search_text(&entry.content, &[]);
    for existing in manager.load_project_graph()?.active_memories() {
        if is_provenance_memory(existing) || existing.category != entry.category {
            continue;
        }
        let existing_content =
            crate::memory_types::normalize_memory_search_text(&existing.content, &[]);
        if existing_content == target_content {
            return Ok(Some(existing.id.clone()));
        }
    }
    Ok(None)
}

fn reinforce_memory(manager: &MemoryManager, memory_id: &str, session_id: &str) -> Result<bool> {
    let mut project_graph = manager.load_project_graph()?;
    if let Some(entry) = project_graph.get_memory_mut(memory_id) {
        entry.reinforce(session_id, 0);
        manager.save_project_graph(&project_graph)?;
        return Ok(true);
    }

    let mut global_graph = manager.load_global_graph()?;
    if let Some(entry) = global_graph.get_memory_mut(memory_id) {
        entry.reinforce(session_id, 0);
        manager.save_global_graph(&global_graph)?;
        return Ok(true);
    }

    Ok(false)
}

fn link_derived_memories(
    manager: &MemoryManager,
    provenance_id: &str,
    derived_ids: &[String],
) -> Result<()> {
    if derived_ids.is_empty() {
        return Ok(());
    }

    let mut graph = manager.load_project_graph()?;
    if !graph.memories.contains_key(provenance_id) {
        return Ok(());
    }
    let mut changed = false;
    for derived_id in derived_ids {
        if graph.memories.contains_key(derived_id) {
            graph.add_edge(derived_id, provenance_id, EdgeKind::DerivedFrom);
            changed = true;
        }
    }
    if changed {
        manager.save_project_graph(&graph)?;
    }
    Ok(())
}

#[cfg(test)]
fn collect_broker_memories(
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
    include_provenance: bool,
) -> Result<Vec<BrokerMemoryContextItem>> {
    collect_broker_memory_results(working_dir, query, limit, include_provenance)
        .map(|results| results.into_iter().map(|result| result.memory).collect())
}

#[derive(Debug, Clone)]
struct BrokerMemoryResult {
    memory: BrokerMemoryContextItem,
    relevance: Option<BrokerContextRelevance>,
}

#[derive(Debug)]
struct BrokerMemorySearchHit {
    entry: MemoryEntry,
    score: Option<f32>,
    retrieval_mode: Option<&'static str>,
}

#[derive(Debug, Clone)]
struct BrokerSkillContextSummary {
    name: String,
    description: String,
    scope: String,
    source: String,
    path: Option<String>,
    allowed_tools: Vec<String>,
}

#[derive(Debug, Clone)]
struct BrokerSearchHitContext {
    id: String,
    title: String,
    summary: String,
    content: String,
    snippet: String,
    session_id: String,
    working_dir: Option<String>,
    provider_key: Option<String>,
    model: Option<String>,
    message_id: Option<String>,
    message_index: Option<usize>,
    role: Option<String>,
    timestamp: Option<String>,
    updated_at: Option<String>,
    query: Option<String>,
    score: Option<f32>,
    rank: Option<usize>,
    matched_terms: Vec<String>,
    metadata: serde_json::Value,
}

fn collect_broker_memory_results(
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
    include_provenance: bool,
) -> Result<Vec<BrokerMemoryResult>> {
    collect_broker_memory_results_impl(working_dir, query, None, limit, include_provenance)
}

#[cfg(test)]
fn collect_broker_memory_results_with_query_embedding(
    working_dir: Option<&str>,
    query: &str,
    query_embedding: &[f32],
    limit: usize,
    include_provenance: bool,
) -> Result<Vec<BrokerMemoryResult>> {
    collect_broker_memory_results_impl(
        working_dir,
        Some(query),
        Some(query_embedding),
        limit,
        include_provenance,
    )
}

fn collect_broker_memory_results_impl(
    working_dir: Option<&str>,
    query: Option<&str>,
    query_embedding: Option<&[f32]>,
    limit: usize,
    include_provenance: bool,
) -> Result<Vec<BrokerMemoryResult>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let manager = match working_dir {
        Some(dir) => MemoryManager::new().with_project_dir(PathBuf::from(dir)),
        None => MemoryManager::new(),
    };

    let mut seen = HashSet::new();
    let mut memories = Vec::new();

    if working_dir.is_some() {
        append_recent_lineage_results(
            &manager,
            MemoryScope::Project,
            "project",
            query,
            limit,
            &mut seen,
            &mut memories,
        )?;
        append_scoped_memory_results(
            &manager,
            MemoryScope::Project,
            "project",
            query,
            query_embedding,
            limit,
            include_provenance,
            &mut seen,
            &mut memories,
        )?;
    }

    append_recent_lineage_results(
        &manager,
        MemoryScope::Global,
        "global",
        query,
        limit,
        &mut seen,
        &mut memories,
    )?;
    append_scoped_memory_results(
        &manager,
        MemoryScope::Global,
        "global",
        query,
        query_embedding,
        limit,
        include_provenance,
        &mut seen,
        &mut memories,
    )?;

    memories.truncate(limit);
    Ok(memories)
}

fn append_recent_lineage_results(
    manager: &MemoryManager,
    scope: MemoryScope,
    scope_label: &str,
    query: Option<&str>,
    limit: usize,
    seen: &mut HashSet<String>,
    memories: &mut Vec<BrokerMemoryResult>,
) -> Result<()> {
    if memories.len() >= limit {
        return Ok(());
    }

    let mut entries: Vec<MemoryEntry> = manager
        .list_all_scoped(scope)?
        .into_iter()
        .filter(|entry| entry.active && !is_provenance_memory(entry) && is_lineage_memory(entry))
        .collect();
    sort_lineage_entries(&mut entries, query);

    for entry in entries.into_iter().take(BROKER_LINEAGE_CONTEXT_LIMIT) {
        if memories.len() >= limit {
            break;
        }
        if !seen.insert(entry.id.clone()) {
            continue;
        }
        let rank = memories.len() + 1;
        memories.push(BrokerMemoryResult {
            memory: memory_context_item(entry, scope_label),
            relevance: Some(BrokerContextRelevance {
                query: None,
                retrieval_mode: Some("lineage_checkpoint".to_string()),
                score: None,
                rank: Some(rank),
                matched_terms: vec!["lineage".to_string(), "checkpoint".to_string()],
                exact_match: Some(false),
            }),
        });
    }

    Ok(())
}

fn append_scoped_memory_results(
    manager: &MemoryManager,
    scope: MemoryScope,
    scope_label: &str,
    query: Option<&str>,
    query_embedding: Option<&[f32]>,
    limit: usize,
    include_provenance: bool,
    seen: &mut HashSet<String>,
    memories: &mut Vec<BrokerMemoryResult>,
) -> Result<()> {
    let hits = scoped_memory_search_hits(manager, scope, query, query_embedding, limit)?;

    for hit in hits {
        if memories.len() >= limit {
            break;
        }
        let entry = hit.entry;
        if !seen.insert(entry.id.clone()) {
            continue;
        }
        if !include_provenance && is_provenance_memory(&entry) {
            continue;
        }
        let rank = memories.len() + 1;
        let relevance = memory_relevance(query, &entry, hit.retrieval_mode, hit.score, rank);
        memories.push(BrokerMemoryResult {
            memory: memory_context_item(entry, scope_label),
            relevance,
        });
    }

    Ok(())
}

fn scoped_memory_search_hits(
    manager: &MemoryManager,
    scope: MemoryScope,
    query: Option<&str>,
    query_embedding: Option<&[f32]>,
    limit: usize,
) -> Result<Vec<BrokerMemorySearchHit>> {
    let query = query.map(str::trim).filter(|query| !query.is_empty());
    if let Some(query) = query {
        let semantic_hits = semantic_cascade_hits(manager, scope, query, query_embedding, limit)?;
        if !semantic_hits.is_empty() {
            return Ok(semantic_hits);
        }

        let mut entries = manager.search_scoped(query, scope)?;
        sort_entries_by_updated_at(&mut entries);
        return Ok(entries
            .into_iter()
            .map(|entry| BrokerMemorySearchHit {
                entry,
                score: None,
                retrieval_mode: Some("keyword"),
            })
            .collect());
    }

    let mut entries = manager.list_all_scoped(scope)?;
    sort_entries_by_updated_at(&mut entries);
    Ok(entries
        .into_iter()
        .map(|entry| BrokerMemorySearchHit {
            entry,
            score: None,
            retrieval_mode: None,
        })
        .collect())
}

fn semantic_cascade_hits(
    manager: &MemoryManager,
    scope: MemoryScope,
    query: &str,
    query_embedding: Option<&[f32]>,
    limit: usize,
) -> Result<Vec<BrokerMemorySearchHit>> {
    let hits = match query_embedding {
        Some(embedding) => find_semantic_cascade_with_embedding(manager, embedding, limit, scope)?,
        None => manager.find_similar_with_cascade_scoped(
            query,
            BROKER_SEMANTIC_THRESHOLD,
            limit,
            scope,
        )?,
    };

    Ok(hits
        .into_iter()
        .map(|(entry, score)| BrokerMemorySearchHit {
            entry,
            score: Some(score),
            retrieval_mode: Some("semantic_cascade"),
        })
        .collect())
}

fn find_semantic_cascade_with_embedding(
    manager: &MemoryManager,
    query_embedding: &[f32],
    limit: usize,
    scope: MemoryScope,
) -> Result<Vec<(MemoryEntry, f32)>> {
    let embedding_hits = manager.find_similar_with_embedding_scoped(
        query_embedding,
        BROKER_SEMANTIC_THRESHOLD,
        limit,
        scope,
    )?;
    if embedding_hits.is_empty() {
        return Ok(Vec::new());
    }

    let seed_ids: Vec<String> = embedding_hits
        .iter()
        .map(|(entry, _)| entry.id.clone())
        .collect();
    let seed_scores: Vec<f32> = embedding_hits.iter().map(|(_, score)| *score).collect();
    let mut merged: HashMap<String, f32> = embedding_hits
        .iter()
        .map(|(entry, score)| (entry.id.clone(), *score))
        .collect();
    let mut project_graph = if scope.includes_project() {
        Some(manager.load_project_graph()?)
    } else {
        None
    };
    let mut global_graph = if scope.includes_global() {
        Some(manager.load_global_graph()?)
    } else {
        None
    };

    if let Some(graph) = project_graph.as_mut() {
        merge_cascade_scores(
            &mut merged,
            graph.cascade_retrieve(&seed_ids, &seed_scores, 2, limit * 2),
        );
    }
    if let Some(graph) = global_graph.as_mut() {
        merge_cascade_scores(
            &mut merged,
            graph.cascade_retrieve(&seed_ids, &seed_scores, 2, limit * 2),
        );
    }

    let mut scored: Vec<(MemoryEntry, f32)> = merged
        .into_iter()
        .filter_map(|(id, score)| {
            project_graph
                .as_ref()
                .and_then(|graph| graph.get_memory(&id))
                .or_else(|| {
                    global_graph
                        .as_ref()
                        .and_then(|graph| graph.get_memory(&id))
                })
                .cloned()
                .map(|entry| (entry, score))
        })
        .collect();
    scored.sort_by(|(a_entry, a_score), (b_entry, b_score)| {
        b_score
            .total_cmp(a_score)
            .then_with(|| a_entry.id.cmp(&b_entry.id))
    });
    scored.truncate(limit);
    Ok(scored)
}

fn merge_cascade_scores(merged: &mut HashMap<String, f32>, scores: Vec<(String, f32)>) {
    for (id, score) in scores {
        let existing = merged.get(&id).copied().unwrap_or(0.0);
        if score > existing {
            merged.insert(id, score);
        }
    }
}

fn sort_entries_by_updated_at(entries: &mut [MemoryEntry]) {
    entries.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn sort_lineage_entries(entries: &mut [MemoryEntry], query: Option<&str>) {
    entries.sort_by(|a, b| {
        lineage_entry_quality_score(b, query)
            .cmp(&lineage_entry_quality_score(a, query))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn lineage_entry_quality_score(entry: &MemoryEntry, query: Option<&str>) -> i32 {
    let text = format!(
        "{} {} {:?}",
        entry.content.to_ascii_lowercase(),
        entry
            .source
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase(),
        entry.tags
    );
    let mut score = 0;

    if text.contains("next action") {
        score += 50;
    }
    if text.contains("decision:") {
        score += 20;
    }
    if text.contains("verified") || text.contains("verification") {
        score += 10;
    }
    if text.contains("command") || text.contains("cargo ") || text.contains("/users/") {
        score += 5;
    }
    if text.contains("no artifact-trail lines detected") {
        score -= 50;
    }

    if let Some(query) = query {
        for term in query
            .split(|character: char| !character.is_ascii_alphanumeric())
            .filter(|term| term.len() >= 4)
        {
            if text.contains(&term.to_ascii_lowercase()) {
                score += 1;
            }
        }
    }

    score
}

fn is_provenance_memory(entry: &MemoryEntry) -> bool {
    entry.tags.iter().any(|tag| tag == "broker-provenance")
        || matches!(&entry.category, MemoryCategory::Custom(category) if category == "provenance")
}

fn is_lineage_memory(entry: &MemoryEntry) -> bool {
    entry.tags.iter().any(|tag| tag == BROKER_LINEAGE_TAG)
        || matches!(&entry.category, MemoryCategory::Custom(category) if category == "checkpoint")
}

fn tag_value(tags: &[String], prefix: &str) -> Option<String> {
    tags.iter()
        .find_map(|tag| tag.strip_prefix(prefix).map(str::to_string))
}

fn memory_context_item(entry: MemoryEntry, scope: &str) -> BrokerMemoryContextItem {
    BrokerMemoryContextItem {
        id: entry.id,
        category: entry.category.to_string(),
        scope: scope.to_string(),
        content: entry.content,
        tags: entry.tags,
        source: entry.source,
    }
}

fn collect_context_items(
    session_id: &str,
    working_dir: Option<&str>,
    tool_names: &[String],
    memories: &[BrokerMemoryResult],
    side_panel: &jcode_side_panel_types::SidePanelSnapshot,
    todos: &[TodoItem],
    skill_summaries: &[BrokerSkillContextSummary],
    session_search_hits: &[BrokerSearchHitContext],
    conversation_search_hits: &[BrokerSearchHitContext],
) -> Vec<BrokerContextItem> {
    let mut items = Vec::new();

    items.extend(
        tool_names
            .iter()
            .map(|tool_name| tool_broker_item(tool_name)),
    );
    items.extend(
        memories
            .iter()
            .map(|memory| memory_broker_item(memory, working_dir)),
    );
    items.extend(side_panel.pages.iter().map(|page| {
        side_panel_broker_item(session_id, page, side_panel.focused_page_id.as_deref())
    }));
    items.extend(todos.iter().map(|todo| todo_broker_item(session_id, todo)));
    items.extend(
        skill_summaries
            .iter()
            .map(|skill| skill_broker_item(skill, working_dir)),
    );
    items.extend(
        session_search_hits
            .iter()
            .map(session_search_hit_broker_item),
    );
    items.extend(
        conversation_search_hits
            .iter()
            .map(conversation_search_hit_broker_item),
    );

    items
}

fn broker_context_items_with_vault_priority(
    context_items: Vec<BrokerContextItem>,
    mut vault_items: Vec<BrokerContextItem>,
) -> Vec<BrokerContextItem> {
    vault_items.extend(context_items);
    vault_items
}

fn clio_context_packet_from_items(items: &[BrokerContextItem]) -> ClioContextPacketV1 {
    let mut packet = ClioContextPacketV1::default();

    for item in items {
        let packet_item = clio_context_packet_item(item);
        match packet_item.slot.as_deref().unwrap_or("vault_evidence") {
            "active_task" => packet.active_task.push(packet_item),
            "authority" => packet.authority.push(packet_item),
            "lineage" => packet.lineage.push(packet_item),
            "durable_memory" => packet.durable_memory.push(packet_item),
            "session_evidence" => packet.session_evidence.push(packet_item),
            "artifact_refs" => packet.artifact_refs.push(packet_item),
            "conflicts" => packet.conflicts.push(packet_item),
            "skill_hints" => packet.skill_hints.push(packet_item),
            "tool_hints" => packet.tool_hints.push(packet_item),
            _ => packet.vault_evidence.push(packet_item),
        }
    }

    packet
}

fn clio_context_packet_is_empty(packet: &ClioContextPacketV1) -> bool {
    packet.active_task.is_empty()
        && packet.authority.is_empty()
        && packet.lineage.is_empty()
        && packet.vault_evidence.is_empty()
        && packet.durable_memory.is_empty()
        && packet.session_evidence.is_empty()
        && packet.artifact_refs.is_empty()
        && packet.conflicts.is_empty()
        && packet.skill_hints.is_empty()
        && packet.tool_hints.is_empty()
}

fn clio_context_packet_item(item: &BrokerContextItem) -> ClioContextPacketItem {
    let source_uri = clio_source_uri(item);
    let source_path = clio_source_path(item, source_uri.as_deref());
    let authority_class = clio_authority_class(item, source_path.as_deref());
    let slot = clio_packet_slot(item, authority_class.as_str());
    let (line_start, line_end) = clio_line_span(item);
    let workflow_status =
        clio_workflow_status(item, authority_class.as_str(), source_path.as_deref());
    let why_included = clio_why_included(
        slot.as_str(),
        authority_class.as_str(),
        item.relevance.as_ref().and_then(|relevance| relevance.rank),
    );
    let conflict_group = if slot == "conflicts" {
        Some(clio_conflict_group(item, source_path.as_deref()))
    } else {
        None
    };

    ClioContextPacketItem {
        item: item.clone(),
        slot: Some(slot),
        source_uri,
        source_path,
        line_start,
        line_end,
        authority_class: Some(authority_class),
        workflow_status,
        why_included: Some(why_included),
        conflict_group,
    }
}

fn clio_source_uri(item: &BrokerContextItem) -> Option<String> {
    item.origin
        .uri
        .clone()
        .or_else(|| json_string_field(&item.metadata, "uri"))
        .or_else(|| {
            item.source
                .as_deref()
                .filter(|source| source.starts_with("vault://") || source.starts_with("file://"))
                .map(str::to_string)
        })
}

fn clio_source_path(item: &BrokerContextItem, source_uri: Option<&str>) -> Option<String> {
    item.origin
        .path
        .clone()
        .or_else(|| json_string_field(&item.metadata, "path"))
        .or_else(|| json_string_field(&item.metadata, "file_path"))
        .or_else(|| json_string_field(&item.metadata, "source_path"))
        .or_else(|| {
            source_uri.and_then(|uri| {
                uri.strip_prefix("vault://")
                    .map(|path| path.split('#').next().unwrap_or(path).to_string())
            })
        })
        .or_else(|| {
            item.source.as_deref().and_then(|source| {
                if source.starts_with('/') || source.contains(".md") {
                    Some(source.to_string())
                } else {
                    None
                }
            })
        })
}

fn clio_line_span(item: &BrokerContextItem) -> (Option<i64>, Option<i64>) {
    let start = json_i64_field(&item.metadata, "start_line")
        .or_else(|| json_i64_field(&item.metadata, "line_start"))
        .or_else(|| json_i64_field(&item.metadata, "line"));
    let end = json_i64_field(&item.metadata, "end_line")
        .or_else(|| json_i64_field(&item.metadata, "line_end"))
        .or_else(|| json_i64_field(&item.metadata, "line"));
    (start, end)
}

fn clio_authority_class(item: &BrokerContextItem, source_path: Option<&str>) -> String {
    let kind = item.kind.as_str();
    if matches!(kind, "tool" | "skill") {
        return "procedural_hint".to_string();
    }
    if kind == "memory" {
        return "durable_memory".to_string();
    }
    if matches!(kind, "session_search_hit" | "conversation_search_hit") {
        return "session_evidence".to_string();
    }
    if matches!(kind, "goal" | "todo") {
        return "active_task_note".to_string();
    }
    if kind == "side_panel" {
        return "artifact_ref".to_string();
    }
    if matches!(kind, "checkpoint" | "lineage" | "compression_checkpoint") {
        return "lineage_checkpoint".to_string();
    }

    let path = source_path.unwrap_or_default();
    if clio_metadata_workflow_status(item)
        .as_deref()
        .is_some_and(clio_status_is_historical)
    {
        return "historical_context".to_string();
    }
    if path.contains("/System/Clio/Core/") {
        return "clio_core".to_string();
    }
    if clio_path_is_current_project_authority(path) {
        return "current_project_authority".to_string();
    }
    if path.contains("TaskNotes/") {
        return "active_task_note".to_string();
    }
    if clio_path_is_historical(path) {
        return "historical_context".to_string();
    }
    if kind.starts_with("vault_") {
        return "source_evidence".to_string();
    }
    "broker_context".to_string()
}

fn clio_packet_slot(item: &BrokerContextItem, authority_class: &str) -> String {
    match item.kind.as_str() {
        "goal" | "todo" => "active_task".to_string(),
        "memory" => "durable_memory".to_string(),
        "session_search_hit" | "conversation_search_hit" => "session_evidence".to_string(),
        "side_panel" => "artifact_refs".to_string(),
        "skill" => "skill_hints".to_string(),
        "tool" => "tool_hints".to_string(),
        "checkpoint" | "lineage" | "compression_checkpoint" => "lineage".to_string(),
        "conflict" => "conflicts".to_string(),
        _ if matches!(
            authority_class,
            "current_project_authority" | "clio_core" | "active_task_note"
        ) =>
        {
            "authority".to_string()
        }
        _ if authority_class == "historical_context"
            && !clio_item_query_allows_historical_context(item) =>
        {
            "conflicts".to_string()
        }
        _ => "vault_evidence".to_string(),
    }
}

fn clio_workflow_status(
    item: &BrokerContextItem,
    authority_class: &str,
    source_path: Option<&str>,
) -> Option<String> {
    if authority_class == "historical_context" {
        return Some("historical".to_string());
    }
    if let Some(status) = clio_metadata_workflow_status(item) {
        return Some(status);
    }
    if matches!(
        authority_class,
        "current_project_authority" | "clio_core" | "active_task_note"
    ) {
        return Some("active".to_string());
    }
    source_path.and_then(|path| {
        if path.contains("Archive") {
            Some("archived".to_string())
        } else {
            None
        }
    })
}

fn clio_metadata_workflow_status(item: &BrokerContextItem) -> Option<String> {
    json_string_field(&item.metadata, "workflow_status")
        .or_else(|| json_string_field(&item.metadata, "status"))
        .or_else(|| json_frontmatter_string_field(&item.metadata, "workflow_status"))
        .or_else(|| json_frontmatter_string_field(&item.metadata, "status"))
}

fn json_frontmatter_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    let frontmatter = json_string_field(value, "frontmatter_json")?;
    serde_json::from_str::<serde_json::Value>(&frontmatter)
        .ok()
        .and_then(|metadata| json_string_field(&metadata, key))
}

fn clio_status_is_historical(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "historical" | "superseded" | "archived" | "archive" | "rollback" | "rejected"
    )
}

fn clio_path_is_current_project_authority(path: &str) -> bool {
    if clio_path_is_historical(path) {
        return false;
    }

    let basename = path.rsplit('/').next().unwrap_or(path);
    if matches!(
        basename,
        "CURRENT.md"
            | "Master-Second-Brain-Execution-Plan.md"
            | "Clio-Context-Engineering-Operating-Model.md"
            | "jcode-Nervous-System-Broker-Parity-Plan.md"
    ) {
        return true;
    }

    path.contains("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/")
}

fn clio_path_is_historical(path: &str) -> bool {
    let path_lower = path.to_ascii_lowercase();
    let active_clio_project = path_lower.contains("projects/hermes-honcho-langgraph-second-brain/");
    path_lower.contains("pre-super-session-adoption")
        || path_lower.contains("super-session-fork")
        || path_lower.contains("/archive/")
        || path_lower.starts_with("archive/")
        || path_lower.contains("/backup")
        || path_lower.contains("rollback")
        || (!active_clio_project
            && (path_lower.contains("openclaw") || path_lower.contains("honcho")))
}

fn clio_item_query_allows_historical_context(item: &BrokerContextItem) -> bool {
    item.relevance
        .as_ref()
        .and_then(|relevance| relevance.query.as_deref())
        .is_some_and(query_allows_historical_context)
}

fn query_allows_historical_context(query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    [
        "history",
        "historical",
        "rollback",
        "migration",
        "migrate",
        "provenance",
        "old",
        "older",
        "previous",
        "before",
        "legacy",
        "archive",
        "archived",
        "compare",
        "comparison",
    ]
    .iter()
    .any(|needle| query.contains(needle))
}

fn clio_conflict_group(item: &BrokerContextItem, source_path: Option<&str>) -> String {
    let path = source_path.unwrap_or_default().to_ascii_lowercase();
    let basename = path.rsplit('/').next().unwrap_or(&path);
    let mut text_haystack = String::new();
    for value in [
        item.title.as_deref(),
        item.summary.as_deref(),
        item.content.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        text_haystack.push('\n');
        text_haystack.push_str(&value.to_ascii_lowercase());
    }
    if text_haystack.contains("provider")
        || text_haystack.contains("honcho")
        || text_haystack.contains("surrealdb")
        || text_haystack.contains("jcode_graph")
        || text_haystack.contains("memory path")
        || basename.contains("honcho")
    {
        return "provider_currentness".to_string();
    }
    if path.contains("openclaw") || text_haystack.contains("project name") {
        return "project_name".to_string();
    }
    if text_haystack.contains("task")
        || text_haystack.contains("checklist")
        || text_haystack.contains("progress")
        || text_haystack.contains("status")
    {
        return "task_state".to_string();
    }
    "currentness".to_string()
}

fn clio_why_included(slot: &str, authority_class: &str, rank: Option<usize>) -> String {
    let rank = rank.map(|rank| format!(" rank {rank}")).unwrap_or_default();
    match slot {
        "authority" => format!("current authority context; class={authority_class}{rank}"),
        "conflicts" => format!("currentness/conflict note; class={authority_class}{rank}"),
        "active_task" => format!("active task state; class={authority_class}{rank}"),
        "lineage" => format!("super-session lineage evidence; class={authority_class}{rank}"),
        "durable_memory" => format!("durable memory evidence; class={authority_class}{rank}"),
        "session_evidence" => format!("session evidence; class={authority_class}{rank}"),
        "artifact_refs" => format!("artifact reference; class={authority_class}{rank}"),
        "skill_hints" => format!("procedural routing hint; class={authority_class}{rank}"),
        "tool_hints" => format!("compact broker tool hint; class={authority_class}{rank}"),
        _ => format!("source-backed evidence; class={authority_class}{rank}"),
    }
}

fn json_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|field| field.as_str())
        .filter(|field| !field.trim().is_empty())
        .map(str::to_string)
}

fn json_i64_field(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|field| field.as_i64())
}

#[cfg(feature = "duckdb-storage")]
struct BrokerDuckDbServiceState {
    path: PathBuf,
    service: DuckDbBrokerStoreService,
}

#[cfg(feature = "duckdb-storage")]
static BROKER_DUCKDB_SERVICE: OnceLock<StdMutex<Option<BrokerDuckDbServiceState>>> =
    OnceLock::new();

#[cfg(feature = "duckdb-storage")]
fn collect_vault_context_items(
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<BrokerContextItem>> {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return Ok(Vec::new());
    };
    let relationship_query = vault_relationship_query_requested(Some(query));
    let limit = if relationship_query {
        limit
    } else {
        broker_search_hit_limit(limit)
    };
    if limit == 0 {
        return Ok(Vec::new());
    }
    let Some(db_path) = std::env::var_os(BROKER_DUCKDB_PATH_ENV).map(PathBuf::from) else {
        return Ok(Vec::new());
    };

    let client = broker_duckdb_store_client(db_path)?;
    let query_embedding = if relationship_query {
        None
    } else {
        crate::embedding::embed(query).ok()
    };
    collect_vault_context_items_with_client(
        &client,
        working_dir,
        query,
        query_embedding.as_deref(),
        limit,
    )
}

#[cfg(feature = "duckdb-storage")]
fn collect_vault_context_items_with_client(
    client: &DuckDbBrokerStoreClient,
    working_dir: Option<&str>,
    query: &str,
    query_embedding: Option<&[f32]>,
    limit: usize,
) -> Result<Vec<BrokerContextItem>> {
    let mut hits = Vec::new();
    let mut seen_chunks = HashSet::new();
    let mut seen_relationships = HashSet::new();

    for path in vault_relationship_anchor_paths_from_query(query) {
        for row in client.query_vault_relationships_for_path(&path, limit)? {
            if seen_relationships.insert(row.id.clone()) {
                hits.push(VaultContextHit::Relationship(row));
            }
        }
    }

    for row in client.query_vault_chunks(query, limit)? {
        seen_chunks.insert(row.id.clone());
        hits.push(VaultContextHit::Chunk(row));
    }

    if let Some(query_embedding) = query_embedding {
        let embedding_model = broker_vault_embedding_model();
        for hit in
            client.query_vault_chunks_by_embedding(&embedding_model, query_embedding, limit)?
        {
            if hit.score < f64::from(BROKER_SEMANTIC_THRESHOLD) {
                continue;
            }
            seen_chunks.insert(hit.id.clone());
            hits.push(VaultContextHit::SemanticChunk(hit));
        }
    }
    hits.extend(
        client
            .query_vault_tasks(query, limit)?
            .into_iter()
            .map(VaultContextHit::Task),
    );
    hits.extend(
        client
            .query_vault_links(query, limit)?
            .into_iter()
            .map(VaultContextHit::Link),
    );
    let strong_lexical_threshold = strong_lexical_match_threshold(query);
    let task_item_query = is_vault_task_item_query(query);
    let relationship_query = vault_relationship_query_requested(Some(query));
    hits.sort_by(|left, right| {
        let primary_order = if relationship_query {
            left.priority(query, strong_lexical_threshold, task_item_query)
                .cmp(&right.priority(query, strong_lexical_threshold, task_item_query))
                .then_with(|| {
                    left.currentness_priority(query)
                        .cmp(&right.currentness_priority(query))
                })
        } else {
            left.currentness_priority(query)
                .cmp(&right.currentness_priority(query))
                .then_with(|| {
                    left.priority(query, strong_lexical_threshold, task_item_query)
                        .cmp(&right.priority(query, strong_lexical_threshold, task_item_query))
                })
        };

        primary_order
            .then_with(|| {
                right
                    .score()
                    .partial_cmp(&left.score())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.tie_breaker().cmp(&right.tie_breaker()))
    });
    hits.truncate(limit);
    Ok(hits
        .iter()
        .enumerate()
        .map(|(idx, hit)| hit.to_broker_item(query, idx + 1, working_dir))
        .collect())
}

#[cfg(feature = "duckdb-storage")]
fn broker_vault_embedding_model() -> String {
    std::env::var(BROKER_VAULT_EMBEDDING_MODEL_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(crate::embedding::configured_model_label)
}

#[cfg(not(feature = "duckdb-storage"))]
fn collect_vault_context_items(
    _working_dir: Option<&str>,
    _query: Option<&str>,
    _limit: usize,
) -> Result<Vec<BrokerContextItem>> {
    Ok(Vec::new())
}

#[cfg(feature = "duckdb-storage")]
fn broker_duckdb_store_client(db_path: PathBuf) -> Result<DuckDbBrokerStoreClient> {
    let db_path = if db_path.is_absolute() {
        db_path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(db_path)
    };
    let service_slot = BROKER_DUCKDB_SERVICE.get_or_init(|| StdMutex::new(None));
    let mut guard = service_slot
        .lock()
        .map_err(|_| anyhow::anyhow!("DuckDB broker store service lock poisoned"))?;
    let should_start = guard
        .as_ref()
        .map(|state| state.path != db_path)
        .unwrap_or(true);
    if should_start {
        *guard = Some(BrokerDuckDbServiceState {
            path: db_path.clone(),
            service: DuckDbBrokerStoreService::start(&db_path)?,
        });
    }
    guard
        .as_ref()
        .map(|state| state.service.client())
        .context("DuckDB broker store service did not initialize")
}

#[cfg(feature = "duckdb-storage")]
enum VaultContextHit {
    Relationship(VaultRelationshipContextRow),
    SemanticChunk(VaultChunkEmbeddingHit),
    Chunk(VaultChunkContextRow),
    Task(VaultTaskContextRow),
    Link(VaultLinkContextRow),
}

#[cfg(feature = "duckdb-storage")]
impl VaultContextHit {
    fn score(&self) -> f64 {
        match self {
            Self::Relationship(row) => row.score,
            Self::SemanticChunk(row) => row.score,
            Self::Chunk(row) => row.score,
            Self::Task(row) => row.score,
            Self::Link(row) => row.score,
        }
    }

    fn path(&self) -> &str {
        match self {
            Self::Relationship(row) => &row.source_path,
            Self::SemanticChunk(row) => &row.path,
            Self::Chunk(row) => &row.path,
            Self::Task(row) => &row.path,
            Self::Link(row) => &row.source_path,
        }
    }

    fn frontmatter_json(&self) -> &str {
        match self {
            Self::Relationship(row) => &row.frontmatter_json,
            Self::SemanticChunk(row) => &row.frontmatter_json,
            Self::Chunk(row) => &row.frontmatter_json,
            Self::Task(row) => &row.frontmatter_json,
            Self::Link(row) => &row.frontmatter_json,
        }
    }

    fn currentness_priority(&self, query: &str) -> usize {
        vault_currentness_priority(self.path(), self.frontmatter_json(), query)
    }

    fn priority(
        &self,
        query: &str,
        strong_lexical_threshold: usize,
        task_item_query: bool,
    ) -> usize {
        match self {
            Self::Relationship(row) => match row.relationship.as_str() {
                "backlink" => 0,
                "outlink" => 1,
                "folder_neighbor" => 2,
                _ => 3,
            },
            Self::Task(row)
                if task_item_query
                    && is_strong_lexical_match(
                        row.matched_terms.len(),
                        strong_lexical_threshold,
                    ) =>
            {
                1
            }
            Self::Chunk(row) if is_exact_vault_chunk_match(row, query) => 2,
            Self::SemanticChunk(_) => 3,
            Self::Chunk(row)
                if is_strong_lexical_match(row.matched_terms.len(), strong_lexical_threshold) =>
            {
                4
            }
            Self::Task(_) if task_item_query => 5,
            Self::Chunk(_) => 6,
            Self::Task(_) => 7,
            Self::Link(_) => 8,
        }
    }

    fn tie_breaker(&self) -> String {
        match self {
            Self::Relationship(row) => format!(
                "relationship:{}:{}:{}",
                row.relationship,
                row.source_path,
                row.target_path.as_deref().unwrap_or(&row.target)
            ),
            Self::SemanticChunk(row) => {
                format!("semantic_chunk:{}:{:012}", row.path, row.start_line)
            }
            Self::Chunk(row) => format!("chunk:{}:{:012}", row.path, row.start_line),
            Self::Task(row) => format!("task:{}:{:012}", row.path, row.line),
            Self::Link(row) => format!("link:{}:{}", row.source_path, row.target),
        }
    }

    fn to_broker_item(
        &self,
        query: &str,
        rank: usize,
        working_dir: Option<&str>,
    ) -> BrokerContextItem {
        match self {
            Self::Relationship(row) => {
                vault_relationship_broker_item(row, query, rank, working_dir)
            }
            Self::SemanticChunk(row) => {
                vault_semantic_chunk_broker_item(row, query, rank, working_dir)
            }
            Self::Chunk(row) => vault_chunk_broker_item(row, query, rank, working_dir),
            Self::Task(row) => vault_task_broker_item(row, query, rank, working_dir),
            Self::Link(row) => vault_link_broker_item(row, query, rank, working_dir),
        }
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_currentness_priority(path: &str, frontmatter_json: &str, query: &str) -> usize {
    if query_explicitly_names_vault_path(query, path) {
        return 0;
    }

    let workflow_status = frontmatter_json_string(frontmatter_json, "workflow_status")
        .or_else(|| frontmatter_json_string(frontmatter_json, "status"));
    if workflow_status
        .as_deref()
        .is_some_and(clio_status_is_historical)
        || vault_path_is_historical(path)
    {
        return if query_allows_historical_context(query) {
            4
        } else {
            20
        };
    }

    if vault_path_is_current_project_authority(path) && query_requests_current_authority(query) {
        return 1;
    }
    if path.contains("/System/Clio/Core/") || path.starts_with("System/Clio/Core/") {
        return 2;
    }
    if path.contains("TaskNotes/") || path.starts_with("TaskNotes/") {
        return 3;
    }
    5
}

#[cfg(feature = "duckdb-storage")]
fn query_explicitly_names_vault_path(query: &str, path: &str) -> bool {
    let query = query.to_ascii_lowercase();
    let path = path.to_ascii_lowercase();
    if query.contains(&path) {
        return true;
    }
    let basename = path.rsplit('/').next().unwrap_or(&path);
    query.contains(basename)
}

#[cfg(feature = "duckdb-storage")]
fn query_requests_current_authority(query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    [
        "active plan",
        "canonical plan",
        "current plan",
        "current project",
        "current state",
        "current status",
        "context contract",
        "context engineering",
        "context packet",
        "context provider",
        "operating model",
        "provider",
        "broker",
        "clio",
        "hermes",
        "jcode",
        "super-session",
        "handoff",
        "next action",
    ]
    .iter()
    .any(|needle| query.contains(needle))
}

#[cfg(feature = "duckdb-storage")]
fn vault_path_is_current_project_authority(path: &str) -> bool {
    clio_path_is_current_project_authority(path)
}

#[cfg(feature = "duckdb-storage")]
fn vault_path_is_historical(path: &str) -> bool {
    clio_path_is_historical(path)
}

#[cfg(feature = "duckdb-storage")]
fn frontmatter_json_string(frontmatter_json: &str, key: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(frontmatter_json)
        .ok()
        .and_then(|metadata| json_string_field(&metadata, key))
}

#[cfg(feature = "duckdb-storage")]
fn strong_lexical_match_threshold(query: &str) -> usize {
    let term_count = query_terms(query).len();
    if term_count <= 1 {
        usize::MAX
    } else {
        term_count.min(4)
    }
}

#[cfg(feature = "duckdb-storage")]
fn is_strong_lexical_match(matched_term_count: usize, threshold: usize) -> bool {
    matched_term_count >= threshold
}

#[cfg(feature = "duckdb-storage")]
fn is_exact_vault_chunk_match(row: &VaultChunkContextRow, query: &str) -> bool {
    let needle = normalize_vault_match_text(query);
    if needle.split_whitespace().count() < 2 {
        return false;
    }
    let haystack =
        normalize_vault_match_text(&format!("{}\n{}\n{}", row.title, row.heading, row.content));
    haystack.contains(&needle)
}

#[cfg(feature = "duckdb-storage")]
fn normalize_vault_match_text(value: &str) -> String {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|part| {
            let part = part.trim().to_lowercase();
            if part.is_empty() { None } else { Some(part) }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(feature = "duckdb-storage")]
fn is_vault_task_item_query(query: &str) -> bool {
    let query = query.to_lowercase();
    query.contains("task item")
        || query.contains("unchecked task")
        || query.contains("checked task")
        || query.contains("todo item")
        || query.contains("todo")
        || query.contains("[ ]")
        || query.contains("- [ ]")
}

#[cfg(feature = "duckdb-storage")]
fn vault_path_candidates_from_query(query: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();

    for line in query.lines() {
        if let Some(candidate) = vault_path_candidate_from_line(line) {
            push_vault_path_candidate(candidate, &mut candidates, &mut seen);
        }
    }

    for token in query.split_whitespace() {
        let mut token =
            token.trim_matches(['`', '"', '\'', '<', '>', '(', ')', '[', ']', ',', ';', ':']);
        if let Some(rest) = token.strip_prefix("vault://") {
            token = rest;
        }
        token = token.split('#').next().unwrap_or(token);
        token = token.split('|').next().unwrap_or(token);
        token = token.trim_end_matches(['.', '?', '!']);
        if !token.to_lowercase().contains(".md") {
            continue;
        }
        let Some(md_end) = token.to_lowercase().find(".md").map(|idx| idx + 3) else {
            continue;
        };
        let candidate = token[..md_end].trim_matches('/').to_string();
        if candidate.is_empty() {
            continue;
        }
        push_vault_path_candidate(candidate, &mut candidates, &mut seen);
    }
    candidates
}

#[cfg(feature = "duckdb-storage")]
fn vault_path_candidate_from_line(line: &str) -> Option<String> {
    let line = line.trim();
    if !line.to_lowercase().contains(".md") {
        return None;
    }

    let start = if let Some(vault_start) = line.find("vault://") {
        vault_start + "vault://".len()
    } else {
        vault_path_prefix_start(line)?
    };
    let pathish = &line[start..];
    let md_end = pathish.to_lowercase().find(".md")? + 3;
    let candidate = pathish[..md_end]
        .split('#')
        .next()
        .unwrap_or("")
        .split('|')
        .next()
        .unwrap_or("")
        .trim_matches([
            '`', '"', '\'', '<', '>', '(', ')', '[', ']', ',', ';', ':', '.', '?', '!',
        ])
        .trim_matches('/')
        .trim()
        .to_string();
    if candidate.is_empty() {
        None
    } else {
        Some(candidate)
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_path_prefix_start(value: &str) -> Option<usize> {
    [
        "Projects/",
        "TaskNotes/",
        "Knowledge/",
        "System/",
        "Templates/",
        "Views/",
        "Daily/",
        "Archive/",
        "Sources/",
        "Attachments/",
    ]
    .iter()
    .filter_map(|prefix| value.find(prefix))
    .min()
}

#[cfg(feature = "duckdb-storage")]
fn push_vault_path_candidate(
    candidate: String,
    candidates: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let normalized = candidate.to_lowercase();
    if candidates
        .iter()
        .any(|existing| existing.to_lowercase().ends_with(&normalized))
    {
        return;
    }
    if seen.insert(normalized) {
        candidates.push(candidate);
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_relationship_anchor_paths_from_query(query: &str) -> Vec<String> {
    vault_path_candidates_from_query(query)
        .into_iter()
        .take(1)
        .collect()
}

#[cfg(feature = "duckdb-storage")]
fn vault_relationship_query_requested(query: Option<&str>) -> bool {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return false;
    };
    if vault_path_candidates_from_query(query).is_empty() {
        return false;
    }
    let query = query.to_lowercase();
    [
        "backlink",
        "back link",
        "outlink",
        "out link",
        "link",
        "related",
        "relationship",
        "neighbor",
        "neighbour",
    ]
    .iter()
    .any(|term| query.contains(term))
}

#[cfg(feature = "duckdb-storage")]
fn vault_relationship_broker_item(
    row: &VaultRelationshipContextRow,
    query: &str,
    rank: usize,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let source_uri = vault_chunk_uri(&row.source_path, "");
    let target_uri = row
        .target_path
        .as_ref()
        .map(|path| vault_chunk_uri(path, ""))
        .unwrap_or_else(|| vault_link_uri(&row.source_path, &row.target));
    let score = Some(row.score as f32);
    let content = match row.relationship.as_str() {
        "backlink" => format!(
            "{} links to {} via {}",
            row.source_path,
            row.target_path.as_deref().unwrap_or(&row.target),
            row.raw.as_deref().unwrap_or(&row.target)
        ),
        "outlink" => format!(
            "{} links to {} via {}",
            row.source_path,
            row.target_path.as_deref().unwrap_or(&row.target),
            row.raw.as_deref().unwrap_or(&row.target)
        ),
        "folder_neighbor" => format!(
            "{} shares folder with {}",
            row.target_path.as_deref().unwrap_or(&row.target),
            row.source_path
        ),
        _ => format!(
            "{} relates to {}",
            row.source_path,
            row.target_path.as_deref().unwrap_or(&row.target)
        ),
    };
    BrokerContextItem {
        id: row.id.clone(),
        kind: "vault_relationship".to_string(),
        scope: "vault".to_string(),
        content_format: "plain_text".to_string(),
        title: Some(format!(
            "{}: {} -> {}",
            row.relationship,
            row.source_title,
            row.target_title.as_deref().unwrap_or(&row.target)
        )),
        summary: Some(format!("Vault {} relationship", row.relationship)),
        content: Some(content.clone()),
        tags: vec![
            "vault".to_string(),
            "vault_relationship".to_string(),
            row.relationship.clone(),
        ],
        source: Some(source_uri.clone()),
        score,
        origin: BrokerContextOrigin {
            tool: Some("duckdb_broker_store".to_string()),
            source: Some("vault".to_string()),
            working_dir: working_dir.map(str::to_string),
            path: Some(row.source_path.clone()),
            uri: Some(source_uri.clone()),
            ..Default::default()
        },
        relevance: Some(BrokerContextRelevance {
            query: Some(query.to_string()),
            retrieval_mode: Some(row.retrieval_mode.clone()),
            score,
            rank: Some(rank),
            matched_terms: Vec::new(),
            exact_match: Some(true),
        }),
        fragments: vec![BrokerContextFragment {
            relation: row.relationship.clone(),
            content,
            content_format: "plain_text".to_string(),
            role: None,
            message_index: None,
            message_id: None,
            timestamp: None,
        }],
        metadata: json!({
            "durable_memory": false,
            "source_kind": "vault_relationship",
            "relationship": row.relationship,
            "source_file_id": row.source_file_id,
            "source_path": row.source_path,
            "source_title": row.source_title,
            "source_uri": source_uri,
            "target": row.target,
            "target_path": row.target_path,
            "target_title": row.target_title,
            "target_uri": target_uri,
            "link_kind": row.link_kind,
            "raw": row.raw,
            "source_checksum": row.source_checksum,
            "target_checksum": row.target_checksum,
            "mtime_ns": row.mtime_ns,
            "frontmatter_json": row.frontmatter_json,
        }),
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_chunk_broker_item(
    row: &VaultChunkContextRow,
    query: &str,
    rank: usize,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let uri = vault_chunk_uri(&row.path, &row.heading);
    let score = Some(row.score as f32);
    BrokerContextItem {
        id: format!("vault_chunk:{}", row.id),
        kind: "vault_chunk".to_string(),
        scope: "vault".to_string(),
        content_format: "markdown".to_string(),
        title: Some(if row.heading.trim().is_empty() {
            row.title.clone()
        } else {
            format!("{} / {}", row.title, row.heading)
        }),
        summary: Some(summarize_content(&row.content)),
        content: Some(row.content.clone()),
        tags: vec!["vault".to_string(), "vault_chunk".to_string()],
        source: Some(uri.clone()),
        score,
        origin: BrokerContextOrigin {
            tool: Some("duckdb_broker_store".to_string()),
            source: Some("vault".to_string()),
            working_dir: working_dir.map(str::to_string),
            path: Some(row.path.clone()),
            uri: Some(uri.clone()),
            ..Default::default()
        },
        relevance: Some(BrokerContextRelevance {
            query: Some(query.to_string()),
            retrieval_mode: Some("duckdb_broker_store".to_string()),
            score,
            rank: Some(rank),
            matched_terms: row.matched_terms.clone(),
            exact_match: Some(row.content.to_lowercase().contains(&query.to_lowercase())),
        }),
        fragments: vec![BrokerContextFragment {
            relation: "source_span".to_string(),
            content: row.content.clone(),
            content_format: "markdown".to_string(),
            role: None,
            message_index: None,
            message_id: None,
            timestamp: None,
        }],
        metadata: json!({
            "durable_memory": false,
            "source_kind": "vault_chunk",
            "file_id": row.file_id,
            "chunk_id": row.id,
            "checksum": row.checksum,
            "source_checksum": row.source_checksum,
            "mtime_ns": row.mtime_ns,
            "frontmatter_json": row.frontmatter_json,
            "start_line": row.start_line,
            "end_line": row.end_line,
            "uri": uri,
        }),
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_semantic_chunk_broker_item(
    row: &VaultChunkEmbeddingHit,
    query: &str,
    rank: usize,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let uri = vault_chunk_uri(&row.path, &row.heading);
    let score = Some(row.score as f32);
    BrokerContextItem {
        id: format!("vault_chunk:{}", row.id),
        kind: "vault_chunk".to_string(),
        scope: "vault".to_string(),
        content_format: "markdown".to_string(),
        title: Some(if row.heading.trim().is_empty() {
            row.title.clone()
        } else {
            format!("{} / {}", row.title, row.heading)
        }),
        summary: Some(summarize_content(&row.content)),
        content: Some(row.content.clone()),
        tags: vec![
            "vault".to_string(),
            "vault_chunk".to_string(),
            "semantic".to_string(),
        ],
        source: Some(uri.clone()),
        score,
        origin: BrokerContextOrigin {
            tool: Some("duckdb_broker_store".to_string()),
            source: Some("vault".to_string()),
            working_dir: working_dir.map(str::to_string),
            path: Some(row.path.clone()),
            uri: Some(uri.clone()),
            ..Default::default()
        },
        relevance: Some(BrokerContextRelevance {
            query: Some(query.to_string()),
            retrieval_mode: Some("duckdb_broker_store_semantic".to_string()),
            score,
            rank: Some(rank),
            matched_terms: Vec::new(),
            exact_match: Some(row.content.to_lowercase().contains(&query.to_lowercase())),
        }),
        fragments: vec![BrokerContextFragment {
            relation: "source_span".to_string(),
            content: row.content.clone(),
            content_format: "markdown".to_string(),
            role: None,
            message_index: None,
            message_id: None,
            timestamp: None,
        }],
        metadata: json!({
            "durable_memory": false,
            "source_kind": "vault_chunk",
            "file_id": row.file_id,
            "chunk_id": row.id,
            "checksum": row.checksum,
            "source_checksum": row.source_checksum,
            "mtime_ns": row.mtime_ns,
            "frontmatter_json": row.frontmatter_json,
            "start_line": row.start_line,
            "end_line": row.end_line,
            "uri": uri,
            "embedding_model": row.embedding_model,
        }),
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_task_broker_item(
    row: &VaultTaskContextRow,
    query: &str,
    rank: usize,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let uri = vault_line_uri(&row.path, row.line);
    let score = Some(row.score as f32);
    let task_state = if row.checked { "completed" } else { "open" };
    BrokerContextItem {
        id: format!("vault_task:{}", row.id),
        kind: "vault_task".to_string(),
        scope: "vault".to_string(),
        content_format: "plain_text".to_string(),
        title: Some(format!("{} / task", row.title)),
        summary: Some(format!("{task_state} task")),
        content: Some(row.content.clone()),
        tags: vec![
            "vault".to_string(),
            "vault_task".to_string(),
            task_state.to_string(),
        ],
        source: Some(uri.clone()),
        score,
        origin: BrokerContextOrigin {
            tool: Some("duckdb_broker_store".to_string()),
            source: Some("vault".to_string()),
            working_dir: working_dir.map(str::to_string),
            path: Some(row.path.clone()),
            uri: Some(uri.clone()),
            ..Default::default()
        },
        relevance: Some(BrokerContextRelevance {
            query: Some(query.to_string()),
            retrieval_mode: Some("duckdb_broker_store".to_string()),
            score,
            rank: Some(rank),
            matched_terms: row.matched_terms.clone(),
            exact_match: Some(row.content.to_lowercase().contains(&query.to_lowercase())),
        }),
        fragments: vec![BrokerContextFragment {
            relation: "source_span".to_string(),
            content: row.content.clone(),
            content_format: "plain_text".to_string(),
            role: None,
            message_index: None,
            message_id: None,
            timestamp: None,
        }],
        metadata: json!({
            "durable_memory": false,
            "source_kind": "vault_task",
            "file_id": row.file_id,
            "task_id": row.id,
            "checked": row.checked,
            "source_checksum": row.source_checksum,
            "mtime_ns": row.mtime_ns,
            "frontmatter_json": row.frontmatter_json,
            "line": row.line,
            "uri": uri,
        }),
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_link_broker_item(
    row: &VaultLinkContextRow,
    query: &str,
    rank: usize,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let uri = vault_link_uri(&row.source_path, &row.target);
    let score = Some(row.score as f32);
    BrokerContextItem {
        id: format!("vault_link:{}", row.id),
        kind: "vault_link".to_string(),
        scope: "vault".to_string(),
        content_format: "plain_text".to_string(),
        title: Some(format!("{} -> {}", row.title, row.target)),
        summary: Some(format!("{} link", row.kind)),
        content: Some(row.raw.clone()),
        tags: vec![
            "vault".to_string(),
            "vault_link".to_string(),
            row.kind.clone(),
        ],
        source: Some(uri.clone()),
        score,
        origin: BrokerContextOrigin {
            tool: Some("duckdb_broker_store".to_string()),
            source: Some("vault".to_string()),
            working_dir: working_dir.map(str::to_string),
            path: Some(row.source_path.clone()),
            uri: Some(uri.clone()),
            ..Default::default()
        },
        relevance: Some(BrokerContextRelevance {
            query: Some(query.to_string()),
            retrieval_mode: Some("duckdb_broker_store".to_string()),
            score,
            rank: Some(rank),
            matched_terms: row.matched_terms.clone(),
            exact_match: Some(row.raw.to_lowercase().contains(&query.to_lowercase())),
        }),
        fragments: vec![BrokerContextFragment {
            relation: "source_link".to_string(),
            content: row.raw.clone(),
            content_format: "plain_text".to_string(),
            role: None,
            message_index: None,
            message_id: None,
            timestamp: None,
        }],
        metadata: json!({
            "durable_memory": false,
            "source_kind": "vault_link",
            "source_file_id": row.source_file_id,
            "link_id": row.id,
            "target": row.target,
            "link_kind": row.kind,
            "source_checksum": row.source_checksum,
            "mtime_ns": row.mtime_ns,
            "frontmatter_json": row.frontmatter_json,
            "uri": uri,
        }),
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_chunk_uri(path: &str, heading: &str) -> String {
    if heading.trim().is_empty() {
        format!("vault://{path}")
    } else {
        format!("vault://{path}#{heading}")
    }
}

#[cfg(feature = "duckdb-storage")]
fn vault_line_uri(path: &str, line: i64) -> String {
    format!("vault://{path}#L{line}")
}

#[cfg(feature = "duckdb-storage")]
fn vault_link_uri(path: &str, target: &str) -> String {
    if target.trim().is_empty() {
        format!("vault://{path}")
    } else {
        format!("vault://{path}#link-{}", target.replace(' ', "-"))
    }
}

fn collect_session_search_hits(
    session_id: &str,
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<BrokerSearchHitContext>> {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return Ok(Vec::new());
    };
    let hit_limit = broker_search_hit_limit(limit);
    if hit_limit == 0 {
        return Ok(Vec::new());
    }

    let options = crate::tool::session_search::StructuredSessionSearchOptions::broker_prior_session(
        session_id,
        working_dir.map(str::to_string),
        hit_limit,
    );
    let report = crate::tool::session_search::search_jcode_sessions_structured(query, options)?;
    Ok(report
        .results
        .into_iter()
        .take(hit_limit)
        .enumerate()
        .map(|(idx, result)| search_result_hit_context(result, query, idx + 1))
        .collect())
}

fn collect_conversation_search_hits(
    session: &crate::session::Session,
    query: Option<&str>,
    limit: usize,
) -> Vec<BrokerSearchHitContext> {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return Vec::new();
    };
    let hit_limit = broker_search_hit_limit(limit);
    if hit_limit == 0 {
        return Vec::new();
    }

    let options =
        crate::tool::session_search::StructuredSessionSearchOptions::broker_current_session(
            session.id.clone(),
            hit_limit,
        );
    crate::tool::session_search::search_session_structured(session, query, options)
        .results
        .into_iter()
        .take(hit_limit)
        .enumerate()
        .map(|(idx, result)| search_result_hit_context(result, query, idx + 1))
        .collect()
}

fn search_result_hit_context(
    result: SessionSearchResult,
    query: &str,
    rank: usize,
) -> BrokerSearchHitContext {
    let message_id = result.message_id.clone();
    let message_index = result.message_index;
    let id = message_id
        .clone()
        .or_else(|| message_index.map(|idx| idx.to_string()))
        .unwrap_or_else(|| "metadata".to_string());
    let session_label = result
        .title
        .clone()
        .or_else(|| result.short_name.clone())
        .unwrap_or_else(|| result.session_id.clone());
    let title = format!("{} match in {}", result.role, session_label);
    let summary = format!("{} search hit from {}", result.role, session_label);
    let metadata = json!({
        "source": result.source,
        "result_kind": result.kind.label(),
        "session_title": result.title,
        "short_name": result.short_name,
        "source_session_path": session_path_for_metadata(&result.session_id),
        "context_count": result.context.len(),
    });

    BrokerSearchHitContext {
        id,
        title,
        summary,
        content: result.snippet.clone(),
        snippet: result.snippet,
        session_id: result.session_id,
        working_dir: result.working_dir,
        provider_key: result.provider_key,
        model: result.model,
        message_id,
        message_index,
        role: Some(result.role),
        timestamp: result
            .message_timestamp
            .map(|timestamp| timestamp.to_rfc3339()),
        updated_at: Some(result.updated_at.to_rfc3339()),
        query: Some(query.to_string()),
        score: Some(result.score as f32),
        rank: Some(rank),
        matched_terms: result.matched_terms,
        metadata,
    }
}

fn session_path_for_metadata(session_id: &str) -> Option<String> {
    crate::session::session_path(session_id)
        .ok()
        .map(|path| path.display().to_string())
}

fn collect_skill_context_summaries(
    working_dir: Option<&str>,
    query: Option<&str>,
) -> Vec<BrokerSkillContextSummary> {
    let working_path = working_dir.map(PathBuf::from);
    let registry = match crate::skill::SkillRegistry::load_for_working_dir(working_path.as_deref())
    {
        Ok(registry) => registry,
        Err(error) => {
            crate::logging::warn(&format!(
                "Broker context skill summary load failed: {error}"
            ));
            return Vec::new();
        }
    };
    let terms = query.map(query_terms).unwrap_or_default();
    let mut summaries: Vec<BrokerSkillContextSummary> = registry
        .list()
        .into_iter()
        .filter_map(|skill| {
            let scope = skill_scope_for_path(&skill.path, working_path.as_deref());
            let is_project_local = scope == "project";
            if !is_project_local && !skill_matches_query(&skill.name, &skill.description, &terms) {
                return None;
            }
            Some(BrokerSkillContextSummary {
                name: skill.name.clone(),
                description: skill.description.clone(),
                scope: scope.to_string(),
                source: "skill_registry".to_string(),
                path: Some(skill.path.display().to_string()),
                allowed_tools: skill.allowed_tools.clone().unwrap_or_default(),
            })
        })
        .collect();
    summaries.sort_by(|a, b| {
        skill_scope_rank(&a.scope)
            .cmp(&skill_scope_rank(&b.scope))
            .then_with(|| a.name.cmp(&b.name))
    });
    summaries.truncate(BROKER_SKILL_SUMMARY_LIMIT);
    summaries
}

fn skill_scope_for_path(path: &Path, working_dir: Option<&Path>) -> &'static str {
    if working_dir.is_some_and(|working_dir| path.starts_with(working_dir)) {
        "project"
    } else {
        "global"
    }
}

fn skill_scope_rank(scope: &str) -> usize {
    if scope == "project" { 0 } else { 1 }
}

fn skill_matches_query(name: &str, description: &str, terms: &[String]) -> bool {
    if terms.is_empty() {
        return false;
    }
    let searchable = format!("{name} {description}").to_ascii_lowercase();
    terms.iter().any(|term| searchable.contains(term.as_str()))
}

fn broker_search_hit_limit(limit: usize) -> usize {
    if limit == 0 {
        0
    } else {
        limit.min(BROKER_SEARCH_HIT_LIMIT)
    }
}

fn tool_broker_item(tool_name: &str) -> BrokerContextItem {
    BrokerContextItem {
        id: tool_name.to_string(),
        kind: "tool".to_string(),
        scope: "session".to_string(),
        content_format: "plain_text".to_string(),
        title: Some(tool_name.to_string()),
        summary: Some("broker tool".to_string()),
        content: None,
        tags: Vec::new(),
        source: Some("broker_tool_registry".to_string()),
        score: None,
        origin: BrokerContextOrigin {
            tool: Some("tool_registry".to_string()),
            source: Some("broker_tool_registry".to_string()),
            ..Default::default()
        },
        relevance: None,
        fragments: Vec::new(),
        metadata: json!({
            "name": tool_name,
        }),
    }
}

fn memory_broker_item(result: &BrokerMemoryResult, working_dir: Option<&str>) -> BrokerContextItem {
    let memory = &result.memory;
    let score = result
        .relevance
        .as_ref()
        .and_then(|relevance| relevance.score);
    let is_lineage_checkpoint = memory.tags.iter().any(|tag| tag == BROKER_LINEAGE_TAG);
    let kind = if is_lineage_checkpoint {
        "compression_checkpoint"
    } else {
        "memory"
    };
    let mut metadata = json!({
        "category": memory.category,
        "scope": memory.scope,
    });
    if let Some(mode) = result
        .relevance
        .as_ref()
        .and_then(|relevance| relevance.retrieval_mode.as_deref())
    {
        metadata["retrieval_mode"] = json!(mode);
    }
    if is_lineage_checkpoint {
        metadata["logical_super_session_id"] =
            json!(tag_value(&memory.tags, "logical-super-session:").unwrap_or_default());
        metadata["session_segment_id"] =
            json!(tag_value(&memory.tags, "session-segment:").unwrap_or_default());
        metadata["checkpoint_kind"] =
            json!(tag_value(&memory.tags, "checkpoint-kind:").unwrap_or_default());
    }
    BrokerContextItem {
        id: memory.id.clone(),
        kind: kind.to_string(),
        scope: memory.scope.clone(),
        content_format: "plain_text".to_string(),
        title: Some(memory.category.clone()),
        summary: Some(summarize_content(&memory.content)),
        content: Some(memory.content.clone()),
        tags: memory.tags.clone(),
        source: memory.source.clone(),
        score,
        origin: BrokerContextOrigin {
            tool: Some("memory".to_string()),
            source: memory.source.clone(),
            working_dir: working_dir.map(str::to_string),
            ..Default::default()
        },
        relevance: result.relevance.clone(),
        fragments: Vec::new(),
        metadata,
    }
}

fn side_panel_broker_item(
    session_id: &str,
    page: &jcode_side_panel_types::SidePanelPage,
    focused_page_id: Option<&str>,
) -> BrokerContextItem {
    let kind = if page.id.starts_with("goal.") {
        "goal"
    } else {
        "side_panel"
    };
    let mut tags = vec!["side_panel".to_string()];
    if kind == "goal" {
        tags.push("goal".to_string());
    }

    BrokerContextItem {
        id: page.id.clone(),
        kind: kind.to_string(),
        scope: "session".to_string(),
        content_format: "markdown".to_string(),
        title: Some(page.title.clone()),
        summary: Some(summarize_content(&page.content)),
        content: Some(page.content.clone()),
        tags,
        source: Some(page.file_path.clone()),
        score: None,
        origin: BrokerContextOrigin {
            tool: Some(kind.to_string()),
            source: Some(page.file_path.clone()),
            session_id: Some(session_id.to_string()),
            path: Some(page.file_path.clone()),
            ..Default::default()
        },
        relevance: None,
        fragments: Vec::new(),
        metadata: json!({
            "format": page.format,
            "source": page.source,
            "updated_at_ms": page.updated_at_ms,
            "focused": focused_page_id == Some(page.id.as_str()),
        }),
    }
}

fn todo_broker_item(session_id: &str, todo: &TodoItem) -> BrokerContextItem {
    let mut tags = Vec::new();
    if !todo.status.trim().is_empty() {
        tags.push(todo.status.clone());
    }
    if !todo.priority.trim().is_empty() {
        tags.push(todo.priority.clone());
    }

    BrokerContextItem {
        id: todo.id.clone(),
        kind: "todo".to_string(),
        scope: "session".to_string(),
        content_format: "plain_text".to_string(),
        title: Some(todo.content.clone()),
        summary: Some(format!("{}/{}", todo.status, todo.priority)),
        content: Some(todo.content.clone()),
        tags,
        source: Some(session_id.to_string()),
        score: None,
        origin: BrokerContextOrigin {
            tool: Some("todo".to_string()),
            source: Some(session_id.to_string()),
            session_id: Some(session_id.to_string()),
            ..Default::default()
        },
        relevance: None,
        fragments: Vec::new(),
        metadata: json!({
            "status": todo.status,
            "priority": todo.priority,
            "blocked_by": todo.blocked_by,
            "assigned_to": todo.assigned_to,
        }),
    }
}

fn skill_broker_item(
    skill: &BrokerSkillContextSummary,
    working_dir: Option<&str>,
) -> BrokerContextItem {
    let mut metadata = json!({
        "name": skill.name,
        "allowed_tools": skill.allowed_tools,
    });
    if let Some(path) = skill.path.as_deref() {
        metadata["path"] = json!(path);
    }

    BrokerContextItem {
        id: format!("skill:{}", skill.name),
        kind: "skill".to_string(),
        scope: skill.scope.clone(),
        content_format: "plain_text".to_string(),
        title: Some(skill.name.clone()),
        summary: Some(skill.description.clone()),
        content: Some(skill.description.clone()),
        tags: vec!["skill".to_string()],
        source: skill.path.clone().or_else(|| Some(skill.source.clone())),
        score: None,
        origin: BrokerContextOrigin {
            tool: Some("skill_registry".to_string()),
            source: Some(skill.source.clone()),
            working_dir: working_dir.map(str::to_string),
            path: skill.path.clone(),
            ..Default::default()
        },
        relevance: None,
        fragments: Vec::new(),
        metadata,
    }
}

fn session_search_hit_broker_item(hit: &BrokerSearchHitContext) -> BrokerContextItem {
    search_hit_broker_item(
        "session_search_hit",
        "session_search",
        "project",
        "session",
        format!("session:{}:{}", hit.session_id, hit.id),
        hit,
    )
}

fn conversation_search_hit_broker_item(hit: &BrokerSearchHitContext) -> BrokerContextItem {
    search_hit_broker_item(
        "conversation_search_hit",
        "conversation_search",
        "session",
        "conversation",
        format!("conversation:{}:{}", hit.session_id, hit.id),
        hit,
    )
}

fn search_hit_broker_item(
    kind: &str,
    origin_tool: &str,
    scope: &str,
    tag: &str,
    id: String,
    hit: &BrokerSearchHitContext,
) -> BrokerContextItem {
    let metadata = search_hit_metadata(&hit.metadata);
    let relevance = hit.query.as_ref().map(|query| BrokerContextRelevance {
        query: Some(query.clone()),
        retrieval_mode: Some(origin_tool.to_string()),
        score: hit.score,
        rank: hit.rank,
        matched_terms: hit.matched_terms.clone(),
        exact_match: Some(false),
    });
    let fragments = if hit.snippet.trim().is_empty() {
        Vec::new()
    } else {
        vec![BrokerContextFragment {
            relation: "match".to_string(),
            content: hit.snippet.clone(),
            content_format: "plain_text".to_string(),
            role: hit.role.clone(),
            message_index: hit.message_index,
            message_id: hit.message_id.clone(),
            timestamp: hit.timestamp.clone(),
        }]
    };

    BrokerContextItem {
        id,
        kind: kind.to_string(),
        scope: scope.to_string(),
        content_format: "plain_text".to_string(),
        title: Some(hit.title.clone()),
        summary: Some(hit.summary.clone()),
        content: Some(hit.content.clone()),
        tags: vec!["search_hit".to_string(), tag.to_string()],
        source: Some(origin_tool.to_string()),
        score: hit.score,
        origin: BrokerContextOrigin {
            tool: Some(origin_tool.to_string()),
            source: Some(origin_tool.to_string()),
            session_id: Some(hit.session_id.clone()),
            working_dir: hit.working_dir.clone(),
            provider_key: hit.provider_key.clone(),
            model: hit.model.clone(),
            message_id: hit.message_id.clone(),
            message_index: hit.message_index,
            role: hit.role.clone(),
            timestamp: hit.timestamp.clone(),
            updated_at: hit.updated_at.clone(),
            ..Default::default()
        },
        relevance,
        fragments,
        metadata,
    }
}

fn search_hit_metadata(metadata: &serde_json::Value) -> serde_json::Value {
    let mut map = match metadata {
        serde_json::Value::Object(map) => map.clone(),
        serde_json::Value::Null => serde_json::Map::new(),
        value => {
            let mut map = serde_json::Map::new();
            map.insert("value".to_string(), value.clone());
            map
        }
    };
    map.insert("durable_memory".to_string(), json!(false));
    serde_json::Value::Object(map)
}

fn memory_relevance(
    query: Option<&str>,
    entry: &MemoryEntry,
    retrieval_mode: Option<&str>,
    score: Option<f32>,
    rank: usize,
) -> Option<BrokerContextRelevance> {
    let query = query.map(str::trim).filter(|query| !query.is_empty())?;
    let normalized_query = crate::memory_types::normalize_search_text(query);
    let exact_match =
        !normalized_query.is_empty() && entry.searchable_text().contains(normalized_query.as_str());
    Some(BrokerContextRelevance {
        query: Some(query.to_string()),
        retrieval_mode: retrieval_mode.map(str::to_string),
        score,
        rank: Some(rank),
        matched_terms: matched_query_terms(query, entry),
        exact_match: Some(exact_match),
        ..Default::default()
    })
}

fn matched_query_terms(query: &str, entry: &MemoryEntry) -> Vec<String> {
    let searchable = entry.searchable_text();
    query_terms(query)
        .into_iter()
        .filter(|term| searchable.contains(term.as_str()))
        .collect()
}

fn query_terms(query: &str) -> Vec<String> {
    let normalized = crate::memory_types::normalize_search_text(query);
    let mut seen = HashSet::new();
    normalized
        .split_whitespace()
        .filter(|term| !is_query_stopword(term))
        .filter(|term| seen.insert((*term).to_string()))
        .map(str::to_string)
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

fn summarize_content(content: &str) -> String {
    content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
        .chars()
        .take(160)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sidecar::ExtractedMemory;
    use std::ffi::OsString;
    use std::path::Path;
    use std::sync::Mutex;

    #[cfg(feature = "duckdb-storage")]
    use jcode_storage::duckdb_broker_store::{
        DuckDbBrokerStoreService, VaultChunkRecord, VaultEmbeddingRecord, VaultFileRecord,
        VaultLinkRecord, VaultRecordBatch, VaultTaskRecord,
    };

    struct TestHome {
        old_home: Option<OsString>,
        temp_home: tempfile::TempDir,
    }

    impl TestHome {
        fn new() -> Self {
            let old_home = std::env::var_os("JCODE_HOME");
            let temp_home = tempfile::Builder::new()
                .prefix("jcode-broker-context-test-")
                .tempdir()
                .expect("create temp home");
            crate::env::set_var("JCODE_HOME", temp_home.path());
            Self {
                old_home,
                temp_home,
            }
        }

        fn path(&self) -> &Path {
            self.temp_home.path()
        }
    }

    impl Drop for TestHome {
        fn drop(&mut self) {
            match self.old_home.as_ref() {
                Some(value) => crate::env::set_var("JCODE_HOME", value),
                None => crate::env::remove_var("JCODE_HOME"),
            }
        }
    }

    struct TestEnvVar {
        name: &'static str,
        old_value: Option<OsString>,
    }

    impl TestEnvVar {
        fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let old_value = std::env::var_os(name);
            crate::env::set_var(name, value);
            Self { name, old_value }
        }
    }

    impl Drop for TestEnvVar {
        fn drop(&mut self) {
            match self.old_value.as_ref() {
                Some(value) => crate::env::set_var(self.name, value),
                None => crate::env::remove_var(self.name),
            }
        }
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn vault_relationship_query_requested_requires_path_and_relationship_intent() {
        assert!(vault_relationship_query_requested(Some(
            "What backlinks relate to Projects/Demo/Plans/Target.md?"
        )));
        assert!(vault_relationship_query_requested(Some(
            "Show related neighboring notes for vault://Projects/Demo/Plans/Target.md#Decision"
        )));
        assert!(!vault_relationship_query_requested(Some(
            "What note contains Projects/Demo/Plans/Target.md?"
        )));
        assert!(!vault_relationship_query_requested(Some(
            "What links did we discuss yesterday?"
        )));
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn vault_path_candidates_extract_spaced_vault_paths_from_lines() {
        assert_eq!(
            vault_path_candidates_from_query(
                "For this Vault note:\n\nSystem/jcode-validation/Freshness Probe Source 2026-05-14.md\n\nWhat links, backlinks, or neighboring notes does the broker return?"
            ),
            vec!["System/jcode-validation/Freshness Probe Source 2026-05-14.md"]
        );
    }

    struct FakeTranscriptExtractor {
        memories: Vec<ExtractedMemory>,
        seen_existing: Mutex<Vec<String>>,
    }

    impl FakeTranscriptExtractor {
        fn new(memories: Vec<ExtractedMemory>) -> Self {
            Self {
                memories,
                seen_existing: Mutex::new(Vec::new()),
            }
        }

        fn seen_existing(&self) -> Vec<String> {
            self.seen_existing.lock().expect("seen existing").clone()
        }
    }

    impl TranscriptMemoryExtractor for FakeTranscriptExtractor {
        fn extract<'a>(
            &'a self,
            transcript: &'a str,
            existing: &'a [String],
        ) -> TranscriptExtractionFuture<'a> {
            self.seen_existing
                .lock()
                .expect("seen existing")
                .extend(existing.iter().cloned());
            let memories = self.memories.clone();
            Box::pin(async move {
                assert!(!transcript.trim().is_empty());
                Ok(memories)
            })
        }
    }

    fn with_temp_home<F, T>(f: F) -> T
    where
        F: FnOnce(&Path) -> T,
    {
        let _guard = crate::storage::lock_test_env();
        let old_home = std::env::var_os("JCODE_HOME");
        let temp_home = tempfile::Builder::new()
            .prefix("jcode-broker-context-test-")
            .tempdir()
            .expect("create temp home");
        crate::env::set_var("JCODE_HOME", temp_home.path());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(temp_home.path())));

        match old_home {
            Some(value) => crate::env::set_var("JCODE_HOME", value),
            None => crate::env::remove_var("JCODE_HOME"),
        }

        match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    #[test]
    fn derived_extraction_storage_links_provenance_and_remains_recallable() {
        with_temp_home(|home| {
            let project_dir = home.join("project");
            std::fs::create_dir_all(&project_dir).expect("create project dir");
            let manager = MemoryManager::new().with_project_dir(&project_dir);
            let provenance_id = store_provenance_memory(
                &manager,
                "ses_test",
                "hermes:session_end",
                "External transcript synced from hermes:session_end.\n\nUser: Remember Rob prefers focused broker tests.".to_string(),
                vec!["broker-transcript-sync".to_string()],
            )
            .expect("store provenance");

            let derived_ids = store_derived_memories(
                &manager,
                vec![ExtractedMemory {
                    category: "preference".to_string(),
                    content: "Rob prefers focused broker tests for memory-provider changes."
                        .to_string(),
                    trust: "high".to_string(),
                }],
                "ses_test",
                "hermes:session_end",
                &provenance_id,
            )
            .expect("store derived memories");

            assert_eq!(derived_ids.len(), 1);
            let derived_id = derived_ids[0].clone();

            let context = collect_broker_memories(
                Some(project_dir.to_string_lossy().as_ref()),
                Some("focused broker tests"),
                8,
                false,
            )
            .expect("collect broker memories");
            assert!(
                context.iter().any(|memory| memory.id == derived_id
                    && memory.content.contains("focused broker tests")
                    && memory.tags.iter().any(|tag| tag == "broker-derived")),
                "derived memory should be recallable by default, got {context:?}"
            );
            assert!(
                context.iter().all(|memory| memory.id != provenance_id),
                "provenance memory should remain hidden by default, got {context:?}"
            );

            let graph = manager.load_project_graph().expect("load graph");
            let edges = graph.edges.get(&derived_id).expect("derived edges");
            assert!(
                edges.iter().any(|edge| edge.target == provenance_id
                    && matches!(edge.kind, EdgeKind::DerivedFrom)),
                "derived memory should link back to provenance"
            );
        });
    }

    #[tokio::test]
    async fn transcript_sync_with_fake_extractor_returns_derived_memory_event_and_context() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let project_dir = _env.path().join("project");
        std::fs::create_dir_all(&project_dir).expect("create project dir");
        let manager = MemoryManager::new().with_project_dir(&project_dir);
        let extractor = FakeTranscriptExtractor::new(vec![ExtractedMemory {
            category: "preference".to_string(),
            content: "Rob prefers focused broker tests for memory-provider changes.".to_string(),
            trust: "high".to_string(),
        }]);

        let event = broker_transcript_sync_event_for_manager(
            77,
            "ses_test".to_string(),
            &manager,
            "User: Remember Rob prefers focused broker tests.\nAssistant: Stored.",
            "hermes:pre_compress",
            &extractor,
        )
        .await
        .expect("sync transcript");

        let ServerEvent::BrokerTranscriptSynced {
            memory_ids,
            provenance_memory_ids,
            derived_memory_ids,
            extraction_status,
            ..
        } = event
        else {
            panic!("expected broker transcript synced event");
        };

        assert_eq!(extraction_status, BrokerMemoryExtractionStatus::Extracted);
        assert_eq!(provenance_memory_ids.len(), 1);
        assert_eq!(derived_memory_ids.len(), 1);
        assert_eq!(memory_ids.len(), 3);
        assert!(memory_ids.contains(&provenance_memory_ids[0]));
        assert!(memory_ids.contains(&derived_memory_ids[0]));
        assert_eq!(extractor.seen_existing(), Vec::<String>::new());

        let context = collect_broker_memories(
            Some(project_dir.to_string_lossy().as_ref()),
            Some("focused broker tests"),
            8,
            false,
        )
        .expect("collect broker memories");
        assert!(
            context
                .iter()
                .any(|memory| memory.id == derived_memory_ids[0]
                    && memory.content.contains("focused broker tests")
                    && memory.tags.iter().any(|tag| tag == "broker-derived")),
            "derived memory should be recallable by default, got {context:?}"
        );
        assert!(
            context
                .iter()
                .all(|memory| memory.id != provenance_memory_ids[0]),
            "provenance memory should stay hidden by default, got {context:?}"
        );
    }

    #[tokio::test]
    async fn transcript_sync_stores_lineage_checkpoint_for_packet_lineage() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let project_dir = _env.path().join("Hermes-Honcho-LangGraph-Second-Brain");
        std::fs::create_dir_all(&project_dir).expect("create project dir");
        let manager = MemoryManager::new().with_project_dir(&project_dir);
        let extractor = FakeTranscriptExtractor::new(Vec::new());

        let event = broker_transcript_sync_event_for_manager_with_gate(
            99,
            "session_lineage".to_string(),
            &manager,
            "User: We deployed 7036e23.\nAssistant: Next action is lineage checkpoints.",
            "hermes:pre_compress",
            Some(project_dir.to_string_lossy().as_ref()),
            false,
            &extractor,
        )
        .await
        .expect("sync transcript");

        let ServerEvent::BrokerTranscriptSynced { memory_ids, .. } = event else {
            panic!("expected broker transcript synced event");
        };
        assert_eq!(memory_ids.len(), 2);
        assert_ne!(
            memory_ids[0], memory_ids[1],
            "checkpoint must be a distinct memory, not a dedup reinforcement of provenance"
        );

        let memory_results = collect_broker_memory_results(
            Some(project_dir.to_string_lossy().as_ref()),
            Some("lineage checkpoint 7036e23 next action"),
            8,
            false,
        )
        .expect("collect broker memory results");
        let items: Vec<BrokerContextItem> = memory_results
            .iter()
            .map(|result| memory_broker_item(result, Some(project_dir.to_string_lossy().as_ref())))
            .collect();
        let packet = clio_context_packet_from_items(&items);

        assert!(
            packet.lineage.iter().any(|item| {
                item.item.kind == "compression_checkpoint"
                    && item
                        .item
                        .summary
                        .as_deref()
                        .unwrap_or_default()
                        .contains("Hermes compression checkpoint")
                    && item
                        .item
                        .metadata
                        .get("logical_super_session_id")
                        .and_then(|value| value.as_str())
                        == Some("clio-super-session")
                    && item
                        .item
                        .metadata
                        .get("session_segment_id")
                        .and_then(|value| value.as_str())
                        == Some("session_lineage")
            }),
            "lineage packet should contain compact checkpoint item, got {:?}",
            packet.lineage
        );
    }

    #[tokio::test]
    async fn lineage_checkpoint_with_next_action_beats_newer_low_info_smoke() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let project_dir = _env.path().join("Hermes-Honcho-LangGraph-Second-Brain");
        std::fs::create_dir_all(&project_dir).expect("create project dir");
        let manager = MemoryManager::new().with_project_dir(&project_dir);
        let extractor = FakeTranscriptExtractor::new(Vec::new());

        broker_transcript_sync_event_for_manager_with_gate(
            10,
            "session_good_checkpoint".to_string(),
            &manager,
            "Decision: hidden sync gate passed.\nNext action: run non-Vault restraint probe.",
            "hermes:pre_compress",
            Some(project_dir.to_string_lossy().as_ref()),
            false,
            &extractor,
        )
        .await
        .expect("sync good checkpoint");

        for index in 0..3 {
            broker_transcript_sync_event_for_manager_with_gate(
                20 + index,
                format!("session_smoke_{index}"),
                &manager,
                &format!("user: smoke marker {index}\nassistant: acknowledged"),
                "hermes:session_end",
                Some(project_dir.to_string_lossy().as_ref()),
                false,
                &extractor,
            )
            .await
            .expect("sync smoke checkpoint");
        }

        let memory_results = collect_broker_memory_results(
            Some(project_dir.to_string_lossy().as_ref()),
            Some("lineage checkpoint next action"),
            8,
            false,
        )
        .expect("collect broker memory results");
        let items: Vec<BrokerContextItem> = memory_results
            .iter()
            .map(|result| memory_broker_item(result, Some(project_dir.to_string_lossy().as_ref())))
            .collect();
        let packet = clio_context_packet_from_items(&items);

        assert!(
            packet.lineage.iter().any(|item| {
                item.item
                    .content
                    .as_deref()
                    .unwrap_or_default()
                    .contains("Next action")
            }),
            "lineage packet should keep the actionable checkpoint ahead of low-info smoke, got {:?}",
            packet.lineage
        );
    }

    #[tokio::test]
    async fn transcript_sync_reinforces_duplicate_derived_memory() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let project_dir = _env.path().join("project");
        std::fs::create_dir_all(&project_dir).expect("create project dir");
        let manager = MemoryManager::new().with_project_dir(&project_dir);
        let existing_id = manager
            .remember_project(
                MemoryEntry::new(
                    MemoryCategory::Preference,
                    "Rob prefers focused broker tests for memory-provider changes.",
                )
                .with_source("seed"),
            )
            .expect("seed existing memory");
        let extractor = FakeTranscriptExtractor::new(vec![ExtractedMemory {
            category: "preference".to_string(),
            content: "Rob prefers focused broker tests for memory-provider changes.".to_string(),
            trust: "high".to_string(),
        }]);

        let event = broker_transcript_sync_event_for_manager(
            88,
            "ses_duplicate".to_string(),
            &manager,
            "User: Rob again mentioned focused broker tests.",
            "hermes:session_end",
            &extractor,
        )
        .await
        .expect("sync transcript");

        let ServerEvent::BrokerTranscriptSynced {
            provenance_memory_ids,
            derived_memory_ids,
            extraction_status,
            ..
        } = event
        else {
            panic!("expected broker transcript synced event");
        };

        assert_eq!(extraction_status, BrokerMemoryExtractionStatus::Extracted);
        assert_eq!(derived_memory_ids, vec![existing_id.clone()]);

        let graph = manager.load_project_graph().expect("load graph");
        let existing = graph
            .get_memory(&existing_id)
            .expect("existing memory remains");
        assert_eq!(existing.strength, 2);
        assert_eq!(existing.reinforcements.len(), 1);
        assert_eq!(existing.reinforcements[0].session_id, "ses_duplicate");
        assert_eq!(
            graph
                .active_memories()
                .filter(|memory| memory.content
                    == "Rob prefers focused broker tests for memory-provider changes.")
                .count(),
            1
        );
        let edges = graph.edges.get(&existing_id).expect("derived edges");
        assert!(
            edges
                .iter()
                .any(|edge| edge.target == provenance_memory_ids[0]
                    && matches!(edge.kind, EdgeKind::DerivedFrom)),
            "reinforced memory should link back to new transcript provenance"
        );
    }

    #[test]
    fn broker_memory_retrieval_falls_back_to_keyword_when_embeddings_are_unavailable() {
        with_temp_home(|home| {
            let project_dir = home.join("project");
            std::fs::create_dir_all(&project_dir).expect("create project dir");
            let manager = MemoryManager::new().with_project_dir(&project_dir);
            manager
                .remember_project(MemoryEntry::new(
                    MemoryCategory::Fact,
                    "Rob wants broker retrieval to fall back without embeddings.",
                ))
                .expect("remember project memory");
            manager
                .remember_global(MemoryEntry::new(
                    MemoryCategory::Preference,
                    "Global memory should keep its own scope.",
                ))
                .expect("remember global memory");

            let results = collect_broker_memory_results(
                Some(project_dir.to_string_lossy().as_ref()),
                Some("broker retrieval"),
                8,
                false,
            )
            .expect("collect broker memory results");

            let hit = results
                .iter()
                .find(|result| {
                    result
                        .memory
                        .content
                        .contains("fall back without embeddings")
                })
                .expect("fallback keyword hit");
            let relevance = hit.relevance.as_ref().expect("relevance metadata");
            assert_eq!(relevance.retrieval_mode.as_deref(), Some("keyword"));
            assert_eq!(relevance.rank, Some(1));
            assert!(relevance.matched_terms.contains(&"broker".to_string()));
            assert!(relevance.matched_terms.contains(&"retrieval".to_string()));
            assert_eq!(hit.memory.scope, "project");
        });
    }

    #[test]
    fn broker_memory_retrieval_cascades_from_semantic_seed_to_tag_related_memory() {
        with_temp_home(|home| {
            let project_dir = home.join("project");
            std::fs::create_dir_all(&project_dir).expect("create project dir");
            let manager = MemoryManager::new().with_project_dir(&project_dir);
            manager
                .remember_project(
                    MemoryEntry::new(
                        MemoryCategory::Fact,
                        "Semantic seed: broker retrieval uses embeddings.",
                    )
                    .with_tags(vec!["broker-cascade".to_string()])
                    .with_embedding(vec![1.0, 0.0]),
                )
                .expect("remember semantic seed");
            manager
                .remember_project(
                    MemoryEntry::new(
                        MemoryCategory::Fact,
                        "Tag-related memory appears through cascade traversal.",
                    )
                    .with_tags(vec!["broker-cascade".to_string()]),
                )
                .expect("remember cascade related memory");
            manager
                .remember_global(
                    MemoryEntry::new(
                        MemoryCategory::Fact,
                        "Global semantic memory keeps global scope.",
                    )
                    .with_embedding(vec![0.6, 0.8]),
                )
                .expect("remember global semantic memory");

            let results = collect_broker_memory_results_with_query_embedding(
                Some(project_dir.to_string_lossy().as_ref()),
                "semantic broker retrieval",
                &[1.0, 0.0],
                8,
                false,
            )
            .expect("collect semantic broker memory results");

            let related = results
                .iter()
                .find(|result| result.memory.content.contains("cascade traversal"))
                .expect("cascade-related memory");
            let relevance = related.relevance.as_ref().expect("relevance metadata");
            assert_eq!(
                relevance.retrieval_mode.as_deref(),
                Some("semantic_cascade")
            );
            assert!(relevance.score.unwrap_or_default() > 0.0);
            assert!(relevance.rank.unwrap_or_default() >= 2);
            assert_eq!(related.memory.scope, "project");

            let global = results
                .iter()
                .find(|result| result.memory.content.contains("global scope"))
                .expect("global semantic memory");
            assert_eq!(global.memory.scope, "global");
            assert_eq!(
                global
                    .relevance
                    .as_ref()
                    .and_then(|relevance| relevance.retrieval_mode.as_deref()),
                Some("semantic_cascade")
            );
        });
    }

    #[test]
    fn broker_context_mapper_contract_covers_phase_2_item_kinds() {
        let memory_result = BrokerMemoryResult {
            memory: BrokerMemoryContextItem {
                id: "mem_phase_2".to_string(),
                category: "fact".to_string(),
                scope: "project".to_string(),
                content: "Phase 2 context keeps typed items as the adapter surface.".to_string(),
                tags: vec!["phase-2".to_string()],
                source: Some("derived:hermes:ses_contract".to_string()),
            },
            relevance: Some(BrokerContextRelevance {
                query: Some("typed context".to_string()),
                retrieval_mode: Some("keyword".to_string()),
                rank: Some(1),
                matched_terms: vec!["context".to_string()],
                exact_match: Some(false),
                ..Default::default()
            }),
        };
        let goal_page = jcode_side_panel_types::SidePanelPage {
            id: "goal.phase-2-context".to_string(),
            title: "Phase 2 Context".to_string(),
            file_path: "/tmp/project/.jcode/goals/phase-2.md".to_string(),
            format: jcode_side_panel_types::SidePanelPageFormat::Markdown,
            source: jcode_side_panel_types::SidePanelPageSource::Managed,
            content: "# Phase 2\n\nKeep context typed.".to_string(),
            updated_at_ms: 100,
        };
        let note_page = jcode_side_panel_types::SidePanelPage {
            id: "note.phase-2-context".to_string(),
            title: "Phase 2 Note".to_string(),
            file_path: "/tmp/project/.jcode/notes/phase-2.md".to_string(),
            format: jcode_side_panel_types::SidePanelPageFormat::Markdown,
            source: jcode_side_panel_types::SidePanelPageSource::Ephemeral,
            content: "Context note content.".to_string(),
            updated_at_ms: 101,
        };
        let todo = TodoItem {
            id: "todo-phase-2".to_string(),
            content: "Normalize broker context items".to_string(),
            status: "in_progress".to_string(),
            priority: "high".to_string(),
            blocked_by: Vec::new(),
            assigned_to: Some("broker".to_string()),
        };
        let skill = BrokerSkillContextSummary {
            name: "hermes-jcode-graph".to_string(),
            description: "Summarize Hermes graph memory adapter behavior.".to_string(),
            scope: "project".to_string(),
            source: "skill_registry".to_string(),
            path: Some("/tmp/project/.jcode/skills/hermes-jcode-graph/SKILL.md".to_string()),
            allowed_tools: vec!["memory".to_string(), "goal".to_string()],
        };
        let session_hit = BrokerSearchHitContext {
            id: "42".to_string(),
            title: "Prior session evidence".to_string(),
            summary: "Prior session mentioned typed context.".to_string(),
            content: "The prior session said typed context should stay evidence.".to_string(),
            snippet: "typed context should stay evidence".to_string(),
            session_id: "ses_prior".to_string(),
            working_dir: Some("/tmp/project".to_string()),
            provider_key: Some("openai".to_string()),
            model: Some("gpt-5.4".to_string()),
            message_id: Some("msg_42".to_string()),
            message_index: Some(42),
            role: Some("assistant".to_string()),
            timestamp: Some("2026-05-10T12:00:00Z".to_string()),
            updated_at: Some("2026-05-10T12:05:00Z".to_string()),
            query: Some("typed context".to_string()),
            score: Some(0.88),
            rank: Some(2),
            matched_terms: vec!["typed".to_string(), "context".to_string()],
            metadata: json!({"channel": "session_history"}),
        };
        let conversation_hit = BrokerSearchHitContext {
            id: "3".to_string(),
            title: "Current conversation evidence".to_string(),
            summary: "Current conversation mentioned the adapter surface.".to_string(),
            content: "This turn asked for the broker_context.items contract.".to_string(),
            snippet: "broker_context.items contract".to_string(),
            session_id: "ses_contract".to_string(),
            working_dir: Some("/tmp/project".to_string()),
            provider_key: None,
            model: None,
            message_id: Some("msg_3".to_string()),
            message_index: Some(3),
            role: Some("user".to_string()),
            timestamp: None,
            updated_at: None,
            query: Some("typed context".to_string()),
            score: Some(0.74),
            rank: Some(3),
            matched_terms: vec!["context".to_string()],
            metadata: json!({"turn": 3}),
        };

        let items = vec![
            tool_broker_item("memory"),
            memory_broker_item(&memory_result, Some("/tmp/project")),
            side_panel_broker_item("ses_contract", &goal_page, Some("goal.phase-2-context")),
            side_panel_broker_item("ses_contract", &note_page, Some("goal.phase-2-context")),
            todo_broker_item("ses_contract", &todo),
            skill_broker_item(&skill, Some("/tmp/project")),
            session_search_hit_broker_item(&session_hit),
            conversation_search_hit_broker_item(&conversation_hit),
        ];

        let kinds: HashSet<String> = items.iter().map(|item| item.kind.clone()).collect();
        for expected in [
            "memory",
            "goal",
            "todo",
            "side_panel",
            "skill",
            "session_search_hit",
            "conversation_search_hit",
            "tool",
        ] {
            assert!(
                kinds.contains(expected),
                "missing context item kind {expected}"
            );
        }
        for item in &items {
            assert!(
                item.origin.tool.is_some(),
                "{} should keep a stable origin tool",
                item.kind
            );
            assert!(
                !item.scope.trim().is_empty(),
                "{} should keep a stable scope",
                item.kind
            );
            if item.kind != "tool" {
                assert!(
                    item.summary
                        .as_deref()
                        .is_some_and(|summary| !summary.trim().is_empty()),
                    "{} should have a useful summary",
                    item.kind
                );
            }
        }
    }

    #[test]
    fn broker_search_hit_items_are_evidence_not_durable_memory() {
        let hit = BrokerSearchHitContext {
            id: "7".to_string(),
            title: "Search evidence".to_string(),
            summary: "Search evidence is prompt context only.".to_string(),
            content: "Search hits should not masquerade as durable memory.".to_string(),
            snippet: "not masquerade as durable memory".to_string(),
            session_id: "ses_evidence".to_string(),
            working_dir: Some("/tmp/project".to_string()),
            provider_key: None,
            model: None,
            message_id: Some("msg_7".to_string()),
            message_index: Some(7),
            role: Some("assistant".to_string()),
            timestamp: None,
            updated_at: None,
            query: Some("durable memory".to_string()),
            score: Some(0.91),
            rank: Some(1),
            matched_terms: vec!["durable".to_string(), "memory".to_string()],
            metadata: json!({"source_session_path": "/tmp/project/.jcode/sessions/ses_evidence.json"}),
        };

        let session_item = session_search_hit_broker_item(&hit);
        let conversation_item = conversation_search_hit_broker_item(&hit);

        for item in [session_item, conversation_item] {
            assert_ne!(item.kind, "memory");
            assert_eq!(item.metadata["durable_memory"], false);
            assert_eq!(
                item.relevance
                    .as_ref()
                    .and_then(|relevance| relevance.query.as_deref()),
                Some("durable memory")
            );
            assert_eq!(item.fragments.len(), 1);
            assert_eq!(item.fragments[0].relation, "match");
            assert_eq!(
                item.fragments[0].content,
                "not masquerade as durable memory"
            );
        }
    }

    #[test]
    fn broker_context_item_order_puts_vault_evidence_before_tool_inventory() {
        let tool_item = tool_broker_item("memory");
        let vault_item = BrokerContextItem {
            id: "vault_chunk:needle".to_string(),
            kind: "vault_chunk".to_string(),
            scope: "vault".to_string(),
            content_format: "markdown".to_string(),
            title: Some("Needle note".to_string()),
            summary: Some("Exact Vault evidence.".to_string()),
            content: None,
            tags: Vec::new(),
            source: Some("duckdb_broker_store".to_string()),
            score: Some(1.0),
            origin: BrokerContextOrigin {
                tool: Some("duckdb_broker_store".to_string()),
                path: Some("TaskNotes/Needle.md".to_string()),
                ..Default::default()
            },
            relevance: None,
            fragments: Vec::new(),
            metadata: json!({"durable_memory": false}),
        };

        let items =
            broker_context_items_with_vault_priority(vec![tool_item], vec![vault_item.clone()]);

        assert_eq!(items[0], vault_item);
        assert_eq!(items[1].kind, "tool");
    }

    #[test]
    fn clio_context_packet_routes_authority_conflicts_and_hints() {
        let canonical_plan = BrokerContextItem {
            id: "vault_chunk:canonical-plan".to_string(),
            kind: "vault_chunk".to_string(),
            scope: "vault".to_string(),
            content_format: "markdown".to_string(),
            title: Some("jcode Super-Session Context Broker Plan / 9".to_string()),
            summary: Some("Clio Context Packet v1 is the active contract.".to_string()),
            content: None,
            tags: Vec::new(),
            source: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md#9".to_string()),
            score: Some(0.99),
            origin: BrokerContextOrigin {
                tool: Some("duckdb_broker_store".to_string()),
                uri: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md#9".to_string()),
                path: Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string()),
                ..Default::default()
            },
            relevance: Some(BrokerContextRelevance {
                query: Some("context packet".to_string()),
                retrieval_mode: Some("duckdb_broker_store_semantic".to_string()),
                rank: Some(1),
                ..Default::default()
            }),
            fragments: Vec::new(),
            metadata: json!({"start_line": 1660, "end_line": 1712}),
        };
        let historical_fork = BrokerContextItem {
            id: "vault_chunk:historical-fork".to_string(),
            kind: "vault_chunk".to_string(),
            scope: "vault".to_string(),
            content_format: "markdown".to_string(),
            title: Some("jcode Super-Session Context Broker Fork Plan / 9".to_string()),
            summary: Some("The fork is preserved but no longer canonical.".to_string()),
            content: None,
            tags: Vec::new(),
            source: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan-Super-Session-Fork.md#9".to_string()),
            score: Some(0.98),
            origin: BrokerContextOrigin {
                tool: Some("duckdb_broker_store".to_string()),
                uri: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan-Super-Session-Fork.md#9".to_string()),
                path: Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan-Super-Session-Fork.md".to_string()),
                ..Default::default()
            },
            relevance: Some(BrokerContextRelevance {
                query: Some("context packet".to_string()),
                retrieval_mode: Some("duckdb_broker_store_semantic".to_string()),
                rank: Some(2),
                ..Default::default()
            }),
            fragments: Vec::new(),
            metadata: json!({"start_line": 1660, "end_line": 1712}),
        };

        let packet = clio_context_packet_from_items(&[
            canonical_plan.clone(),
            historical_fork.clone(),
            tool_broker_item("memory"),
        ]);

        assert_eq!(packet.version, "clio_context_packet_v1");
        assert_eq!(packet.authority.len(), 1);
        assert_eq!(packet.authority[0].item.id, canonical_plan.id);
        assert_eq!(packet.authority[0].slot.as_deref(), Some("authority"));
        assert_eq!(
            packet.authority[0].authority_class.as_deref(),
            Some("current_project_authority")
        );
        assert_eq!(packet.authority[0].line_start, Some(1660));
        assert_eq!(packet.authority[0].line_end, Some(1712));
        assert_eq!(packet.conflicts.len(), 1);
        assert_eq!(packet.conflicts[0].item.id, historical_fork.id);
        assert_eq!(
            packet.conflicts[0].conflict_group.as_deref(),
            Some("currentness")
        );
        assert_eq!(
            packet.conflicts[0].authority_class.as_deref(),
            Some("historical_context")
        );
        assert_eq!(packet.tool_hints.len(), 1);
        assert_eq!(
            packet.tool_hints[0].authority_class.as_deref(),
            Some("procedural_hint")
        );
    }

    #[test]
    fn clio_context_packet_routes_provider_currentness_disagreement_to_conflicts() {
        let old_provider_note = BrokerContextItem {
            id: "vault_chunk:old-provider-note".to_string(),
            kind: "vault_chunk".to_string(),
            scope: "vault".to_string(),
            content_format: "markdown".to_string(),
            title: Some("Old Honcho provider note".to_string()),
            summary: Some("Older note says Honcho is the main context path.".to_string()),
            content: Some("Honcho is the active memory provider.".to_string()),
            tags: Vec::new(),
            source: Some(
                "vault://Projects/OpenClaw-Stack/Honcho-Provider-Plan.md#provider".to_string(),
            ),
            score: Some(0.99),
            origin: BrokerContextOrigin {
                tool: Some("duckdb_broker_store".to_string()),
                uri: Some(
                    "vault://Projects/OpenClaw-Stack/Honcho-Provider-Plan.md#provider".to_string(),
                ),
                path: Some("Projects/OpenClaw-Stack/Honcho-Provider-Plan.md".to_string()),
                ..Default::default()
            },
            relevance: Some(BrokerContextRelevance {
                query: Some("current Clio provider".to_string()),
                retrieval_mode: Some("duckdb_broker_store".to_string()),
                rank: Some(1),
                ..Default::default()
            }),
            fragments: Vec::new(),
            metadata: json!({"start_line": 1, "end_line": 8}),
        };

        let packet = clio_context_packet_from_items(&[old_provider_note.clone()]);

        assert_eq!(packet.conflicts.len(), 1);
        assert_eq!(packet.conflicts[0].item.id, old_provider_note.id);
        assert_eq!(
            packet.conflicts[0].conflict_group.as_deref(),
            Some("provider_currentness")
        );
        assert_eq!(
            packet.conflicts[0].workflow_status.as_deref(),
            Some("historical")
        );
    }

    #[test]
    fn clio_context_packet_does_not_treat_active_project_folder_honcho_as_historical() {
        let active_plan_note = BrokerContextItem {
            id: "vault_chunk:active-project-plan-note".to_string(),
            kind: "vault_chunk".to_string(),
            scope: "vault".to_string(),
            content_format: "markdown".to_string(),
            title: Some("Active Clio implementation note".to_string()),
            summary: Some("Current super-session implementation details.".to_string()),
            content: Some("This is an active Clio plan note in the Hermes plan folder.".to_string()),
            tags: Vec::new(),
            source: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/Clio-Implementation-Details.md#current".to_string()),
            score: Some(0.99),
            origin: BrokerContextOrigin {
                tool: Some("duckdb_broker_store".to_string()),
                uri: Some("vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/Clio-Implementation-Details.md#current".to_string()),
                path: Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/Clio-Implementation-Details.md".to_string()),
                ..Default::default()
            },
            relevance: Some(BrokerContextRelevance {
                query: Some("current Clio implementation plan".to_string()),
                retrieval_mode: Some("duckdb_broker_store".to_string()),
                rank: Some(1),
                ..Default::default()
            }),
            fragments: Vec::new(),
            metadata: json!({"start_line": 20, "end_line": 30}),
        };

        let packet = clio_context_packet_from_items(&[active_plan_note.clone()]);

        assert_eq!(packet.authority.len(), 1);
        assert_eq!(packet.authority[0].item.id, active_plan_note.id);
        assert_eq!(
            packet.authority[0].authority_class.as_deref(),
            Some("current_project_authority")
        );
        assert!(packet.conflicts.is_empty());
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_ranks_current_authority_above_historical_context_for_current_queries() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "current-file".to_string(),
                        path: "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string(),
                        title: "jcode Super-Session Context Broker Plan".to_string(),
                        checksum: "sha256:current".to_string(),
                        size_bytes: 100,
                        mtime_ns: 20,
                        frontmatter_json: r#"{"workflow_status":"active","dateModified":"2026-05-26T00:00:00-0400"}"#.to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "historical-file".to_string(),
                        path: "Archive/OpenClaw/Honcho-Provider-Plan.md".to_string(),
                        title: "Old Honcho provider plan".to_string(),
                        checksum: "sha256:historical".to_string(),
                        size_bytes: 100,
                        mtime_ns: 10,
                        frontmatter_json: r#"{"workflow_status":"historical","dateModified":"2026-04-01T00:00:00-0400"}"#.to_string(),
                        deleted_at: None,
                    },
                ],
                chunks: vec![
                    VaultChunkRecord {
                        id: "current-chunk".to_string(),
                        file_id: "current-file".to_string(),
                        path: "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string(),
                        heading: "Current provider".to_string(),
                        content: "Current Clio context provider is jcode_graph with DuckDB gates.".to_string(),
                        start_line: 10,
                        end_line: 12,
                        checksum: "sha256:current-chunk".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "historical-chunk".to_string(),
                        file_id: "historical-file".to_string(),
                        path: "Archive/OpenClaw/Honcho-Provider-Plan.md".to_string(),
                        heading: "Historical provider".to_string(),
                        content: "Current Clio provider provider provider context context broker broker was Honcho in this old note.".to_string(),
                        start_line: 1,
                        end_line: 3,
                        checksum: "sha256:historical-chunk".to_string(),
                        deleted_at: None,
                    },
                ],
                ..Default::default()
            })
            .expect("replace records");

        let items = collect_vault_context_items_with_client(
            &service.client(),
            Some("/Users/rob/Vault/Projects/Hermes-Honcho-LangGraph-Second-Brain"),
            "current Clio provider context broker",
            None,
            4,
        )
        .expect("collect context");
        let first_path = items[0].origin.path.as_deref().expect("first path");
        assert!(
            first_path.ends_with("jcode-Nervous-System-Broker-Parity-Plan.md"),
            "current authority should outrank historical context, got {items:?}"
        );

        let packet = clio_context_packet_from_items(&items);
        assert_eq!(
            packet.authority[0].source_path.as_deref(),
            Some(
                "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md"
            )
        );
        assert!(
            packet
                .conflicts
                .iter()
                .any(|item| item.source_path.as_deref()
                    == Some("Archive/OpenClaw/Honcho-Provider-Plan.md")),
            "historical provider note should be conflict evidence: {:?}",
            packet.conflicts
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_collects_vault_chunk_items_from_duckdb_store() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let _db_env = TestEnvVar::set("JCODE_BROKER_DUCKDB_PATH", db_path.as_os_str());
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![VaultFileRecord {
                    id: "file_alpha".to_string(),
                    path: "Alpha.md".to_string(),
                    title: "Alpha".to_string(),
                    checksum: "sha256:file".to_string(),
                    size_bytes: 64,
                    mtime_ns: 123,
                    frontmatter_json: "{}".to_string(),
                    deleted_at: None,
                }],
                chunks: vec![VaultChunkRecord {
                    id: "chunk_alpha".to_string(),
                    file_id: "file_alpha".to_string(),
                    path: "Alpha.md".to_string(),
                    heading: "DuckDB broker".to_string(),
                    content:
                        "DuckDB vault context should route through broker_context chunk items."
                            .to_string(),
                    start_line: 2,
                    end_line: 4,
                    checksum: "sha256:chunk".to_string(),
                    deleted_at: None,
                }],
                links: vec![VaultLinkRecord {
                    id: "link_alpha_beta".to_string(),
                    source_file_id: "file_alpha".to_string(),
                    source_path: "Alpha.md".to_string(),
                    target: "Beta".to_string(),
                    kind: "wikilink".to_string(),
                    raw: "[[Beta]]".to_string(),
                    deleted_at: None,
                }],
                tasks: vec![VaultTaskRecord {
                    id: "task_alpha".to_string(),
                    file_id: "file_alpha".to_string(),
                    path: "Alpha.md".to_string(),
                    checked: false,
                    content: "Track ingestion writer".to_string(),
                    line: 5,
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        drop(service);

        let items = collect_vault_context_items(
            Some("/tmp/project"),
            Some("DuckDB vault context Beta ingestion writer"),
            8,
        )
        .expect("collect vault context items");

        let item = items
            .iter()
            .find(|item| item.kind == "vault_chunk")
            .expect("vault chunk item");
        assert_eq!(item.kind, "vault_chunk");
        assert_eq!(item.scope, "vault");
        assert_eq!(item.content_format, "markdown");
        assert_eq!(item.origin.tool.as_deref(), Some("duckdb_broker_store"));
        assert_eq!(item.origin.path.as_deref(), Some("Alpha.md"));
        assert_eq!(item.metadata["durable_memory"], false);
        assert_eq!(item.metadata["source_checksum"], "sha256:file");
        assert!(
            item.relevance
                .as_ref()
                .expect("relevance")
                .matched_terms
                .contains(&"duckdb".to_string())
        );
        let task_item = items
            .iter()
            .find(|item| item.kind == "vault_task")
            .expect("vault task item");
        assert_eq!(task_item.origin.path.as_deref(), Some("Alpha.md"));
        assert_eq!(task_item.metadata["checked"], false);
        assert_eq!(task_item.content.as_deref(), Some("Track ingestion writer"));

        let link_item = items
            .iter()
            .find(|item| item.kind == "vault_link")
            .expect("vault link item");
        assert_eq!(link_item.origin.path.as_deref(), Some("Alpha.md"));
        assert_eq!(link_item.metadata["target"], "Beta");
        assert_eq!(link_item.content.as_deref(), Some("[[Beta]]"));
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_exposes_vault_relationship_neighborhood_for_path_queries() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let _db_env = TestEnvVar::set("JCODE_BROKER_DUCKDB_PATH", db_path.as_os_str());
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_target".to_string(),
                        path: "Plans/Target.md".to_string(),
                        title: "Target".to_string(),
                        checksum: "sha256:target".to_string(),
                        size_bytes: 64,
                        mtime_ns: 123,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_backlink".to_string(),
                        path: "Plans/Backlink.md".to_string(),
                        title: "Backlink".to_string(),
                        checksum: "sha256:backlink".to_string(),
                        size_bytes: 64,
                        mtime_ns: 124,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                links: vec![VaultLinkRecord {
                    id: "link_backlink_target".to_string(),
                    source_file_id: "file_backlink".to_string(),
                    source_path: "Plans/Backlink.md".to_string(),
                    target: "Plans/Target".to_string(),
                    kind: "wikilink".to_string(),
                    raw: "[[Plans/Target]]".to_string(),
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        drop(service);

        let items = collect_vault_context_items(
            Some("/tmp/project"),
            Some("What links or backlinks relate to Projects/Demo/Plans/Target.md?"),
            8,
        )
        .expect("collect vault context items");

        let relationship = items
            .iter()
            .find(|item| item.kind == "vault_relationship")
            .expect("relationship item");
        assert_eq!(relationship.scope, "vault");
        assert_eq!(relationship.metadata["source_kind"], "vault_relationship");
        assert_eq!(relationship.metadata["relationship"], "backlink");
        assert_eq!(relationship.metadata["source_path"], "Plans/Backlink.md");
        assert_eq!(relationship.metadata["target_path"], "Plans/Target.md");
        assert_eq!(
            relationship
                .relevance
                .as_ref()
                .and_then(|relevance| relevance.retrieval_mode.as_deref()),
            Some("duckdb_broker_store_relationship")
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_relationship_query_anchors_on_first_vault_path() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let _db_env = TestEnvVar::set("JCODE_BROKER_DUCKDB_PATH", db_path.as_os_str());
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_source".to_string(),
                        path: "Projects/OpenClaw-Stack/CURRENT.md".to_string(),
                        title: "OpenClaw Current".to_string(),
                        checksum: "sha256:source".to_string(),
                        size_bytes: 64,
                        mtime_ns: 123,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_expected_target".to_string(),
                        path: "Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App.md"
                            .to_string(),
                        title: "Hermes Centered Assistant App".to_string(),
                        checksum: "sha256:expected-target".to_string(),
                        size_bytes: 64,
                        mtime_ns: 124,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_unrelated_backlink".to_string(),
                        path: "Projects/OpenClaw-Stack/Architecture-Reset-Hermes-Personal-Runtime-and-Project-LLM-Wikis.md"
                            .to_string(),
                        title: "Runtime Wikis".to_string(),
                        checksum: "sha256:unrelated-backlink".to_string(),
                        size_bytes: 64,
                        mtime_ns: 125,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                links: vec![VaultLinkRecord {
                    id: "link_unrelated_to_expected_target".to_string(),
                    source_file_id: "file_unrelated_backlink".to_string(),
                    source_path:
                        "Projects/OpenClaw-Stack/Architecture-Reset-Hermes-Personal-Runtime-and-Project-LLM-Wikis.md"
                            .to_string(),
                    target:
                        "Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App"
                            .to_string(),
                    kind: "wikilink".to_string(),
                    raw: "[[Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App]]"
                        .to_string(),
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        drop(service);

        let items = collect_vault_context_items(
            Some("/tmp/project"),
            Some(
                "For this Vault note: Projects/OpenClaw-Stack/CURRENT.md what links, backlinks, \
                 or neighboring notes does the broker return? Specifically say whether an outlink \
                 to Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App.md appears.",
            ),
            8,
        )
        .expect("collect vault context items");

        let relationship_items: Vec<_> = items
            .iter()
            .filter(|item| item.kind == "vault_relationship")
            .collect();
        assert!(!relationship_items.is_empty());
        for item in relationship_items {
            let source_path = item.metadata["source_path"].as_str().expect("source path");
            let target_path = item.metadata["target_path"].as_str().expect("target path");
            assert!(
                source_path == "Projects/OpenClaw-Stack/CURRENT.md"
                    || target_path == "Projects/OpenClaw-Stack/CURRENT.md",
                "relationship item must involve the first Vault path anchor, got {source_path} -> {target_path}"
            );
        }
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_relationship_query_does_not_truncate_outlinks_to_search_hit_cap() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let _db_env = TestEnvVar::set("JCODE_BROKER_DUCKDB_PATH", db_path.as_os_str());
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");

        let mut files = vec![
            VaultFileRecord {
                id: "file_source".to_string(),
                path: "Projects/OpenClaw-Stack/CURRENT.md".to_string(),
                title: "OpenClaw Current".to_string(),
                checksum: "sha256:source".to_string(),
                size_bytes: 64,
                mtime_ns: 123,
                frontmatter_json: "{}".to_string(),
                deleted_at: None,
            },
            VaultFileRecord {
                id: "file_expected_target".to_string(),
                path:
                    "Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App.md"
                        .to_string(),
                title: "Hermes Centered Assistant App".to_string(),
                checksum: "sha256:expected-target".to_string(),
                size_bytes: 64,
                mtime_ns: 124,
                frontmatter_json: "{}".to_string(),
                deleted_at: None,
            },
        ];
        let mut links = vec![VaultLinkRecord {
            id: "link_source_expected_target".to_string(),
            source_file_id: "file_source".to_string(),
            source_path: "Projects/OpenClaw-Stack/CURRENT.md".to_string(),
            target: "Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App"
                .to_string(),
            kind: "wikilink".to_string(),
            raw: "[[Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App]]"
                .to_string(),
            deleted_at: None,
        }];
        for idx in 1..=4 {
            files.push(VaultFileRecord {
                id: format!("file_backlink_{idx}"),
                path: format!("Projects/OpenClaw-Stack/Backlink-{idx}.md"),
                title: format!("Backlink {idx}"),
                checksum: format!("sha256:backlink-{idx}"),
                size_bytes: 64,
                mtime_ns: 130 + idx,
                frontmatter_json: "{}".to_string(),
                deleted_at: None,
            });
            links.push(VaultLinkRecord {
                id: format!("link_backlink_{idx}_source"),
                source_file_id: format!("file_backlink_{idx}"),
                source_path: format!("Projects/OpenClaw-Stack/Backlink-{idx}.md"),
                target: "Projects/OpenClaw-Stack/CURRENT".to_string(),
                kind: "wikilink".to_string(),
                raw: "[[Projects/OpenClaw-Stack/CURRENT]]".to_string(),
                deleted_at: None,
            });
        }

        service
            .replace_vault_records(VaultRecordBatch {
                files,
                links,
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        drop(service);

        let items = collect_vault_context_items(
            Some("/tmp/project"),
            Some(
                "For this Vault note: Projects/OpenClaw-Stack/CURRENT.md what links, backlinks, \
                 or neighboring notes does the broker return? Specifically say whether an outlink \
                 to Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App.md appears.",
            ),
            8,
        )
        .expect("collect vault context items");

        assert!(
            items.iter().any(|item| {
                item.kind == "vault_relationship"
                    && item.metadata["relationship"] == "outlink"
                    && item.metadata["source_path"] == "Projects/OpenClaw-Stack/CURRENT.md"
                    && item.metadata["target_path"]
                        == "Projects/OpenClaw-Stack/Architecture-Reset-v2-Hermes-Centered-Assistant-App.md"
            }),
            "{items:?}"
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_relationship_query_handles_spaced_vault_paths() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let _db_env = TestEnvVar::set("JCODE_BROKER_DUCKDB_PATH", db_path.as_os_str());
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_source".to_string(),
                        path: "System/jcode-validation/Freshness Probe Source 2026-05-14.md"
                            .to_string(),
                        title: "Freshness Probe Source 2026-05-14".to_string(),
                        checksum: "sha256:spaced-source".to_string(),
                        size_bytes: 64,
                        mtime_ns: 123,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_target".to_string(),
                        path: "System/jcode-validation/Freshness Probe Target 2026-05-14.md"
                            .to_string(),
                        title: "Freshness Probe Target 2026-05-14".to_string(),
                        checksum: "sha256:spaced-target".to_string(),
                        size_bytes: 64,
                        mtime_ns: 124,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                links: vec![VaultLinkRecord {
                    id: "link_source_target".to_string(),
                    source_file_id: "file_source".to_string(),
                    source_path: "System/jcode-validation/Freshness Probe Source 2026-05-14.md"
                        .to_string(),
                    target: "System/jcode-validation/Freshness Probe Target 2026-05-14".to_string(),
                    kind: "wikilink".to_string(),
                    raw: "[[System/jcode-validation/Freshness Probe Target 2026-05-14]]"
                        .to_string(),
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        drop(service);

        let items = collect_vault_context_items(
            Some("/tmp/project"),
            Some(
                "For this Vault note:\n\nSystem/jcode-validation/Freshness Probe Source 2026-05-14.md\n\nWhat links, backlinks, or neighboring notes does the broker return?",
            ),
            8,
        )
        .expect("collect vault context items");

        assert!(
            items.iter().any(|item| {
                item.kind == "vault_relationship"
                    && item.metadata["relationship"] == "outlink"
                    && item.metadata["source_path"]
                        == "System/jcode-validation/Freshness Probe Source 2026-05-14.md"
                    && item.metadata["target_path"]
                        == "System/jcode-validation/Freshness Probe Target 2026-05-14.md"
            }),
            "{items:?}"
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_prefers_semantic_vault_chunks_when_embeddings_are_available() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![VaultFileRecord {
                    id: "file_semantic".to_string(),
                    path: "Semantic.md".to_string(),
                    title: "Semantic".to_string(),
                    checksum: "sha256:file-semantic".to_string(),
                    size_bytes: 64,
                    mtime_ns: 456,
                    frontmatter_json: "{}".to_string(),
                    deleted_at: None,
                }],
                chunks: vec![VaultChunkRecord {
                    id: "chunk_semantic".to_string(),
                    file_id: "file_semantic".to_string(),
                    path: "Semantic.md".to_string(),
                    heading: "Broker embeddings".to_string(),
                    content: "Stored vectors should retrieve this note without lexical overlap."
                        .to_string(),
                    start_line: 1,
                    end_line: 2,
                    checksum: "sha256:chunk-semantic".to_string(),
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        let client = service.client();
        client
            .upsert_vault_embeddings(vec![VaultEmbeddingRecord {
                id: "embedding:test-model:chunk_semantic".to_string(),
                record_id: "chunk_semantic".to_string(),
                record_kind: "vault_chunk".to_string(),
                embedding_model: "test-model".to_string(),
                embedding: vec![0.0, 1.0, 0.0],
                content_checksum: "sha256:chunk-semantic".to_string(),
                source_checksum: "sha256:file-semantic".to_string(),
                updated_at: "2026-05-11T22:15:00Z".to_string(),
                deleted_at: None,
            }])
            .expect("upsert semantic embedding");
        let _model_env = TestEnvVar::set("JCODE_BROKER_VAULT_EMBEDDING_MODEL", "test-model");

        let items = collect_vault_context_items_with_client(
            &client,
            Some("/tmp/project"),
            "unmatched-query-token",
            Some(&[0.0, 1.0, 0.0]),
            5,
        )
        .expect("collect semantic vault context");

        assert_eq!(items.len(), 1);
        let item = &items[0];
        assert_eq!(item.kind, "vault_chunk");
        assert_eq!(item.metadata["embedding_model"], "test-model");
        assert_eq!(
            item.relevance
                .as_ref()
                .and_then(|relevance| relevance.retrieval_mode.as_deref()),
            Some("duckdb_broker_store_semantic")
        );
        assert!(
            item.score.unwrap_or_default() > 0.99,
            "semantic score should carry through: {item:?}"
        );
        assert_eq!(
            item.content.as_deref(),
            Some("Stored vectors should retrieve this note without lexical overlap.")
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_promotes_exact_vault_task_item_over_generic_chunk_matches() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_task".to_string(),
                        path:
                            "TaskNotes/Polish Hermes TUI reasoning and progress display parity with Codex.md"
                                .to_string(),
                        title: "Polish Hermes TUI reasoning and progress display parity with Codex"
                            .to_string(),
                        checksum: "sha256:file-task".to_string(),
                        size_bytes: 512,
                        mtime_ns: 111,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_startup".to_string(),
                        path:
                            "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Root-Default-Startup-Pack.md"
                                .to_string(),
                        title: "Hermes Root Default Startup Pack".to_string(),
                        checksum: "sha256:file-startup".to_string(),
                        size_bytes: 512,
                        mtime_ns: 112,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                chunks: vec![
                    VaultChunkRecord {
                        id: "chunk_future_work".to_string(),
                        file_id: "file_task".to_string(),
                        path:
                            "TaskNotes/Polish Hermes TUI reasoning and progress display parity with Codex.md"
                                .to_string(),
                        heading: "Future Work".to_string(),
                        content: "## Future Work\n- [ ] Test modern Hermes TUI display with a small task.\n- [ ] If reasoning is duplicated, find the existing display/config knob before patching anything.\n- [ ] Keep Rob's preference: do not disable visible reasoning by default.\n- [ ] Prefer built-in display settings over custom code.".to_string(),
                        start_line: 31,
                        end_line: 35,
                        checksum: "sha256:chunk-future-work".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "chunk_do_not_load".to_string(),
                        file_id: "file_startup".to_string(),
                        path:
                            "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Root-Default-Startup-Pack.md"
                                .to_string(),
                        heading: "Do Not Load By Default".to_string(),
                        content: "## Do Not Load By Default\n- full `.hermes/skills/**`\n- OpenClaw runtime directories for cleanup unless doing a dependency-map pass".to_string(),
                        start_line: 70,
                        end_line: 78,
                        checksum: "sha256:chunk-do-not-load".to_string(),
                        deleted_at: None,
                    },
                ],
                tasks: vec![VaultTaskRecord {
                    id: "task_keep_reasoning".to_string(),
                    file_id: "file_task".to_string(),
                    path: "TaskNotes/Polish Hermes TUI reasoning and progress display parity with Codex.md"
                        .to_string(),
                    checked: false,
                    content: "Keep Rob's preference: do not disable visible reasoning by default."
                        .to_string(),
                    line: 34,
                    deleted_at: None,
                }],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        let client = service.client();

        let items = collect_vault_context_items_with_client(
            &client,
            Some("/tmp/project"),
            "\"Keep Rob's preference: do not disable visible reasoning by default\" unchecked task item",
            None,
            5,
        )
        .expect("collect task-item vault context");

        let first = items.first().expect("at least one vault item");
        assert_eq!(
            first.kind, "vault_task",
            "exact task-item query should surface the structured task first: {items:?}"
        );
        assert_eq!(
            first.origin.path.as_deref(),
            Some("TaskNotes/Polish Hermes TUI reasoning and progress display parity with Codex.md")
        );
        assert_eq!(first.metadata["source_kind"], "vault_task");
        assert_eq!(first.metadata["line"], 34);
        assert_eq!(
            first.content.as_deref(),
            Some("Keep Rob's preference: do not disable visible reasoning by default.")
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_ignores_query_frame_stopwords_for_broad_vault_note_lookup() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_ghostty".to_string(),
                        path: "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md".to_string(),
                        title: "Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off".to_string(),
                        checksum: "sha256:file-ghostty-broad".to_string(),
                        size_bytes: 512,
                        mtime_ns: 800,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_plan".to_string(),
                        path: "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string(),
                        title: "jcode Nervous-System Broker Parity Plan".to_string(),
                        checksum: "sha256:file-plan-broad".to_string(),
                        size_bytes: 512,
                        mtime_ns: 801,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                chunks: vec![
                    VaultChunkRecord {
                        id: "chunk_ghostty_advanced".to_string(),
                        file_id: "file_ghostty".to_string(),
                        path: "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md".to_string(),
                        heading: "Advanced Tips: Make Ghostty Even Better".to_string(),
                        content: "Install Starship as a cross-shell prompt, add fastfetch for system info, use btop for real-time monitoring, and tune the Ghostty terminal emulator for productive development.".to_string(),
                        start_line: 199,
                        end_line: 233,
                        checksum: "sha256:chunk-ghostty-broad".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "chunk_plan_setup".to_string(),
                        file_id: "file_plan".to_string(),
                        path: "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string(),
                        heading: "0.1 Plan and source-of-truth setup".to_string(),
                        content: "This system setup section works with the plan and source of truth migration.".to_string(),
                        start_line: 83,
                        end_line: 100,
                        checksum: "sha256:chunk-plan-broad".to_string(),
                        deleted_at: None,
                    },
                ],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        let client = service.client();
        client
            .upsert_vault_embeddings(vec![VaultEmbeddingRecord {
                id: "embedding:test-model:chunk_ghostty_advanced".to_string(),
                record_id: "chunk_ghostty_advanced".to_string(),
                record_kind: "vault_chunk".to_string(),
                embedding_model: "test-model".to_string(),
                embedding: vec![0.0, 1.0, 0.0],
                content_checksum: "sha256:chunk-ghostty-broad".to_string(),
                source_checksum: "sha256:file-ghostty-broad".to_string(),
                updated_at: "2026-05-14T12:20:00Z".to_string(),
                deleted_at: None,
            }])
            .expect("upsert broad semantic embedding");
        let _model_env = TestEnvVar::set("JCODE_BROKER_VAULT_EMBEDDING_MODEL", "test-model");

        let items = collect_vault_context_items_with_client(
            &client,
            Some("/tmp/project"),
            "improving a terminal emulator setup with shell prompt styling, system monitor integration, and productivity tweaks?",
            Some(&[0.0, 1.0, 0.0]),
            5,
        )
        .expect("collect broad vault-note context");

        let first = items.first().expect("at least one vault item");
        assert_eq!(
            first.origin.path.as_deref(),
            Some(
                "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md"
            ),
            "generic query frame terms should not promote the jcode plan over the target note: {items:?}"
        );
        let matched_terms = &first.relevance.as_ref().expect("relevance").matched_terms;
        assert!(!matched_terms.contains(&"with".to_string()));
        assert!(!matched_terms.contains(&"and".to_string()));
        assert_eq!(
            first
                .relevance
                .as_ref()
                .and_then(|relevance| relevance.retrieval_mode.as_deref()),
            Some("duckdb_broker_store_semantic")
        );
    }

    #[cfg(feature = "duckdb-storage")]
    #[test]
    fn broker_context_promotes_exact_vault_needle_hits_over_semantic_distractors() {
        let _guard = crate::storage::lock_test_env();
        let _env = TestHome::new();
        let db_path = _env.path().join("broker.duckdb");
        let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
        service
            .replace_vault_records(VaultRecordBatch {
                files: vec![
                    VaultFileRecord {
                        id: "file_ghostty".to_string(),
                        path: "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md".to_string(),
                        title: "Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off".to_string(),
                        checksum: "sha256:file-ghostty".to_string(),
                        size_bytes: 512,
                        mtime_ns: 789,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                    VaultFileRecord {
                        id: "file_distractors".to_string(),
                        path: "TaskNotes/jcode Coding Agent Harness.md".to_string(),
                        title: "jcode Coding Agent Harness".to_string(),
                        checksum: "sha256:file-distractors".to_string(),
                        size_bytes: 512,
                        mtime_ns: 790,
                        frontmatter_json: "{}".to_string(),
                        deleted_at: None,
                    },
                ],
                chunks: vec![
                    VaultChunkRecord {
                        id: "chunk_ghostty_starship".to_string(),
                        file_id: "file_ghostty".to_string(),
                        path: "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md".to_string(),
                        heading: "1. Install Starship Rainbow Status Bar".to_string(),
                        content: "Advanced Tips: Make Ghostty Even Better\n\n1. Install Starship Rainbow Status Bar\n\nStarship is a cross-shell prompt tool.".to_string(),
                        start_line: 199,
                        end_line: 203,
                        checksum: "sha256:chunk-ghostty-starship".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "chunk_install_one".to_string(),
                        file_id: "file_distractors".to_string(),
                        path: "TaskNotes/jcode Coding Agent Harness.md".to_string(),
                        heading: "Installation".to_string(),
                        content: "Detailed installation instructions for the coding harness.".to_string(),
                        start_line: 10,
                        end_line: 12,
                        checksum: "sha256:chunk-install-one".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "chunk_install_two".to_string(),
                        file_id: "file_distractors".to_string(),
                        path: "TaskNotes/jcode Coding Agent Harness.md".to_string(),
                        heading: "Quick Install".to_string(),
                        content: "Quick install steps for jcode.".to_string(),
                        start_line: 20,
                        end_line: 22,
                        checksum: "sha256:chunk-install-two".to_string(),
                        deleted_at: None,
                    },
                    VaultChunkRecord {
                        id: "chunk_install_three".to_string(),
                        file_id: "file_distractors".to_string(),
                        path: "TaskNotes/jcode Coding Agent Harness.md".to_string(),
                        heading: "Detailed Installation".to_string(),
                        content: "Another installation section for unrelated tooling.".to_string(),
                        start_line: 30,
                        end_line: 32,
                        checksum: "sha256:chunk-install-three".to_string(),
                        deleted_at: None,
                    },
                ],
                ..VaultRecordBatch::default()
            })
            .expect("seed broker store");
        let client = service.client();
        client
            .upsert_vault_embeddings(vec![
                VaultEmbeddingRecord {
                    id: "embedding:test-model:chunk_install_one".to_string(),
                    record_id: "chunk_install_one".to_string(),
                    record_kind: "vault_chunk".to_string(),
                    embedding_model: "test-model".to_string(),
                    embedding: vec![1.0, 0.0, 0.0],
                    content_checksum: "sha256:chunk-install-one".to_string(),
                    source_checksum: "sha256:file-distractors".to_string(),
                    updated_at: "2026-05-13T11:00:00Z".to_string(),
                    deleted_at: None,
                },
                VaultEmbeddingRecord {
                    id: "embedding:test-model:chunk_install_two".to_string(),
                    record_id: "chunk_install_two".to_string(),
                    record_kind: "vault_chunk".to_string(),
                    embedding_model: "test-model".to_string(),
                    embedding: vec![1.0, 0.0, 0.0],
                    content_checksum: "sha256:chunk-install-two".to_string(),
                    source_checksum: "sha256:file-distractors".to_string(),
                    updated_at: "2026-05-13T11:00:00Z".to_string(),
                    deleted_at: None,
                },
                VaultEmbeddingRecord {
                    id: "embedding:test-model:chunk_install_three".to_string(),
                    record_id: "chunk_install_three".to_string(),
                    record_kind: "vault_chunk".to_string(),
                    embedding_model: "test-model".to_string(),
                    embedding: vec![1.0, 0.0, 0.0],
                    content_checksum: "sha256:chunk-install-three".to_string(),
                    source_checksum: "sha256:file-distractors".to_string(),
                    updated_at: "2026-05-13T11:00:00Z".to_string(),
                    deleted_at: None,
                },
            ])
            .expect("upsert distractor embeddings");
        let _model_env = TestEnvVar::set("JCODE_BROKER_VAULT_EMBEDDING_MODEL", "test-model");

        let items = collect_vault_context_items_with_client(
            &client,
            Some("/tmp/project"),
            "Install Starship Rainbow Status Bar",
            Some(&[1.0, 0.0, 0.0]),
            3,
        )
        .expect("collect needle vault context");

        let first = items.first().expect("at least one vault item");
        assert_eq!(first.kind, "vault_chunk");
        assert_eq!(
            first.origin.path.as_deref(),
            Some(
                "TaskNotes/Ghostty Terminal Hands-On Set Up in 5 Minutes, Development Efficiency Takes Off.md"
            ),
            "exact lexical Vault needle should outrank semantic distractors: {items:?}"
        );
        assert_eq!(
            first
                .relevance
                .as_ref()
                .and_then(|relevance| relevance.retrieval_mode.as_deref()),
            Some("duckdb_broker_store")
        );
    }
}
