use super::SessionAgents;
use crate::memory::TrustLevel;
use crate::memory::{MemoryCategory, MemoryEntry, MemoryManager, MemoryScope};
use crate::memory_graph::EdgeKind;
use crate::protocol::{
    BrokerContextFragment, BrokerContextItem, BrokerContextOrigin, BrokerContextRelevance,
    BrokerMemoryContextItem, BrokerMemoryExtractionStatus, ServerEvent,
};
use crate::todo::TodoItem;
use anyhow::{Context, Result};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::sync::mpsc;

type TranscriptExtractionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<crate::sidecar::ExtractedMemory>>> + Send + 'a>>;

const BROKER_SEMANTIC_THRESHOLD: f32 = crate::memory::EMBEDDING_SIMILARITY_THRESHOLD;

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
        id, session_id, manager, transcript, source, true, extractor,
    )
    .await
}

async fn broker_transcript_sync_event_for_manager_with_gate<E>(
    id: u64,
    session_id: String,
    manager: &MemoryManager,
    transcript: &str,
    source: &str,
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

    let (working_dir, mut tool_names) = {
        let agent_guard = agent.lock().await;
        (
            agent_guard.working_dir().map(str::to_string),
            agent_guard.tool_names().await,
        )
    };
    tool_names.sort();

    let memory_results =
        collect_broker_memory_results(working_dir.as_deref(), query, limit, include_provenance)?;
    let memories: Vec<BrokerMemoryContextItem> = memory_results
        .iter()
        .map(|result| result.memory.clone())
        .collect();
    let side_panel = crate::side_panel::snapshot_for_session(&session_id).unwrap_or_default();
    let todos = crate::todo::load_todos(&session_id).unwrap_or_default();
    let items = collect_context_items(
        &session_id,
        working_dir.as_deref(),
        &tool_names,
        &memory_results,
        &side_panel,
        &todos,
    );

    Ok(ServerEvent::BrokerContext {
        id,
        session_id,
        working_dir,
        tool_names,
        items,
        memories,
        side_panel,
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
        .filter(|entry| entry.active && !is_provenance_memory(entry))
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

// Populated by later Phase 2 emitters; kept here so the item mapper contract is stable.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct BrokerSkillContextSummary {
    name: String,
    description: String,
    scope: String,
    source: String,
    path: Option<String>,
    allowed_tools: Vec<String>,
}

#[allow(dead_code)]
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

fn is_provenance_memory(entry: &MemoryEntry) -> bool {
    entry.tags.iter().any(|tag| tag == "broker-provenance")
        || matches!(&entry.category, MemoryCategory::Custom(category) if category == "provenance")
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

    items
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
    BrokerContextItem {
        id: memory.id.clone(),
        kind: "memory".to_string(),
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
        .filter(|term| seen.insert((*term).to_string()))
        .map(str::to_string)
        .collect()
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
        assert_eq!(memory_ids.len(), 2);
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
}
