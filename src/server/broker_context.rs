use super::SessionAgents;
use crate::memory::TrustLevel;
use crate::memory::{MemoryCategory, MemoryEntry, MemoryManager, MemoryScope};
use crate::memory_graph::EdgeKind;
use crate::protocol::{
    BrokerContextItem, BrokerContextOrigin, BrokerContextRelevance, BrokerMemoryContextItem,
    BrokerMemoryExtractionStatus, ServerEvent,
};
use crate::todo::TodoItem;
use anyhow::{Context, Result};
use serde_json::json;
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::sync::mpsc;

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

    let source = normalized_source(source, "hermes:transcript");
    let manager = manager_for_working_dir(working_dir.as_deref());
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

    let (derived_memory_ids, extraction_status) =
        extract_derived_memories(&manager, transcript, &session_id, source, &provenance_id).await?;
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

    let memories =
        collect_broker_memories(working_dir.as_deref(), query, limit, include_provenance)?;
    let side_panel = crate::side_panel::snapshot_for_session(&session_id).unwrap_or_default();
    let todos = crate::todo::load_todos(&session_id).unwrap_or_default();
    let items = collect_context_items(
        &session_id,
        working_dir.as_deref(),
        query,
        &tool_names,
        &memories,
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
) -> Result<(Vec<String>, BrokerMemoryExtractionStatus)> {
    if !crate::memory::memory_sidecar_enabled() {
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

    let sidecar = crate::sidecar::Sidecar::new();
    let extracted = match sidecar
        .extract_memories_with_existing(transcript, &existing)
        .await
    {
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
        ids.push(manager.remember_project(entry)?);
    }

    link_derived_memories(manager, provenance_id, &ids)?;
    Ok(ids)
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

fn collect_broker_memories(
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
    include_provenance: bool,
) -> Result<Vec<BrokerMemoryContextItem>> {
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
        append_scoped_memories(
            &manager,
            MemoryScope::Project,
            "project",
            query,
            limit,
            include_provenance,
            &mut seen,
            &mut memories,
        )?;
    }

    append_scoped_memories(
        &manager,
        MemoryScope::Global,
        "global",
        query,
        limit,
        include_provenance,
        &mut seen,
        &mut memories,
    )?;

    memories.truncate(limit);
    Ok(memories)
}

fn append_scoped_memories(
    manager: &MemoryManager,
    scope: MemoryScope,
    scope_label: &str,
    query: Option<&str>,
    limit: usize,
    include_provenance: bool,
    seen: &mut HashSet<String>,
    memories: &mut Vec<BrokerMemoryContextItem>,
) -> Result<()> {
    let mut entries = match query {
        Some(query) if !query.trim().is_empty() => manager.search_scoped(query, scope)?,
        _ => manager.list_all_scoped(scope)?,
    };
    entries.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });

    for entry in entries {
        if memories.len() >= limit {
            break;
        }
        if !seen.insert(entry.id.clone()) {
            continue;
        }
        if !include_provenance && is_provenance_memory(&entry) {
            continue;
        }
        memories.push(memory_context_item(entry, scope_label));
    }

    Ok(())
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
    query: Option<&str>,
    tool_names: &[String],
    memories: &[BrokerMemoryContextItem],
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
            .enumerate()
            .map(|(index, memory)| memory_broker_item(memory, working_dir, query, index + 1)),
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

fn memory_broker_item(
    memory: &BrokerMemoryContextItem,
    working_dir: Option<&str>,
    query: Option<&str>,
    rank: usize,
) -> BrokerContextItem {
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
        score: None,
        origin: BrokerContextOrigin {
            tool: Some("memory".to_string()),
            source: memory.source.clone(),
            working_dir: working_dir.map(str::to_string),
            ..Default::default()
        },
        relevance: memory_relevance(query, rank),
        fragments: Vec::new(),
        metadata: json!({
            "category": memory.category,
            "scope": memory.scope,
        }),
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

fn memory_relevance(query: Option<&str>, rank: usize) -> Option<BrokerContextRelevance> {
    let query = query.map(str::trim).filter(|query| !query.is_empty())?;
    Some(BrokerContextRelevance {
        query: Some(query.to_string()),
        retrieval_mode: Some("keyword".to_string()),
        rank: Some(rank),
        matched_terms: query
            .split_whitespace()
            .map(|term| term.trim_matches(|ch: char| !ch.is_alphanumeric()))
            .filter(|term| !term.is_empty())
            .map(|term| term.to_ascii_lowercase())
            .collect(),
        ..Default::default()
    })
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
    use std::path::Path;

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
}
