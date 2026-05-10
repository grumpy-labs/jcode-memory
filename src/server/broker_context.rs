use super::SessionAgents;
use crate::memory::{MemoryEntry, MemoryManager, MemoryScope};
use crate::protocol::{BrokerMemoryContextItem, ServerEvent};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::sync::mpsc;

pub(super) async fn handle_broker_context(
    id: u64,
    requested_session_id: Option<String>,
    query: Option<String>,
    limit: usize,
    fallback_session_id: Option<&str>,
    sessions: &SessionAgents,
    client_event_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let event = match broker_context_event(
        id,
        requested_session_id,
        query.as_deref(),
        limit,
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

async fn broker_context_event(
    id: u64,
    requested_session_id: Option<String>,
    query: Option<&str>,
    limit: usize,
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

    let memories = collect_broker_memories(working_dir.as_deref(), query, limit)?;
    let side_panel = crate::side_panel::snapshot_for_session(&session_id).unwrap_or_default();

    Ok(ServerEvent::BrokerContext {
        id,
        session_id,
        working_dir,
        tool_names,
        memories,
        side_panel,
    })
}

fn collect_broker_memories(
    working_dir: Option<&str>,
    query: Option<&str>,
    limit: usize,
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
        memories.push(memory_context_item(entry, scope_label));
    }

    Ok(())
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
