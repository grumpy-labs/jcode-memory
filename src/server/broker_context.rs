use super::SessionAgents;
use crate::memory::{MemoryEntry, MemoryManager, MemoryScope};
use crate::protocol::{BrokerContextItem, BrokerMemoryContextItem, ServerEvent};
use crate::todo::TodoItem;
use anyhow::{Context, Result};
use serde_json::json;
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
    let todos = crate::todo::load_todos(&session_id).unwrap_or_default();
    let items = collect_context_items(&session_id, &tool_names, &memories, &side_panel, &todos);

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

fn collect_context_items(
    session_id: &str,
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
    items.extend(memories.iter().map(memory_broker_item));
    items.extend(
        side_panel
            .pages
            .iter()
            .map(|page| side_panel_broker_item(page, side_panel.focused_page_id.as_deref())),
    );
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
        metadata: json!({
            "name": tool_name,
        }),
    }
}

fn memory_broker_item(memory: &BrokerMemoryContextItem) -> BrokerContextItem {
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
        metadata: json!({
            "category": memory.category,
            "scope": memory.scope,
        }),
    }
}

fn side_panel_broker_item(
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
        metadata: json!({
            "status": todo.status,
            "priority": todo.priority,
            "blocked_by": todo.blocked_by,
            "assigned_to": todo.assigned_to,
        }),
    }
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
