#[test]
fn test_request_roundtrip() -> Result<()> {
    let req = Request::Message {
        id: 1,
        content: "hello".to_string(),
        images: vec![],
        system_reminder: None,
    };
    let json = serde_json::to_string(&req)?;
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 1);
    Ok(())
}

#[test]
fn test_compacted_history_request_roundtrip() -> Result<()> {
    let req = Request::GetCompactedHistory {
        id: 7,
        visible_messages: 64,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"get_compacted_history\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 7);
    let Request::GetCompactedHistory {
        visible_messages, ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(visible_messages, 64);
    Ok(())
}

#[test]
fn test_broker_context_request_roundtrip() -> Result<()> {
    let req = Request::BrokerContext {
        id: 12,
        session_id: Some("ses_broker_123".to_string()),
        query: Some("project memory".to_string()),
        limit: 5,
        include_provenance: true,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"broker_context\""));
    assert!(json.contains("\"include_provenance\":true"));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 12);
    let Request::BrokerContext {
        session_id,
        query,
        limit,
        include_provenance,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(session_id.as_deref(), Some("ses_broker_123"));
    assert_eq!(query.as_deref(), Some("project memory"));
    assert_eq!(limit, 5);
    assert!(include_provenance);
    Ok(())
}

#[test]
fn test_broker_context_request_defaults_provenance_off() -> Result<()> {
    let decoded = parse_request_json(
        r#"{"type":"broker_context","id":13,"session_id":"ses_broker_123","query":"project memory"}"#,
    )?;
    let Request::BrokerContext {
        include_provenance,
        limit,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert!(!include_provenance);
    assert_eq!(limit, 8);
    Ok(())
}

#[test]
fn test_broker_turn_sync_roundtrip_has_extraction_status() -> Result<()> {
    let req = Request::BrokerTurnSync {
        id: 14,
        session_id: Some("ses_broker_123".to_string()),
        user_content: "remember hidden provenance".to_string(),
        assistant_content: "stored for audit".to_string(),
        source: Some("hermes".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"broker_turn_sync\""));
    let decoded = parse_request_json(&json)?;
    let Request::BrokerTurnSync {
        session_id,
        user_content,
        assistant_content,
        source,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(session_id.as_deref(), Some("ses_broker_123"));
    assert_eq!(user_content, "remember hidden provenance");
    assert_eq!(assistant_content, "stored for audit");
    assert_eq!(source.as_deref(), Some("hermes"));

    let event = ServerEvent::BrokerTurnSynced {
        id: 14,
        session_id: "ses_broker_123".to_string(),
        memory_ids: vec!["mem_prov_1".to_string()],
        provenance_memory_ids: vec!["mem_prov_1".to_string()],
        derived_memory_ids: Vec::new(),
        extraction_status: BrokerMemoryExtractionStatus::StoredProvenance,
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"broker_turn_synced\""));
    assert!(json.contains("\"extraction_status\":\"stored_provenance\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::BrokerTurnSynced {
        memory_ids,
        provenance_memory_ids,
        derived_memory_ids,
        extraction_status,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(memory_ids, vec!["mem_prov_1"]);
    assert_eq!(provenance_memory_ids, vec!["mem_prov_1"]);
    assert!(derived_memory_ids.is_empty());
    assert_eq!(
        extraction_status,
        BrokerMemoryExtractionStatus::StoredProvenance
    );
    Ok(())
}

#[test]
fn test_broker_transcript_sync_roundtrip_has_extraction_status() -> Result<()> {
    let req = Request::BrokerTranscriptSync {
        id: 15,
        session_id: Some("ses_broker_123".to_string()),
        transcript: "user: remember that the broker extracts durable facts".to_string(),
        source: Some("hermes:pre_compress".to_string()),
        surface_session_id: Some("hermes_surface_456".to_string()),
        parent_segment_id: Some("hermes_parent_123".to_string()),
        surface: Some("hermes".to_string()),
        runtime_summary: Some(
            "Hermes runtime summary: active task is preserving true compression context."
                .to_string(),
        ),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"broker_transcript_sync\""));
    let decoded = parse_request_json(&json)?;
    let Request::BrokerTranscriptSync {
        session_id,
        transcript,
        source,
        surface_session_id,
        parent_segment_id,
        surface,
        runtime_summary,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(session_id.as_deref(), Some("ses_broker_123"));
    assert!(transcript.contains("durable facts"));
    assert_eq!(source.as_deref(), Some("hermes:pre_compress"));
    assert_eq!(surface_session_id.as_deref(), Some("hermes_surface_456"));
    assert_eq!(parent_segment_id.as_deref(), Some("hermes_parent_123"));
    assert_eq!(surface.as_deref(), Some("hermes"));
    assert_eq!(
        runtime_summary.as_deref(),
        Some("Hermes runtime summary: active task is preserving true compression context.")
    );

    let event = ServerEvent::BrokerTranscriptSynced {
        id: 15,
        session_id: "ses_broker_123".to_string(),
        memory_ids: vec!["mem_prov_1".to_string(), "mem_derived_1".to_string()],
        provenance_memory_ids: vec!["mem_prov_1".to_string()],
        derived_memory_ids: vec!["mem_derived_1".to_string()],
        extraction_status: BrokerMemoryExtractionStatus::Extracted,
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"broker_transcript_synced\""));
    assert!(json.contains("\"extraction_status\":\"extracted\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::BrokerTranscriptSynced {
        memory_ids,
        provenance_memory_ids,
        derived_memory_ids,
        extraction_status,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(memory_ids, vec!["mem_prov_1", "mem_derived_1"]);
    assert_eq!(provenance_memory_ids, vec!["mem_prov_1"]);
    assert_eq!(derived_memory_ids, vec!["mem_derived_1"]);
    assert_eq!(extraction_status, BrokerMemoryExtractionStatus::Extracted);
    Ok(())
}

#[test]
fn test_broker_vault_refresh_roundtrip_has_counts() -> Result<()> {
    let decoded = parse_request_json(
        r#"{"type":"broker_vault_refresh","id":16,"vault":"/srv/hermes-jcode/vault","embed_missing":true,"embedding_model":"jcode-local-embedding","embedding_limit":10000}"#,
    )?;
    assert_eq!(decoded.id(), 16);
    let Request::BrokerVaultRefresh {
        vault,
        embed_missing,
        embedding_model,
        embedding_limit,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(vault, "/srv/hermes-jcode/vault");
    assert!(embed_missing);
    assert_eq!(embedding_model, "jcode-local-embedding");
    assert_eq!(embedding_limit, 10000);

    let event = ServerEvent::BrokerVaultRefreshed {
        id: 16,
        vault: "/srv/hermes-jcode/vault".to_string(),
        db: Some("/srv/hermes-jcode/broker/hermes-vault.duckdb".to_string()),
        new_files: 0,
        updated_files: 1,
        unchanged_files: 462,
        tombstoned_files: 0,
        renamed_files: 0,
        embedded_chunks: 63,
        counts: BrokerVaultRefreshCounts {
            active_vault_file: 463,
            active_vault_chunk: 5357,
            active_vault_embedding: 5357,
            active_vault_task: 2141,
            active_graph_edge: 10476,
        },
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"broker_vault_refreshed\""));
    assert!(json.contains("\"embedded_chunks\":63"));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::BrokerVaultRefreshed {
        updated_files,
        embedded_chunks,
        counts,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(updated_files, 1);
    assert_eq!(embedded_chunks, 63);
    assert_eq!(counts.active_vault_embedding, 5357);
    Ok(())
}

#[test]
fn test_rewind_request_roundtrip() -> Result<()> {
    let req = Request::Rewind {
        id: 8,
        message_index: 3,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"rewind\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 8);
    let Request::Rewind { message_index, .. } = decoded else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(message_index, 3);
    Ok(())
}

#[test]
fn test_rewind_undo_request_roundtrip() -> Result<()> {
    let req = Request::RewindUndo { id: 9 };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"rewind_undo\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 9);
    let Request::RewindUndo { .. } = decoded else {
        return Err(anyhow!("wrong request type"));
    };
    Ok(())
}

#[test]
fn test_rename_session_request_roundtrip() -> Result<()> {
    let req = Request::RenameSession {
        id: 10,
        title: Some("Release planning".to_string()),
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"rename_session\""));
    assert!(json.contains("\"title\":\"Release planning\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 10);
    let Request::RenameSession { title, .. } = decoded else {
        return Err(anyhow!("wrong request type"));
    };
    assert_eq!(title.as_deref(), Some("Release planning"));
    Ok(())
}

#[test]
fn test_rename_session_clear_request_roundtrip_omits_title() -> Result<()> {
    let req = Request::RenameSession {
        id: 11,
        title: None,
    };
    let json = serde_json::to_string(&req)?;
    assert!(json.contains("\"type\":\"rename_session\""));
    assert!(!json.contains("\"title\""));
    let decoded = parse_request_json(&json)?;
    assert_eq!(decoded.id(), 11);
    let Request::RenameSession { title, .. } = decoded else {
        return Err(anyhow!("wrong request type"));
    };
    assert!(title.is_none());
    Ok(())
}

#[test]
fn test_event_roundtrip() -> Result<()> {
    let event = ServerEvent::TextDelta {
        text: "hello".to_string(),
    };
    let json = encode_event(&event);
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::TextDelta { text } = decoded else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(text, "hello");
    Ok(())
}

#[test]
fn test_session_renamed_event_roundtrip() -> Result<()> {
    let event = ServerEvent::SessionRenamed {
        session_id: "sess_123".to_string(),
        title: Some("Release planning".to_string()),
        display_title: "Release planning".to_string(),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"session_renamed\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::SessionRenamed {
        session_id,
        title,
        display_title,
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(session_id, "sess_123");
    assert_eq!(title.as_deref(), Some("Release planning"));
    assert_eq!(display_title, "Release planning");
    Ok(())
}

#[test]
fn test_interrupted_event_decodes_from_json() -> Result<()> {
    let json = r#"{"type":"interrupted"}"#;
    let decoded = parse_event_json(json)?;
    let ServerEvent::Interrupted = decoded else {
        return Err(anyhow!("wrong event type"));
    };
    Ok(())
}

#[test]
fn test_connection_type_event_roundtrip() -> Result<()> {
    let event = ServerEvent::ConnectionType {
        connection: "websocket".to_string(),
    };
    let json = encode_event(&event);
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::ConnectionType { connection } = decoded else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(connection, "websocket");
    Ok(())
}

#[test]
fn test_status_detail_event_roundtrip() -> Result<()> {
    let event = ServerEvent::StatusDetail {
        detail: "reusing websocket".to_string(),
    };
    let json = encode_event(&event);
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::StatusDetail { detail } = decoded else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(detail, "reusing websocket");
    Ok(())
}

#[test]
fn test_generated_image_event_roundtrip() -> Result<()> {
    let event = ServerEvent::GeneratedImage {
        id: "ig_123".to_string(),
        path: "/tmp/generated.png".to_string(),
        metadata_path: Some("/tmp/generated.json".to_string()),
        output_format: "png".to_string(),
        revised_prompt: Some("A polished image prompt".to_string()),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"generated_image\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::GeneratedImage {
        id,
        path,
        metadata_path,
        output_format,
        revised_prompt,
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(id, "ig_123");
    assert_eq!(path, "/tmp/generated.png");
    assert_eq!(metadata_path.as_deref(), Some("/tmp/generated.json"));
    assert_eq!(output_format, "png");
    assert_eq!(revised_prompt.as_deref(), Some("A polished image prompt"));
    Ok(())
}

#[test]
fn test_interrupted_event_roundtrip() -> Result<()> {
    let event = ServerEvent::Interrupted;
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"interrupted\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::Interrupted = decoded else {
        return Err(anyhow!("wrong event type"));
    };
    Ok(())
}

#[test]
fn test_history_event_decodes_without_compaction_mode_for_older_servers() -> Result<()> {
    let json = r#"{
            "type":"history",
            "id":1,
            "session_id":"ses_test_123",
            "messages":[],
            "provider_name":"openai",
            "provider_model":"gpt-5.4",
            "available_models":["gpt-5.4"],
            "connection_type":"websocket"
        }"#;
    let decoded = parse_event_json(json)?;
    let ServerEvent::History {
        provider_name,
        provider_model,
        available_models,
        connection_type,
        compaction_mode,
        side_panel,
        ..
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(provider_name.as_deref(), Some("openai"));
    assert_eq!(provider_model.as_deref(), Some("gpt-5.4"));
    assert_eq!(available_models, vec!["gpt-5.4"]);
    assert_eq!(connection_type.as_deref(), Some("websocket"));
    assert_eq!(
        compaction_mode,
        jcode_config_types::CompactionMode::Reactive
    );
    assert!(!side_panel.has_pages());
    Ok(())
}

#[test]
fn test_history_event_roundtrip_preserves_side_panel_snapshot() -> Result<()> {
    let event = ServerEvent::History {
        id: 101,
        session_id: "ses_test_456".to_string(),
        messages: vec![HistoryMessage {
            role: "assistant".to_string(),
            content: "hello".to_string(),
            tool_calls: None,
            tool_data: None,
        }],
        images: Vec::new(),
        provider_name: Some("openai".to_string()),
        provider_model: Some("gpt-5.4".to_string()),
        available_models: vec!["gpt-5.4".to_string()],
        available_model_routes: Vec::new(),
        mcp_servers: Vec::new(),
        skills: Vec::new(),
        total_tokens: None,
        all_sessions: Vec::new(),
        client_count: None,
        is_canary: None,
        reload_recovery: None,
        server_version: None,
        server_name: None,
        server_icon: None,
        server_has_update: None,
        was_interrupted: None,
        connection_type: Some("websocket".to_string()),
        status_detail: None,
        upstream_provider: None,
        reasoning_effort: None,
        service_tier: None,
        subagent_model: None,
        autoreview_enabled: None,
        autojudge_enabled: None,
        compaction_mode: jcode_config_types::CompactionMode::Reactive,
        activity: None,
        side_panel: jcode_side_panel_types::SidePanelSnapshot {
            focused_page_id: Some("page-1".to_string()),
            pages: vec![jcode_side_panel_types::SidePanelPage {
                id: "page-1".to_string(),
                title: "Notes".to_string(),
                file_path: "/tmp/notes.md".to_string(),
                format: jcode_side_panel_types::SidePanelPageFormat::Markdown,
                source: jcode_side_panel_types::SidePanelPageSource::Managed,
                content: "# Notes".to_string(),
                updated_at_ms: 42,
            }],
        },
    };
    let json = encode_event(&event);
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::History {
        id,
        side_panel,
        messages,
        provider_name,
        provider_model,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected History event"));
    };
    assert_eq!(id, 101);
    assert_eq!(provider_name.as_deref(), Some("openai"));
    assert_eq!(provider_model.as_deref(), Some("gpt-5.4"));
    assert_eq!(messages.len(), 1);
    assert_eq!(side_panel.focused_page_id.as_deref(), Some("page-1"));
    assert_eq!(side_panel.pages.len(), 1);
    assert_eq!(side_panel.pages[0].title, "Notes");
    assert_eq!(side_panel.pages[0].content, "# Notes");
    Ok(())
}

#[test]
fn test_broker_context_event_roundtrip() -> Result<()> {
    let event = ServerEvent::BrokerContext {
        id: 33,
        session_id: "ses_broker_456".to_string(),
        working_dir: Some("/tmp/project".to_string()),
        tool_names: vec!["goal".to_string(), "memory".to_string()],
        items: vec![
            BrokerContextItem {
                id: "mem_1".to_string(),
                kind: "memory".to_string(),
                scope: "project".to_string(),
                content_format: "plain_text".to_string(),
                title: Some("fact".to_string()),
                summary: Some("Project memory".to_string()),
                content: Some("Project memory".to_string()),
                tags: vec!["broker".to_string()],
                source: Some("ses_broker_456".to_string()),
                score: None,
                origin: BrokerContextOrigin {
                    tool: Some("memory".to_string()),
                    source: Some("ses_broker_456".to_string()),
                    session_id: Some("ses_broker_456".to_string()),
                    working_dir: Some("/tmp/project".to_string()),
                    ..Default::default()
                },
                relevance: Some(BrokerContextRelevance {
                    query: Some("project memory".to_string()),
                    retrieval_mode: Some("keyword".to_string()),
                    rank: Some(1),
                    matched_terms: vec!["project".to_string(), "memory".to_string()],
                    ..Default::default()
                }),
                fragments: vec![BrokerContextFragment {
                    relation: "self".to_string(),
                    content: "Project memory".to_string(),
                    content_format: "plain_text".to_string(),
                    ..Default::default()
                }],
                metadata: serde_json::json!({"category": "fact"}),
            },
            BrokerContextItem {
                id: "todo_1".to_string(),
                kind: "todo".to_string(),
                scope: "session".to_string(),
                content_format: "plain_text".to_string(),
                title: Some("Check broker context".to_string()),
                summary: Some("pending/high".to_string()),
                content: Some("Check broker context".to_string()),
                tags: vec!["pending".to_string(), "high".to_string()],
                source: Some("ses_broker_456".to_string()),
                score: None,
                origin: BrokerContextOrigin {
                    tool: Some("todo".to_string()),
                    session_id: Some("ses_broker_456".to_string()),
                    source: Some("ses_broker_456".to_string()),
                    ..Default::default()
                },
                relevance: None,
                fragments: Vec::new(),
                metadata: serde_json::json!({"status": "pending", "priority": "high"}),
            },
            BrokerContextItem {
                id: "memory".to_string(),
                kind: "tool".to_string(),
                scope: "session".to_string(),
                content_format: "plain_text".to_string(),
                title: Some("memory".to_string()),
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
                metadata: serde_json::json!({"name": "memory"}),
            },
            BrokerContextItem {
                id: "session:ses_prior_123:42".to_string(),
                kind: "session_search_hit".to_string(),
                scope: "project".to_string(),
                content_format: "plain_text".to_string(),
                title: Some("Prior session match".to_string()),
                summary: Some("Search hit from a related broker session".to_string()),
                content: Some("The graph adapter should keep provenance with retrieved context.".to_string()),
                tags: vec!["search_hit".to_string(), "session".to_string()],
                source: Some("session_search".to_string()),
                score: Some(0.87),
                origin: BrokerContextOrigin {
                    tool: Some("session_search".to_string()),
                    source: Some("session_search".to_string()),
                    session_id: Some("ses_prior_123".to_string()),
                    working_dir: Some("/tmp/project".to_string()),
                    provider_key: Some("openai".to_string()),
                    model: Some("gpt-5.4".to_string()),
                    message_id: Some("msg_42".to_string()),
                    message_index: Some(42),
                    role: Some("assistant".to_string()),
                    timestamp: Some("2026-05-10T12:00:00Z".to_string()),
                    updated_at: Some("2026-05-10T12:05:00Z".to_string()),
                    ..Default::default()
                },
                relevance: Some(BrokerContextRelevance {
                    query: Some("project memory".to_string()),
                    retrieval_mode: Some("session_search".to_string()),
                    score: Some(0.87),
                    rank: Some(2),
                    matched_terms: vec!["project".to_string(), "memory".to_string()],
                    exact_match: Some(false),
                }),
                fragments: vec![BrokerContextFragment {
                    relation: "match".to_string(),
                    content: "keep provenance with retrieved context".to_string(),
                    content_format: "plain_text".to_string(),
                    role: Some("assistant".to_string()),
                    message_index: Some(42),
                    message_id: Some("msg_42".to_string()),
                    timestamp: Some("2026-05-10T12:00:00Z".to_string()),
                }],
                metadata: serde_json::json!({"channel": "session_history"}),
            },
            BrokerContextItem {
                id: "conversation:ses_broker_456:3".to_string(),
                kind: "conversation_search_hit".to_string(),
                scope: "session".to_string(),
                content_format: "plain_text".to_string(),
                title: Some("Current conversation match".to_string()),
                summary: Some("Search hit from the active broker conversation".to_string()),
                content: Some("Hermes asked whether this context belongs in the prompt.".to_string()),
                tags: vec!["search_hit".to_string(), "conversation".to_string()],
                source: Some("conversation_search".to_string()),
                score: Some(0.74),
                origin: BrokerContextOrigin {
                    tool: Some("conversation_search".to_string()),
                    source: Some("conversation_search".to_string()),
                    session_id: Some("ses_broker_456".to_string()),
                    message_id: Some("msg_3".to_string()),
                    message_index: Some(3),
                    role: Some("user".to_string()),
                    ..Default::default()
                },
                relevance: Some(BrokerContextRelevance {
                    query: Some("project memory".to_string()),
                    retrieval_mode: Some("conversation_search".to_string()),
                    score: Some(0.74),
                    rank: Some(3),
                    matched_terms: vec!["context".to_string(), "prompt".to_string()],
                    exact_match: Some(false),
                }),
                fragments: vec![BrokerContextFragment {
                    relation: "match".to_string(),
                    content: "belongs in the prompt".to_string(),
                    content_format: "plain_text".to_string(),
                    role: Some("user".to_string()),
                    message_index: Some(3),
                    message_id: Some("msg_3".to_string()),
                    ..Default::default()
                }],
                metadata: serde_json::json!({"turn": 3}),
            },
        ],
        memories: vec![BrokerMemoryContextItem {
            id: "mem_1".to_string(),
            category: "fact".to_string(),
            scope: "project".to_string(),
            content: "Project memory".to_string(),
            tags: vec!["broker".to_string()],
            source: Some("ses_broker_456".to_string()),
        }],
        side_panel: jcode_side_panel_types::SidePanelSnapshot {
            focused_page_id: Some("goal.project-memory".to_string()),
            pages: vec![jcode_side_panel_types::SidePanelPage {
                id: "goal.project-memory".to_string(),
                title: "Project Memory".to_string(),
                file_path: "/tmp/project/.jcode/goal.md".to_string(),
                format: jcode_side_panel_types::SidePanelPageFormat::Markdown,
                source: jcode_side_panel_types::SidePanelPageSource::Managed,
                content: "# Goal".to_string(),
                updated_at_ms: 77,
            }],
        },
        packet: Some(ClioContextPacketV1 {
            active_task: vec![ClioContextPacketItem {
                item: BrokerContextItem {
                    id: "goal_packet_1".to_string(),
                    kind: "goal".to_string(),
                    scope: "session".to_string(),
                    content_format: "plain_text".to_string(),
                    title: Some("Implement packet spine".to_string()),
                    summary: Some("Active task is Clio Context Packet v1.".to_string()),
                    content: None,
                    tags: Vec::new(),
                    source: Some("ses_broker_456".to_string()),
                    score: None,
                    origin: BrokerContextOrigin {
                        tool: Some("goal".to_string()),
                        session_id: Some("ses_broker_456".to_string()),
                        ..Default::default()
                    },
                    relevance: None,
                    fragments: Vec::new(),
                    metadata: serde_json::json!({"status": "active"}),
                },
                slot: Some("active_task".to_string()),
                source_uri: None,
                source_path: None,
                line_start: None,
                line_end: None,
                authority_class: Some("active_task_note".to_string()),
                workflow_status: None,
                why_included: Some("current active goal".to_string()),
                conflict_group: None,
            }],
            authority: vec![ClioContextPacketItem {
                item: BrokerContextItem {
                    id: "vault_authority_1".to_string(),
                    kind: "vault_chunk".to_string(),
                    scope: "project".to_string(),
                    content_format: "markdown".to_string(),
                    title: Some("jcode Super-Session Context Broker Plan / §9".to_string()),
                    summary: Some("Clio Context Packet v1 is mandatory.".to_string()),
                    content: None,
                    tags: vec!["authority".to_string()],
                    source: Some("vault://plan#9".to_string()),
                    score: Some(0.99),
                    origin: BrokerContextOrigin {
                        tool: Some("vault".to_string()),
                        path: Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string()),
                        uri: Some("vault://plan#9".to_string()),
                        ..Default::default()
                    },
                    relevance: Some(BrokerContextRelevance {
                        query: Some("context packet".to_string()),
                        retrieval_mode: Some("duckdb_broker_store_semantic".to_string()),
                        rank: Some(1),
                        ..Default::default()
                    }),
                    fragments: Vec::new(),
                    metadata: serde_json::json!({"source_kind": "vault_chunk"}),
                },
                slot: Some("authority".to_string()),
                source_uri: Some("vault://plan#9".to_string()),
                source_path: Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md".to_string()),
                line_start: Some(1660),
                line_end: Some(1712),
                authority_class: Some("current_project_authority".to_string()),
                workflow_status: Some("active".to_string()),
                why_included: Some("current canonical plan".to_string()),
                conflict_group: None,
            }],
            ..Default::default()
        }),
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"broker_context\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::BrokerContext {
        id,
        session_id,
        working_dir,
        tool_names,
        items,
        memories,
        side_panel,
        packet,
    } = decoded
    else {
        return Err(anyhow!("expected BrokerContext event"));
    };
    assert_eq!(id, 33);
    assert_eq!(session_id, "ses_broker_456");
    assert_eq!(working_dir.as_deref(), Some("/tmp/project"));
    assert_eq!(tool_names, vec!["goal", "memory"]);
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].kind, "memory");
    assert_eq!(items[0].content_format, "plain_text");
    assert_eq!(items[0].origin.tool.as_deref(), Some("memory"));
    assert_eq!(
        items[0]
            .relevance
            .as_ref()
            .and_then(|relevance| relevance.query.as_deref()),
        Some("project memory")
    );
    assert_eq!(items[0].fragments[0].relation, "self");
    assert_eq!(items[0].metadata["category"], "fact");
    assert_eq!(items[1].kind, "todo");
    assert_eq!(items[1].content_format, "plain_text");
    assert_eq!(items[1].origin.tool.as_deref(), Some("todo"));
    assert_eq!(items[1].metadata["status"], "pending");
    assert_eq!(items[2].kind, "tool");
    assert_eq!(items[2].origin.tool.as_deref(), Some("tool_registry"));
    assert_eq!(items[2].metadata["name"], "memory");
    assert_eq!(items[3].kind, "session_search_hit");
    assert_eq!(items[3].scope, "project");
    assert_eq!(items[3].origin.tool.as_deref(), Some("session_search"));
    assert_eq!(items[3].origin.message_index, Some(42));
    assert_eq!(
        items[3]
            .relevance
            .as_ref()
            .and_then(|relevance| relevance.retrieval_mode.as_deref()),
        Some("session_search")
    );
    assert_eq!(items[3].fragments[0].relation, "match");
    assert_eq!(items[3].metadata["channel"], "session_history");
    assert_eq!(items[4].kind, "conversation_search_hit");
    assert_eq!(items[4].scope, "session");
    assert_eq!(
        items[4].origin.tool.as_deref(),
        Some("conversation_search")
    );
    assert_eq!(items[4].origin.role.as_deref(), Some("user"));
    assert_eq!(
        items[4]
            .relevance
            .as_ref()
            .and_then(|relevance| relevance.retrieval_mode.as_deref()),
        Some("conversation_search")
    );
    assert_eq!(items[4].fragments[0].message_id.as_deref(), Some("msg_3"));
    assert_eq!(memories[0].scope, "project");
    assert_eq!(memories[0].content, "Project memory");
    assert_eq!(
        side_panel.focused_page_id.as_deref(),
        Some("goal.project-memory")
    );
    let packet = packet.ok_or_else(|| anyhow!("missing Clio Context Packet v1"))?;
    assert_eq!(packet.version, "clio_context_packet_v1");
    assert_eq!(packet.active_task[0].slot.as_deref(), Some("active_task"));
    assert_eq!(
        packet.authority[0].authority_class.as_deref(),
        Some("current_project_authority")
    );
    assert_eq!(packet.authority[0].line_start, Some(1660));
    assert_eq!(packet.authority[0].line_end, Some(1712));
    assert_eq!(
        packet.authority[0].source_path.as_deref(),
        Some("Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md")
    );
    Ok(())
}

#[test]
fn test_compacted_history_event_roundtrip() -> Result<()> {
    let event = ServerEvent::CompactedHistory {
        id: 77,
        session_id: "ses_compact_123".to_string(),
        messages: vec![HistoryMessage {
            role: "assistant".to_string(),
            content: "older response".to_string(),
            tool_calls: None,
            tool_data: None,
        }],
        images: Vec::new(),
        compacted_total: 128,
        compacted_visible: 64,
        compacted_remaining: 64,
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"compacted_history\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::CompactedHistory {
        id,
        session_id,
        messages,
        compacted_total,
        compacted_visible,
        compacted_remaining,
        ..
    } = decoded
    else {
        return Err(anyhow!("expected CompactedHistory event"));
    };
    assert_eq!(id, 77);
    assert_eq!(session_id, "ses_compact_123");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content, "older response");
    assert_eq!(compacted_total, 128);
    assert_eq!(compacted_visible, 64);
    assert_eq!(compacted_remaining, 64);
    Ok(())
}

#[test]
fn test_side_panel_state_event_roundtrip() -> Result<()> {
    let event = ServerEvent::SidePanelState {
        snapshot: jcode_side_panel_types::SidePanelSnapshot {
            focused_page_id: Some("page-1".to_string()),
            pages: vec![jcode_side_panel_types::SidePanelPage {
                id: "page-1".to_string(),
                title: "Notes".to_string(),
                file_path: "/tmp/notes.md".to_string(),
                format: jcode_side_panel_types::SidePanelPageFormat::Markdown,
                source: jcode_side_panel_types::SidePanelPageSource::Managed,
                content: "updated".to_string(),
                updated_at_ms: 99,
            }],
        },
    };
    let json = encode_event(&event);
    assert!(json.contains("\"type\":\"side_panel_state\""));
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::SidePanelState { snapshot } = decoded else {
        return Err(anyhow!("expected SidePanelState event"));
    };
    assert_eq!(snapshot.focused_page_id.as_deref(), Some("page-1"));
    assert_eq!(snapshot.pages.len(), 1);
    assert_eq!(snapshot.pages[0].title, "Notes");
    assert_eq!(snapshot.pages[0].content, "updated");
    Ok(())
}

#[test]
fn test_error_event_retry_after_roundtrip() -> Result<()> {
    let event = ServerEvent::Error {
        id: 42,
        message: "rate limited".to_string(),
        retry_after_secs: Some(17),
    };
    let json = encode_event(&event);
    let decoded = parse_event_json(json.trim())?;
    let ServerEvent::Error {
        id,
        message,
        retry_after_secs,
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(id, 42);
    assert_eq!(message, "rate limited");
    assert_eq!(retry_after_secs, Some(17));
    Ok(())
}

#[test]
fn test_error_event_retry_after_back_compat_default() -> Result<()> {
    let json = r#"{"type":"error","id":7,"message":"oops"}"#;
    let decoded = parse_event_json(json)?;
    let ServerEvent::Error {
        id,
        message,
        retry_after_secs,
    } = decoded
    else {
        return Err(anyhow!("wrong event type"));
    };
    assert_eq!(id, 7);
    assert_eq!(message, "oops");
    assert_eq!(retry_after_secs, None);
    Ok(())
}
