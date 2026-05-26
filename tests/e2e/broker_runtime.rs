use crate::test_support::*;
use jcode::protocol::BrokerMemoryExtractionStatus;
use serde_json::json;
use std::collections::HashSet;

#[tokio::test]
async fn broker_headless_session_exposes_context_artifacts_over_api() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-runtime-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    std::fs::create_dir_all(&project_dir)?;
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let provider = MockProvider::new();
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        let tools =
            debug_run_command_json(debug_socket_path.clone(), "tools", Some(&session_id)).await?;
        let tool_names: HashSet<String> =
            serde_json::from_value(tools).context("decode broker tool list")?;
        let expected: HashSet<String> = [
            "conversation_search",
            "glob",
            "goal",
            "grep",
            "ls",
            "memory",
            "read",
            "session_search",
            "skill_manage",
            "swarm",
            "todo",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(tool_names, expected);

        let remember_output = debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:memory {"action":"remember","content":"Broker memory proof survives the debug API","category":"fact","scope":"project","tags":["broker-proof"]}"#,
            Some(&session_id),
        )
        .await?;
        assert!(
            remember_output
                .get("output")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .contains("Remembered fact"),
            "unexpected memory remember output: {remember_output}"
        );

        let memory_search = debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:memory {"action":"search","query":"Broker memory proof","scope":"project"}"#,
            Some(&session_id),
        )
        .await?;
        assert!(
            memory_search
                .get("output")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .contains("Broker memory proof survives the debug API"),
            "unexpected memory search output: {memory_search}"
        );

        let goal_output = debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:goal {"action":"create","title":"Broker runtime API proof","scope":"project","next_steps":["confirm history exposes context artifacts"]}"#,
            Some(&session_id),
        )
        .await?;
        assert!(
            goal_output
                .get("output")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .contains("Created goal"),
            "unexpected goal output: {goal_output}"
        );

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let events = collect_until_history_unix(&mut client, resume_id).await?;
        let history = events
            .into_iter()
            .find_map(|event| match event {
                ServerEvent::History { id, side_panel, mcp_servers, .. } if id == resume_id => {
                    Some((side_panel, mcp_servers))
                }
                _ => None,
            })
            .context("missing history event for resumed broker session")?;

        let (side_panel, mcp_servers) = history;
        assert!(mcp_servers.is_empty(), "broker should not expose MCP tools");
        assert_eq!(
            side_panel.focused_page_id.as_deref(),
            Some("goal.broker-runtime-api-proof")
        );
        let goal_page = side_panel
            .pages
            .iter()
            .find(|page| page.id == "goal.broker-runtime-api-proof")
            .context("history side panel missing broker goal artifact")?;
        assert!(goal_page
            .content
            .contains("confirm history exposes context artifacts"));

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}

#[tokio::test]
async fn typed_broker_context_api_returns_memory_tools_and_artifacts() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-context-api-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    std::fs::create_dir_all(&project_dir)?;
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let provider = MockProvider::new();
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:memory {"action":"remember","content":"Typed broker context memory proof","category":"fact","scope":"project","tags":["typed-broker-context"]}"#,
            Some(&session_id),
        )
        .await?;

        debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:goal {"action":"create","title":"Typed Broker Context API","scope":"project","next_steps":["return context without debug command strings"]}"#,
            Some(&session_id),
        )
        .await?;

        debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:todo {"todos":[{"id":"todo-broker-contract","content":"Normalize broker context item contract","status":"in_progress","priority":"high"}]}"#,
            Some(&session_id),
        )
        .await?;

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let _ = collect_until_history_unix(&mut client, resume_id).await?;

        let context_event = client
            .get_broker_context(Some(session_id.clone()), Some("Typed broker".to_string()), 8)
            .await?;

        let ServerEvent::BrokerContext {
            session_id: returned_session_id,
            working_dir,
            tool_names,
            memories,
            side_panel,
            items,
            ..
        } = context_event
        else {
            anyhow::bail!("expected broker context event, got {context_event:?}");
        };

        assert_eq!(returned_session_id, session_id);
        assert_eq!(working_dir.as_deref(), Some(project_dir.to_string_lossy().as_ref()));

        let tool_names: HashSet<String> = tool_names.into_iter().collect();
        assert!(tool_names.contains("memory"));
        assert!(tool_names.contains("goal"));
        assert!(!tool_names.contains("side_panel"));
        assert!(!tool_names.contains("bash"));

        assert!(
            memories
                .iter()
                .any(|memory| memory.content == "Typed broker context memory proof"),
            "broker context should include project memory, got {memories:?}"
        );

        let item_keys: HashSet<(String, String)> = items
            .iter()
            .map(|item| (item.kind.clone(), item.id.clone()))
            .collect();
        let memory_id = memories
            .iter()
            .find(|memory| memory.content == "Typed broker context memory proof")
            .map(|memory| memory.id.clone())
            .context("missing explicit memory in legacy memories field")?;
        assert!(
            item_keys.contains(&("memory".to_string(), memory_id.clone())),
            "broker context items should include the memory item, got {items:?}"
        );
        assert!(
            item_keys.contains(&(
                "goal".to_string(),
                "goal.typed-broker-context-api".to_string()
            )),
            "broker context items should include the goal artifact, got {items:?}"
        );
        assert!(
            item_keys.contains(&("todo".to_string(), "todo-broker-contract".to_string())),
            "broker context items should include the session todo, got {items:?}"
        );
        assert!(
            item_keys.contains(&("tool".to_string(), "memory".to_string())),
            "broker context items should include broker tool entries, got {items:?}"
        );
        let todo_item = items
            .iter()
            .find(|item| item.kind == "todo" && item.id == "todo-broker-contract")
            .context("missing todo broker item")?;
        assert_eq!(todo_item.scope, "session");
        assert_eq!(todo_item.content_format, "plain_text");
        assert_eq!(todo_item.origin.tool.as_deref(), Some("todo"));
        assert_eq!(todo_item.origin.session_id.as_deref(), Some(session_id.as_str()));
        assert_eq!(
            todo_item.title.as_deref(),
            Some("Normalize broker context item contract")
        );
        assert_eq!(todo_item.metadata["status"], "in_progress");
        assert_eq!(todo_item.metadata["priority"], "high");
        let goal_item = items
            .iter()
            .find(|item| item.kind == "goal" && item.id == "goal.typed-broker-context-api")
            .context("missing goal broker item")?;
        assert_eq!(goal_item.content_format, "markdown");
        assert_eq!(goal_item.origin.tool.as_deref(), Some("goal"));
        assert!(
            goal_item
                .origin
                .path
                .as_deref()
                .unwrap_or_default()
                .ends_with("goal.typed-broker-context-api.md")
        );
        let tool_item = items
            .iter()
            .find(|item| item.kind == "tool" && item.id == "memory")
            .context("missing memory tool broker item")?;
        assert_eq!(tool_item.origin.tool.as_deref(), Some("tool_registry"));
        assert_eq!(tool_item.metadata["name"], "memory");
        let memory_item = items
            .iter()
            .find(|item| item.kind == "memory" && item.id == memory_id)
            .context("missing memory broker item")?;
        assert_eq!(memory_item.origin.tool.as_deref(), Some("memory"));
        assert_eq!(
            memory_item.origin.working_dir.as_deref(),
            Some(project_dir.to_string_lossy().as_ref())
        );
        let relevance = memory_item
            .relevance
            .as_ref()
            .context("memory broker item should include relevance")?;
        assert_eq!(relevance.query.as_deref(), Some("Typed broker"));
        assert!(
            matches!(
                relevance.retrieval_mode.as_deref(),
                Some("keyword") | Some("semantic_cascade")
            ),
            "unexpected retrieval mode: {:?}",
            relevance.retrieval_mode
        );
        assert!(relevance.rank.is_some());

        assert_eq!(
            side_panel.focused_page_id.as_deref(),
            Some("goal.typed-broker-context-api")
        );
        let goal_page = side_panel
            .pages
            .iter()
            .find(|page| page.id == "goal.typed-broker-context-api")
            .context("broker context missing goal artifact")?;
        assert!(goal_page
            .content
            .contains("return context without debug command strings"));

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}

#[cfg(feature = "duckdb-storage")]
#[tokio::test]
async fn broker_vault_refresh_reconciles_vault_without_restarting_broker() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-vault-refresh-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    let vault_dir = runtime_dir.join("vault");
    let db_path = runtime_dir.join("broker.duckdb");
    std::fs::create_dir_all(&project_dir)?;
    std::fs::create_dir_all(&vault_dir)?;
    std::fs::write(
        vault_dir.join("Needle.md"),
        "# Gateway Refresh\n\nInitial gateway refresh proof.\n",
    )?;
    let _db = EnvVarGuard::set("JCODE_BROKER_DUCKDB_PATH", &db_path);
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let provider = MockProvider::new();
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let _ = collect_until_history_unix(&mut client, resume_id).await?;

        let refresh_event = client
            .refresh_broker_vault(
                vault_dir.to_string_lossy().to_string(),
                false,
                "jcode-local-embedding".to_string(),
                0,
            )
            .await?;
        let ServerEvent::BrokerVaultRefreshed {
            vault,
            new_files,
            updated_files,
            unchanged_files,
            tombstoned_files,
            embedded_chunks,
            counts,
            ..
        } = refresh_event
        else {
            anyhow::bail!("expected broker vault refresh event, got {refresh_event:?}");
        };
        assert_eq!(vault, vault_dir.to_string_lossy());
        assert_eq!(new_files, 1);
        assert_eq!(updated_files, 0);
        assert_eq!(unchanged_files, 0);
        assert_eq!(tombstoned_files, 0);
        assert_eq!(embedded_chunks, 0);
        assert_eq!(counts.active_vault_file, 1);
        assert!(counts.active_vault_chunk > 0);

        let context_event = client
            .get_broker_context(
                Some(session_id.clone()),
                Some("Initial gateway refresh proof".to_string()),
                4,
            )
            .await?;
        let ServerEvent::BrokerContext { items, .. } = context_event else {
            anyhow::bail!("expected broker context event, got {context_event:?}");
        };
        assert!(
            items.iter().any(|item| {
                item.kind == "vault_chunk"
                    && item
                        .origin
                        .path
                        .as_deref()
                        .is_some_and(|path| path.ends_with("Needle.md"))
                    && item
                        .content
                        .as_deref()
                        .unwrap_or_default()
                        .contains("Initial gateway refresh proof")
            }),
            "broker context should include freshly refreshed Vault chunk, got {items:?}"
        );

        std::fs::write(
            vault_dir.join("Needle.md"),
            "# Gateway Refresh\n\nUpdated in-service refresh proof.\n",
        )?;
        let refresh_event = client
            .refresh_broker_vault(
                vault_dir.to_string_lossy().to_string(),
                false,
                "jcode-local-embedding".to_string(),
                0,
            )
            .await?;
        let ServerEvent::BrokerVaultRefreshed {
            new_files,
            updated_files,
            unchanged_files,
            embedded_chunks,
            counts,
            ..
        } = refresh_event
        else {
            anyhow::bail!("expected second broker vault refresh event, got {refresh_event:?}");
        };
        assert_eq!(new_files, 0);
        assert_eq!(updated_files, 1);
        assert_eq!(unchanged_files, 0);
        assert_eq!(embedded_chunks, 0);
        assert_eq!(counts.active_vault_file, 1);

        let context_event = client
            .get_broker_context(
                Some(session_id),
                Some("Updated in-service refresh proof".to_string()),
                4,
            )
            .await?;
        let ServerEvent::BrokerContext { items, .. } = context_event else {
            anyhow::bail!("expected updated broker context event, got {context_event:?}");
        };
        assert!(
            items.iter().any(|item| {
                item.kind == "vault_chunk"
                    && item
                        .content
                        .as_deref()
                        .unwrap_or_default()
                        .contains("Updated in-service refresh proof")
            }),
            "broker context should include updated Vault chunk without a restart, got {items:?}"
        );

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}

#[tokio::test]
async fn broker_context_emits_phase_2_context_evidence_candidates_and_boundaries() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-phase-2-context-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    std::fs::create_dir_all(&project_dir)?;
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let mut prior_session =
        Session::create_with_id("prior-phase-2-session".to_string(), None, None);
    prior_session.working_dir = Some(project_dir.to_string_lossy().to_string());
    prior_session.provider_key = Some("mock".to_string());
    prior_session.model = Some("mock".to_string());
    prior_session.add_message(
        Role::Assistant,
        vec![ContentBlock::Text {
            text: "Prior phase two context evidence should appear as a session search hit."
                .to_string(),
            cache_control: None,
        }],
    );
    prior_session.add_message(
        Role::Assistant,
        vec![ContentBlock::ToolUse {
            id: "tool-noise".to_string(),
            name: "bash".to_string(),
            input: json!({"cmd": "phase two context evidence hidden tool noise"}),
        }],
    );
    prior_session.save()?;

    let skill_dir = project_dir.join(".jcode/skills/phase-two-context");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: phase-two-context\ndescription: Phase two context evidence summaries for broker tests.\nallowed-tools: memory, session_search\n---\n# Phase Two Context\nThis full body should not be injected automatically.\n",
    )?;

    let provider = MockProvider::new();
    provider.queue_response(vec![
        StreamEvent::TextDelta(
            "Assistant noted current phase two context evidence from the active transcript."
                .to_string(),
        ),
        StreamEvent::MessageEnd {
            stop_reason: Some("end_turn".to_string()),
        },
    ]);
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:goal {"action":"create","title":"Phase Two Context Goal","scope":"project","next_steps":["keep goals in typed broker context"]}"#,
            Some(&session_id),
        )
        .await?;

        debug_run_command_json(
            debug_socket_path.clone(),
            r#"tool:todo {"todos":[{"id":"todo-phase-2-context","content":"Finish Phase 2 context parity","status":"pending","priority":"high"}]}"#,
            Some(&session_id),
        )
        .await?;

        jcode::side_panel::write_markdown_page(
            &session_id,
            "artifact.phase-two-context",
            Some("Phase Two Artifact"),
            "Phase two context evidence artifact stays typed, not presentation UI.",
            false,
        )?;

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let _ = collect_until_history_unix(&mut client, resume_id).await?;

        let message_id =
            client
                .send_message("Current phase two context evidence belongs in conversation search.")
                .await?;
        let _ = collect_until_done_unix(&mut client, message_id).await?;

        let context_event = client
            .get_broker_context(
                Some(session_id.clone()),
                Some("phase two context evidence".to_string()),
                8,
            )
            .await?;

        let ServerEvent::BrokerContext {
            tool_names, items, ..
        } = context_event
        else {
            anyhow::bail!("expected broker context event, got {context_event:?}");
        };

        let tool_names: HashSet<String> = tool_names.into_iter().collect();
        for expected in [
            "memory",
            "goal",
            "todo",
            "session_search",
            "conversation_search",
            "swarm",
            "skill_manage",
        ] {
            assert!(tool_names.contains(expected), "missing broker tool {expected}");
        }
        for forbidden in [
            "side_panel",
            "agentgrep",
            "bash",
            "write",
            "edit",
            "patch",
            "browser",
            "gmail",
            "mcp",
            "selfdev",
            "debug_socket",
            "schedule_ambient",
        ] {
            assert!(
                !tool_names.contains(forbidden),
                "broker context should not expose noisy/operator tool {forbidden}: {tool_names:?}"
            );
        }

        let session_hit = items
            .iter()
            .find(|item| item.kind == "session_search_hit")
            .context("missing session search hit item")?;
        assert_eq!(
            session_hit.origin.session_id.as_deref(),
            Some("prior-phase-2-session")
        );
        assert!(
            session_hit
                .fragments
                .iter()
                .any(|fragment| fragment.content.contains("session search hit")),
            "session hit should include snippet fragments, got {session_hit:?}"
        );
        assert_eq!(session_hit.metadata["durable_memory"], false);
        assert_ne!(session_hit.kind, "memory");
        assert!(
            !session_hit
                .content
                .as_deref()
                .unwrap_or_default()
                .contains("hidden tool noise"),
            "session evidence should hide tool-only noise"
        );

        let conversation_hit = items
            .iter()
            .find(|item| item.kind == "conversation_search_hit")
            .context("missing conversation search hit item")?;
        assert_eq!(
            conversation_hit.origin.session_id.as_deref(),
            Some(session_id.as_str())
        );
        assert!(
            conversation_hit
                .fragments
                .iter()
                .any(|fragment| fragment.content.contains("conversation search")),
            "conversation hit should include active transcript snippet, got {conversation_hit:?}"
        );

        let skill_item = items
            .iter()
            .find(|item| item.kind == "skill" && item.id == "skill:phase-two-context")
            .context("missing project-local skill summary item")?;
        assert_eq!(
            skill_item.summary.as_deref(),
            Some("Phase two context evidence summaries for broker tests.")
        );
        assert_eq!(skill_item.metadata["name"], "phase-two-context");
        assert_eq!(
            skill_item.metadata["allowed_tools"],
            json!(["memory", "session_search"])
        );
        assert!(
            !skill_item
                .content
                .as_deref()
                .unwrap_or_default()
                .contains("full body should not be injected"),
            "broker should not auto-inject full skill body"
        );

        assert!(
            items
                .iter()
                .any(|item| item.kind == "goal" && item.id == "goal.phase-two-context-goal"),
            "goal item should remain in typed context, got {items:?}"
        );
        assert!(
            items
                .iter()
                .any(|item| item.kind == "todo" && item.id == "todo-phase-2-context"),
            "todo item should remain in typed context, got {items:?}"
        );
        assert!(
            items
                .iter()
                .any(|item| item.kind == "side_panel"
                    && item.id == "artifact.phase-two-context"
                    && item
                        .content
                        .as_deref()
                        .unwrap_or_default()
                        .contains("typed, not presentation UI")),
            "non-goal side-panel artifact should remain a typed artifact, got {items:?}"
        );

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}

#[tokio::test]
async fn broker_turn_sync_persists_hermes_turn_as_hidden_provenance() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-turn-sync-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    std::fs::create_dir_all(&project_dir)?;
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let provider = MockProvider::new();
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let _ = collect_until_history_unix(&mut client, resume_id).await?;

        let sync_event = client
            .sync_broker_turn(
                Some(session_id.clone()),
                "Hermes user turn should become jcode memory".to_string(),
                "Hermes assistant reply was observed by the broker".to_string(),
                Some("hermes".to_string()),
            )
            .await?;

        let ServerEvent::BrokerTurnSynced {
            session_id: returned_session_id,
            memory_ids,
            provenance_memory_ids,
            derived_memory_ids,
            extraction_status,
            ..
        } = sync_event
        else {
            anyhow::bail!("expected broker turn synced event, got {sync_event:?}");
        };

        assert_eq!(returned_session_id, session_id);
        assert_eq!(memory_ids.len(), 1);
        assert_eq!(provenance_memory_ids, memory_ids);
        assert!(derived_memory_ids.is_empty());
        assert_eq!(
            extraction_status,
            BrokerMemoryExtractionStatus::StoredProvenance
        );

        let context_event = client
            .get_broker_context(
                Some(session_id.clone()),
                Some("Hermes user turn should become jcode memory".to_string()),
                8,
            )
            .await?;

        let ServerEvent::BrokerContext {
            memories, items, ..
        } = context_event
        else {
            anyhow::bail!("expected broker context event, got {context_event:?}");
        };

        assert!(
            memories.iter().all(|memory| !memory
                .content
                .contains("Hermes user turn should become jcode memory")),
            "default broker context should hide synced Hermes provenance, got {memories:?}"
        );
        assert!(
            items.iter().all(|item| item
                .content
                .as_deref()
                .map(|content| !content.contains("Hermes user turn"))
                .unwrap_or(true)),
            "default broker context items should hide synced turn provenance, got {items:?}"
        );

        let provenance_context_event = client
            .get_broker_context_with_options(
                Some(session_id.clone()),
                Some("Hermes user turn should become jcode memory".to_string()),
                8,
                true,
            )
            .await?;

        let ServerEvent::BrokerContext {
            memories, items, ..
        } = provenance_context_event
        else {
            anyhow::bail!("expected broker context event, got {provenance_context_event:?}");
        };

        assert!(
            memories.iter().any(|memory| memory
                .content
                .contains("Hermes user turn should become jcode memory")
                && memory.tags.iter().any(|tag| tag == "broker-provenance")
                && memory.tags.iter().any(|tag| tag == "hermes-turn")),
            "provenance broker context should include synced Hermes turn memory, got {memories:?}"
        );
        assert!(
            items.iter().any(|item| item.kind == "memory"
                && item.tags.iter().any(|tag| tag == "broker-provenance")
                && item
                    .content
                    .as_deref()
                    .unwrap_or_default()
                    .contains("Hermes user turn")),
            "explicit provenance context items should expose synced turn memory, got {items:?}"
        );

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}

#[tokio::test]
async fn broker_transcript_sync_stores_provenance_and_skips_without_sidecar() -> Result<()> {
    let _env = setup_test_env()?;
    let _profile = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let runtime_dir = short_runtime_dir(format!(
        "jcode-broker-transcript-sync-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let project_dir = runtime_dir.join("project");
    std::fs::create_dir_all(&project_dir)?;
    let socket_path = runtime_dir.join("jcode.sock");
    let debug_socket_path = runtime_dir.join("jcode-debug.sock");

    let provider = MockProvider::new();
    let provider: Arc<dyn jcode::provider::Provider> = Arc::new(provider);
    let server_instance =
        server::Server::new_with_paths(provider, socket_path.clone(), debug_socket_path.clone());
    let server_handle = tokio::spawn(async move { server_instance.run().await });

    let result = async {
        wait_for_server_ready(&socket_path, &debug_socket_path).await?;

        let create_command = format!("create_session:{}", project_dir.display());
        let session_id =
            debug_create_headless_session_with_command(debug_socket_path.clone(), &create_command)
                .await?;

        let mut client = server::Client::connect_with_path(socket_path.clone()).await?;
        let resume_id = client.resume_session(&session_id).await?;
        let _ = collect_until_history_unix(&mut client, resume_id).await?;

        let sync_event = client
            .sync_broker_transcript(
                Some(session_id.clone()),
                "User: Remember Rob prefers focused broker tests.\nAssistant: Stored for extraction."
                    .to_string(),
                Some("hermes:session_end".to_string()),
            )
            .await?;

        let ServerEvent::BrokerTranscriptSynced {
            session_id: returned_session_id,
            memory_ids,
            provenance_memory_ids,
            derived_memory_ids,
            extraction_status,
            ..
        } = sync_event
        else {
            anyhow::bail!("expected broker transcript synced event, got {sync_event:?}");
        };

        assert_eq!(returned_session_id, session_id);
        assert_eq!(memory_ids.len(), 1);
        assert_eq!(provenance_memory_ids, memory_ids);
        assert!(derived_memory_ids.is_empty());
        assert_eq!(
            extraction_status,
            BrokerMemoryExtractionStatus::SkippedSidecarDisabled
        );

        let default_context = client
            .get_broker_context(
                Some(session_id.clone()),
                Some("focused broker tests".to_string()),
                8,
            )
            .await?;
        let ServerEvent::BrokerContext {
            memories, items, ..
        } = default_context
        else {
            anyhow::bail!("expected broker context event, got {default_context:?}");
        };
        assert!(
            memories
                .iter()
                .all(|memory| !memory.content.contains("focused broker tests")),
            "default broker context should hide transcript provenance, got {memories:?}"
        );
        assert!(
            items.iter().all(|item| item
                .content
                .as_deref()
                .map(|content| !content.contains("focused broker tests"))
                .unwrap_or(true)),
            "default broker context items should hide transcript provenance, got {items:?}"
        );

        let provenance_context = client
            .get_broker_context_with_options(
                Some(session_id.clone()),
                Some("focused broker tests".to_string()),
                8,
                true,
            )
            .await?;
        let ServerEvent::BrokerContext { memories, .. } = provenance_context else {
            anyhow::bail!("expected broker context event, got {provenance_context:?}");
        };
        assert!(
            memories.iter().any(|memory| memory
                .content
                .contains("focused broker tests")
                && memory.tags.iter().any(|tag| tag == "broker-provenance")
                && memory.tags.iter().any(|tag| tag == "broker-transcript-sync")),
            "explicit provenance context should include synced transcript, got {memories:?}"
        );

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}
