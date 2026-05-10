use crate::test_support::*;
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
        assert_eq!(relevance.retrieval_mode.as_deref(), Some("keyword"));
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

#[tokio::test]
async fn broker_turn_sync_persists_hermes_turn_into_project_memory() -> Result<()> {
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
            ..
        } = sync_event
        else {
            anyhow::bail!("expected broker turn synced event, got {sync_event:?}");
        };

        assert_eq!(returned_session_id, session_id);
        assert_eq!(memory_ids.len(), 1);

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
            memories.iter().any(|memory| memory
                .content
                .contains("Hermes user turn should become jcode memory")
                && memory.tags.iter().any(|tag| tag == "hermes-turn")),
            "broker context should include synced Hermes turn memory, got {memories:?}"
        );
        assert!(
            items.iter().any(|item| item.kind == "memory"
                && item
                    .content
                    .as_deref()
                    .unwrap_or_default()
                    .contains("Hermes user turn")),
            "broker context items should expose synced turn memory, got {items:?}"
        );

        Ok::<_, anyhow::Error>(())
    }
    .await;

    abort_server_and_cleanup(&server_handle, &socket_path, &debug_socket_path);
    result
}
