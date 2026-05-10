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
