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
