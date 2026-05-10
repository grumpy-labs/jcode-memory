use super::*;
use crate::message::{Message, ToolDefinition};
use crate::provider::{EventStream, Provider};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashSet;

struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        crate::env::set_var(key, value);
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        crate::env::remove_var(key);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => crate::env::set_var(self.key, value),
            None => crate::env::remove_var(self.key),
        }
    }
}

struct MockProvider;

#[async_trait]
impl Provider for MockProvider {
    async fn complete(
        &self,
        _messages: &[Message],
        _tools: &[ToolDefinition],
        _system: &str,
        _resume_session_id: Option<&str>,
    ) -> anyhow::Result<EventStream> {
        Err(anyhow::anyhow!(
            "Mock provider should not be used for streaming completions in tool registry tests"
        ))
    }

    fn name(&self) -> &str {
        "mock"
    }

    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(MockProvider)
    }
}

#[tokio::test]
async fn test_tool_definitions_are_sorted() {
    // Create registry with mock provider
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;

    // Get definitions multiple times and verify they're always in the same order
    let defs1 = registry.definitions(None).await;
    let defs2 = registry.definitions(None).await;

    // Should have the same order
    assert_eq!(defs1.len(), defs2.len());
    for (d1, d2) in defs1.iter().zip(defs2.iter()) {
        assert_eq!(d1.name, d2.name);
    }

    // Verify they're sorted alphabetically
    let names: Vec<&str> = defs1.iter().map(|d| d.name.as_str()).collect();
    let mut sorted_names = names.clone();
    sorted_names.sort();
    assert_eq!(
        names, sorted_names,
        "Tool definitions should be sorted alphabetically"
    );
}

#[tokio::test]
async fn broker_profile_keeps_exact_context_broker_tool_set() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Broker).await;
    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();
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

    assert_eq!(names, expected);
}

#[test]
fn registry_profile_marks_operator_presentation_boundary() {
    assert!(RegistryProfile::Full.exposes_operator_presentation_tools());
    assert!(!RegistryProfile::Harness.exposes_operator_presentation_tools());
    assert!(!RegistryProfile::Broker.exposes_operator_presentation_tools());
}

#[tokio::test(flavor = "current_thread")]
async fn broker_goal_writes_context_artifact_without_side_panel_tool() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let project = temp.path().join("repo");
    std::fs::create_dir_all(&project).expect("project dir");
    let home = temp.path().to_string_lossy().to_string();
    let _home = EnvVarGuard::set("JCODE_HOME", &home);

    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Broker).await;
    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();
    assert!(!names.contains("side_panel"));
    assert!(names.contains("goal"));

    let ctx = ToolContext {
        session_id: "ses_broker_goal_artifact".to_string(),
        message_id: "msg1".to_string(),
        tool_call_id: "tool1".to_string(),
        working_dir: Some(project),
        stdin_request_tx: None,
        graceful_shutdown_signal: None,
        execution_mode: ToolExecutionMode::AgentTurn,
    };

    let create = registry
        .execute(
            "goal",
            serde_json::json!({
                "action": "create",
                "title": "Broker keeps context artifacts",
                "scope": "project",
                "next_steps": ["keep the broker headless"]
            }),
            ctx,
        )
        .await
        .expect("create goal through broker profile");

    assert!(create.output.contains("Created goal"));
    let snapshot = crate::side_panel::snapshot_for_session("ses_broker_goal_artifact")
        .expect("side panel snapshot");
    assert_eq!(
        snapshot.focused_page_id.as_deref(),
        Some("goal.broker-keeps-context-artifacts")
    );
    let page = snapshot
        .pages
        .iter()
        .find(|page| page.id == "goal.broker-keeps-context-artifacts")
        .expect("goal context artifact page");
    assert!(page.content.contains("keep the broker headless"));
}

#[tokio::test]
async fn harness_profile_excludes_product_integrations_by_default() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Harness).await;
    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();

    for kept in [
        "agentgrep",
        "apply_patch",
        "bash",
        "batch",
        "conversation_search",
        "goal",
        "memory",
        "selfdev",
        "session_search",
        "skill_manage",
        "subagent",
        "swarm",
        "todo",
    ] {
        assert!(
            names.contains(kept),
            "harness profile should keep standalone harness tool {kept}"
        );
    }

    for excluded in [
        "browser",
        "gmail",
        "open",
        "schedule",
        "side_panel",
        "webfetch",
        "websearch",
    ] {
        assert!(
            !names.contains(excluded),
            "harness profile should gate product integration tool {excluded}"
        );
    }
}

#[cfg(not(feature = "product-tools"))]
#[tokio::test]
async fn full_profile_excludes_product_tools_without_feature() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Full).await;
    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();

    for excluded in ["browser", "gmail", "webfetch", "websearch"] {
        assert!(
            !names.contains(excluded),
            "default build should require product-tools feature for {excluded}"
        );
    }

    for tool_name in ["goal", "memory", "schedule", "side_panel"] {
        assert!(
            names.contains(tool_name),
            "default build should keep non-product tool {tool_name}"
        );
    }
}

#[cfg(feature = "product-tools")]
#[tokio::test]
async fn full_profile_includes_product_tools_with_feature() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Full).await;
    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();

    for included in ["browser", "gmail", "webfetch", "websearch"] {
        assert!(
            names.contains(included),
            "product-tools feature should include product tool {included}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registry_new_from_env_defaults_to_harness_profile() {
    let _guard = crate::storage::lock_test_env();
    let _env = EnvVarGuard::remove("JCODE_TOOL_PROFILE");
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_from_env(provider).await;

    assert_eq!(registry.profile(), RegistryProfile::Harness);
    assert!(registry.tool_names().await.contains(&"memory".to_string()));
    assert!(!registry.tool_names().await.contains(&"gmail".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn registry_new_from_env_uses_broker_profile() {
    let _guard = crate::storage::lock_test_env();
    let _env = EnvVarGuard::set("JCODE_TOOL_PROFILE", "broker");
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_from_env(provider).await;

    assert_eq!(registry.profile(), RegistryProfile::Broker);
    assert!(registry.tool_names().await.contains(&"memory".to_string()));
    assert!(!registry.tool_names().await.contains(&"bash".to_string()));
}

#[cfg(feature = "product-tools")]
#[tokio::test(flavor = "current_thread")]
async fn registry_new_from_env_allows_full_product_profile() {
    let _guard = crate::storage::lock_test_env();
    let _env = EnvVarGuard::set("JCODE_TOOL_PROFILE", "full");
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_from_env(provider).await;

    assert_eq!(registry.profile(), RegistryProfile::Full);
    assert!(registry.tool_names().await.contains(&"browser".to_string()));
    assert!(registry.tool_names().await.contains(&"gmail".to_string()));
}

#[tokio::test]
async fn broker_profile_skips_dynamic_product_tools() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Broker).await;

    registry.register_selfdev_tools().await;
    registry.register_ambient_tools().await;
    registry.register_mcp_tools(None, None, None).await;

    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();

    for excluded in [
        "debug_socket",
        "end_ambient_cycle",
        "mcp",
        "request_permission",
        "schedule_ambient",
        "send_message",
        "selfdev",
    ] {
        assert!(
            !names.contains(excluded),
            "broker profile should skip dynamic tool {excluded}"
        );
    }
}

#[tokio::test]
async fn harness_profile_skips_dynamic_product_tools() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new_with_profile(provider, RegistryProfile::Harness).await;

    registry.register_ambient_tools().await;
    registry.register_mcp_tools(None, None, None).await;

    let names: HashSet<String> = registry.tool_names().await.into_iter().collect();

    for excluded in [
        "end_ambient_cycle",
        "mcp",
        "request_permission",
        "schedule_ambient",
        "send_message",
    ] {
        assert!(
            !names.contains(excluded),
            "harness profile should skip dynamic product tool {excluded}"
        );
    }
}

struct BareSchemaTool;

#[async_trait]
impl Tool for BareSchemaTool {
    fn name(&self) -> &str {
        "bare_schema"
    }

    fn description(&self) -> &str {
        "Test tool without an explicit intent property."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string"}
            }
        })
    }

    async fn execute(&self, _input: Value, _ctx: ToolContext) -> Result<ToolOutput> {
        Ok(ToolOutput::new("ok"))
    }
}

#[test]
fn tool_definitions_do_not_auto_inject_intent() {
    let def = BareSchemaTool.to_definition();
    assert!(def.input_schema["properties"]["intent"].is_null());
}

#[tokio::test]
async fn first_party_tool_definitions_include_optional_intent_explicitly() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;
    registry.register_ambient_tools().await;

    let defs = registry.definitions(None).await;
    assert!(!defs.is_empty());

    for def in defs {
        let schema = &def.input_schema;
        if schema["type"] != "object" {
            continue;
        }

        assert_eq!(
            schema["properties"]["intent"]["type"], "string",
            "{} should explicitly define optional intent in its schema",
            def.name
        );
        assert!(
            schema["properties"]["intent"]["description"]
                .as_str()
                .unwrap_or_default()
                .contains("display only"),
            "{} intent description should say it is display-only",
            def.name
        );
        let required = schema["required"].as_array().cloned().unwrap_or_default();
        assert!(
            !required.iter().any(|value| value == "intent"),
            "{} must not require intent",
            def.name
        );
    }
}

#[test]
fn test_resolve_tool_name_oauth_aliases() {
    assert_eq!(Registry::resolve_tool_name("file_grep"), "grep");
    assert_eq!(Registry::resolve_tool_name("file_read"), "read");
    assert_eq!(Registry::resolve_tool_name("file_write"), "write");
    assert_eq!(Registry::resolve_tool_name("file_edit"), "edit");
    assert_eq!(Registry::resolve_tool_name("file_glob"), "glob");
    assert_eq!(Registry::resolve_tool_name("shell_exec"), "bash");
    assert_eq!(Registry::resolve_tool_name("task_runner"), "subagent");
    assert_eq!(Registry::resolve_tool_name("task"), "subagent");
    assert_eq!(Registry::resolve_tool_name("launch"), "open");
    assert_eq!(Registry::resolve_tool_name("todo_read"), "todo");
    assert_eq!(Registry::resolve_tool_name("todo_write"), "todo");
    assert_eq!(Registry::resolve_tool_name("todoread"), "todo");
    assert_eq!(Registry::resolve_tool_name("todowrite"), "todo");
    assert_eq!(Registry::resolve_tool_name("bash"), "bash");
    assert_eq!(Registry::resolve_tool_name("grep"), "grep");
    assert_eq!(Registry::resolve_tool_name("batch"), "batch");
    assert_eq!(Registry::resolve_tool_name("memory"), "memory");
}

#[tokio::test]
async fn test_batch_resolves_oauth_names() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;
    let temp_dir = std::env::temp_dir();
    let temp_dir_str = temp_dir.to_string_lossy().to_string();

    let ctx = ToolContext {
        session_id: "test".to_string(),
        message_id: "test".to_string(),
        tool_call_id: "test".to_string(),
        working_dir: Some(temp_dir),
        stdin_request_tx: None,
        graceful_shutdown_signal: None,
        execution_mode: ToolExecutionMode::Direct,
    };

    let result = registry
        .execute(
            "file_grep",
            serde_json::json!({"pattern": "nonexistent_xyz", "path": temp_dir_str}),
            ctx,
        )
        .await;
    assert!(result.is_ok(), "file_grep should resolve to grep tool");
}

#[tokio::test]
async fn test_definitions_keep_batch_schema_generic() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;

    let defs = registry.definitions(None).await;
    let batch_def = defs
        .iter()
        .find(|def| def.name == "batch")
        .expect("batch definition should exist");

    assert!(batch_def.input_schema["properties"]["tool_calls"]["items"]["oneOf"].is_null());
    assert!(
        batch_def.input_schema["properties"]["tool_calls"]["items"]["required"]
            .as_array()
            .map(|required| required.iter().any(|value| value == "tool"))
            .unwrap_or(false)
    );
    assert!(
        batch_def.input_schema["properties"]["tool_calls"]["items"]["properties"]["parameters"]
            .is_null()
    );
}

#[test]
fn resolve_tool_name_maps_communicate_to_swarm() {
    assert_eq!(Registry::resolve_tool_name("communicate"), "swarm");
}

#[tokio::test]
#[ignore]
async fn print_tool_definition_token_report() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;
    let mut defs = registry.definitions(None).await;
    defs.sort_by_key(|def| std::cmp::Reverse(def.prompt_token_estimate()));

    println!("name,total_tokens,description_tokens");
    for def in defs {
        println!(
            "{},{},{}",
            def.name,
            def.prompt_token_estimate(),
            def.description_token_estimate()
        );
    }
}

fn schema_type_includes(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values
            .iter()
            .any(|value| value.as_str().is_some_and(|value| value == expected)),
        _ => false,
    }
}

fn collect_schema_errors(schema: &Value, path: &str, errors: &mut Vec<String>) {
    match schema {
        Value::Object(map) => {
            if schema_type_includes(schema, "array") && !map.contains_key("items") {
                errors.push(format!("{path}: array schema missing items"));
            }

            for keyword in ["anyOf", "oneOf", "allOf"] {
                let Some(branches) = map.get(keyword) else {
                    continue;
                };
                let Some(branches) = branches.as_array() else {
                    errors.push(format!("{path}.{keyword}: must be an array"));
                    continue;
                };
                for (idx, branch) in branches.iter().enumerate() {
                    let branch_path = format!("{path}.{keyword}[{idx}]");
                    match branch {
                        Value::Object(branch_map) => {
                            if !branch_map.contains_key("type") {
                                errors.push(format!("{branch_path}: schema missing type"));
                            }
                        }
                        _ => errors.push(format!("{branch_path}: schema branch must be an object")),
                    }
                }
            }

            for (key, value) in map {
                collect_schema_errors(value, &format!("{path}.{key}"), errors);
            }
        }
        Value::Array(values) => {
            for (idx, value) in values.iter().enumerate() {
                collect_schema_errors(value, &format!("{path}[{idx}]"), errors);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn test_tool_definitions_do_not_expose_invalid_array_schemas() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;

    let defs = registry.definitions(None).await;
    let mut errors = Vec::new();
    for def in &defs {
        collect_schema_errors(
            &def.input_schema,
            &format!("tool `{}`", def.name),
            &mut errors,
        );
    }

    assert!(
        errors.is_empty(),
        "tool definitions must not expose invalid schemas:\n{}",
        errors.join("\n")
    );
}

#[test]
fn test_schema_validator_rejects_any_of_branches_without_type() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "status_filter": {
                "anyOf": [
                    { "enum": ["running", "completed"] },
                    { "type": "array", "items": { "type": "string" } }
                ]
            }
        }
    });

    let mut errors = Vec::new();
    collect_schema_errors(&schema, "tool `test`", &mut errors);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("status_filter.anyOf[0]: schema missing type")),
        "expected missing type error, got: {errors:?}"
    );
}

#[tokio::test]
async fn test_context_guard_small_output_passes_through() {
    let compaction = Arc::new(RwLock::new(CompactionManager::new().with_budget(200_000)));
    let registry = Registry {
        tools: Arc::new(RwLock::new(HashMap::new())),
        skills: Arc::new(RwLock::new(crate::skill::SkillRegistry::default())),
        compaction,
        profile: RegistryProfile::Full,
    };

    let output = ToolOutput::new("small output");
    let result = registry.guard_context_overflow("test", output).await;
    assert_eq!(result.output, "small output");
}

#[tokio::test]
async fn test_context_guard_truncates_huge_single_output() {
    let compaction = Arc::new(RwLock::new(CompactionManager::new().with_budget(1000)));
    let registry = Registry {
        tools: Arc::new(RwLock::new(HashMap::new())),
        skills: Arc::new(RwLock::new(crate::skill::SkillRegistry::default())),
        compaction,
        profile: RegistryProfile::Full,
    };

    // 30% of 1000 = 300 tokens = 1200 chars max for a single output
    // Create output that's way larger
    let big_output = "x".repeat(8000); // 2000 tokens, well over 30% of 1000
    let output = ToolOutput::new(big_output.clone());
    let result = registry.guard_context_overflow("test", output).await;
    assert!(
        result.output.len() < big_output.len(),
        "Output should be truncated"
    );
    assert!(
        result.output.contains("TRUNCATED"),
        "Should contain truncation warning"
    );
}

#[tokio::test]
async fn test_context_guard_truncates_when_context_nearly_full() {
    let compaction = Arc::new(RwLock::new(CompactionManager::new().with_budget(10_000)));
    {
        let mut mgr = compaction.write().await;
        mgr.update_observed_input_tokens(9500); // 95% full
    }
    let registry = Registry {
        tools: Arc::new(RwLock::new(HashMap::new())),
        skills: Arc::new(RwLock::new(crate::skill::SkillRegistry::default())),
        compaction,
        profile: RegistryProfile::Full,
    };

    // Even a modest output should get truncated when context is 95% full
    let output = ToolOutput::new("x".repeat(4000)); // 1000 tokens
    let result = registry.guard_context_overflow("test", output).await;
    assert!(
        result.output.contains("TRUNCATED") || result.output.contains("CONTEXT LIMIT"),
        "Should warn about context limits when nearly full"
    );
}

#[tokio::test]
async fn test_context_guard_zero_budget_passes_through() {
    let compaction = Arc::new(RwLock::new(CompactionManager::new().with_budget(0)));
    let registry = Registry {
        tools: Arc::new(RwLock::new(HashMap::new())),
        skills: Arc::new(RwLock::new(crate::skill::SkillRegistry::default())),
        compaction,
        profile: RegistryProfile::Full,
    };

    let output = ToolOutput::new("x".repeat(100_000));
    let result = registry.guard_context_overflow("test", output).await;
    assert_eq!(
        result.output.len(),
        100_000,
        "Zero budget should pass through"
    );
}

#[tokio::test]
async fn test_request_permission_is_ambient_only() {
    let provider: Arc<dyn Provider> = Arc::new(MockProvider);
    let registry = Registry::new(provider).await;

    let defs = registry.definitions(None).await;
    assert!(
        !defs.iter().any(|d| d.name == "request_permission"),
        "request_permission should not be available in normal sessions"
    );

    registry.register_ambient_tools().await;
    let defs_after = registry.definitions(None).await;
    assert!(
        defs_after.iter().any(|d| d.name == "request_permission"),
        "request_permission should be available after ambient tool registration"
    );
}
