use super::*;
use chrono::Duration;
use std::ffi::OsString;

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set_path(key: &'static str, value: &std::path::Path) -> Self {
        let previous = std::env::var_os(key);
        crate::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => crate::env::set_var(self.key, value),
            None => crate::env::remove_var(self.key),
        }
    }
}

#[test]
fn test_ambient_status_default() {
    let status = AmbientStatus::default();
    assert_eq!(status, AmbientStatus::Idle);
}

#[test]
fn test_priority_ordering() {
    assert!(Priority::High > Priority::Normal);
    assert!(Priority::Normal > Priority::Low);
}

#[test]
fn test_scheduled_queue_push_and_pop() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut queue = ScheduledQueue::load(path);
    assert!(queue.is_empty());

    let past = Utc::now() - Duration::minutes(5);
    let future = Utc::now() + Duration::hours(1);

    queue.push(ScheduledItem {
        id: "s1".into(),
        scheduled_for: past,
        context: "past item".into(),
        priority: Priority::Low,
        target: ScheduleTarget::Ambient,
        created_by_session: "test".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    queue.push(ScheduledItem {
        id: "s2".into(),
        scheduled_for: future,
        context: "future item".into(),
        priority: Priority::High,
        target: ScheduleTarget::Ambient,
        created_by_session: "test".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    assert_eq!(queue.len(), 2);

    let ready = queue.pop_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, "s1");

    // Future item still in queue
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.peek_next().unwrap().id, "s2");
}

#[test]
fn test_pop_ready_sorts_by_priority_then_time() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut queue = ScheduledQueue::load(path);
    let past1 = Utc::now() - Duration::minutes(10);
    let past2 = Utc::now() - Duration::minutes(5);

    queue.push(ScheduledItem {
        id: "low_early".into(),
        scheduled_for: past1,
        context: "low early".into(),
        priority: Priority::Low,
        target: ScheduleTarget::Ambient,
        created_by_session: "test".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    queue.push(ScheduledItem {
        id: "high_late".into(),
        scheduled_for: past2,
        context: "high late".into(),
        priority: Priority::High,
        target: ScheduleTarget::Ambient,
        created_by_session: "test".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    let ready = queue.pop_ready();
    assert_eq!(ready.len(), 2);
    // High priority should come first
    assert_eq!(ready[0].id, "high_late");
    assert_eq!(ready[1].id, "low_early");
}

#[test]
fn test_take_ready_direct_items_only_removes_direct_targets() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut queue = ScheduledQueue::load(path);
    let past = Utc::now() - Duration::minutes(5);

    queue.push(ScheduledItem {
        id: "session_due".into(),
        scheduled_for: past,
        context: "scheduled session task".into(),
        priority: Priority::Normal,
        target: ScheduleTarget::Session {
            session_id: "session_123".into(),
        },
        created_by_session: "session_123".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    queue.push(ScheduledItem {
        id: "spawn_due".into(),
        scheduled_for: past,
        context: "spawned session task".into(),
        priority: Priority::High,
        target: ScheduleTarget::Spawn {
            parent_session_id: "session_123".into(),
        },
        created_by_session: "session_123".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    queue.push(ScheduledItem {
        id: "ambient_due".into(),
        scheduled_for: past,
        context: "scheduled ambient task".into(),
        priority: Priority::High,
        target: ScheduleTarget::Ambient,
        created_by_session: "ambient".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    let ready_direct = queue.take_ready_direct_items();
    assert_eq!(ready_direct.len(), 2);
    assert_eq!(ready_direct[0].id, "spawn_due");
    assert_eq!(ready_direct[1].id, "session_due");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.items()[0].id, "ambient_due");
}

#[test]
fn test_ambient_state_record_cycle() {
    let mut state = AmbientState::default();
    assert_eq!(state.total_cycles, 0);

    let result = AmbientCycleResult {
        summary: "Merged 2 duplicates".into(),
        memories_modified: 3,
        compactions: 1,
        proactive_work: None,
        next_schedule: None,
        started_at: Utc::now() - Duration::seconds(30),
        ended_at: Utc::now(),
        status: CycleStatus::Complete,
        conversation: None,
    };

    state.record_cycle(&result);
    assert_eq!(state.total_cycles, 1);
    assert_eq!(state.last_summary.as_deref(), Some("Merged 2 duplicates"));
    assert_eq!(state.last_compactions, Some(1));
    assert_eq!(state.last_memories_modified, Some(3));
    assert_eq!(state.status, AmbientStatus::Idle);
}

#[test]
fn test_ambient_state_record_cycle_with_schedule() {
    let mut state = AmbientState::default();

    let result = AmbientCycleResult {
        summary: "Done".into(),
        memories_modified: 0,
        compactions: 0,
        proactive_work: None,
        next_schedule: Some(ScheduleRequest {
            wake_in_minutes: Some(15),
            wake_at: None,
            context: "check CI".into(),
            priority: Priority::Normal,
            target: ScheduleTarget::Ambient,
            created_by_session: "ambient_test".into(),
            working_dir: None,
            task_description: None,
            relevant_files: Vec::new(),
            git_branch: None,
            additional_context: None,
        }),
        started_at: Utc::now() - Duration::seconds(10),
        ended_at: Utc::now(),
        status: CycleStatus::Complete,
        conversation: None,
    };

    state.record_cycle(&result);
    assert!(matches!(state.status, AmbientStatus::Scheduled { .. }));
}

#[test]
#[cfg(feature = "duckdb-storage")]
fn garden_report_surfaces_read_only_duckdb_vault_work_items() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    std::fs::write(
        vault.join("Alpha.md"),
        "# Alpha\nShared garden context.\n[[Shared Ambient Topic]]\n",
    )
    .expect("write Alpha");
    std::fs::write(
        vault.join("Beta.md"),
        "# Beta\nAnother note for garden context.\n[[Shared Ambient Topic]]\n",
    )
    .expect("write Beta");

    let db_path = temp.path().join("broker.duckdb");
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)
        .expect("start broker store");
    service.reconcile_vault_path(&vault).expect("ingest vault");
    drop(service);

    let report = gather_ambient_garden_report(Some(db_path), "test-model", 10)
        .expect("ambient garden report");

    assert_eq!(report.mode, "garden_only");
    assert!(report.read_only);
    assert!(!report.autonomous_actions_allowed);
    assert!(!report.system_changes_allowed);
    assert_eq!(report.counts.active_vault_file, 2);
    assert_eq!(report.counts.missing_vault_chunk_embeddings, 2);
    assert!(report.work_items.iter().any(|item| {
        item.kind == "embedding_backfill"
            && item.count == 2
            && item
                .command
                .as_deref()
                .is_some_and(|command| command.contains("jcode broker embed-vault"))
    }));
    assert!(report.work_items.iter().any(|item| {
        item.kind == "duplicate_entity_candidate"
            && item.count == 2
            && item.summary.contains("Shared Ambient Topic")
    }));
}

#[test]
#[cfg(feature = "duckdb-storage")]
fn garden_report_surfaces_stale_fact_verification_candidates() {
    use jcode_storage::duckdb_broker_store::{
        DuckDbBrokerStoreService, VaultFileRecord, VaultRecordBatch, VaultSummaryRecord,
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&db_path).expect("start broker store");
    service
        .replace_vault_records(VaultRecordBatch {
            files: vec![VaultFileRecord {
                id: "file-alpha".to_string(),
                path: "Notes/Alpha.md".to_string(),
                title: "Alpha".to_string(),
                checksum: "current-source-checksum".to_string(),
                size_bytes: 42,
                mtime_ns: 123,
                frontmatter_json: "{}".to_string(),
                deleted_at: None,
            }],
            summaries: vec![VaultSummaryRecord {
                id: "summary-alpha".to_string(),
                file_id: "file-alpha".to_string(),
                path: "Notes/Alpha.md".to_string(),
                summary: "Alpha still uses the old storage plan.".to_string(),
                checksum: "summary-checksum".to_string(),
                source_checksum: "old-source-checksum".to_string(),
                deleted_at: None,
            }],
            ..Default::default()
        })
        .expect("seed stale summary");
    drop(service);

    let report = gather_ambient_garden_report(Some(db_path), "test-model", 10)
        .expect("ambient garden report");

    assert_eq!(report.counts.stale_vault_summary_facts, 1);
    assert!(report.work_items.iter().any(|item| {
        item.kind == "stale_fact_verification"
            && item.count == 1
            && item.summary.contains("outdated Vault source")
            && item.paths == vec!["Notes/Alpha.md".to_string()]
    }));
}

#[test]
#[cfg(feature = "duckdb-storage")]
fn garden_report_surfaces_retroactive_extraction_candidates_for_crashed_sessions() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let mut session = crate::session::Session::create_with_id(
        "session_ambient_missed_1772405007295".to_string(),
        None,
        Some("Missed extraction proof".to_string()),
    );
    session.set_debug(false);
    session.add_message(
        crate::message::Role::User,
        vec![crate::message::ContentBlock::Text {
            text: "Remember this crashed session needs retroactive extraction.".to_string(),
            cache_control: None,
        }],
    );
    session.mark_crashed(Some("test crash".to_string()));
    session.save().expect("save crashed session");

    let db_path = temp.path().join("broker.duckdb");
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)
        .expect("start broker store");
    drop(service);

    let report = gather_ambient_garden_report(Some(db_path), "test-model", 10)
        .expect("ambient garden report");

    assert_eq!(report.counts.missed_extraction_sessions, 1);
    assert!(report.work_items.iter().any(|item| {
        item.kind == "retroactive_extraction_candidate"
            && item.count == 1
            && item.summary.contains("crashed/error session")
            && item
                .paths
                .iter()
                .any(|path| path.ends_with("session_ambient_missed_1772405007295.json"))
    }));
}

#[test]
#[cfg(feature = "duckdb-storage")]
fn garden_apply_reconciles_stale_facts_and_audits_retroactive_extraction() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::tempdir().expect("tempdir");
    let _home = EnvVarGuard::set_path("JCODE_HOME", temp.path());

    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    std::fs::write(
        vault.join("Alpha.md"),
        "# Alpha\nThe garden verifier should refresh this summary.\n",
    )
    .expect("write Alpha");

    let db_path = temp.path().join("broker.duckdb");
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)
        .expect("start broker store");
    service.reconcile_vault_path(&vault).expect("ingest vault");
    let mut alpha = service
        .list_vault_files()
        .expect("list files")
        .into_iter()
        .find(|file| file.path == "Alpha.md")
        .expect("Alpha file");
    alpha.checksum = "sha256:stale-source-checksum".to_string();
    service
        .upsert_vault_records(jcode_storage::duckdb_broker_store::VaultRecordBatch {
            files: vec![alpha],
            ..Default::default()
        })
        .expect("make summary stale");
    assert_eq!(
        service.count_stale_vault_summaries().expect("stale count"),
        1
    );
    drop(service);

    let mut session = crate::session::Session::create_with_id(
        "session_ambient_apply_1772405007295".to_string(),
        None,
        Some("Apply missed extraction proof".to_string()),
    );
    session.set_debug(false);
    session.add_message(
        crate::message::Role::User,
        vec![crate::message::ContentBlock::Text {
            text: "Remember the garden apply retroactive extraction audit path.".to_string(),
            cache_control: None,
        }],
    );
    session.mark_crashed(Some("test crash".to_string()));
    session.save().expect("save crashed session");

    let report = apply_ambient_garden_actions(AmbientGardenApplyOptions {
        db_path: Some(db_path.clone()),
        vault_path: Some(vault),
        embedding_model: "test-model".to_string(),
        kinds: vec![
            AmbientGardenActionKind::VerifyStaleFacts,
            AmbientGardenActionKind::RetroactiveExtraction,
        ],
        limit: 10,
        tombstone_retention_days: 0,
    })
    .expect("apply ambient garden actions");

    assert!(!report.read_only);
    assert!(!report.autonomous_actions_allowed);
    assert!(!report.system_changes_allowed);
    assert_eq!(report.counts_after.stale_vault_summary_facts, 0);
    assert!(report.actions.iter().any(|action| {
        action.kind == "stale_fact_verification"
            && action.status == "applied"
            && action.summary.contains("reconciled Vault")
    }));
    assert!(report.actions.iter().any(|action| {
        action.kind == "retroactive_extraction"
            && action.status == "skipped_sidecar_disabled"
            && action
                .paths
                .iter()
                .any(|path| path.ends_with("session_ambient_apply_1772405007295.json"))
    }));

    let audit_path = temp
        .path()
        .join("ambient")
        .join("retroactive_extraction.jsonl");
    let audit = std::fs::read_to_string(audit_path).expect("retroactive extraction audit");
    assert!(audit.contains("session_ambient_apply_1772405007295"));
}

#[test]
#[cfg(feature = "duckdb-storage")]
fn garden_apply_consolidates_duplicates_and_prunes_tombstones() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    std::fs::write(
        vault.join("Alpha.md"),
        "# Alpha\nOld tombstone candidate.\n",
    )
    .expect("write Alpha");
    std::fs::write(vault.join("Beta.md"), "# Beta\n[[Shared Ambient Topic]]\n")
        .expect("write Beta");
    std::fs::write(
        vault.join("Gamma.md"),
        "# Gamma\n[[Shared Ambient Topic]]\n",
    )
    .expect("write Gamma");

    let db_path = temp.path().join("broker.duckdb");
    let service = jcode_storage::duckdb_broker_store::DuckDbBrokerStoreService::start(&db_path)
        .expect("start broker store");
    service.reconcile_vault_path(&vault).expect("ingest vault");
    let alpha = service
        .list_vault_files()
        .expect("list files")
        .into_iter()
        .find(|file| file.path == "Alpha.md")
        .expect("Alpha file");
    service
        .tombstone_file_records(&alpha.id, "2000-01-01T00:00:00Z")
        .expect("tombstone Alpha");
    drop(service);

    let report = apply_ambient_garden_actions(AmbientGardenApplyOptions {
        db_path: Some(db_path),
        vault_path: None,
        embedding_model: "test-model".to_string(),
        kinds: vec![
            AmbientGardenActionKind::ConsolidateDuplicates,
            AmbientGardenActionKind::PruneTombstones,
        ],
        limit: 10,
        tombstone_retention_days: 0,
    })
    .expect("apply ambient garden actions");

    assert!(report.actions.iter().any(|action| {
        action.kind == "duplicate_entity_consolidation"
            && action.status == "applied"
            && action.count == 1
    }));
    assert!(report.actions.iter().any(|action| {
        action.kind == "stale_tombstone_prune" && action.status == "applied" && action.count > 0
    }));
    assert_eq!(report.counts_after.tombstoned_vault_file, 0);
    assert_eq!(report.counts_after.tombstoned_vault_chunk, 0);
}

#[test]
fn test_ambient_lock_release() {
    // Use a temp dir so we don't conflict with real state
    let tmp_dir = tempfile::tempdir().unwrap();
    let lock_file = tmp_dir.path().join("test.lock");

    // Manually create a lock to test release/drop
    std::fs::write(&lock_file, std::process::id().to_string()).unwrap();
    let lock = AmbientLock {
        lock_path: lock_file.clone(),
    };
    lock.release().unwrap();
    assert!(!lock_file.exists());
}

#[test]
fn test_schedule_id_format() {
    let id = format!("sched_{:08x}", rand::random::<u32>());
    assert!(id.starts_with("sched_"));
    assert_eq!(id.len(), 6 + 8); // "sched_" + 8 hex chars
}

#[test]
fn test_format_duration_rough() {
    assert_eq!(format_duration_rough(Duration::seconds(30)), "30s");
    assert_eq!(format_duration_rough(Duration::minutes(5)), "5m");
    assert_eq!(format_duration_rough(Duration::hours(2)), "2h");
    assert_eq!(
        format_duration_rough(Duration::hours(2) + Duration::minutes(30)),
        "2h 30m"
    );
    assert_eq!(format_duration_rough(Duration::days(3)), "3d");
    assert_eq!(format_duration_rough(Duration::seconds(-5)), "0s");
}

#[test]
fn test_build_ambient_system_prompt_minimal() {
    let state = AmbientState::default();
    let queue = vec![];
    let health = MemoryGraphHealth::default();
    let sessions = vec![];
    let feedback: Vec<String> = vec![];
    let budget = ResourceBudget {
        provider: "anthropic-oauth".into(),
        tokens_remaining_desc: "unknown".into(),
        window_resets_desc: "unknown".into(),
        user_usage_rate_desc: "0 tokens/min".into(),
        cycle_budget_desc: "stay under 50k tokens".into(),
    };

    let prompt =
        build_ambient_system_prompt(&state, &queue, &health, &sessions, &feedback, &budget, 0);

    assert!(prompt.contains("ambient agent for jcode"));
    assert!(prompt.contains("## Current State"));
    assert!(prompt.contains("never (first run)"));
    assert!(prompt.contains("Active user sessions: none"));
    assert!(prompt.contains("## Scheduled Queue"));
    assert!(prompt.contains("Empty"));
    assert!(prompt.contains("## Memory Graph Health"));
    assert!(prompt.contains("Total memories: 0"));
    assert!(prompt.contains("## User Feedback History"));
    assert!(prompt.contains("No feedback memories"));
    assert!(prompt.contains("## Resource Budget"));
    assert!(prompt.contains("anthropic-oauth"));
    assert!(prompt.contains("## Instructions"));
    assert!(prompt.contains("end_ambient_cycle"));
    assert!(prompt.contains("reviewer-ready"));
    assert!(prompt.contains("context.why_permission_needed"));
    assert!(prompt.contains("Do not use `send_message` directly"));
    assert!(prompt.contains("code edits, pull requests, pushes, external messages"));
}

#[test]
fn test_build_ambient_system_prompt_with_data() {
    let state = AmbientState {
        last_run: Some(Utc::now() - Duration::minutes(15)),
        total_cycles: 7,
        ..Default::default()
    };

    let queue = vec![ScheduledItem {
        id: "sched_001".into(),
        scheduled_for: Utc::now(),
        context: "Check CI status".into(),
        priority: Priority::High,
        target: ScheduleTarget::Ambient,
        created_by_session: "session_abc".into(),
        created_at: Utc::now() - Duration::minutes(10),
        working_dir: Some("/home/user/project".into()),
        task_description: Some("Check CI status for the main branch".into()),
        relevant_files: vec!["src/main.rs".into()],
        git_branch: Some("main".into()),
        additional_context: Some("Background: Tests were flaky yesterday".into()),
    }];

    let health = MemoryGraphHealth {
        total: 42,
        active: 38,
        inactive: 4,
        low_confidence: 3,
        contradictions: 1,
        missing_embeddings: 5,
        duplicate_candidates: 0,
        last_consolidation: Some(Utc::now() - Duration::hours(2)),
    };

    let sessions = vec![RecentSessionInfo {
        id: "session_fox_123".into(),
        status: "closed".into(),
        topic: Some("Fix auth bug".into()),
        duration_secs: 900,
        extraction_status: "extracted".into(),
    }];

    let feedback = vec![
        "User approved ambient fixing typos in docs".into(),
        "User rejected ambient refactoring tests".into(),
    ];

    let budget = ResourceBudget {
        provider: "openai-oauth".into(),
        tokens_remaining_desc: "~85k".into(),
        window_resets_desc: "in 3h 20m".into(),
        user_usage_rate_desc: "120 tokens/min".into(),
        cycle_budget_desc: "stay under 15k tokens".into(),
    };

    let prompt =
        build_ambient_system_prompt(&state, &queue, &health, &sessions, &feedback, &budget, 2);

    assert!(prompt.contains("15m ago"));
    assert!(prompt.contains("Active user sessions: 2"));
    assert!(prompt.contains("Total cycles completed: 7"));
    assert!(prompt.contains("Check CI status"));
    assert!(prompt.contains("HIGH"));
    assert!(prompt.contains("42"));
    assert!(prompt.contains("38 active"));
    assert!(prompt.contains("confidence < 0.1: 3"));
    assert!(prompt.contains("contradictions: 1"));
    assert!(prompt.contains("without embeddings: 5"));
    assert!(prompt.contains("Fix auth bug"));
    assert!(prompt.contains("approved ambient fixing typos"));
    assert!(prompt.contains("rejected ambient refactoring"));
    assert!(prompt.contains("openai-oauth"));
    assert!(prompt.contains("~85k"));
    assert!(prompt.contains("Working dir: /home/user/project"));
    assert!(prompt.contains("Details: Check CI status for the main branch"));
    assert!(prompt.contains("Files: src/main.rs"));
    assert!(prompt.contains("Branch: main"));
    assert!(prompt.contains("Tests were flaky yesterday"));
}

#[test]
fn test_scheduled_queue_items_accessor() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    let mut queue = ScheduledQueue::load(path);

    queue.push(ScheduledItem {
        id: "s1".into(),
        scheduled_for: Utc::now(),
        context: "test item".into(),
        priority: Priority::Normal,
        target: ScheduleTarget::Ambient,
        created_by_session: "test".into(),
        created_at: Utc::now(),
        working_dir: None,
        task_description: None,
        relevant_files: Vec::new(),
        git_branch: None,
        additional_context: None,
    });

    let items = queue.items();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, "s1");
}
