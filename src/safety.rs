use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::notifications::NotificationDispatcher;
use crate::storage;

// ---------------------------------------------------------------------------
// Action classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionTier {
    AutoAllowed,
    RequiresPermission,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyActionCategory {
    LocalRead,
    LocalMemory,
    LocalGarden,
    LocalVerification,
    LocalState,
    ExternalCommunication,
    CodeChange,
    RemoteGit,
    SystemChange,
    Deployment,
    DestructiveData,
    FinancialOrAccount,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionClassification {
    pub tier: ActionTier,
    pub category: SafetyActionCategory,
    pub reason: String,
}

impl ActionClassification {
    pub fn requires_permission(&self) -> bool {
        self.tier == ActionTier::RequiresPermission
    }

    fn auto_allowed(category: SafetyActionCategory, reason: impl Into<String>) -> Self {
        Self {
            tier: ActionTier::AutoAllowed,
            category,
            reason: reason.into(),
        }
    }

    fn permission_required(category: SafetyActionCategory, reason: impl Into<String>) -> Self {
        Self {
            tier: ActionTier::RequiresPermission,
            category,
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Urgency {
    Low,
    Normal,
    High,
}

// ---------------------------------------------------------------------------
// Permission request / result / decision
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: String,
    pub action: String,
    pub description: String,
    pub rationale: String,
    pub urgency: Urgency,
    pub wait: bool,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PermissionResult {
    Approved { message: Option<String> },
    Denied { reason: Option<String> },
    Queued { request_id: String },
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub request_id: String,
    pub approved: bool,
    pub decided_at: DateTime<Utc>,
    pub decided_via: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Action log / transcript
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionLog {
    pub action_type: String,
    pub description: String,
    pub tier: ActionTier,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptStatus {
    Complete,
    Interrupted,
    Incomplete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbientTranscript {
    pub session_id: String,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    pub status: TranscriptStatus,
    pub provider: String,
    pub model: String,
    pub actions: Vec<ActionLog>,
    pub pending_permissions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub compactions: u32,
    pub memories_modified: u32,
    /// Full conversation transcript (markdown) for email notifications
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<String>,
}

// ---------------------------------------------------------------------------
// Tier-1 (auto-allowed) action names
// ---------------------------------------------------------------------------

const LOCAL_READ_ACTIONS: &[&str] = &[
    "read",
    "glob",
    "grep",
    "ls",
    "conversation_search",
    "session_search",
    "codesearch",
    "query_vault",
    "git_status",
    "git_log",
    "ambient_status",
    "ambient_queue",
    "ambient_log",
    "ambient_permissions",
    "ambient_safety_classify",
];

const LOCAL_MEMORY_ACTIONS: &[&str] = &[
    "memory",
    "todo",
    "todowrite",
    "todoread",
    "memory_consolidation",
    "memory_prune",
    "memory_verification",
    "derive_memory",
    "semantic_search",
    "cascade_retrieval",
];

const LOCAL_GARDEN_ACTIONS: &[&str] = &[
    "ambient_garden",
    "ambient_garden_apply",
    "ambient_garden_report",
    "ambient_garden_embedding_backfill",
    "ambient_garden_duplicate_consolidation",
    "ambient_garden_tombstone_prune",
    "ambient_garden_stale_fact_verification",
    "ambient_garden_retroactive_extraction",
    "vault_embedding_backfill",
    "duplicate_entity_consolidation",
    "stale_tombstone_prune",
    "stale_fact_verification",
    "retroactive_extraction",
];

const LOCAL_VERIFICATION_ACTIONS: &[&str] = &[
    "run_tests",
    "cargo_test",
    "cargo_check",
    "python_unittest",
    "lint",
];

const LOCAL_STATE_ACTIONS: &[&str] = &[
    "schedule_ambient",
    "end_ambient_cycle",
    "write_ambient_log",
    "save_transcript",
];

const EXTERNAL_COMMUNICATION_ACTIONS: &[&str] = &[
    "send_message",
    "send_email",
    "email",
    "gmail_send",
    "slack_post",
    "discord_post",
    "telegram_send",
    "communicate",
    "pr_comment",
    "github_pr_comment",
    "create_github_issue",
];

const CODE_CHANGE_ACTIONS: &[&str] = &[
    "write",
    "edit",
    "multiedit",
    "patch",
    "apply_patch",
    "modify_code",
    "code_change",
    "refactor",
    "rewrite_file",
];

const REMOTE_GIT_ACTIONS: &[&str] = &[
    "push",
    "git_push",
    "create_pull_request",
    "open_pr",
    "merge_pr",
    "pull_request",
    "pr",
];

const SYSTEM_CHANGE_ACTIONS: &[&str] = &[
    "bash",
    "shell",
    "exec",
    "install",
    "brew_install",
    "apt_install",
    "install_system_package",
    "modify_dotfiles",
    "launchctl",
    "open_port",
    "start_network_service",
    "system_config",
    "launch",
    "open",
    "webfetch",
    "websearch",
];

const DEPLOYMENT_ACTIONS: &[&str] = &["deploy", "release", "publish"];

const DESTRUCTIVE_DATA_ACTIONS: &[&str] = &[
    "delete",
    "delete_file",
    "rm",
    "drop_database",
    "clear_cache",
    "truncate",
    "destroy",
    "wipe",
];

const FINANCIAL_OR_ACCOUNT_ACTIONS: &[&str] = &[
    "purchase",
    "billing_change",
    "change_password",
    "revoke_token",
    "modify_permissions",
    "account_change",
];

fn classify_action_name(action: &str) -> ActionClassification {
    let normalized = normalize_action(action);
    if normalized.is_empty() {
        return ActionClassification::permission_required(
            SafetyActionCategory::Unknown,
            "Empty or missing action name requires review.",
        );
    }

    if action_matches(&normalized, LOCAL_READ_ACTIONS, &[]) {
        return ActionClassification::auto_allowed(
            SafetyActionCategory::LocalRead,
            "Read-only local inspection stays inside the local sandbox.",
        );
    }
    if action_matches(&normalized, LOCAL_MEMORY_ACTIONS, &[]) {
        return ActionClassification::auto_allowed(
            SafetyActionCategory::LocalMemory,
            "Local memory or todo maintenance stays inside broker memory.",
        );
    }
    if action_matches(&normalized, LOCAL_GARDEN_ACTIONS, &["ambient_garden"]) {
        return ActionClassification::auto_allowed(
            SafetyActionCategory::LocalGarden,
            "Ambient garden work mutates only local broker memory/index state.",
        );
    }
    if action_matches(&normalized, LOCAL_VERIFICATION_ACTIONS, &[]) {
        return ActionClassification::auto_allowed(
            SafetyActionCategory::LocalVerification,
            "Local verification does not communicate externally or change project files.",
        );
    }
    if action_matches(&normalized, LOCAL_STATE_ACTIONS, &[]) {
        return ActionClassification::auto_allowed(
            SafetyActionCategory::LocalState,
            "Local ambient state updates stay inside the local runtime.",
        );
    }

    if action_matches(
        &normalized,
        EXTERNAL_COMMUNICATION_ACTIONS,
        &["send_", "post_"],
    ) {
        return ActionClassification::permission_required(
            SafetyActionCategory::ExternalCommunication,
            "External or human-facing communication must be approved first.",
        );
    }
    if action_matches(
        &normalized,
        CODE_CHANGE_ACTIONS,
        &["edit_", "write_", "patch_"],
    ) {
        return ActionClassification::permission_required(
            SafetyActionCategory::CodeChange,
            "Code or file modifications must be approved before an ambient agent acts.",
        );
    }
    if action_matches(&normalized, REMOTE_GIT_ACTIONS, &["git_push", "pr_"]) {
        return ActionClassification::permission_required(
            SafetyActionCategory::RemoteGit,
            "Remote git, pull request, or review activity leaves a durable external trace.",
        );
    }
    if action_matches(&normalized, SYSTEM_CHANGE_ACTIONS, &["install_", "system_"]) {
        return ActionClassification::permission_required(
            SafetyActionCategory::SystemChange,
            "System, shell, network, or dependency changes require review.",
        );
    }
    if action_matches(
        &normalized,
        DEPLOYMENT_ACTIONS,
        &["deploy_", "release_", "publish_"],
    ) {
        return ActionClassification::permission_required(
            SafetyActionCategory::Deployment,
            "Deployments, releases, and publishing actions affect external environments.",
        );
    }
    if action_matches(
        &normalized,
        DESTRUCTIVE_DATA_ACTIONS,
        &["delete_", "drop_", "rm_"],
    ) {
        return ActionClassification::permission_required(
            SafetyActionCategory::DestructiveData,
            "Destructive data or file operations require explicit approval.",
        );
    }
    if action_matches(
        &normalized,
        FINANCIAL_OR_ACCOUNT_ACTIONS,
        &["billing_", "account_"],
    ) {
        return ActionClassification::permission_required(
            SafetyActionCategory::FinancialOrAccount,
            "Account, permission, credential, or financial operations require review.",
        );
    }

    ActionClassification::permission_required(
        SafetyActionCategory::Unknown,
        "Unknown action type defaults to permission required.",
    )
}

fn normalize_action(action: &str) -> String {
    action
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

fn action_matches(action: &str, exact: &[&str], prefixes: &[&str]) -> bool {
    exact.iter().any(|candidate| *candidate == action)
        || prefixes
            .iter()
            .any(|prefix| action.starts_with(prefix) && action.len() > prefix.len())
}

// ---------------------------------------------------------------------------
// SafetySystem
// ---------------------------------------------------------------------------

pub struct SafetySystem {
    queue: Mutex<Vec<PermissionRequest>>,
    history: Mutex<Vec<Decision>>,
    actions: Mutex<Vec<ActionLog>>,
    notifier: NotificationDispatcher,
}

impl SafetySystem {
    /// Create a new SafetySystem, loading persisted queue/history from disk.
    pub fn new() -> Self {
        let queue: Vec<PermissionRequest> = queue_path()
            .ok()
            .and_then(|p| storage::read_json(&p).ok())
            .unwrap_or_default();

        let history: Vec<Decision> = history_path()
            .ok()
            .and_then(|p| storage::read_json(&p).ok())
            .unwrap_or_default();

        SafetySystem {
            queue: Mutex::new(queue),
            history: Mutex::new(history),
            actions: Mutex::new(Vec::new()),
            notifier: NotificationDispatcher::new(),
        }
    }

    /// Classify an action name into a tier.
    pub fn classify(&self, action: &str) -> ActionTier {
        self.classify_action(action).tier
    }

    /// Classify an action with the category and explanation used by ambient gating.
    pub fn classify_action(&self, action: &str) -> ActionClassification {
        classify_action_name(action)
    }

    /// True when the action must be reviewed before execution.
    pub fn requires_permission(&self, action: &str) -> bool {
        self.classify_action(action).requires_permission()
    }

    /// Apply the v1 safety gate: local memory/read/garden operations are approved
    /// immediately, while non-local autonomy is queued for review.
    pub fn review_or_request_permission(&self, request: PermissionRequest) -> PermissionResult {
        let classification = self.classify_action(&request.action);
        if classification.requires_permission() {
            return self.request_permission(request);
        }

        let reason = classification.reason.clone();
        let details = serde_json::json!({
            "safety_category": classification.category,
            "safety_reason": reason,
        });
        self.log_action(ActionLog {
            action_type: request.action.clone(),
            description: request.description,
            tier: classification.tier,
            details: Some(details),
            timestamp: Utc::now(),
        });
        PermissionResult::Approved {
            message: Some(format!(
                "auto-allowed local action: {}",
                classification.reason
            )),
        }
    }

    /// Submit a permission request. Returns `Queued` with the request id.
    pub fn request_permission(&self, request: PermissionRequest) -> PermissionResult {
        let request_id = request.id.clone();
        let action = request.action.clone();
        let description = request.description.clone();
        if let Ok(mut q) = self.queue.lock() {
            q.push(request);
            let _ = persist_queue(&q);
        }
        // Send high-priority notification for permission request
        self.notifier
            .dispatch_permission_request(&action, &description, &request_id);
        PermissionResult::Queued { request_id }
    }

    /// Expire pending permission requests that can no longer be serviced
    /// because their originating session is no longer active.
    pub fn expire_dead_session_requests(&self, via: &str) -> Result<Vec<String>> {
        let mut expired: Vec<(String, String)> = Vec::new();

        if let Ok(mut q) = self.queue.lock() {
            let mut retained: Vec<PermissionRequest> = Vec::with_capacity(q.len());
            for req in q.drain(..) {
                if let Some(reason) = stale_request_reason(&req) {
                    expired.push((req.id.clone(), reason));
                } else {
                    retained.push(req);
                }
            }
            *q = retained;
            let _ = persist_queue(&q);
        }

        if expired.is_empty() {
            return Ok(Vec::new());
        }

        if let Ok(mut h) = self.history.lock() {
            for (request_id, reason) in &expired {
                h.push(Decision {
                    request_id: request_id.clone(),
                    approved: false,
                    decided_at: Utc::now(),
                    decided_via: via.to_string(),
                    message: Some(format!(
                        "Expired automatically: {}. Original agent is no longer active.",
                        reason
                    )),
                });
            }
            let _ = persist_history(&h);
        }

        Ok(expired.into_iter().map(|(id, _)| id).collect())
    }

    /// Record a user decision (approve / deny) for a pending request.
    pub fn record_decision(
        &self,
        request_id: &str,
        approved: bool,
        via: &str,
        message: Option<String>,
    ) -> Result<()> {
        // Remove from queue
        if let Ok(mut q) = self.queue.lock() {
            q.retain(|r| r.id != request_id);
            let _ = persist_queue(&q);
        }

        let decision = Decision {
            request_id: request_id.to_string(),
            approved,
            decided_at: Utc::now(),
            decided_via: via.to_string(),
            message,
        };

        if let Ok(mut h) = self.history.lock() {
            h.push(decision);
            let _ = persist_history(&h);
        }

        Ok(())
    }

    /// Return all pending permission requests.
    pub fn pending_requests(&self) -> Vec<PermissionRequest> {
        self.queue.lock().map(|q| q.clone()).unwrap_or_default()
    }

    /// Append an action to the in-memory log.
    pub fn log_action(&self, log: ActionLog) {
        if let Ok(mut actions) = self.actions.lock() {
            actions.push(log);
        }
    }

    /// Generate a human-readable summary of logged actions.
    pub fn generate_summary(&self) -> String {
        let actions = self.actions.lock().map(|a| a.clone()).unwrap_or_default();
        let pending = self.pending_requests();

        let mut lines: Vec<String> = Vec::new();

        if actions.is_empty() && pending.is_empty() {
            return "No actions recorded.".to_string();
        }

        // Separate auto vs permission-required
        let auto: Vec<&ActionLog> = actions
            .iter()
            .filter(|a| a.tier == ActionTier::AutoAllowed)
            .collect();
        let perm: Vec<&ActionLog> = actions
            .iter()
            .filter(|a| a.tier == ActionTier::RequiresPermission)
            .collect();

        if !auto.is_empty() {
            lines.push("Done (auto-allowed):".to_string());
            for a in &auto {
                lines.push(format!("- {} — {}", a.action_type, a.description));
            }
        }

        if !perm.is_empty() {
            lines.push(String::new());
            lines.push("Done (with permission):".to_string());
            for a in &perm {
                lines.push(format!("- {} — {}", a.action_type, a.description));
            }
        }

        if !pending.is_empty() {
            lines.push(String::new());
            lines.push("Needs your review:".to_string());
            for r in &pending {
                lines.push(format!(
                    "- [{:?}] {} — {}",
                    r.urgency, r.action, r.description
                ));
            }
        }

        lines.join("\n")
    }

    /// Persist a transcript to ~/.jcode/ambient/transcripts/{timestamp}.json
    pub fn save_transcript(&self, transcript: &AmbientTranscript) -> Result<()> {
        let dir = storage::jcode_dir()?.join("ambient").join("transcripts");
        storage::ensure_dir(&dir)?;

        let filename = transcript.started_at.format("%Y-%m-%d-%H%M%S").to_string();
        let path = dir.join(format!("{}.json", filename));
        storage::write_json(&path, transcript)
    }
}

impl Default for SafetySystem {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

fn queue_path() -> Result<std::path::PathBuf> {
    Ok(storage::jcode_dir()?.join("safety").join("queue.json"))
}

fn history_path() -> Result<std::path::PathBuf> {
    Ok(storage::jcode_dir()?.join("safety").join("history.json"))
}

fn persist_queue(queue: &[PermissionRequest]) -> Result<()> {
    let path = queue_path()?;
    storage::write_json(&path, queue)
}

fn persist_history(history: &[Decision]) -> Result<()> {
    let path = history_path()?;
    storage::write_json(&path, history)
}

// ---------------------------------------------------------------------------
// File-based permission decision (for IMAP poller / external callers)
// ---------------------------------------------------------------------------

/// Record a permission decision by directly manipulating the queue/history JSON files.
/// Used by the IMAP reply poller which doesn't have access to the live SafetySystem instance.
pub fn record_permission_via_file(
    request_id: &str,
    approved: bool,
    via: &str,
    message: Option<String>,
) -> Result<()> {
    let qp = queue_path()?;
    if let Some(parent) = qp.parent() {
        storage::ensure_dir(parent)?;
    }
    let mut queue: Vec<PermissionRequest> = if qp.exists() {
        storage::read_json(&qp).unwrap_or_default()
    } else {
        Vec::new()
    };
    queue.retain(|r| r.id != request_id);
    persist_queue(&queue)?;

    let hp = history_path()?;
    if let Some(parent) = hp.parent() {
        storage::ensure_dir(parent)?;
    }
    let mut history: Vec<Decision> = if hp.exists() {
        storage::read_json(&hp).unwrap_or_default()
    } else {
        Vec::new()
    };
    history.push(Decision {
        request_id: request_id.to_string(),
        approved,
        decided_at: Utc::now(),
        decided_via: via.to_string(),
        message,
    });
    persist_history(&history)?;

    Ok(())
}

/// Expire stale permission requests directly via queue/history files.
/// Used by processes that don't hold the live SafetySystem instance.
pub fn expire_stale_permissions_via_file(via: &str) -> Result<Vec<String>> {
    let qp = queue_path()?;
    if let Some(parent) = qp.parent() {
        storage::ensure_dir(parent)?;
    }
    let mut queue: Vec<PermissionRequest> = if qp.exists() {
        storage::read_json(&qp).unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut expired: Vec<(String, String)> = Vec::new();
    queue.retain(|req| {
        if let Some(reason) = stale_request_reason(req) {
            expired.push((req.id.clone(), reason));
            false
        } else {
            true
        }
    });
    persist_queue(&queue)?;

    if expired.is_empty() {
        return Ok(Vec::new());
    }

    let hp = history_path()?;
    if let Some(parent) = hp.parent() {
        storage::ensure_dir(parent)?;
    }
    let mut history: Vec<Decision> = if hp.exists() {
        storage::read_json(&hp).unwrap_or_default()
    } else {
        Vec::new()
    };
    for (request_id, reason) in &expired {
        history.push(Decision {
            request_id: request_id.clone(),
            approved: false,
            decided_at: Utc::now(),
            decided_via: via.to_string(),
            message: Some(format!(
                "Expired automatically: {}. Original agent is no longer active.",
                reason
            )),
        });
    }
    persist_history(&history)?;

    Ok(expired.into_iter().map(|(id, _)| id).collect())
}

fn stale_request_reason(request: &PermissionRequest) -> Option<String> {
    let session_id = request_session_id(request)?;
    let mut session = match crate::session::Session::load(&session_id) {
        Ok(s) => s,
        Err(_) => return Some(format!("owner session '{}' was not found", session_id)),
    };

    // Refresh crash status based on PID if needed.
    if session.detect_crash() {
        let _ = session.save();
    }

    if session.status == crate::session::SessionStatus::Active {
        None
    } else {
        Some(format!(
            "owner session '{}' is {}",
            session_id,
            session.status.display()
        ))
    }
}

fn request_session_id(request: &PermissionRequest) -> Option<String> {
    let context = request.context.as_ref()?;

    context
        .get("session_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            context
                .get("requester")
                .and_then(|r| r.get("session_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
}

// ---------------------------------------------------------------------------
// ID generation helper
// ---------------------------------------------------------------------------

/// Generate a unique permission request id: `req_{timestamp}_{random}`
pub fn new_request_id() -> String {
    crate::id::new_id("req")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn with_temp_home<F, T>(f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let _guard = crate::storage::lock_test_env();
        let prev_home = std::env::var_os("JCODE_HOME");
        let temp = tempfile::TempDir::new().expect("create temp dir");
        crate::env::set_var("JCODE_HOME", temp.path());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));

        match prev_home {
            Some(value) => crate::env::set_var("JCODE_HOME", value),
            None => crate::env::remove_var("JCODE_HOME"),
        }

        result.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
    }

    #[test]
    fn test_classify_auto_allowed() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            assert_eq!(sys.classify("read"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("glob"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("grep"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("ls"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("memory"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("todo"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("todowrite"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("todoread"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("conversation_search"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("session_search"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("codesearch"), ActionTier::AutoAllowed);
        });
    }

    #[test]
    fn test_classify_requires_permission() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            assert_eq!(sys.classify("bash"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("write"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("edit"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("multiedit"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("patch"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("apply_patch"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("communicate"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("open"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("launch"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("webfetch"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("websearch"), ActionTier::RequiresPermission);
            assert_eq!(sys.classify("unknown_tool"), ActionTier::RequiresPermission);
        });
    }

    #[test]
    fn test_classify_case_insensitive() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            assert_eq!(sys.classify("Read"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("GLOB"), ActionTier::AutoAllowed);
            assert_eq!(sys.classify("Bash"), ActionTier::RequiresPermission);
        });
    }

    #[test]
    fn test_classify_non_local_autonomy_with_categories() {
        with_temp_home(|| {
            let sys = SafetySystem::new();

            let local = sys.classify_action("ambient_garden_apply");
            assert_eq!(local.tier, ActionTier::AutoAllowed);
            assert_eq!(local.category, SafetyActionCategory::LocalGarden);
            assert!(!local.requires_permission());

            let message = sys.classify_action("send_message");
            assert_eq!(message.tier, ActionTier::RequiresPermission);
            assert_eq!(
                message.category,
                SafetyActionCategory::ExternalCommunication
            );
            assert!(message.requires_permission());

            let edit = sys.classify_action("apply_patch");
            assert_eq!(edit.tier, ActionTier::RequiresPermission);
            assert_eq!(edit.category, SafetyActionCategory::CodeChange);

            let pr = sys.classify_action("create_pull_request");
            assert_eq!(pr.tier, ActionTier::RequiresPermission);
            assert_eq!(pr.category, SafetyActionCategory::RemoteGit);

            let system = sys.classify_action("install_system_package");
            assert_eq!(system.tier, ActionTier::RequiresPermission);
            assert_eq!(system.category, SafetyActionCategory::SystemChange);

            let deploy = sys.classify_action("deploy");
            assert_eq!(deploy.tier, ActionTier::RequiresPermission);
            assert_eq!(deploy.category, SafetyActionCategory::Deployment);

            let destructive = sys.classify_action("drop_database");
            assert_eq!(destructive.tier, ActionTier::RequiresPermission);
            assert_eq!(destructive.category, SafetyActionCategory::DestructiveData);

            let account = sys.classify_action("purchase");
            assert_eq!(account.tier, ActionTier::RequiresPermission);
            assert_eq!(account.category, SafetyActionCategory::FinancialOrAccount);
        });
    }

    #[test]
    fn test_review_or_request_permission_auto_allows_local_memory_actions() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let baseline = sys.pending_requests().len();
            let req = PermissionRequest {
                id: "req_memory_auto".to_string(),
                action: "memory_consolidation".to_string(),
                description: "Merge duplicate memories".to_string(),
                rationale: "Local memory garden maintenance".to_string(),
                urgency: Urgency::Low,
                wait: false,
                created_at: Utc::now(),
                context: None,
            };

            let result = sys.review_or_request_permission(req);
            assert!(matches!(result, PermissionResult::Approved { .. }));
            assert_eq!(sys.pending_requests().len(), baseline);
        });
    }

    #[test]
    fn test_review_or_request_permission_queues_non_local_autonomy() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let baseline = sys.pending_requests().len();
            let req = PermissionRequest {
                id: "req_send_message".to_string(),
                action: "send_message".to_string(),
                description: "Send a status message to Telegram".to_string(),
                rationale: "External human-facing communication".to_string(),
                urgency: Urgency::Normal,
                wait: false,
                created_at: Utc::now(),
                context: None,
            };

            let result = sys.review_or_request_permission(req);
            assert!(matches!(result, PermissionResult::Queued { .. }));
            assert_eq!(sys.pending_requests().len(), baseline + 1);
        });
    }

    #[test]
    fn test_request_permission_returns_queued() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let baseline = sys.pending_requests().len();
            let req = PermissionRequest {
                id: "req_test_1".to_string(),
                action: "create_pull_request".to_string(),
                description: "Create PR for test fixes".to_string(),
                rationale: "Found failing tests".to_string(),
                urgency: Urgency::Normal,
                wait: false,
                created_at: Utc::now(),
                context: None,
            };

            let result = sys.request_permission(req);
            match result {
                PermissionResult::Queued { request_id } => {
                    assert_eq!(request_id, "req_test_1");
                }
                _ => panic!("Expected Queued result"),
            }

            assert_eq!(sys.pending_requests().len(), baseline + 1);
        });
    }

    #[test]
    fn test_record_decision_removes_from_queue() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let baseline = sys.pending_requests().len();
            let req = PermissionRequest {
                id: "req_test_2".to_string(),
                action: "push".to_string(),
                description: "Push to origin".to_string(),
                rationale: "Ready for review".to_string(),
                urgency: Urgency::Low,
                wait: false,
                created_at: Utc::now(),
                context: None,
            };

            sys.request_permission(req);
            assert_eq!(sys.pending_requests().len(), baseline + 1);

            sys.record_decision("req_test_2", true, "tui", Some("looks good".to_string()))
                .unwrap();
            assert_eq!(sys.pending_requests().len(), baseline);
        });
    }

    #[test]
    fn test_log_action_and_summary() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            sys.log_action(ActionLog {
                action_type: "memory_consolidation".to_string(),
                description: "Merged 2 duplicate memories".to_string(),
                tier: ActionTier::AutoAllowed,
                details: None,
                timestamp: Utc::now(),
            });
            sys.log_action(ActionLog {
                action_type: "edit".to_string(),
                description: "Fixed typo in README".to_string(),
                tier: ActionTier::RequiresPermission,
                details: None,
                timestamp: Utc::now(),
            });

            let summary = sys.generate_summary();
            assert!(summary.contains("memory_consolidation"));
            assert!(summary.contains("edit"));
            assert!(summary.contains("Done (auto-allowed)"));
            assert!(summary.contains("Done (with permission)"));
        });
    }

    #[test]
    fn test_empty_summary() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let summary = sys.generate_summary();
            assert_eq!(summary, "No actions recorded.");
        });
    }

    #[test]
    fn test_new_request_id_format() {
        with_temp_home(|| {
            let id = new_request_id();
            assert!(id.starts_with("req_"));
        });
    }

    #[test]
    fn test_record_permission_via_file() {
        with_temp_home(|| {
            let sys = SafetySystem::new();
            let baseline = sys.pending_requests().len();
            let req = PermissionRequest {
                id: "req_file_test".to_string(),
                action: "push".to_string(),
                description: "Push to origin".to_string(),
                rationale: "Ready for review".to_string(),
                urgency: Urgency::Low,
                wait: false,
                created_at: Utc::now(),
                context: None,
            };
            sys.request_permission(req);
            assert_eq!(sys.pending_requests().len(), baseline + 1);

            record_permission_via_file("req_file_test", true, "email_reply", None).unwrap();

            let sys2 = SafetySystem::new();
            let still_pending = sys2
                .pending_requests()
                .iter()
                .any(|r| r.id == "req_file_test");
            assert!(
                !still_pending,
                "request should have been removed from queue"
            );
        });
    }
}
