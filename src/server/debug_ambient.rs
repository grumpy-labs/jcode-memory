use crate::ambient_runner::AmbientRunnerHandle;
use crate::provider::Provider;
use crate::safety::{PermissionRequest, Urgency};
use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;

pub(super) async fn maybe_handle_ambient_command(
    cmd: &str,
    ambient_runner: &Option<AmbientRunnerHandle>,
    provider: &Arc<dyn Provider>,
) -> Result<Option<String>> {
    if cmd == "ambient:status" {
        let output = if let Some(runner) = ambient_runner {
            runner.status_json().await
        } else {
            serde_json::json!({
                "enabled": false,
                "status": "disabled",
                "message": "Ambient mode is not enabled in config"
            })
            .to_string()
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:queue" {
        let output = if let Some(runner) = ambient_runner {
            runner.queue_json().await
        } else {
            "[]".to_string()
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:trigger" {
        let output = if let Some(runner) = ambient_runner {
            runner.trigger().await;
            "Ambient cycle triggered".to_string()
        } else {
            return Err(anyhow::anyhow!("Ambient mode is not enabled"));
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:log" {
        let output = if let Some(runner) = ambient_runner {
            runner.log_json().await
        } else {
            "[]".to_string()
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:garden" {
        let report = crate::ambient::gather_ambient_garden_report_from_env()?;
        let output = serde_json::to_string_pretty(&report)?;
        return Ok(Some(output));
    }

    if let Some(rest) = cmd.strip_prefix("ambient:garden:apply") {
        let action = rest.trim_start_matches(':').trim();
        let kinds = match action {
            "" | "all" => crate::ambient::AmbientGardenActionKind::all(),
            "embeddings" | "embedding_backfill" => {
                vec![crate::ambient::AmbientGardenActionKind::EmbeddingBackfill]
            }
            "duplicates" | "duplicate_entity_consolidation" => {
                vec![crate::ambient::AmbientGardenActionKind::ConsolidateDuplicates]
            }
            "tombstones" | "stale_tombstone_prune" => {
                vec![crate::ambient::AmbientGardenActionKind::PruneTombstones]
            }
            "facts" | "stale_fact_verification" => {
                vec![crate::ambient::AmbientGardenActionKind::VerifyStaleFacts]
            }
            "retroactive" | "retroactive_extraction" => {
                vec![crate::ambient::AmbientGardenActionKind::RetroactiveExtraction]
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Usage: ambient:garden:apply[:all|embeddings|duplicates|tombstones|facts|retroactive]"
                ));
            }
        };
        let report = crate::ambient::apply_ambient_garden_from_env(kinds)?;
        let output = serde_json::to_string_pretty(&report)?;
        return Ok(Some(output));
    }

    if let Some(action) = cmd.strip_prefix("ambient:safety:classify:") {
        let action = action.trim();
        if action.is_empty() {
            return Err(anyhow::anyhow!("Usage: ambient:safety:classify:<action>"));
        }
        let classification = if let Some(runner) = ambient_runner {
            runner.safety().classify_action(action)
        } else {
            crate::safety::SafetySystem::new().classify_action(action)
        };
        let output = serde_json::to_string_pretty(&classification)?;
        return Ok(Some(output));
    }

    if cmd == "ambient:permissions" {
        let output = if let Some(runner) = ambient_runner {
            let _ = runner
                .safety()
                .expire_dead_session_requests("debug_socket_gc");
            let pending = runner.safety().pending_requests();
            let items: Vec<serde_json::Value> = pending
                .iter()
                .map(|request| {
                    let review_summary = request
                        .context
                        .as_ref()
                        .and_then(|ctx| ctx.get("review"))
                        .and_then(|review| review.get("summary"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(&request.description);
                    let review_why = request
                        .context
                        .as_ref()
                        .and_then(|ctx| ctx.get("review"))
                        .and_then(|review| review.get("why_permission_needed"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(&request.rationale);
                    serde_json::json!({
                        "id": request.id,
                        "action": request.action,
                        "description": request.description,
                        "rationale": request.rationale,
                        "summary": review_summary,
                        "why_permission_needed": review_why,
                        "urgency": format!("{:?}", request.urgency),
                        "wait": request.wait,
                        "created_at": request.created_at.to_rfc3339(),
                        "context": request.context,
                    })
                })
                .collect();
            serde_json::to_string_pretty(&items).unwrap_or_else(|_| "[]".to_string())
        } else {
            "[]".to_string()
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:permission:inbox" || cmd == "ambient:inbox" {
        let output = if let Some(runner) = ambient_runner {
            let _ = runner
                .safety()
                .expire_dead_session_requests("debug_socket_gc");
            let pending = runner.safety().pending_requests();
            serde_json::to_string_pretty(&permission_inbox_payload(&pending))
                .unwrap_or_else(|_| "{}".to_string())
        } else {
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "disabled",
                "pending_count": 0,
                "card": null,
                "message": "Ambient mode is not enabled"
            }))
            .unwrap_or_else(|_| "{}".to_string())
        };
        return Ok(Some(output));
    }

    if cmd.starts_with("ambient:approve:") {
        let request_id = cmd.strip_prefix("ambient:approve:").unwrap_or("").trim();
        if request_id.is_empty() {
            return Err(anyhow::anyhow!("Usage: ambient:approve:<request_id>"));
        }
        let output = if let Some(runner) = ambient_runner {
            runner
                .safety()
                .record_decision(request_id, true, "debug_socket", None)?;
            format!("Approved: {}", request_id)
        } else {
            return Err(anyhow::anyhow!("Ambient mode is not enabled"));
        };
        return Ok(Some(output));
    }

    if cmd.starts_with("ambient:deny:") {
        let rest = cmd.strip_prefix("ambient:deny:").unwrap_or("").trim();
        if rest.is_empty() {
            return Err(anyhow::anyhow!("Usage: ambient:deny:<request_id> [reason]"));
        }
        let output = if let Some(runner) = ambient_runner {
            let mut parts = rest.splitn(2, char::is_whitespace);
            let request_id = parts.next().unwrap_or("").trim();
            let message = parts
                .next()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            runner
                .safety()
                .record_decision(request_id, false, "debug_socket", message)?;
            format!("Denied: {}", request_id)
        } else {
            return Err(anyhow::anyhow!("Ambient mode is not enabled"));
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:stop" {
        let output = if let Some(runner) = ambient_runner {
            runner.stop().await;
            "Ambient mode stopped".to_string()
        } else {
            return Err(anyhow::anyhow!("Ambient mode is not enabled"));
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:start" {
        let output = if let Some(runner) = ambient_runner {
            if runner.start(Arc::clone(provider)).await {
                "Ambient mode started".to_string()
            } else {
                "Ambient mode is already running".to_string()
            }
        } else {
            return Err(anyhow::anyhow!("Ambient mode is not enabled in config"));
        };
        return Ok(Some(output));
    }

    if cmd == "ambient:help" {
        return Ok(Some(
            r#"Ambient mode debug commands (ambient: prefix):
  ambient:status              - Ambient + schedule runner state, counts, next due items
  ambient:queue               - Scheduled queue contents with target/session metadata
  ambient:trigger             - Manually trigger an ambient cycle
  ambient:log                 - Recent transcript summaries
  ambient:garden              - Read-only broker index garden report
  ambient:garden:apply[:kind] - Explicitly apply garden actions: all, embeddings, duplicates, tombstones, facts, retroactive
  ambient:safety:classify:<a> - Show safety tier/category for an action
  ambient:permission:inbox   - Show the next pending permission as one review card
  ambient:permissions         - List pending permission requests
  ambient:approve:<id>        - Approve a permission request
  ambient:deny:<id> [reason]  - Deny a permission request (optional reason)
  ambient:start               - Start/restart ambient mode
  ambient:stop                - Stop ambient mode"#
                .to_string(),
        ));
    }

    Ok(None)
}

fn permission_inbox_payload(pending: &[PermissionRequest]) -> serde_json::Value {
    let now = Utc::now();
    let first = pending.first().map(|request| {
        let review = request.context.as_ref().and_then(|ctx| ctx.get("review"));
        let summary = review
            .and_then(|review| review.get("summary"))
            .and_then(|v| v.as_str())
            .unwrap_or(&request.description);
        let why_permission_needed = review
            .and_then(|review| review.get("why_permission_needed"))
            .and_then(|v| v.as_str())
            .unwrap_or(&request.rationale);
        let safety = review
            .and_then(|review| review.get("safety"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let age_seconds = now
            .signed_duration_since(request.created_at)
            .num_seconds()
            .max(0);

        serde_json::json!({
            "id": request.id,
            "action": request.action,
            "summary": summary,
            "why_permission_needed": why_permission_needed,
            "urgency": urgency_label(request.urgency),
            "age_seconds": age_seconds,
            "safety": safety,
            "approve_command": format!("ambient:approve:{}", request.id),
            "deny_command": format!("ambient:deny:{} <reason>", request.id),
        })
    });

    serde_json::json!({
        "status": if pending.is_empty() { "empty" } else { "pending" },
        "pending_count": pending.len(),
        "card": first,
    })
}

fn urgency_label(urgency: Urgency) -> &'static str {
    match urgency {
        Urgency::Low => "low",
        Urgency::Normal => "normal",
        Urgency::High => "high",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safety::{PermissionRequest, Urgency};
    use chrono::{Duration, Utc};

    #[test]
    fn permission_inbox_payload_reports_empty_state() {
        let payload = permission_inbox_payload(&[]);
        assert_eq!(
            payload.get("status").and_then(|v| v.as_str()),
            Some("empty")
        );
        assert_eq!(
            payload.get("pending_count").and_then(|v| v.as_u64()),
            Some(0)
        );
        assert!(payload.get("card").is_some_and(|v| v.is_null()));
    }

    #[test]
    fn permission_inbox_payload_formats_single_review_card() {
        let request = PermissionRequest {
            id: "req_permission_card".to_string(),
            action: "send_message".to_string(),
            description: "Send a status note".to_string(),
            rationale: "External communication".to_string(),
            urgency: Urgency::High,
            wait: false,
            created_at: Utc::now() - Duration::seconds(12),
            context: Some(serde_json::json!({
                "review": {
                    "summary": "Tell Rob the import finished",
                    "why_permission_needed": "This sends a human-facing Telegram message",
                    "safety": {
                        "tier": "requires_permission",
                        "category": "external_communication",
                        "requires_permission": true
                    }
                }
            })),
        };

        let payload = permission_inbox_payload(&[request]);
        assert_eq!(
            payload.get("status").and_then(|v| v.as_str()),
            Some("pending")
        );
        assert_eq!(
            payload.get("pending_count").and_then(|v| v.as_u64()),
            Some(1)
        );
        let card = payload.get("card").expect("card present");
        assert_eq!(
            card.get("id").and_then(|v| v.as_str()),
            Some("req_permission_card")
        );
        assert_eq!(
            card.get("action").and_then(|v| v.as_str()),
            Some("send_message")
        );
        assert_eq!(
            card.get("summary").and_then(|v| v.as_str()),
            Some("Tell Rob the import finished")
        );
        assert_eq!(card.get("urgency").and_then(|v| v.as_str()), Some("high"));
        assert_eq!(
            card.get("approve_command").and_then(|v| v.as_str()),
            Some("ambient:approve:req_permission_card")
        );
        assert_eq!(
            card.get("deny_command").and_then(|v| v.as_str()),
            Some("ambient:deny:req_permission_card <reason>")
        );
        assert_eq!(
            card.get("safety")
                .and_then(|v| v.get("category"))
                .and_then(|v| v.as_str()),
            Some("external_communication")
        );
        assert!(
            card.get("age_seconds")
                .and_then(|v| v.as_i64())
                .is_some_and(|age| age >= 0)
        );
    }
}
