use crate::tool::ToolContext;
use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::path::{Component, Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub(crate) async fn archive_existing_file_for_ambient(
    ctx: &ToolContext,
    path: &Path,
    operation: &str,
) -> Result<Option<PathBuf>> {
    if !crate::tool::ambient::is_ambient_session(&ctx.session_id) || !path.is_file() {
        return Ok(None);
    }

    let archive_root = crate::storage::jcode_dir()?.join("ambient").join("archive");
    tokio::fs::create_dir_all(&archive_root).await?;

    let batch = format!(
        "{}_{}_{}",
        Utc::now().format("%Y%m%dT%H%M%SZ"),
        sanitize_segment(&ctx.session_id),
        sanitize_segment(&ctx.tool_call_id),
    );
    let relative = archive_relative_path(ctx, path);
    let archive_path = archive_root.join(batch).join(&relative);
    if let Some(parent) = archive_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(path, &archive_path)
        .await
        .with_context(|| format!("failed to archive {} before {}", path.display(), operation))?;

    let manifest_entry = json!({
        "ts": Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        "session_id": ctx.session_id,
        "message_id": ctx.message_id,
        "tool_call_id": ctx.tool_call_id,
        "operation": operation,
        "working_dir": ctx.working_dir.as_ref().map(|path| path.display().to_string()),
        "original_path": path.display().to_string(),
        "archive_path": archive_path.display().to_string(),
    });
    let manifest_path = archive_root.join("manifest.jsonl");
    let mut manifest = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .await?;
    manifest
        .write_all(serde_json::to_string(&manifest_entry)?.as_bytes())
        .await?;
    manifest.write_all(b"\n").await?;

    Ok(Some(archive_path))
}

fn archive_relative_path(ctx: &ToolContext, path: &Path) -> PathBuf {
    if let Some(base) = ctx.working_dir.as_deref()
        && let Ok(relative) = path.strip_prefix(base)
    {
        return sanitize_path(relative);
    }
    sanitize_path(path)
}

fn sanitize_path(path: &Path) -> PathBuf {
    let mut sanitized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => sanitized.push(sanitize_segment(&value.to_string_lossy())),
            Component::RootDir => sanitized.push("root"),
            Component::Prefix(prefix) => {
                sanitized.push(sanitize_segment(&prefix.as_os_str().to_string_lossy()))
            }
            Component::CurDir | Component::ParentDir => {}
        }
    }
    if sanitized.as_os_str().is_empty() {
        sanitized.push("file");
    }
    sanitized
}

fn sanitize_segment(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "item".to_string()
    } else {
        sanitized
    }
}
