use crate::duckdb_broker_store::{
    BrokerStoreCounts, DuckDbBrokerStoreClient, DuckDbBrokerStoreService, GraphEdgeRecord,
    VaultChunkRecord, VaultEntityRecord, VaultFileRecord, VaultLinkRecord, VaultRecordBatch,
    VaultSummaryRecord, VaultTaskRecord,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const EXCLUDED_DIRS: &[&str] = &[
    ".git",
    ".obsidian",
    ".trash",
    ".DS_Store",
    "__pycache__",
    "node_modules",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultRename {
    pub file_id: String,
    pub from_path: String,
    pub to_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultIngestionReport {
    pub new_files: usize,
    pub updated_files: usize,
    pub unchanged_files: usize,
    pub tombstoned_files: usize,
    pub renamed_files: Vec<VaultRename>,
    pub counts: BrokerStoreCounts,
}

impl DuckDbBrokerStoreService {
    pub fn reconcile_vault_path(
        &self,
        vault_path: impl AsRef<Path>,
    ) -> Result<VaultIngestionReport> {
        self.client().reconcile_vault_path(vault_path)
    }
}

impl DuckDbBrokerStoreClient {
    pub fn reconcile_vault_path(
        &self,
        vault_path: impl AsRef<Path>,
    ) -> Result<VaultIngestionReport> {
        let mut incoming = collect_vault_records(vault_path)?;
        let existing = self.list_vault_files()?;
        let renamed_files = preserve_renamed_file_ids(&mut incoming, &existing);
        let incoming_ids: HashSet<String> =
            incoming.files.iter().map(|file| file.id.clone()).collect();
        let existing_by_id: HashMap<String, VaultFileRecord> = existing
            .iter()
            .cloned()
            .map(|file| (file.id.clone(), file))
            .collect();

        let mut new_files = 0;
        let mut updated_files = 0;
        let mut unchanged_files = 0;
        let renamed_ids: HashSet<String> = renamed_files
            .iter()
            .map(|rename| rename.file_id.clone())
            .collect();

        for file in &incoming.files {
            let batch = records_for_file(&incoming, &file.id);
            match existing_by_id.get(&file.id) {
                None => {
                    new_files += 1;
                    self.replace_file_records(&file.id, batch)?;
                }
                Some(existing_file)
                    if existing_file.deleted_at.is_some()
                        || existing_file.checksum != file.checksum
                        || existing_file.path != file.path =>
                {
                    if !renamed_ids.contains(&file.id) && existing_file.checksum != file.checksum {
                        updated_files += 1;
                    }
                    self.replace_file_records(&file.id, batch)?;
                }
                Some(_) => {
                    unchanged_files += 1;
                }
            }
        }

        let deleted_at = deleted_at_timestamp();
        let mut tombstoned_files = 0;
        for file in existing
            .iter()
            .filter(|file| file.deleted_at.is_none() && !incoming_ids.contains(&file.id))
        {
            tombstoned_files += 1;
            self.tombstone_file_records(&file.id, &deleted_at)?;
        }

        Ok(VaultIngestionReport {
            new_files,
            updated_files,
            unchanged_files,
            tombstoned_files,
            renamed_files,
            counts: self.table_counts()?,
        })
    }
}

pub fn collect_vault_records(vault_path: impl AsRef<Path>) -> Result<VaultRecordBatch> {
    let vault_path = vault_path.as_ref();
    let mut files = Vec::new();
    collect_markdown_files(vault_path, vault_path, &mut files)?;
    files.sort();

    let mut batch = VaultRecordBatch::default();
    for path in files {
        append_markdown_file_records(vault_path, &path, &mut batch)?;
    }
    Ok(batch)
}

fn collect_markdown_files(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(current)
        .with_context(|| format!("failed to read vault directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if should_skip_name(&name) {
            continue;
        }
        if path.is_dir() {
            collect_markdown_files(root, &path, files)?;
        } else if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
            && path.starts_with(root)
        {
            files.push(path);
        }
    }
    Ok(())
}

fn append_markdown_file_records(
    root: &Path,
    path: &Path,
    batch: &mut VaultRecordBatch,
) -> Result<()> {
    let rel_path = relative_vault_path(root, path)?;
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read vault file {}", path.display()))?;
    let body = strip_frontmatter(&text);
    let metadata = std::fs::metadata(path)?;
    let file_id = stable_id("vault_file", &[&rel_path]);
    let checksum = sha256_text(&text);
    let file = VaultFileRecord {
        id: file_id.clone(),
        path: rel_path.clone(),
        title: title_from_body(path, body),
        checksum,
        size_bytes: metadata.len() as i64,
        mtime_ns: modified_time_ns(&metadata),
        frontmatter_json: "{}".to_string(),
        deleted_at: None,
    };
    let chunks = chunk_body(&file_id, &rel_path, body);
    let links = parse_links(&file_id, &rel_path, body);
    let tasks = parse_tasks(&file_id, &rel_path, body);
    let summaries = summarize_file(&file, &chunks);
    let entities = extract_entities(&file, &links);

    batch.edges.extend(
        chunks
            .iter()
            .map(|chunk| graph_edge(&chunk.id, &file_id, "ChunkOf")),
    );
    batch.edges.extend(
        tasks
            .iter()
            .map(|task| graph_edge(&task.id, &file_id, "TaskOf")),
    );
    batch.edges.extend(
        links
            .iter()
            .map(|link| graph_edge(&link.id, &file_id, "LinkFrom")),
    );
    batch.edges.extend(
        summaries
            .iter()
            .map(|summary| graph_edge(&summary.id, &file_id, "SummaryOf")),
    );
    batch.edges.extend(
        entities
            .iter()
            .map(|entity| graph_edge(&entity.id, &file_id, "EntityOf")),
    );
    batch.files.push(file);
    batch.chunks.extend(chunks);
    batch.links.extend(links);
    batch.tasks.extend(tasks);
    batch.summaries.extend(summaries);
    batch.entities.extend(entities);
    Ok(())
}

fn records_for_file(batch: &VaultRecordBatch, file_id: &str) -> VaultRecordBatch {
    VaultRecordBatch {
        files: batch
            .files
            .iter()
            .filter(|record| record.id == file_id)
            .cloned()
            .collect(),
        chunks: batch
            .chunks
            .iter()
            .filter(|record| record.file_id == file_id)
            .cloned()
            .collect(),
        links: batch
            .links
            .iter()
            .filter(|record| record.source_file_id == file_id)
            .cloned()
            .collect(),
        tasks: batch
            .tasks
            .iter()
            .filter(|record| record.file_id == file_id)
            .cloned()
            .collect(),
        summaries: batch
            .summaries
            .iter()
            .filter(|record| record.file_id == file_id)
            .cloned()
            .collect(),
        entities: batch
            .entities
            .iter()
            .filter(|record| record.file_id == file_id)
            .cloned()
            .collect(),
        edges: batch
            .edges
            .iter()
            .filter(|record| {
                record.source_id == file_id
                    || record.target_id == file_id
                    || batch
                        .chunks
                        .iter()
                        .any(|chunk| chunk.file_id == file_id && chunk.id == record.source_id)
                    || batch
                        .tasks
                        .iter()
                        .any(|task| task.file_id == file_id && task.id == record.source_id)
                    || batch
                        .summaries
                        .iter()
                        .any(|summary| summary.file_id == file_id && summary.id == record.source_id)
                    || batch
                        .entities
                        .iter()
                        .any(|entity| entity.file_id == file_id && entity.id == record.source_id)
                    || batch
                        .links
                        .iter()
                        .any(|link| link.source_file_id == file_id && link.id == record.source_id)
            })
            .cloned()
            .collect(),
    }
}

fn preserve_renamed_file_ids(
    batch: &mut VaultRecordBatch,
    existing: &[VaultFileRecord],
) -> Vec<VaultRename> {
    let incoming_ids: HashSet<String> = batch.files.iter().map(|file| file.id.clone()).collect();
    let mut incoming_by_checksum: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, file) in batch.files.iter().enumerate() {
        incoming_by_checksum
            .entry(file.checksum.clone())
            .or_default()
            .push(idx);
    }

    let mut used_incoming = HashSet::new();
    let mut renames = Vec::new();
    for old in existing
        .iter()
        .filter(|file| file.deleted_at.is_none() && !incoming_ids.contains(&file.id))
    {
        let Some(candidates) = incoming_by_checksum.get(&old.checksum) else {
            continue;
        };
        let Some(&idx) = candidates.iter().find(|idx| !used_incoming.contains(*idx)) else {
            continue;
        };
        let new_id = batch.files[idx].id.clone();
        if batch.files[idx].path == old.path {
            continue;
        }
        let to_path = batch.files[idx].path.clone();
        retarget_file_id(batch, &new_id, &old.id);
        used_incoming.insert(idx);
        renames.push(VaultRename {
            file_id: old.id.clone(),
            from_path: old.path.clone(),
            to_path,
        });
    }
    renames
}

fn retarget_file_id(batch: &mut VaultRecordBatch, old_file_id: &str, new_file_id: &str) {
    let mut id_map = HashMap::new();
    for file in &mut batch.files {
        if file.id == old_file_id {
            file.id = new_file_id.to_string();
        }
    }
    for chunk in &mut batch.chunks {
        if chunk.file_id == old_file_id {
            let old_id = chunk.id.clone();
            chunk.file_id = new_file_id.to_string();
            chunk.id = stable_id(
                "vault_chunk",
                &[new_file_id, &chunk.start_line.to_string(), &chunk.checksum],
            );
            id_map.insert(old_id, chunk.id.clone());
        }
    }
    for task in &mut batch.tasks {
        if task.file_id == old_file_id {
            let old_id = task.id.clone();
            task.file_id = new_file_id.to_string();
            task.id = stable_id(
                "vault_task",
                &[new_file_id, &task.line.to_string(), &task.content],
            );
            id_map.insert(old_id, task.id.clone());
        }
    }
    for link in &mut batch.links {
        if link.source_file_id == old_file_id {
            let old_id = link.id.clone();
            link.source_file_id = new_file_id.to_string();
            link.id = stable_id(
                "vault_link",
                &[new_file_id, &link.kind, &link.target, &link.raw],
            );
            id_map.insert(old_id, link.id.clone());
        }
    }
    for summary in &mut batch.summaries {
        if summary.file_id == old_file_id {
            let old_id = summary.id.clone();
            summary.file_id = new_file_id.to_string();
            summary.id = stable_id("vault_summary", &[new_file_id, &summary.source_checksum]);
            id_map.insert(old_id, summary.id.clone());
        }
    }
    for entity in &mut batch.entities {
        if entity.file_id == old_file_id {
            let old_id = entity.id.clone();
            entity.file_id = new_file_id.to_string();
            entity.id = stable_id(
                "vault_entity",
                &[new_file_id, &entity.kind, &entity.name, &entity.source],
            );
            id_map.insert(old_id, entity.id.clone());
        }
    }
    for edge in &mut batch.edges {
        if edge.source_id == old_file_id {
            edge.source_id = new_file_id.to_string();
        }
        if edge.target_id == old_file_id {
            edge.target_id = new_file_id.to_string();
        }
        if let Some(new_id) = id_map.get(&edge.source_id) {
            edge.source_id = new_id.clone();
        }
        if let Some(new_id) = id_map.get(&edge.target_id) {
            edge.target_id = new_id.clone();
        }
        edge.id = stable_id(
            "graph_edge",
            &[&edge.source_id, &edge.target_id, &edge.kind],
        );
    }
}

fn chunk_body(file_id: &str, rel_path: &str, body: &str) -> Vec<VaultChunkRecord> {
    let lines: Vec<&str> = body.lines().collect();
    let mut chunks = Vec::new();
    let mut heading = String::new();
    let mut start_line = 1usize;
    let mut current = Vec::new();

    for (idx, line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        if let Some(next_heading) = heading_text(line) {
            if !current.is_empty() {
                push_chunk(
                    &mut chunks,
                    file_id,
                    rel_path,
                    &heading,
                    start_line,
                    line_no.saturating_sub(1),
                    &current,
                );
                current.clear();
            }
            heading = next_heading;
            start_line = line_no;
        }
        current.push((*line).to_string());
    }
    if !current.is_empty() {
        push_chunk(
            &mut chunks,
            file_id,
            rel_path,
            &heading,
            start_line,
            lines.len().max(start_line),
            &current,
        );
    }
    chunks
}

fn push_chunk(
    chunks: &mut Vec<VaultChunkRecord>,
    file_id: &str,
    rel_path: &str,
    heading: &str,
    start_line: usize,
    end_line: usize,
    lines: &[String],
) {
    let content = lines.join("\n").trim().to_string();
    if content.is_empty() {
        return;
    }
    let checksum = sha256_text(&content);
    chunks.push(VaultChunkRecord {
        id: stable_id(
            "vault_chunk",
            &[file_id, &start_line.to_string(), &checksum],
        ),
        file_id: file_id.to_string(),
        path: rel_path.to_string(),
        heading: heading.to_string(),
        content: content.chars().take(4000).collect(),
        start_line: start_line as i64,
        end_line: end_line as i64,
        checksum,
        deleted_at: None,
    });
}

fn parse_tasks(file_id: &str, rel_path: &str, body: &str) -> Vec<VaultTaskRecord> {
    body.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            let trimmed = line.trim_start();
            let checked = if trimmed.starts_with("- [ ] ") || trimmed.starts_with("* [ ] ") {
                false
            } else if trimmed.starts_with("- [x] ")
                || trimmed.starts_with("- [X] ")
                || trimmed.starts_with("* [x] ")
                || trimmed.starts_with("* [X] ")
            {
                true
            } else {
                return None;
            };
            let content = trimmed[6..].trim().to_string();
            let line_no = (idx + 1) as i64;
            Some(VaultTaskRecord {
                id: stable_id("vault_task", &[file_id, &line_no.to_string(), &content]),
                file_id: file_id.to_string(),
                path: rel_path.to_string(),
                checked,
                content,
                line: line_no,
                deleted_at: None,
            })
        })
        .collect()
}

fn summarize_file(file: &VaultFileRecord, chunks: &[VaultChunkRecord]) -> Vec<VaultSummaryRecord> {
    let Some(summary) = chunks
        .iter()
        .map(|chunk| compact_summary_text(&chunk.content))
        .find(|summary| !summary.is_empty())
    else {
        return Vec::new();
    };
    let summary = summary.chars().take(600).collect::<String>();
    vec![VaultSummaryRecord {
        id: stable_id("vault_summary", &[&file.id, &file.checksum]),
        file_id: file.id.clone(),
        path: file.path.clone(),
        checksum: sha256_text(&summary),
        source_checksum: file.checksum.clone(),
        summary,
        deleted_at: None,
    }]
}

fn compact_summary_text(content: &str) -> String {
    content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|line| line.trim_start_matches('#').trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_entities(file: &VaultFileRecord, links: &[VaultLinkRecord]) -> Vec<VaultEntityRecord> {
    let mut entities = Vec::new();
    let mut seen = HashSet::new();
    push_entity(
        &mut entities,
        &mut seen,
        &file.id,
        &file.path,
        "title",
        &file.title,
        "title",
    );
    for link in links {
        push_entity(
            &mut entities,
            &mut seen,
            &file.id,
            &file.path,
            "link_target",
            &link.target,
            "vault_link",
        );
    }
    entities
}

fn push_entity(
    entities: &mut Vec<VaultEntityRecord>,
    seen: &mut HashSet<String>,
    file_id: &str,
    path: &str,
    kind: &str,
    name: &str,
    source: &str,
) {
    let name = name.trim();
    if name.is_empty() {
        return;
    }
    let dedupe_key = format!("{kind}\u{1f}{name}\u{1f}{source}");
    if !seen.insert(dedupe_key) {
        return;
    }
    entities.push(VaultEntityRecord {
        id: stable_id("vault_entity", &[file_id, kind, name, source]),
        file_id: file_id.to_string(),
        path: path.to_string(),
        name: name.to_string(),
        kind: kind.to_string(),
        source: source.to_string(),
        deleted_at: None,
    });
}

fn parse_links(file_id: &str, rel_path: &str, body: &str) -> Vec<VaultLinkRecord> {
    let mut links = parse_wikilinks(file_id, rel_path, body);
    links.extend(parse_markdown_links(file_id, rel_path, body));
    links
}

fn parse_wikilinks(file_id: &str, rel_path: &str, body: &str) -> Vec<VaultLinkRecord> {
    let mut links = Vec::new();
    let mut offset = 0usize;
    while let Some(start) = body[offset..].find("[[") {
        let absolute_start = offset + start;
        let target_start = absolute_start + 2;
        let Some(end) = body[target_start..].find("]]") else {
            break;
        };
        let absolute_end = target_start + end;
        let raw = &body[absolute_start..absolute_end + 2];
        let target = normalize_wikilink_target(&body[target_start..absolute_end]);
        if !target.is_empty() {
            links.push(VaultLinkRecord {
                id: stable_id(
                    "vault_link",
                    &[file_id, "wikilink", &absolute_start.to_string(), &target],
                ),
                source_file_id: file_id.to_string(),
                source_path: rel_path.to_string(),
                target,
                kind: "wikilink".to_string(),
                raw: raw.to_string(),
                deleted_at: None,
            });
        }
        offset = absolute_end + 2;
    }
    links
}

fn parse_markdown_links(file_id: &str, rel_path: &str, body: &str) -> Vec<VaultLinkRecord> {
    let mut links = Vec::new();
    let mut offset = 0usize;
    while let Some(label_end) = body[offset..].find("](") {
        let target_start = offset + label_end + 2;
        let Some(target_end) = body[target_start..].find(')') else {
            break;
        };
        let absolute_end = target_start + target_end;
        let target = body[target_start..absolute_end].trim();
        if !target.is_empty() && !target.contains("://") && !target.starts_with('#') {
            let raw_start = body[..offset + label_end].rfind('[').unwrap_or(offset);
            let raw = &body[raw_start..absolute_end + 1];
            links.push(VaultLinkRecord {
                id: stable_id(
                    "vault_link",
                    &[file_id, "markdown", &raw_start.to_string(), target],
                ),
                source_file_id: file_id.to_string(),
                source_path: rel_path.to_string(),
                target: target.to_string(),
                kind: "markdown".to_string(),
                raw: raw.to_string(),
                deleted_at: None,
            });
        }
        offset = absolute_end + 1;
    }
    links
}

fn graph_edge(source_id: &str, target_id: &str, kind: &str) -> GraphEdgeRecord {
    GraphEdgeRecord {
        id: stable_id("graph_edge", &[source_id, target_id, kind]),
        source_id: source_id.to_string(),
        target_id: target_id.to_string(),
        kind: kind.to_string(),
        weight: 1.0,
        deleted_at: None,
    }
}

fn strip_frontmatter(text: &str) -> &str {
    if !text.starts_with("---") {
        return text;
    }
    let mut lines = text.lines();
    if lines.next() != Some("---") {
        return text;
    }
    let mut consumed = 4usize;
    for line in lines {
        consumed += line.len() + 1;
        if line.trim() == "---" {
            return text.get(consumed..).unwrap_or_default();
        }
    }
    text
}

fn title_from_body(path: &Path, body: &str) -> String {
    body.lines().find_map(heading_text).unwrap_or_else(|| {
        path.file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    })
}

fn heading_text(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let level = trimmed.chars().take_while(|ch| *ch == '#').count();
    if level == 0 || level > 6 {
        return None;
    }
    let rest = trimmed[level..].trim();
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

fn normalize_wikilink_target(target: &str) -> String {
    target
        .split(['#', '|'])
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn should_skip_name(name: &str) -> bool {
    EXCLUDED_DIRS.contains(&name) || name.starts_with(".sync-conflict")
}

fn relative_vault_path(root: &Path, path: &Path) -> Result<String> {
    Ok(path
        .strip_prefix(root)?
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn modified_time_ns(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn deleted_at_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("unix:{seconds}")
}

fn stable_id(prefix: &str, parts: &[&str]) -> String {
    let joined = parts.join("\u{1f}");
    let digest = sha256_hex(joined.as_bytes());
    format!("{prefix}_{}", &digest[..20])
}

fn sha256_text(text: &str) -> String {
    format!("sha256:{}", sha256_hex(text.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}
