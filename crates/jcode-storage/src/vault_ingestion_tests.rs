use crate::duckdb_broker_store::DuckDbBrokerStoreService;
use crate::vault_ingestion::collect_vault_records;

#[test]
fn vault_ingestion_collects_markdown_records_for_broker_store() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    std::fs::write(
        vault.join("Alpha.md"),
        "# Alpha\nDuckDB ingestion context lives here.\n[[Beta]]\n- [ ] Track ingestion writer\n",
    )
    .expect("write Alpha");
    std::fs::write(vault.join("Ignored.txt"), "not markdown").expect("write ignored");
    std::fs::create_dir_all(vault.join(".obsidian")).expect("create obsidian");
    std::fs::write(
        vault.join(".obsidian").join("Hidden.md"),
        "# Hidden\nSkip me.\n",
    )
    .expect("write hidden");

    let records = collect_vault_records(&vault).expect("collect records");

    assert_eq!(records.files.len(), 1);
    assert_eq!(records.files[0].path, "Alpha.md");
    assert_eq!(records.files[0].title, "Alpha");
    assert_eq!(records.chunks.len(), 1);
    assert_eq!(records.chunks[0].path, "Alpha.md");
    assert!(
        records.chunks[0]
            .content
            .contains("DuckDB ingestion context")
    );
    assert_eq!(records.links.len(), 1);
    assert_eq!(records.links[0].target, "Beta");
    assert_eq!(records.tasks.len(), 1);
    assert_eq!(records.tasks[0].content, "Track ingestion writer");
    assert_eq!(records.summaries.len(), 1);
    assert_eq!(records.summaries[0].file_id, records.files[0].id);
    assert!(
        records.summaries[0]
            .summary
            .contains("DuckDB ingestion context")
    );
    assert!(
        records
            .entities
            .iter()
            .any(|entity| entity.name == "Alpha" && entity.kind == "title")
    );
    assert!(
        records
            .entities
            .iter()
            .any(|entity| entity.name == "Beta" && entity.kind == "link_target")
    );
    assert!(
        records
            .edges
            .iter()
            .any(|edge| edge.kind == "ChunkOf" && edge.target_id == records.files[0].id)
    );
    assert!(
        records
            .edges
            .iter()
            .any(|edge| edge.kind == "SummaryOf" && edge.target_id == records.files[0].id)
    );
    assert!(
        records
            .edges
            .iter()
            .any(|edge| edge.kind == "EntityOf" && edge.target_id == records.files[0].id)
    );
}

#[test]
fn vault_ingestion_reports_source_lines_after_frontmatter() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    std::fs::write(
        vault.join("Frontmatter.md"),
        "---\ntitle: Frontmatter\n---\n# Frontmatter\nIntro line.\n## Target Heading\n- [ ] Verify original source line\n",
    )
    .expect("write Frontmatter");

    let records = collect_vault_records(&vault).expect("collect records");
    let target = records
        .chunks
        .iter()
        .find(|chunk| chunk.heading == "Target Heading")
        .expect("target heading chunk");
    assert_eq!(target.start_line, 6);
    assert_eq!(target.end_line, 7);
    let task = records
        .tasks
        .iter()
        .find(|task| task.content == "Verify original source line")
        .expect("task record");
    assert_eq!(task.line, 7);
}

#[test]
fn vault_ingestion_reconciles_updates_deletes_and_renames() {
    let temp = tempfile::tempdir().expect("tempdir");
    let vault = temp.path().join("Vault");
    std::fs::create_dir_all(&vault).expect("create vault");
    let alpha = vault.join("Alpha.md");
    let stale = vault.join("Stale.md");
    let rename_me = vault.join("RenameMe.md");
    std::fs::write(
        &alpha,
        "# Alpha\nInitial DuckDB ingestion evidence.\n- [ ] Track ingestion writer\n",
    )
    .expect("write Alpha");
    std::fs::write(&stale, "# Stale\nRetired stale-only phrase.\n").expect("write Stale");
    std::fs::write(&rename_me, "# Rename\nRename identity should persist.\n")
        .expect("write RenameMe");

    let service =
        DuckDbBrokerStoreService::start(temp.path().join("broker.duckdb")).expect("start store");
    let initial = service
        .reconcile_vault_path(&vault)
        .expect("initial reconcile");
    assert_eq!(initial.new_files, 3);
    assert_eq!(initial.updated_files, 0);
    assert_eq!(initial.tombstoned_files, 0);
    assert_eq!(initial.renamed_files.len(), 0);

    std::fs::write(
        &alpha,
        "# Alpha\nUpdated DuckDB ingestion evidence for broker context.\n- [x] Track ingestion writer\n",
    )
    .expect("update Alpha");
    std::fs::remove_file(&stale).expect("delete Stale");
    std::fs::rename(&rename_me, vault.join("Renamed.md")).expect("rename note");

    let updated = service
        .reconcile_vault_path(&vault)
        .expect("updated reconcile");
    let counts = service.table_counts().expect("counts");
    let updated_hits = service
        .query_vault_chunks("Updated DuckDB ingestion evidence", 5)
        .expect("query updated");
    let stale_hits = service
        .query_vault_chunks("stale-only", 5)
        .expect("query stale");
    let renamed_hits = service
        .query_vault_chunks("Rename identity should persist", 5)
        .expect("query renamed");

    assert_eq!(updated.new_files, 0);
    assert_eq!(updated.updated_files, 1);
    assert_eq!(updated.tombstoned_files, 1);
    assert_eq!(updated.renamed_files.len(), 1);
    assert_eq!(updated.renamed_files[0].from_path, "RenameMe.md");
    assert_eq!(updated.renamed_files[0].to_path, "Renamed.md");
    assert_eq!(counts.vault_file, 3);
    assert_eq!(counts.active_vault_file, 2);
    assert!(updated_hits.iter().any(|hit| hit.path == "Alpha.md"));
    assert!(stale_hits.is_empty(), "{stale_hits:?}");
    assert_eq!(renamed_hits.len(), 1);
    assert_eq!(renamed_hits[0].path, "Renamed.md");
    assert_eq!(
        renamed_hits[0].file_id, updated.renamed_files[0].file_id,
        "renamed file should keep stable file identity"
    );
}
