use crate::duckdb_broker_store::{
    DuckDbBrokerStoreService, GraphEdgeRecord, VaultChunkRecord, VaultEmbeddingRecord,
    VaultFileRecord, VaultLinkRecord, VaultRecordBatch, VaultTaskRecord,
};

fn sample_file(id: &str, path: &str, checksum: &str) -> VaultFileRecord {
    VaultFileRecord {
        id: id.to_string(),
        path: path.to_string(),
        title: "Alpha".to_string(),
        checksum: checksum.to_string(),
        size_bytes: 64,
        mtime_ns: 123,
        frontmatter_json: "{}".to_string(),
        deleted_at: None,
    }
}

fn sample_chunk(id: &str, file_id: &str, content: &str) -> VaultChunkRecord {
    VaultChunkRecord {
        id: id.to_string(),
        file_id: file_id.to_string(),
        path: "Alpha.md".to_string(),
        heading: "DuckDB broker".to_string(),
        content: content.to_string(),
        start_line: 1,
        end_line: 3,
        checksum: format!("sha256:{id}"),
        deleted_at: None,
    }
}

fn sample_embedding(
    model: &str,
    chunk: &VaultChunkRecord,
    file: &VaultFileRecord,
    embedding: Vec<f32>,
) -> VaultEmbeddingRecord {
    VaultEmbeddingRecord {
        id: format!("embedding:{model}:{}", chunk.id),
        record_id: chunk.id.clone(),
        record_kind: "vault_chunk".to_string(),
        embedding_model: model.to_string(),
        embedding,
        content_checksum: chunk.checksum.clone(),
        source_checksum: file.checksum.clone(),
        updated_at: "2026-05-11T21:30:00Z".to_string(),
        deleted_at: None,
    }
}

#[test]
fn duckdb_broker_store_replaces_vault_records_and_queries_context() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&path).expect("start broker store");

    let batch = VaultRecordBatch {
        files: vec![sample_file("file_alpha", "Alpha.md", "sha256:file")],
        chunks: vec![sample_chunk(
            "chunk_alpha",
            "file_alpha",
            "DuckDB broker context should cite this vault evidence.",
        )],
        links: vec![VaultLinkRecord {
            id: "link_alpha_beta".to_string(),
            source_file_id: "file_alpha".to_string(),
            source_path: "Alpha.md".to_string(),
            target: "Beta".to_string(),
            kind: "wikilink".to_string(),
            raw: "[[Beta]]".to_string(),
            deleted_at: None,
        }],
        tasks: vec![VaultTaskRecord {
            id: "task_alpha".to_string(),
            file_id: "file_alpha".to_string(),
            path: "Alpha.md".to_string(),
            checked: false,
            content: "wire vault context formatting".to_string(),
            line: 4,
            deleted_at: None,
        }],
        edges: vec![GraphEdgeRecord {
            id: "edge_chunk_file".to_string(),
            source_id: "chunk_alpha".to_string(),
            target_id: "file_alpha".to_string(),
            kind: "ChunkOf".to_string(),
            weight: 1.0,
            deleted_at: None,
        }],
    };

    service
        .replace_vault_records(batch)
        .expect("replace vault records");

    let counts = service.table_counts().expect("table counts");
    assert_eq!(counts.vault_file, 1);
    assert_eq!(counts.vault_chunk, 1);
    assert_eq!(counts.vault_link, 1);
    assert_eq!(counts.vault_task, 1);
    assert_eq!(counts.graph_edge, 1);

    let hits = service
        .query_vault_chunks("DuckDB broker context", 5)
        .expect("query chunks");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "chunk_alpha");
    assert_eq!(hits[0].path, "Alpha.md");
    assert_eq!(hits[0].source_checksum, "sha256:file");
    assert!(hits[0].score > 0.0);
    assert!(hits[0].matched_terms.contains(&"duckdb".to_string()));

    let task_hits = service
        .query_vault_tasks("vault context formatting", 5)
        .expect("query tasks");
    assert_eq!(task_hits.len(), 1);
    assert_eq!(task_hits[0].id, "task_alpha");
    assert_eq!(task_hits[0].source_checksum, "sha256:file");

    let link_hits = service.query_vault_links("Beta", 5).expect("query links");
    assert_eq!(link_hits.len(), 1);
    assert_eq!(link_hits[0].id, "link_alpha_beta");
    assert_eq!(link_hits[0].target, "Beta");
}

#[test]
fn duckdb_broker_store_tombstones_deleted_files_from_context() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&path).expect("start broker store");

    let batch = VaultRecordBatch {
        files: vec![sample_file("file_alpha", "Alpha.md", "sha256:file")],
        chunks: vec![sample_chunk(
            "chunk_alpha",
            "file_alpha",
            "Operational reconciliation should hide deleted evidence.",
        )],
        ..VaultRecordBatch::default()
    };
    service
        .replace_vault_records(batch)
        .expect("replace vault records");

    service
        .tombstone_file_records("file_alpha", "2026-05-11T20:30:00Z")
        .expect("tombstone file");

    let counts = service.table_counts().expect("table counts");
    assert_eq!(counts.vault_file, 1);
    assert_eq!(counts.active_vault_file, 0);
    assert_eq!(counts.vault_chunk, 1);
    assert_eq!(counts.active_vault_chunk, 0);

    let hits = service
        .query_vault_chunks("Operational reconciliation", 5)
        .expect("query chunks");
    assert!(hits.is_empty(), "{hits:?}");
}

#[test]
fn duckdb_broker_store_refreshes_vault_embeddings_for_changed_chunks() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&path).expect("start broker store");

    let file = sample_file("file_alpha", "Alpha.md", "sha256:file");
    let chunk = sample_chunk(
        "chunk_alpha",
        "file_alpha",
        "Semantic retrieval should find the original vault evidence.",
    );
    service
        .replace_vault_records(VaultRecordBatch {
            files: vec![file.clone()],
            chunks: vec![chunk.clone()],
            ..VaultRecordBatch::default()
        })
        .expect("replace vault records");

    let missing = service
        .list_missing_vault_chunk_embeddings("test-model", 10)
        .expect("list missing embeddings");
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].id, "chunk_alpha");

    service
        .upsert_vault_embeddings(vec![sample_embedding(
            "test-model",
            &chunk,
            &file,
            vec![1.0, 0.0, 0.0],
        )])
        .expect("upsert embedding");

    let counts = service.table_counts().expect("table counts");
    assert_eq!(counts.vault_embedding, 1);
    assert_eq!(counts.active_vault_embedding, 1);
    let missing = service
        .list_missing_vault_chunk_embeddings("test-model", 10)
        .expect("list missing embeddings");
    assert!(missing.is_empty(), "{missing:?}");

    let hits = service
        .query_vault_chunks_by_embedding("test-model", &[1.0, 0.0, 0.0], 5)
        .expect("query semantic chunks");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "chunk_alpha");
    assert!(hits[0].score > 0.99, "{hits:?}");

    let updated_source = sample_file("file_alpha", "Alpha.md", "sha256:file-source-v2");
    service
        .upsert_vault_records(VaultRecordBatch {
            files: vec![updated_source.clone()],
            ..VaultRecordBatch::default()
        })
        .expect("upsert source checksum");
    let missing = service
        .list_missing_vault_chunk_embeddings("test-model", 10)
        .expect("list missing embeddings after source update");
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].id, "chunk_alpha");
    let hits = service
        .query_vault_chunks_by_embedding("test-model", &[1.0, 0.0, 0.0], 5)
        .expect("query semantic chunks after source update");
    assert!(hits.is_empty(), "{hits:?}");
    service
        .upsert_vault_embeddings(vec![sample_embedding(
            "test-model",
            &chunk,
            &updated_source,
            vec![1.0, 0.0, 0.0],
        )])
        .expect("refresh source embedding");

    let updated_file = sample_file("file_alpha", "Alpha.md", "sha256:file-v2");
    let updated_chunk = sample_chunk(
        "chunk_alpha_v2",
        "file_alpha",
        "Semantic retrieval should require a fresh vector after note edits.",
    );
    service
        .replace_file_records(
            "file_alpha",
            VaultRecordBatch {
                files: vec![updated_file],
                chunks: vec![updated_chunk],
                ..VaultRecordBatch::default()
            },
        )
        .expect("replace file records");

    let counts = service.table_counts().expect("table counts");
    assert_eq!(counts.vault_embedding, 0);
    assert_eq!(counts.active_vault_embedding, 0);
    let missing = service
        .list_missing_vault_chunk_embeddings("test-model", 10)
        .expect("list missing embeddings after update");
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].id, "chunk_alpha_v2");
    let hits = service
        .query_vault_chunks_by_embedding("test-model", &[1.0, 0.0, 0.0], 5)
        .expect("query semantic chunks after update");
    assert!(hits.is_empty(), "{hits:?}");
}

#[test]
fn duckdb_broker_store_serializes_concurrent_client_writes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&path).expect("start broker store");
    let mut handles = Vec::new();

    for idx in 0..8 {
        let client = service.client();
        handles.push(std::thread::spawn(move || {
            let file_id = format!("file_{idx}");
            let chunk_id = format!("chunk_{idx}");
            client
                .upsert_vault_records(VaultRecordBatch {
                    files: vec![sample_file(&file_id, &format!("{idx}.md"), "sha256:file")],
                    chunks: vec![sample_chunk(
                        &chunk_id,
                        &file_id,
                        &format!("serialized DuckDB write number {idx}"),
                    )],
                    ..VaultRecordBatch::default()
                })
                .expect("upsert records");
        }));
    }

    for handle in handles {
        handle.join().expect("client thread");
    }

    let counts = service.table_counts().expect("table counts");
    assert_eq!(counts.vault_file, 8);
    assert_eq!(counts.vault_chunk, 8);

    let hits = service
        .query_vault_chunks("serialized DuckDB write", 20)
        .expect("query chunks");
    assert_eq!(hits.len(), 8);
}

#[test]
fn duckdb_broker_store_backs_up_and_restores_database_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("broker.duckdb");
    let backup_path = temp.path().join("backups").join("broker-backup.duckdb");
    let restore_path = temp.path().join("restored").join("broker.duckdb");
    let service = DuckDbBrokerStoreService::start(&path).expect("start broker store");

    service
        .replace_vault_records(VaultRecordBatch {
            files: vec![sample_file("file_alpha", "Alpha.md", "sha256:file")],
            chunks: vec![sample_chunk(
                "chunk_alpha",
                "file_alpha",
                "DuckDB backup restore should preserve broker context.",
            )],
            ..VaultRecordBatch::default()
        })
        .expect("replace records");
    service.backup_to(&backup_path).expect("backup database");
    drop(service);

    DuckDbBrokerStoreService::restore_from_backup(&backup_path, &restore_path)
        .expect("restore backup");
    let restored = DuckDbBrokerStoreService::start(&restore_path).expect("start restored store");
    let counts = restored.table_counts().expect("table counts");
    let hits = restored
        .query_vault_chunks("backup restore broker context", 5)
        .expect("query restored chunks");

    assert_eq!(counts.vault_file, 1);
    assert_eq!(counts.vault_chunk, 1);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "chunk_alpha");
}
