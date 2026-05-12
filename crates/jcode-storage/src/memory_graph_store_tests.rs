use crate::memory_graph_store::{
    JsonMemoryGraphStore, MemoryGraphRecord, MemoryGraphScope, MemoryGraphStore,
};

fn sample_record(content: &str) -> MemoryGraphRecord {
    MemoryGraphRecord {
        id: "memory:test".to_string(),
        scope: MemoryGraphScope::Project,
        graph_json: format!(r#"{{"memories":[{{"content":"{}"}}]}}"#, content),
    }
}

#[test]
fn json_memory_graph_store_round_trips_records() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("memory.json");
    let store = JsonMemoryGraphStore::new();
    let record = sample_record("json fallback");

    store.save_record(&path, &record).expect("save json record");
    let loaded = store.load_record(&path).expect("load json record");

    assert_eq!(loaded, Some(record));
}

#[cfg(feature = "duckdb-storage")]
#[test]
fn duckdb_memory_graph_store_round_trips_records() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("memory.duckdb");
    let store =
        crate::memory_graph_store::DuckDbMemoryGraphStore::open(&path).expect("open duckdb store");
    let record = sample_record("duckdb durable boundary");

    store
        .save_record(&path, &record)
        .expect("save duckdb record");
    drop(store);

    let reopened = crate::memory_graph_store::DuckDbMemoryGraphStore::open(&path)
        .expect("reopen duckdb store");
    let loaded = reopened.load_record(&path).expect("load duckdb record");

    assert_eq!(loaded, Some(record));
}
