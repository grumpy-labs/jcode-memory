use crate::{read_json, write_json};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
#[cfg(feature = "duckdb-storage")]
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryGraphScope {
    Project,
    Global,
}

impl MemoryGraphScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Global => "global",
        }
    }

    #[cfg(feature = "duckdb-storage")]
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "project" => Ok(Self::Project),
            "global" => Ok(Self::Global),
            other => anyhow::bail!("unknown memory graph scope: {other}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryGraphRecord {
    pub id: String,
    pub scope: MemoryGraphScope,
    pub graph_json: String,
}

pub trait MemoryGraphStore {
    fn load_record(&self, storage_path: &Path) -> Result<Option<MemoryGraphRecord>>;
    fn save_record(&self, storage_path: &Path, record: &MemoryGraphRecord) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonMemoryGraphStore;

impl JsonMemoryGraphStore {
    pub fn new() -> Self {
        Self
    }
}

impl MemoryGraphStore for JsonMemoryGraphStore {
    fn load_record(&self, storage_path: &Path) -> Result<Option<MemoryGraphRecord>> {
        if !storage_path.exists() {
            return Ok(None);
        }
        read_json(storage_path).map(Some)
    }

    fn save_record(&self, storage_path: &Path, record: &MemoryGraphRecord) -> Result<()> {
        write_json(storage_path, record)
    }
}

#[cfg(feature = "duckdb-storage")]
pub struct DuckDbMemoryGraphStore {
    db_path: PathBuf,
    connection: duckdb::Connection,
}

#[cfg(feature = "duckdb-storage")]
impl DuckDbMemoryGraphStore {
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().expand_homeish();
        if let Some(parent) = db_path.parent() {
            crate::ensure_dir(parent)?;
        }
        let connection = duckdb::Connection::open(&db_path)?;
        let store = Self {
            db_path,
            connection,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    fn initialize_schema(&self) -> Result<()> {
        self.connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS memory_graph_record (
                storage_key VARCHAR PRIMARY KEY,
                id VARCHAR NOT NULL,
                scope VARCHAR NOT NULL,
                graph_json VARCHAR NOT NULL,
                updated_at TIMESTAMP DEFAULT current_timestamp
            )
            "#,
        )?;
        Ok(())
    }
}

#[cfg(feature = "duckdb-storage")]
impl MemoryGraphStore for DuckDbMemoryGraphStore {
    fn load_record(&self, storage_path: &Path) -> Result<Option<MemoryGraphRecord>> {
        let storage_key = storage_key(storage_path);
        let mut statement = self.connection.prepare(
            r#"
            SELECT id, scope, graph_json
            FROM memory_graph_record
            WHERE storage_key = ?
            "#,
        )?;
        let mut rows = statement.query([storage_key])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let scope: String = row.get(1)?;
        Ok(Some(MemoryGraphRecord {
            id: row.get(0)?,
            scope: MemoryGraphScope::from_str(&scope)?,
            graph_json: row.get(2)?,
        }))
    }

    fn save_record(&self, storage_path: &Path, record: &MemoryGraphRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO memory_graph_record (storage_key, id, scope, graph_json, updated_at)
            VALUES (?, ?, ?, ?, now())
            ON CONFLICT(storage_key) DO UPDATE SET
                id = excluded.id,
                scope = excluded.scope,
                graph_json = excluded.graph_json,
                updated_at = now()
            "#,
            duckdb::params![
                storage_key(storage_path),
                record.id,
                record.scope.as_str(),
                record.graph_json
            ],
        )?;
        Ok(())
    }
}

#[cfg(feature = "duckdb-storage")]
fn storage_key(path: &Path) -> String {
    path.expand_homeish().display().to_string()
}

#[cfg(feature = "duckdb-storage")]
trait ExpandHomeish {
    fn expand_homeish(&self) -> PathBuf;
}

#[cfg(feature = "duckdb-storage")]
impl ExpandHomeish for Path {
    fn expand_homeish(&self) -> PathBuf {
        if self.is_absolute() {
            self.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(self)
        }
    }
}
