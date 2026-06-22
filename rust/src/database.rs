use arrow::record_batch::RecordBatchReader;

use crate::types::{ColumnInfo, ConstraintInfo, Dialect, IndexInfo, RelationInfo, SchemaInfo};
use crate::{DbError, Result};

pub type RecordBatchStream = Box<dyn RecordBatchReader + Send>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArrowExtraction {
    SqlxRows,
    NativeArrow(&'static str),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DbValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertMode {
    Append,
    Truncate,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DbPoolOptions {
    pub min_connections: Option<u32>,
    pub max_connections: Option<u32>,
}

impl DbPoolOptions {
    pub fn new(min_connections: Option<u32>, max_connections: Option<u32>) -> Self {
        Self {
            min_connections,
            max_connections,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_connections == Some(0) {
            return Err(DbError::InvalidArgument(
                "max_connections must be greater than 0".to_string(),
            ));
        }
        if let (Some(min), Some(max)) = (self.min_connections, self.max_connections) {
            if min > max {
                return Err(DbError::InvalidArgument(
                    "min_connections cannot exceed max_connections".to_string(),
                ));
            }
        }
        Ok(())
    }
}

pub trait Database: Send + Sync {
    fn dialect(&self) -> Dialect;

    fn arrow_extraction(&self) -> ArrowExtraction {
        ArrowExtraction::SqlxRows
    }

    fn list_schemas(&self) -> Result<Vec<SchemaInfo>>;

    fn list_relations(&self, schema: &str) -> Result<Vec<RelationInfo>>;

    fn list_columns(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>>;

    fn list_indexes(&self, schema: &str, relation: &str) -> Result<Vec<IndexInfo>>;

    fn list_constraints(&self, schema: &str, relation: &str) -> Result<Vec<ConstraintInfo>>;

    fn relation_info(&self, schema: &str, relation: &str) -> Result<RelationInfo>;

    fn view_definition(&self, schema: &str, view: &str) -> Result<String>;

    fn query(&self, sql: &str, params: &[DbValue]) -> Result<RecordBatchStream>;

    fn insert(
        &self,
        schema: &str,
        relation: &str,
        batches: RecordBatchStream,
        mode: InsertMode,
    ) -> Result<u64>;
}
