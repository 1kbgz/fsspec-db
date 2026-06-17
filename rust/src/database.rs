use arrow::record_batch::RecordBatchReader;

use crate::types::{ColumnInfo, ConstraintInfo, Dialect, IndexInfo, RelationInfo, SchemaInfo};
use crate::Result;

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
