use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{AssertSqlSafe, Column, Row, TypeInfo, ValueRef};
use tokio::runtime::Runtime;

use crate::codec::rows_to_arrow;
use crate::database::{Database, DbValue, InsertMode, RecordBatchStream};
use crate::sql::{insert_sql, quote_identifier};
use crate::types::{
    ColumnInfo, ConstraintInfo, ConstraintKind, Dialect, IndexInfo, RelationInfo, RelationKind,
    SchemaInfo,
};
use crate::{DbError, Result};

pub struct SqliteDatabase {
    pool: SqlitePool,
    runtime: Runtime,
}

impl SqliteDatabase {
    pub fn connect(source: &str) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(DbError::from)?;
        let options = sqlite_options(source)?;
        let pool = runtime.block_on(
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options),
        )?;
        Ok(Self { pool, runtime })
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.runtime.block_on(future)
    }

    async fn list_relations_async(&self, schema: &str) -> Result<Vec<RelationInfo>> {
        let sql = format!(
            "SELECT name, type FROM {}.sqlite_master \
             WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
            quote_identifier(&Dialect::Sqlite, schema)?
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await?;
        let mut relations = Vec::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            let kind: String = row.try_get("type")?;
            let kind = match kind.as_str() {
                "table" => RelationKind::Table,
                "view" => RelationKind::View,
                other => {
                    return Err(DbError::Other(format!(
                        "unknown sqlite relation type: {other}"
                    )))
                }
            };
            relations.push(RelationInfo {
                row_count: relation_row_count(&self.pool, schema, &name, &kind).await?,
                name,
                kind,
                size_bytes: None,
                comment: None,
            });
        }
        Ok(relations)
    }

    async fn list_columns_async(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        self.relation_info_async(schema, relation).await?;
        self.table_columns_async(schema, relation).await
    }

    async fn table_columns_async(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        let sql = format!(
            "PRAGMA {}.table_info({})",
            quote_identifier(&Dialect::Sqlite, schema)?,
            quote_identifier(&Dialect::Sqlite, relation)?
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let cid: i64 = row.try_get("cid")?;
                let name: String = row.try_get("name")?;
                let data_type: String = row.try_get("type")?;
                let not_null: i64 = row.try_get("notnull")?;
                let default: Option<String> = row.try_get("dflt_value")?;
                let primary_key: i64 = row.try_get("pk")?;
                Ok(ColumnInfo {
                    name,
                    data_type,
                    nullable: not_null == 0,
                    default,
                    ordinal: (cid + 1) as u32,
                    primary_key: primary_key > 0,
                    comment: None,
                })
            })
            .collect()
    }

    async fn relation_info_async(&self, schema: &str, relation: &str) -> Result<RelationInfo> {
        let sql = format!(
            "SELECT name, type FROM {}.sqlite_master \
             WHERE type IN ('table', 'view') AND name = ?",
            quote_identifier(&Dialect::Sqlite, schema)?
        );
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(relation)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DbError::NotFound(format!("relation not found: {schema}.{relation}")))?;
        let name: String = row.try_get("name")?;
        let kind: String = row.try_get("type")?;
        let kind = match kind.as_str() {
            "table" => RelationKind::Table,
            "view" => RelationKind::View,
            other => {
                return Err(DbError::Other(format!(
                    "unknown sqlite relation type: {other}"
                )))
            }
        };
        let row_count = relation_row_count(&self.pool, schema, &name, &kind).await?;
        Ok(RelationInfo {
            name,
            kind,
            row_count,
            size_bytes: None,
            comment: None,
        })
    }
}

impl Database for SqliteDatabase {
    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }

    fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.block_on(async {
            let rows = sqlx::query("PRAGMA database_list")
                .fetch_all(&self.pool)
                .await?;
            rows.into_iter()
                .map(|row| {
                    let name: String = row.try_get("name")?;
                    Ok(SchemaInfo {
                        name,
                        catalog: None,
                        comment: None,
                    })
                })
                .collect()
        })
    }

    fn list_relations(&self, schema: &str) -> Result<Vec<RelationInfo>> {
        self.block_on(self.list_relations_async(schema))
    }

    fn list_columns(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        self.block_on(self.list_columns_async(schema, relation))
    }

    fn list_indexes(&self, schema: &str, relation: &str) -> Result<Vec<IndexInfo>> {
        self.block_on(async {
            self.relation_info_async(schema, relation).await?;
            let sql = format!(
                "PRAGMA {}.index_list({})",
                quote_identifier(&Dialect::Sqlite, schema)?,
                quote_identifier(&Dialect::Sqlite, relation)?
            );
            let rows = sqlx::query(AssertSqlSafe(sql))
                .fetch_all(&self.pool)
                .await?;
            let mut indexes = Vec::new();
            for row in rows {
                let name: String = row.try_get("name")?;
                let unique: i64 = row.try_get("unique")?;
                indexes.push(IndexInfo {
                    columns: index_columns(&self.pool, schema, &name).await?,
                    name,
                    unique: unique != 0,
                    method: None,
                });
            }
            Ok(indexes)
        })
    }

    fn list_constraints(&self, schema: &str, relation: &str) -> Result<Vec<ConstraintInfo>> {
        self.block_on(async {
            let columns = self.list_columns_async(schema, relation).await?;
            let mut constraints = Vec::new();
            let pk_columns = columns
                .iter()
                .filter(|column| column.primary_key)
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            if !pk_columns.is_empty() {
                constraints.push(ConstraintInfo {
                    name: format!("pk_{relation}"),
                    kind: ConstraintKind::PrimaryKey,
                    columns: pk_columns,
                    references: None,
                    expr: None,
                });
            }

            let sql = format!(
                "PRAGMA {}.foreign_key_list({})",
                quote_identifier(&Dialect::Sqlite, schema)?,
                quote_identifier(&Dialect::Sqlite, relation)?
            );
            let rows = sqlx::query(AssertSqlSafe(sql))
                .fetch_all(&self.pool)
                .await?;
            let mut by_id: BTreeMap<i64, (String, Vec<(i64, String)>)> = BTreeMap::new();
            for row in rows {
                let id: i64 = row.try_get("id")?;
                let seq: i64 = row.try_get("seq")?;
                let table: String = row.try_get("table")?;
                let from: String = row.try_get("from")?;
                by_id
                    .entry(id)
                    .or_insert((table, Vec::new()))
                    .1
                    .push((seq, from));
            }
            for (id, (table, mut columns)) in by_id {
                columns.sort_by_key(|(seq, _)| *seq);
                constraints.push(ConstraintInfo {
                    name: format!("fk_{relation}_{id}"),
                    kind: ConstraintKind::ForeignKey,
                    columns: columns.into_iter().map(|(_, column)| column).collect(),
                    references: Some(table),
                    expr: None,
                });
            }
            Ok(constraints)
        })
    }

    fn relation_info(&self, schema: &str, relation: &str) -> Result<RelationInfo> {
        self.block_on(self.relation_info_async(schema, relation))
    }

    fn view_definition(&self, schema: &str, view: &str) -> Result<String> {
        self.block_on(async {
            let info = self.relation_info_async(schema, view).await?;
            if info.kind != RelationKind::View {
                return Err(DbError::InvalidArgument(format!(
                    "relation is not a view: {schema}.{view}"
                )));
            }
            let sql = format!(
                "SELECT sql FROM {}.sqlite_master WHERE type = 'view' AND name = ?",
                quote_identifier(&Dialect::Sqlite, schema)?
            );
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(view)
                .fetch_one(&self.pool)
                .await?;
            row.try_get("sql").map_err(DbError::from)
        })
    }

    fn query(&self, sql: &str, params: &[DbValue]) -> Result<RecordBatchStream> {
        self.block_on(async {
            let mut query = sqlx::query(AssertSqlSafe(sql.to_string()));
            for param in params {
                query = bind_value(query, param);
            }
            let rows = query.fetch_all(&self.pool).await?;
            sqlite_rows_to_arrow(rows)
        })
    }

    fn insert(
        &self,
        schema: &str,
        relation: &str,
        mut batches: RecordBatchStream,
        mode: InsertMode,
    ) -> Result<u64> {
        self.block_on(async {
            self.relation_info_async(schema, relation).await?;
            let valid_columns = self
                .table_columns_async(schema, relation)
                .await?
                .into_iter()
                .map(|column| column.name.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            let mut tx = self.pool.begin().await?;
            if mode == InsertMode::Truncate {
                let sql = format!(
                    "DELETE FROM {}.{}",
                    quote_identifier(&Dialect::Sqlite, schema)?,
                    quote_identifier(&Dialect::Sqlite, relation)?
                );
                sqlx::query(AssertSqlSafe(sql)).execute(&mut *tx).await?;
            }

            let mut inserted = 0u64;
            for batch in batches.by_ref() {
                let batch = batch.map_err(DbError::from)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>();
                for column in &columns {
                    if !valid_columns.contains(&column.to_ascii_lowercase()) {
                        return Err(DbError::InvalidArgument(format!(
                            "column not found in {schema}.{relation}: {column}"
                        )));
                    }
                }
                let sql = insert_sql(
                    &Dialect::Sqlite,
                    schema,
                    relation,
                    &columns,
                    batch.num_rows(),
                )?;
                let mut query = sqlx::query(AssertSqlSafe(sql));
                for row in 0..batch.num_rows() {
                    for column in batch.columns() {
                        query = bind_arrow_value(query, column.as_ref(), row)?;
                    }
                }
                inserted += query.execute(&mut *tx).await?.rows_affected();
            }

            tx.commit().await?;
            Ok(inserted)
        })
    }
}

fn sqlite_options(source: &str) -> Result<SqliteConnectOptions> {
    let url = if source == ":memory:" {
        "sqlite::memory:".to_string()
    } else if source.starts_with("sqlite:") {
        source.to_string()
    } else {
        format!("sqlite://{source}")
    };
    SqliteConnectOptions::from_str(&url)
        .map(|options| options.create_if_missing(true))
        .map_err(DbError::from)
}

async fn relation_row_count(
    pool: &SqlitePool,
    schema: &str,
    relation: &str,
    kind: &RelationKind,
) -> Result<Option<u64>> {
    if *kind != RelationKind::Table {
        return Ok(None);
    }
    let sql = format!(
        "SELECT COUNT(*) AS count FROM {}.{}",
        quote_identifier(&Dialect::Sqlite, schema)?,
        quote_identifier(&Dialect::Sqlite, relation)?
    );
    let row = sqlx::query(AssertSqlSafe(sql)).fetch_one(pool).await?;
    let count: i64 = row.try_get("count")?;
    Ok(Some(count as u64))
}

async fn index_columns(pool: &SqlitePool, schema: &str, index: &str) -> Result<Vec<String>> {
    let sql = format!(
        "PRAGMA {}.index_info({})",
        quote_identifier(&Dialect::Sqlite, schema)?,
        quote_identifier(&Dialect::Sqlite, index)?
    );
    let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| row.try_get("name").map_err(DbError::from))
        .collect()
}

fn bind_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    value: &'q DbValue,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    match value {
        DbValue::Null => query.bind(Option::<i64>::None),
        DbValue::Bool(value) => query.bind(*value),
        DbValue::Int64(value) => query.bind(*value),
        DbValue::Float64(value) => query.bind(*value),
        DbValue::String(value) => query.bind(value),
        DbValue::Binary(value) => query.bind(value.as_slice()),
    }
}

fn bind_arrow_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    array: &'q dyn Array,
    row: usize,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>> {
    if array.is_null(row) {
        return Ok(query.bind(Option::<i64>::None));
    }

    match array.data_type() {
        DataType::Null => Ok(query.bind(Option::<i64>::None)),
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array)?;
            let value = i64::try_from(array.value(row)).map_err(|_| {
                DbError::InvalidArgument("UInt64 value does not fit in SQLite INTEGER".to_string())
            })?;
            Ok(query.bind(value))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array)?;
            Ok(query.bind(f64::from(array.value(row))))
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => bind_temporal_value(query, array, row),
        other => Err(DbError::NotSupported(format!(
            "SQLite insert does not support Arrow type {other:?}"
        ))),
    }
}

fn bind_temporal_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    array: &'q dyn Array,
    row: usize,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>> {
    match array.data_type() {
        DataType::Date32 => {
            let array = downcast_array::<arrow::array::Date32Array>(array)?;
            Ok(query.bind(i64::from(array.value(row))))
        }
        DataType::Date64 => {
            let array = downcast_array::<arrow::array::Date64Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let array = downcast_array::<arrow::array::Time32SecondArray>(array)?;
                Ok(query.bind(i64::from(array.value(row))))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array::<arrow::array::Time32MillisecondArray>(array)?;
                Ok(query.bind(i64::from(array.value(row))))
            }
            _ => Err(DbError::NotSupported(format!(
                "SQLite insert does not support Arrow type {:?}",
                array.data_type()
            ))),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                let array = downcast_array::<arrow::array::Time64MicrosecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array::<arrow::array::Time64NanosecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
            _ => Err(DbError::NotSupported(format!(
                "SQLite insert does not support Arrow type {:?}",
                array.data_type()
            ))),
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let array = downcast_array::<arrow::array::TimestampSecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
            TimeUnit::Millisecond => {
                let array = downcast_array::<arrow::array::TimestampMillisecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
            TimeUnit::Microsecond => {
                let array = downcast_array::<arrow::array::TimestampMicrosecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
            TimeUnit::Nanosecond => {
                let array = downcast_array::<arrow::array::TimestampNanosecondArray>(array)?;
                Ok(query.bind(array.value(row)))
            }
        },
        _ => unreachable!(),
    }
}

fn downcast_array<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DbError::InvalidArgument(format!(
            "Arrow array storage does not match type {:?}",
            array.data_type()
        ))
    })
}

fn sqlite_rows_to_arrow(rows: Vec<SqliteRow>) -> Result<RecordBatchStream> {
    let Some(first) = rows.first() else {
        return rows_to_arrow(Vec::new());
    };
    let names = first
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    let mut type_names = vec![Vec::new(); names.len()];
    for row in &rows {
        for (index, names) in type_names.iter_mut().enumerate() {
            let raw = row.try_get_raw(index)?;
            if !raw.is_null() {
                names.push(raw.type_info().name().to_string());
            }
        }
    }
    for (index, names) in type_names.iter_mut().enumerate() {
        if names.is_empty() {
            names.push(first.columns()[index].type_info().name().to_string());
        }
    }
    let mut columns = type_names
        .iter()
        .map(|names| ColumnValues::new(names))
        .collect::<Vec<_>>();

    for row in &rows {
        for (index, values) in columns.iter_mut().enumerate() {
            values.push(row, index)?;
        }
    }

    let fields = names
        .iter()
        .zip(&columns)
        .map(|(name, values)| Field::new(name, values.data_type(), true))
        .collect::<Vec<_>>();
    let arrays = columns
        .into_iter()
        .map(ColumnValues::finish)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    rows_to_arrow(vec![batch])
}

enum ColumnValues {
    Bool(Vec<Option<bool>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Binary(Vec<Option<Vec<u8>>>),
    Utf8(Vec<Option<String>>),
}

impl ColumnValues {
    fn new(sqlite_types: &[String]) -> Self {
        let sqlite_types = sqlite_types
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if sqlite_types
            .iter()
            .any(|sqlite_type| sqlite_type.contains("bool"))
        {
            Self::Bool(Vec::new())
        } else if sqlite_types.iter().any(|sqlite_type| {
            sqlite_type.contains("real")
                || sqlite_type.contains("floa")
                || sqlite_type.contains("doub")
        }) {
            Self::Float64(Vec::new())
        } else if sqlite_types
            .iter()
            .any(|sqlite_type| sqlite_type.contains("int"))
        {
            Self::Int64(Vec::new())
        } else if sqlite_types
            .iter()
            .any(|sqlite_type| sqlite_type.contains("blob"))
        {
            Self::Binary(Vec::new())
        } else {
            Self::Utf8(Vec::new())
        }
    }

    fn push(&mut self, row: &SqliteRow, index: usize) -> Result<()> {
        if row.try_get_raw(index)?.is_null() {
            self.push_null();
            return Ok(());
        }
        match self {
            ColumnValues::Bool(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Int64(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Float64(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Binary(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Utf8(values) => values.push(Some(row.try_get(index)?)),
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            ColumnValues::Bool(values) => values.push(None),
            ColumnValues::Int64(values) => values.push(None),
            ColumnValues::Float64(values) => values.push(None),
            ColumnValues::Binary(values) => values.push(None),
            ColumnValues::Utf8(values) => values.push(None),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            ColumnValues::Bool(_) => DataType::Boolean,
            ColumnValues::Int64(_) => DataType::Int64,
            ColumnValues::Float64(_) => DataType::Float64,
            ColumnValues::Binary(_) => DataType::Binary,
            ColumnValues::Utf8(_) => DataType::Utf8,
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnValues::Bool(values) => Arc::new(BooleanArray::from(values)) as ArrayRef,
            ColumnValues::Int64(values) => Arc::new(Int64Array::from(values)) as ArrayRef,
            ColumnValues::Float64(values) => Arc::new(Float64Array::from(values)) as ArrayRef,
            ColumnValues::Binary(values) => {
                let refs = values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                Arc::new(BinaryArray::from(refs)) as ArrayRef
            }
            ColumnValues::Utf8(values) => Arc::new(StringArray::from(values)) as ArrayRef,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::ArrowExtraction;

    fn seeded_db() -> SqliteDatabase {
        let db = SqliteDatabase::connect(":memory:").unwrap();
        db.runtime
            .block_on(async {
                sqlx::query(
                    "CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        score REAL,
                        active BOOLEAN
                    )",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query("CREATE INDEX idx_users_name ON users(name)")
                    .execute(&db.pool)
                    .await?;
                sqlx::query("INSERT INTO users (name, score, active) VALUES ('ada', 1.5, 1), ('grace', NULL, 0)")
                    .execute(&db.pool)
                    .await?;
                sqlx::query("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
                    .execute(&db.pool)
                    .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();
        db
    }

    #[test]
    fn introspects_sqlite_database() {
        let db = seeded_db();
        assert_eq!(db.arrow_extraction(), ArrowExtraction::SqlxRows);

        assert_eq!(db.list_schemas().unwrap()[0].name, "main");
        let relations = db.list_relations("main").unwrap();
        assert!(relations
            .iter()
            .any(|relation| relation.name == "users" && relation.kind == RelationKind::Table));
        assert!(
            relations
                .iter()
                .any(|relation| relation.name == "active_users"
                    && relation.kind == RelationKind::View)
        );

        let columns = db.list_columns("main", "users").unwrap();
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].primary_key);
        assert_eq!(columns[1].data_type, "TEXT");

        let indexes = db.list_indexes("main", "users").unwrap();
        assert_eq!(indexes[0].columns, vec!["name"]);

        let constraints = db.list_constraints("main", "users").unwrap();
        assert_eq!(constraints[0].kind, ConstraintKind::PrimaryKey);

        assert!(db
            .view_definition("main", "active_users")
            .unwrap()
            .contains("CREATE VIEW active_users"));
    }

    #[test]
    fn queries_sqlite_to_arrow() {
        let db = seeded_db();

        let mut reader = db
            .query(
                "SELECT id, name, score FROM users WHERE id > ? ORDER BY id",
                &[DbValue::Int64(0)],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn inserts_and_truncates_sqlite_database() {
        let db = seeded_db();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let append = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("katherine")])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(3.0)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ],
        )
        .unwrap();

        let inserted = db
            .insert(
                "main",
                "users",
                rows_to_arrow(vec![append]).unwrap(),
                InsertMode::Append,
            )
            .unwrap();
        assert_eq!(inserted, 1);

        let mut reader = db.query("SELECT name FROM users ORDER BY id", &[]).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let truncate = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("dorothy")])) as ArrayRef,
                Arc::new(Float64Array::from(vec![None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef,
            ],
        )
        .unwrap();
        let inserted = db
            .insert(
                "main",
                "users",
                rows_to_arrow(vec![truncate]).unwrap(),
                InsertMode::Truncate,
            )
            .unwrap();
        assert_eq!(inserted, 1);

        let mut reader = db.query("SELECT name FROM users ORDER BY id", &[]).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(names.value(0), "dorothy");
    }

    #[test]
    fn rejects_unknown_insert_columns() {
        let db = seeded_db();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "missing",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        let err = db
            .insert(
                "main",
                "users",
                rows_to_arrow(vec![batch]).unwrap(),
                InsertMode::Append,
            )
            .unwrap_err();

        assert!(matches!(err, DbError::InvalidArgument(_)));
    }

    #[test]
    fn infers_query_type_after_null_values() {
        let db = seeded_db();

        let mut reader = db
            .query(
                "SELECT NULL AS maybe_id UNION ALL SELECT 42 AS maybe_id",
                &[],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert!(batch.column(0).is_null(0));
    }
}
