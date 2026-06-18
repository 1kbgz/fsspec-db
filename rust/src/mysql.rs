use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use sqlx::mysql::{
    MySqlArguments, MySqlColumn, MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow,
};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sqlx::types::{BigDecimal, JsonValue};
use sqlx::{AssertSqlSafe, Column, Executor, Row, SqlSafeStr, Statement, TypeInfo, ValueRef};
use tokio::runtime::Runtime;

use crate::codec::rows_to_arrow;
use crate::database::{Database, DbValue, InsertMode, RecordBatchStream};
use crate::sql::{insert_sql, quote_identifier};
use crate::types::{
    ColumnInfo, ConstraintInfo, ConstraintKind, Dialect, IndexInfo, RelationInfo, RelationKind,
    SchemaInfo,
};
use crate::{DbError, Result};

type MySqlQuery<'q> = sqlx::query::Query<'q, sqlx::MySql, MySqlArguments>;

pub struct MySqlDatabase {
    pool: MySqlPool,
    runtime: Runtime,
}

impl MySqlDatabase {
    pub fn connect(source: &str) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(DbError::from)?;
        let options = MySqlConnectOptions::from_str(source)?;
        let pool = runtime.block_on(MySqlPoolOptions::new().connect_with(options))?;
        Ok(Self { pool, runtime })
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.runtime.block_on(future)
    }

    async fn relation_info_async(&self, schema: &str, relation: &str) -> Result<RelationInfo> {
        let row = sqlx::query(
            "SELECT
                table_name AS table_name,
                table_type AS table_type,
                table_rows AS table_rows
             FROM information_schema.tables
             WHERE table_schema = ?
               AND table_name = ?
               AND table_type IN ('BASE TABLE', 'VIEW')",
        )
        .bind(schema)
        .bind(relation)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("relation not found: {schema}.{relation}")))?;
        relation_from_row(row)
    }

    async fn table_columns_async(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        let rows = sqlx::query(
            "SELECT
                column_name AS column_name,
                column_type AS column_type,
                is_nullable AS is_nullable,
                column_default AS column_default,
                ordinal_position AS ordinal_position,
                column_key AS column_key
             FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ?
             ORDER BY ordinal_position",
        )
        .bind(schema)
        .bind(relation)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let nullable: String = row.try_get("is_nullable")?;
                let ordinal: u32 = row.try_get("ordinal_position")?;
                let column_key: String = row.try_get("column_key")?;
                Ok(ColumnInfo {
                    name: row.try_get("column_name")?,
                    data_type: row.try_get("column_type")?,
                    nullable: nullable == "YES",
                    default: row.try_get("column_default")?,
                    ordinal,
                    primary_key: column_key == "PRI",
                    comment: None,
                })
            })
            .collect()
    }
}

impl Database for MySqlDatabase {
    fn dialect(&self) -> Dialect {
        Dialect::MySql
    }

    fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT schema_name AS schema_name
                 FROM information_schema.schemata
                 WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                 ORDER BY schema_name",
            )
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(SchemaInfo {
                        name: row.try_get("schema_name")?,
                        catalog: None,
                        comment: None,
                    })
                })
                .collect()
        })
    }

    fn list_relations(&self, schema: &str) -> Result<Vec<RelationInfo>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT
                    table_name AS table_name,
                    table_type AS table_type,
                    table_rows AS table_rows
                 FROM information_schema.tables
                 WHERE table_schema = ?
                   AND table_type IN ('BASE TABLE', 'VIEW')
                 ORDER BY table_name",
            )
            .bind(schema)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter().map(relation_from_row).collect()
        })
    }

    fn list_columns(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        self.block_on(async {
            self.relation_info_async(schema, relation).await?;
            self.table_columns_async(schema, relation).await
        })
    }

    fn list_indexes(&self, schema: &str, relation: &str) -> Result<Vec<IndexInfo>> {
        self.block_on(async {
            self.relation_info_async(schema, relation).await?;
            let rows = sqlx::query(
                "SELECT
                    index_name AS index_name,
                    MIN(non_unique) AS non_unique,
                    MAX(index_type) AS index_type,
                    GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns
                 FROM information_schema.statistics
                 WHERE table_schema = ? AND table_name = ?
                 GROUP BY index_name
                 ORDER BY index_name",
            )
            .bind(schema)
            .bind(relation)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    let columns: Option<String> = row.try_get("columns")?;
                    let non_unique: i64 = row.try_get("non_unique")?;
                    Ok(IndexInfo {
                        name: row.try_get("index_name")?,
                        columns: split_csv(columns.as_deref().unwrap_or("")),
                        unique: non_unique == 0,
                        method: row.try_get("index_type")?,
                    })
                })
                .collect()
        })
    }

    fn list_constraints(&self, schema: &str, relation: &str) -> Result<Vec<ConstraintInfo>> {
        self.block_on(async {
            self.relation_info_async(schema, relation).await?;
            let rows = sqlx::query(
                "SELECT
                    tc.constraint_name AS constraint_name,
                    tc.constraint_type AS constraint_type,
                    COALESCE(GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position SEPARATOR ','), '')
                        AS columns,
                    MAX(
                        CASE
                            WHEN kcu.referenced_table_name IS NOT NULL
                            THEN CONCAT(kcu.referenced_table_schema, '.', kcu.referenced_table_name)
                            ELSE NULL
                        END
                    ) AS referenced_relation,
                    MAX(cc.check_clause) AS check_clause
                 FROM information_schema.table_constraints tc
                 LEFT JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_schema = kcu.constraint_schema
                  AND tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                 LEFT JOIN information_schema.check_constraints cc
                   ON tc.constraint_schema = cc.constraint_schema
                  AND tc.constraint_name = cc.constraint_name
                 WHERE tc.table_schema = ? AND tc.table_name = ?
                 GROUP BY tc.constraint_name, tc.constraint_type
                 ORDER BY tc.constraint_name",
            )
            .bind(schema)
            .bind(relation)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    let kind = constraint_kind(row.try_get::<String, _>("constraint_type")?.as_str())?;
                    let references = if kind == ConstraintKind::ForeignKey {
                        row.try_get("referenced_relation")?
                    } else {
                        None
                    };
                    let expr = if kind == ConstraintKind::Check {
                        row.try_get("check_clause")?
                    } else {
                        None
                    };
                    let columns: String = row.try_get("columns")?;
                    Ok(ConstraintInfo {
                        name: row.try_get("constraint_name")?,
                        kind,
                        columns: split_csv(&columns),
                        references,
                        expr,
                    })
                })
                .collect()
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
            let row = sqlx::query(
                "SELECT view_definition AS view_definition
                 FROM information_schema.views
                 WHERE table_schema = ? AND table_name = ?",
            )
            .bind(schema)
            .bind(view)
            .fetch_one(&self.pool)
            .await?;
            row.try_get("view_definition").map_err(DbError::from)
        })
    }

    fn query(&self, sql: &str, params: &[DbValue]) -> Result<RecordBatchStream> {
        self.block_on(async {
            let mut query = sqlx::query(AssertSqlSafe(sql.to_string()));
            for param in params {
                query = bind_value(query, param);
            }
            let rows = query.fetch_all(&self.pool).await?;
            let columns = match rows.first() {
                Some(row) => mysql_column_metadata(row.columns()),
                None => {
                    let mut conn = self.pool.acquire().await?;
                    let statement = (&mut *conn)
                        .prepare(AssertSqlSafe(sql.to_string()).into_sql_str())
                        .await?;
                    mysql_column_metadata(statement.columns())
                }
            };
            mysql_rows_to_arrow(columns, rows)
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
                    quote_identifier(&Dialect::MySql, schema)?,
                    quote_identifier(&Dialect::MySql, relation)?
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
                    &Dialect::MySql,
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

fn relation_from_row(row: MySqlRow) -> Result<RelationInfo> {
    let kind = relation_kind(row.try_get::<String, _>("table_type")?.as_str())?;
    let row_count = if kind == RelationKind::Table {
        row.try_get("table_rows")?
    } else {
        None
    };
    Ok(RelationInfo {
        name: row.try_get("table_name")?,
        kind,
        row_count,
        size_bytes: None,
        comment: None,
    })
}

fn bind_value<'q>(query: MySqlQuery<'q>, value: &'q DbValue) -> MySqlQuery<'q> {
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
    query: MySqlQuery<'q>,
    array: &'q dyn Array,
    row: usize,
) -> Result<MySqlQuery<'q>> {
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
            Ok(query.bind(array.value(row)))
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array)?;
            Ok(query.bind(array.value(row)))
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array)?;
            Ok(query.bind(array.value(row)))
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
        other => Err(DbError::NotSupported(format!(
            "MySQL insert does not support Arrow type {other:?}"
        ))),
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

fn mysql_column_metadata(columns: &[MySqlColumn]) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|column| {
            (
                column.name().to_string(),
                column.type_info().name().to_string(),
            )
        })
        .collect()
}

fn mysql_rows_to_arrow(
    column_metadata: Vec<(String, String)>,
    rows: Vec<MySqlRow>,
) -> Result<RecordBatchStream> {
    let names = column_metadata
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let mut type_names = column_metadata
        .into_iter()
        .map(|(_, type_name)| vec![type_name])
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, names) in type_names.iter_mut().enumerate() {
            let raw = row.try_get_raw(index)?;
            if !raw.is_null() {
                names.push(raw.type_info().name().to_string());
            }
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
    SignedInt(Vec<Option<i64>>),
    UnsignedInt(Vec<Option<i64>>),
    Float32(Vec<Option<f64>>),
    Float64(Vec<Option<f64>>),
    Binary(Vec<Option<Vec<u8>>>),
    Utf8(Vec<Option<String>>),
    Date32(Vec<Option<i32>>),
    Timestamp(Vec<Option<i64>>),
    Decimal(Vec<Option<String>>),
    Json(Vec<Option<String>>),
    Unsupported(String, Vec<Option<String>>),
}

impl ColumnValues {
    fn new(mysql_types: &[String]) -> Self {
        let mysql_type = mysql_types
            .first()
            .map(|value| value.to_ascii_uppercase())
            .unwrap_or_else(|| "TEXT".to_string());
        match mysql_type.as_str() {
            "BOOLEAN" => Self::Bool(Vec::new()),
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" | "YEAR" => {
                Self::SignedInt(Vec::new())
            }
            "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
            | "BIGINT UNSIGNED" | "BIT" => Self::UnsignedInt(Vec::new()),
            "FLOAT" => Self::Float32(Vec::new()),
            "DOUBLE" => Self::Float64(Vec::new()),
            "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                Self::Binary(Vec::new())
            }
            "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM"
            | "SET" | "NULL" => Self::Utf8(Vec::new()),
            "DATE" => Self::Date32(Vec::new()),
            "DATETIME" | "TIMESTAMP" => Self::Timestamp(Vec::new()),
            "DECIMAL" => Self::Decimal(Vec::new()),
            "JSON" => Self::Json(Vec::new()),
            other => Self::Unsupported(other.to_string(), Vec::new()),
        }
    }

    fn push(&mut self, row: &MySqlRow, index: usize) -> Result<()> {
        if row.try_get_raw(index)?.is_null() {
            self.push_null();
            return Ok(());
        }
        match self {
            ColumnValues::Bool(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::SignedInt(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::UnsignedInt(values) => {
                let value: u64 = row.try_get(index)?;
                values.push(Some(i64::try_from(value).map_err(|_| {
                    DbError::InvalidArgument(
                        "MySQL unsigned integer does not fit in Arrow Int64".to_string(),
                    )
                })?));
            }
            ColumnValues::Float32(values) => {
                values.push(Some(f64::from(row.try_get::<f32, _>(index)?)))
            }
            ColumnValues::Float64(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Binary(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Utf8(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Date32(values) => values.push(Some(naive_date_to_days(
                row.try_get::<NaiveDate, _>(index)?,
            ))),
            ColumnValues::Timestamp(values) => values.push(Some(timestamp_micros(row, index)?)),
            ColumnValues::Decimal(values) => {
                values.push(Some(row.try_get::<BigDecimal, _>(index)?.to_string()))
            }
            ColumnValues::Json(values) => {
                values.push(Some(row.try_get::<JsonValue, _>(index)?.to_string()))
            }
            ColumnValues::Unsupported(name, _) => {
                return Err(DbError::NotSupported(format!(
                    "MySQL query output type is not yet mapped to Arrow: {name}"
                )))
            }
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            ColumnValues::Bool(values) => values.push(None),
            ColumnValues::SignedInt(values) | ColumnValues::UnsignedInt(values) => {
                values.push(None)
            }
            ColumnValues::Float32(values) | ColumnValues::Float64(values) => values.push(None),
            ColumnValues::Binary(values) => values.push(None),
            ColumnValues::Utf8(values) | ColumnValues::Unsupported(_, values) => values.push(None),
            ColumnValues::Date32(values) => values.push(None),
            ColumnValues::Timestamp(values) => values.push(None),
            ColumnValues::Decimal(values) | ColumnValues::Json(values) => values.push(None),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            ColumnValues::Bool(_) => DataType::Boolean,
            ColumnValues::SignedInt(_) | ColumnValues::UnsignedInt(_) => DataType::Int64,
            ColumnValues::Float32(_) | ColumnValues::Float64(_) => DataType::Float64,
            ColumnValues::Binary(_) => DataType::Binary,
            ColumnValues::Utf8(_)
            | ColumnValues::Unsupported(_, _)
            | ColumnValues::Decimal(_)
            | ColumnValues::Json(_) => DataType::Utf8,
            ColumnValues::Date32(_) => DataType::Date32,
            ColumnValues::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnValues::Bool(values) => Arc::new(BooleanArray::from(values)) as ArrayRef,
            ColumnValues::SignedInt(values) | ColumnValues::UnsignedInt(values) => {
                Arc::new(Int64Array::from(values)) as ArrayRef
            }
            ColumnValues::Float32(values) | ColumnValues::Float64(values) => {
                Arc::new(Float64Array::from(values)) as ArrayRef
            }
            ColumnValues::Binary(values) => {
                let refs = values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                Arc::new(BinaryArray::from(refs)) as ArrayRef
            }
            ColumnValues::Utf8(values)
            | ColumnValues::Unsupported(_, values)
            | ColumnValues::Decimal(values)
            | ColumnValues::Json(values) => Arc::new(StringArray::from(values)) as ArrayRef,
            ColumnValues::Date32(values) => Arc::new(Date32Array::from(values)) as ArrayRef,
            ColumnValues::Timestamp(values) => {
                Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef
            }
        }
    }
}

fn naive_date_to_days(date: NaiveDate) -> i32 {
    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch"))
        .num_days() as i32
}

fn timestamp_micros(row: &MySqlRow, index: usize) -> Result<i64> {
    // MySQL `TIMESTAMP` may decode to `DateTime<Utc>`; `DATETIME` to `NaiveDateTime`.
    match row.try_get::<DateTime<Utc>, _>(index) {
        Ok(value) => Ok(value.timestamp_micros()),
        Err(_) => Ok(row
            .try_get::<NaiveDateTime, _>(index)?
            .and_utc()
            .timestamp_micros()),
    }
}

fn relation_kind(table_type: &str) -> Result<RelationKind> {
    match table_type {
        "BASE TABLE" => Ok(RelationKind::Table),
        "VIEW" => Ok(RelationKind::View),
        other => Err(DbError::Other(format!(
            "unknown MySQL relation type: {other}"
        ))),
    }
}

fn constraint_kind(constraint_type: &str) -> Result<ConstraintKind> {
    match constraint_type {
        "PRIMARY KEY" => Ok(ConstraintKind::PrimaryKey),
        "FOREIGN KEY" => Ok(ConstraintKind::ForeignKey),
        "UNIQUE" => Ok(ConstraintKind::Unique),
        "CHECK" => Ok(ConstraintKind::Check),
        other => Err(DbError::Other(format!(
            "unknown MySQL constraint type: {other}"
        ))),
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::ArrowExtraction;

    #[test]
    fn classifies_common_mysql_types() {
        assert_eq!(
            ColumnValues::new(&["BOOLEAN".to_string()]).data_type(),
            DataType::Boolean
        );
        assert_eq!(
            ColumnValues::new(&["INT UNSIGNED".to_string()]).data_type(),
            DataType::Int64
        );
        assert_eq!(
            ColumnValues::new(&["DOUBLE".to_string()]).data_type(),
            DataType::Float64
        );
        assert_eq!(
            ColumnValues::new(&["BLOB".to_string()]).data_type(),
            DataType::Binary
        );
        assert_eq!(
            ColumnValues::new(&["TEXT".to_string()]).data_type(),
            DataType::Utf8
        );
    }

    #[test]
    fn splits_mysql_column_lists() {
        assert_eq!(split_csv("id,name"), vec!["id", "name"]);
        assert!(split_csv("").is_empty());
    }

    #[test]
    fn builds_empty_arrow_schema_from_mysql_metadata() {
        let mut reader = mysql_rows_to_arrow(
            vec![
                ("id".to_string(), "BIGINT".to_string()),
                ("name".to_string(), "VARCHAR".to_string()),
            ],
            Vec::new(),
        )
        .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).name(), "name");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn mysql_rich_types_map_to_arrow() {
        let Ok(url) = std::env::var("FSSPEC_DB_MYSQL_URL") else {
            return;
        };
        let db = MySqlDatabase::connect(&url).unwrap();
        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS fsspec_db_rich")
                    .execute(&db.pool)
                    .await?;
                sqlx::query(
                    "CREATE TABLE fsspec_db_rich (
                        amount DECIMAL(10, 2),
                        created DATE,
                        ts DATETIME,
                        doc JSON
                    )",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query(
                    "INSERT INTO fsspec_db_rich VALUES
                     (12.34, '2020-01-02', '2020-01-02 03:04:05', '{\"a\": 1}'),
                     (NULL, NULL, NULL, NULL)",
                )
                .execute(&db.pool)
                .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();

        let mut reader = db
            .query(
                "SELECT amount, created, ts, doc FROM fsspec_db_rich ORDER BY created IS NULL, created",
                &[],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8); // decimal -> text
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8); // json -> text
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn mysql_integration_round_trips_when_configured() {
        let Ok(url) = std::env::var("FSSPEC_DB_MYSQL_URL") else {
            return;
        };
        let db = MySqlDatabase::connect(&url).unwrap();
        assert_eq!(db.arrow_extraction(), ArrowExtraction::SqlxRows);
        let schema: String = db
            .runtime
            .block_on(async {
                let row = sqlx::query("SELECT DATABASE() AS schema_name")
                    .fetch_one(&db.pool)
                    .await?;
                row.try_get("schema_name")
            })
            .unwrap();
        assert!(!schema.is_empty());

        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS fsspec_db_users")
                    .execute(&db.pool)
                    .await?;
                sqlx::query(
                    "CREATE TABLE fsspec_db_users (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        score DOUBLE,
                        active BOOLEAN,
                        payload BLOB
                    )",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query("CREATE INDEX idx_fsspec_db_users_name ON fsspec_db_users(name)")
                    .execute(&db.pool)
                    .await?;
                sqlx::query(
                    "INSERT INTO fsspec_db_users (name, score, active, payload)
                     VALUES ('ada', 1.5, TRUE, X'01'), ('grace', NULL, FALSE, NULL)",
                )
                .execute(&db.pool)
                .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();

        let relations = db.list_relations(&schema).unwrap();
        assert!(relations
            .iter()
            .any(|relation| relation.name == "fsspec_db_users"));
        let columns = db.list_columns(&schema, "fsspec_db_users").unwrap();
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].primary_key);
        let indexes = db.list_indexes(&schema, "fsspec_db_users").unwrap();
        assert!(indexes.iter().any(|index| index.columns == vec!["name"]));

        let mut reader = db
            .query(
                "SELECT id, name, score, active, payload
                 FROM fsspec_db_users
                 WHERE id > ?
                 ORDER BY id",
                &[DbValue::Int64(0)],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Boolean);
        assert_eq!(batch.schema().field(4).data_type(), &DataType::Binary);

        let mut reader = db
            .query("SELECT id, name FROM fsspec_db_users WHERE id < 0", &[])
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");

        let insert_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let payloads: Vec<Option<&[u8]>> = vec![Some(&[2])];
        let batch = RecordBatch::try_new(
            insert_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("katherine")])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(3.0)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
                Arc::new(BinaryArray::from(payloads)) as ArrayRef,
            ],
        )
        .unwrap();
        let inserted = db
            .insert(
                &schema,
                "fsspec_db_users",
                rows_to_arrow(vec![batch]).unwrap(),
                InsertMode::Append,
            )
            .unwrap();
        assert_eq!(inserted, 1);

        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS fsspec_db_users")
                    .execute(&db.pool)
                    .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();
    }
}
