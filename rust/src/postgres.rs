use std::collections::{BTreeSet, HashSet};
use std::fmt::Write;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use sqlx::postgres::{PgArguments, PgColumn, PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::types::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use sqlx::types::{BigDecimal, JsonValue, Uuid};
use sqlx::{AssertSqlSafe, Column, Executor, Row, SqlSafeStr, Statement, TypeInfo, ValueRef};
use tokio::runtime::Runtime;

use crate::codec::rows_to_arrow;
use crate::database::{Database, DbPoolOptions, DbValue, InsertMode, RecordBatchStream};
use crate::sql::quote_identifier;
use crate::types::{
    ColumnInfo, ConstraintInfo, ConstraintKind, Dialect, IndexInfo, RelationInfo, RelationKind,
    SchemaInfo,
};
use crate::{DbError, Result};

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, PgArguments>;

pub struct PostgresDatabase {
    pool: PgPool,
    runtime: Runtime,
}

impl PostgresDatabase {
    pub fn connect(source: &str) -> Result<Self> {
        Self::connect_with_pool_options(source, DbPoolOptions::default())
    }

    pub fn connect_with_pool_options(source: &str, pool_options: DbPoolOptions) -> Result<Self> {
        pool_options.validate()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(DbError::from)?;
        let options = PgConnectOptions::from_str(source)?;
        let mut builder = PgPoolOptions::new();
        if let Some(min_connections) = pool_options.min_connections {
            builder = builder.min_connections(min_connections);
        }
        if let Some(max_connections) = pool_options.max_connections {
            builder = builder.max_connections(max_connections);
        }
        let pool = runtime.block_on(builder.connect_with(options))?;
        Ok(Self { pool, runtime })
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.runtime.block_on(future)
    }

    async fn relation_info_async(&self, schema: &str, relation: &str) -> Result<RelationInfo> {
        let row = sqlx::query(
            "SELECT table_name, table_type
             FROM information_schema.tables
             WHERE table_schema = $1
               AND table_name = $2
               AND table_type IN ('BASE TABLE', 'VIEW')",
        )
        .bind(schema)
        .bind(relation)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("relation not found: {schema}.{relation}")))?;
        let name: String = row.try_get("table_name")?;
        let kind = relation_kind(row.try_get::<String, _>("table_type")?.as_str())?;
        let row_count = relation_row_count(&self.pool, schema, &name, &kind).await?;
        Ok(RelationInfo {
            name,
            kind,
            row_count,
            size_bytes: None,
            comment: None,
        })
    }

    async fn table_columns_async(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
        let primary_keys = primary_key_columns(&self.pool, schema, relation).await?;
        let rows = sqlx::query(
            "SELECT column_name, data_type, udt_name, is_nullable, column_default, ordinal_position
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
        )
        .bind(schema)
        .bind(relation)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let name: String = row.try_get("column_name")?;
                let data_type: String = row.try_get("data_type")?;
                let udt_name: String = row.try_get("udt_name")?;
                let nullable: String = row.try_get("is_nullable")?;
                let ordinal: i32 = row.try_get("ordinal_position")?;
                Ok(ColumnInfo {
                    primary_key: primary_keys.contains(&name),
                    name,
                    data_type: if data_type == "USER-DEFINED" {
                        udt_name
                    } else {
                        data_type
                    },
                    nullable: nullable == "YES",
                    default: row.try_get("column_default")?,
                    ordinal: ordinal as u32,
                    comment: None,
                })
            })
            .collect()
    }
}

impl Database for PostgresDatabase {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT schema_name
                 FROM information_schema.schemata
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                   AND schema_name NOT LIKE 'pg_toast%'
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
                    t.table_name,
                    t.table_type,
                    c.reltuples::bigint AS row_count
                 FROM information_schema.tables t
                 LEFT JOIN pg_namespace n
                   ON n.nspname = t.table_schema
                 LEFT JOIN pg_class c
                   ON c.relnamespace = n.oid
                  AND c.relname = t.table_name
                 WHERE t.table_schema = $1
                   AND t.table_type IN ('BASE TABLE', 'VIEW')
                 ORDER BY t.table_name",
            )
            .bind(schema)
            .fetch_all(&self.pool)
            .await?;
            let mut relations = Vec::new();
            for row in rows {
                let name: String = row.try_get("table_name")?;
                let kind = relation_kind(row.try_get::<String, _>("table_type")?.as_str())?;
                let row_count = if kind == RelationKind::Table {
                    let estimate: Option<i64> = row.try_get("row_count")?;
                    estimate.and_then(|value| u64::try_from(value).ok())
                } else {
                    None
                };
                relations.push(RelationInfo {
                    name,
                    kind,
                    row_count,
                    size_bytes: None,
                    comment: None,
                });
            }
            Ok(relations)
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
                    i.relname AS name,
                    ix.indisunique AS is_unique,
                    am.amname AS method,
                    COALESCE(
                        string_agg(a.attname, ',' ORDER BY ord.ordinality)
                            FILTER (WHERE a.attname IS NOT NULL),
                        ''
                    ) AS columns
                 FROM pg_class t
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 JOIN pg_index ix ON ix.indrelid = t.oid
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_am am ON am.oid = i.relam
                 LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                    ON TRUE
                 LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
                 WHERE n.nspname = $1 AND t.relname = $2
                 GROUP BY i.relname, ix.indisunique, am.amname
                 ORDER BY i.relname",
            )
            .bind(schema)
            .bind(relation)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    let columns: String = row.try_get("columns")?;
                    Ok(IndexInfo {
                        name: row.try_get("name")?,
                        columns: split_csv(&columns),
                        unique: row.try_get("is_unique")?,
                        method: row.try_get("method")?,
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
                    con.conname AS constraint_name,
                    con.contype AS constraint_type,
                    COALESCE(string_agg(att.attname, ',' ORDER BY ord.ordinality), '') AS columns,
                    CASE
                        WHEN con.confrelid <> 0 THEN ref_ns.nspname || '.' || ref_rel.relname
                        ELSE NULL
                    END AS references,
                    CASE
                        WHEN con.contype = 'c' THEN pg_get_constraintdef(con.oid, true)
                        ELSE NULL
                    END AS check_clause
                 FROM pg_constraint con
                 JOIN pg_class rel ON rel.oid = con.conrelid
                 JOIN pg_namespace ns ON ns.oid = rel.relnamespace
                 LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, ordinality)
                    ON TRUE
                 LEFT JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attnum = ord.attnum
                 LEFT JOIN pg_class ref_rel ON ref_rel.oid = con.confrelid
                 LEFT JOIN pg_namespace ref_ns ON ref_ns.oid = ref_rel.relnamespace
                 WHERE ns.nspname = $1
                   AND rel.relname = $2
                   AND con.contype IN ('p', 'f', 'u', 'c')
                 GROUP BY con.oid, con.conname, con.contype, ref_ns.nspname, ref_rel.relname
                 ORDER BY con.conname",
            )
            .bind(schema)
            .bind(relation)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| {
                    let kind =
                        constraint_kind(row.try_get::<String, _>("constraint_type")?.as_str())?;
                    let references = if kind == ConstraintKind::ForeignKey {
                        row.try_get("references")?
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
                "SELECT view_definition
                 FROM information_schema.views
                 WHERE table_schema = $1 AND table_name = $2",
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
                Some(row) => postgres_column_metadata(row.columns()),
                None => {
                    let mut conn = self.pool.acquire().await?;
                    let statement = (&mut *conn)
                        .prepare(AssertSqlSafe(sql.to_string()).into_sql_str())
                        .await?;
                    postgres_column_metadata(statement.columns())
                }
            };
            postgres_rows_to_arrow(columns, rows)
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
                    "TRUNCATE TABLE {}.{}",
                    quote_identifier(&Dialect::Postgres, schema)?,
                    quote_identifier(&Dialect::Postgres, relation)?
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
                let sql = copy_in_sql(schema, relation, &columns)?;
                let data = batch_to_copy_csv(&batch)?;
                let mut copy = (*tx).copy_in_raw(&sql).await?;
                if let Err(err) = copy.send(data).await {
                    let _ = copy.abort("fsspec-db COPY input failed").await;
                    return Err(DbError::from(err));
                }
                inserted += copy.finish().await?;
            }

            tx.commit().await?;
            Ok(inserted)
        })
    }
}

fn copy_in_sql(schema: &str, relation: &str, columns: &[String]) -> Result<String> {
    if columns.is_empty() {
        return Err(DbError::InvalidArgument(
            "COPY requires at least one column".to_string(),
        ));
    }

    let relation = format!(
        "{}.{}",
        quote_identifier(&Dialect::Postgres, schema)?,
        quote_identifier(&Dialect::Postgres, relation)?
    );
    let columns = columns
        .iter()
        .map(|column| quote_identifier(&Dialect::Postgres, column))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "COPY {relation} ({columns}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
    ))
}

fn batch_to_copy_csv(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    for row in 0..batch.num_rows() {
        for (index, column) in batch.columns().iter().enumerate() {
            if index > 0 {
                data.push(b',');
            }
            match copy_csv_value(column.as_ref(), row)? {
                Some(value) => push_csv_escaped_field(&mut data, value.as_bytes()),
                None => data.extend_from_slice(b"\\N"),
            }
        }
        data.push(b'\n');
    }
    Ok(data)
}

fn copy_csv_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
        return Ok(None);
    }

    let value = match array.data_type() {
        DataType::Null => return Ok(None),
        DataType::Boolean => {
            let array = downcast_array::<BooleanArray>(array)?;
            (if array.value(row) { "true" } else { "false" }).to_string()
        }
        DataType::Int8 => {
            let array = downcast_array::<Int8Array>(array)?;
            i16::from(array.value(row)).to_string()
        }
        DataType::Int16 => {
            let array = downcast_array::<Int16Array>(array)?;
            array.value(row).to_string()
        }
        DataType::Int32 => {
            let array = downcast_array::<Int32Array>(array)?;
            array.value(row).to_string()
        }
        DataType::Int64 => {
            let array = downcast_array::<Int64Array>(array)?;
            array.value(row).to_string()
        }
        DataType::UInt8 => {
            let array = downcast_array::<UInt8Array>(array)?;
            i16::from(array.value(row)).to_string()
        }
        DataType::UInt16 => {
            let array = downcast_array::<UInt16Array>(array)?;
            i32::from(array.value(row)).to_string()
        }
        DataType::UInt32 => {
            let array = downcast_array::<UInt32Array>(array)?;
            i64::from(array.value(row)).to_string()
        }
        DataType::UInt64 => {
            let array = downcast_array::<UInt64Array>(array)?;
            let value = i64::try_from(array.value(row)).map_err(|_| {
                DbError::InvalidArgument("UInt64 value does not fit in Postgres BIGINT".to_string())
            })?;
            value.to_string()
        }
        DataType::Float32 => {
            let array = downcast_array::<Float32Array>(array)?;
            array.value(row).to_string()
        }
        DataType::Float64 => {
            let array = downcast_array::<Float64Array>(array)?;
            array.value(row).to_string()
        }
        DataType::Utf8 => {
            let array = downcast_array::<StringArray>(array)?;
            array.value(row).to_string()
        }
        DataType::LargeUtf8 => {
            let array = downcast_array::<LargeStringArray>(array)?;
            array.value(row).to_string()
        }
        DataType::Binary => {
            let array = downcast_array::<BinaryArray>(array)?;
            postgres_bytea_hex(array.value(row))?
        }
        DataType::LargeBinary => {
            let array = downcast_array::<LargeBinaryArray>(array)?;
            postgres_bytea_hex(array.value(row))?
        }
        other => {
            return Err(DbError::NotSupported(format!(
                "Postgres insert does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(Some(value))
}

fn push_csv_escaped_field(data: &mut Vec<u8>, value: &[u8]) {
    let mut writer = csv_core::WriterBuilder::new()
        .quote_style(csv_core::QuoteStyle::Always)
        .build();
    let mut input = value;
    let mut out = [0_u8; 1024];
    loop {
        let (result, consumed, written) = writer.field(input, &mut out);
        data.extend_from_slice(&out[..written]);
        input = &input[consumed..];
        match result {
            csv_core::WriteResult::InputEmpty => break,
            csv_core::WriteResult::OutputFull => {}
        }
    }
    loop {
        let (result, written) = writer.finish(&mut out);
        data.extend_from_slice(&out[..written]);
        match result {
            csv_core::WriteResult::InputEmpty => return,
            csv_core::WriteResult::OutputFull => {}
        }
    }
}

fn postgres_bytea_hex(value: &[u8]) -> Result<String> {
    let mut out = String::with_capacity(2 + value.len() * 2);
    out.push_str("\\x");
    for byte in value {
        write!(&mut out, "{byte:02x}").map_err(|err| DbError::Other(err.to_string()))?;
    }
    Ok(out)
}

async fn primary_key_columns(
    pool: &PgPool,
    schema: &str,
    relation: &str,
) -> Result<HashSet<String>> {
    let rows = sqlx::query(
        "SELECT kcu.column_name
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_catalog = kcu.constraint_catalog
          AND tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
         WHERE tc.table_schema = $1
           AND tc.table_name = $2
           AND tc.constraint_type = 'PRIMARY KEY'
         ORDER BY kcu.ordinal_position",
    )
    .bind(schema)
    .bind(relation)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| row.try_get("column_name").map_err(DbError::from))
        .collect()
}

async fn relation_row_count(
    pool: &PgPool,
    schema: &str,
    relation: &str,
    kind: &RelationKind,
) -> Result<Option<u64>> {
    if *kind != RelationKind::Table {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT c.reltuples::bigint AS estimate
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(relation)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let estimate: i64 = row.try_get("estimate")?;
    Ok(u64::try_from(estimate).ok())
}

fn bind_value<'q>(query: PgQuery<'q>, value: &'q DbValue) -> PgQuery<'q> {
    match value {
        DbValue::Null => query.bind(Option::<i64>::None),
        DbValue::Bool(value) => query.bind(*value),
        DbValue::Int64(value) => query.bind(*value),
        DbValue::Float64(value) => query.bind(*value),
        DbValue::String(value) => query.bind(value),
        DbValue::Binary(value) => query.bind(value.as_slice()),
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

fn postgres_column_metadata(columns: &[PgColumn]) -> Vec<(String, String)> {
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

fn postgres_rows_to_arrow(
    column_metadata: Vec<(String, String)>,
    rows: Vec<PgRow>,
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
    Int16(Vec<Option<i64>>),
    Int32(Vec<Option<i64>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f64>>),
    Float64(Vec<Option<f64>>),
    Binary(Vec<Option<Vec<u8>>>),
    Utf8(Vec<Option<String>>),
    Date32(Vec<Option<i32>>),
    Time64(Vec<Option<i64>>),
    Timestamp(Vec<Option<i64>>),
    // Rich scalar types rendered to their lossless text form.
    Text(Vec<Option<String>>, TextKind),
    Unsupported(String, Vec<Option<String>>),
}

#[derive(Clone, Copy)]
enum TextKind {
    Decimal,
    Uuid,
    Json,
}

impl ColumnValues {
    fn new(postgres_types: &[String]) -> Self {
        let postgres_type = postgres_types
            .first()
            .map(|value| value.to_ascii_uppercase())
            .unwrap_or_else(|| "TEXT".to_string());
        match postgres_type.as_str() {
            "BOOL" => Self::Bool(Vec::new()),
            "INT2" => Self::Int16(Vec::new()),
            "INT4" | "OID" => Self::Int32(Vec::new()),
            "INT8" => Self::Int64(Vec::new()),
            "FLOAT4" => Self::Float32(Vec::new()),
            "FLOAT8" => Self::Float64(Vec::new()),
            "BYTEA" => Self::Binary(Vec::new()),
            "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "NAME" => Self::Utf8(Vec::new()),
            "DATE" => Self::Date32(Vec::new()),
            "TIME" => Self::Time64(Vec::new()),
            "TIMESTAMP" | "TIMESTAMPTZ" => Self::Timestamp(Vec::new()),
            "NUMERIC" => Self::Text(Vec::new(), TextKind::Decimal),
            "UUID" => Self::Text(Vec::new(), TextKind::Uuid),
            "JSON" | "JSONB" => Self::Text(Vec::new(), TextKind::Json),
            other => Self::Unsupported(other.to_string(), Vec::new()),
        }
    }

    fn push(&mut self, row: &PgRow, index: usize) -> Result<()> {
        if row.try_get_raw(index)?.is_null() {
            self.push_null();
            return Ok(());
        }
        match self {
            ColumnValues::Bool(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Int16(values) => {
                values.push(Some(i64::from(row.try_get::<i16, _>(index)?)))
            }
            ColumnValues::Int32(values) => {
                values.push(Some(i64::from(row.try_get::<i32, _>(index)?)))
            }
            ColumnValues::Int64(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Float32(values) => {
                values.push(Some(f64::from(row.try_get::<f32, _>(index)?)))
            }
            ColumnValues::Float64(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Binary(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Utf8(values) => values.push(Some(row.try_get(index)?)),
            ColumnValues::Date32(values) => values.push(Some(naive_date_to_days(
                row.try_get::<NaiveDate, _>(index)?,
            ))),
            ColumnValues::Time64(values) => values.push(Some(naive_time_to_micros(
                row.try_get::<NaiveTime, _>(index)?,
            ))),
            ColumnValues::Timestamp(values) => values.push(Some(timestamp_micros(row, index)?)),
            ColumnValues::Text(values, kind) => values.push(Some(decode_text(row, index, *kind)?)),
            ColumnValues::Unsupported(name, _) => {
                return Err(DbError::NotSupported(format!(
                    "Postgres query output type is not yet mapped to Arrow: {name}"
                )))
            }
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            ColumnValues::Bool(values) => values.push(None),
            ColumnValues::Int16(values)
            | ColumnValues::Int32(values)
            | ColumnValues::Int64(values) => values.push(None),
            ColumnValues::Float32(values) | ColumnValues::Float64(values) => values.push(None),
            ColumnValues::Binary(values) => values.push(None),
            ColumnValues::Utf8(values) | ColumnValues::Unsupported(_, values) => values.push(None),
            ColumnValues::Date32(values) => values.push(None),
            ColumnValues::Time64(values) | ColumnValues::Timestamp(values) => values.push(None),
            ColumnValues::Text(values, _) => values.push(None),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            ColumnValues::Bool(_) => DataType::Boolean,
            ColumnValues::Int16(_) | ColumnValues::Int32(_) | ColumnValues::Int64(_) => {
                DataType::Int64
            }
            ColumnValues::Float32(_) | ColumnValues::Float64(_) => DataType::Float64,
            ColumnValues::Binary(_) => DataType::Binary,
            ColumnValues::Utf8(_) | ColumnValues::Unsupported(_, _) | ColumnValues::Text(_, _) => {
                DataType::Utf8
            }
            ColumnValues::Date32(_) => DataType::Date32,
            ColumnValues::Time64(_) => DataType::Time64(TimeUnit::Microsecond),
            ColumnValues::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnValues::Bool(values) => Arc::new(BooleanArray::from(values)) as ArrayRef,
            ColumnValues::Int16(values)
            | ColumnValues::Int32(values)
            | ColumnValues::Int64(values) => Arc::new(Int64Array::from(values)) as ArrayRef,
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
            | ColumnValues::Text(values, _) => Arc::new(StringArray::from(values)) as ArrayRef,
            ColumnValues::Date32(values) => Arc::new(Date32Array::from(values)) as ArrayRef,
            ColumnValues::Time64(values) => {
                Arc::new(Time64MicrosecondArray::from(values)) as ArrayRef
            }
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

fn naive_time_to_micros(time: NaiveTime) -> i64 {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight");
    (time - midnight).num_microseconds().unwrap_or(0)
}

fn timestamp_micros(row: &PgRow, index: usize) -> Result<i64> {
    // `timestamptz` decodes to `DateTime<Utc>`; plain `timestamp` to `NaiveDateTime`.
    match row.try_get::<DateTime<Utc>, _>(index) {
        Ok(value) => Ok(value.timestamp_micros()),
        Err(_) => Ok(row
            .try_get::<NaiveDateTime, _>(index)?
            .and_utc()
            .timestamp_micros()),
    }
}

fn decode_text(row: &PgRow, index: usize, kind: TextKind) -> Result<String> {
    Ok(match kind {
        TextKind::Decimal => row.try_get::<BigDecimal, _>(index)?.to_string(),
        TextKind::Uuid => row.try_get::<Uuid, _>(index)?.to_string(),
        TextKind::Json => row.try_get::<JsonValue, _>(index)?.to_string(),
    })
}

fn relation_kind(table_type: &str) -> Result<RelationKind> {
    match table_type {
        "BASE TABLE" => Ok(RelationKind::Table),
        "VIEW" => Ok(RelationKind::View),
        other => Err(DbError::Other(format!(
            "unknown postgres relation type: {other}"
        ))),
    }
}

fn constraint_kind(constraint_type: &str) -> Result<ConstraintKind> {
    match constraint_type {
        "p" => Ok(ConstraintKind::PrimaryKey),
        "f" => Ok(ConstraintKind::ForeignKey),
        "u" => Ok(ConstraintKind::Unique),
        "c" => Ok(ConstraintKind::Check),
        other => Err(DbError::Other(format!(
            "unknown postgres constraint type: {other}"
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
    fn classifies_common_postgres_types() {
        assert_eq!(
            ColumnValues::new(&["BOOL".to_string()]).data_type(),
            DataType::Boolean
        );
        assert_eq!(
            ColumnValues::new(&["INT4".to_string()]).data_type(),
            DataType::Int64
        );
        assert_eq!(
            ColumnValues::new(&["FLOAT8".to_string()]).data_type(),
            DataType::Float64
        );
        assert_eq!(
            ColumnValues::new(&["BYTEA".to_string()]).data_type(),
            DataType::Binary
        );
        assert_eq!(
            ColumnValues::new(&["TEXT".to_string()]).data_type(),
            DataType::Utf8
        );
    }

    #[test]
    fn splits_postgres_column_lists() {
        assert_eq!(split_csv("id,name"), vec!["id", "name"]);
        assert!(split_csv("").is_empty());
    }

    #[test]
    fn builds_postgres_copy_input() {
        assert_eq!(
            copy_in_sql(
                "public",
                "users",
                &["name".to_string(), "score".to_string()]
            )
            .unwrap(),
            "COPY \"public\".\"users\" (\"name\", \"score\") FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        );

        let payloads = [Some(vec![0_u8, 255_u8]), None];
        let payload_refs = payloads
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("score", DataType::Float64, true),
                Field::new("payload", DataType::Binary, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("Ada, \"A\""), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), None])) as ArrayRef,
                Arc::new(BinaryArray::from(payload_refs)) as ArrayRef,
            ],
        )
        .unwrap();

        let csv = String::from_utf8(batch_to_copy_csv(&batch).unwrap()).unwrap();
        assert_eq!(csv, "\"Ada, \"\"A\"\"\",\"1.5\",\"\\x00ff\"\n\\N,\\N,\\N\n");
    }

    #[test]
    fn builds_empty_arrow_schema_from_postgres_metadata() {
        let mut reader = postgres_rows_to_arrow(
            vec![
                ("id".to_string(), "INT8".to_string()),
                ("name".to_string(), "TEXT".to_string()),
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
    fn postgres_rich_types_map_to_arrow() {
        let Ok(url) = std::env::var("FSSPEC_DB_POSTGRES_URL") else {
            return;
        };
        let db = PostgresDatabase::connect(&url).unwrap();
        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS public.fsspec_db_rich")
                    .execute(&db.pool)
                    .await?;
                sqlx::query(
                    "CREATE TABLE public.fsspec_db_rich (
                        amount NUMERIC(10, 2),
                        created DATE,
                        ts TIMESTAMP,
                        tsz TIMESTAMPTZ,
                        uid UUID,
                        doc JSONB
                    )",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query(
                    "INSERT INTO public.fsspec_db_rich VALUES
                     (12.34, '2020-01-02', '2020-01-02 03:04:05',
                      '2020-01-02 03:04:05+00',
                      '00000000-0000-0000-0000-000000000001', '{\"a\": 1}'),
                     (NULL, NULL, NULL, NULL, NULL, NULL)",
                )
                .execute(&db.pool)
                .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();

        let mut reader = db
            .query(
                "SELECT amount, created, ts, tsz, uid, doc FROM public.fsspec_db_rich ORDER BY created NULLS LAST",
                &[],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8); // numeric -> text
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8); // uuid -> text
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8); // jsonb -> text
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn postgres_integration_round_trips_when_configured() {
        let Ok(url) = std::env::var("FSSPEC_DB_POSTGRES_URL") else {
            return;
        };
        let db = PostgresDatabase::connect(&url).unwrap();
        assert_eq!(db.arrow_extraction(), ArrowExtraction::SqlxRows);
        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS public.fsspec_db_users")
                    .execute(&db.pool)
                    .await?;
                sqlx::query(
                    "CREATE TABLE public.fsspec_db_users (
                        id BIGSERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        score DOUBLE PRECISION,
                        active BOOLEAN,
                        payload BYTEA
                    )",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query(
                    "CREATE INDEX idx_fsspec_db_users_name ON public.fsspec_db_users(name)",
                )
                .execute(&db.pool)
                .await?;
                sqlx::query(
                    "INSERT INTO public.fsspec_db_users (name, score, active, payload)
                     VALUES ('ada', 1.5, TRUE, '\\x01'::bytea), ('grace', NULL, FALSE, NULL)",
                )
                .execute(&db.pool)
                .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();

        let relations = db.list_relations("public").unwrap();
        assert!(relations
            .iter()
            .any(|relation| relation.name == "fsspec_db_users"));
        let columns = db.list_columns("public", "fsspec_db_users").unwrap();
        assert_eq!(columns[0].name, "id");
        assert!(columns[0].primary_key);
        let indexes = db.list_indexes("public", "fsspec_db_users").unwrap();
        assert!(indexes.iter().any(|index| index.columns == vec!["name"]));

        let mut reader = db
            .query(
                "SELECT id, name, score, active, payload
                 FROM public.fsspec_db_users
                 WHERE id > $1
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
            .query(
                "SELECT id, name FROM public.fsspec_db_users WHERE id < 0",
                &[],
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");

        db.runtime
            .block_on(async {
                sqlx::query("DROP TABLE IF EXISTS public.fsspec_db_users")
                    .execute(&db.pool)
                    .await?;
                Ok::<_, sqlx::Error>(())
            })
            .unwrap();
    }
}
