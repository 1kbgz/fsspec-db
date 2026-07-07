use std::fs;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use fsspec_rs::{
    buffered::Uploader, BufferedFile, FileInfo, FileSystem, FsError, FsFile, FsResult, OpenMode,
    OpenOptions,
};

use crate::codec::{csv_to_arrow, format_reader, ipc_to_arrow, jsonl_to_arrow, parquet_to_arrow};
use crate::database::{Database, DbValue, InsertMode};
use crate::file::DbFile;
use crate::path::{DataFormat, DbFacet, DbPath, DbPathKind};
use crate::sql::{select_sql, SelectOptions};
use crate::types::{
    ColumnInfo, ConstraintInfo, Dialect, IndexInfo, RelationInfo, RelationKind, SchemaInfo,
};
use crate::{DbError, Result};

const PROTOCOLS: &[&str] = &["db"];

pub struct DatabaseFs<D> {
    db: Arc<D>,
}

impl<D> DatabaseFs<D> {
    pub fn new(db: D) -> Self {
        Self { db: Arc::new(db) }
    }

    pub fn database(&self) -> &D {
        self.db.as_ref()
    }
}

impl<D> FileSystem for DatabaseFs<D>
where
    D: Database + 'static,
{
    fn protocol(&self) -> &[&str] {
        PROTOCOLS
    }

    fn root_marker(&self) -> &str {
        "/"
    }

    fn ls(&self, path: &str, _detail: bool) -> FsResult<Vec<FileInfo>> {
        self.ls_db(path).map_err(FsError::from)
    }

    fn rm_file(&self, path: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "database delete is not supported yet: {path}"
        )))
    }

    fn cp_file(&self, src: &str, dst: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "database copy is not supported yet: {src} -> {dst}"
        )))
    }

    fn open(
        &self,
        path: &str,
        mode: OpenMode,
        opts: Option<OpenOptions>,
    ) -> FsResult<Box<dyn FsFile>> {
        let opts = opts.unwrap_or_default();
        self.open_db(path, mode, &opts).map_err(FsError::from)
    }

    fn info(&self, path: &str) -> FsResult<FileInfo> {
        self.info_db(path).map_err(FsError::from)
    }

    fn mkdir(&self, path: &str, _create_parents: bool) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "database DDL is not supported yet: {path}"
        )))
    }

    fn rmdir(&self, path: &str) -> FsResult<()> {
        Err(FsError::NotSupported(format!(
            "database DDL is not supported yet: {path}"
        )))
    }

    fn put_file(&self, local: &str, remote: &str) -> FsResult<()> {
        let data = fs::read(local).map_err(FsError::from)?;
        self.write_bytes(remote, &data, InsertMode::Truncate)
            .map(|_| ())
            .map_err(FsError::from)
    }
}

impl<D> DatabaseFs<D>
where
    D: Database + 'static,
{
    pub fn write_bytes(&self, path: &str, data: &[u8], mode: InsertMode) -> Result<u64> {
        write_bytes_to_database(self.db.as_ref(), path, data, mode)
    }

    fn ls_db(&self, path: &str) -> Result<Vec<FileInfo>> {
        let parsed = DbPath::parse(path)?;
        match parsed.kind.clone() {
            DbPathKind::Root => Ok(self
                .db
                .list_schemas()?
                .into_iter()
                .map(schema_info)
                .collect()),
            DbPathKind::Schema => {
                let schema = required_schema(&parsed)?;
                self.ensure_schema(schema)?;
                Ok(self
                    .db
                    .list_relations(schema)?
                    .into_iter()
                    .map(|relation| relation_info(schema, relation))
                    .collect())
            }
            DbPathKind::Relation => {
                let (schema, relation) = required_relation(&parsed)?;
                let info = self.db.relation_info(schema, relation)?;
                let mut entries = vec![
                    facet_dir(schema, relation, DbFacet::Columns),
                    facet_dir(schema, relation, DbFacet::Indexes),
                    facet_dir(schema, relation, DbFacet::Constraints),
                ];
                if info.kind == RelationKind::View {
                    entries.push(facet_dir(schema, relation, DbFacet::DependsOn));
                    entries.push(view_definition_file(schema, relation, 0));
                }
                Ok(entries)
            }
            DbPathKind::Facet { facet, item: None } => {
                let (schema, relation) = required_relation(&parsed)?;
                self.facet_entries(schema, relation, facet)
            }
            DbPathKind::Facet { item: Some(_), .. }
            | DbPathKind::RelationData { .. }
            | DbPathKind::ViewDefinition => {
                Err(DbError::NotADirectory(format!("not a directory: {path}")))
            }
        }
    }

    fn info_db(&self, path: &str) -> Result<FileInfo> {
        let parsed = DbPath::parse(path)?;
        match parsed.kind.clone() {
            DbPathKind::Root => Ok(FileInfo::directory("/")),
            DbPathKind::Schema => {
                let schema = required_schema(&parsed)?;
                self.ensure_schema(schema).map(schema_info)
            }
            DbPathKind::Relation => {
                let (schema, relation) = required_relation(&parsed)?;
                self.db
                    .relation_info(schema, relation)
                    .map(|info| relation_info(schema, info))
            }
            DbPathKind::Facet { facet, item: None } => {
                let (schema, relation) = required_relation(&parsed)?;
                self.db.relation_info(schema, relation)?;
                Ok(facet_dir(schema, relation, facet))
            }
            DbPathKind::Facet {
                facet,
                item: Some(item),
            } => {
                let (schema, relation) = required_relation(&parsed)?;
                self.facet_item_info(schema, relation, facet, &item)
            }
            DbPathKind::RelationData { format } => {
                let (schema, relation) = required_relation(&parsed)?;
                let relation_info = self.db.relation_info(schema, relation)?;
                relation_data_file(self.db.dialect(), schema, relation, &relation_info, format)
            }
            DbPathKind::ViewDefinition => {
                let (schema, relation) = required_relation(&parsed)?;
                let definition = self.db.view_definition(schema, relation)?;
                Ok(view_definition_file(
                    schema,
                    relation,
                    definition.len() as u64,
                ))
            }
        }
    }

    fn open_db(&self, path: &str, mode: OpenMode, opts: &OpenOptions) -> Result<Box<dyn FsFile>> {
        if matches!(mode, OpenMode::Write | OpenMode::Append) {
            return self.open_write_db(path, mode, opts);
        }
        if mode == OpenMode::Exclusive {
            return Err(DbError::NotSupported(
                "exclusive create is not supported for database relation writes".to_string(),
            ));
        }

        let parsed = DbPath::parse(path)?;
        match parsed.kind.clone() {
            DbPathKind::RelationData { format } => {
                let (schema, relation) = required_relation(&parsed)?;
                let relation_info = self.db.relation_info(schema, relation)?;
                if format == DataFormat::Sql {
                    let data = ddl_sql(
                        self.db.dialect(),
                        schema,
                        relation,
                        &relation_info,
                        self.db.as_ref(),
                    )?
                    .into_bytes();
                    let mut info = relation_data_file(
                        self.db.dialect(),
                        schema,
                        relation,
                        &relation_info,
                        format,
                    )?;
                    info.size = data.len() as u64;
                    info.extra
                        .insert("size_known".to_string(), "true".to_string());
                    return Ok(Box::new(DbFile::readable(data, info)));
                }
                let options = select_options_from_query(&self.db.dialect(), &parsed.query)?;
                let sql = select_sql(&self.db.dialect(), schema, relation, &options)?;
                let reader = self.db.query(&sql, &[] as &[DbValue])?;
                let data = format_reader(reader, &format)?;
                let mut info = relation_data_file(
                    self.db.dialect(),
                    schema,
                    relation,
                    &relation_info,
                    format,
                )?;
                info.size = data.len() as u64;
                info.extra
                    .insert("size_known".to_string(), "true".to_string());
                Ok(Box::new(DbFile::readable(data, info)))
            }
            DbPathKind::ViewDefinition => {
                let (schema, relation) = required_relation(&parsed)?;
                let data = self.db.view_definition(schema, relation)?.into_bytes();
                let info = view_definition_file(schema, relation, data.len() as u64);
                Ok(Box::new(DbFile::readable(data, info)))
            }
            _ => Err(DbError::IsADirectory(format!(
                "path is not a readable database file: {path}"
            ))),
        }
    }

    fn open_write_db(
        &self,
        path: &str,
        mode: OpenMode,
        opts: &OpenOptions,
    ) -> Result<Box<dyn FsFile>> {
        if !opts.autocommit {
            return Err(DbError::NotSupported(
                "autocommit=False is not supported for database relation writes".to_string(),
            ));
        }

        let parsed = DbPath::parse(path)?;
        let DbPathKind::RelationData { format } = parsed.kind.clone() else {
            return Err(DbError::InvalidArgument(format!(
                "database writes require a relation data path: {path}"
            )));
        };
        if format == DataFormat::Sql {
            return Err(DbError::NotSupported(
                "database DDL writes are not supported yet".to_string(),
            ));
        }

        let (schema, relation) = required_relation(&parsed)?;
        let relation_info = self.db.relation_info(schema, relation)?;
        if relation_info.kind != RelationKind::Table {
            return Err(DbError::NotSupported(format!(
                "database writes require a table path: {path}"
            )));
        }

        let db = Arc::clone(&self.db);
        let write_path = path.to_string();
        let file_path = write_path.clone();
        let insert_mode = match mode {
            OpenMode::Write => InsertMode::Truncate,
            OpenMode::Append => InsertMode::Append,
            _ => unreachable!(),
        };
        let uploader: Uploader = Box::new(move |data| {
            write_bytes_to_database(db.as_ref(), &write_path, data, insert_mode.clone())
                .map(|_| ())
                .map_err(FsError::from)
        });
        Ok(Box::new(BufferedFile::new_write(
            file_path, uploader, false,
        )))
    }

    fn ensure_schema(&self, name: &str) -> Result<SchemaInfo> {
        self.db
            .list_schemas()?
            .into_iter()
            .find(|schema| schema.name == name)
            .ok_or_else(|| DbError::NotFound(format!("schema not found: {name}")))
    }

    fn facet_entries(&self, schema: &str, relation: &str, facet: DbFacet) -> Result<Vec<FileInfo>> {
        self.db.relation_info(schema, relation)?;
        match facet {
            DbFacet::Columns => self
                .db
                .list_columns(schema, relation)?
                .into_iter()
                .map(|column| column_info(schema, relation, column))
                .collect(),
            DbFacet::Indexes => self
                .db
                .list_indexes(schema, relation)?
                .into_iter()
                .map(|index| index_info(schema, relation, index))
                .collect(),
            DbFacet::Constraints => self
                .db
                .list_constraints(schema, relation)?
                .into_iter()
                .map(|constraint| constraint_info(schema, relation, constraint))
                .collect(),
            DbFacet::DependsOn => {
                let info = self.db.relation_info(schema, relation)?;
                if info.kind != RelationKind::View {
                    return Ok(Vec::new());
                }
                let definition = self.db.view_definition(schema, relation)?;
                Ok(
                    crate::sql::view_dependencies(&self.db.dialect(), &definition)
                        .iter()
                        .filter(|dependency| {
                            // Exclude a self-reference to the view itself.
                            let table = dependency
                                .rsplit_once('.')
                                .map(|(_, table)| table)
                                .unwrap_or(dependency);
                            table != relation
                        })
                        .map(|dependency| depends_on_entry(schema, relation, dependency))
                        .collect(),
                )
            }
        }
    }

    fn facet_item_info(
        &self,
        schema: &str,
        relation: &str,
        facet: DbFacet,
        item: &str,
    ) -> Result<FileInfo> {
        self.facet_entries(schema, relation, facet)?
            .into_iter()
            .find(|entry| entry.name.rsplit('/').next() == Some(item))
            .ok_or_else(|| DbError::NotFound(format!("database metadata item not found: {item}")))
    }
}

fn write_bytes_to_database<D: Database>(
    db: &D,
    path: &str,
    data: &[u8],
    mode: InsertMode,
) -> Result<u64> {
    let parsed = DbPath::parse(path)?;
    let DbPathKind::RelationData { format } = parsed.kind.clone() else {
        return Err(DbError::InvalidArgument(format!(
            "database writes require a relation data path: {path}"
        )));
    };
    if format == DataFormat::Sql {
        return Err(DbError::NotSupported(
            "database DDL writes are not supported yet".to_string(),
        ));
    }

    let (schema, relation) = required_relation(&parsed)?;
    let relation_info = db.relation_info(schema, relation)?;
    if relation_info.kind != RelationKind::Table {
        return Err(DbError::NotSupported(format!(
            "database writes require a table path: {path}"
        )));
    }

    let reader = match format {
        DataFormat::Parquet => parquet_to_arrow(data.to_vec())?,
        DataFormat::Arrow => ipc_to_arrow(data.to_vec())?,
        DataFormat::Csv => csv_to_arrow(data.to_vec(), arrow_schema(db, schema, relation)?)?,
        DataFormat::Jsonl => jsonl_to_arrow(data.to_vec(), arrow_schema(db, schema, relation)?)?,
        DataFormat::Sql => unreachable!(),
    };
    db.insert(schema, relation, reader, mode)
}

fn arrow_schema<D: Database>(db: &D, schema: &str, relation: &str) -> Result<SchemaRef> {
    let fields = db
        .list_columns(schema, relation)?
        .into_iter()
        .map(|column| {
            Field::new(
                column.name,
                arrow_type_for_database_type(&column.data_type),
                column.nullable,
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

fn required_schema(path: &DbPath) -> Result<&str> {
    path.schema
        .as_deref()
        .ok_or_else(|| DbError::InvalidArgument("schema path is missing schema".to_string()))
}

fn required_relation(path: &DbPath) -> Result<(&str, &str)> {
    let schema = required_schema(path)?;
    let relation = path
        .relation
        .as_deref()
        .ok_or_else(|| DbError::InvalidArgument("relation path is missing relation".to_string()))?;
    Ok((schema, relation))
}

fn schema_info(schema: SchemaInfo) -> FileInfo {
    let mut info = FileInfo::directory(DbPath::schema_path(&schema.name));
    info.extra.insert("kind".to_string(), "schema".to_string());
    if let Some(catalog) = schema.catalog {
        info.extra.insert("catalog".to_string(), catalog);
    }
    if let Some(comment) = schema.comment {
        info.extra.insert("comment".to_string(), comment);
    }
    info
}

fn relation_info(schema: &str, relation: RelationInfo) -> FileInfo {
    let mut info = FileInfo::directory(DbPath::relation_path(schema, &relation.name));
    info.extra
        .insert("kind".to_string(), relation.kind.as_str().to_string());
    if let Some(row_count) = relation.row_count {
        info.extra
            .insert("row_count".to_string(), row_count.to_string());
    }
    if let Some(size_bytes) = relation.size_bytes {
        info.extra
            .insert("size_bytes".to_string(), size_bytes.to_string());
    }
    if let Some(comment) = relation.comment {
        info.extra.insert("comment".to_string(), comment);
    }
    info
}

fn facet_dir(schema: &str, relation: &str, facet: DbFacet) -> FileInfo {
    let mut info = FileInfo::directory(DbPath::facet_path(schema, relation, facet.clone()));
    info.extra
        .insert("kind".to_string(), facet.as_str().to_string());
    info
}

fn column_info(schema: &str, relation: &str, column: ColumnInfo) -> Result<FileInfo> {
    let mut info = FileInfo::file(
        DbPath::facet_item_path(schema, relation, DbFacet::Columns, &column.name),
        0,
    );
    info.extra.insert("kind".to_string(), "column".to_string());
    info.extra.insert("data_type".to_string(), column.data_type);
    info.extra
        .insert("nullable".to_string(), column.nullable.to_string());
    info.extra
        .insert("ordinal".to_string(), column.ordinal.to_string());
    info.extra
        .insert("primary_key".to_string(), column.primary_key.to_string());
    if let Some(default) = column.default {
        info.extra.insert("default".to_string(), default);
    }
    if let Some(comment) = column.comment {
        info.extra.insert("comment".to_string(), comment);
    }
    Ok(info)
}

fn index_info(schema: &str, relation: &str, index: IndexInfo) -> Result<FileInfo> {
    let mut info = FileInfo::file(
        DbPath::facet_item_path(schema, relation, DbFacet::Indexes, &index.name),
        0,
    );
    info.extra.insert("kind".to_string(), "index".to_string());
    info.extra
        .insert("columns".to_string(), index.columns.join(","));
    info.extra
        .insert("unique".to_string(), index.unique.to_string());
    if let Some(method) = index.method {
        info.extra.insert("method".to_string(), method);
    }
    Ok(info)
}

fn constraint_info(schema: &str, relation: &str, constraint: ConstraintInfo) -> Result<FileInfo> {
    let mut info = FileInfo::file(
        DbPath::facet_item_path(schema, relation, DbFacet::Constraints, &constraint.name),
        0,
    );
    info.extra
        .insert("kind".to_string(), constraint.kind.as_str().to_string());
    info.extra
        .insert("columns".to_string(), constraint.columns.join(","));
    if let Some(references) = constraint.references {
        info.extra.insert("references".to_string(), references);
    }
    if let Some(expr) = constraint.expr {
        info.extra.insert("expr".to_string(), expr);
    }
    Ok(info)
}

fn relation_data_file(
    dialect: Dialect,
    schema: &str,
    relation: &str,
    relation_info: &RelationInfo,
    format: DataFormat,
) -> Result<FileInfo> {
    let mut info = FileInfo::file(
        DbPath::relation_data_path(schema, relation, format.clone()),
        0,
    );
    info.extra
        .insert("size_known".to_string(), "false".to_string());
    info.extra
        .insert("kind".to_string(), relation_info.kind.as_str().to_string());
    info.extra
        .insert("format".to_string(), format.extension().to_string());
    info.extra
        .insert("dialect".to_string(), dialect.as_str().to_string());
    Ok(info)
}

fn view_definition_file(schema: &str, relation: &str, size: u64) -> FileInfo {
    let mut info = FileInfo::file(DbPath::view_definition_path(schema, relation), size);
    info.extra
        .insert("kind".to_string(), "view_definition".to_string());
    info.extra.insert("format".to_string(), "sql".to_string());
    info
}

fn depends_on_entry(schema: &str, relation: &str, dependency: &str) -> FileInfo {
    let (target_schema, target_relation) = match dependency.rsplit_once('.') {
        Some((dep_schema, dep_relation)) => (dep_schema.to_string(), dep_relation.to_string()),
        None => (schema.to_string(), dependency.to_string()),
    };
    let mut info = FileInfo::file(
        DbPath::facet_item_path(schema, relation, DbFacet::DependsOn, &target_relation),
        0,
    );
    info.extra
        .insert("kind".to_string(), "depends_on".to_string());
    info.extra.insert(
        "target".to_string(),
        DbPath::relation_path(&target_schema, &target_relation),
    );
    info
}

fn select_options_from_query(
    dialect: &Dialect,
    query: &[(String, String)],
) -> Result<SelectOptions> {
    let mut options = SelectOptions::default();
    for (key, value) in query {
        match key.as_str() {
            "columns" => {
                options.columns = value
                    .split(',')
                    .filter_map(|column| {
                        let column = column.trim();
                        (!column.is_empty()).then(|| column.to_string())
                    })
                    .collect();
            }
            "limit" => {
                let limit = value.parse::<u64>().map_err(|_| {
                    DbError::InvalidArgument(format!("invalid limit query parameter: {value}"))
                })?;
                options.limit = Some(limit);
            }
            "where" => {
                options.where_clause = Some(crate::sql::validate_predicate(dialect, value)?);
            }
            other => {
                return Err(DbError::InvalidArgument(format!(
                    "unknown query parameter: {other}"
                )));
            }
        }
    }
    Ok(options)
}

fn arrow_type_for_database_type(data_type: &str) -> DataType {
    let normalized = data_type.to_ascii_lowercase();
    if normalized.contains("bool") {
        DataType::Boolean
    } else if normalized.contains("int") {
        DataType::Int64
    } else if normalized.contains("real")
        || normalized.contains("floa")
        || normalized.contains("doub")
        || normalized.contains("numeric")
        || normalized.contains("decimal")
    {
        DataType::Float64
    } else if normalized.contains("blob") || normalized.contains("binary") {
        DataType::Binary
    } else {
        DataType::Utf8
    }
}

fn ddl_sql<D: Database>(
    dialect: Dialect,
    schema: &str,
    relation: &str,
    relation_info: &RelationInfo,
    db: &D,
) -> Result<String> {
    if relation_info.kind == RelationKind::View {
        return db.view_definition(schema, relation);
    }
    let columns = db.list_columns(schema, relation)?;
    let mut lines = Vec::new();
    for column in columns {
        let mut line = format!(
            "  {} {}",
            crate::quote_identifier(&dialect, &column.name)?,
            column.data_type
        );
        if !column.nullable {
            line.push_str(" NOT NULL");
        }
        if let Some(default) = column.default {
            line.push_str(" DEFAULT ");
            line.push_str(&default);
        }
        lines.push(line);
    }
    Ok(format!(
        "CREATE TABLE {}.{} (\n{}\n);",
        crate::quote_identifier(&dialect, schema)?,
        crate::quote_identifier(&dialect, relation)?,
        lines.join(",\n")
    ))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use fsspec_rs::{FileSystem, FileType, OpenOptions};

    use super::*;
    use crate::codec::{arrow_to_ipc, rows_to_arrow};
    use crate::database::{InsertMode, RecordBatchStream};
    use crate::types::ConstraintKind;

    struct MockDatabase {
        inserts: Mutex<Vec<(String, String, InsertMode, usize)>>,
        queries: Mutex<Vec<String>>,
    }

    impl MockDatabase {
        fn new() -> Self {
            Self {
                inserts: Mutex::new(Vec::new()),
                queries: Mutex::new(Vec::new()),
            }
        }

        fn batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("ada"), Some("grace")])) as ArrayRef,
                ],
            )
            .unwrap()
        }
    }

    impl Database for MockDatabase {
        fn dialect(&self) -> Dialect {
            Dialect::Sqlite
        }

        fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
            Ok(vec![SchemaInfo {
                name: "main".to_string(),
                catalog: None,
                comment: None,
            }])
        }

        fn list_relations(&self, schema: &str) -> Result<Vec<RelationInfo>> {
            if schema != "main" {
                return Err(DbError::NotFound(schema.to_string()));
            }
            Ok(vec![
                RelationInfo {
                    name: "users".to_string(),
                    kind: RelationKind::Table,
                    row_count: Some(2),
                    size_bytes: None,
                    comment: Some("app users".to_string()),
                },
                RelationInfo {
                    name: "active_users".to_string(),
                    kind: RelationKind::View,
                    row_count: None,
                    size_bytes: None,
                    comment: None,
                },
            ])
        }

        fn list_columns(&self, schema: &str, relation: &str) -> Result<Vec<ColumnInfo>> {
            self.relation_info(schema, relation)?;
            Ok(vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default: None,
                    ordinal: 1,
                    primary_key: true,
                    comment: None,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default: None,
                    ordinal: 2,
                    primary_key: false,
                    comment: None,
                },
            ])
        }

        fn list_indexes(&self, schema: &str, relation: &str) -> Result<Vec<IndexInfo>> {
            self.relation_info(schema, relation)?;
            Ok(vec![IndexInfo {
                name: "idx_users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
                method: Some("btree".to_string()),
            }])
        }

        fn list_constraints(&self, schema: &str, relation: &str) -> Result<Vec<ConstraintInfo>> {
            self.relation_info(schema, relation)?;
            Ok(vec![ConstraintInfo {
                name: "pk_users".to_string(),
                kind: ConstraintKind::PrimaryKey,
                columns: vec!["id".to_string()],
                references: None,
                expr: None,
            }])
        }

        fn relation_info(&self, schema: &str, relation: &str) -> Result<RelationInfo> {
            self.list_relations(schema)?
                .into_iter()
                .find(|info| info.name == relation)
                .ok_or_else(|| DbError::NotFound(relation.to_string()))
        }

        fn view_definition(&self, schema: &str, view: &str) -> Result<String> {
            self.relation_info(schema, view)?;
            Ok("CREATE VIEW active_users AS SELECT * FROM users".to_string())
        }

        fn query(&self, sql: &str, _params: &[DbValue]) -> Result<RecordBatchStream> {
            self.queries.lock().unwrap().push(sql.to_string());
            rows_to_arrow(vec![Self::batch()])
        }

        fn insert(
            &self,
            schema: &str,
            relation: &str,
            batches: RecordBatchStream,
            mode: InsertMode,
        ) -> Result<u64> {
            let mut rows = 0usize;
            for batch in batches {
                rows += batch.map_err(DbError::from)?.num_rows();
            }
            self.inserts.lock().unwrap().push((
                schema.to_string(),
                relation.to_string(),
                mode,
                rows,
            ));
            Ok(rows as u64)
        }
    }

    #[test]
    fn lists_schemas_and_relations() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let root = fs.ls("/", true).unwrap();
        assert_eq!(root[0].name, "/main");
        assert_eq!(root[0].file_type, FileType::Directory);

        let relations = fs.ls("/main", true).unwrap();
        assert_eq!(relations.len(), 2);
        assert_eq!(relations[0].extra["kind"], "table");
        assert_eq!(relations[0].extra["row_count"], "2");
    }

    #[test]
    fn returns_column_info() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let info = fs.info("/main/users/columns/id").unwrap();
        assert_eq!(info.name, "/main/users/columns/id");
        assert_eq!(info.file_type, FileType::File);
        assert_eq!(info.extra["data_type"], "INTEGER");
        assert_eq!(info.extra["primary_key"], "true");
    }

    #[test]
    fn relation_directory_lists_facets_and_view_definition() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let table = fs.ls("/main/users", true).unwrap();
        assert_eq!(
            table
                .iter()
                .map(|info| info.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "/main/users/columns",
                "/main/users/indexes",
                "/main/users/constraints"
            ]
        );

        let view = fs.ls("/main/active_users", true).unwrap();
        assert!(view
            .iter()
            .any(|info| info.name == "/main/active_users/definition.sql"));
    }

    #[test]
    fn opens_arrow_data_file() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs.open("/main/users.arrow", OpenMode::Read, None).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        assert!(!data.is_empty());

        let queries = fs.database().queries.lock().unwrap();
        assert_eq!(queries[0], "SELECT * FROM \"main\".\"users\"");
    }

    #[test]
    fn opens_parquet_data_file() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs
            .open("/main/users.parquet", OpenMode::Read, None)
            .unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        assert!(data.starts_with(b"PAR1"));
    }

    #[test]
    fn applies_query_params_to_select() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs
            .open(
                "/main/users.arrow?columns=id,name&limit=1",
                OpenMode::Read,
                None,
            )
            .unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        let queries = fs.database().queries.lock().unwrap();
        assert_eq!(
            queries[0],
            "SELECT \"id\", \"name\" FROM \"main\".\"users\" LIMIT 1"
        );
    }

    #[test]
    fn applies_where_query_param() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs
            .open(
                "/main/users.arrow?where=id > 0 AND name IS NOT NULL",
                OpenMode::Read,
                None,
            )
            .unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let queries = fs.database().queries.lock().unwrap();
        assert_eq!(
            queries[0],
            "SELECT * FROM \"main\".\"users\" WHERE id > 0 AND name IS NOT NULL"
        );
    }

    #[test]
    fn applies_percent_encoded_where_query_param() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs
            .open("/main/users.arrow?where=id%20%3E%200", OpenMode::Read, None)
            .unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let queries = fs.database().queries.lock().unwrap();
        assert_eq!(queries[0], "SELECT * FROM \"main\".\"users\" WHERE id > 0");
    }

    #[test]
    fn rejects_injection_where_query_param() {
        let fs = DatabaseFs::new(MockDatabase::new());
        assert!(matches!(
            fs.open(
                "/main/users.arrow?where=1); DROP TABLE users;--",
                OpenMode::Read,
                None
            ),
            Err(FsError::InvalidArgument(_))
        ));
    }

    #[test]
    fn lists_view_depends_on() {
        let fs = DatabaseFs::new(MockDatabase::new());
        // The mock view definition is `... SELECT * FROM users`.
        let entries = fs.ls("/main/active_users", true).unwrap();
        assert!(entries
            .iter()
            .any(|info| info.name == "/main/active_users/depends_on"));

        let deps = fs.ls("/main/active_users/depends_on", true).unwrap();
        let users = deps
            .iter()
            .find(|info| info.name == "/main/active_users/depends_on/users")
            .expect("view should depend on users");
        assert_eq!(users.extra["kind"], "depends_on");
        assert_eq!(users.extra["target"], "/main/users");
    }

    #[test]
    fn opens_relation_ddl() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let mut file = fs.open("/main/users.sql", OpenMode::Read, None).unwrap();
        let mut ddl = String::new();
        file.read_to_string(&mut ddl).unwrap();
        assert!(ddl.starts_with("CREATE TABLE \"main\".\"users\""));
        assert!(ddl.contains("\"id\" INTEGER NOT NULL"));
    }

    #[test]
    fn reports_missing_and_non_directory_paths() {
        let fs = DatabaseFs::new(MockDatabase::new());
        assert!(matches!(
            fs.info("/missing/users"),
            Err(FsError::NotFound(_))
        ));
        assert!(matches!(
            fs.ls("/main/users.arrow", true),
            Err(FsError::NotADirectory(_))
        ));
    }

    #[test]
    fn rejects_unimplemented_mutating_primitives() {
        let fs = DatabaseFs::new(MockDatabase::new());
        assert!(matches!(
            fs.rm_file("/main/users.arrow"),
            Err(FsError::NotSupported(_))
        ));
        assert!(matches!(
            fs.open("/main/users.arrow", OpenMode::Exclusive, None),
            Err(FsError::NotSupported(_))
        ));
    }

    #[test]
    fn writes_relation_data_to_database_insert() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let data = arrow_to_ipc(rows_to_arrow(vec![MockDatabase::batch()]).unwrap()).unwrap();

        let rows = fs
            .write_bytes("/main/users.arrow", &data, InsertMode::Truncate)
            .unwrap();

        assert_eq!(rows, 2);
        let inserts = fs.database().inserts.lock().unwrap();
        assert_eq!(
            inserts[0],
            (
                "main".to_string(),
                "users".to_string(),
                InsertMode::Truncate,
                2
            )
        );
    }

    #[test]
    fn write_open_commits_relation_data_to_database_insert() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let data = arrow_to_ipc(rows_to_arrow(vec![MockDatabase::batch()]).unwrap()).unwrap();

        let mut file = fs.open("/main/users.arrow", OpenMode::Write, None).unwrap();
        file.write_all(&data).unwrap();
        assert!(fs.database().inserts.lock().unwrap().is_empty());
        file.commit().unwrap();

        let inserts = fs.database().inserts.lock().unwrap();
        assert_eq!(
            inserts[0],
            (
                "main".to_string(),
                "users".to_string(),
                InsertMode::Truncate,
                2
            )
        );
    }

    #[test]
    fn write_open_drop_does_not_commit_relation_data() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let data = arrow_to_ipc(rows_to_arrow(vec![MockDatabase::batch()]).unwrap()).unwrap();

        {
            let mut file = fs.open("/main/users.arrow", OpenMode::Write, None).unwrap();
            file.write_all(&data).unwrap();
        }

        assert!(fs.database().inserts.lock().unwrap().is_empty());
    }

    #[test]
    fn write_open_rejects_autocommit_false() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let opts = OpenOptions {
            autocommit: false,
            ..Default::default()
        };

        assert!(matches!(
            fs.open("/main/users.arrow", OpenMode::Write, Some(opts)),
            Err(FsError::NotSupported(_))
        ));
    }

    #[test]
    fn append_open_uses_append_insert_mode() {
        let fs = DatabaseFs::new(MockDatabase::new());
        let data = arrow_to_ipc(rows_to_arrow(vec![MockDatabase::batch()]).unwrap()).unwrap();

        let mut file = fs
            .open("/main/users.arrow", OpenMode::Append, None)
            .unwrap();
        file.write_all(&data).unwrap();
        file.commit().unwrap();

        let inserts = fs.database().inserts.lock().unwrap();
        assert_eq!(inserts[0].2, InsertMode::Append);
    }
}
