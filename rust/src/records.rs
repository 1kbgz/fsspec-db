use std::collections::BTreeSet;

use arrow::array::{Array, AsArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use fsspec_rs::FileInfo;
use serde_json::Value;

use crate::codec::{arrow_to_jsonl, rows_to_arrow};
use crate::database::{Database, DbValue};
use crate::fs::arrow_type_for_database_type;
use crate::path::DbPath;
use crate::sql::{placeholder, qualified_name, quote_identifier};
use crate::types::{ColumnInfo, ConstraintKind, Dialect};
use crate::{DbError, Result};

const JSON_SUFFIX: &str = ".json";

/// A table with a primary key, seen as `schema/table/records/{key...}`.
///
/// Path segments map to primary-key columns in order. The last key column takes the rest of
/// the path, so it may contain `/` and be listed by prefix. A record is the raw bytes of the
/// table's only non-key column when that column is binary, and otherwise the row as JSON at
/// `{key}.json`.
pub(crate) struct RecordTable {
    schema: String,
    relation: String,
    dialect: Dialect,
    keys: Vec<ColumnInfo>,
    columns: Vec<String>,
    value: Option<String>,
}

impl RecordTable {
    pub(crate) fn load<D: Database>(db: &D, schema: &str, relation: &str) -> Result<Self> {
        let primary_key = db
            .list_constraints(schema, relation)?
            .into_iter()
            .find(|constraint| constraint.kind == ConstraintKind::PrimaryKey)
            .ok_or_else(|| {
                DbError::NotFound(format!(
                    "{schema}.{relation} has no primary key, so it has no records"
                ))
            })?;
        let columns = db.list_columns(schema, relation)?;
        let keys = primary_key
            .columns
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .find(|column| &column.name == name)
                    .cloned()
                    .ok_or_else(|| DbError::Other(format!("primary key column not found: {name}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let others = columns
            .iter()
            .filter(|column| !primary_key.columns.contains(&column.name))
            .collect::<Vec<_>>();
        let value = match others.as_slice() {
            [column] if is_binary(&column.data_type) => Some(column.name.clone()),
            _ => None,
        };
        Ok(Self {
            schema: schema.to_string(),
            relation: relation.to_string(),
            dialect: db.dialect(),
            keys,
            columns: columns.into_iter().map(|column| column.name).collect(),
            value,
        })
    }

    pub(crate) fn list<D: Database>(&self, db: &D, segments: &[String]) -> Result<Vec<FileInfo>> {
        let last = self.keys.len() - 1;
        let depth = segments.len().min(last);
        let mut params = Vec::new();
        let mut conditions = Vec::new();
        for (column, value) in self.keys.iter().zip(&segments[..depth]) {
            let Some(condition) = self.equal(column, value, &mut params)? else {
                return Err(self.not_found(segments));
            };
            conditions.push(condition);
        }
        let column = &self.keys[depth];
        let prefix = if segments.len() > depth {
            format!("{}/", segments[depth..].join("/"))
        } else {
            String::new()
        };
        if !prefix.is_empty() {
            conditions.push(self.prefix(column, &prefix, &mut params)?);
        }

        let mut sql = format!(
            "SELECT {}{} FROM {}",
            if depth < last { "DISTINCT " } else { "" },
            quote_identifier(&self.dialect, &column.name)?,
            qualified_name(&self.dialect, &self.schema, &self.relation)?
        );
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let mut directories = BTreeSet::new();
        let mut entries = Vec::new();
        for value in query_strings(db, &sql, &params)? {
            let Some(rest) = value.strip_prefix(&prefix) else {
                continue;
            };
            match rest.split_once('/') {
                Some((child, _)) => {
                    directories.insert(child.to_string());
                }
                None if depth < last => {
                    directories.insert(rest.to_string());
                }
                None => {
                    let name = match self.value {
                        Some(_) => rest.to_string(),
                        None => format!("{rest}{JSON_SUFFIX}"),
                    };
                    entries.push(self.entry(segments, &name, false));
                }
            }
        }
        if entries.is_empty() && directories.is_empty() && !segments.is_empty() {
            return Err(self.not_found(segments));
        }
        entries.extend(
            directories
                .iter()
                .map(|name| self.entry(segments, name, true)),
        );
        Ok(entries)
    }

    pub(crate) fn info<D: Database>(&self, db: &D, segments: &[String]) -> Result<FileInfo> {
        let path = DbPath::records_path(&self.schema, &self.relation, segments);
        if !segments.is_empty() {
            match self.read(db, segments) {
                Ok(data) => return Ok(record_info(path, data.len() as u64)),
                Err(DbError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
            self.list(db, segments)?;
        }
        Ok(directory_info(path))
    }

    pub(crate) fn read<D: Database>(&self, db: &D, segments: &[String]) -> Result<Vec<u8>> {
        let mut params = Vec::new();
        let Some(conditions) = self.key_conditions(segments, &mut params)? else {
            return Err(self.not_found(segments));
        };
        let columns = match &self.value {
            Some(value) => quote_identifier(&self.dialect, value)?,
            None => "*".to_string(),
        };
        let sql = format!(
            "SELECT {columns} FROM {} WHERE {conditions}",
            qualified_name(&self.dialect, &self.schema, &self.relation)?
        );
        let batches = db
            .query(&sql, &params)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
            return Err(self.not_found(segments));
        };
        if self.value.is_some() {
            return binary_value(batch);
        }
        let mut data = arrow_to_jsonl(rows_to_arrow(vec![batch.slice(0, 1)])?)?;
        if data.last() == Some(&b'\n') {
            data.pop();
        }
        Ok(data)
    }

    pub(crate) fn write<D: Database>(
        &self,
        db: &D,
        segments: &[String],
        data: &[u8],
    ) -> Result<()> {
        let path = DbPath::records_path(&self.schema, &self.relation, segments);
        let invalid_key = || {
            DbError::InvalidArgument(format!(
                "record path must name every primary-key column{}: {path}",
                if self.value.is_some() {
                    ""
                } else {
                    " and end in .json"
                }
            ))
        };
        let key = self.record_key(segments).ok_or_else(invalid_key)?;
        let mut columns = Vec::new();
        let mut params = Vec::new();
        for (column, value) in self.keys.iter().zip(&key) {
            columns.push(column.name.clone());
            params.push(key_value(column, value).ok_or_else(invalid_key)?);
        }
        match &self.value {
            Some(value) => {
                columns.push(value.clone());
                params.push(DbValue::Binary(data.to_vec()));
            }
            None => {
                let Value::Object(fields) = serde_json::from_slice(data).map_err(|err| {
                    DbError::InvalidArgument(format!("record is not valid JSON: {err}"))
                })?
                else {
                    return Err(DbError::InvalidArgument(
                        "record must be a JSON object".to_string(),
                    ));
                };
                for (name, value) in fields {
                    if let Some(index) = self.keys.iter().position(|column| column.name == name) {
                        if !json_matches_key(&value, &key[index]) {
                            return Err(DbError::InvalidArgument(format!(
                                "record field {name} does not match its path: {path}"
                            )));
                        }
                        continue;
                    }
                    if !self.columns.contains(&name) {
                        return Err(DbError::InvalidArgument(format!(
                            "column not found in {}.{}: {name}",
                            self.schema, self.relation
                        )));
                    }
                    columns.push(name);
                    params.push(json_value(value));
                }
            }
        }
        db.execute(&self.upsert_sql(&columns)?, &params)?;
        Ok(())
    }

    /// Delete one record. Deleting a missing record or a key prefix does nothing.
    pub(crate) fn delete<D: Database>(&self, db: &D, segments: &[String]) -> Result<()> {
        let mut params = Vec::new();
        let Some(conditions) = self.key_conditions(segments, &mut params)? else {
            return Ok(());
        };
        let sql = format!(
            "DELETE FROM {} WHERE {conditions}",
            qualified_name(&self.dialect, &self.schema, &self.relation)?
        );
        db.execute(&sql, &params)?;
        Ok(())
    }

    /// Key values for a record path, or `None` when the path names a key prefix.
    fn record_key(&self, segments: &[String]) -> Option<Vec<String>> {
        let last = self.keys.len() - 1;
        if segments.len() <= last {
            return None;
        }
        let mut key = segments[..last].to_vec();
        let tail = segments[last..].join("/");
        key.push(match self.value {
            Some(_) => tail,
            None => tail.strip_suffix(JSON_SUFFIX)?.to_string(),
        });
        Some(key)
    }

    fn key_conditions(
        &self,
        segments: &[String],
        params: &mut Vec<DbValue>,
    ) -> Result<Option<String>> {
        let Some(key) = self.record_key(segments) else {
            return Ok(None);
        };
        let mut conditions = Vec::new();
        for (column, value) in self.keys.iter().zip(&key) {
            let Some(condition) = self.equal(column, value, params)? else {
                return Ok(None);
            };
            conditions.push(condition);
        }
        Ok(Some(conditions.join(" AND ")))
    }

    /// An equality condition, or `None` when `value` cannot be a value of `column`.
    fn equal(
        &self,
        column: &ColumnInfo,
        value: &str,
        params: &mut Vec<DbValue>,
    ) -> Result<Option<String>> {
        let Some(value) = key_value(column, value) else {
            return Ok(None);
        };
        params.push(value);
        Ok(Some(format!(
            "{} = {}",
            quote_identifier(&self.dialect, &column.name)?,
            placeholder(&self.dialect, params.len())
        )))
    }

    /// Match keys starting with `prefix` by byte order, independent of the column collation.
    fn prefix(
        &self,
        column: &ColumnInfo,
        prefix: &str,
        params: &mut Vec<DbValue>,
    ) -> Result<String> {
        // `prefix` ends in `/`, and `0` is the next byte, so this bounds every extension.
        let upper = format!("{}0", &prefix[..prefix.len() - 1]);
        let quoted = quote_identifier(&self.dialect, &column.name)?;
        let column = match self.dialect {
            Dialect::Sqlite => format!("{quoted} COLLATE BINARY"),
            Dialect::Postgres => format!("{quoted} COLLATE \"C\""),
            Dialect::MySql => format!("CAST({quoted} AS BINARY)"),
            Dialect::Generic => quoted,
        };
        params.push(DbValue::String(prefix.to_string()));
        let lower = placeholder(&self.dialect, params.len());
        params.push(DbValue::String(upper));
        let upper = placeholder(&self.dialect, params.len());
        Ok(format!("{column} >= {lower} AND {column} < {upper}"))
    }

    fn upsert_sql(&self, columns: &[String]) -> Result<String> {
        let quoted = columns
            .iter()
            .map(|column| quote_identifier(&self.dialect, column))
            .collect::<Result<Vec<_>>>()?;
        let placeholders = (1..=columns.len())
            .map(|index| placeholder(&self.dialect, index))
            .collect::<Vec<_>>();
        let (keys, values) = quoted.split_at(self.keys.len());
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            qualified_name(&self.dialect, &self.schema, &self.relation)?,
            quoted.join(", "),
            placeholders.join(", ")
        );
        Ok(match self.dialect {
            Dialect::MySql => {
                let updates = if values.is_empty() {
                    format!("{0} = {0}", keys[0])
                } else {
                    values
                        .iter()
                        .map(|column| format!("{column} = VALUES({column})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!("{insert} ON DUPLICATE KEY UPDATE {updates}")
            }
            _ if values.is_empty() => {
                format!("{insert} ON CONFLICT ({}) DO NOTHING", keys.join(", "))
            }
            _ => format!(
                "{insert} ON CONFLICT ({}) DO UPDATE SET {}",
                keys.join(", "),
                values
                    .iter()
                    .map(|column| format!("{column} = excluded.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        })
    }

    fn entry(&self, segments: &[String], name: &str, directory: bool) -> FileInfo {
        let mut key = segments.to_vec();
        key.push(name.to_string());
        let path = DbPath::records_path(&self.schema, &self.relation, &key);
        if directory {
            directory_info(path)
        } else {
            let mut info = record_info(path, 0);
            info.extra
                .insert("size_known".to_string(), "false".to_string());
            info
        }
    }

    fn not_found(&self, segments: &[String]) -> DbError {
        DbError::NotFound(format!(
            "record not found: {}",
            DbPath::records_path(&self.schema, &self.relation, segments)
        ))
    }
}

fn record_info(path: String, size: u64) -> FileInfo {
    let mut info = FileInfo::file(path, size);
    info.extra.insert("kind".to_string(), "record".to_string());
    info
}

fn directory_info(path: String) -> FileInfo {
    let mut info = FileInfo::directory(path);
    info.extra.insert("kind".to_string(), "records".to_string());
    info
}

fn is_binary(data_type: &str) -> bool {
    let data_type = data_type.to_ascii_lowercase();
    data_type.contains("blob") || data_type.contains("binary") || data_type == "bytea"
}

fn key_value(column: &ColumnInfo, value: &str) -> Option<DbValue> {
    match arrow_type_for_database_type(&column.data_type) {
        DataType::Int64 => value.parse().ok().map(DbValue::Int64),
        _ => Some(DbValue::String(value.to_string())),
    }
}

fn json_matches_key(value: &Value, key: &str) -> bool {
    match value {
        Value::String(value) => value == key,
        Value::Number(value) => value.to_string() == key,
        _ => false,
    }
}

fn json_value(value: Value) -> DbValue {
    match value {
        Value::Null => DbValue::Null,
        Value::Bool(value) => DbValue::Bool(value),
        Value::Number(value) => match value.as_i64() {
            Some(value) => DbValue::Int64(value),
            None => DbValue::Float64(value.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(value) => DbValue::String(value),
        value => DbValue::String(value.to_string()),
    }
}

fn query_strings<D: Database>(db: &D, sql: &str, params: &[DbValue]) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in db.query(sql, params)? {
        let column = cast(batch?.column(0), &DataType::Utf8)?;
        values.extend(
            column
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::to_string),
        );
    }
    Ok(values)
}

fn binary_value(batch: &RecordBatch) -> Result<Vec<u8>> {
    let column = cast(batch.column(0), &DataType::Binary)?;
    let column = column.as_binary::<i32>();
    Ok(if column.is_valid(0) {
        column.value(0).to_vec()
    } else {
        Vec::new()
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use fsspec_rs::{FileSystem, FileType, FsError, OpenMode};

    use super::*;
    use crate::{DatabaseFs, InsertMode, SqliteDatabase};

    fn sqlite(statements: &[&str]) -> DatabaseFs<SqliteDatabase> {
        let db = SqliteDatabase::connect(":memory:").unwrap();
        for statement in statements {
            db.execute(statement, &[]).unwrap();
        }
        DatabaseFs::new(db)
    }

    fn names(entries: Vec<FileInfo>) -> Vec<String> {
        entries.into_iter().map(|entry| entry.name).collect()
    }

    fn raw_store() -> DatabaseFs<SqliteDatabase> {
        sqlite(&["CREATE TABLE kv (ns TEXT, key TEXT, value BLOB NOT NULL, PRIMARY KEY (ns, key))"])
    }

    #[test]
    fn raw_records_round_trip_and_delete() {
        let fs = raw_store();
        fs.write_bytes("/main/kv/records/one/a", b"first", InsertMode::Truncate)
            .unwrap();
        fs.write_bytes("/main/kv/records/one/a", &[0, 255], InsertMode::Truncate)
            .unwrap();
        fs.write_bytes("/main/kv/records/two/b", b"", InsertMode::Truncate)
            .unwrap();

        assert_eq!(
            fs.cat_file("/main/kv/records/one/a", None, None).unwrap(),
            vec![0, 255]
        );
        assert_eq!(
            fs.cat_file("/main/kv/records/two/b", None, None).unwrap(),
            b""
        );
        assert_eq!(fs.info("/main/kv/records/one/a").unwrap().size, 2);
        assert_eq!(
            fs.info("/main/kv/records/one").unwrap().file_type,
            FileType::Directory
        );
        assert_eq!(
            names(fs.ls("/main/kv/records", true).unwrap()),
            ["/main/kv/records/one", "/main/kv/records/two"]
        );

        fs.rm_file("/main/kv/records/one/a").unwrap();
        fs.rm_file("/main/kv/records/one/a").unwrap();
        assert!(matches!(
            fs.cat_file("/main/kv/records/one/a", None, None),
            Err(FsError::NotFound(_))
        ));
        assert!(matches!(
            fs.ls("/main/kv/records/one", true),
            Err(FsError::NotFound(_))
        ));
        assert!(fs
            .ls("/main/kv", true)
            .unwrap()
            .iter()
            .any(|e| e.name == "/main/kv/records"));
    }

    #[test]
    fn last_key_column_lists_by_byte_prefix() {
        let fs = raw_store();
        for key in ["a", "a/b", "a/c/d", "a0", "a.b", "B"] {
            fs.write_bytes(
                &format!("/main/kv/records/ns/{key}"),
                key.as_bytes(),
                InsertMode::Truncate,
            )
            .unwrap();
        }

        let root = fs.ls("/main/kv/records/ns", true).unwrap();
        let mut entries = root
            .iter()
            .map(|entry| (entry.name.as_str(), entry.file_type.clone()))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        assert_eq!(
            entries,
            [
                ("/main/kv/records/ns/B", FileType::File),
                ("/main/kv/records/ns/a", FileType::File),
                ("/main/kv/records/ns/a", FileType::Directory),
                ("/main/kv/records/ns/a.b", FileType::File),
                ("/main/kv/records/ns/a0", FileType::File),
            ]
        );
        assert_eq!(
            names(fs.ls("/main/kv/records/ns/a", true).unwrap()),
            ["/main/kv/records/ns/a/b", "/main/kv/records/ns/a/c"]
        );
        assert_eq!(
            fs.cat_file("/main/kv/records/ns/a/c/d", None, None)
                .unwrap(),
            b"a/c/d"
        );
    }

    #[test]
    fn json_records_upsert_rows() {
        let fs = sqlite(&["CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)"]);
        fs.write_bytes(
            "/main/users/records/1.json",
            br#"{"name": "ada", "score": 1.5}"#,
            InsertMode::Truncate,
        )
        .unwrap();
        fs.write_bytes(
            "/main/users/records/1.json",
            br#"{"id": 1, "name": "grace"}"#,
            InsertMode::Truncate,
        )
        .unwrap();

        assert_eq!(
            fs.cat_file("/main/users/records/1.json", None, None)
                .unwrap(),
            br#"{"id":1,"name":"grace","score":1.5}"#
        );
        assert_eq!(
            names(fs.ls("/main/users/records", true).unwrap()),
            ["/main/users/records/1.json"]
        );
        assert!(matches!(
            fs.cat_file("/main/users/records/x.json", None, None),
            Err(FsError::NotFound(_))
        ));
        for (path, data) in [
            ("/main/users/records/1", &br#"{}"#[..]),
            ("/main/users/records/1.json", br#"{"id": 2}"#),
            ("/main/users/records/1.json", br#"{"missing": 2}"#),
            ("/main/users/records/1.json", br#"[1]"#),
        ] {
            assert!(matches!(
                fs.write_bytes(path, data, InsertMode::Truncate),
                Err(DbError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn write_open_commits_on_close_only() {
        let fs = raw_store();
        fs.write_bytes("/main/kv/records/ns/a", b"old", InsertMode::Truncate)
            .unwrap();
        let mut file = fs
            .open("/main/kv/records/ns/a", OpenMode::Write, None)
            .unwrap();
        file.write_all(b"new").unwrap();
        file.flush().unwrap();
        assert_eq!(
            fs.cat_file("/main/kv/records/ns/a", None, None).unwrap(),
            b"old"
        );
        file.commit().unwrap();
        assert_eq!(
            fs.cat_file("/main/kv/records/ns/a", None, None).unwrap(),
            b"new"
        );

        assert!(matches!(
            fs.open("/main/kv/records/ns/a", OpenMode::Append, None),
            Err(FsError::NotSupported(_))
        ));
    }

    #[test]
    fn composite_keys_follow_primary_key_order() {
        let fs =
            sqlite(&["CREATE TABLE pairs (a TEXT, b INTEGER, value BLOB, PRIMARY KEY (b, a))"]);
        fs.write_bytes("/main/pairs/records/7/x", b"v", InsertMode::Truncate)
            .unwrap();

        assert_eq!(
            names(fs.ls("/main/pairs/records", true).unwrap()),
            ["/main/pairs/records/7"]
        );
        assert!(matches!(
            fs.cat_file("/main/pairs/records/x/7", None, None),
            Err(FsError::NotFound(_))
        ));
    }

    #[test]
    fn tables_without_primary_keys_have_no_records() {
        let fs = sqlite(&["CREATE TABLE logs (line TEXT)"]);
        assert!(matches!(
            fs.ls("/main/logs/records", true),
            Err(FsError::NotFound(_))
        ));
        assert!(!fs
            .ls("/main/logs", true)
            .unwrap()
            .iter()
            .any(|e| e.name.ends_with("records")));
    }

    #[test]
    fn builds_dialect_upserts() {
        let table = |dialect, value: Option<&str>| RecordTable {
            schema: "app".to_string(),
            relation: "kv".to_string(),
            dialect,
            keys: vec![ColumnInfo {
                name: "key".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                ordinal: 1,
                primary_key: true,
                comment: None,
            }],
            columns: vec!["key".to_string(), "value".to_string()],
            value: value.map(str::to_string),
        };
        let columns = ["key".to_string(), "value".to_string()];

        assert_eq!(
            table(Dialect::Postgres, Some("value"))
                .upsert_sql(&columns)
                .unwrap(),
            "INSERT INTO \"app\".\"kv\" (\"key\", \"value\") VALUES ($1, $2) \
             ON CONFLICT (\"key\") DO UPDATE SET \"value\" = excluded.\"value\""
        );
        assert_eq!(
            table(Dialect::MySql, Some("value"))
                .upsert_sql(&columns)
                .unwrap(),
            "INSERT INTO `app`.`kv` (`key`, `value`) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)"
        );
        assert_eq!(
            table(Dialect::MySql, None)
                .upsert_sql(&columns[..1])
                .unwrap(),
            "INSERT INTO `app`.`kv` (`key`) VALUES (?) ON DUPLICATE KEY UPDATE `key` = `key`"
        );
        assert_eq!(
            table(Dialect::Sqlite, None)
                .upsert_sql(&columns[..1])
                .unwrap(),
            "INSERT INTO \"app\".\"kv\" (\"key\") VALUES (?) ON CONFLICT (\"key\") DO NOTHING"
        );

        let mut params = Vec::new();
        let column = &table(Dialect::Postgres, None).keys[0];
        assert_eq!(
            table(Dialect::Postgres, None)
                .prefix(column, "a/", &mut params)
                .unwrap(),
            "\"key\" COLLATE \"C\" >= $1 AND \"key\" COLLATE \"C\" < $2"
        );
        assert_eq!(
            params,
            [
                DbValue::String("a/".to_string()),
                DbValue::String("a0".to_string())
            ]
        );
    }
}
