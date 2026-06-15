use std::sync::Arc;

use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIsADirectoryError, PyNotADirectoryError, PyOSError,
    PyPermissionError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};

use fsspec_db::{
    arrow_to_ipc, ipc_to_arrow, ColumnInfo, ConstraintInfo, ConstraintKind, Database, DatabaseFs,
    DbError, DbValue, Dialect, FileSystem, FsError, IndexInfo, InsertMode, RecordBatchStream,
    RelationInfo, RelationKind, SchemaInfo, SqliteDatabase,
};

use crate::types::file_info_to_dict;

#[derive(Clone)]
struct PyDatabase {
    obj: Arc<Py<PyAny>>,
}

impl PyDatabase {
    fn new(obj: Py<PyAny>) -> Self {
        Self { obj: Arc::new(obj) }
    }
}

impl Database for PyDatabase {
    fn dialect(&self) -> Dialect {
        Python::attach(|py| {
            self.obj
                .bind(py)
                .call_method0("dialect")
                .and_then(|value| value.extract::<String>())
                .map(|dialect| parse_dialect(&dialect))
                .unwrap_or(Dialect::Generic)
        })
    }

    fn list_schemas(&self) -> fsspec_db::Result<Vec<SchemaInfo>> {
        Python::attach(|py| {
            let value = map_py(py, self.obj.bind(py).call_method0("list_schemas"))?;
            map_py(py, collect_py_vec(&value, schema_from_py))
        })
    }

    fn list_relations(&self, schema: &str) -> fsspec_db::Result<Vec<RelationInfo>> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj.bind(py).call_method1("list_relations", (schema,)),
            )?;
            map_py(py, collect_py_vec(&value, relation_from_py))
        })
    }

    fn list_columns(&self, schema: &str, relation: &str) -> fsspec_db::Result<Vec<ColumnInfo>> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("list_columns", (schema, relation)),
            )?;
            map_py(py, collect_py_vec(&value, column_from_py))
        })
    }

    fn list_indexes(&self, schema: &str, relation: &str) -> fsspec_db::Result<Vec<IndexInfo>> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("list_indexes", (schema, relation)),
            )?;
            map_py(py, collect_py_vec(&value, index_from_py))
        })
    }

    fn list_constraints(
        &self,
        schema: &str,
        relation: &str,
    ) -> fsspec_db::Result<Vec<ConstraintInfo>> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("list_constraints", (schema, relation)),
            )?;
            map_py(py, collect_py_vec(&value, constraint_from_py))
        })
    }

    fn relation_info(&self, schema: &str, relation: &str) -> fsspec_db::Result<RelationInfo> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("relation_info", (schema, relation)),
            )?;
            map_py(py, relation_from_py(&value))
        })
    }

    fn view_definition(&self, schema: &str, view: &str) -> fsspec_db::Result<String> {
        Python::attach(|py| {
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("view_definition", (schema, view)),
            )?;
            map_py(py, value.extract::<String>())
        })
    }

    fn query(&self, sql: &str, params: &[DbValue]) -> fsspec_db::Result<RecordBatchStream> {
        Python::attach(|py| {
            let params = map_py(py, db_params_to_py(py, params))?;
            let table = map_py(py, self.obj.bind(py).call_method1("query", (sql, params)))?;
            let bytes = map_py(py, table_to_ipc(py, &table))?;
            ipc_to_arrow(bytes)
        })
    }

    fn insert(
        &self,
        schema: &str,
        relation: &str,
        batches: RecordBatchStream,
        mode: InsertMode,
    ) -> fsspec_db::Result<u64> {
        let bytes = arrow_to_ipc(batches)?;
        Python::attach(|py| {
            let table = map_py(py, ipc_to_table(py, &bytes))?;
            let mode = insert_mode_as_str(&mode);
            let value = map_py(
                py,
                self.obj
                    .bind(py)
                    .call_method1("insert", (schema, relation, table.bind(py), mode)),
            )?;
            map_py(py, value.extract::<u64>())
        })
    }
}

#[pyclass(name = "RustDatabaseFs", skip_from_py_object)]
pub struct PyDatabaseFs {
    inner: DatabaseFs<PyDatabase>,
}

#[pymethods]
impl PyDatabaseFs {
    #[new]
    fn py_new(database: Py<PyAny>) -> Self {
        Self {
            inner: DatabaseFs::new(PyDatabase::new(database)),
        }
    }

    fn protocol(&self) -> Vec<String> {
        self.inner
            .protocol()
            .iter()
            .map(|protocol| protocol.to_string())
            .collect()
    }

    #[pyo3(signature = (path, detail = true))]
    fn ls<'py>(&self, py: Python<'py>, path: &str, detail: bool) -> PyResult<Py<PyAny>> {
        let entries = self.inner.ls(path, detail).map_err(fs_error_to_pyerr)?;
        if detail {
            let list = PyList::empty(py);
            for info in entries {
                list.append(file_info_to_dict(py, &info)?)?;
            }
            Ok(list.into_any().unbind())
        } else {
            let names = entries.into_iter().map(|entry| entry.name);
            Ok(PyList::new(py, names)?.into_any().unbind())
        }
    }

    fn info<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let info = self.inner.info(path).map_err(fs_error_to_pyerr)?;
        file_info_to_dict(py, &info)
    }

    #[pyo3(signature = (path, start = None, end = None))]
    fn cat_file(&self, path: &str, start: Option<i64>, end: Option<i64>) -> PyResult<Vec<u8>> {
        self.inner
            .cat_file(path, start, end)
            .map_err(fs_error_to_pyerr)
    }

    #[pyo3(signature = (sql, params = None))]
    fn query_arrow(&self, sql: &str, params: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<u8>> {
        let values = match params {
            Some(params) if !params.is_none() => py_params_to_db_values(params)?,
            _ => Vec::new(),
        };
        let reader = self
            .inner
            .database()
            .query(sql, &values)
            .map_err(db_error_to_pyerr)?;
        arrow_to_ipc(reader).map_err(db_error_to_pyerr)
    }

    #[pyo3(signature = (path, data, mode = "wb"))]
    fn write_file(&self, path: &str, data: &Bound<'_, PyBytes>, mode: &str) -> PyResult<u64> {
        self.inner
            .write_bytes(path, data.as_bytes(), parse_insert_mode(mode)?)
            .map_err(db_error_to_pyerr)
    }
}

#[pyclass(name = "RustSqliteDatabaseFs", skip_from_py_object)]
pub struct PySqliteDatabaseFs {
    inner: DatabaseFs<SqliteDatabase>,
}

#[pymethods]
impl PySqliteDatabaseFs {
    #[new]
    fn py_new(source: &str) -> PyResult<Self> {
        Ok(Self {
            inner: DatabaseFs::new(SqliteDatabase::connect(source).map_err(db_error_to_pyerr)?),
        })
    }

    fn protocol(&self) -> Vec<String> {
        self.inner
            .protocol()
            .iter()
            .map(|protocol| protocol.to_string())
            .collect()
    }

    #[pyo3(signature = (path, detail = true))]
    fn ls<'py>(&self, py: Python<'py>, path: &str, detail: bool) -> PyResult<Py<PyAny>> {
        let entries = py
            .detach(|| self.inner.ls(path, detail))
            .map_err(fs_error_to_pyerr)?;
        if detail {
            let list = PyList::empty(py);
            for info in entries {
                list.append(file_info_to_dict(py, &info)?)?;
            }
            Ok(list.into_any().unbind())
        } else {
            let names = entries.into_iter().map(|entry| entry.name);
            Ok(PyList::new(py, names)?.into_any().unbind())
        }
    }

    fn info<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let info = py
            .detach(|| self.inner.info(path))
            .map_err(fs_error_to_pyerr)?;
        file_info_to_dict(py, &info)
    }

    #[pyo3(signature = (path, start = None, end = None))]
    fn cat_file(
        &self,
        py: Python<'_>,
        path: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<Vec<u8>> {
        py.detach(|| self.inner.cat_file(path, start, end))
            .map_err(fs_error_to_pyerr)
    }

    #[pyo3(signature = (sql, params = None))]
    fn query_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<u8>> {
        let values = match params {
            Some(params) if !params.is_none() => py_params_to_db_values(params)?,
            _ => Vec::new(),
        };
        let reader = py
            .detach(|| self.inner.database().query(sql, &values))
            .map_err(db_error_to_pyerr)?;
        arrow_to_ipc(reader).map_err(db_error_to_pyerr)
    }

    #[pyo3(signature = (path, data, mode = "wb"))]
    fn write_file(
        &self,
        py: Python<'_>,
        path: &str,
        data: &Bound<'_, PyBytes>,
        mode: &str,
    ) -> PyResult<u64> {
        let data = data.as_bytes().to_vec();
        let mode = parse_insert_mode(mode)?;
        py.detach(|| self.inner.write_bytes(path, &data, mode))
            .map_err(db_error_to_pyerr)
    }
}

fn parse_insert_mode(mode: &str) -> PyResult<InsertMode> {
    match mode {
        "wb" | "w" | "overwrite" | "truncate" => Ok(InsertMode::Truncate),
        "ab" | "a" | "append" => Ok(InsertMode::Append),
        "xb" | "x" | "create" => Err(PyValueError::new_err(
            "exclusive create is not supported for database relation writes",
        )),
        other => Err(PyValueError::new_err(format!(
            "unsupported database write mode: {other}"
        ))),
    }
}

fn insert_mode_as_str(mode: &InsertMode) -> &'static str {
    match mode {
        InsertMode::Append => "append",
        InsertMode::Truncate => "truncate",
    }
}

fn parse_dialect(dialect: &str) -> Dialect {
    match dialect {
        "sqlite" => Dialect::Sqlite,
        "postgres" | "postgresql" => Dialect::Postgres,
        "mysql" => Dialect::MySql,
        _ => Dialect::Generic,
    }
}

fn collect_py_vec<T>(
    obj: &Bound<'_, PyAny>,
    convert: fn(&Bound<'_, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let mut values = Vec::new();
    for item in obj.try_iter()? {
        values.push(convert(&item?)?);
    }
    Ok(values)
}

fn schema_from_py(obj: &Bound<'_, PyAny>) -> PyResult<SchemaInfo> {
    Ok(SchemaInfo {
        name: required_string(obj, "name")?,
        catalog: optional_string(obj, "catalog")?,
        comment: optional_string(obj, "comment")?,
    })
}

fn relation_from_py(obj: &Bound<'_, PyAny>) -> PyResult<RelationInfo> {
    Ok(RelationInfo {
        name: required_string(obj, "name")?,
        kind: relation_kind_from_str(&required_string(obj, "kind")?)?,
        row_count: optional_u64(obj, "row_count")?,
        size_bytes: optional_u64(obj, "size_bytes")?,
        comment: optional_string(obj, "comment")?,
    })
}

fn column_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ColumnInfo> {
    Ok(ColumnInfo {
        name: required_string(obj, "name")?,
        data_type: required_string(obj, "data_type")?,
        nullable: required_bool(obj, "nullable")?,
        default: optional_string(obj, "default")?,
        ordinal: required_u32(obj, "ordinal")?,
        primary_key: optional_bool(obj, "primary_key")?.unwrap_or(false),
        comment: optional_string(obj, "comment")?,
    })
}

fn index_from_py(obj: &Bound<'_, PyAny>) -> PyResult<IndexInfo> {
    Ok(IndexInfo {
        name: required_string(obj, "name")?,
        columns: required_string_list(obj, "columns")?,
        unique: required_bool(obj, "unique")?,
        method: optional_string(obj, "method")?,
    })
}

fn constraint_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ConstraintInfo> {
    Ok(ConstraintInfo {
        name: required_string(obj, "name")?,
        kind: constraint_kind_from_str(&required_string(obj, "kind")?)?,
        columns: required_string_list(obj, "columns")?,
        references: optional_string(obj, "references")?,
        expr: optional_string(obj, "expr")?,
    })
}

fn optional_field<'py>(obj: &Bound<'py, PyAny>, name: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        return dict.get_item(name);
    }
    if let Some(value) = obj.getattr_opt(name)? {
        return Ok(Some(value));
    }
    if let Some(to_dict) = obj.getattr_opt("to_dict")? {
        let dict_obj = to_dict.call0()?;
        if let Ok(dict) = dict_obj.cast::<PyDict>() {
            return dict.get_item(name);
        }
    }
    Ok(None)
}

fn required_field<'py>(obj: &Bound<'py, PyAny>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    optional_field(obj, name)?
        .ok_or_else(|| PyValueError::new_err(format!("missing field: {name}")))
}

fn required_string(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<String> {
    required_field(obj, name)?.extract::<String>()
}

fn optional_string(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<String>> {
    match optional_field(obj, name)? {
        Some(value) if value.is_none() => Ok(None),
        Some(value) => value.extract::<String>().map(Some),
        None => Ok(None),
    }
}

fn required_bool(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<bool> {
    required_field(obj, name)?.extract::<bool>()
}

fn optional_bool(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<bool>> {
    match optional_field(obj, name)? {
        Some(value) if value.is_none() => Ok(None),
        Some(value) => value.extract::<bool>().map(Some),
        None => Ok(None),
    }
}

fn required_u32(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<u32> {
    required_field(obj, name)?.extract::<u32>()
}

fn optional_u64(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<u64>> {
    match optional_field(obj, name)? {
        Some(value) if value.is_none() => Ok(None),
        Some(value) => value.extract::<u64>().map(Some),
        None => Ok(None),
    }
}

fn required_string_list(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<String>> {
    required_field(obj, name)?.extract::<Vec<String>>()
}

fn relation_kind_from_str(kind: &str) -> PyResult<RelationKind> {
    match kind {
        "table" => Ok(RelationKind::Table),
        "view" => Ok(RelationKind::View),
        other => Err(PyValueError::new_err(format!(
            "unknown relation kind: {other}"
        ))),
    }
}

fn constraint_kind_from_str(kind: &str) -> PyResult<ConstraintKind> {
    match kind {
        "pk" => Ok(ConstraintKind::PrimaryKey),
        "fk" => Ok(ConstraintKind::ForeignKey),
        "unique" => Ok(ConstraintKind::Unique),
        "check" => Ok(ConstraintKind::Check),
        other => Err(PyValueError::new_err(format!(
            "unknown constraint kind: {other}"
        ))),
    }
}

fn db_params_to_py<'py>(py: Python<'py>, params: &[DbValue]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for param in params {
        match param {
            DbValue::Null => list.append(py.None())?,
            DbValue::Bool(value) => list.append(*value)?,
            DbValue::Int64(value) => list.append(*value)?,
            DbValue::Float64(value) => list.append(*value)?,
            DbValue::String(value) => list.append(value)?,
            DbValue::Binary(value) => list.append(PyBytes::new(py, value))?,
        }
    }
    Ok(list)
}

fn py_params_to_db_values(params: &Bound<'_, PyAny>) -> PyResult<Vec<DbValue>> {
    let mut values = Vec::new();
    for item in params.try_iter()? {
        let item = item?;
        values.push(py_value_to_db_value(&item)?);
    }
    Ok(values)
}

fn py_value_to_db_value(value: &Bound<'_, PyAny>) -> PyResult<DbValue> {
    if value.is_none() {
        return Ok(DbValue::Null);
    }
    if value.cast::<PyBool>().is_ok() {
        return value.extract::<bool>().map(DbValue::Bool);
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(DbValue::Int64(value));
    }
    if let Ok(value) = value.extract::<f64>() {
        return Ok(DbValue::Float64(value));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(DbValue::String(value));
    }
    if value.cast::<PyBytes>().is_ok() {
        return value.extract::<Vec<u8>>().map(DbValue::Binary);
    }
    Err(PyValueError::new_err("unsupported query parameter type"))
}

fn table_to_ipc(py: Python<'_>, table: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let pyarrow = py.import("pyarrow")?;
    let ipc = py.import("pyarrow.ipc")?;
    let sink = pyarrow.getattr("BufferOutputStream")?.call0()?;
    let schema = table.getattr("schema")?;
    let writer = ipc.getattr("new_stream")?.call1((&sink, &schema))?;
    writer.call_method1("write_table", (table,))?;
    writer.call_method0("close")?;
    let buffer = sink.call_method0("getvalue")?;
    buffer.call_method0("to_pybytes")?.extract::<Vec<u8>>()
}

fn ipc_to_table<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Py<PyAny>> {
    let ipc = py.import("pyarrow.ipc")?;
    let reader = ipc
        .getattr("open_stream")?
        .call1((PyBytes::new(py, data),))?;
    Ok(reader.call_method0("read_all")?.unbind())
}

fn map_py<T>(py: Python<'_>, result: PyResult<T>) -> fsspec_db::Result<T> {
    result.map_err(|err| pyerr_to_db_error(py, err))
}

fn pyerr_to_db_error(py: Python<'_>, err: PyErr) -> DbError {
    let message = err.to_string();
    if err.is_instance_of::<PyFileNotFoundError>(py) {
        DbError::NotFound(message)
    } else if err.is_instance_of::<PyPermissionError>(py) {
        DbError::PermissionDenied(message)
    } else if err.is_instance_of::<PyFileExistsError>(py) {
        DbError::AlreadyExists(message)
    } else if err.is_instance_of::<PyNotADirectoryError>(py) {
        DbError::NotADirectory(message)
    } else if err.is_instance_of::<PyIsADirectoryError>(py) {
        DbError::IsADirectory(message)
    } else if err.is_instance_of::<PyValueError>(py) {
        DbError::InvalidArgument(message)
    } else {
        DbError::Other(message)
    }
}

fn db_error_to_pyerr(err: DbError) -> PyErr {
    match err {
        DbError::NotFound(msg) => PyFileNotFoundError::new_err(msg),
        DbError::PermissionDenied(msg) => PyPermissionError::new_err(msg),
        DbError::AlreadyExists(msg) => PyFileExistsError::new_err(msg),
        DbError::NotADirectory(msg) => PyNotADirectoryError::new_err(msg),
        DbError::IsADirectory(msg) => PyIsADirectoryError::new_err(msg),
        DbError::InvalidArgument(msg) => PyValueError::new_err(msg),
        DbError::NotSupported(msg) => PyOSError::new_err(format!("not supported: {msg}")),
        DbError::Io(err) => PyOSError::new_err(err.to_string()),
        DbError::Arrow(err) => PyOSError::new_err(err.to_string()),
        DbError::Parquet(err) => PyOSError::new_err(err.to_string()),
        DbError::Other(msg) => PyOSError::new_err(msg),
    }
}

fn fs_error_to_pyerr(err: FsError) -> PyErr {
    match err {
        FsError::NotFound(msg) => PyFileNotFoundError::new_err(msg),
        FsError::PermissionDenied(msg) => PyPermissionError::new_err(msg),
        FsError::AlreadyExists(msg) => PyFileExistsError::new_err(msg),
        FsError::NotADirectory(msg) => PyNotADirectoryError::new_err(msg),
        FsError::IsADirectory(msg) => PyIsADirectoryError::new_err(msg),
        FsError::IoError(err) => PyOSError::new_err(err.to_string()),
        FsError::InvalidArgument(msg) => PyValueError::new_err(msg),
        FsError::NotSupported(msg) => PyOSError::new_err(format!("not supported: {msg}")),
        FsError::Other(msg) => PyOSError::new_err(msg),
    }
}
