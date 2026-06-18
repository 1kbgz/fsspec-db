use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use fsspec_db::{
    ColumnInfo, ConstraintInfo, ConstraintKind, FileInfo, FileType, IndexInfo, RelationInfo,
    RelationKind, SchemaInfo,
};

#[pyclass(name = "SchemaInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PySchemaInfo {
    inner: SchemaInfo,
}

#[pymethods]
impl PySchemaInfo {
    #[new]
    #[pyo3(signature = (name, catalog = None, comment = None))]
    fn py_new(name: String, catalog: Option<String>, comment: Option<String>) -> Self {
        Self {
            inner: SchemaInfo {
                name,
                catalog,
                comment,
            },
        }
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        schema_to_dict(py, &self.inner)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn catalog(&self) -> Option<&str> {
        self.inner.catalog.as_deref()
    }

    #[getter]
    fn comment(&self) -> Option<&str> {
        self.inner.comment.as_deref()
    }
}

#[pyclass(name = "RelationInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyRelationInfo {
    inner: RelationInfo,
}

#[pymethods]
impl PyRelationInfo {
    #[new]
    #[pyo3(signature = (name, kind, row_count = None, size_bytes = None, comment = None))]
    fn py_new(
        name: String,
        kind: &str,
        row_count: Option<u64>,
        size_bytes: Option<u64>,
        comment: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: RelationInfo {
                name,
                kind: parse_relation_kind(kind)?,
                row_count,
                size_bytes,
                comment,
            },
        })
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        relation_to_dict(py, &self.inner)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn kind(&self) -> &str {
        self.inner.kind.as_str()
    }

    #[getter]
    fn row_count(&self) -> Option<u64> {
        self.inner.row_count
    }

    #[getter]
    fn size_bytes(&self) -> Option<u64> {
        self.inner.size_bytes
    }

    #[getter]
    fn comment(&self) -> Option<&str> {
        self.inner.comment.as_deref()
    }
}

#[pyclass(name = "ColumnInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyColumnInfo {
    inner: ColumnInfo,
}

#[pymethods]
impl PyColumnInfo {
    #[new]
    #[pyo3(signature = (name, data_type, nullable, default, ordinal, primary_key = false, comment = None))]
    fn py_new(
        name: String,
        data_type: String,
        nullable: bool,
        default: Option<String>,
        ordinal: u32,
        primary_key: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            inner: ColumnInfo {
                name,
                data_type,
                nullable,
                default,
                ordinal,
                primary_key,
                comment,
            },
        }
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        column_to_dict(py, &self.inner)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn data_type(&self) -> &str {
        &self.inner.data_type
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.inner.nullable
    }

    #[getter]
    fn default(&self) -> Option<&str> {
        self.inner.default.as_deref()
    }

    #[getter]
    fn ordinal(&self) -> u32 {
        self.inner.ordinal
    }

    #[getter]
    fn primary_key(&self) -> bool {
        self.inner.primary_key
    }

    #[getter]
    fn comment(&self) -> Option<&str> {
        self.inner.comment.as_deref()
    }
}

#[pyclass(name = "IndexInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyIndexInfo {
    inner: IndexInfo,
}

#[pymethods]
impl PyIndexInfo {
    #[new]
    #[pyo3(signature = (name, columns, unique, method = None))]
    fn py_new(name: String, columns: Vec<String>, unique: bool, method: Option<String>) -> Self {
        Self {
            inner: IndexInfo {
                name,
                columns,
                unique,
                method,
            },
        }
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        index_to_dict(py, &self.inner)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.inner.columns.clone()
    }

    #[getter]
    fn unique(&self) -> bool {
        self.inner.unique
    }

    #[getter]
    fn method(&self) -> Option<&str> {
        self.inner.method.as_deref()
    }
}

#[pyclass(name = "ConstraintInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyConstraintInfo {
    inner: ConstraintInfo,
}

#[pymethods]
impl PyConstraintInfo {
    #[new]
    #[pyo3(signature = (name, kind, columns, references = None, expr = None))]
    fn py_new(
        name: String,
        kind: &str,
        columns: Vec<String>,
        references: Option<String>,
        expr: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: ConstraintInfo {
                name,
                kind: parse_constraint_kind(kind)?,
                columns,
                references,
                expr,
            },
        })
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        constraint_to_dict(py, &self.inner)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn kind(&self) -> &str {
        self.inner.kind.as_str()
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.inner.columns.clone()
    }

    #[getter]
    fn references(&self) -> Option<&str> {
        self.inner.references.as_deref()
    }

    #[getter]
    fn expr(&self) -> Option<&str> {
        self.inner.expr.as_deref()
    }
}

pub fn file_info_to_dict<'py>(py: Python<'py>, info: &FileInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &info.name)?;
    dict.set_item("size", info.size)?;
    dict.set_item(
        "type",
        match info.file_type {
            FileType::File => "file",
            FileType::Directory => "directory",
            FileType::Other => "other",
        },
    )?;
    for (key, value) in &info.extra {
        if matches!(key.as_str(), "name" | "size" | "type") {
            continue;
        }
        set_extra_item(&dict, key, value)?;
    }
    Ok(dict)
}

fn schema_to_dict<'py>(py: Python<'py>, schema: &SchemaInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &schema.name)?;
    dict.set_item("catalog", &schema.catalog)?;
    dict.set_item("comment", &schema.comment)?;
    Ok(dict)
}

fn relation_to_dict<'py>(py: Python<'py>, relation: &RelationInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &relation.name)?;
    dict.set_item("kind", relation.kind.as_str())?;
    dict.set_item("row_count", relation.row_count)?;
    dict.set_item("size_bytes", relation.size_bytes)?;
    dict.set_item("comment", &relation.comment)?;
    Ok(dict)
}

fn column_to_dict<'py>(py: Python<'py>, column: &ColumnInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &column.name)?;
    dict.set_item("data_type", &column.data_type)?;
    dict.set_item("nullable", column.nullable)?;
    dict.set_item("default", &column.default)?;
    dict.set_item("ordinal", column.ordinal)?;
    dict.set_item("primary_key", column.primary_key)?;
    dict.set_item("comment", &column.comment)?;
    Ok(dict)
}

fn index_to_dict<'py>(py: Python<'py>, index: &IndexInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &index.name)?;
    dict.set_item("columns", &index.columns)?;
    dict.set_item("unique", index.unique)?;
    dict.set_item("method", &index.method)?;
    Ok(dict)
}

fn constraint_to_dict<'py>(
    py: Python<'py>,
    constraint: &ConstraintInfo,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &constraint.name)?;
    dict.set_item("kind", constraint.kind.as_str())?;
    dict.set_item("columns", &constraint.columns)?;
    dict.set_item("references", &constraint.references)?;
    dict.set_item("expr", &constraint.expr)?;
    Ok(dict)
}

fn parse_relation_kind(kind: &str) -> PyResult<RelationKind> {
    match kind {
        "table" => Ok(RelationKind::Table),
        "view" => Ok(RelationKind::View),
        other => Err(PyValueError::new_err(format!(
            "unknown relation kind: {other}"
        ))),
    }
}

fn parse_constraint_kind(kind: &str) -> PyResult<ConstraintKind> {
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

fn set_extra_item(dict: &Bound<'_, PyDict>, key: &str, value: &str) -> PyResult<()> {
    match key {
        "nullable" | "primary_key" | "size_known" | "unique" => match value {
            "true" => dict.set_item(key, true),
            "false" => dict.set_item(key, false),
            _ => dict.set_item(key, value),
        },
        "row_count" | "size_bytes" => match value.parse::<u64>() {
            Ok(parsed) => dict.set_item(key, parsed),
            Err(_) => dict.set_item(key, value),
        },
        "ordinal" => match value.parse::<u32>() {
            Ok(parsed) => dict.set_item(key, parsed),
            Err(_) => dict.set_item(key, value),
        },
        "columns" => dict.set_item(
            key,
            value
                .split(',')
                .filter(|column| !column.is_empty())
                .collect::<Vec<_>>(),
        ),
        _ => dict.set_item(key, value),
    }
}
