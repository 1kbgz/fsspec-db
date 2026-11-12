use pyo3::prelude::*;

mod database;
mod registry;
mod types;

#[pymodule]
fn fsspec_db(m: &Bound<PyModule>) -> PyResult<()> {
    registry::register()?;
    m.add_function(wrap_pyfunction!(registry::linked_to_libfsspec, m)?)?;
    m.add_class::<database::PyDatabaseFs>()?;
    m.add_class::<database::PyDbFile>()?;
    m.add_class::<database::PySqliteDatabaseFs>()?;
    m.add_class::<database::PyPostgresDatabaseFs>()?;
    m.add_class::<database::PyMySqlDatabaseFs>()?;
    m.add_class::<types::PySchemaInfo>()?;
    m.add_class::<types::PyRelationInfo>()?;
    m.add_class::<types::PyColumnInfo>()?;
    m.add_class::<types::PyIndexInfo>()?;
    m.add_class::<types::PyConstraintInfo>()?;
    Ok(())
}
