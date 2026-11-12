//! Puts this extension's protocols in the registry every extension in the process shares.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Whether the protocols went into the shared `libfsspec` rather than a registry
/// compiled into this extension.
pub const LINKED_TO_LIBFSSPEC: bool = cfg!(feature = "capi");

pub fn register() -> PyResult<()> {
    #[cfg(feature = "capi")]
    fsspec_sys::use_shared_registry();
    ::fsspec_db::factories()
        .into_iter()
        .try_for_each(|(protocol, factory)| fsspec_rs::try_register_filesystem(protocol, factory))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Whether this extension registered its protocols in the shared `libfsspec`.
#[pyfunction]
pub fn linked_to_libfsspec() -> bool {
    LINKED_TO_LIBFSSPEC
}
