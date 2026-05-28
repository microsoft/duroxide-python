// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::prelude::*;
use std::sync::Arc;

use crate::runtime::TOKIO_RT;

/// Wraps duroxide's SqliteProvider for use from Python.
#[pyclass]
pub struct PySqliteProvider {
    pub(crate) inner: Arc<duroxide::providers::sqlite::SqliteProvider>,
}

#[pymethods]
impl PySqliteProvider {
    /// Open a SQLite database at the given file path.
    /// Path should be a sqlite: URL, e.g. "sqlite:./data.db" or "sqlite:/tmp/test.db".
    /// The file will be created if it does not exist.
    #[staticmethod]
    fn open(path: String) -> PyResult<Self> {
        // Ensure the file exists — sqlx won't create it by default
        let file_path = path.strip_prefix("sqlite:").unwrap_or(&path);
        if !file_path.contains(":memory:") {
            if let Some(parent) = std::path::Path::new(file_path).parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(file_path)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to create DB file: {e}"
                    ))
                })?;
        }
        let url = if path.starts_with("sqlite:") {
            path
        } else {
            format!("sqlite:{path}")
        };
        let provider = TOKIO_RT.block_on(async {
            duroxide::providers::sqlite::SqliteProvider::new(&url, None)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to open SQLite: {e}"
                    ))
                })
        })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }

    /// Create an in-memory SQLite database (useful for testing).
    #[staticmethod]
    fn in_memory() -> PyResult<Self> {
        let provider = TOKIO_RT.block_on(async {
            duroxide::providers::sqlite::SqliteProvider::new_in_memory()
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to create in-memory SQLite: {e}"
                    ))
                })
        })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }
}
