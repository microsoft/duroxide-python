use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::TOKIO_RT;

/// Python-visible options for Entra ID (Azure AD) authentication.
///
/// All fields are optional; omitting a field uses the duroxide-pg default.
#[pyclass]
#[derive(Clone, Default)]
pub struct PyPostgresEntraOptions {
    pub audience: Option<String>,
    pub max_connections: Option<u32>,
    pub acquire_timeout_ms: Option<u64>,
    pub refresh_interval_ms: Option<u64>,
}

#[pymethods]
impl PyPostgresEntraOptions {
    #[new]
    #[pyo3(signature = (*, audience=None, max_connections=None, acquire_timeout_ms=None, refresh_interval_ms=None))]
    fn new(
        audience: Option<String>,
        max_connections: Option<u32>,
        acquire_timeout_ms: Option<u64>,
        refresh_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            audience,
            max_connections,
            acquire_timeout_ms,
            refresh_interval_ms,
        }
    }
}

impl PyPostgresEntraOptions {
    fn into_entra_auth_options(self) -> duroxide_pg::EntraAuthOptions {
        let mut opts = duroxide_pg::EntraAuthOptions::new();
        if let Some(aud) = self.audience {
            opts = opts.audience(aud);
        }
        if let Some(mc) = self.max_connections {
            opts = opts.max_connections(mc);
        }
        if let Some(ms) = self.acquire_timeout_ms {
            opts = opts.acquire_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = self.refresh_interval_ms {
            opts = opts.refresh_interval(Duration::from_millis(ms));
        }
        opts
    }
}

/// Wraps duroxide-pg's PostgresProvider for use from Python.
#[pyclass]
pub struct PyPostgresProvider {
    pub(crate) inner: Arc<dyn duroxide::providers::Provider>,
}

#[pymethods]
impl PyPostgresProvider {
    /// Connect to a PostgreSQL database.
    /// Uses the default "public" schema.
    #[staticmethod]
    fn connect(database_url: String) -> PyResult<Self> {
        let provider = TOKIO_RT.block_on(async {
            duroxide_pg::PostgresProvider::new(&database_url)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to connect to PostgreSQL: {e}"
                    ))
                })
        })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }

    /// Connect to a PostgreSQL database with a custom schema.
    /// The schema will be created if it does not exist.
    #[staticmethod]
    fn connect_with_schema(database_url: String, schema: String) -> PyResult<Self> {
        let provider = TOKIO_RT.block_on(async {
            duroxide_pg::PostgresProvider::new_with_schema(&database_url, Some(&schema))
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to connect to PostgreSQL: {e}"
                    ))
                })
        })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }

    /// Connect to Azure Database for PostgreSQL using Microsoft Entra ID
    /// (Azure AD) token authentication. The runtime fetches and refreshes
    /// the token automatically via the DefaultAzureCredential chain.
    ///
    /// `user` must be the Entra principal name mapped to a PostgreSQL role
    /// on the server. Pass `None` for `options` to use defaults.
    #[staticmethod]
    #[pyo3(signature = (host, port, database, user, options=None))]
    fn connect_with_entra(
        host: String,
        port: u16,
        database: String,
        user: String,
        options: Option<PyPostgresEntraOptions>,
    ) -> PyResult<Self> {
        let entra_opts = options.unwrap_or_default().into_entra_auth_options();
        let provider = TOKIO_RT
            .block_on(async {
                duroxide_pg::PostgresProvider::new_with_entra(
                    &host, port, &database, &user, entra_opts,
                )
                .await
            })
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to connect to PostgreSQL with Entra auth: {e}"
                ))
            })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }

    /// Same as `connect_with_entra` but uses a custom schema for tenant
    /// isolation. The schema will be created if it does not exist.
    #[staticmethod]
    #[pyo3(signature = (host, port, database, user, schema, options=None))]
    fn connect_with_schema_and_entra(
        host: String,
        port: u16,
        database: String,
        user: String,
        schema: String,
        options: Option<PyPostgresEntraOptions>,
    ) -> PyResult<Self> {
        let entra_opts = options.unwrap_or_default().into_entra_auth_options();
        let provider = TOKIO_RT
            .block_on(async {
                duroxide_pg::PostgresProvider::new_with_schema_and_entra(
                    &host,
                    port,
                    &database,
                    &user,
                    Some(&schema),
                    entra_opts,
                )
                .await
            })
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to connect to PostgreSQL with Entra auth: {e}"
                ))
            })?;
        Ok(Self {
            inner: Arc::new(provider),
        })
    }
}
