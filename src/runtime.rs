use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime::{self, OrchestrationHandler, OrchestrationRegistry};

use crate::handlers::{PyActivityHandler, PyOrchestrationHandler};
use crate::pg_provider::PyPostgresProvider;
use crate::provider::PySqliteProvider;
use crate::types::PyMetricsSnapshot;

/// Global tokio runtime shared by all Python-facing blocking methods.
pub(crate) static TOKIO_RT: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")
    });

/// Runtime options configurable from Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyRuntimeOptions {
    /// Orchestration concurrency (default: 4)
    pub orchestration_concurrency: Option<i32>,
    /// Worker/activity concurrency (default: 8)
    pub worker_concurrency: Option<i32>,
    /// Dispatcher poll interval in ms (default: 100)
    pub dispatcher_poll_interval_ms: Option<i64>,
    /// Worker lock timeout in ms (default: 30000)
    pub worker_lock_timeout_ms: Option<i64>,
    /// Log format: "json", "pretty", or "compact" (default)
    pub log_format: Option<String>,
    /// Log level filter (e.g. "info", "debug")
    pub log_level: Option<String>,
    /// Service name for identification in logs/metrics
    pub service_name: Option<String>,
    /// Optional service version
    pub service_version: Option<String>,
    /// Maximum concurrent sessions per runtime (default: 10)
    pub max_sessions_per_runtime: Option<i32>,
    /// Session idle timeout in ms (default: 300000 = 5 minutes)
    pub session_idle_timeout_ms: Option<i64>,
    /// Stable worker identity for session ownership (e.g., K8s pod name)
    pub worker_node_id: Option<String>,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyRuntimeOptions {
    #[new]
    #[pyo3(signature = (
        orchestration_concurrency=None,
        worker_concurrency=None,
        dispatcher_poll_interval_ms=None,
        worker_lock_timeout_ms=None,
        log_format=None,
        log_level=None,
        service_name=None,
        service_version=None,
        max_sessions_per_runtime=None,
        session_idle_timeout_ms=None,
        worker_node_id=None,
    ))]
    fn new(
        orchestration_concurrency: Option<i32>,
        worker_concurrency: Option<i32>,
        dispatcher_poll_interval_ms: Option<i64>,
        worker_lock_timeout_ms: Option<i64>,
        log_format: Option<String>,
        log_level: Option<String>,
        service_name: Option<String>,
        service_version: Option<String>,
        max_sessions_per_runtime: Option<i32>,
        session_idle_timeout_ms: Option<i64>,
        worker_node_id: Option<String>,
    ) -> Self {
        Self {
            orchestration_concurrency,
            worker_concurrency,
            dispatcher_poll_interval_ms,
            worker_lock_timeout_ms,
            log_format,
            log_level,
            service_name,
            service_version,
            max_sessions_per_runtime,
            session_idle_timeout_ms,
            worker_node_id,
        }
    }
}

/// Builder for the duroxide runtime, wrapping registration and startup.
#[pyclass]
pub struct PyRuntime {
    provider: Arc<dyn duroxide::providers::Provider>,
    activity_builders: Vec<(String, Py<PyAny>)>,
    orchestration_names: Vec<(String, Option<String>)>,
    create_fn: Option<Py<PyAny>>,
    next_fn: Option<Py<PyAny>>,
    dispose_fn: Option<Py<PyAny>>,
    options: Option<PyRuntimeOptions>,
    inner: Option<Arc<runtime::Runtime>>,
}

#[pymethods]
impl PyRuntime {
    /// Create a runtime backed by SQLite.
    #[staticmethod]
    #[pyo3(signature = (provider, options=None))]
    fn from_sqlite(provider: &PySqliteProvider, options: Option<PyRuntimeOptions>) -> Self {
        Self {
            provider: provider.inner.clone(),
            activity_builders: Vec::new(),
            orchestration_names: Vec::new(),
            create_fn: None,
            next_fn: None,
            dispose_fn: None,
            options,
            inner: None,
        }
    }

    /// Create a runtime backed by PostgreSQL.
    #[staticmethod]
    #[pyo3(signature = (provider, options=None))]
    fn from_postgres(provider: &PyPostgresProvider, options: Option<PyRuntimeOptions>) -> Self {
        Self {
            provider: provider.inner.clone(),
            activity_builders: Vec::new(),
            orchestration_names: Vec::new(),
            create_fn: None,
            next_fn: None,
            dispose_fn: None,
            options,
            inner: None,
        }
    }

    /// Set the generator driver functions (called once from Python before registering orchestrations).
    /// These three functions handle: creating generators, driving next steps, and disposing.
    fn set_generator_driver(
        &mut self,
        create_fn: Py<PyAny>,
        next_fn: Py<PyAny>,
        dispose_fn: Py<PyAny>,
    ) {
        self.create_fn = Some(create_fn);
        self.next_fn = Some(next_fn);
        self.dispose_fn = Some(dispose_fn);
    }

    /// Register a Python activity function.
    /// The function receives a payload string and returns a result string.
    fn register_activity(&mut self, name: String, callback: Py<PyAny>) {
        self.activity_builders.push((name, callback));
    }

    /// Register a Python orchestration (generator function).
    fn register_orchestration(&mut self, name: String) {
        self.orchestration_names.push((name, None));
    }

    /// Register a versioned Python orchestration.
    fn register_orchestration_versioned(&mut self, name: String, version: String) {
        self.orchestration_names.push((name, Some(version)));
    }

    /// Start the runtime. This processes orchestrations and activities until shutdown.
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        // Build activity registry
        let mut activity_builder = duroxide::runtime::registry::ActivityRegistry::builder();
        for (name, callback) in self.activity_builders.drain(..) {
            let handler = Arc::new(PyActivityHandler::new(name.clone(), callback));
            activity_builder = activity_builder.register(&name, move |ctx, input| {
                let h = handler.clone();
                async move { h.invoke(ctx, input).await }
            });
        }
        let activities = activity_builder.build();

        // Build orchestration registry
        let create_fn = self.create_fn.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Generator driver not set. Call set_generator_driver() before start().",
            )
        })?;
        let next_fn = self.next_fn.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Generator driver not set.")
        })?;
        let dispose_fn = self.dispose_fn.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Generator driver not set.")
        })?;

        let mut orch_builder = OrchestrationRegistry::builder();
        for (name, version) in self.orchestration_names.drain(..) {
            let handler = Arc::new(PyOrchestrationHandler::new(
                create_fn.clone_ref(py),
                next_fn.clone_ref(py),
                dispose_fn.clone_ref(py),
            ));
            if let Some(ver) = version {
                orch_builder =
                    orch_builder.register_versioned(&name, &ver, move |ctx, input| {
                        let h = handler.clone();
                        async move { h.invoke(ctx, input).await }
                    });
            } else {
                orch_builder = orch_builder.register(&name, move |ctx, input| {
                    let h = handler.clone();
                    async move { h.invoke(ctx, input).await }
                });
            }
        }
        let orchestrations = orch_builder.build();

        // Build runtime options
        let mut rt_options = runtime::RuntimeOptions::default();
        if let Some(ref opts) = self.options {
            if let Some(c) = opts.orchestration_concurrency {
                rt_options.orchestration_concurrency = c as usize;
            }
            if let Some(c) = opts.worker_concurrency {
                rt_options.worker_concurrency = c as usize;
            }
            if let Some(ms) = opts.dispatcher_poll_interval_ms {
                rt_options.dispatcher_min_poll_interval = Duration::from_millis(ms as u64);
            }
            if let Some(ms) = opts.worker_lock_timeout_ms {
                rt_options.worker_lock_timeout = Duration::from_millis(ms as u64);
            }
            if let Some(ref fmt) = opts.log_format {
                rt_options.observability.log_format = match fmt.as_str() {
                    "json" => runtime::LogFormat::Json,
                    "pretty" => runtime::LogFormat::Pretty,
                    _ => runtime::LogFormat::Compact,
                };
            }
            if let Some(ref level) = opts.log_level {
                rt_options.observability.log_level = level.clone();
            }
            if let Some(ref name) = opts.service_name {
                rt_options.observability.service_name = name.clone();
            }
            if let Some(ref ver) = opts.service_version {
                rt_options.observability.service_version = Some(ver.clone());
            }
            if let Some(max) = opts.max_sessions_per_runtime {
                rt_options.max_sessions_per_runtime = max as usize;
            }
            if let Some(ms) = opts.session_idle_timeout_ms {
                rt_options.session_idle_timeout = Duration::from_millis(ms as u64);
            }
            if let Some(ref nid) = opts.worker_node_id {
                rt_options.worker_node_id = Some(nid.clone());
            }
        }

        // Release GIL before blocking — orchestration handlers need GIL access
        let provider = self.provider.clone();
        let rt = py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                runtime::Runtime::start_with_options(
                    provider,
                    activities,
                    orchestrations,
                    rt_options,
                )
                .await
            })
        });

        self.inner = Some(rt);
        Ok(())
    }

    /// Get a snapshot of runtime metrics.
    fn metrics_snapshot(&self) -> Option<PyMetricsSnapshot> {
        self.inner.as_ref()?.metrics_snapshot().map(|m| PyMetricsSnapshot {
            orch_starts: m.orch_starts,
            orch_completions: m.orch_completions,
            orch_failures: m.orch_failures,
            orch_application_errors: m.orch_application_errors,
            orch_infrastructure_errors: m.orch_infrastructure_errors,
            orch_configuration_errors: m.orch_configuration_errors,
            orch_poison: m.orch_poison,
            activity_success: m.activity_success,
            activity_app_errors: m.activity_app_errors,
            activity_infra_errors: m.activity_infra_errors,
            activity_config_errors: m.activity_config_errors,
            activity_poison: m.activity_poison,
            orch_dispatcher_items_fetched: m.orch_dispatcher_items_fetched,
            worker_dispatcher_items_fetched: m.worker_dispatcher_items_fetched,
            orch_continue_as_new: m.orch_continue_as_new,
            suborchestration_calls: m.suborchestration_calls,
            provider_errors: m.provider_errors,
        })
    }

    /// Shutdown the runtime gracefully.
    #[pyo3(signature = (timeout_ms=None))]
    fn shutdown(&mut self, py: Python<'_>, timeout_ms: Option<i64>) -> PyResult<()> {
        if let Some(rt) = self.inner.take() {
            let timeout = timeout_ms.map(|ms| ms as u64);
            py.allow_threads(|| {
                TOKIO_RT.block_on(async {
                    rt.shutdown(timeout).await;
                });
            });
        }
        Ok(())
    }
}
