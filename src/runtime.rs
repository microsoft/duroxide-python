// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::{Duration, Instant};

use duroxide::providers::TagFilter;
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
#[pyclass(name = "RuntimeOptions", get_all)]
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
    /// Worker tag filter for activity routing.
    /// Accepts: "default_only", "any", "none", or JSON like '{"tags":["gpu"]}' / '{"default_and":["gpu"]}'
    pub worker_tag_filter: Option<String>,
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
        worker_tag_filter=None,
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
        worker_tag_filter: Option<String>,
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
            worker_tag_filter,
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
    started: bool,
    shutdown_requested: bool,
    startup_error: Option<runtime::RuntimeStartError>,
    #[cfg(feature = "test-hooks")]
    pub(crate) test_hooks: runtime::test_hooks::LifecycleHooks,
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
            started: false,
            shutdown_requested: false,
            startup_error: None,
            #[cfg(feature = "test-hooks")]
            test_hooks: runtime::test_hooks::LifecycleHooks::default(),
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
            started: false,
            shutdown_requested: false,
            startup_error: None,
            #[cfg(feature = "test-hooks")]
            test_hooks: runtime::test_hooks::LifecycleHooks::default(),
        }
    }

    /// Set the generator driver functions (called once from Python before registering orchestrations).
    /// These three functions handle: creating generators, driving next steps, and disposing.
    fn set_generator_driver(
        &mut self,
        create_fn: Py<PyAny>,
        next_fn: Py<PyAny>,
        dispose_fn: Py<PyAny>,
    ) -> PyResult<()> {
        self.ensure_configurable()?;
        self.create_fn = Some(create_fn);
        self.next_fn = Some(next_fn);
        self.dispose_fn = Some(dispose_fn);
        Ok(())
    }

    /// Register a Python activity function.
    /// The function receives a payload string and returns a result string.
    fn register_activity(&mut self, name: String, callback: Py<PyAny>) -> PyResult<()> {
        self.ensure_configurable()?;
        self.activity_builders.push((name, callback));
        Ok(())
    }

    /// Register a Python orchestration (generator function).
    fn register_orchestration(&mut self, name: String) -> PyResult<()> {
        self.ensure_configurable()?;
        self.orchestration_names.push((name, None));
        Ok(())
    }

    /// Register a versioned Python orchestration.
    fn register_orchestration_versioned(&mut self, name: String, version: String) -> PyResult<()> {
        self.ensure_configurable()?;
        self.orchestration_names.push((name, Some(version)));
        Ok(())
    }

    /// Start the runtime. This processes orchestrations and activities until shutdown.
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        self.ensure_configurable()?;
        if self.create_fn.is_none() || self.next_fn.is_none() || self.dispose_fn.is_none() {
            return Err(PyRuntimeError::new_err(
                "lifecycle_start_failed: generator driver not set",
            ));
        }
        self.started = true;
        let prepared = catch_unwind(AssertUnwindSafe(|| {
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
            let create_fn = self
                .create_fn
                .take()
                .ok_or(runtime::RuntimeStartError::StartupFailed)?;
            let next_fn = self
                .next_fn
                .take()
                .ok_or(runtime::RuntimeStartError::StartupFailed)?;
            let dispose_fn = self
                .dispose_fn
                .take()
                .ok_or(runtime::RuntimeStartError::StartupFailed)?;

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
                if let Some(ref filter_str) = opts.worker_tag_filter {
                    rt_options.worker_tag_filter = parse_tag_filter(filter_str);
                }
            }

            // Release GIL before blocking — orchestration handlers need GIL access
            let provider = self.provider.clone();
            #[cfg(feature = "test-hooks")]
            let provider = Arc::new(runtime::test_hooks::TestProvider::new(
                provider,
                self.test_hooks.clone(),
            ));
            runtime::Runtime::prepare(provider, activities, orchestrations, rt_options)
        }));
        let rt = match prepared {
            Ok(result) => result,
            Err(payload) => {
                // Payload destruction must not reintroduce a panic across the language boundary.
                if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    std::mem::forget(secondary);
                }
                Err(runtime::RuntimeStartError::StartupFailed)
            }
        }
        .map_err(|error| {
            self.startup_error = Some(error);
            start_error(error)
        })?;
        self.inner = Some(rt.clone());
        #[cfg(feature = "test-hooks")]
        rt.set_lifecycle_hooks(self.test_hooks.clone())
            .map_err(|error| {
                self.startup_error = Some(error);
                start_error(error)
            })?;
        py.allow_threads(|| TOKIO_RT.block_on(rt.start_execution()))
            .map_err(|error| {
                self.startup_error = Some(error);
                start_error(error)
            })
    }

    /// Get a snapshot of runtime metrics.
    fn metrics_snapshot(&self) -> Option<PyMetricsSnapshot> {
        if self.shutdown_requested || self.startup_error.is_some() {
            return None;
        }
        self.inner
            .as_ref()?
            .metrics_snapshot()
            .map(|m| PyMetricsSnapshot {
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
        let (grace, grace_deadline, total_deadline) = shutdown_deadlines(timeout_ms)?;
        if let Some(rt) = &self.inner {
            py.allow_threads(|| {
                let _executor = TOKIO_RT.enter();
                rt.request_shutdown_until(grace_deadline, total_deadline)
            })
            .map_err(shutdown_error)?;
        }
        self.shutdown_requested = true;
        if let Some(rt) = &self.inner {
            py.allow_threads(|| TOKIO_RT.block_on(rt.clone().shutdown_with_grace(grace)))
                .map_err(shutdown_error)?;
        } else {
            self.activity_builders.clear();
            self.orchestration_names.clear();
            self.create_fn = None;
            self.next_fn = None;
            self.dispose_fn = None;
        }
        if let Some(error) = self.startup_error {
            return Err(start_error(error));
        }
        Ok(())
    }
}

impl PyRuntime {
    fn ensure_configurable(&self) -> PyResult<()> {
        if self.started || self.shutdown_requested {
            return Err(PyRuntimeError::new_err(
                "lifecycle_terminal: runtime has already started or shutdown was requested",
            ));
        }
        Ok(())
    }
}

fn start_error(error: runtime::RuntimeStartError) -> PyErr {
    PyRuntimeError::new_err(format!("lifecycle_start_failed: {error}"))
}

fn shutdown_error(error: runtime::RuntimeShutdownError) -> PyErr {
    match error {
        runtime::RuntimeShutdownError::TimedOut => {
            PyTimeoutError::new_err(format!("lifecycle_shutdown_timed_out: {error}"))
        }
        runtime::RuntimeShutdownError::InvalidTimeouts
        | runtime::RuntimeShutdownError::DeadlineOverflow => {
            PyValueError::new_err(format!("lifecycle_invalid_timeout: {error}"))
        }
        _ => PyRuntimeError::new_err(format!("lifecycle_shutdown_failed: {error}")),
    }
}

fn shutdown_deadlines(timeout_ms: Option<i64>) -> PyResult<(Duration, Instant, Instant)> {
    let invalid = || {
        PyValueError::new_err(
        "lifecycle_invalid_timeout: timeout_ms must be a nonnegative supported millisecond duration",
    )
    };
    let millis = u64::try_from(timeout_ms.unwrap_or(1000)).map_err(|_| invalid())?;
    let grace = Duration::from_millis(millis);
    let total = grace
        .checked_add(Duration::from_secs(5))
        .ok_or_else(invalid)?;
    if total > Duration::from_nanos(u64::MAX) {
        return Err(invalid());
    }
    let now = Instant::now();
    let grace_deadline = now.checked_add(grace).ok_or_else(invalid)?;
    let total_deadline = now.checked_add(total).ok_or_else(invalid)?;
    total_deadline
        .checked_add(Duration::from_millis(1))
        .ok_or_else(invalid)?;
    Ok((grace, grace_deadline, total_deadline))
}

/// Parse a tag filter string into a TagFilter.
///
/// Accepts:
/// - `"default_only"` → TagFilter::DefaultOnly
/// - `"any"` → TagFilter::Any
/// - `"none"` → TagFilter::None
/// - JSON `{"tags": ["gpu", "cpu"]}` → TagFilter::Tags
/// - JSON `{"default_and": ["gpu"]}` → TagFilter::DefaultAnd
fn parse_tag_filter(s: &str) -> TagFilter {
    match s {
        "default_only" => TagFilter::default_only(),
        "any" => TagFilter::any(),
        "none" => TagFilter::none(),
        other => {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(other) {
                if let Some(tags) = val.get("tags").and_then(|v| v.as_array()) {
                    let tag_list: Vec<String> = tags
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                    TagFilter::tags(tag_list)
                } else if let Some(tags) = val.get("default_and").and_then(|v| v.as_array()) {
                    let tag_list: Vec<String> = tags
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                    TagFilter::default_and(tag_list)
                } else {
                    TagFilter::default_only()
                }
            } else {
                TagFilter::default_only()
            }
        }
    }
}
