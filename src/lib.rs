mod client;
mod handlers;
mod pg_provider;
mod provider;
mod runtime;
mod types;

use pyo3::prelude::*;

/// Emit an activity trace through the current Rust ActivityContext.
/// Delegates to ActivityContext.trace_info/warn/error/debug which includes
/// all structured fields (instance_id, activity_name, activity_id, worker_id, etc.)
#[pyfunction]
fn activity_trace_log(token: String, level: String, message: String) {
    handlers::activity_trace(&token, &level, &message);
}

/// Emit an orchestration trace through the Rust OrchestrationContext.
/// Delegates to OrchestrationContext.trace() which checks is_replaying
/// and includes all structured fields (instance_id, orchestration_name, etc.)
#[pyfunction]
fn orchestration_trace_log(instance_id: String, level: String, message: String) {
    handlers::orchestration_trace(&instance_id, &level, &message);
}

/// Check if an activity's cancellation token has been triggered.
/// Returns true if the activity has been cancelled (e.g., due to losing a race/select).
#[pyfunction]
fn activity_is_cancelled(token: String) -> bool {
    handlers::activity_is_cancelled(&token)
}

/// Get a Client from the stored ActivityContext (for use in activities).
#[pyfunction]
fn activity_get_client(token: String) -> Option<client::PyClient> {
    handlers::activity_get_client(&token)
}

/// Install a tracing subscriber that writes to a file.
///
/// Must be called **before** `runtime.start()`. Since duroxide uses
/// `try_init()` (first-writer-wins), the runtime's built-in subscriber
/// will silently no-op if one is already installed.
#[pyfunction]
#[pyo3(signature = (log_file, log_level=None, log_format=None))]
fn init_tracing(
    log_file: String,
    log_level: Option<String>,
    log_format: Option<String>,
) -> PyResult<()> {
    use std::fs::OpenOptions;
    use pyo3::exceptions::PyRuntimeError;
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let level = log_level.unwrap_or_else(|| "info".to_string());
    let filter_expr = format!("warn,duroxide::orchestration={level},duroxide::activity={level}");
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(filter_expr));

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open log file '{log_file}': {e}")))?;

    let format = log_format.unwrap_or_default();
    match format.as_str() {
        "json" => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json().with_writer(file))
                .try_init()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to init tracing: {e}")))?;
        }
        "pretty" => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().with_writer(file))
                .try_init()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to init tracing: {e}")))?;
        }
        _ => {
            // compact (default)
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().compact().with_writer(file))
                .try_init()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to init tracing: {e}")))?;
        }
    }

    Ok(())
}

#[pymodule]
fn _duroxide(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(activity_trace_log, m)?)?;
    m.add_function(wrap_pyfunction!(orchestration_trace_log, m)?)?;
    m.add_function(wrap_pyfunction!(activity_is_cancelled, m)?)?;
    m.add_function(wrap_pyfunction!(activity_get_client, m)?)?;
    m.add_function(wrap_pyfunction!(init_tracing, m)?)?;
    m.add_class::<provider::PySqliteProvider>()?;
    m.add_class::<pg_provider::PyPostgresProvider>()?;
    m.add_class::<client::PyClient>()?;
    m.add_class::<runtime::PyRuntime>()?;
    m.add_class::<runtime::PyRuntimeOptions>()?;
    m.add_class::<types::PyOrchestrationStatus>()?;
    m.add_class::<types::PySystemMetrics>()?;
    m.add_class::<types::PyQueueDepths>()?;
    m.add_class::<types::PyInstanceInfo>()?;
    m.add_class::<types::PyExecutionInfo>()?;
    m.add_class::<types::PyInstanceTree>()?;
    m.add_class::<types::PyDeleteInstanceResult>()?;
    m.add_class::<types::PyPruneOptions>()?;
    m.add_class::<types::PyPruneResult>()?;
    m.add_class::<types::PyInstanceFilter>()?;
    m.add_class::<types::PyEvent>()?;
    m.add_class::<types::PyMetricsSnapshot>()?;
    Ok(())
}
