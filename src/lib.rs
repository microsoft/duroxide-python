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

#[pymodule]
fn _duroxide(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(activity_trace_log, m)?)?;
    m.add_function(wrap_pyfunction!(orchestration_trace_log, m)?)?;
    m.add_function(wrap_pyfunction!(activity_is_cancelled, m)?)?;
    m.add_function(wrap_pyfunction!(activity_get_client, m)?)?;
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
