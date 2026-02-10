use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use duroxide::client::Client;
use duroxide::OrchestrationStatus;

use crate::pg_provider::PyPostgresProvider;
use crate::provider::PySqliteProvider;
use crate::runtime::TOKIO_RT;
use crate::types::{
    PyDeleteInstanceResult, PyEvent, PyExecutionInfo, PyInstanceFilter, PyInstanceInfo,
    PyInstanceTree, PyOrchestrationStatus, PyPruneOptions, PyPruneResult, PyQueueDepths,
    PySystemMetrics,
};

/// Wraps duroxide's Client for use from Python.
#[pyclass]
pub struct PyClient {
    inner: Arc<Client>,
}

impl PyClient {
    pub(crate) fn from_client(client: Client) -> Self {
        Self {
            inner: Arc::new(client),
        }
    }
}

#[pymethods]
impl PyClient {
    /// Create a client backed by SQLite.
    #[staticmethod]
    fn from_sqlite(provider: &PySqliteProvider) -> Self {
        let p: Arc<dyn duroxide::providers::Provider> = provider.inner.clone();
        Self {
            inner: Arc::new(Client::new(p)),
        }
    }

    /// Create a client backed by PostgreSQL.
    #[staticmethod]
    fn from_postgres(provider: &PyPostgresProvider) -> Self {
        Self {
            inner: Arc::new(Client::new(provider.inner.clone())),
        }
    }

    /// Start a new orchestration instance.
    fn start_orchestration(
        &self,
        py: Python<'_>,
        instance_id: String,
        orchestration_name: String,
        input: String,
    ) -> PyResult<()> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .start_orchestration(&instance_id, &orchestration_name, input)
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Start a new orchestration instance with a specific version.
    fn start_orchestration_versioned(
        &self,
        py: Python<'_>,
        instance_id: String,
        orchestration_name: String,
        input: String,
        version: String,
    ) -> PyResult<()> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .start_orchestration_versioned(
                        &instance_id,
                        &orchestration_name,
                        &version,
                        input,
                    )
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Get the current status of an orchestration instance.
    fn get_status(&self, py: Python<'_>, instance_id: String) -> PyResult<PyOrchestrationStatus> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let status = client
                    .get_orchestration_status(&instance_id)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(convert_status(status))
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Wait for an orchestration to complete (with timeout in milliseconds).
    fn wait_for_orchestration(
        &self,
        py: Python<'_>,
        instance_id: String,
        timeout_ms: i64,
    ) -> PyResult<PyOrchestrationStatus> {
        let client = self.inner.clone();
        let timeout = Duration::from_millis(timeout_ms as u64);
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let status = client
                    .wait_for_orchestration(&instance_id, timeout)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(convert_status(status))
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Cancel a running orchestration instance.
    #[pyo3(signature = (instance_id, reason=None))]
    fn cancel_instance(
        &self,
        py: Python<'_>,
        instance_id: String,
        reason: Option<String>,
    ) -> PyResult<()> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .cancel_instance(&instance_id, reason.unwrap_or_default())
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Raise an external event to an orchestration instance.
    fn raise_event(
        &self,
        py: Python<'_>,
        instance_id: String,
        event_name: String,
        data: String,
    ) -> PyResult<()> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .raise_event(&instance_id, &event_name, data)
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Get system metrics (if provider supports management).
    fn get_system_metrics(&self, py: Python<'_>) -> PyResult<PySystemMetrics> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let metrics = client
                    .get_system_metrics()
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PySystemMetrics {
                    total_instances: metrics.total_instances as i64,
                    total_executions: metrics.total_executions as i64,
                    running_instances: metrics.running_instances as i64,
                    completed_instances: metrics.completed_instances as i64,
                    failed_instances: metrics.failed_instances as i64,
                    total_events: metrics.total_events as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Get queue depths (if provider supports management).
    fn get_queue_depths(&self, py: Python<'_>) -> PyResult<PyQueueDepths> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let depths = client
                    .get_queue_depths()
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyQueueDepths {
                    orchestrator_queue: depths.orchestrator_queue as i64,
                    worker_queue: depths.worker_queue as i64,
                    timer_queue: depths.timer_queue as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// List all orchestration instance IDs.
    fn list_all_instances(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .list_all_instances()
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// List orchestration instance IDs by status.
    fn list_instances_by_status(
        &self,
        py: Python<'_>,
        status: String,
    ) -> PyResult<Vec<String>> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                client
                    .list_instances_by_status(&status)
                    .await
                    .map_err(|e| format!("{e}"))
            })
        })
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Get detailed info about a specific instance.
    fn get_instance_info(
        &self,
        py: Python<'_>,
        instance_id: String,
    ) -> PyResult<PyInstanceInfo> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let info = client
                    .get_instance_info(&instance_id)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyInstanceInfo {
                    instance_id: info.instance_id,
                    orchestration_name: info.orchestration_name,
                    orchestration_version: info.orchestration_version,
                    current_execution_id: info.current_execution_id as i64,
                    status: info.status,
                    output: info.output,
                    created_at: info.created_at as i64,
                    updated_at: info.updated_at as i64,
                    parent_instance_id: info.parent_instance_id,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Get detailed info about a specific execution within an instance.
    fn get_execution_info(
        &self,
        py: Python<'_>,
        instance_id: String,
        execution_id: i64,
    ) -> PyResult<PyExecutionInfo> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let info = client
                    .get_execution_info(&instance_id, execution_id as u64)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyExecutionInfo {
                    execution_id: info.execution_id as i64,
                    status: info.status,
                    output: info.output,
                    started_at: info.started_at as i64,
                    completed_at: info.completed_at.map(|v| v as i64),
                    event_count: info.event_count as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// List execution IDs for an instance.
    fn list_executions(&self, py: Python<'_>, instance_id: String) -> PyResult<Vec<i64>> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let ids = client
                    .list_executions(&instance_id)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(ids.into_iter().map(|id| id as i64).collect())
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Read the event history for a specific execution.
    fn read_execution_history(
        &self,
        py: Python<'_>,
        instance_id: String,
        execution_id: i64,
    ) -> PyResult<Vec<PyEvent>> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let events = client
                    .read_execution_history(&instance_id, execution_id as u64)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(events
                    .into_iter()
                    .map(|e| PyEvent {
                        event_id: e.event_id() as i64,
                        kind: format!("{:?}", e.kind)
                            .split('{')
                            .next()
                            .unwrap_or("Unknown")
                            .trim()
                            .to_string(),
                        source_event_id: e.source_event_id.map(|v| v as i64),
                        timestamp_ms: e.timestamp_ms as i64,
                    })
                    .collect())
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Get the full instance tree (root + all descendants).
    fn get_instance_tree(
        &self,
        py: Python<'_>,
        instance_id: String,
    ) -> PyResult<PyInstanceTree> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let tree = client
                    .get_instance_tree(&instance_id)
                    .await
                    .map_err(|e| format!("{e}"))?;
                let size = tree.size() as i64;
                Ok(PyInstanceTree {
                    root_id: tree.root_id,
                    all_ids: tree.all_ids,
                    size,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Delete an orchestration instance and all its data.
    fn delete_instance(
        &self,
        py: Python<'_>,
        instance_id: String,
        force: bool,
    ) -> PyResult<PyDeleteInstanceResult> {
        let client = self.inner.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let result = client
                    .delete_instance(&instance_id, force)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyDeleteInstanceResult {
                    instances_deleted: result.instances_deleted as i64,
                    executions_deleted: result.executions_deleted as i64,
                    events_deleted: result.events_deleted as i64,
                    queue_messages_deleted: result.queue_messages_deleted as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Delete multiple instances matching a filter.
    fn delete_instance_bulk(
        &self,
        py: Python<'_>,
        filter: &PyInstanceFilter,
    ) -> PyResult<PyDeleteInstanceResult> {
        let client = self.inner.clone();
        let rust_filter = duroxide::providers::InstanceFilter {
            instance_ids: filter.instance_ids.clone(),
            completed_before: filter.completed_before.map(|v| v as u64),
            limit: filter.limit.map(|v| v as u32),
        };
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let result = client
                    .delete_instance_bulk(rust_filter)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyDeleteInstanceResult {
                    instances_deleted: result.instances_deleted as i64,
                    executions_deleted: result.executions_deleted as i64,
                    events_deleted: result.events_deleted as i64,
                    queue_messages_deleted: result.queue_messages_deleted as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Prune old executions from a single instance.
    fn prune_executions(
        &self,
        py: Python<'_>,
        instance_id: String,
        options: &PyPruneOptions,
    ) -> PyResult<PyPruneResult> {
        let client = self.inner.clone();
        let rust_options = duroxide::providers::PruneOptions {
            keep_last: options.keep_last.map(|v| v as u32),
            completed_before: options.completed_before.map(|v| v as u64),
        };
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let result = client
                    .prune_executions(&instance_id, rust_options)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyPruneResult {
                    instances_processed: result.instances_processed as i64,
                    executions_deleted: result.executions_deleted as i64,
                    events_deleted: result.events_deleted as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }

    /// Prune old executions from multiple instances matching a filter.
    fn prune_executions_bulk(
        &self,
        py: Python<'_>,
        filter: &PyInstanceFilter,
        options: &PyPruneOptions,
    ) -> PyResult<PyPruneResult> {
        let client = self.inner.clone();
        let rust_filter = duroxide::providers::InstanceFilter {
            instance_ids: filter.instance_ids.clone(),
            completed_before: filter.completed_before.map(|v| v as u64),
            limit: filter.limit.map(|v| v as u32),
        };
        let rust_options = duroxide::providers::PruneOptions {
            keep_last: options.keep_last.map(|v| v as u32),
            completed_before: options.completed_before.map(|v| v as u64),
        };
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                let result = client
                    .prune_executions_bulk(rust_filter, rust_options)
                    .await
                    .map_err(|e| format!("{e}"))?;
                Ok(PyPruneResult {
                    instances_processed: result.instances_processed as i64,
                    executions_deleted: result.executions_deleted as i64,
                    events_deleted: result.events_deleted as i64,
                })
            })
        })
        .map_err(|e: String| pyo3::exceptions::PyRuntimeError::new_err(e))
    }
}

fn convert_status(status: OrchestrationStatus) -> PyOrchestrationStatus {
    match status {
        OrchestrationStatus::NotFound => PyOrchestrationStatus {
            status: "NotFound".to_string(),
            output: None,
            error: None,
        },
        OrchestrationStatus::Running => PyOrchestrationStatus {
            status: "Running".to_string(),
            output: None,
            error: None,
        },
        OrchestrationStatus::Completed { output } => PyOrchestrationStatus {
            status: "Completed".to_string(),
            output: Some(output),
            error: None,
        },
        OrchestrationStatus::Failed { details } => PyOrchestrationStatus {
            status: "Failed".to_string(),
            output: None,
            error: Some(details.display_message()),
        },
    }
}
