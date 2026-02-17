use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// A scheduled task descriptor yielded from orchestration generators.
/// The Rust runtime executes these using the real OrchestrationContext.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ScheduledTask {
    #[serde(rename = "activity")]
    Activity { name: String, input: String },
    #[serde(rename = "activityWithSession")]
    ActivityWithSession {
        name: String,
        input: String,
        #[serde(rename = "sessionId")]
        session_id: String,
    },
    #[serde(rename = "activityWithRetry")]
    ActivityWithRetry {
        name: String,
        input: String,
        retry: RetryPolicyConfig,
    },
    #[serde(rename = "timer")]
    Timer {
        #[serde(rename = "delayMs")]
        delay_ms: u64,
    },
    #[serde(rename = "waitEvent")]
    WaitEvent { name: String },
    #[serde(rename = "subOrchestration")]
    SubOrchestration { name: String, input: String },
    #[serde(rename = "subOrchestrationWithId")]
    SubOrchestrationWithId {
        name: String,
        #[serde(rename = "instanceId")]
        instance_id: String,
        input: String,
    },
    #[serde(rename = "subOrchestrationVersioned")]
    SubOrchestrationVersioned {
        name: String,
        version: Option<String>,
        input: String,
    },
    #[serde(rename = "subOrchestrationVersionedWithId")]
    SubOrchestrationVersionedWithId {
        name: String,
        version: Option<String>,
        #[serde(rename = "instanceId")]
        instance_id: String,
        input: String,
    },
    #[serde(rename = "orchestration")]
    Orchestration {
        name: String,
        #[serde(rename = "instanceId")]
        instance_id: String,
        input: String,
    },
    #[serde(rename = "orchestrationVersioned")]
    OrchestrationVersioned {
        name: String,
        version: Option<String>,
        #[serde(rename = "instanceId")]
        instance_id: String,
        input: String,
    },
    #[serde(rename = "newGuid")]
    NewGuid,
    #[serde(rename = "utcNow")]
    UtcNow,
    #[serde(rename = "continueAsNew")]
    ContinueAsNew { input: String },
    #[serde(rename = "continueAsNewVersioned")]
    ContinueAsNewVersioned {
        input: String,
        version: Option<String>,
    },
    #[serde(rename = "join")]
    Join { tasks: Vec<ScheduledTask> },
    #[serde(rename = "select")]
    Select { tasks: Vec<ScheduledTask> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetryPolicyConfig {
    #[serde(rename = "maxAttempts")]
    pub max_attempts: u32,
    #[serde(rename = "timeoutMs")]
    pub timeout_ms: Option<u64>,
    #[serde(rename = "totalTimeoutMs")]
    pub total_timeout_ms: Option<u64>,
    pub backoff: Option<String>,
}

/// Result of driving a generator one step.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum GeneratorStepResult {
    #[serde(rename = "yielded")]
    Yielded {
        #[serde(rename = "generatorId")]
        generator_id: u64,
        task: ScheduledTask,
    },
    #[serde(rename = "completed")]
    Completed { output: String },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Orchestration status returned to Python.
#[pyclass(get_all, set_all)]
#[derive(Debug, Clone)]
pub struct PyOrchestrationStatus {
    pub status: String,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// System metrics returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PySystemMetrics {
    pub total_instances: i64,
    pub total_executions: i64,
    pub running_instances: i64,
    pub completed_instances: i64,
    pub failed_instances: i64,
    pub total_events: i64,
}

/// Queue depths returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyQueueDepths {
    pub orchestrator_queue: i64,
    pub worker_queue: i64,
    pub timer_queue: i64,
}

/// Instance info returned to Python.
#[pyclass(get_all, set_all)]
#[derive(Debug, Clone)]
pub struct PyInstanceInfo {
    pub instance_id: String,
    pub orchestration_name: String,
    pub orchestration_version: String,
    pub current_execution_id: i64,
    pub status: String,
    pub output: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub parent_instance_id: Option<String>,
}

/// Execution info returned to Python.
#[pyclass(get_all, set_all)]
#[derive(Debug, Clone)]
pub struct PyExecutionInfo {
    pub execution_id: i64,
    pub status: String,
    pub output: Option<String>,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub event_count: i64,
}

/// Instance tree returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyInstanceTree {
    pub root_id: String,
    pub all_ids: Vec<String>,
    pub size: i64,
}

/// Delete result returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyDeleteInstanceResult {
    pub instances_deleted: i64,
    pub executions_deleted: i64,
    pub events_deleted: i64,
    pub queue_messages_deleted: i64,
}

/// Prune options from Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyPruneOptions {
    pub keep_last: Option<i64>,
    pub completed_before: Option<i64>,
}

#[pymethods]
impl PyPruneOptions {
    #[new]
    #[pyo3(signature = (keep_last=None, completed_before=None))]
    fn new(keep_last: Option<i64>, completed_before: Option<i64>) -> Self {
        Self {
            keep_last,
            completed_before,
        }
    }
}

/// Prune result returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyPruneResult {
    pub instances_processed: i64,
    pub executions_deleted: i64,
    pub events_deleted: i64,
}

/// Instance filter from Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyInstanceFilter {
    pub instance_ids: Option<Vec<String>>,
    pub completed_before: Option<i64>,
    pub limit: Option<i64>,
}

#[pymethods]
impl PyInstanceFilter {
    #[new]
    #[pyo3(signature = (instance_ids=None, completed_before=None, limit=None))]
    fn new(
        instance_ids: Option<Vec<String>>,
        completed_before: Option<i64>,
        limit: Option<i64>,
    ) -> Self {
        Self {
            instance_ids,
            completed_before,
            limit,
        }
    }
}

/// Runtime metrics snapshot returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyMetricsSnapshot {
    pub orch_starts: u64,
    pub orch_completions: u64,
    pub orch_failures: u64,
    pub orch_application_errors: u64,
    pub orch_infrastructure_errors: u64,
    pub orch_configuration_errors: u64,
    pub orch_poison: u64,
    pub activity_success: u64,
    pub activity_app_errors: u64,
    pub activity_infra_errors: u64,
    pub activity_config_errors: u64,
    pub activity_poison: u64,
    pub orch_dispatcher_items_fetched: u64,
    pub worker_dispatcher_items_fetched: u64,
    pub orch_continue_as_new: u64,
    pub suborchestration_calls: u64,
    pub provider_errors: u64,
}

/// A single history event returned to Python.
#[pyclass(get_all)]
#[derive(Debug, Clone)]
pub struct PyEvent {
    pub event_id: i64,
    pub kind: String,
    pub source_event_id: Option<i64>,
    pub timestamp_ms: i64,
    /// Event-specific data (activity result, input, error, timer fire_at, etc.)
    pub data: Option<String>,
}
