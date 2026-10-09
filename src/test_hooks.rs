// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use duroxide::runtime::test_hooks::{
    LifecycleHooks, LifecyclePoint, LifecycleTask, ProviderOperation,
};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError};
use pyo3::prelude::*;
use std::time::Duration;

use crate::runtime::{PyRuntime, TOKIO_RT};

#[pyclass]
pub struct LifecycleTestHooks {
    hooks: LifecycleHooks,
}

#[pyfunction]
pub fn _lifecycle_test_hooks(runtime: &PyRuntime) -> LifecycleTestHooks {
    LifecycleTestHooks {
        hooks: runtime.test_hooks.clone(),
    }
}

#[pymethods]
impl LifecycleTestHooks {
    fn hold(&self, point: &str) -> PyResult<()> {
        self.hooks
            .hold(parse_point(point)?)
            .map(|_| ())
            .map_err(|error| PyRuntimeError::new_err(format!("test gate: {error:?}")))
    }

    fn release(&self, point: &str) -> PyResult<()> {
        self.hooks
            .gate(parse_point(point)?)
            .ok_or_else(|| PyRuntimeError::new_err("test gate not armed"))?
            .release();
        Ok(())
    }

    fn wait_entered(&self, py: Python<'_>, point: &str) -> PyResult<()> {
        let gate = self
            .hooks
            .gate(parse_point(point)?)
            .ok_or_else(|| PyRuntimeError::new_err("test gate not armed"))?;
        py.allow_threads(|| {
            TOKIO_RT.block_on(async {
                tokio::time::timeout(Duration::from_secs(3), gate.entered()).await
            })
        })
        .map_err(|_| PyTimeoutError::new_err("test gate entry timed out"))
    }

    fn fail(&self, point: &str, message: &str) -> PyResult<()> {
        self.hooks
            .fail_once_with_message(parse_point(point)?, message)
            .map_err(|error| PyRuntimeError::new_err(format!("test fault: {error:?}")))
    }

    fn snapshot(&self) -> String {
        let counts = self.hooks.counts();
        serde_json::json!({
            "started": counts.iter().map(|count| count.started).sum::<usize>(),
            "completed": counts.iter().map(|count| count.completed).sum::<usize>(),
            "active": counts.iter().map(|count| count.active).sum::<usize>(),
            "cleanupActive": self.hooks.cleanup_counts().active,
            "coordinators": self.hooks.hits(LifecyclePoint::ShutdownCoordinator),
            "forceRequests": self.hooks.hits(LifecyclePoint::ForceRequested),
        })
        .to_string()
    }
}

fn parse_point(point: &str) -> PyResult<LifecyclePoint> {
    match point {
        "startup" => Ok(LifecyclePoint::StartupBeforeIo),
        "partial-startup" => Ok(LifecyclePoint::StartupAfterSpawn(
            LifecycleTask::OrchestrationDispatcher,
        )),
        "provider" => Ok(LifecyclePoint::ProviderReturn(
            ProviderOperation::FetchOrchestration,
        )),
        "worker" => Ok(LifecyclePoint::ParentWork(LifecycleTask::WorkDispatcher)),
        "force" => Ok(LifecyclePoint::ForceRequested),
        "coordinator" => Ok(LifecyclePoint::ShutdownCoordinator),
        _ => Err(PyRuntimeError::new_err("unknown lifecycle test point")),
    }
}
