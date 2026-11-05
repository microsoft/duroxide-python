// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use duroxide::runtime::test_hooks::{
    LifecycleHooks, LifecyclePoint, LifecycleTask, ProviderOperation,
};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::runtime::{PyRuntime, TOKIO_RT};
use crate::stop_observation::StopSource;

#[pyclass]
pub struct LifecycleTestHooks {
    hooks: LifecycleHooks,
    observation: ObservationHooks,
}

#[pyfunction]
pub fn _lifecycle_test_hooks(runtime: &PyRuntime) -> LifecycleTestHooks {
    LifecycleTestHooks {
        hooks: runtime.test_hooks.clone(),
        observation: runtime.stop_source.hooks.clone(),
    }
}

#[pymethods]
impl LifecycleTestHooks {
    fn hold(&self, point: &str) -> PyResult<()> {
        if point.starts_with("observer-") {
            return self.observation.hold(point);
        }
        let parsed = parse_point(point)?;
        let gate = if point == "blocking-provider" {
            self.hooks.hold_blocking(parsed)
        } else {
            self.hooks.hold(parsed)
        };
        gate.map(|_| ())
            .map_err(|error| PyRuntimeError::new_err(format!("test gate: {error:?}")))
    }

    fn release(&self, point: &str) -> PyResult<()> {
        if point.starts_with("observer-") {
            self.observation.gate(point)?.released.send_replace(true);
            return Ok(());
        }
        self.hooks
            .gate(parse_point(point)?)
            .ok_or_else(|| PyRuntimeError::new_err("test gate not armed"))?
            .release();
        Ok(())
    }

    fn wait_entered(&self, py: Python<'_>, point: &str) -> PyResult<()> {
        if point.starts_with("observer-") {
            let gate = self.observation.gate(point)?;
            return py
                .allow_threads(|| {
                    TOKIO_RT.block_on(async {
                        tokio::time::timeout(
                            Duration::from_secs(3),
                            signaled(gate.entered.subscribe()),
                        )
                        .await
                    })
                })
                .map_err(|_| PyTimeoutError::new_err("test gate entry timed out"));
        }
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
        if point == "observer-delivery" {
            self.observation.0.lock().failures.insert(point.to_owned());
            return Ok(());
        }
        self.hooks
            .fail_once_with_message(parse_point(point)?, message)
            .map_err(|error| PyRuntimeError::new_err(format!("test fault: {error:?}")))
    }

    fn retire_observation_publisher(&self, runtime: &mut PyRuntime) -> PyResult<()> {
        if !Arc::ptr_eq(&self.observation.0, &runtime.stop_source.hooks.0) {
            return Err(PyRuntimeError::new_err(
                "test observation publisher does not match",
            ));
        }
        drop(std::mem::replace(
            &mut runtime.stop_source,
            StopSource::new(),
        ));
        Ok(())
    }

    fn snapshot(&self) -> String {
        let counts = self.hooks.counts();
        let observation = self.observation.0.lock();
        serde_json::json!({
            "started": counts.iter().map(|count| count.started).sum::<usize>(),
            "completed": counts.iter().map(|count| count.completed).sum::<usize>(),
            "active": counts.iter().map(|count| count.active).sum::<usize>(),
            "cleanupActive": self.hooks.cleanup_counts().active,
            "coordinators": self.hooks.hits(LifecyclePoint::ShutdownCoordinator),
            "forceRequests": self.hooks.hits(LifecyclePoint::ForceRequested),
            "observerPending": observation.pending,
            "observerDeliveries": observation.deliveries,
        })
        .to_string()
    }
}

#[derive(Clone, Default)]
pub(crate) struct ObservationHooks(Arc<parking_lot::Mutex<ObservationState>>);

#[derive(Default)]
struct ObservationState {
    gates: HashMap<String, Arc<ObservationGate>>,
    failures: HashSet<String>,
    pending: usize,
    deliveries: usize,
}

struct ObservationGate {
    entered: watch::Sender<bool>,
    released: watch::Sender<bool>,
}

async fn signaled(mut signal: watch::Receiver<bool>) {
    while !*signal.borrow_and_update() {
        signal
            .changed()
            .await
            .expect("test gate sender is retained");
    }
}

impl ObservationHooks {
    fn hold(&self, point: &str) -> PyResult<()> {
        if !matches!(
            point,
            "observer-wait" | "observer-delivery" | "observer-final-access"
        ) {
            return Err(PyRuntimeError::new_err("unknown observation test point"));
        }
        let mut state = self.0.lock();
        if state
            .gates
            .get(point)
            .is_some_and(|gate| !*gate.released.borrow())
        {
            return Err(PyRuntimeError::new_err(
                "observation test gate already armed",
            ));
        }
        state.gates.insert(
            point.to_owned(),
            Arc::new(ObservationGate {
                entered: watch::channel(false).0,
                released: watch::channel(false).0,
            }),
        );
        Ok(())
    }

    fn gate(&self, point: &str) -> PyResult<Arc<ObservationGate>> {
        self.0
            .lock()
            .gates
            .get(point)
            .cloned()
            .ok_or_else(|| PyRuntimeError::new_err("observation test gate not armed"))
    }

    pub(crate) async fn checkpoint(&self, point: &str) -> bool {
        let (gate, failed) = {
            let mut state = self.0.lock();
            (
                state.gates.get(point).cloned(),
                state.failures.remove(point),
            )
        };
        if let Some(gate) = gate {
            gate.entered.send_replace(true);
            signaled(gate.released.subscribe()).await;
        }
        failed
    }

    pub(crate) fn register(&self) -> ObservationTask {
        self.0.lock().pending += 1;
        ObservationTask(self.clone())
    }

    pub(crate) fn delivered(&self, delivered: bool) {
        if delivered {
            self.0.lock().deliveries += 1;
        }
    }
}

pub(crate) struct ObservationTask(ObservationHooks);

impl Drop for ObservationTask {
    fn drop(&mut self) {
        self.0 .0.lock().pending -= 1;
    }
}

fn parse_point(point: &str) -> PyResult<LifecyclePoint> {
    match point {
        "startup" => Ok(LifecyclePoint::StartupBeforeIo),
        "partial-startup" => Ok(LifecyclePoint::StartupAfterSpawn(
            LifecycleTask::OrchestrationSlot,
        )),
        "provider" | "blocking-provider" => Ok(LifecyclePoint::ProviderReturn(
            ProviderOperation::FetchOrchestration,
        )),
        "worker" => Ok(LifecyclePoint::ParentWork(LifecycleTask::WorkerSlot)),
        "force" => Ok(LifecyclePoint::ForceRequested),
        "coordinator" => Ok(LifecyclePoint::ShutdownCoordinator),
        _ => Err(PyRuntimeError::new_err("unknown lifecycle test point")),
    }
}
