// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use duroxide::runtime::{RuntimeStopObserver, RuntimeStopStatus};
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

use crate::runtime::TOKIO_RT;

const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
struct SourceState {
    status: RuntimeStopStatus,
    core: Option<RuntimeStopObserver>,
    closed: bool,
}

fn merge(left: RuntimeStopStatus, right: RuntimeStopStatus) -> RuntimeStopStatus {
    use RuntimeStopStatus::*;
    match (left, right) {
        (Faulted, _) | (_, Faulted) => Faulted,
        (Requested, _) | (_, Requested) => Requested,
        _ => NotRequested,
    }
}

impl SourceState {
    fn status(&self) -> RuntimeStopStatus {
        merge(
            self.status,
            self.core.as_ref().map_or(self.status, |core| core.state()),
        )
    }
}

pub(crate) struct StopSource {
    state: watch::Sender<SourceState>,
    #[cfg(feature = "test-hooks")]
    pub(crate) hooks: crate::test_hooks::ObservationHooks,
}

impl StopSource {
    pub fn new() -> Self {
        Self {
            state: watch::channel(SourceState {
                status: RuntimeStopStatus::NotRequested,
                core: None,
                closed: false,
            })
            .0,
            #[cfg(feature = "test-hooks")]
            hooks: crate::test_hooks::ObservationHooks::default(),
        }
    }

    pub fn subscribe(&self) -> PyRuntimeStopSource {
        PyRuntimeStopSource {
            state: self.state.subscribe(),
            #[cfg(feature = "test-hooks")]
            hooks: self.hooks.clone(),
        }
    }

    pub fn attach(&self, core: RuntimeStopObserver) {
        self.state.send_modify(|state| {
            assert!(!state.closed && state.core.is_none());
            state.status = merge(state.status, core.state());
            state.core = Some(core);
        });
    }

    pub fn requested(&self) {
        self.state.send_if_modified(|state| {
            let status = merge(state.status, RuntimeStopStatus::Requested);
            if state.closed || state.status == status {
                return false;
            }
            state.status = status;
            true
        });
    }

    pub fn close(&self) {
        self.state.send_modify(|state| {
            state.status = state.status();
            state.core = None;
            state.closed = true;
        });
    }
}

impl Drop for StopSource {
    fn drop(&mut self) {
        // Freeze the final snapshot instead of leaving a live core receiver behind.
        self.close();
    }
}

#[pyclass]
pub struct PyRuntimeStopSource {
    state: watch::Receiver<SourceState>,
    #[cfg(feature = "test-hooks")]
    hooks: crate::test_hooks::ObservationHooks,
}

#[pymethods]
impl PyRuntimeStopSource {
    fn observer(&self) -> PyRuntimeStopObserver {
        PyRuntimeStopObserver {
            state: Mutex::new(ObserverState {
                source: Some(self.state.clone()),
                pending: None,
            }),
            #[cfg(feature = "test-hooks")]
            hooks: self.hooks.clone(),
        }
    }
}

struct ObserverState {
    source: Option<watch::Receiver<SourceState>>,
    pending: Option<watch::Sender<()>>,
}

#[pyclass]
pub struct PyRuntimeStopObserver {
    state: Mutex<ObserverState>,
    #[cfg(feature = "test-hooks")]
    hooks: crate::test_hooks::ObservationHooks,
}

#[pymethods]
impl PyRuntimeStopObserver {
    #[getter]
    fn state(&self) -> PyResult<&'static str> {
        let state = self.state.lock();
        let source = state.source.as_ref().ok_or_else(closed_error)?;
        let snapshot = source.borrow();
        Ok(encode(snapshot.status()))
    }

    #[pyo3(signature = (last_seen, timeout_ms=None))]
    fn wait_for_change(
        &self,
        py: Python<'_>,
        last_seen: &str,
        timeout_ms: Option<i64>,
    ) -> PyResult<Option<&'static str>> {
        let last_seen = decode(last_seen)?;
        let deadline = observation_deadline(timeout_ms)?;
        let (source, mut canceled) = {
            let mut state = self.state.lock();
            let source = state.source.as_ref().ok_or_else(closed_error)?;
            if state.pending.is_some() {
                return Err(PyRuntimeError::new_err(
                    "stop_observation_busy: this observer already has a pending wait",
                ));
            }
            {
                let snapshot = source.borrow();
                let status = snapshot.status();
                if status != last_seen {
                    return Ok(Some(encode(status)));
                }
                if snapshot.closed {
                    return Ok(None);
                }
            }
            if deadline.is_some_and(|until| Instant::now() >= until) {
                return Err(timeout_error());
            }
            let source = source.clone();
            let (cancel, canceled) = watch::channel(());
            state.pending = Some(cancel);
            (source, canceled)
        };
        let _pending = PendingWait(&self.state);
        let mut completion = spawn_wait(
            source,
            last_seen,
            canceled.clone(),
            #[cfg(feature = "test-hooks")]
            self.hooks.clone(),
        );
        loop {
            let check_signals_at = Instant::now() + SIGNAL_CHECK_INTERVAL;
            let wake_at = deadline.map_or(check_signals_at, |until| until.min(check_signals_at));
            let result = py.allow_threads(|| {
                TOKIO_RT.block_on(async {
                    tokio::select! {
                        biased;
                        _ = canceled.changed() => Some(Err(ObservationError::Closed)),
                        result = &mut completion => Some(result.unwrap_or(Err(ObservationError::Failed))),
                        () = tokio::time::sleep_until(wake_at.into()) => None,
                    }
                })
            });
            // Signal handlers may reenter close/state/wait. Never run them under a metadata lock.
            py.check_signals()?;
            if self.state.lock().source.is_none() {
                return Err(closed_error());
            }
            let result = result.or_else(|| match completion.try_recv() {
                Ok(result) => Some(result),
                Err(oneshot::error::TryRecvError::Empty) => None,
                Err(oneshot::error::TryRecvError::Closed) => Some(Err(ObservationError::Failed)),
            });
            if let Some(result) = result {
                return result
                    .map(|status| status.map(encode))
                    .map_err(|error| match error {
                        ObservationError::Closed => closed_error(),
                        ObservationError::Failed => PyRuntimeError::new_err(
                            "stop_observation_failed: native observation failed",
                        ),
                    });
            }
            if deadline.is_some_and(|until| Instant::now() >= until) {
                return Err(timeout_error());
            }
        }
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.source = None;
        state.pending = None;
    }
}

struct PendingWait<'a>(&'a Mutex<ObserverState>);

impl Drop for PendingWait<'_> {
    fn drop(&mut self) {
        self.0.lock().pending = None;
    }
}

enum ObservationError {
    Closed,
    Failed,
}

fn spawn_wait(
    mut source: watch::Receiver<SourceState>,
    last_seen: RuntimeStopStatus,
    mut canceled: watch::Receiver<()>,
    #[cfg(feature = "test-hooks")] hooks: crate::test_hooks::ObservationHooks,
) -> oneshot::Receiver<Result<Option<RuntimeStopStatus>, ObservationError>> {
    let (completed, completion) = oneshot::channel();
    #[cfg(feature = "test-hooks")]
    let pending = hooks.register();
    #[cfg(feature = "test-hooks")]
    let waiting_hooks = hooks.clone();
    // Both tasks own metadata only. No Python reference or runtime owner crosses this boundary.
    let waiting = TOKIO_RT.spawn(async move {
        tokio::select! {
            biased;
            _ = canceled.changed() => Err(ObservationError::Closed),
            status = async {
                #[cfg(feature = "test-hooks")]
                waiting_hooks.checkpoint("observer-wait").await;
                changed(&mut source, last_seen).await
            } => Ok(status),
        }
    });
    TOKIO_RT.spawn(async move {
        let result = match waiting.await {
            Ok(result) => result,
            Err(error) => {
                if error.is_panic() {
                    crate::runtime::discard_panic_payload(error.into_panic());
                }
                Err(ObservationError::Failed)
            }
        };
        #[cfg(feature = "test-hooks")]
        let result = if hooks.checkpoint("observer-delivery").await {
            Err(ObservationError::Failed)
        } else {
            result
        };
        // The caller can already have timed out, been interrupted or closed.
        let delivered = completed.send(result).is_ok();
        #[cfg(feature = "test-hooks")]
        {
            hooks.delivered(delivered);
            hooks.checkpoint("observer-final-access").await;
            drop(pending);
        }
        #[cfg(not(feature = "test-hooks"))]
        let _ = delivered;
    });
    completion
}

async fn changed(
    source: &mut watch::Receiver<SourceState>,
    last_seen: RuntimeStopStatus,
) -> Option<RuntimeStopStatus> {
    loop {
        let (state, status) = {
            let state = source.borrow_and_update();
            (state.clone(), state.status())
        };
        if status != last_seen {
            return Some(status);
        }
        if state.closed {
            return None;
        }
        if let Some(mut core) = state.core {
            let native_status = core.state();
            if merge(state.status, native_status) != last_seen {
                continue;
            }
            tokio::select! {
                _ = source.changed() => {},
                result = core.changed(native_status) => {
                    if result.is_none() {
                        // Core cleanup does not end a live SDK publisher.
                        let _ = source.changed().await;
                    }
                },
            }
        } else {
            let _ = source.changed().await;
        }
    }
}

fn encode(status: RuntimeStopStatus) -> &'static str {
    match status {
        RuntimeStopStatus::NotRequested => "NotRequested",
        RuntimeStopStatus::Requested => "Requested",
        RuntimeStopStatus::Faulted => "Faulted",
    }
}

fn decode(status: &str) -> PyResult<RuntimeStopStatus> {
    match status {
        "NotRequested" => Ok(RuntimeStopStatus::NotRequested),
        "Requested" => Ok(RuntimeStopStatus::Requested),
        "Faulted" => Ok(RuntimeStopStatus::Faulted),
        _ => Err(PyValueError::new_err(
            "stop_observation_invalid_status: invalid last-seen status",
        )),
    }
}

fn observation_deadline(timeout_ms: Option<i64>) -> PyResult<Option<Instant>> {
    let Some(millis) = timeout_ms else {
        return Ok(None);
    };
    let invalid = || {
        PyValueError::new_err(
        "stop_observation_invalid_timeout: timeout_ms must be a nonnegative supported millisecond duration",
    )
    };
    let duration = Duration::from_millis(u64::try_from(millis).map_err(|_| invalid())?);
    if duration > Duration::from_nanos(u64::MAX) {
        return Err(invalid());
    }
    let deadline = Instant::now().checked_add(duration).ok_or_else(invalid)?;
    deadline
        .checked_add(Duration::from_millis(1))
        .ok_or_else(invalid)?;
    Ok(Some(deadline))
}

fn closed_error() -> PyErr {
    PyRuntimeError::new_err("stop_observation_closed: this observer is closed")
}

fn timeout_error() -> PyErr {
    PyTimeoutError::new_err(
        "stop_observation_timed_out: no changed state was delivered before the deadline",
    )
}
