# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.9] - 2026-02-21

### Added
- **`ctx.get_custom_status()`** — read the current custom status value from within an orchestration. Returns the status string or `None` if none has been set. Reflects all `set_custom_status`/`reset_custom_status` calls, including across turn boundaries and continue-as-new.

### Changed
- Upgraded duroxide to 0.1.20, duroxide-pg to 0.1.22

## [0.1.8] - 2026-02-15

### Added
- **Custom Status documentation** — user guide and architecture docs for `ctx.set_custom_status()`, `ctx.reset_custom_status()`, and `client.wait_for_status_change()`
- **Event Queues documentation** — user guide and architecture docs for `ctx.dequeue_event()` and `client.enqueue_event()`
- **Retry on Session documentation** — user guide docs for `ctx.schedule_activity_with_retry_on_session()`
- **Copilot Chat pattern** — real-world example combining event queues, custom status, and continue-as-new for interactive chat bots
- Updated README with custom status, event queues, and retry-on-session API examples

### Changed
- Bumped `duroxide` dependency to 0.1.19

### Deprecated
- `client.raise_event_persistent()` — use `client.enqueue_event()` instead
- `ctx.schedule_wait_persistent()` — use `ctx.dequeue_event()` instead

## [0.1.7] - 2026-02-14

### Added
- **Custom Status API** — `ctx.set_custom_status(status)` and `ctx.reset_custom_status()` for setting/clearing custom status from within orchestrations (fire-and-forget, no yield needed)
- **Custom Status on OrchestrationResult** — `custom_status` and `custom_status_version` fields now available on all status results (`get_status`, `wait_for_orchestration`, `wait_for_status_change`)
- **`client.wait_for_status_change(instance_id, last_seen_version, poll_interval_ms, timeout_ms)`** — polls for custom status changes on an orchestration instance
- **Event Queue API** — `ctx.dequeue_event(queue_name)` for FIFO mailbox-style event consumption in orchestrations, paired with `client.enqueue_event(instance_id, queue_name, data)` for sending messages
- **`ctx.schedule_activity_with_retry_on_session(name, input, retry, session_id)`** — retry with session affinity (all attempts pinned to the same worker)
- Queue event kinds (`QueueSubscribed`, `QueueEventDelivered`) now included in `read_execution_history` event data

### Changed
- Bumped `duroxide` dependency to 0.1.19
- Bumped `duroxide-pg` dependency to 0.1.21

## [0.1.6] - 2026-02-13

### Added
- `init_tracing(log_file, log_level?, log_format?)` — install a file-based tracing subscriber before `runtime.start()`. Uses first-writer-wins (`try_init`) so the runtime's built-in subscriber silently no-ops if one is already installed. Supports `"json"`, `"pretty"`, and `"compact"` (default) log formats.
- 3 new tests for `init_tracing`: import check, file write, invalid path error

## [0.1.5] - 2026-02-12

### Fixed
- Reverted premature tracing init change from 0.1.4 (library handles this correctly)

## [0.1.3] - 2026-02-12

### Added
- `data` field on history events (`read_execution_history`) — exposes activity results, inputs, errors, timer fire times, and all event-specific content as JSON strings
- Execution history example in README Admin APIs section
- New test: `test_read_execution_history_data`

## [0.1.2] - 2026-02-10

### Added
- `ctx.get_client()` on ActivityContext — activities can now start orchestrations, raise events, etc.
- `runtime.metrics_snapshot()` — get runtime metric counters (orchestration starts/completions, activity results, provider errors)
- Observability options on `PyRuntimeOptions`: `log_level`, `log_format`, `service_name`, `service_version`
- 5 new e2e tests: retry exhaustion, continue-as-new version upgrade, version routing, activity get_client, metrics snapshot (54 total)

## [0.1.1] - 2026-02-10

### Fixed
- README doc links now use absolute GitHub URLs so they work on PyPI

## [0.1.0] - 2026-02-10

### Added
- Generator-based orchestration API with deterministic replay
- Activity support with cooperative cancellation (`ctx.is_cancelled()`)
- `all()` (fan-out/fan-in) and `race()` (select) composition primitives
- Durable timers (`schedule_timer`) and external events (`wait_for_event`)
- Sub-orchestrations (blocking and fire-and-forget)
- Continue-as-new for eternal orchestrations
- Versioned orchestration registration
- Deterministic `utc_now()` and `new_guid()`
- SQLite provider (in-memory and file-based)
- PostgreSQL provider via duroxide-pg-opt (long-polling with LISTEN/NOTIFY)
- Schema isolation for PostgreSQL (`connect_with_schema`)
- Full admin API: list instances, get info, executions, history, tree, delete, prune
- Structured tracing via Rust `tracing` crate (controlled by `RUST_LOG`)
- Activity retry with configurable backoff policy
- Decorator-based registration (`@runtime.register_activity`, `@runtime.register_orchestration`)
- Runtime with configurable concurrency and poll intervals
- 49 tests across e2e, races, admin API, and scenario suites
- Documentation: user guide, architecture guide
