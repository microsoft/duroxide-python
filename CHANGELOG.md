# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
