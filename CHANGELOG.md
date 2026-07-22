# Changelog

Earlier versions: see git tags.

## v1.1.0

- TriggerActiveSameDBProgress: same-DB cross-thread trigger contention now waits-and-proceeds inside the engine instead of surfacing retry churn to callers.
- `CallbackActiveCrossThread { Trigger }` keeps its exact Display string, but its normal trigger scope narrows to captured callback tx-bound handles used from the wrong thread and deadlock-guard timeout paths; unrelated cross-DB writers proceed independently.
- Added the `CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS` override for the bounded same-DB trigger wait guard. Default: 60 seconds; no enforced minimum.
- Deadlock-guard timeouts emit one structured `tracing::warn!` with `trigger_name`, `waited_ms`, and `surface`.
- Class A callback-thread misuse returns `CallbackReentry`; cron same-DB callback contention remains an immediate typed callback-active error.
