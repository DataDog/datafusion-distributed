# Routing tasks to workers

By default, each distributed task is routed to an available worker. When a
task's data has a *home* (e.g, a worker that already holds it in a cache or on
local disk) a custom routing handler can send the task there instead. As routing
handlers are responsible for establishing connection to remote workers, they are
`async`.

Implement `RouteTaskHandler` and register it on the coordinating session. The
handler is called once per task with a `RouteTaskEvent`, providing contextual
information about what task-specialized plan is getting routed, the task
identifier to which its routed, etc..

Return `None` when the handler does not apply, allowing the next custom or
built-in handler to run. Otherwise, call `dialer.dial(url).await` and return the
response for the selected connection. The dialer may be called more than once,
sequentially or concurrently, to implement retries.

`dialer.dial(url).await` connects to a remote worker under the hood, and returns
the already established connection. If this call succeeds, it means that the
worker is in a good state for being part of the query.

```rust
use async_trait::async_trait;
use datafusion::common::{Result, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{
    DistributedExt, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler,
    ok_or_some_err
};

struct RetryRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for RetryRouteTaskHandler {
    async fn handle(&self, event: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let urls = match ok_or_some_err!(event.worker_resolver.get_urls());
        if urls.is_empty() {
            return Some(exec_err!("no workers available"));
        }

        let start = event.task_key.task_number % urls.len();
        let mut last_error = None;
        for offset in 0..urls.len() {
            let url = urls[(start + offset) % urls.len()].clone();
            match event.dialer.dial(url).await {
                Ok(response) => return Some(Ok(response)),
                Err(error) => last_error = Some(error),
            }
        }
        Some(Err(last_error.expect("at least one worker was attempted")))
    }
}

SessionStateBuilder::new()
    .with_distributed_route_task_handler(RetryRouteTaskHandler);
```

## Affinity for feed-backed scans

Register `AffinityRouteTaskHandler` to prefer stable data homes for providers that
implement `WorkUnitFeedProvider::task_affinity()`:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{AffinityRouteTaskHandler, DistributedExt};

let builder = SessionStateBuilder::new()
    .with_distributed_route_task_handler(AffinityRouteTaskHandler);
```

The coordinator collects task-specific hints before converting the native feeds
to remote handles. The handler hashes each distinct key against the available
worker URLs, then prefers the worker owning the greatest hinted weight. Duplicate
keys use their maximum weight; independent scans are not deduplicated. Worker URL
order and query/task IDs do not affect ownership. Worker membership changes or a
hash implementation change on upgrade can change homes.

Use URLs identifying individual stable workers, not a load balancer. There is no
cache lookup or cache-specific dependency. This is best-effort whole-task placement,
not per-file execution routing, work stealing, or memory admission. No hints defers
to the existing routing handlers. Classified connection errors retain the normal
retry/failover behavior; routing can fall back away from a preferred worker.

For custom load-aware routing, inspect `event.work_unit_affinity()` and optionally
use `AffinityRouteTaskHandler::rank_workers()`.

### Affinity metrics

The coordinator exposes these metrics on `DistributedExec`:

| Metric | Meaning |
| --- | --- |
| `work_unit_affinity_routed_tasks` | Non-deferred affinity routing attempts, not individual connection attempts. |
| `work_unit_affinity_preferred_placements` | Successful connections whose returned worker URL equals the preferred URL, including successful same-worker retries. |
| `work_unit_affinity_fallback_placements` | Successful connections whose returned worker URL differs from the preferred URL. |
| `work_unit_affinity_routing_failures` | Routes returning an error: terminal connection errors, exhausted retries/candidates, or resolver/configuration errors. |
| `work_unit_affinity_routing_duration` | Summed elapsed time spent selecting and connecting, including retry backoff. This is not query wall-clock latency. |
| `work_unit_affinity_hinted_objects` | Sum of distinct positive-weight keys per routing attempt. |
| `work_unit_affinity_hinted_bytes` | Sum of candidate-byte estimates per routing attempt; duplicate keys use their maximum weight, as in worker ranking. |

No hints, only zero-weight hints, or an empty worker list defer without recording
these metrics. Cancellation after an attempt starts records elapsed time but no
terminal placement/failure, so attempts can exceed the sum of outcomes. Object
and byte totals deduplicate **within each task**, not across tasks or queries;
byte estimates saturate at `usize::MAX` per attempt.

Iceberg scan nodes also expose work-feed delivery metrics alongside reader metrics
(e.g. `work_unit_count` and `output_rows`). Hinted bytes are neither work-feed
serialized bytes nor actual storage reads. None of these metrics measures cache
hits or decoded-memory usage; monitor cache/backend metrics and worker memory
separately.

Routing also pairs naturally with `ScaleUpLeafNodeHandler`: that decides *what* data
task `i` reads, and `RouteTaskHandler` decides *where* that specialized task
runs. The task index can be used to keep a stable slot-to-worker mapping for
cache affinity.

For a complete, runnable walkthrough where parquet files consistently routed to
workers by hashing the file path, so each worker can serve them from an
in-memory cache on repeat queries, see the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
