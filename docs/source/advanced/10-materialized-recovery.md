# Materialized execution and recovery

Materialized execution is an opt-in library facility for applications that can
replay read-only queries against stable inputs. It writes intermediate Arrow IPC
batches to an application-provided store, completes each producer stage before
starting its consumers, and materializes the final result before returning rows.
The default execution mode remains streaming.

## Application responsibilities

Register a `MaterializedStore` session extension independently on the coordinator
and every worker. They must address the same objects. The provided
`ObjectStoreIntermediateStore` wraps DataFusion's `object_store::ObjectStore`,
so applications can supply object storage, a shared filesystem, or an in-memory
store for tests. Credentials and storage hosting remain application concerns.

Register `MaterializedExecution` only on the coordinator. Its required
`ReplayPolicy` validates the complete query before execution and classifies
failures. Validation must establish that sources refer to stable snapshots or
immutable objects, and that UDFs, plan codecs, session construction, and worker
hooks tolerate repeated execution. Serialization is not proof of replay safety.
Do not use a permissive policy for arbitrary user queries.

```rust,ignore
let store = Arc::new(MaterializedStore::new(Arc::new(
    ObjectStoreIntermediateStore::new(object_store, prefix),
)));

// On the coordinator and independently in the WorkerSessionBuilder:
config.set_extension(Arc::clone(&store));
config.options_mut().optimizer.enable_dynamic_filter_pushdown = false;

// Only on the coordinator:
config.set_extension(Arc::new(
    MaterializedExecution::new(replay_policy)
        .with_retry_limits(3, 2, 16)
        .with_timeout(Duration::from_secs(300)),
));
```

The runnable `materialized_recovery` example uses the existing in-memory gRPC
cluster tooling and a shared object store:

```sh
cargo run --features integration --example materialized_recovery
```

Static task counts are required. Disable dynamic filter pushdown before planning;
materialized execution rejects adaptive task counts and enabled dynamic filters
rather than silently changing the requested execution semantics.

## What retries mean

`ReplayPolicy::on_error` chooses one of three bounded actions:

- `Fail`: preserve and return the execution error.
- `RetryTask`: reconstruct the failed logical task with a fresh execution
  identity and operator state, reusing its complete intermediate inputs.
- `RebuildStages`: reconstruct all stages from the same sealed work-unit inputs.
  Use this when an accepted intermediate object has been lost or corrupted.

The initial implementation conservatively rebuilds all stages for lost inputs;
selective dependency invalidation is a future optimization. Stage tasks execute
sequentially, while all output partitions of a task are drained concurrently.
This avoids requiring a cluster scheduler but is not a performance claim.

Work-unit providers are enumerated once per logical task. Their encoded payloads,
partition assignments, and end-of-feed are journaled before that task starts.
Each attempt replays the journal, including after a stage rebuild. Source content
must remain stable independently of the journal. A failure while sealing a feed
is not retried, because partially consuming an arbitrary provider is not safe.
The initial per-task journal limit equals `MaterializedStore`'s segment byte limit
(default 64 MiB), bounding queued work on the existing control-channel protocol.

Each attempt writes to an independent UUID namespace. The coordinator accepts
its manifest only after every partition finishes and the completion stream ends
successfully. An abandoned attempt cannot replace an accepted result. Half-built
aggregates are discarded: the replacement task reads its committed inputs into
fresh state. Task retries, stage rebuilds, and a combined query-wide retry budget
prevent unbounded recovery.

Execution may happen multiple times. This facility does not guarantee exactly-once
external side effects. Deterministic computation failures should not be retried.

## Result delivery, retention, and cancellation

No rows are exposed until the root result is complete. Once delivery starts, a
read failure terminates the result stream; the library does not rerun the query
and concatenate a replacement result with rows already delivered. Exact cursor
resumption and persistent result handles are not implemented.

The coordinator must remain alive throughout execution and delivery. A durable
store alone does not provide coordinator failover. Running the query again after
coordinator loss is a new invocation and may require the application to restore
its original source snapshot explicitly.

The library awaits cleanup on successful delivery and requests best-effort
cleanup when the stream is dropped. A store must also provide orphan expiry or
an application-owned janitor: process termination, failed cleanup, or a late
remote write can leave objects behind. `remove_query` is idempotent and can be
called repeatedly. Enforce query storage quotas in the supplied store; the
library's segment and journal limits are not a total disk quota.

Registering a coordinator-only pool is optional. The root executes locally, and
remote stages use the existing routing policy and worker resolver. To keep
remote stages off coordinators, exclude coordinator addresses from the resolver
and do not register local-worker execution on them.

`materialized_retries` and `materialized_rebuilds` are recorded on
`DistributedExec`. The completed prepared plan exposes the final materialized
reader. Reconstruction of per-attempt worker operator metrics and the full
materialized dependency graph in post-execution explain output is not yet
implemented.

## Compatibility

The facility adds new physical-plan codecs and session extensions. Existing
worker-channel message types and streaming execution semantics are unchanged.
Every participating worker must run a version with these codecs and have the
store extension installed. An unsupported worker fails plan decoding; it never
falls back to streaming a supposedly recoverable stage.
