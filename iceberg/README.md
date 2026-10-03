# DataFusion Distributed Iceberg

Read-only Apache Iceberg tables for DataFusion Distributed.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

async fn example() -> datafusion::error::Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.sql(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
     LOCATION 's3://warehouse/taxi/metadata/v1.metadata.json'",
    )
        .await?
        .collect()
        .await?;
    Ok(())
}
```

The default storage factory resolves `file://`, S3 (`s3://`, `s3a://`,
`s3n://`), and GCS (`gs://`, `gcs://`) URIs. Use
`IcebergIntegrationOptions` to supply custom storage or an Iceberg runtime.

```bash
cargo test -p datafusion-distributed-iceberg
```

## Whole-file assignment and cache affinity

Both options below are opt-in and require `iceberg.plan_files=true` (the default).
Register the routing handler on the **coordinator**, in addition to the normal
Iceberg integration, distributed planner, worker resolver and channel resolver:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{AffinityRouteTaskHandler, DistributedExt};
use datafusion_distributed_iceberg::IcebergConfig;

fn enable_file_routing(builder: SessionStateBuilder) -> SessionStateBuilder {
    let mut iceberg = IcebergConfig::default();
    iceberg.greedy_file_assignment = true;
    iceberg.file_task_affinity = true;
    builder
        .with_distributed_option_extension(iceberg)
        .with_distributed_route_task_handler(AffinityRouteTaskHandler)
}
```

- `greedy_file_assignment` places largest files on the least-loaded task using
  candidate-file bytes. It does not split files. File paths/ranges break ties
  deterministically. The resulting assignment is shared by routing and feeds.
- `file_task_affinity` publishes this task's assigned data-file paths and byte
  weights before worker selection. It can also be used with round-robin assignment.
- `AffinityRouteTaskHandler` uses weighted rendezvous ownership of immutable file
  keys to choose a worker for the **whole task**. Repeated references to one object
  count once for affinity, but every scan still executes independently. Delete-file
  locality and actual requested column/range overlap are not estimated.

Use stable worker URLs identifying individual cache owners (for example stateful
worker DNS names), not a load-balanced service URL. The connector does not know
about Foyer or any other cache. A cache must already wrap the workers' storage IO.
Grouping changes or worker membership changes can move files; this is best-effort
locality, not a guarantee that every occurrence of a file reaches the same worker.
The normal connection retry policy can fall back when a preferred worker rejects a
connection. This policy does not measure live load or enforce memory/concurrency
limits; keep admission controls and memory headroom when testing hot objects.

The metric `work_unit_affinity_routed_tasks` counts tasks offered to affinity
routing, not cache hits. Compare remote-read bytes, cold/warm cache hits and
coalesced misses alongside worker memory and task skew. No hit-rate or end-to-end
latency improvement has been established by the fixture tests.

See [The life of an iceberg file](docs/the-life-of-an-iceberg-file.md) for the
relationship between scans, work units, tasks and workers.

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.
