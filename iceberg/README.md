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

## Column sizes and join costing

Column statistics remain opt-in. With `iceberg.column_stats_enabled=true`,
filtered preplanned scans sum compressed column sizes from the **selected files**.
Nested columns require sizes for every leaf; missing metrics remain unknown.
Whole-snapshot bounds, null counts and distinct counts are not published for these
filtered scans. No Parquet footers are read, but the pinned Iceberg API requires
another manifest pass to recover the sizes. Scan task counts continue to use
whole-file bytes, not projected column bytes.

To retain these estimates through grouping keys and inner hash joins, register
`ColumnByteStatisticsProvider` on the session that performs physical planning:

```rust
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_distributed::ColumnByteStatisticsProvider;
use datafusion_distributed_iceberg::IcebergExt;

fn enable_column_size_costing(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.config().get_or_insert_default()
        .options_mut().optimizer.use_statistics_registry = true;
    builder
        .with_iceberg_column_stats_enabled(true)
        .with_statistics_registry(StatisticsRegistry::with_providers(vec![
            Arc::new(ColumnByteStatisticsProvider),
        ]))
}
```

Call this after configuring the session. This example replaces its statistics
registry; put the provider after any custom providers if composing an existing
chain. It uses the operators' normal row estimates, not the registry's alternative
built-in join cardinality model. There is no automatic registry installation.

The provider assumes uniform column widths when rescaling to output rows.
Compressed storage bytes are **not** measured Arrow or hash-table memory, and
missing metrics are not filled with guessed widths. Enabling these estimates can
change both build-side and collect/partitioned join choices; validate representative
queries and memory use before rollout. No runtime speedup is implied by a plan
change alone.

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
