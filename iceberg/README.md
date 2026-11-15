# DataFusion Distributed Iceberg

Distributed Iceberg scans using upstream table providers and readers.

**WIP dependency:** this draft requires unpublished `datafusion-iceberg` extensions
on top of [upstream PR #67](https://github.com/apache/datafusion-iceberg/pull/67).
The pinned revision supplies the eager APIs, but not the task-source/scan-factory,
runtime, statistics, shared reader settings, and SQL snapshot extensions used here.
A checkout of this branch alone is not yet buildable; the passing local checks used
a Cargo path patch to the extended upstream prototype. No local path is committed.

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

### Upstream providers

Use `iceberg_datafusion::IcebergStaticTableProvider` or
`iceberg_datafusion::IcebergCatalogTableProvider` directly. This crate implements
no table provider or catalog: `with_iceberg_integration` installs an upstream
scan-construction hook and the distributed codecs. Programmatic providers and
SQL-created providers use the same hook.

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use iceberg::table::Table;
use iceberg_datafusion::IcebergStaticTableProvider;

async fn register(
    ctx: &SessionContext,
    table: Table,
    runtime: iceberg::Runtime,
) -> datafusion::error::Result<()> {
    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await?
        .with_runtime(runtime);
    ctx.register_table("taxi", Arc::new(provider))?;
    Ok(())
}
```

A provider's explicit runtime takes precedence over the integration default.
It configures the Iceberg reader's runtime-backed delete loading and eager task
collection; ordinary data-file reads remain on the stream's polling runtime.
It does not rebind table/cache internals or catalog refresh. Keep the underlying
Tokio runtimes alive while queries run. Workers reconstruct readers using their
own configured runtime; runtime handles are not serialized.

SQL time travel remains available with `OPTIONS ('iceberg.snapshot_id' '123')`.
The upstream SQL factory selects the historical schema before query planning.
DFD and upstream providers use the same `IcebergDataFusionConfig` extension and
`iceberg.execution.*` options. The integration preserves an existing registration.
Runtime and storage factories remain process-local integration settings.

Upstream catalog writes remain available in sessions without distributed
planning. The installed Iceberg task-count handler rejects unsupported write and
commit operators when distributing a plan; no automatic single-node fallback
is promised. Dynamic planning can reject during execution-time preparation,
after read producers have started. As with other event handlers, an earlier
custom task-count response can override this default policy.

By default, Iceberg discovers files progressively during execution. Enable
`iceberg.execution.eager_file_planning` to discover and retain file tasks during physical
planning instead. Retained descriptors use the query memory pool; there is no
file-count limit.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
use iceberg_datafusion::IcebergDataFusionConfig;

let config = SessionConfig::new()
    .with_option_extension(IcebergDataFusionConfig::default())
    .set_bool("iceberg.execution.eager_file_planning", true);
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(config)
    .with_iceberg_integration(IcebergIntegrationOptions::default())
    .build();
```

```text
Eager: discover → source owns files → assign task-local groups → route → worker reads
Lazy:  scan definition → remote feed reference → discover and dispatch → worker reads
```

`EagerIcebergDataSource` owns eager partition groups. Like standard file scans,
the scale-up handler creates a source per task before routing. The router can
borrow those files from `RouteTaskEvent::task_specialized_plan`:

```rust
use iceberg_datafusion::read::EagerIcebergDataSource;

fn assigned_paths(source: &EagerIcebergDataSource) -> Vec<&str> {
    source.planned_files().map(|file| file.data_file_path()).collect()
}
```

Use `planned_partition_files(partition)` to inspect a particular local partition.
DFD's eager assignment considers largest files first and places each on the task
with the fewest assigned bytes, cycling that task's local partitions. Upstream
preserves the supplied groups. File identity breaks equal-size ties; task
file count and task index break equal-load ties.
This is the eager policy, with no separate configuration flag. File bytes are
scheduling estimates; assignment neither splits files nor chooses workers.
Task-local sources own their assigned descriptors and reserve query memory independently;
the original source retains its own reservation. Reader clones share delete-loading state.
The codec sends only each task's assigned groups in its plan; workers restore
those groups without discovering or reassigning files.

Lazy scans use upstream `iceberg_datafusion::read::IcebergDataSource<FeedTaskSource>`
directly; DFD has no lazy `DataSource` implementation or wrapper. `FeedTaskSource`
implements upstream's `FileScanTaskSource` around DFD's local or remote work-unit
feed. Access the feed with `source.task_source().feed()`. The coordinator obtains
tasks from upstream's table-scan source; codecs reconstruct the same upstream
source on workers without discovering files there.

Both eager and lazy scans use upstream data sources directly; DFD implements no
Iceberg `DataSource` wrapper. Upstream owns reading, FileIO, statistics, retained
files, memory reservations, and regrouping. DFD chooses distributed assignments,
delivers tasks and transports plans. Reader configuration and statistics settings
come from the shared upstream options. Snapshot totals, selected-file totals, and optional manifest
column statistics are computed upstream.
Iceberg's `FileIO` handles storage in both cases. Eager planning waits for metadata
discovery and increases worker-plan payloads; lazy scans overlap discovery with
reading and deliver descriptors through the coordinator's feed.

Eager scans derive row counts and bytes from selected files and cap task estimates
by the number of selected files. Predicates or deletes make row counts inexact;
empty selections have exactly zero rows. Whole-snapshot column statistics are
skipped when they would describe a different row set. Lazy scans retain snapshot
statistics and task-size estimates. Lazy row counts are inexact when deletes may
apply or a predicate can remove rows. Output statistics account for per-partition
fetch limits; task sizing uses upstream's `unlimited_statistics()` instead,
retaining the complete file-work estimates. The scan factory returns the upstream
eager source directly, without regrouping or replacing its statistics.
Selected-file column statistics remain a separate change. Task-local eager variants
and decoded plans do not inherit whole-assignment statistics: estimates are unknown
apart from bounds implied by a fetch limit.

```bash
cargo test -p datafusion-distributed-iceberg -p datafusion-distributed-iceberg-test-tools
```

Tests generate the Iceberg tables they read with the unpublished
[`datafusion-distributed-iceberg-test-tools`](test-tools) package instead of
reading checked-in data files.

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this distributed-scan crate.

Eager source construction and regrouping are independent of distributed scheduling.
DFD's scale-up handler calls upstream's `with_planned_files(groups)` with native
file tasks grouped by local partition. It supplies the same number of groups for
every distributed task, including empty groups, so variants have compatible
partition counts. Reassignment preserves reader settings and clears old statistics.
Initial scans keep upstream's partition count, bounded by selected files and with
one partition for an empty scan, instead of forcing `target_partitions` groups.

The codec reconstructs sources with `IcebergDataSourceBuilder::from_planned_files`
and synchronous `build_planned`, preserving transported groups without discovery
or a table/catalog lookup. Duplicate tasks are not deduplicated; assignment
is responsible for avoiding unwanted duplicate work.

The current PoC keeps data-file concurrency at 8, row-group filtering on and row
selection off. Portable `ArrowReaderOptions` transport from iceberg-rust #3514
is a follow-up; workers currently construct readers from their session settings.

### Custom file IO serialization

Set `IcebergIntegrationOptions::file_io_codec` to customize how eager and lazy
scans transport `FileIO`. Implement `IcebergFileIOCodec::encode` to return bytes
and `decode` to rebuild file IO using those bytes and the worker's `TaskContext`.
This lets a custom storage factory capture query-local observability without
replacing the physical-plan codecs.

```rust,ignore
let state = state.with_iceberg_integration(IcebergIntegrationOptions {
    file_io_codec: Some(Arc::new(MyFileIOCodec::new(/* application state */))),
    ..Default::default()
});
```

Register compatible codecs on coordinators and workers. With no custom codec,
scans retain the existing storage-properties encoding. Custom payloads use a
separate optional byte field; a receiver without a matching codec returns an error.
