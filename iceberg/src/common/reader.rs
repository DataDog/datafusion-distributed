use datafusion::execution::TaskContext;
use iceberg::arrow::{ArrowReader, ArrowReaderBuilder};
use iceberg::io::FileIO;
use iceberg_datafusion::IcebergDataFusionConfig;

/// Use upstream session options for a process-local reader. Clones share
/// its delete loader; worker decoding constructs its own reader and runtime.
pub(crate) fn scan_reader(
    file_io: FileIO,
    runtime: iceberg::Runtime,
    context: &TaskContext,
) -> ArrowReader {
    let config = context
        .session_config()
        .options()
        .extensions
        .get::<IcebergDataFusionConfig>()
        .cloned()
        .unwrap_or_default();
    config
        .execution
        .configure_reader(
            ArrowReaderBuilder::new(file_io, runtime),
            context.session_config().batch_size(),
        )
        .build()
}
