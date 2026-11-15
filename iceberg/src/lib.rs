//! Distributed Iceberg scan assignment and work delivery for upstream providers.
//!
//! Use `iceberg_datafusion` table providers directly. This crate installs their
//! scan-construction hook, distributed task delivery, and worker codecs. Upstream
//! catalog writes remain available without distributed planning; distributing
//! Iceberg write/commit operators is not supported.

mod common;
mod distributed_desired_task_count_handler;
mod distributed_scale_up_handler;
mod eager;
mod file_io_codec;
mod iceberg_ext;
mod lazy;
mod proto;
mod scan_factory;

pub use distributed_desired_task_count_handler::iceberg_desired_task_count;
pub use eager::EagerIcebergCodec;
pub use file_io_codec::IcebergFileIOCodec;
pub use iceberg_ext::IcebergExt;
pub use iceberg_ext::IcebergIntegrationOptions;
pub use lazy::FeedTaskSource;
pub use lazy::IcebergWorkUnitFeed;
pub use lazy::LazyIcebergCodec;

// re-export of iceberg-rust.
pub use iceberg;
