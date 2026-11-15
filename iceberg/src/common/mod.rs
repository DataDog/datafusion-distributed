mod error;

pub(crate) use error::{df_err, iceberg_err};

mod file_partition;
pub(crate) mod reader;
pub(crate) use file_partition::file_partition;
