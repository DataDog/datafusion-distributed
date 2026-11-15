mod codec;

pub use codec::EagerIcebergCodec;

mod file_assignment;
pub(crate) use file_assignment::assign_files;
