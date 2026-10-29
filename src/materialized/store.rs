use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::{Result, config_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use futures::TryStreamExt;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

/// Immutable storage used for intermediate batches and sealed work-unit journals.
///
/// Keys are relative, library-generated paths beneath a query UUID. `put` must
/// reject overwrites and return only once the complete bytes are readable. `get`
/// must return an error for missing or truncated objects, never partial success.
/// Implementations must bound buffering and provide orphan expiry after process
/// failure. The library requests cleanup on normal completion and cancellation;
/// process termination still requires an application-owned janitor or TTL.
#[async_trait]
pub trait IntermediateResultStore: Debug + Send + Sync {
    /// Atomically create an immutable object. No partial object may be readable.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;
    /// Read one complete immutable object.
    async fn get(&self, key: &str) -> Result<Bytes>;
    /// Remove all objects beneath exactly this query's UUID namespace.
    /// Must be idempotent. Applications may call this again to clean late writes.
    async fn remove_query(&self, query: Uuid) -> Result<()>;
}

/// Shared-store adapter, usable with local files, object storage, or test memory.
///
/// Each process must register an adapter pointing at the same store and prefix.
/// Backend credentials stay in the application; they are never serialized in plans.
#[derive(Debug)]
pub struct ObjectStoreIntermediateStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreIntermediateStore {
    /// Store intermediate objects below a dedicated application-owned prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }

    fn path(&self, key: &str) -> Result<Path> {
        let relative = Path::parse(key)?;
        Ok(relative
            .parts()
            .fold(self.prefix.clone(), |path, part| path.join(part)))
    }
}

#[async_trait]
impl IntermediateResultStore for ObjectStoreIntermediateStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.store
            .put_opts(
                &self.path(key)?,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        Ok(self.store.get(&self.path(key)?).await?.bytes().await?)
    }

    async fn remove_query(&self, query: Uuid) -> Result<()> {
        let prefix = self.prefix.clone().join(query.to_string());
        let mut objects = self.store.list(Some(&prefix));
        while let Some(object) = objects.try_next().await? {
            self.store.delete(&object.location).await?;
        }
        Ok(())
    }
}

/// Session extension installed independently on every participating process.
#[derive(Clone, Debug)]
pub struct MaterializedStore {
    pub(crate) store: Arc<dyn IntermediateResultStore>,
    pub(crate) max_segment_bytes: usize,
}

impl MaterializedStore {
    /// Use the store with a 64 MiB maximum encoded batch or journal item.
    pub fn new(store: Arc<dyn IntermediateResultStore>) -> Self {
        Self {
            store,
            max_segment_bytes: 64 * 1024 * 1024,
        }
    }

    /// Bound the size of an individual encoded object (not the whole query).
    pub fn with_max_segment_bytes(mut self, bytes: usize) -> Result<Self> {
        if bytes == 0 {
            return exec_err!("Materialized segment limit must be nonzero");
        }
        self.max_segment_bytes = bytes;
        Ok(self)
    }

    pub(crate) fn from_ctx(ctx: &TaskContext) -> Result<Arc<Self>> {
        ctx.session_config().get_extension::<Self>().ok_or_else(|| {
            config_datafusion_err!(
                "MaterializedStore must be registered on the coordinator and every worker"
            )
        })
    }

    pub(crate) async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        if bytes.len() > self.max_segment_bytes {
            return exec_err!(
                "Materialized object exceeds the {} byte segment limit",
                self.max_segment_bytes
            );
        }
        self.store.put(key, bytes).await
    }
}
