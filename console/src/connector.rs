use datafusion_distributed::grpc::ObservabilityServiceClient;
use tonic::transport::{Channel, Error};
use url::Url;

/// Opens gRPC channels to the workers the console monitors.
///
/// Per-worker polling ([`crate::worker::WorkerConn`]) and cluster discovery
/// ([`crate::worker::discover_cluster_workers`]) both dial workers, and both must agree on how a
/// worker URL becomes a connection. Holding that decision in one cheap, cloneable value keeps the
/// two call sites from drifting apart when the transport changes.
#[derive(Clone, Debug, Default)]
pub(crate) struct WorkerConnector;

impl WorkerConnector {
    /// Opens a channel to a worker.
    ///
    /// Connecting alone does not prove the worker serves the observability API, so callers that
    /// care about liveness follow up with their own `Ping`.
    pub(crate) async fn connect(
        &self,
        url: &Url,
    ) -> Result<ObservabilityServiceClient<Channel>, Error> {
        ObservabilityServiceClient::connect(url.to_string()).await
    }

    /// Turns a worker URL reported by `GetClusterWorkers` into a URL this connector can dial.
    ///
    /// Workers describe themselves through their own `WorkerResolver`, so the reported form is
    /// whatever that implementation chose and is not guaranteed to be directly dialable.
    pub(crate) fn worker_url(&self, reported: &str) -> Result<Url, url::ParseError> {
        Url::parse(reported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use tokio::net::TcpListener;

    #[test]
    fn worker_url_keeps_a_reported_url_as_is() -> Result<(), Box<dyn Error>> {
        let connector = WorkerConnector;
        let url = connector.worker_url("http://10.0.0.1:9001")?;
        assert_eq!(url.as_str(), "http://10.0.0.1:9001/");
        Ok(())
    }

    #[test]
    fn worker_url_rejects_a_value_that_is_not_a_url() {
        let connector = WorkerConnector;
        assert!(connector.worker_url("not a url").is_err());
    }

    #[tokio::test]
    async fn connect_fails_when_nothing_is_listening() -> Result<(), Box<dyn Error>> {
        // Binding and dropping a listener yields a port that is very unlikely to be reused.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener);

        let connector = WorkerConnector;
        let url = Url::parse(&format!("http://127.0.0.1:{port}"))?;
        assert!(connector.connect(&url).await.is_err());
        Ok(())
    }
}
