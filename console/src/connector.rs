use datafusion_distributed::grpc::ObservabilityServiceClient;
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::net::TcpStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use url::{Host, ParseError, Url};

/// Anything that can go wrong opening a worker connection.
///
/// Dialing now spans URL handling, TLS setup and the transport itself, and `tonic` does not let
/// callers build its transport error, so the failures are boxed behind their `Display`, which is
/// all the console does with them.
pub(crate) type ConnectError = Box<dyn std::error::Error + Send + Sync>;

/// Budget for opening a worker connection: the TCP connect plus, when the worker is behind TLS,
/// the handshake.
///
/// The console dials on the UI thread's tick, so an unreachable worker must fail rather than
/// stall the whole cluster view; a second is long enough for a handshake across a region and
/// short enough to notice.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(1000);

/// The name a cluster answers to, as opposed to the address the console dials.
///
/// A console may reach workers through addresses that are not present on their certificates. This
/// holds the name half: the scheme, host and port used for TLS, SNI and the gRPC `:authority`, plus
/// the CA that signs the workers if it is not a public one. Every physical worker address must
/// still be reachable from the console process.
#[derive(Clone, Debug)]
pub(crate) struct LogicalOrigin {
    url: Url,
    ca_certificate: Option<Certificate>,
}

impl LogicalOrigin {
    /// Builds an origin, rejecting URLs that cannot name a gRPC endpoint.
    ///
    /// The origin is only ever used for its scheme, host and port, so a URL missing any of them
    /// would fail later at dial time with a much less obvious message.
    pub(crate) fn new(url: Url, ca_certificate: Option<Certificate>) -> Result<Self, String> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!(
                "worker origin must be http or https, got `{}` in {url}",
                url.scheme()
            ));
        }
        if !url.has_host() {
            return Err(format!("worker origin {url} has no host"));
        }
        if url.scheme() != "https" && ca_certificate.is_some() {
            return Err("a CA certificate requires an https worker origin".to_string());
        }
        Ok(Self {
            url,
            ca_certificate,
        })
    }
}

/// Opens gRPC channels to the workers the console monitors.
///
/// Per-worker polling ([`crate::worker::WorkerConn`]) and cluster discovery
/// ([`crate::worker::discover_cluster_workers`]) both dial workers, and both must agree on how a
/// worker URL becomes a connection. Holding that decision in one cheap, cloneable value keeps the
/// two call sites from drifting apart when the transport changes.
#[derive(Clone, Debug)]
pub(crate) struct WorkerConnector {
    /// `None` dials whatever address the worker reports, in the clear.
    origin: Option<LogicalOrigin>,
    connect_timeout: Duration,
}

#[cfg(test)]
impl Default for WorkerConnector {
    fn default() -> Self {
        Self::new(DEFAULT_CONNECT_TIMEOUT)
    }
}

impl WorkerConnector {
    /// Builds a connector that talks plaintext to the addresses workers report.
    pub(crate) fn new(connect_timeout: Duration) -> Self {
        Self {
            origin: None,
            connect_timeout,
        }
    }

    /// Makes every connection present `origin` on the wire while still dialing the physical
    /// address it was handed.
    pub(crate) fn with_origin(self, origin: LogicalOrigin) -> Self {
        Self {
            origin: Some(origin),
            ..self
        }
    }

    /// Opens a channel to a worker at `target`.
    ///
    /// `target` is the address to dial. Where an origin is configured, it — not `target` — decides
    /// the scheme, the certificate name and the `:authority` header, so the console can reach a
    /// worker through a tunnel or a rewritten address without the worker's certificate having to
    /// mention it.
    ///
    /// Connecting alone does not prove the worker serves the observability API, so callers that
    /// care about liveness follow up with their own `Ping`.
    pub(crate) async fn connect(
        &self,
        target: &Url,
    ) -> Result<ObservabilityServiceClient<Channel>, ConnectError> {
        let (host, port) = dial_target(target)
            .ok_or_else(|| format!("worker address {target} has no host and port to dial"))?;

        let channel = self
            .endpoint(target)?
            .connect_with_connector(tower::service_fn(move |_| {
                // The endpoint URI names the worker; this closure decides where the bytes go. The
                // URI it is handed is deliberately ignored so the two can disagree.
                let address = (host.clone(), port);
                async move {
                    let stream = TcpStream::connect(address).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;

        Ok(ObservabilityServiceClient::new(channel))
    }

    /// Describes the worker to `tonic`: the URI it reports as `:authority` and the TLS it expects.
    fn endpoint(&self, target: &Url) -> Result<Endpoint, ConnectError> {
        let named = self.origin.as_ref().map_or(target, |origin| &origin.url);

        let endpoint =
            Endpoint::from_shared(named.to_string())?.connect_timeout(self.connect_timeout);

        // `tonic` only wraps the connector in TLS when the endpoint URI is https, so the origin's
        // scheme is what turns TLS on.
        if named.scheme() != "https" {
            return Ok(endpoint);
        }

        let mut tls = ClientTlsConfig::new();
        // SNI needs a DNS name. An IP-literal origin has none, and `tonic` already derives the
        // right `ServerName` from the endpoint URI in that case.
        if let Some(Host::Domain(domain)) = named.host() {
            tls = tls.domain_name(domain);
        }
        if let Some(ca) = self.origin.as_ref().and_then(|o| o.ca_certificate.clone()) {
            tls = tls.ca_certificate(ca);
        } else {
            tls = tls.with_enabled_roots();
        }

        Ok(endpoint.tls_config(tls)?)
    }

    /// Turns a worker URL reported by `GetClusterWorkers` into a URL this connector can dial.
    ///
    /// Workers describe themselves through their own `WorkerResolver`, so the reported form is
    /// whatever that implementation chose and is not guaranteed to be directly dialable.
    ///
    /// With an origin configured the report is moved onto the origin's scheme, because a worker
    /// that only knows its in-cluster plaintext address cannot report the https front door the
    /// console reaches it through. A port the worker states explicitly is kept — it is the one
    /// piece of the report that is per-worker and cannot be reconstructed — and otherwise the
    /// origin's port applies. A reported port that matches its scheme's default (`:80` on http)
    /// is indistinguishable from no port at all and takes the origin's port.
    pub(crate) fn worker_url(&self, reported: &str) -> Result<Url, ParseError> {
        let reported = parse_reported(reported, self.scheme())?;

        let Some(origin) = &self.origin else {
            return Ok(reported);
        };

        // Rebuilt as text rather than mutated in place: `host_str` already brackets IPv6
        // literals, which is exactly the form a URL wants them in.
        let host = reported.host_str().ok_or(ParseError::EmptyHost)?;
        let port = reported
            .port()
            .or_else(|| origin.url.port_or_known_default());

        let mut rewritten = format!("{}://{host}", origin.url.scheme());
        if let Some(port) = port {
            rewritten.push_str(&format!(":{port}"));
        }
        Url::parse(&rewritten)
    }

    /// Scheme reported URLs are assumed to use when they do not say.
    fn scheme(&self) -> &str {
        self.origin
            .as_ref()
            .map_or("http", |origin| origin.url.scheme())
    }
}

/// Parses a worker's self-report, which may be a full URL or a bare authority.
///
/// `Url::parse` reads `localhost:9001` as the scheme `localhost` with the path `9001`, and rejects
/// `10.0.0.1:9001` and `[::1]:9001` outright, so anything that comes back without a host is
/// retried as an authority under `scheme`.
fn parse_reported(reported: &str, scheme: &str) -> Result<Url, ParseError> {
    match Url::parse(reported) {
        Ok(url) if url.has_host() => Ok(url),
        Ok(_) | Err(ParseError::RelativeUrlWithoutBase) => {
            Url::parse(&format!("{scheme}://{reported}"))
        }
        Err(e) => Err(e),
    }
}

/// Splits a URL into the host and port to hand to the TCP stack.
///
/// The host is unbracketed: URLs write IPv6 literals as `[::1]` but the socket layer wants `::1`.
fn dial_target(url: &Url) -> Option<(String, u16)> {
    let host = match url.host()? {
        Host::Domain(domain) => domain.to_string(),
        Host::Ipv4(addr) => addr.to_string(),
        Host::Ipv6(addr) => addr.to_string(),
    };
    Some((host, url.port_or_known_default()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::runtime::SpawnedTask;
    use std::error::Error;
    use tokio::net::TcpListener;

    fn origin(url: &str) -> LogicalOrigin {
        LogicalOrigin::new(Url::parse(url).expect("valid origin URL"), None).expect("usable origin")
    }

    fn logical(url: &str) -> WorkerConnector {
        WorkerConnector::default().with_origin(origin(url))
    }

    #[test]
    fn worker_url_keeps_a_reported_url_as_is() -> Result<(), Box<dyn Error>> {
        let connector = WorkerConnector::default();
        let url = connector.worker_url("http://10.0.0.1:9001")?;
        assert_eq!(url.as_str(), "http://10.0.0.1:9001/");
        Ok(())
    }

    #[test]
    fn worker_url_rejects_a_value_that_is_not_a_url() {
        let connector = WorkerConnector::default();
        assert!(connector.worker_url("not a url").is_err());
    }

    #[test]
    fn worker_url_reads_a_bare_authority_as_a_host_and_port() -> Result<(), Box<dyn Error>> {
        let connector = WorkerConnector::default();
        // Without the authority fallback `Url::parse` reads this as the scheme `localhost`.
        assert_eq!(
            connector.worker_url("localhost:9001")?.as_str(),
            "http://localhost:9001/"
        );
        assert_eq!(
            connector.worker_url("10.0.0.1:9001")?.as_str(),
            "http://10.0.0.1:9001/"
        );
        Ok(())
    }

    #[test]
    fn worker_url_moves_a_report_onto_the_logical_scheme() -> Result<(), Box<dyn Error>> {
        let connector = logical("https://workers.example.com");
        assert_eq!(
            connector
                .worker_url("http://worker-3.internal:9001")?
                .as_str(),
            "https://worker-3.internal:9001/"
        );
        Ok(())
    }

    #[test]
    fn worker_url_takes_the_logical_port_when_the_worker_states_none() -> Result<(), Box<dyn Error>>
    {
        let connector = logical("https://workers.example.com:8443");
        assert_eq!(
            connector.worker_url("worker-3.internal")?.as_str(),
            "https://worker-3.internal:8443/"
        );
        // A default port for the reported scheme is erased by `Url` and cannot be told apart
        // from no port, so it takes the logical port too.
        assert_eq!(
            connector
                .worker_url("http://worker-3.internal:80")?
                .as_str(),
            "https://worker-3.internal:8443/"
        );
        Ok(())
    }

    #[test]
    fn worker_url_keeps_a_port_the_worker_states_explicitly() -> Result<(), Box<dyn Error>> {
        let connector = logical("https://workers.example.com:8443");
        assert_eq!(
            connector
                .worker_url("http://worker-3.internal:9001")?
                .as_str(),
            "https://worker-3.internal:9001/"
        );
        Ok(())
    }

    #[test]
    fn worker_url_defaults_to_the_logical_schemes_port() -> Result<(), Box<dyn Error>> {
        // The origin states no port, so https' own default is what the workers get. `Url` writes
        // a scheme's default port as no port at all, so the dial target is what to assert on.
        let connector = logical("https://workers.example.com");
        let url = connector.worker_url("worker-3.internal")?;
        assert_eq!(url.as_str(), "https://worker-3.internal/");
        assert_eq!(
            dial_target(&url).ok_or("expected a dialable address")?,
            ("worker-3.internal".to_string(), 443)
        );
        Ok(())
    }

    #[test]
    fn worker_url_handles_ipv4_and_ipv6_literals() -> Result<(), Box<dyn Error>> {
        let connector = logical("https://workers.example.com:8443");

        assert_eq!(
            connector.worker_url("http://10.0.0.1:9001")?.as_str(),
            "https://10.0.0.1:9001/"
        );
        assert_eq!(
            connector.worker_url("10.0.0.1")?.as_str(),
            "https://10.0.0.1:8443/"
        );
        // IPv6 literals stay bracketed in URL form, whether reported as a URL or an authority.
        assert_eq!(
            connector.worker_url("http://[2001:db8::1]:9001")?.as_str(),
            "https://[2001:db8::1]:9001/"
        );
        assert_eq!(
            connector.worker_url("[::1]:9001")?.as_str(),
            "https://[::1]:9001/"
        );
        assert_eq!(
            connector.worker_url("[::1]")?.as_str(),
            "https://[::1]:8443/"
        );
        Ok(())
    }

    #[test]
    fn logical_origin_rejects_a_scheme_it_cannot_dial() {
        let url = Url::parse("grpc://workers.example.com").expect("valid URL");
        assert!(LogicalOrigin::new(url, None).is_err());
    }

    #[test]
    fn logical_origin_rejects_ca_for_plaintext() {
        let url = Url::parse("http://workers.example.com").expect("valid URL");
        let ca = Certificate::from_pem(b"not parsed until a TLS connection is attempted");
        assert!(LogicalOrigin::new(url, Some(ca)).is_err());
    }

    #[test]
    fn dial_target_unbrackets_ipv6_and_fills_in_the_default_port() -> Result<(), Box<dyn Error>> {
        let (host, port) = dial_target(&Url::parse("https://[2001:db8::1]:9001")?)
            .ok_or("expected a dialable address")?;
        assert_eq!((host.as_str(), port), ("2001:db8::1", 9001));

        let (host, port) = dial_target(&Url::parse("https://workers.example.com")?)
            .ok_or("expected an address")?;
        assert_eq!((host.as_str(), port), ("workers.example.com", 443));
        Ok(())
    }

    #[tokio::test]
    async fn connect_fails_when_nothing_is_listening() -> Result<(), Box<dyn Error>> {
        // Binding and dropping a listener yields a port that is very unlikely to be reused.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        drop(listener);

        let connector = WorkerConnector::default();
        let url = Url::parse(&format!("http://127.0.0.1:{port}"))?;
        assert!(connector.connect(&url).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn connect_dials_the_physical_address_and_not_the_origin() -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        // Dropping the accepted socket ends the handshake, so `connect` fails fast either way;
        // the connection arriving at all is what proves the dial ignored the origin's host.
        let accepted = SpawnedTask::spawn(async move { listener.accept().await.map(|_| ()) });

        // `.invalid` never resolves (RFC 2606), so nothing can reach this listener by name.
        let connector = logical(&format!("http://workers.invalid:{port}"));
        let target = Url::parse(&format!("http://127.0.0.1:{port}"))?;
        let _ = connector.connect(&target).await;

        accepted.await??;
        Ok(())
    }
}
