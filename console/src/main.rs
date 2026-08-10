mod app;
mod connector;
mod input;
mod state;
mod ui;
mod worker;

use app::App;
use color_eyre::eyre::{Report, bail, eyre};
use connector::{DEFAULT_CONNECT_TIMEOUT, LogicalOrigin, WorkerConnector};
use crossterm::event::{self, Event};
use ratatui::DefaultTerminal;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tonic::transport::Certificate;
use url::Url;

#[derive(StructOpt)]
#[structopt(
    name = "datafusion-distributed-console",
    about = "Console for monitoring DataFusion distributed workers"
)]
struct Args {
    /// Address of a worker to connect to for auto-discovery: a port (`9001`), a `host:port`, or
    /// a full URL. This is the address the console dials, which with --worker-origin need not be
    /// the name the worker answers to.
    /// The console calls GetClusterWorkers on this worker to discover the full cluster.
    seed: String,

    /// Origin the workers answer to, e.g. `https://workers.example.com`. TLS, SNI and the gRPC
    /// authority come from this URL, and the workers discovered through the seed are moved onto
    /// its scheme and port. Omit it to talk plaintext to the addresses workers report.
    #[structopt(long = "worker-origin")]
    worker_origin: Option<Url>,

    /// Path to a PEM-encoded CA certificate that signs the worker certificates, for a cluster
    /// whose CA is not a public one. Requires --worker-origin.
    #[structopt(long = "ca-cert", parse(from_os_str))]
    ca_cert: Option<PathBuf>,

    /// Polling interval in milliseconds
    #[structopt(long = "poll-interval", default_value = "1000")]
    poll_interval: u64,

    /// Budget for opening a worker connection, TCP connect and TLS handshake together, in
    /// milliseconds [default: 1000]
    #[structopt(long = "connect-timeout")]
    connect_timeout: Option<u64>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let args = Args::from_args();

    let connector = build_connector(&args)?;
    // The seed goes through the same normalization as a discovered worker, so the address the
    // console is started with and the ones the cluster reports are dialed the same way.
    let seed_url = connector.worker_url(&seed_address(&args.seed))?;

    let poll_interval = Duration::from_millis(args.poll_interval);
    let mut app = App::new(seed_url, connector);

    let mut terminal = ratatui::init();
    terminal.clear()?;

    let result = run_app(&mut terminal, &mut app, poll_interval).await;

    ratatui::restore();

    result
}

/// Expands a bare port, which is how the console has always been started, into an authority.
fn seed_address(seed: &str) -> String {
    match seed.parse::<u16>() {
        Ok(port) => format!("localhost:{port}"),
        Err(_) => seed.to_string(),
    }
}

/// Builds the transport policy shared by discovery and every per-worker connection.
fn build_connector(args: &Args) -> color_eyre::Result<WorkerConnector> {
    let connect_timeout = args
        .connect_timeout
        .map_or(DEFAULT_CONNECT_TIMEOUT, Duration::from_millis);
    let connector = WorkerConnector::new(connect_timeout);

    let Some(origin) = args.worker_origin.clone() else {
        // Accepting a CA and then ignoring it would leave the console talking plaintext to a
        // cluster the operator believes it is verifying.
        if args.ca_cert.is_some() {
            bail!("--ca-cert only applies together with --worker-origin");
        }
        return Ok(connector);
    };

    let ca_certificate = match &args.ca_cert {
        Some(path) => Some(Certificate::from_pem(std::fs::read(path).map_err(|e| {
            eyre!("failed to read CA certificate {}: {e}", path.display())
        })?)),
        None => None,
    };

    let origin = LogicalOrigin::new(origin, ca_certificate).map_err(Report::msg)?;
    Ok(connector.with_origin(origin))
}

async fn run_app(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    poll_interval: Duration,
) -> color_eyre::Result<()> {
    let mut last_poll = Instant::now();

    loop {
        if last_poll.elapsed() >= poll_interval {
            app.tick().await;
            last_poll = Instant::now();
        }

        terminal.draw(|frame| ui::render(frame, app))?;

        // Check for keyboard input (16ms timeout ~ 60fps responsiveness)
        if event::poll(Duration::from_millis(16))? {
            if let Event::Key(key) = event::read()? {
                input::handle_key_event(app, key);
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(worker_origin: Option<Url>, ca_cert: Option<PathBuf>) -> Args {
        Args {
            seed: "9001".to_string(),
            worker_origin,
            ca_cert,
            poll_interval: 1000,
            connect_timeout: None,
        }
    }

    #[test]
    fn bare_seed_port_keeps_legacy_cli_compatibility() {
        assert_eq!(seed_address("9001"), "localhost:9001");
        assert_eq!(seed_address("127.0.0.1:9001"), "127.0.0.1:9001");
    }

    #[test]
    fn ca_certificate_requires_worker_origin() {
        let args = args(None, Some(PathBuf::from("unused.pem")));
        let error = build_connector(&args).expect_err("CA without origin must be rejected");
        assert!(error.to_string().contains("--worker-origin"));
    }
}
