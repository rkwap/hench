//! What the benchmark binaries share. Each binary is a port of one of Sidekiq's benchmark
//! scripts (`bin/sidekiqload`, `bin/multi_queue_bench`, `bin/webload` in Sidekiq 8.1) and
//! takes the same environment variables, so that a run of either system can be read next to
//! the other; `benchmarks/README.md` has the results and how to reproduce them.

use std::io::IsTerminal;
use std::str::FromStr;
use std::time::Duration;

use hench::FetchStrategy;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Any error: a benchmark stops at the first one, with its message.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// How often the monitors read the queues' lengths, as Sidekiq's scripts do.
pub const MONITOR_INTERVAL: Duration = Duration::from_millis(200);

/// The environment variable `name` parsed, or `default` when it is not set.
pub fn env_or<T>(name: &str, default: T) -> Result<T, Error>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("{name}={value}: {error}").into()),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("{name}: {error}").into()),
    }
}

/// `FETCH`: `reliable` (the default, Hench's default) or `basic` (Sidekiq's open-source
/// fetch, a plain pop: what Sidekiq's scripts measure).
pub fn fetch_strategy() -> Result<FetchStrategy, Error> {
    match env_or("FETCH", "reliable".to_owned())?.as_str() {
        "reliable" => Ok(FetchStrategy::Reliable),
        "basic" => Ok(FetchStrategy::Basic),
        other => Err(format!("FETCH={other}: expected reliable or basic").into()),
    }
}

/// The name `FETCH` takes for `fetch`.
pub fn fetch_name(fetch: FetchStrategy) -> &'static str {
    match fetch {
        FetchStrategy::Basic => "basic",
        _ => "reliable",
    }
}

/// The first line each binary prints (Sidekiq's scripts print `RUBY_DESCRIPTION`).
pub fn describe() -> String {
    let build = if cfg!(debug_assertions) {
        "debug build: use --release"
    } else {
        "release build"
    };
    format!(
        "hench {} ({build}, {}-{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::ARCH,
        std::env::consts::OS
    )
}

/// Logs at `warn`, the level Sidekiq's scripts give their logger: Hench still logs every job,
/// and the filter drops the lines, as it would in production.
pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
}

/// This process's resident set size in KB, read with `ps` as Sidekiq's scripts read theirs
/// (0 when `ps` fails).
pub fn rss_kb() -> u64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// A client of toxiproxy's HTTP API, which `bin/sidekiqload` uses to delay Redis's replies
/// (`TOXIPROXY_URL`, else `http://127.0.0.1:8474`, as the toxiproxy gem). Its server,
/// `toxiproxy-server`, must be running.
pub struct Toxiproxy {
    /// `host:port` of the API.
    address: String,
}

impl Toxiproxy {
    /// The name the API gives a downstream latency toxic, which the toxiproxy gem keeps.
    const LATENCY_TOXIC: &'static str = "latency_downstream";

    /// The API named by `TOXIPROXY_URL`, else the default one.
    pub fn from_env() -> Self {
        let url =
            std::env::var("TOXIPROXY_URL").unwrap_or_else(|_| "http://127.0.0.1:8474".to_owned());
        let address = url
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        Self { address }
    }

    /// Creates the proxy `name` from `listen` to `upstream`, replacing one of that name.
    pub async fn populate(&self, name: &str, listen: &str, upstream: &str) -> Result<(), Error> {
        let proxies =
            json!([{ "name": name, "listen": listen, "upstream": upstream, "enabled": true }]);
        self.request("POST", "/populate", &proxies.to_string())
            .await
    }

    /// Delays every reply that goes through the proxy `name` by `latency_ms`.
    pub async fn add_latency(&self, name: &str, latency_ms: u64) -> Result<(), Error> {
        let toxic = json!({
            "name": Self::LATENCY_TOXIC,
            "type": "latency",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": { "latency": latency_ms, "jitter": 0 },
        });
        self.request(
            "POST",
            &format!("/proxies/{name}/toxics"),
            &toxic.to_string(),
        )
        .await
    }

    /// Removes the latency [`add_latency`](Self::add_latency) added.
    pub async fn remove_latency(&self, name: &str) -> Result<(), Error> {
        let path = format!("/proxies/{name}/toxics/{}", Self::LATENCY_TOXIC);
        self.request("DELETE", &path, "").await
    }

    /// One HTTP/1.1 request, its connection closed after the reply; any status but 2xx fails.
    async fn request(&self, method: &str, path: &str, body: &str) -> Result<(), Error> {
        let mut stream = TcpStream::connect(&self.address).await.map_err(|error| {
            format!(
                "toxiproxy at {}: {error} (start toxiproxy-server)",
                self.address
            )
        })?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.address,
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        let response = String::from_utf8_lossy(&response);
        let status_line = response.lines().next().unwrap_or("no reply");
        match status_line.split_whitespace().nth(1) {
            Some(status) if status.starts_with('2') => Ok(()),
            _ => Err(format!("toxiproxy: {method} {path}: {status_line}").into()),
        }
    }
}
