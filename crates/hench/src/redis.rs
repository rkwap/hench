//! Redis connections: [`RedisConfig`] and [`RedisPool`].

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use ::redis::AsyncConnectionConfig;
use ::redis::aio::{ConnectionManager, ConnectionManagerConfig};
use ::redis::io::tcp::TcpSettings;
use ::redis::{ConnectionInfo, IntoConnectionInfo, RedisError, RedisResult};

use crate::error::{Error, Result};

mod sentinel;

use sentinel::{SentinelTarget, SentinelUrl};

/// The connection type handed out by [`RedisPool`]: an auto-reconnecting multiplexed
/// connection. Use it with any `redis` command API (`redis::cmd`, `redis::pipe`,
/// `redis::AsyncCommands`, scripts).
pub type RedisConnection = ConnectionManager;

/// How to reach Redis and how patiently.
///
/// Every TCP connection a pool opens has `TCP_NODELAY` on, so that the kernel sends each
/// command at once rather than waiting for the acknowledgement of an earlier one (Nagle's
/// algorithm), which slows a shared connection down when Redis is not on the same machine.
///
/// Its `Debug` output shows the URL without its password.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisConfig {
    /// Redis URL: `redis://[user:password@]host[:port][/db]`, `rediss://...` (TLS, needs the
    /// `tls` feature; `rediss://host:port/#insecure` does not verify the server's certificate,
    /// see [`TlsConfig::danger_accept_invalid_certs`]),
    /// `redis+unix:///path/to/redis.sock[?db=N&pass=...]`, `unix:///path/to/redis.sock`, or,
    /// for the primary of a service monitored by Redis Sentinel,
    /// `redis+sentinel://[[user]:password@]host[:port][,host[:port]...]/service[/db]` (the
    /// sentinels' addresses, port 26379 by default; the credentials and the database are the
    /// primary's; `?sentinel_username=...&sentinel_password=...` are the sentinels'). The
    /// sentinels are asked for the primary when a connection opens and again after a failover.
    pub url: String,
    /// Timeout for establishing a connection. Default 3 s.
    pub connection_timeout: Duration,
    /// Timeout for a reply to a (non-blocking) command. Default 3 s.
    pub response_timeout: Duration,
    /// How many times a lost connection is re-established before an operation fails.
    /// Default 6.
    pub reconnect_retries: usize,
    /// Smallest delay between reconnection attempts (the delay grows exponentially).
    /// Default 100 ms.
    pub reconnect_min_delay: Duration,
    /// Largest delay between reconnection attempts. Default 2 s.
    pub reconnect_max_delay: Duration,
    /// Number of shared multiplexed connections [`RedisPool::conn`] hands out in turn.
    /// Default 1; a few more (say one per 50 processors) raise the throughput of a process
    /// running many short jobs, whose single connection's encoding and decoding then runs on
    /// one core at a time. At least 1, at most [`RedisConfig::MAX_SHARED_CONNECTIONS`].
    pub shared_connections: usize,
    /// Certificates for `rediss://` connections (default: trust the system's root
    /// certificates, no client certificate).
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
    /// Where the URL comes from, for diagnostics.
    origin: UrlOrigin,
}

/// Where a [`RedisConfig`]'s URL comes from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum UrlOrigin {
    /// Given by the program.
    #[default]
    Given,
    /// Read from this environment variable.
    Variable(String),
    /// The default URL: no variable was set.
    Default,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RedisConfig");
        // Never the password.
        debug
            .field("url", &self.redacted_url())
            .field("connection_timeout", &self.connection_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("reconnect_retries", &self.reconnect_retries)
            .field("reconnect_min_delay", &self.reconnect_min_delay)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("shared_connections", &self.shared_connections);
        #[cfg(feature = "tls")]
        debug.field("tls", &self.tls);
        debug.finish()
    }
}

/// Certificates for `rediss://` connections (feature `tls`), like Sidekiq's `ssl_params`.
///
/// ```
/// # fn demo(ca_pem: Vec<u8>, cert_pem: Vec<u8>, key_pem: Vec<u8>) {
/// use hench::{RedisConfig, TlsConfig};
///
/// let config = RedisConfig::new("rediss://redis.internal:6380")
///     .tls(TlsConfig::new().root_cert(ca_pem).client_identity(cert_pem, key_pem));
/// # let _ = config; }
/// ```
///
/// A server whose certificate cannot be verified (a self-signed certificate naming another
/// host, as some managed Redis services have, such as Heroku's) needs an explicit opt-out:
/// [`danger_accept_invalid_certs`](Self::danger_accept_invalid_certs) (Sidekiq's
/// `ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE }`), or the `#insecure` fragment of
/// the URL (`rediss://host:port/#insecure`, which also works for the `hench` command).
///
/// TLS goes through rustls with its `ring` crypto provider. When hench creates a `rediss://`
/// client and the process has no default rustls provider yet, it installs `ring` as the
/// default: rustls would otherwise pick one from its cargo features, and panic on the first
/// connection when a dependency enabled a second one (such as aws-lc-rs, rustls's own
/// default). To use another provider, install it before connecting, e.g.
/// `rustls::crypto::aws_lc_rs::default_provider().install_default()`.
#[cfg(feature = "tls")]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    root_cert: Option<Vec<u8>>,
    client_identity: Option<(Vec<u8>, Vec<u8>)>,
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Default settings: system root certificates, no client certificate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Trusts the root certificate(s) in `pem` (PEM encoded) instead of the system's.
    #[must_use]
    pub fn root_cert(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_cert = Some(pem.into());
        self
    }

    /// Authenticates with this client certificate and private key (both PEM encoded), for
    /// servers that require mutual TLS.
    #[must_use]
    pub fn client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_identity = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Does not verify the server's certificate at all: neither who signed it nor the names
    /// it is valid for (OpenSSL's `VERIFY_NONE`). The connection is still encrypted, but
    /// anyone between this process and Redis can pose as the server and read and change the
    /// jobs. For servers with a self-signed certificate that cannot be pinned with
    /// [`root_cert`](Self::root_cert) (one that is also a CA, or names another host). A
    /// warning is logged when a pool uses it. Same as the `#insecure` fragment of the URL.
    #[must_use]
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Verifies who signed the server's certificate but not that it is valid for the host
    /// the URL names (for a server reached through an address its certificate does not
    /// list). Anyone holding a certificate from a trusted authority can then pose as the
    /// server. A warning is logged when a pool uses it.
    #[must_use]
    pub fn danger_accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.accept_invalid_hostnames = accept;
        self
    }

    /// A client for `info` with these settings.
    fn client(&self, info: ConnectionInfo) -> RedisResult<::redis::Client> {
        let client = ::redis::Client::build_with_tls(info, self.certificates())?;
        if !self.accept_invalid_certs && !self.accept_invalid_hostnames {
            return Ok(client);
        }
        let info = client.get_connection_info().clone();
        let mut addr = match info.addr().clone() {
            ::redis::ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                tls_params,
            } => ::redis::ConnectionAddr::TcpTls {
                host,
                port,
                insecure: insecure || self.accept_invalid_certs,
                tls_params,
            },
            other => other,
        };
        if self.accept_invalid_hostnames {
            addr.set_danger_accept_invalid_hostnames(true);
        }
        ::redis::Client::open(info.set_addr(addr))
    }

    fn certificates(&self) -> ::redis::TlsCertificates {
        ::redis::TlsCertificates {
            client_tls: self
                .client_identity
                .as_ref()
                .map(|(cert, key)| ::redis::ClientTlsConfig {
                    client_cert: cert.clone(),
                    client_key: key.clone(),
                }),
            root_cert: self.root_cert.clone(),
        }
    }
}

#[cfg(feature = "tls")]
impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print certificate or key material.
        f.debug_struct("TlsConfig")
            .field("root_cert", &self.root_cert.is_some())
            .field("client_identity", &self.client_identity.is_some())
            .field("accept_invalid_certs", &self.accept_invalid_certs)
            .field("accept_invalid_hostnames", &self.accept_invalid_hostnames)
            .finish()
    }
}

impl RedisConfig {
    /// URL used when nothing else is configured.
    pub const DEFAULT_URL: &'static str = "redis://127.0.0.1:6379";

    /// The largest number of [shared connections](RedisConfig::shared_connections).
    pub const MAX_SHARED_CONNECTIONS: usize = 64;

    /// Configuration for `url` with default timeouts.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connection_timeout: Duration::from_secs(3),
            response_timeout: Duration::from_secs(3),
            reconnect_retries: 6,
            reconnect_min_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(2),
            shared_connections: 1,
            #[cfg(feature = "tls")]
            tls: None,
            origin: UrlOrigin::Given,
        }
    }

    /// Configuration from the environment, like Sidekiq: if `REDIS_PROVIDER` is set it names
    /// the variable holding the URL; otherwise `REDIS_URL` is used; otherwise
    /// [`RedisConfig::DEFAULT_URL`].
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`RedisConfig::from_env`] with a custom variable lookup.
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let from_provider = match lookup("REDIS_PROVIDER") {
            Some(provider) if provider.contains(':') => {
                return Err(Error::Config(
                    "REDIS_PROVIDER must name the environment variable that holds the Redis \
                     URL (for example REDIS_PROVIDER=REDISTOGO_URL), not contain a URL"
                        .to_owned(),
                ));
            }
            Some(provider) if !provider.is_empty() => lookup(&provider)
                .filter(|url| !url.is_empty())
                .map(|url| (url, provider)),
            _ => None,
        };
        let (url, origin) = match from_provider.or_else(|| {
            lookup("REDIS_URL")
                .filter(|url| !url.is_empty())
                .map(|url| (url, "REDIS_URL".to_owned()))
        }) {
            Some((url, variable)) => (url, UrlOrigin::Variable(variable)),
            None => (Self::DEFAULT_URL.to_owned(), UrlOrigin::Default),
        };
        Ok(Self {
            origin,
            ..Self::new(url)
        })
    }

    /// Where the URL comes from, in words (empty when the program gave it).
    fn origin_note(&self) -> String {
        match &self.origin {
            UrlOrigin::Given => String::new(),
            UrlOrigin::Variable(name) => format!("from {name}"),
            UrlOrigin::Default => {
                "the default: neither REDIS_URL nor REDIS_PROVIDER is set".to_owned()
            }
        }
    }

    /// Sets the connection timeout.
    #[must_use]
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Sets the response timeout for non-blocking commands.
    #[must_use]
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Sets how many reconnection attempts are made before an operation fails.
    #[must_use]
    pub fn reconnect_retries(mut self, retries: usize) -> Self {
        self.reconnect_retries = retries;
        self
    }

    /// Sets how many shared multiplexed connections the pool hands out in turn (see
    /// [`RedisConfig::shared_connections`]); clamped to `1..=`[`RedisConfig::MAX_SHARED_CONNECTIONS`].
    #[must_use]
    pub fn shared_connections(mut self, connections: usize) -> Self {
        self.shared_connections = connections.clamp(1, Self::MAX_SHARED_CONNECTIONS);
        self
    }

    /// Sets the certificates used for `rediss://` connections.
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets the smallest and largest delay between reconnection attempts.
    #[must_use]
    pub fn reconnect_delays(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min_delay = min;
        self.reconnect_max_delay = max.max(min);
        self
    }

    /// Parses and validates the URL. The connection info names the client library `hench`
    /// and turns `TCP_NODELAY` on.
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        if sentinel::is_sentinel_url(&self.url) {
            return Err(Error::Config(format!(
                "`{}` names a service monitored by Redis Sentinel, not one server: its pool \
                 asks the sentinels for the primary",
                self.redacted_url()
            )));
        }
        let parsed = ::redis::parse_redis_url(&self.url).ok_or_else(|| {
            Error::Config(format!(
                "invalid Redis URL `{}`: expected redis://, rediss://, redis+unix://, unix:// \
                 or redis+sentinel://",
                redact_url(&self.url)
            ))
        })?;
        let scheme = parsed.scheme().to_owned();
        let info = parsed.into_connection_info().map_err(|error| {
            let hint = if scheme == "rediss" || scheme == "valkeys" {
                " (TLS URLs need hench's `tls` feature)"
            } else {
                ""
            };
            Error::Config(format!(
                "invalid Redis URL `{}`: {error}{hint}",
                redact_url(&self.url)
            ))
        })?;
        let settings = info
            .redis_settings()
            .clone()
            .set_lib_name("hench", env!("CARGO_PKG_VERSION"));
        Ok(info
            .set_redis_settings(settings)
            .set_tcp_settings(tcp_settings()))
    }

    /// The URL with any password replaced by `REDACTED`, safe for logs.
    pub fn redacted_url(&self) -> String {
        redact_url(&self.url)
    }

    fn manager_config(&self, response_timeout: Duration) -> ConnectionManagerConfig {
        ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connection_timeout))
            .set_response_timeout(Some(response_timeout))
            .set_number_of_retries(self.reconnect_retries)
            .set_min_delay(self.reconnect_min_delay)
            .set_max_delay(self.reconnect_max_delay)
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self::new(Self::DEFAULT_URL)
    }
}

/// The TCP settings of every connection to Redis: `TCP_NODELAY` on, as Redis clients usually
/// have it (redis-rs leaves it off). Commands are small writes, and a process's processors
/// share a multiplexed connection: with Nagle's algorithm the kernel would hold a command back
/// while an earlier one is unacknowledged, which with latency between the process and Redis
/// adds up to a round trip to many commands.
fn tcp_settings() -> TcpSettings {
    TcpSettings::default().set_nodelay(true)
}

/// The client for the one server `config` names.
fn server_client(config: &RedisConfig) -> Result<::redis::Client> {
    let info = config.connection_info()?;
    #[cfg(feature = "tls")]
    if matches!(info.addr(), ::redis::ConnectionAddr::TcpTls { .. }) {
        install_crypto_provider();
    }
    #[cfg(feature = "tls")]
    let client = match &config.tls {
        Some(tls) => tls.client(info),
        None => ::redis::Client::open(info),
    };
    #[cfg(not(feature = "tls"))]
    let client = ::redis::Client::open(info);
    let client = client.map_err(|error| {
        Error::Config(format!(
            "invalid Redis configuration for `{}`: {error}",
            config.redacted_url()
        ))
    })?;
    #[cfg(feature = "tls")]
    warn_if_unverified(config, &client);
    Ok(client)
}

/// Makes sure rustls has a process-wide crypto provider before redis-rs builds a TLS
/// configuration, which it does on rustls's process default at every `rediss://` connection.
/// Without one installed, rustls picks the provider its cargo features name, and panics when
/// they name none or two: an application whose other dependencies enable rustls's default
/// provider (aws-lc-rs) next to the `ring` of hench's `tls` feature has two. hench installs
/// `ring` unless the application installed a provider first.
#[cfg(feature = "tls")]
fn install_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }
    // Another thread may install one first; either way the process then has a provider.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
    {
        tracing::debug!("installed rustls's ring crypto provider as the process default");
    }
}

/// The Sentinel-monitored service `config` names.
fn sentinel_target(config: &RedisConfig) -> Result<SentinelTarget> {
    let invalid = |reason: String| {
        Error::Config(format!(
            "invalid Redis Sentinel URL `{}`: {reason}",
            config.redacted_url()
        ))
    };
    #[cfg(feature = "tls")]
    if config.tls.is_some() {
        return Err(invalid(
            "TLS through Redis Sentinel is not supported yet".to_owned(),
        ));
    }
    let url = SentinelUrl::parse(&config.url).map_err(invalid)?;
    SentinelTarget::new(
        &url,
        config.connection_timeout,
        config.response_timeout,
        env!("CARGO_PKG_VERSION"),
    )
    .map_err(|error| invalid(error.to_string()))
}

/// Warns when a pool does not fully verify its server's TLS certificate
/// ([`TlsConfig::danger_accept_invalid_certs`] or the URL's `#insecure`,
/// [`TlsConfig::danger_accept_invalid_hostnames`]).
#[cfg(feature = "tls")]
fn warn_if_unverified(config: &RedisConfig, client: &::redis::Client) {
    let insecure = matches!(
        client.get_connection_info().addr(),
        ::redis::ConnectionAddr::TcpTls { insecure: true, .. }
    );
    let hostnames = config
        .tls
        .as_ref()
        .is_some_and(|tls| tls.accept_invalid_hostnames);
    if insecure {
        tracing::warn!(
            url = %config.redacted_url(),
            "the Redis server's TLS certificate is not verified: anyone between this process \
             and Redis can pose as the server"
        );
    } else if hostnames {
        tracing::warn!(
            url = %config.redacted_url(),
            "the Redis server's TLS certificate is not checked against the host name: anyone \
             holding a certificate from a trusted authority can pose as the server"
        );
    }
}

/// Replaces passwords in a Redis URL (userinfo or `pass=` query parameter) with `REDACTED`.
fn redact_url(url: &str) -> String {
    const REDACTED: &str = "REDACTED";
    if sentinel::is_sentinel_url(url) {
        return sentinel::redact(url);
    }
    let Some(mut parsed) = ::redis::parse_redis_url(url) else {
        // An unparseable URL may still contain a secret.
        return if url.contains('@') || url.contains("pass") {
            REDACTED.to_owned()
        } else {
            url.to_owned()
        };
    };
    if parsed.password().is_some() && parsed.set_password(Some(REDACTED)).is_err() {
        return REDACTED.to_owned();
    }
    if parsed.query_pairs().any(|(key, _)| key == "pass") {
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(key, value)| {
                let value = if key == "pass" {
                    REDACTED.to_owned()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();
        parsed.query_pairs_mut().clear().extend_pairs(pairs);
    }
    parsed.to_string()
}

/// A handle to Redis shared by every part of hench. Cheap to clone.
///
/// A pool hands out two kinds of connections:
///
/// * [`RedisPool::conn`]: a **shared** multiplexed connection (a [`ConnectionManager`],
///   which reconnects automatically): the only one by default, or each of
///   [`RedisConfig::shared_connections`] in turn. Cheap to clone; use it for every non-blocking
///   command. Call `conn()` for each operation
///   instead of caching the returned value, so that [`RedisPool::reconnect`] takes effect.
/// * [`RedisPool::blocking_conn`]: a **dedicated** connection for blocking commands (`BRPOP`,
///   `BLMOVE`). Its response timeout is longer than the block time. Never run a blocking
///   command on the shared connection: it would stall every other user of the connection and
///   trip the shared response timeout.
///
/// [`RedisPool::with_retry`] runs an operation on the shared connection and retries it once
/// after a failover or a dropped connection, like Sidekiq.
#[derive(Clone)]
pub struct RedisPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: RedisConfig,
    backend: Backend,
}

enum Backend {
    /// Not connected to any Redis (fake and inline testing clients).
    Detached,
    Redis(Box<Connected>),
}

struct Connected {
    target: Target,
    /// The shared connections, each created on first use and dropped by `reconnect`.
    shared: Vec<RwLock<Option<ConnectionManager>>>,
    /// The next shared connection handed out (modulo their number).
    next: AtomicUsize,
}

/// The server a pool talks to.
enum Target {
    /// The one the URL names.
    Server(Box<::redis::Client>),
    /// The primary of a service monitored by Redis Sentinel.
    Sentinel(Box<SentinelTarget>),
}

impl Connected {
    /// A client for the server to talk to: the one the URL names, or the primary the
    /// sentinels name (asking them if `fresh`, or if no primary is known since the last
    /// failover; the shared connections are dropped when it moved).
    async fn client(&self, fresh: bool) -> RedisResult<::redis::Client> {
        match &self.target {
            Target::Server(client) => Ok(client.as_ref().clone()),
            Target::Sentinel(sentinel) => {
                if !fresh && let Some(client) = sentinel.known() {
                    return Ok(client);
                }
                // Boxed: the lookup's future is deep, and every caller of `conn` would embed
                // it.
                let (client, moved) = Box::pin(sentinel.lookup()).await?;
                if moved {
                    self.drop_shared();
                }
                Ok(client)
            }
        }
    }

    /// Drops the shared connections.
    fn drop_shared(&self) {
        for slot in &self.shared {
            *slot.write().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }

    /// `true` behind Redis Sentinel.
    fn is_sentinel(&self) -> bool {
        matches!(self.target, Target::Sentinel(_))
    }
}

impl RedisPool {
    /// Connects to `url` with default settings.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::from_config(RedisConfig::new(url)).await
    }

    /// Connects using `config`: validates the URL and pings the server, so configuration and
    /// connectivity problems surface immediately.
    ///
    /// Logs the URL it connects to (without its password) and where it comes from, at info
    /// level. An unreachable server is tried again as configured
    /// ([`reconnect_retries`](RedisConfig::reconnect_retries), with backoff); errors that no
    /// retry can fix, such as a wrong password or a database that does not exist, fail at once.
    pub async fn from_config(config: RedisConfig) -> Result<Self> {
        let pool = Self::lazy(config)?;
        pool.first_connection().await?;
        tracing::debug!(url = %pool.config().redacted_url(), "connected to Redis");
        Ok(pool)
    }

    /// The first connection of [`RedisPool::from_config`]: attempts on connections of their
    /// own, retried while Redis cannot be reached (or is loading its data), then the shared
    /// connection.
    async fn first_connection(&self) -> Result<()> {
        let Backend::Redis(connected) = &self.inner.backend else {
            return Err(detached_error());
        };
        let config = &self.inner.config;
        let origin = config.origin_note();
        if origin.is_empty() {
            tracing::info!(url = %config.redacted_url(), "connecting to Redis");
        } else {
            tracing::info!(url = %config.redacted_url(), %origin, "connecting to Redis");
        }
        let settings = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(config.connection_timeout))
            .set_response_timeout(Some(config.response_timeout));
        let mut delay = config.reconnect_min_delay;
        for attempt in 0..=config.reconnect_retries {
            let attempted = async {
                let mut conn = connected
                    .client(true)
                    .await?
                    .get_multiplexed_async_connection_with_config(&settings)
                    .await?;
                ::redis::cmd("PING").query_async::<String>(&mut conn).await
            }
            .await;
            match attempted {
                Ok(_) => break,
                Err(error) if attempt == config.reconnect_retries || !is_transient(&error) => {
                    return Err(error.into());
                }
                Err(error) => {
                    tracing::debug!(%error, attempt, "could not connect to Redis; trying again");
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(config.reconnect_max_delay);
                }
            }
        }
        self.ping().await
    }

    /// Validates `config` but does not connect; the shared connection is established on
    /// first use. Does not need a Tokio runtime.
    pub fn lazy(config: RedisConfig) -> Result<Self> {
        let target = if sentinel::is_sentinel_url(&config.url) {
            Target::Sentinel(Box::new(sentinel_target(&config)?))
        } else {
            Target::Server(Box::new(server_client(&config)?))
        };
        let shared = config
            .shared_connections
            .clamp(1, RedisConfig::MAX_SHARED_CONNECTIONS);
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                backend: Backend::Redis(Box::new(Connected {
                    target,
                    shared: (0..shared).map(|_| RwLock::new(None)).collect(),
                    next: AtomicUsize::new(0),
                })),
            }),
        })
    }

    /// A pool that is not connected to any Redis: every connection request fails with
    /// [`Error::Config`]. Used by the fake and inline testing clients.
    pub fn detached() -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config: RedisConfig::new(""),
                backend: Backend::Detached,
            }),
        }
    }

    /// `true` for pools created with [`RedisPool::detached`].
    pub fn is_detached(&self) -> bool {
        matches!(self.inner.backend, Backend::Detached)
    }

    /// The configuration this pool was created with.
    pub fn config(&self) -> &RedisConfig {
        &self.inner.config
    }

    /// A shared, auto-reconnecting multiplexed connection: the only one by default, or the
    /// next of [`RedisConfig::shared_connections`].
    ///
    /// This does not wait for Redis: the connection is (re-)established by the first command
    /// sent on it, and every concurrent caller shares that single attempt. Connection problems
    /// therefore surface as errors of the commands. (Behind Redis Sentinel, a new shared
    /// connection waits for the sentinels to name the primary, unless it is known.)
    pub async fn conn(&self) -> Result<RedisConnection> {
        let Backend::Redis(connected) = &self.inner.backend else {
            return Err(detached_error());
        };
        let count = connected.shared.len();
        let index = if count > 1 {
            connected.next.fetch_add(1, Ordering::Relaxed) % count
        } else {
            0
        };
        // There is always at least one slot (see `RedisConfig::shared_connections`).
        let Some(slot) = connected.shared.get(index) else {
            return Err(detached_error());
        };
        if let Some(manager) = slot.read().unwrap_or_else(PoisonError::into_inner).as_ref() {
            return Ok(manager.clone());
        }
        let client = connected.client(false).await?;
        let mut shared = slot.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(manager) = shared.as_ref() {
            return Ok(manager.clone());
        }
        let config = &self.inner.config;
        let manager = ConnectionManager::new_lazy_with_config(
            client,
            config.manager_config(config.response_timeout),
        )?;
        *shared = Some(manager.clone());
        Ok(manager)
    }

    /// A new dedicated connection for blocking commands that block for at most `block`. Its
    /// response timeout is `block` plus a margin of at least one second (or the configured
    /// response timeout, if larger), so a blocking command never times out on the client side
    /// before Redis answers.
    ///
    /// Unlike [`RedisPool::conn`] this connects immediately, retrying as configured
    /// (`reconnect_retries`), so it can take several seconds to fail while Redis is down: race
    /// it against your shutdown signal. The connection reconnects by itself afterwards.
    pub async fn blocking_conn(&self, block: Duration) -> Result<RedisConnection> {
        let Backend::Redis(connected) = &self.inner.backend else {
            return Err(detached_error());
        };
        // Behind Redis Sentinel, a new dedicated connection asks the sentinels again: one is
        // opened after an error, which may come from a failover.
        let client = connected.client(true).await?;
        let config = &self.inner.config;
        let timeout = block.saturating_add(config.response_timeout.max(Duration::from_secs(1)));
        Ok(ConnectionManager::new_with_config(client, config.manager_config(timeout)).await?)
    }

    /// Sends `PING` on the shared connection.
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        let _: String = ::redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }

    /// Drops the shared connections; the next [`RedisPool::conn`] calls open new sockets. Used
    /// after a failover (`READONLY` replies) to reach the new primary. Behind Redis Sentinel,
    /// the next connection asks the sentinels for the primary again.
    pub fn reconnect(&self) {
        if let Backend::Redis(connected) = &self.inner.backend {
            connected.drop_shared();
            if let Target::Sentinel(sentinel) = &connected.target {
                sentinel.forget();
            }
        }
    }

    /// Runs `operation` on the shared connection, retrying it once after a failover error
    /// (`READONLY`, `NOREPLICAS`, `UNBLOCKED`; the shared connection is replaced first) or a
    /// dropped connection, like Sidekiq. Other errors, including timeouts, are returned as is.
    /// Behind Redis Sentinel, the sentinels are asked for the primary again before the retry
    /// (a dropped or refused connection may mean that the primary moved too).
    ///
    /// The operation may run twice, so it should be idempotent or tolerate duplicates (a job
    /// pushed twice is acceptable: jobs are delivered at least once).
    pub async fn with_retry<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut(RedisConnection) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let conn = self.conn().await?;
        let error = match operation(conn).await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        match retry_action(&error) {
            None => Err(error.into()),
            Some(action) => {
                tracing::warn!(%error, "Redis operation failed; retrying once");
                let sentinel = match &self.inner.backend {
                    Backend::Redis(connected) => connected.is_sentinel(),
                    Backend::Detached => false,
                };
                if action == RetryAction::Reconnect || sentinel {
                    self.reconnect();
                }
                let conn = self.conn().await?;
                Ok(operation(conn).await?)
            }
        }
    }
}

impl fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner.backend {
            Backend::Detached => f.write_str("RedisPool(detached)"),
            Backend::Redis(_) => f
                .debug_tuple("RedisPool")
                .field(&self.inner.config.redacted_url())
                .finish(),
        }
    }
}

fn detached_error() -> Error {
    Error::Config(
        "this pool is not connected to Redis (it belongs to a fake or inline testing client)"
            .to_owned(),
    )
}

/// What to do about a failed Redis operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryAction {
    /// Retry on the same (auto-reconnecting) connection.
    Retry,
    /// Replace the shared connection, then retry.
    Reconnect,
}

/// Error codes after which Sidekiq reopens its socket and retries: the server became a
/// replica (failover), lacks replicas, or a blocking command was force-unblocked.
const FAILOVER_CODES: [&str; 3] = ["READONLY", "NOREPLICAS", "UNBLOCKED"];

/// Error codes with which a server refuses commands for a while (loading its data, busy, its
/// primary down): worth trying again when connecting.
const TRANSIENT_CODES: [&str; 5] = ["LOADING", "BUSY", "MASTERDOWN", "TRYAGAIN", "CLUSTERDOWN"];

/// `true` for connection errors that trying again may fix: the server cannot be reached or
/// is not ready. Others (authentication, an unknown database, a client configuration) cannot.
fn is_transient(error: &RedisError) -> bool {
    error.is_io_error()
        || error.is_timeout()
        || error.is_connection_dropped()
        || error.is_connection_refusal()
        || error
            .code()
            .is_some_and(|code| TRANSIENT_CODES.contains(&code))
}

/// Classifies a Redis error for [`RedisPool::with_retry`].
pub(crate) fn retry_action(error: &RedisError) -> Option<RetryAction> {
    let failover = error
        .code()
        .is_some_and(|code| FAILOVER_CODES.contains(&code))
        || error.clone().into_server_errors().is_some_and(|errors| {
            errors
                .iter()
                .any(|(_, server_error)| FAILOVER_CODES.contains(&server_error.code()))
        });
    if failover {
        Some(RetryAction::Reconnect)
    } else if error.is_connection_dropped() || error.is_connection_refusal() {
        Some(RetryAction::Retry)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::redis::{ConnectionAddr, ErrorKind, make_extension_error};

    #[test]
    fn tcp_urls() {
        let info = RedisConfig::new("redis://:secret@example.com:6380/2")
            .connection_info()
            .unwrap();
        assert_eq!(
            info.addr(),
            &ConnectionAddr::Tcp("example.com".to_owned(), 6380)
        );
        assert_eq!(info.redis_settings().db(), 2);
        assert_eq!(info.redis_settings().password(), Some("secret"));
        assert_eq!(info.redis_settings().lib_name(), Some("hench"));
        assert!(info.tcp_settings().nodelay());

        let info = RedisConfig::new("redis://user:pw@127.0.0.1")
            .connection_info()
            .unwrap();
        assert_eq!(
            info.addr(),
            &ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6379)
        );
        assert_eq!(info.redis_settings().username(), Some("user"));
        assert_eq!(info.redis_settings().db(), 0);
    }

    /// Benchmark review: with 1 ms between a process and Redis, Nagle's algorithm on the
    /// shared connection made `bin/sidekiqload` run 40% slower. Every client a pool builds
    /// keeps `TCP_NODELAY` on, including the TLS clients rebuilt for the `danger_` settings.
    #[test]
    fn clients_turn_nagle_off() {
        let client = server_client(&RedisConfig::new("redis://example.com:6380/1")).unwrap();
        assert!(client.get_connection_info().tcp_settings().nodelay());
        #[cfg(feature = "tls")]
        for tls in [
            TlsConfig::new(),
            TlsConfig::new().danger_accept_invalid_certs(true),
            TlsConfig::new().danger_accept_invalid_hostnames(true),
        ] {
            let config = RedisConfig::new("rediss://example.com:6380").tls(tls);
            let client = server_client(&config).unwrap();
            assert!(client.get_connection_info().tcp_settings().nodelay());
        }
    }

    #[test]
    fn unix_socket_urls() {
        let info = RedisConfig::new("redis+unix:///tmp/redis.sock?db=3")
            .connection_info()
            .unwrap();
        assert_eq!(info.addr(), &ConnectionAddr::Unix("/tmp/redis.sock".into()));
        assert_eq!(info.redis_settings().db(), 3);

        let info = RedisConfig::new("unix:///var/run/redis/redis.sock?pass=pw")
            .connection_info()
            .unwrap();
        assert_eq!(
            info.addr(),
            &ConnectionAddr::Unix("/var/run/redis/redis.sock".into())
        );
        assert_eq!(info.redis_settings().password(), Some("pw"));
    }

    #[test]
    fn tls_urls() {
        let result = RedisConfig::new("rediss://example.com:6380").connection_info();
        if cfg!(feature = "tls") {
            assert!(matches!(
                result.unwrap().addr(),
                ConnectionAddr::TcpTls { host, port: 6380, .. } if host == "example.com"
            ));
        } else {
            let message = result.unwrap_err().to_string();
            assert!(message.contains("tls"), "{message}");
        }
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_settings() {
        let tls = TlsConfig::new()
            .root_cert(b"-----BEGIN CERTIFICATE-----secret".to_vec())
            .client_identity(b"cert".to_vec(), b"PRIVATE KEY".to_vec());
        let debug = format!("{:?}", RedisConfig::new("rediss://h").tls(tls.clone()));
        assert!(
            !debug.contains("PRIVATE") && !debug.contains("secret"),
            "{debug}"
        );
        assert!(debug.contains("client_identity: true"), "{debug}");
        // Certificates need a TLS URL.
        let result = RedisPool::lazy(RedisConfig::new("redis://h").tls(TlsConfig::new()));
        assert!(matches!(result, Err(Error::Config(_))));
        // Garbage certificates are rejected when the client is built.
        let result = RedisPool::lazy(RedisConfig::new("rediss://h").tls(tls));
        assert!(matches!(result, Err(Error::Config(_))));
    }

    /// Release review round 4 (release-surface-2): `rediss://` connections panicked when
    /// rustls could not pick a crypto provider from its features (another dependency enabling
    /// aws-lc-rs next to hench's ring). A process can install its default provider once, so
    /// each of these tests runs alone in a child process of the test binary.
    #[cfg(feature = "tls")]
    mod crypto_provider {
        use super::*;
        use rustls::crypto::CryptoProvider;

        /// Names the test the child process runs.
        const CHILD: &str = "HENCH_CRYPTO_PROVIDER_TEST";

        /// `true` in the child process running test `name` (of this module); otherwise runs
        /// that test alone in a child process, checks that it passed and returns `false`.
        fn alone(name: &str) -> bool {
            let module = module_path!().split_once("::").map_or("", |(_, path)| path);
            let test = format!("{module}::{name}");
            if std::env::var(CHILD).is_ok_and(|child| child == test) {
                return true;
            }
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test, "--nocapture", "--test-threads=1"])
                .env(CHILD, &test)
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "{test} failed in its process:\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(stdout.contains("1 passed"), "{test} did not run:\n{stdout}");
            false
        }

        /// The provider is installed when the client is created, before redis-rs builds any
        /// TLS configuration (here none: the pool does not connect), so rustls never has to
        /// pick one from its features.
        #[test]
        fn a_rediss_client_installs_ring_unless_a_provider_is_installed() {
            if !alone("a_rediss_client_installs_ring_unless_a_provider_is_installed") {
                return;
            }
            assert!(CryptoProvider::get_default().is_none());
            // Plain connections leave rustls alone.
            RedisPool::lazy(RedisConfig::new("redis://127.0.0.1:6379")).unwrap();
            assert!(CryptoProvider::get_default().is_none());
            RedisPool::lazy(RedisConfig::new("rediss://127.0.0.1:6380")).unwrap();
            let installed = CryptoProvider::get_default().expect("a provider is installed");
            assert_eq!(
                installed.cipher_suites,
                rustls::crypto::ring::default_provider().cipher_suites
            );
        }

        /// An application that installed its own provider keeps it, and the TLS connection
        /// uses it.
        #[test]
        fn a_provider_installed_by_the_application_is_kept() {
            if !alone("a_provider_installed_by_the_application_is_kept") {
                return;
            }
            // `ring` with a single cipher suite, to tell it apart from hench's.
            let mut mine = rustls::crypto::ring::default_provider();
            mine.cipher_suites.truncate(1);
            let suites = mine.cipher_suites.clone();
            mine.install_default().unwrap();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let result = runtime.block_on(async {
                // A server that does not speak TLS: the handshake fails once rustls has built
                // the client's configuration on the process default.
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                tokio::spawn(async move {
                    while let Ok((socket, _)) = listener.accept().await {
                        drop(socket);
                    }
                });
                let config = RedisConfig::new(format!("rediss://127.0.0.1:{port}"))
                    .connection_timeout(Duration::from_secs(2))
                    .reconnect_retries(0);
                RedisPool::from_config(config).await
            });
            assert!(matches!(result, Err(Error::Redis(_))), "{result:?}");
            assert_eq!(CryptoProvider::get_default().unwrap().cipher_suites, suites);
        }
    }

    #[test]
    fn invalid_urls() {
        for url in ["http://localhost", "not a url", "", "redis://0.0.0.0"] {
            assert!(
                matches!(
                    RedisConfig::new(url).connection_info(),
                    Err(Error::Config(_))
                ),
                "{url} should be rejected"
            );
        }
        assert!(matches!(
            RedisConfig::new("redis://localhost/notanumber").connection_info(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            RedisPool::lazy(RedisConfig::new("ftp://x")),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn configurations_never_show_their_password() {
        let config = RedisConfig::new("redis://app:s3cr3tRedisPass@redis.internal:6379/0");
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
        assert!(
            debug.contains("redis://app:REDACTED@redis.internal:6379/0"),
            "{debug}"
        );
        assert!(debug.contains("connection_timeout: 3s"), "{debug}");
        let debug = format!(
            "{:?}",
            RedisConfig::new("unix:///tmp/r.sock?db=1&pass=s3cr3t")
        );
        assert!(!debug.contains("s3cr3t"), "{debug}");
    }

    #[test]
    fn the_origin_of_the_url_is_known() {
        let lookup = |vars: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                vars.iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).to_owned())
            }
        };
        let note = |vars| {
            RedisConfig::from_lookup(lookup(vars))
                .unwrap()
                .origin_note()
        };
        assert_eq!(
            note(&[]),
            "the default: neither REDIS_URL nor REDIS_PROVIDER is set"
        );
        assert_eq!(note(&[("REDIS_URL", "redis://a")]), "from REDIS_URL");
        assert_eq!(
            note(&[("REDIS_PROVIDER", "MY_URL"), ("MY_URL", "redis://b")]),
            "from MY_URL"
        );
        assert_eq!(RedisConfig::new("redis://c").origin_note(), "");
    }

    #[test]
    fn transient_connection_errors() {
        let refused = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert!(is_transient(&refused));
        let loading = make_extension_error("LOADING".to_owned(), None);
        assert!(is_transient(&loading));
        let auth = RedisError::from((ErrorKind::AuthenticationFailed, "nope"));
        assert!(!is_transient(&auth));
        let noauth = make_extension_error("NOAUTH".to_owned(), None);
        assert!(!is_transient(&noauth));
    }

    #[test]
    fn passwords_are_redacted() {
        assert_eq!(
            redact_url("redis://:secret@example.com:6380/2"),
            "redis://:REDACTED@example.com:6380/2"
        );
        assert_eq!(
            redact_url("redis://user:secret@example.com"),
            "redis://user:REDACTED@example.com"
        );
        assert_eq!(redact_url("redis://example.com"), "redis://example.com");
        assert_eq!(
            redact_url("redis+unix:///tmp/r.sock?db=1&pass=secret"),
            "redis+unix:///tmp/r.sock?db=1&pass=REDACTED"
        );
        assert_eq!(redact_url("garbage://user:secret@host"), "REDACTED");
        let message = RedisConfig::new("http://user:secret@host")
            .connection_info()
            .unwrap_err()
            .to_string();
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn config_builders() {
        let config = RedisConfig::new("redis://h")
            .connection_timeout(Duration::from_millis(5))
            .response_timeout(Duration::from_millis(6))
            .reconnect_retries(1)
            .reconnect_delays(Duration::from_millis(10), Duration::from_millis(1));
        assert_eq!(config.connection_timeout, Duration::from_millis(5));
        assert_eq!(config.response_timeout, Duration::from_millis(6));
        assert_eq!(config.reconnect_retries, 1);
        assert_eq!(config.reconnect_min_delay, Duration::from_millis(10));
        assert_eq!(config.reconnect_max_delay, Duration::from_millis(10));
        assert_eq!(RedisConfig::default().url, RedisConfig::DEFAULT_URL);
        assert_eq!(RedisConfig::default().shared_connections, 1);
        assert_eq!(config.clone().shared_connections(4).shared_connections, 4);
        assert_eq!(config.clone().shared_connections(0).shared_connections, 1);
        assert_eq!(
            config.shared_connections(1_000).shared_connections,
            RedisConfig::MAX_SHARED_CONNECTIONS
        );
    }

    #[test]
    fn urls_from_the_environment() {
        fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
            let vars: Vec<(String, String)> = vars
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            move |name| {
                vars.iter()
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.clone())
            }
        }
        let url = |vars: &[(&str, &str)]| RedisConfig::from_lookup(lookup(vars)).map(|c| c.url);
        assert_eq!(url(&[]).unwrap(), RedisConfig::DEFAULT_URL);
        assert_eq!(url(&[("REDIS_URL", "redis://a")]).unwrap(), "redis://a");
        assert_eq!(
            url(&[
                ("REDIS_PROVIDER", "MY_URL"),
                ("MY_URL", "redis://provided"),
                ("REDIS_URL", "redis://a")
            ])
            .unwrap(),
            "redis://provided"
        );
        // A provider naming an unset variable falls back to REDIS_URL.
        assert_eq!(
            url(&[("REDIS_PROVIDER", "MISSING"), ("REDIS_URL", "redis://a")]).unwrap(),
            "redis://a"
        );
        assert_eq!(url(&[("REDIS_URL", "")]).unwrap(), RedisConfig::DEFAULT_URL);
        assert!(matches!(
            url(&[("REDIS_PROVIDER", "redis://oops")]),
            Err(Error::Config(ref m)) if m.contains("REDIS_PROVIDER")
        ));
        assert!(RedisConfig::from_env().is_ok());
    }

    #[test]
    fn lazy_pools_need_no_runtime() {
        let pool = RedisPool::lazy(RedisConfig::new("redis://127.0.0.1:1")).unwrap();
        assert!(!pool.is_detached());
        assert!(format!("{pool:?}").contains("127.0.0.1"));
        pool.reconnect();
    }

    #[tokio::test]
    async fn detached_pools_refuse_connections() {
        let pool = RedisPool::detached();
        assert!(pool.is_detached());
        assert!(matches!(pool.conn().await, Err(Error::Config(_))));
        assert!(matches!(
            pool.blocking_conn(Duration::from_secs(1)).await,
            Err(Error::Config(_))
        ));
        assert!(matches!(pool.ping().await, Err(Error::Config(_))));
        assert_eq!(format!("{pool:?}"), "RedisPool(detached)");
    }

    #[test]
    fn failover_errors_reconnect() {
        for code in FAILOVER_CODES {
            let error = make_extension_error(code.to_owned(), Some("detail".to_owned()));
            assert_eq!(retry_action(&error), Some(RetryAction::Reconnect), "{code}");
        }
        let other = make_extension_error("WRONGTYPE".to_owned(), None);
        assert_eq!(retry_action(&other), None);
    }

    #[test]
    fn dropped_connections_retry() {
        let dropped = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert_eq!(retry_action(&dropped), Some(RetryAction::Retry));
        let refused = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert_eq!(retry_action(&refused), Some(RetryAction::Retry));
        let timeout = RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert_eq!(retry_action(&timeout), None);
        let client = RedisError::from((ErrorKind::Client, "nope"));
        assert_eq!(retry_action(&client), None);
    }

    #[tokio::test]
    async fn unreachable_servers_surface_redis_errors() {
        let config = RedisConfig::new("redis://127.0.0.1:1")
            .connection_timeout(Duration::from_millis(200))
            .reconnect_retries(0);
        let pool = RedisPool::lazy(config).unwrap();
        let result = pool
            .with_retry(|mut conn| async move {
                ::redis::cmd("PING").query_async::<String>(&mut conn).await
            })
            .await;
        assert!(matches!(result, Err(Error::Redis(_))));
        assert!(matches!(pool.ping().await, Err(Error::Redis(_))));
    }
}
