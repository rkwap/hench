//! Redis Sentinel: `redis+sentinel://` URLs and the lookup of a service's primary. See
//! `docs/DESIGN.md` §5.
//!
//! The sentinels are asked for the primary (`SENTINEL GET-MASTER-ADDR-BY-NAME`, in the order
//! the URL lists them) when a connection to it is opened, and again after a failover (a
//! `READONLY` reply, or a dropped or refused connection): the server they name is checked
//! (`ROLE`) before it is used.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ::redis::{
    AsyncConnectionConfig, Client, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisConnectionInfo, RedisError, RedisResult, Value,
};

/// The scheme of Sentinel URLs.
const SCHEME: &str = "redis+sentinel://";

/// The scheme of Sentinel URLs over TLS, which are refused.
const TLS_SCHEME: &str = "rediss+sentinel://";

/// The port of a sentinel when the URL gives none.
const DEFAULT_PORT: u16 = 26379;

/// Replaces passwords.
const REDACTED: &str = "REDACTED";

/// `true` for the URL of a service monitored by Redis Sentinel.
pub(super) fn is_sentinel_url(url: &str) -> bool {
    url.starts_with(SCHEME) || url.starts_with(TLS_SCHEME)
}

/// A parsed Sentinel URL:
/// `redis+sentinel://[[user]:password@]host[:port][,host[:port]...]/service[/db][?params]`,
/// where the credentials and the database are the primary's, and the parameters
/// `sentinel_username` and `sentinel_password` the sentinels' credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SentinelUrl {
    /// The sentinels, as `(host, port)`.
    sentinels: Vec<(String, u16)>,
    /// The name the sentinels monitor the primary under (`sentinel monitor <name> ...`).
    service: String,
    /// The database to select on the primary.
    db: i64,
    /// The primary's user name.
    username: Option<String>,
    /// The primary's password.
    password: Option<String>,
    /// The sentinels' user name.
    sentinel_username: Option<String>,
    /// The sentinels' password.
    sentinel_password: Option<String>,
}

impl SentinelUrl {
    /// Parses a Sentinel URL; the error says what is wrong with it.
    pub(super) fn parse(url: &str) -> Result<Self, String> {
        if url.starts_with(TLS_SCHEME) {
            return Err(
                "TLS through Redis Sentinel (`rediss+sentinel://`) is not supported yet".to_owned(),
            );
        }
        let rest = url
            .strip_prefix(SCHEME)
            .ok_or_else(|| format!("expected {SCHEME}"))?;
        if rest.contains('#') {
            return Err("a Sentinel URL takes no fragment".to_owned());
        }
        let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (authority, path) = rest.split_once('/').ok_or_else(|| {
            "expected the name of the service after the sentinels \
             (redis+sentinel://host:26379/mymaster)"
                .to_owned()
        })?;
        let (userinfo, hosts) = match authority.rsplit_once('@') {
            Some((userinfo, hosts)) => (Some(userinfo), hosts),
            None => (None, authority),
        };
        let (username, password) = match userinfo {
            None => (None, None),
            Some(userinfo) => {
                let (user, password) = match userinfo.split_once(':') {
                    Some((user, password)) => (user, Some(password)),
                    None => (userinfo, None),
                };
                (
                    non_empty(decode(user)?),
                    password.map(decode).transpose()?.and_then(non_empty),
                )
            }
        };
        let sentinels = hosts
            .split(',')
            .map(parse_host)
            .collect::<Result<Vec<_>, _>>()?;
        let mut segments = path.split('/');
        let service = decode(segments.next().unwrap_or_default())?;
        if service.is_empty() {
            return Err("expected the name of the service after the sentinels \
                        (redis+sentinel://host:26379/mymaster)"
                .to_owned());
        }
        let db = match segments.next() {
            None | Some("") => 0,
            Some(db) => db
                .parse::<i64>()
                .map_err(|_| format!("invalid database `{db}`"))?,
        };
        if segments.any(|segment| !segment.is_empty()) {
            return Err("expected /service or /service/db as the path".to_owned());
        }
        let mut parsed = Self {
            sentinels,
            service,
            db,
            username,
            password,
            sentinel_username: None,
            sentinel_password: None,
        };
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = non_empty(decode(value)?);
            match key {
                "sentinel_username" => parsed.sentinel_username = value,
                "sentinel_password" => parsed.sentinel_password = value,
                other => {
                    return Err(format!(
                        "unknown parameter `{other}` (expected sentinel_username or \
                         sentinel_password)"
                    ));
                }
            }
        }
        Ok(parsed)
    }

    /// The URL, with its passwords replaced by `REDACTED`.
    fn redacted(&self) -> String {
        let mut url = SCHEME.to_owned();
        if self.username.is_some() || self.password.is_some() {
            url.push_str(self.username.as_deref().unwrap_or_default());
            if self.password.is_some() {
                url.push(':');
                url.push_str(REDACTED);
            }
            url.push('@');
        }
        let hosts: Vec<String> = self
            .sentinels
            .iter()
            .map(|(host, port)| {
                if host.contains(':') {
                    format!("[{host}]:{port}")
                } else {
                    format!("{host}:{port}")
                }
            })
            .collect();
        url.push_str(&hosts.join(","));
        url.push('/');
        url.push_str(&self.service);
        if self.db != 0 {
            url.push_str(&format!("/{}", self.db));
        }
        let mut params = Vec::new();
        if let Some(username) = &self.sentinel_username {
            params.push(format!("sentinel_username={username}"));
        }
        if self.sentinel_password.is_some() {
            params.push(format!("sentinel_password={REDACTED}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        url
    }
}

/// A Sentinel URL with its passwords replaced by `REDACTED`, safe for logs.
pub(super) fn redact(url: &str) -> String {
    match SentinelUrl::parse(url) {
        Ok(parsed) => parsed.redacted(),
        // An unparseable URL may still contain a secret.
        Err(_) if url.contains('@') || url.contains("pass") => REDACTED.to_owned(),
        Err(_) => url.to_owned(),
    }
}

/// `host`, `host:port` or `[v6 address]:port`.
fn parse_host(host: &str) -> Result<(String, u16), String> {
    let port = |port: &str| {
        port.parse::<u16>()
            .map_err(|_| format!("invalid port `{port}` in `{host}`"))
    };
    let (name, port) = if let Some(rest) = host.strip_prefix('[') {
        let (address, after) = rest
            .split_once(']')
            .ok_or_else(|| format!("invalid address `{host}`"))?;
        let port = match after {
            "" => DEFAULT_PORT,
            after => port(
                after
                    .strip_prefix(':')
                    .ok_or_else(|| format!("invalid address `{host}`"))?,
            )?,
        };
        (address, port)
    } else {
        match host.rsplit_once(':') {
            Some((name, number)) => (name, port(number)?),
            None => (host, DEFAULT_PORT),
        }
    };
    if name.is_empty() {
        return Err(format!("missing a sentinel's host in `{host}`"));
    }
    Ok((name.to_owned(), port))
}

/// Decodes `%XX` escapes.
fn decode(text: &str) -> Result<String, String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut rest = text.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        if byte == b'%' {
            let escape = after
                .get(..2)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| format!("invalid escape in `{text}`"))?;
            bytes.push(escape);
            rest = after.get(2..).unwrap_or_default();
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    String::from_utf8(bytes).map_err(|_| format!("invalid UTF-8 in `{text}`"))
}

/// `None` for an empty string.
fn non_empty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}

/// The primary of a service monitored by Redis Sentinel, asked of the sentinels.
pub(super) struct SentinelTarget {
    /// The name of the service.
    service: String,
    /// The sentinels, in the order they are asked.
    sentinels: Vec<ConnectionInfo>,
    /// How to talk to the primary: credentials, database, client name.
    primary_settings: RedisConnectionInfo,
    /// Timeouts of the lookups' connections.
    settings: AsyncConnectionConfig,
    /// Held during a lookup: one at a time.
    lookup: tokio::sync::Mutex<()>,
    /// What the lookups found.
    primary: Mutex<Primary>,
}

/// The primary found by the lookups.
#[derive(Default)]
struct Primary {
    /// The primary to connect to; `None` once forgotten (after a failover error).
    client: Option<Client>,
    /// The address of the last primary found, kept when it is forgotten (to tell a move).
    addr: Option<ConnectionAddr>,
    /// The number of lookups done.
    lookups: u64,
}

/// What a sentinel says about the service.
enum Answer {
    /// It names this primary, which says it is one.
    Primary(Client),
    /// It does not monitor the service.
    Unknown,
    /// It names a server that is not a primary (yet, or any more): a failover is under way.
    Stale(ConnectionAddr),
}

impl SentinelTarget {
    /// A target for `url`; its lookups' connections time out after `connection_timeout` and
    /// `response_timeout`. Connects to nothing yet.
    pub(super) fn new(
        url: &SentinelUrl,
        connection_timeout: Duration,
        response_timeout: Duration,
        lib_version: &'static str,
    ) -> RedisResult<Self> {
        let mut sentinel_settings = RedisConnectionInfo::default();
        if let Some(username) = &url.sentinel_username {
            sentinel_settings = sentinel_settings.set_username(username);
        }
        if let Some(password) = &url.sentinel_password {
            sentinel_settings = sentinel_settings.set_password(password);
        }
        let sentinels = url
            .sentinels
            .iter()
            .map(|(host, port)| {
                Ok(ConnectionAddr::Tcp(host.clone(), *port)
                    .into_connection_info()?
                    .set_redis_settings(sentinel_settings.clone()))
            })
            .collect::<RedisResult<Vec<_>>>()?;
        let mut primary_settings = RedisConnectionInfo::default()
            .set_db(url.db)
            .set_lib_name("hench", lib_version);
        if let Some(username) = &url.username {
            primary_settings = primary_settings.set_username(username);
        }
        if let Some(password) = &url.password {
            primary_settings = primary_settings.set_password(password);
        }
        Ok(Self {
            service: url.service.clone(),
            sentinels,
            primary_settings,
            settings: AsyncConnectionConfig::new()
                .set_connection_timeout(Some(connection_timeout))
                .set_response_timeout(Some(response_timeout)),
            lookup: tokio::sync::Mutex::new(()),
            primary: Mutex::new(Primary::default()),
        })
    }

    /// The primary the last lookup found, unless forgotten since.
    pub(super) fn known(&self) -> Option<Client> {
        self.lock().client.clone()
    }

    /// Forgets the primary: the next connection asks the sentinels again.
    pub(super) fn forget(&self) {
        self.lock().client = None;
    }

    /// Asks the sentinels for the primary, and remembers it; callers that wait for a lookup
    /// under way take its result. Returns the primary, and `true` if it is not the one found
    /// before (a failover moved it).
    pub(super) async fn lookup(&self) -> RedisResult<(Client, bool)> {
        let before = self.lock().lookups;
        let _turn = self.lookup.lock().await;
        {
            let primary = self.lock();
            if primary.lookups != before
                && let Some(client) = &primary.client
            {
                return Ok((client.clone(), false));
            }
        }
        let client = self.ask().await?;
        let addr = client.get_connection_info().addr().clone();
        let mut primary = self.lock();
        let moved = match &primary.addr {
            None => {
                tracing::info!(service = %self.service, primary = %addr, "Redis Sentinel names the primary");
                false
            }
            Some(previous) if *previous != addr => {
                tracing::warn!(
                    service = %self.service,
                    primary = %addr,
                    previous = %previous,
                    "Redis Sentinel names a new primary (failover)"
                );
                true
            }
            Some(_) => false,
        };
        primary.client = Some(client.clone());
        primary.addr = Some(addr);
        primary.lookups = primary.lookups.wrapping_add(1);
        Ok((client, moved))
    }

    /// Asks the sentinels in turn until one names a server that says it is the primary.
    async fn ask(&self) -> RedisResult<Client> {
        let mut unknown = 0;
        let mut stale = None;
        let mut failure = None;
        for sentinel in &self.sentinels {
            match self.ask_one(sentinel).await {
                Ok(Answer::Primary(client)) => return Ok(client),
                Ok(Answer::Unknown) => unknown += 1,
                Ok(Answer::Stale(addr)) => stale = Some(addr),
                Err(error) => {
                    tracing::debug!(sentinel = %sentinel.addr(), %error, "a sentinel did not answer");
                    failure = Some(error);
                }
            }
        }
        if unknown == self.sentinels.len() {
            return Err(RedisError::from((
                ErrorKind::MasterNameNotFoundBySentinel,
                "no sentinel monitors this service",
                self.service.clone(),
            )));
        }
        match (stale, failure) {
            // Under way: try again later.
            (Some(addr), _) => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!(
                    "Redis Sentinel names {addr} as the primary of `{}`, which is not one \
                     (a failover is under way)",
                    self.service
                ),
            ))),
            (None, Some(error)) => Err(error),
            (None, None) => Err(RedisError::from((
                ErrorKind::MasterNameNotFoundBySentinel,
                "no sentinel monitors this service",
                self.service.clone(),
            ))),
        }
    }

    /// What `sentinel` says about the service; the server it names is asked for its `ROLE`.
    async fn ask_one(&self, sentinel: &ConnectionInfo) -> RedisResult<Answer> {
        let mut conn = Client::open(sentinel.clone())?
            .get_multiplexed_async_connection_with_config(&self.settings)
            .await?;
        let named: Option<(String, u16)> = ::redis::cmd("SENTINEL")
            .arg("GET-MASTER-ADDR-BY-NAME")
            .arg(&self.service)
            .query_async(&mut conn)
            .await?;
        let Some((host, port)) = named else {
            return Ok(Answer::Unknown);
        };
        let addr = ConnectionAddr::Tcp(host, port);
        let info = addr
            .clone()
            .into_connection_info()?
            .set_redis_settings(self.primary_settings.clone());
        let client = Client::open(info)?;
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&self.settings)
            .await?;
        let role: Vec<Value> = ::redis::cmd("ROLE").query_async(&mut conn).await?;
        let is_primary = role
            .into_iter()
            .next()
            .and_then(|role| ::redis::from_redis_value::<String>(role).ok())
            .is_some_and(|role| role == "master");
        Ok(if is_primary {
            Answer::Primary(client)
        } else {
            Answer::Stale(addr)
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Primary> {
        self.primary.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let url = SentinelUrl::parse("redis+sentinel://127.0.0.1:26380/mymaster").unwrap();
        assert_eq!(url.sentinels, [("127.0.0.1".to_owned(), 26380)]);
        assert_eq!(url.service, "mymaster");
        assert_eq!((url.db, url.username, url.password), (0, None, None));

        let url = SentinelUrl::parse(
            "redis+sentinel://app:s%40cret@s1,s2:26381,[::1]:26382,[fe80::1]/jobs/3\
             ?sentinel_username=watch&sentinel_password=p%26ss",
        )
        .unwrap();
        assert_eq!(
            url.sentinels,
            [
                ("s1".to_owned(), DEFAULT_PORT),
                ("s2".to_owned(), 26381),
                ("::1".to_owned(), 26382),
                ("fe80::1".to_owned(), DEFAULT_PORT),
            ]
        );
        assert_eq!(url.service, "jobs");
        assert_eq!(url.db, 3);
        assert_eq!(url.username.as_deref(), Some("app"));
        assert_eq!(url.password.as_deref(), Some("s@cret"));
        assert_eq!(url.sentinel_username.as_deref(), Some("watch"));
        assert_eq!(url.sentinel_password.as_deref(), Some("p&ss"));

        // A password alone, as `requirepass` servers have.
        let url = SentinelUrl::parse("redis+sentinel://:secret@s1/mymaster/").unwrap();
        assert_eq!(
            (url.username, url.password.as_deref()),
            (None, Some("secret"))
        );

        for (bad, reason) in [
            ("redis+sentinel://s1", "name of the service"),
            ("redis+sentinel://s1/", "name of the service"),
            ("redis+sentinel://s1:x/mymaster", "invalid port"),
            ("redis+sentinel://s1,/mymaster", "missing a sentinel"),
            ("redis+sentinel://[::1/mymaster", "invalid address"),
            ("redis+sentinel://s1/mymaster/zero", "invalid database"),
            ("redis+sentinel://s1/mymaster/0/x", "path"),
            (
                "redis+sentinel://s1/mymaster?role=replica",
                "unknown parameter",
            ),
            ("redis+sentinel://s1/mymaster#x", "fragment"),
            ("redis+sentinel://:%zz@s1/mymaster", "invalid escape"),
            ("rediss+sentinel://s1/mymaster", "not supported"),
        ] {
            let error = SentinelUrl::parse(bad).unwrap_err();
            assert!(error.contains(reason), "{bad}: {error}");
        }
    }

    #[test]
    fn redaction() {
        assert_eq!(
            redact("redis+sentinel://app:secret@s1,s2:26381/jobs/2?sentinel_password=p"),
            "redis+sentinel://app:REDACTED@s1:26379,s2:26381/jobs/2?sentinel_password=REDACTED"
        );
        assert_eq!(
            redact("redis+sentinel://:secret@[::1]/jobs?sentinel_username=w"),
            "redis+sentinel://:REDACTED@[::1]:26379/jobs?sentinel_username=w"
        );
        assert_eq!(
            redact("redis+sentinel://s1/jobs"),
            "redis+sentinel://s1:26379/jobs"
        );
        // Unparseable: nothing that may be a secret is shown.
        assert_eq!(redact("redis+sentinel://:secret@s1"), "REDACTED");
        assert_eq!(
            redact("redis+sentinel://s1/a?sentinel_password"),
            "redis+sentinel://s1:26379/a"
        );
        assert!(is_sentinel_url("redis+sentinel://s1/a"));
        assert!(is_sentinel_url("rediss+sentinel://s1/a"));
        assert!(!is_sentinel_url("redis://s1/0"));
    }
}
