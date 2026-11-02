//! The host names `hench web` answers for: a defense against DNS rebinding.
//!
//! A page on a hostname its author controls (`http://evil.example:7420`) can have that name
//! re-resolved to `127.0.0.1` after it loaded. Its scripts then talk to a dashboard listening
//! on the loopback interface as if it were their own origin: the browser sends
//! `Host: evil.example:7420`, a matching `Origin` and `Sec-Fetch-Site: same-origin`, so every
//! cross-site check passes and the pages (with their CSRF token) are readable. Only the
//! `Host` header tells such requests apart, and scripts cannot set it.
//!
//! Without credentials, `hench web` therefore answers only for names that cannot be rebound —
//! `localhost` (and `*.localhost`) and IP addresses — plus the names given with
//! `--allowed-host`. With Basic authentication a rebound page has no credentials for its own
//! origin, so every name is served unless `--allowed-host` lists some. The check itself is
//! the dashboard's ([`hench_web::AllowedHosts`], [`hench_web::require_host`]), with a refusal
//! that names the option.

/// The text of the refusal.
const REFUSAL: &str = "This server does not answer for this host name. Without credentials, \
                       hench web serves only localhost and IP addresses; name others with \
                       --allowed-host (or HENCH_WEB_ALLOWED_HOSTS).";

/// The host names a server answers for, besides `localhost` and IP addresses: the dashboard's
/// [`hench_web::AllowedHosts`], refused with a text that names `--allowed-host`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedHosts(hench_web::AllowedHosts);

impl AllowedHosts {
    /// The names given with `--allowed-host` (as [`parse_allowed_host`] normalized them): exact
    /// names, `.example.com` for a domain and its subdomains.
    pub fn new(names: impl IntoIterator<Item = String>) -> Self {
        Self(hench_web::AllowedHosts::new(names).refusal(REFUSAL))
    }

    /// `true` when `host` (a `Host` header value: `name[:port]`) may be served.
    pub fn allows(&self, host: &str) -> bool {
        self.0.allows(host)
    }

    /// The same names for the dashboard and [`hench_web::require_host`].
    pub fn web(&self) -> &hench_web::AllowedHosts {
        &self.0
    }
}

/// Parses an `--allowed-host` value: a host name (`jobs.example.com`), a domain with its
/// subdomains (`.example.com`), an IP address (always allowed anyway) or `*` (every name).
/// Names are compared case-insensitively, whatever the port.
pub fn parse_allowed_host(text: &str) -> Result<String, String> {
    hench_web::AllowedHosts::parse_name(text).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_that_cannot_be_rebound_are_always_allowed() {
        let hosts = AllowedHosts::new([]);
        for host in [
            "localhost",
            "LocalHost:7420",
            "localhost.:80",
            "dash.localhost",
            "127.0.0.1",
            "127.0.0.1:7420",
            "10.0.0.7:8080",
            "[::1]:7420",
            "[2001:db8::1]",
        ] {
            assert!(hosts.allows(host), "{host}");
        }
        for host in [
            "evil.example",
            "evil.example:7420",
            "localhost.evil.example",
            "127.0.0.1.nip.io",
            "",
            "user@localhost",
            "local host",
            "localhost:port",
        ] {
            assert!(!hosts.allows(host), "{host:?}");
        }
    }

    #[test]
    fn allowed_names_and_domains_match_whole_labels() {
        let hosts = AllowedHosts::new(["jobs.example.com".to_owned(), ".corp.example".to_owned()]);
        assert!(hosts.allows("jobs.example.com"));
        assert!(hosts.allows("JOBS.EXAMPLE.COM:443"));
        assert!(hosts.allows("jobs.example.com.:443"));
        assert!(!hosts.allows("x.jobs.example.com"));
        assert!(!hosts.allows("jobs.example.com.evil.example"));
        assert!(hosts.allows("corp.example"));
        assert!(hosts.allows("a.b.corp.example:8080"));
        assert!(!hosts.allows("xcorp.example"));
        assert!(!hosts.allows("corp.example.evil"));
    }

    #[test]
    fn allowed_host_values_are_validated() {
        assert_eq!(
            parse_allowed_host(" Jobs.Example.COM ").unwrap(),
            "jobs.example.com"
        );
        assert_eq!(parse_allowed_host(".example.com").unwrap(), ".example.com");
        assert_eq!(parse_allowed_host("example.com.").unwrap(), "example.com");
        assert_eq!(parse_allowed_host("*").unwrap(), "*");
        assert_eq!(parse_allowed_host("[::1]").unwrap(), "::1");
        assert_eq!(parse_allowed_host("10.0.0.1").unwrap(), "10.0.0.1");
        for bad in [
            "",
            ".",
            "https://jobs.example.com",
            "jobs.example.com:443",
            "jobs.example.com/path",
            "a..b",
            "*.example.com",
            "jobs example",
        ] {
            assert!(parse_allowed_host(bad).is_err(), "{bad:?}");
        }
    }
}
