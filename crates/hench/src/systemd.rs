//! systemd integration: readiness, status and watchdog notifications (`sd_notify`).
//!
//! When a worker runs as a systemd service of `Type=notify` (systemd then sets
//! `NOTIFY_SOCKET`), hench tells systemd when the process is ready (`READY=1`, once it is
//! registered and processing jobs), describes what it is doing (`STATUS=…`, shown by
//! `systemctl status`), announces its shutdown (`STOPPING=1`), and, when the unit sets
//! `WatchdogSec=` (systemd then sets `WATCHDOG_USEC`), sends `WATCHDOG=1` keep-alives twice
//! per watchdog period from a task of their own, whatever Redis or the heartbeat are doing,
//! so that a process whose runtime stops running tasks gets restarted. Without
//! `NOTIFY_SOCKET` nothing is sent.
//!
//! ```ini
//! [Service]
//! Type=notify
//! WatchdogSec=30
//! ExecStart=/usr/local/bin/worker -C /etc/worker/hench.toml
//! KillSignal=SIGTERM
//! TimeoutStopSec=30
//! ```
//!
//! Workers run with [`Hench::run`](crate::Hench::run) use [`Notifier::from_env`] unless told
//! otherwise ([`HenchBuilder::systemd`](crate::HenchBuilder::systemd),
//! [`HenchBuilder::systemd_notifier`](crate::HenchBuilder::systemd_notifier)). Workers
//! embedded in another program ([`Hench::start`](crate::Hench::start),
//! [`Hench::run_until`](crate::Hench::run_until)) send nothing unless given a notifier: the
//! program owns its service's notifications (an embedded `STOPPING=1`, after a remote stop
//! from the dashboard, would make systemd stop the whole service). A [`Notifier`] can also be
//! used directly, for example by a program that embeds hench and reports its own readiness.

use std::ffi::OsString;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The variable naming the notification socket.
pub const NOTIFY_SOCKET: &str = "NOTIFY_SOCKET";

/// The variable holding the watchdog timeout, in microseconds.
pub const WATCHDOG_USEC: &str = "WATCHDOG_USEC";

/// The variable naming the process the watchdog applies to.
pub const WATCHDOG_PID: &str = "WATCHDOG_PID";

/// Whether a process notifies systemd, see [`HenchBuilder::systemd`](crate::HenchBuilder::systemd).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Notifications {
    /// Through the socket systemd names in `NOTIFY_SOCKET`, when it is set (the default; see
    /// [`Notifier::from_env`]), for a process run with [`Hench::run`](crate::Hench::run).
    /// Embedded workers ([`Hench::start`](crate::Hench::start),
    /// [`Hench::run_until`](crate::Hench::run_until)) send nothing.
    #[default]
    FromEnv,
    /// Never.
    Disabled,
    /// Through this notifier, in every mode (embedded workers included).
    To(Notifier),
}

impl Notifications {
    /// The notifier to use, if any.
    pub fn notifier(&self) -> Option<Notifier> {
        match self {
            Self::FromEnv => Notifier::from_env(),
            Self::Disabled => None,
            Self::To(notifier) => Some(notifier.clone()),
        }
    }
}

/// Sends notifications to the service manager's socket (`sd_notify(3)`): a socket path, or an
/// abstract socket name written `@name` (Linux). Cheap to clone; each notification is one
/// non-blocking datagram, so a notification that cannot be sent at once fails instead of
/// blocking.
#[derive(Clone, PartialEq, Eq)]
pub struct Notifier {
    socket: OsString,
    watchdog: Option<Duration>,
}

impl Notifier {
    /// A notifier for the socket `socket` (a path, or `@name` for an abstract socket), without
    /// watchdog.
    pub fn new(socket: impl Into<OsString>) -> Self {
        Self {
            socket: socket.into(),
            watchdog: None,
        }
    }

    /// The notifier systemd set up for this process: `None` without `NOTIFY_SOCKET` (not run by
    /// systemd, or not as `Type=notify`). Its watchdog is `WATCHDOG_USEC` when that is a
    /// positive number and `WATCHDOG_PID`, if set, is this process.
    pub fn from_env() -> Option<Self> {
        Self::from_vars(|name| std::env::var_os(name), std::process::id())
    }

    /// [`Notifier::from_env`] reading variables through `var`, for process `pid`.
    fn from_vars(var: impl Fn(&str) -> Option<OsString>, pid: u32) -> Option<Self> {
        let socket = var(NOTIFY_SOCKET).filter(|socket| !socket.is_empty())?;
        let usec = var(WATCHDOG_USEC)
            .and_then(|usec| usec.into_string().ok())
            .and_then(|usec| usec.trim().parse::<u64>().ok())
            .filter(|usec| *usec > 0);
        let for_us = var(WATCHDOG_PID).is_none_or(|watched| {
            watched
                .to_str()
                .and_then(|watched| watched.trim().parse::<u32>().ok())
                == Some(pid)
        });
        Some(Self {
            socket,
            watchdog: usec.filter(|_| for_us).map(Duration::from_micros),
        })
    }

    /// Expects watchdog keep-alives every `timeout` (what `WATCHDOG_USEC` says).
    #[must_use]
    pub fn with_watchdog(mut self, timeout: Duration) -> Self {
        self.watchdog = Some(timeout).filter(|timeout| !timeout.is_zero());
        self
    }

    /// The watchdog timeout, if the service manager expects keep-alives.
    pub fn watchdog(&self) -> Option<Duration> {
        self.watchdog
    }

    /// The interval keep-alives are sent at: half the watchdog timeout (as systemd recommends),
    /// if there is a watchdog.
    pub fn keep_alive_interval(&self) -> Option<Duration> {
        self.watchdog.map(|timeout| timeout / 2)
    }

    /// The socket notifications go to.
    pub fn socket(&self) -> &OsString {
        &self.socket
    }

    /// Sends `state` (newline-separated `KEY=VALUE` assignments) and returns the number of bytes
    /// sent.
    pub fn notify(&self, state: &str) -> io::Result<usize> {
        send(&self.socket, state.as_bytes())
    }

    /// `READY=1`: the service has started.
    pub fn ready(&self) -> io::Result<usize> {
        self.notify("READY=1")
    }

    /// `RELOADING=1`: the service is reloading its configuration.
    pub fn reloading(&self) -> io::Result<usize> {
        self.notify("RELOADING=1")
    }

    /// `STOPPING=1`: the service is shutting down.
    pub fn stopping(&self) -> io::Result<usize> {
        self.notify("STOPPING=1")
    }

    /// `STATUS=…`: a one-line description of what the service is doing.
    pub fn status(&self, status: &str) -> io::Result<usize> {
        // A newline would start another assignment.
        let line = status.replace('\n', " ");
        self.notify(&format!("STATUS={line}"))
    }

    /// `WATCHDOG=1`: a watchdog keep-alive.
    pub fn watchdog_keep_alive(&self) -> io::Result<usize> {
        self.notify("WATCHDOG=1")
    }

    /// `ERRNO=…`: the service failed with this error number.
    pub fn errno(&self, errno: i32) -> io::Result<usize> {
        self.notify(&format!("ERRNO={errno}"))
    }

    /// `MAINPID=…`: the service's main process.
    pub fn main_pid(&self, pid: u32) -> io::Result<usize> {
        self.notify(&format!("MAINPID={pid}"))
    }

    /// Sends `state`, logging a failure instead of returning it (for the notifications workers
    /// send along the way): the first failure of the process as a warning, later ones at debug
    /// level, since a broken socket fails every beat.
    pub(crate) fn send_quietly(&self, state: &str) {
        static WARNED: AtomicBool = AtomicBool::new(false);
        if let Err(error) = self.notify(state) {
            if WARNED.swap(true, Ordering::Relaxed) {
                tracing::debug!(%error, state, "could not notify systemd");
            } else {
                tracing::warn!(
                    %error,
                    socket = ?self.socket,
                    state,
                    "could not notify systemd; later failures are logged at debug level"
                );
            }
        }
    }
}

impl fmt::Debug for Notifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notifier")
            .field("socket", &self.socket)
            .field("watchdog", &self.watchdog)
            .finish()
    }
}

/// Sends one datagram to the socket `socket` (`@name`: abstract).
#[cfg(unix)]
fn send(socket: &OsString, message: &[u8]) -> io::Result<usize> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixDatagram;

    let sender = UnixDatagram::unbound()?;
    sender.set_nonblocking(true)?;
    let bytes = socket.as_bytes();
    match bytes.strip_prefix(b"@") {
        Some(name) => send_abstract(&sender, name, message),
        None => sender.send_to(message, socket),
    }
}

#[cfg(target_os = "linux")]
fn send_abstract(
    sender: &std::os::unix::net::UnixDatagram,
    name: &[u8],
    message: &[u8],
) -> io::Result<usize> {
    use std::os::linux::net::SocketAddrExt;
    let address = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    sender.send_to_addr(message, &address)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn send_abstract(
    _sender: &std::os::unix::net::UnixDatagram,
    _name: &[u8],
    _message: &[u8],
) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract sockets are only available on Linux",
    ))
}

#[cfg(not(unix))]
fn send(_socket: &OsString, _message: &[u8]) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "systemd notifications need Unix sockets",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    /// A bound socket in a temporary directory, with a short read timeout.
    fn listener() -> (tempfile::TempDir, std::path::PathBuf, UnixDatagram) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("notify.sock");
        let socket = UnixDatagram::bind(&path).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        (directory, path, socket)
    }

    fn receive(socket: &UnixDatagram) -> String {
        let mut buffer = [0_u8; 256];
        let size = socket.recv(&mut buffer).unwrap();
        String::from_utf8_lossy(&buffer[..size]).into_owned()
    }

    #[test]
    fn nothing_without_a_socket() {
        assert_eq!(Notifier::from_vars(vars(&[]), 7), None);
        assert_eq!(Notifier::from_vars(vars(&[(NOTIFY_SOCKET, "")]), 7), None);
        assert_eq!(
            Notifier::from_vars(vars(&[(WATCHDOG_USEC, "100")]), 7),
            None
        );
    }

    #[test]
    fn watchdog_settings() {
        let watchdog = |pairs: &[(&str, &str)]| {
            let mut all = vec![(NOTIFY_SOCKET, "/run/notify")];
            all.extend_from_slice(pairs);
            Notifier::from_vars(vars(&all), 42).unwrap().watchdog()
        };
        assert_eq!(watchdog(&[]), None);
        assert_eq!(watchdog(&[(WATCHDOG_USEC, "abc")]), None);
        assert_eq!(watchdog(&[(WATCHDOG_USEC, "0")]), None);
        assert_eq!(watchdog(&[(WATCHDOG_USEC, "-5")]), None);
        assert_eq!(
            watchdog(&[(WATCHDOG_USEC, "100")]),
            Some(Duration::from_micros(100))
        );
        assert_eq!(
            watchdog(&[(WATCHDOG_USEC, "30000000"), (WATCHDOG_PID, "42")]),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            watchdog(&[(WATCHDOG_USEC, "100"), (WATCHDOG_PID, "43")]),
            None
        );
        let notifier = Notifier::new("/run/notify").with_watchdog(Duration::from_secs(10));
        assert_eq!(notifier.keep_alive_interval(), Some(Duration::from_secs(5)));
        assert_eq!(
            Notifier::new("x")
                .with_watchdog(Duration::ZERO)
                .keep_alive_interval(),
            None
        );
        assert_eq!(notifier.socket(), &OsString::from("/run/notify"));
        assert!(format!("{notifier:?}").contains("/run/notify"));
    }

    #[test]
    fn notifies() {
        let (_directory, path, socket) = listener();
        let notifier = Notifier::new(&path);
        assert_eq!(notifier.ready().unwrap(), 7);
        assert_eq!(receive(&socket), "READY=1");
        assert_eq!(notifier.stopping().unwrap(), 10);
        assert_eq!(receive(&socket), "STOPPING=1");
        notifier.reloading().unwrap();
        assert_eq!(receive(&socket), "RELOADING=1");
        notifier.status("doing work\nsecond line").unwrap();
        assert_eq!(receive(&socket), "STATUS=doing work second line");
        notifier.errno(3).unwrap();
        assert_eq!(receive(&socket), "ERRNO=3");
        notifier.main_pid(123).unwrap();
        assert_eq!(receive(&socket), "MAINPID=123");
        notifier.watchdog_keep_alive().unwrap();
        assert_eq!(receive(&socket), "WATCHDOG=1");
        notifier.notify("FDSTORE=1").unwrap();
        assert_eq!(receive(&socket), "FDSTORE=1");
    }

    #[test]
    fn unreachable_sockets_fail() {
        let notifier = Notifier::new("/nonexistent/hench/missing.sock");
        assert!(notifier.ready().is_err());
        // Logged, not returned.
        notifier.send_quietly("READY=1");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn abstract_sockets() {
        use std::os::linux::net::SocketAddrExt;
        let name = format!("hench-test-{}", crate::payload::random_hex(8));
        let address = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let socket = UnixDatagram::bind_addr(&address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let notifier = Notifier::new(format!("@{name}"));
        notifier.ready().unwrap();
        assert_eq!(receive(&socket), "READY=1");
    }
}
