//! Serving connections: the accept loop of `hench web`, with the limits that keep slow, idle
//! or numerous clients from taking the server offline, and a bounded graceful shutdown.
//!
//! `axum::serve` gives hyper no timer, so hyper drops its request-head timeout, and it limits
//! neither the number of connections nor how long a shutdown waits for them: a client that
//! sends half a request head holds its connection (a file descriptor, a task, a buffer)
//! forever, a thousand such clients exhaust the open-file limit, and one of them keeps a
//! shutting-down server alive. Here every phase of a connection has a deadline:
//!
//! * the request head must arrive within [`Limits::header_timeout`] of the first read (an
//!   idle keep-alive connection waiting for its next request is closed after the same time);
//! * the request body within [`Limits::body_timeout`] of the head (see [`read_body`]);
//! * a response the client stops reading fails after [`Limits::write_timeout`] without
//!   progress ([`WriteTimeout`]);
//! * after the first shutdown signal, requests in flight have [`Limits::shutdown_timeout`] to
//!   finish, and a second signal closes everything at once.
//!
//! At most [`Limits::max_connections`] connections are served at once: past that, the server
//! stops accepting (clients wait in the kernel's backlog) instead of running out of file
//! descriptors.

use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::HttpBody as _;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};

/// The largest request head accepted (request line and headers): larger ones get `431`.
/// Browsers send a few kilobytes; proxies allow 8 to 32 KiB.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// The largest request body accepted (`413` beyond): axum's default limit, which the
/// extractors of the dashboard and the JSON API apply too.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// How the server bounds its connections (see the [module documentation](self)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Connections served at once; further clients wait to be accepted.
    pub max_connections: usize,
    /// How long a client has to send a whole request head, from the first read of it; also
    /// how long an idle keep-alive connection is kept for its next request.
    pub header_timeout: Duration,
    /// How long a client has to send a request body, once its head was read (`408` after).
    pub body_timeout: Duration,
    /// How long a response may wait for the client to read it before the connection is
    /// closed.
    pub write_timeout: Duration,
    /// How long the requests in flight may take to finish after the first shutdown signal.
    pub shutdown_timeout: Duration,
}

impl Limits {
    /// 512 connections, 10 s for a request head (and an idle connection), 30 s for a body,
    /// 30 s for a stalled response, 10 s to finish the requests in flight at shutdown.
    pub const DEFAULT: Limits = Limits {
        max_connections: 512,
        header_timeout: Duration::from_secs(10),
        body_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(10),
    };
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Every SIGINT (Ctrl-C) and SIGTERM the process receives, one message each, for [`run`]: the
/// first starts a graceful shutdown, a second one ends it at once. The handlers are installed
/// before this returns.
pub fn shutdown_signals() -> mpsc::UnboundedReceiver<()> {
    let (sender, receiver) = mpsc::unbounded_channel();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let listen = |kind: SignalKind, name: &'static str| match signal(kind) {
            Ok(stream) => Some(stream),
            Err(error) => {
                tracing::error!(%error, "cannot listen for {name}");
                None
            }
        };
        let mut interrupt = listen(SignalKind::interrupt(), "SIGINT");
        let mut terminate = listen(SignalKind::terminate(), "SIGTERM");
        tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    Some(()) = recv(&mut interrupt) => "SIGINT",
                    Some(()) = recv(&mut terminate) => "SIGTERM",
                    else => break,
                };
                tracing::debug!(signal = received, "received a signal");
                if sender.send(()).is_err() {
                    break;
                }
            }
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        loop {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "cannot listen for Ctrl-C");
                // Keep the sender: a closed channel would mean "shut down".
                std::future::pending::<()>().await;
            }
            if sender.send(()).is_err() {
                break;
            }
        }
    });
    receiver
}

/// The next signal of `stream`, never for a stream that could not be installed.
#[cfg(unix)]
async fn recv(stream: &mut Option<tokio::signal::unix::Signal>) -> Option<()> {
    match stream {
        Some(stream) => stream.recv().await,
        None => std::future::pending().await,
    }
}

/// Serves `app` on `listener` within `limits` until the first message of `signals` (or its
/// closing), then lets the requests in flight finish for at most
/// [`Limits::shutdown_timeout`], or until a second message, and closes what is left.
pub async fn run(
    listener: TcpListener,
    app: Router,
    limits: Limits,
    mut signals: mpsc::UnboundedReceiver<()>,
) {
    let permits = Arc::new(Semaphore::new(limits.max_connections.max(1)));
    let (closing, close) = watch::channel(false);
    let mut connections = JoinSet::new();
    // A slot for the next connection, taken before accepting it.
    let mut permit: Option<OwnedSemaphorePermit> = None;
    let mut warned_full: Option<Instant> = None;
    loop {
        tokio::select! {
            // A signal (or a closed channel) stops accepting.
            _ = signals.recv() => break,
            Some(joined) = connections.join_next() => report(joined),
            acquired = Arc::clone(&permits).acquire_owned(), if permit.is_none() => {
                permit = acquired.ok();
            }
            accepted = listener.accept(), if permit.is_some() => match accepted {
                Ok((stream, peer)) => {
                    let Some(held) = permit.take() else { continue };
                    let recently = warned_full
                        .is_some_and(|warned| warned.elapsed() < Duration::from_secs(60));
                    if permits.available_permits() == 0 && !recently {
                        warned_full = Some(Instant::now());
                        tracing::warn!(
                            limit = limits.max_connections,
                            "the connection limit is reached: new clients wait until a \
                             connection closes"
                        );
                    }
                    let service = TowerToHyperService::new(app.clone());
                    let close = close.clone();
                    connections.spawn(async move {
                        serve_connection(stream, service, limits, close).await;
                        tracing::trace!(%peer, "connection closed");
                        drop(held);
                    });
                }
                Err(error) if is_connection_error(&error) => {
                    tracing::debug!(%error, "a connection failed before it was accepted");
                }
                Err(error) => {
                    // Out of file descriptors or memory: try again shortly.
                    tracing::error!(%error, "cannot accept connections; retrying in a second");
                    tokio::select! {
                        _ = signals.recv() => break,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            },
        }
    }
    drop(listener);
    let open = connections.len();
    tracing::info!(
        connections = open,
        "shutting down: finishing the requests in flight"
    );
    // Idle connections close at once; busy ones after their current request.
    let _ = closing.send(true);
    let deadline = Instant::now() + limits.shutdown_timeout;
    loop {
        tokio::select! {
            joined = connections.join_next() => match joined {
                Some(joined) => report(joined),
                None => return,
            },
            () = tokio::time::sleep_until(deadline) => {
                tracing::warn!(
                    connections = connections.len(),
                    timeout_ms = limits.shutdown_timeout.as_millis() as u64,
                    "the shutdown timeout is over: closing the connections still open"
                );
                break;
            }
            Some(()) = signals.recv() => {
                tracing::warn!(
                    connections = connections.len(),
                    "a second signal: closing the connections still open"
                );
                break;
            }
        }
    }
    connections.shutdown().await;
}

/// Logs a connection task that panicked.
fn report(joined: Result<(), tokio::task::JoinError>) {
    if let Err(error) = joined
        && error.is_panic()
    {
        tracing::error!(%error, "a connection task panicked");
    }
}

/// Errors of one connection that `accept` reports; the next accept is unaffected.
fn is_connection_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// Serves one connection until it closes, or until `close` asks for a graceful shutdown (the
/// current request, if any, finishes first).
async fn serve_connection(
    stream: TcpStream,
    service: TowerToHyperService<Router>,
    limits: Limits,
    mut close: watch::Receiver<bool>,
) {
    // Small responses go out at once rather than waiting for more data.
    let _ = stream.set_nodelay(true);
    let io = TokioIo::new(WriteTimeout::new(stream, limits.write_timeout));
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout)
        .max_header_size(MAX_HEADER_BYTES);
    let mut connection = pin!(builder.serve_connection(io, service));
    // Completes when the server shuts down (or is gone).
    let closing = async move {
        let _ = close.wait_for(|closing| *closing).await;
    };
    let result = tokio::select! {
        result = connection.as_mut() => result,
        () = closing => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };
    if let Err(error) = result {
        tracing::debug!(%error, "connection ended with an error");
    }
}

/// Middleware: reads the request body within [`Limits::body_timeout`] and
/// [`MAX_BODY_BYTES`] before the handler runs (`408 Request Timeout` or `413 Payload Too
/// Large` otherwise), so that a client trickling a body cannot hold a request open. Applied
/// inside the host and credential checks, so unauthenticated requests are refused before
/// their body is read.
pub async fn read_body(timeout: Duration, request: Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    // A declared length beyond the limit is refused without reading anything.
    let too_large = || {
        refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "The request body is too large.",
        )
    };
    if body.size_hint().lower() > MAX_BODY_BYTES as u64 {
        return too_large();
    }
    let collected = tokio::time::timeout(timeout, Limited::new(body, MAX_BODY_BYTES).collect());
    let bytes = match collected.await {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(error)) if error.downcast_ref::<LengthLimitError>().is_some() => return too_large(),
        Ok(Err(error)) => {
            tracing::debug!(%error, path = parts.uri.path(), "could not read a request body");
            return refuse(
                StatusCode::BAD_REQUEST,
                "The request body could not be read.",
            );
        }
        Err(_) => {
            tracing::info!(
                path = parts.uri.path(),
                timeout_ms = timeout.as_millis() as u64,
                "a request body did not arrive in time"
            );
            return refuse(
                StatusCode::REQUEST_TIMEOUT,
                "The request body did not arrive in time.",
            );
        }
    };
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

/// A plain-text refusal of a request whose body was not read whole: the connection closes
/// after it, so that the rest of the body is never read as the next request.
fn refuse(status: StatusCode, message: &str) -> Response {
    let mut response = (status, format!("{message}\n")).into_response();
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
    response
}

/// A stream whose writes fail (`TimedOut`) once they have made no progress for `timeout`: a
/// client that stops reading its response cannot hold the connection forever.
pub struct WriteTimeout<S> {
    inner: S,
    timeout: Duration,
    /// Running while writes are pending; reset by every write that makes progress.
    stalled: Option<Pin<Box<Sleep>>>,
}

impl<S> WriteTimeout<S> {
    /// Wraps `inner`.
    pub fn new(inner: S, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            stalled: None,
        }
    }

    /// Called while a write is pending: the error once the stall has lasted `timeout`.
    fn poll_stall(&mut self, cx: &mut Context<'_>) -> Poll<io::Error> {
        let timeout = self.timeout;
        let stalled = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        match stalled.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(io::Error::new(
                io::ErrorKind::TimedOut,
                "the client stopped reading the response",
            )),
            Poll::Pending => Poll::Pending,
        }
    }

    /// The outcome of a write-side poll: pending writes run the stall clock, completed ones
    /// reset it.
    fn track<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match polled {
            Poll::Pending => self.poll_stall(cx).map(Err),
            ready => {
                self.stalled = None;
                ready
            }
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteTimeout<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.track(cx, polled)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.track(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_flush(cx);
        this.track(cx, polled)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.track(cx, polled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn stalled_writes_time_out_and_progress_resets_the_clock() {
        // A pipe with room for 8 bytes: writes stall once it is full and nobody reads.
        let (client, server) = tokio::io::duplex(8);
        let mut writer = WriteTimeout::new(server, Duration::from_secs(30));
        let mut reader = client;
        writer.write_all(b"12345678").await.unwrap();
        // Reading makes room within the timeout: the next write goes through.
        let started = Instant::now();
        let read = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            let mut buf = [0_u8; 8];
            reader.read_exact(&mut buf).await.unwrap();
            reader
        });
        writer.write_all(b"abcdefgh").await.unwrap();
        assert!(started.elapsed() >= Duration::from_secs(20));
        let _reader = read.await.unwrap();
        // Nobody reads any more: the stalled write fails after the timeout, not before.
        let started = Instant::now();
        let error = writer.write_all(b"more").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(30) && waited < Duration::from_secs(31),
            "{waited:?}"
        );
    }

    #[test]
    fn the_default_limits_bound_every_phase() {
        let limits = Limits::default();
        assert_eq!(limits, Limits::DEFAULT);
        assert!(limits.max_connections >= 64);
        for timeout in [
            limits.header_timeout,
            limits.body_timeout,
            limits.write_timeout,
            limits.shutdown_timeout,
        ] {
            assert!(timeout > Duration::ZERO && timeout <= Duration::from_secs(60));
        }
    }
}
