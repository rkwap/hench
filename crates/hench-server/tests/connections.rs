//! `hench web`'s connections, served by a real listener: the deadline of each phase of a
//! connection (request head, idle keep-alive, request body), the connection limit, and the
//! bounded graceful shutdown (security-supply-chain-1 and -2: a half-open request head was
//! kept forever, about a thousand of them took the server offline, and one of them kept a
//! shutting-down server alive whatever the signals).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use clap::Parser;
use hench::api::Api;
use hench::test_util::TestRedis;
use hench::{RedisConfig, RedisPool};
use hench_server::cli::{Cli, Command};
use hench_server::connections::Limits;
use hench_server::server::{ServerConfig, serve_on};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A `hench web` served in process on a free port.
struct Server {
    address: SocketAddr,
    signals: mpsc::UnboundedSender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Server {
    /// `hench web <args>` with its limits changed by `limits`, on `api`'s Redis.
    async fn start(args: &[&str], api: Api, limits: impl FnOnce(&mut Limits)) -> Self {
        let mut argv = vec!["hench", "web"];
        argv.extend_from_slice(args);
        let Command::Web(web) = Cli::try_parse_from(argv).unwrap().command else {
            panic!("not `hench web`");
        };
        let mut config = ServerConfig::from_args(&web).unwrap();
        limits(&mut config.limits);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (signals, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move { serve_on(listener, &config, api, receiver).await });
        Self {
            address,
            signals,
            task,
        }
    }

    /// Sends a shutdown signal (the first one starts the graceful shutdown).
    fn signal(&self) {
        self.signals.send(()).unwrap();
    }

    /// Waits for the server to stop, at most `limit`; how long it took.
    async fn stopped_within(self, limit: Duration) -> Duration {
        let started = Instant::now();
        tokio::time::timeout(limit, self.task)
            .await
            .unwrap_or_else(|_| panic!("the server still runs {limit:?} later"))
            .unwrap()
            .unwrap();
        started.elapsed()
    }

    async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.address).await.unwrap()
    }
}

/// An `Api` on a Redis nobody listens on: the server listens and serves without one.
fn no_redis() -> Api {
    Api::new(RedisPool::lazy(RedisConfig::new("redis://127.0.0.1:1").reconnect_retries(1)).unwrap())
}

/// The start of a request head that never ends.
const HALF_HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n";

/// Reads until the server closes the connection (or resets it), at most `limit`: how long
/// that took and what it sent before, or `None` while it is still open.
async fn closed_within(stream: &mut TcpStream, limit: Duration) -> Option<(Duration, String)> {
    let started = Instant::now();
    let mut received = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        let left = limit.checked_sub(started.elapsed())?;
        match tokio::time::timeout(left, stream.read(&mut buf)).await {
            Err(_) => return None,
            Ok(Ok(0) | Err(_)) => {
                return Some((
                    started.elapsed(),
                    String::from_utf8_lossy(&received).into_owned(),
                ));
            }
            Ok(Ok(read)) => received.extend_from_slice(&buf[..read]),
        }
    }
}

/// Reads one response (head and `Content-Length` body): its status line and body.
async fn read_response(stream: &mut TcpStream) -> (String, String) {
    let mut received = Vec::new();
    let mut buf = [0_u8; 4096];
    let head_end = loop {
        if let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "closed before a response: {received:?}");
        received.extend_from_slice(&buf[..read]);
    };
    let head = String::from_utf8_lossy(&received[..head_end]).into_owned();
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    while received.len() < head_end + length {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "closed in the middle of a body");
        received.extend_from_slice(&buf[..read]);
    }
    let status = head.lines().next().unwrap_or_default().to_owned();
    let body = String::from_utf8_lossy(&received[head_end..head_end + length]).into_owned();
    (status, body)
}

#[tokio::test]
async fn an_unfinished_request_head_is_closed_after_the_header_timeout() {
    let server = Server::start(&[], no_redis(), |limits| {
        limits.header_timeout = Duration::from_secs(1);
    })
    .await;
    let mut stream = server.connect().await;
    stream.write_all(HALF_HEAD).await.unwrap();
    let (took, _) = closed_within(&mut stream, Duration::from_secs(10))
        .await
        .expect("the server closes a request head that never ends");
    assert!(took >= Duration::from_millis(800), "closed after {took:?}");
    // A client that sends nothing at all is closed the same way.
    let mut silent = server.connect().await;
    closed_within(&mut silent, Duration::from_secs(10))
        .await
        .expect("the server closes a connection that sends nothing");
    // The server itself is fine.
    let mut stream = server.connect().await;
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .unwrap();
    let (status, _) = read_response(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1 503"), "{status}");
}

#[tokio::test]
async fn an_idle_keep_alive_connection_is_closed_after_the_header_timeout() {
    let server = Server::start(&[], no_redis(), |limits| {
        limits.header_timeout = Duration::from_secs(1);
    })
    .await;
    let mut stream = server.connect().await;
    for _ in 0..2 {
        // Requests on the connection keep it open...
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .await
            .unwrap();
        let (status, body) = read_response(&mut stream).await;
        assert!(status.starts_with("HTTP/1.1 503"), "{status}");
        assert_eq!(body, "redis unavailable\n");
    }
    // ...and an idle one is closed.
    let (took, rest) = closed_within(&mut stream, Duration::from_secs(10))
        .await
        .expect("the server closes an idle keep-alive connection");
    assert!(took >= Duration::from_millis(800), "closed after {took:?}");
    assert_eq!(rest, "");
}

#[tokio::test]
async fn a_body_that_does_not_arrive_in_time_is_refused() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let server = Server::start(&["--api"], api.clone(), |limits| {
        limits.body_timeout = Duration::from_secs(1);
    })
    .await;
    let mut stream = server.connect().await;
    stream
        .write_all(
            b"POST /api/v1/jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
              Content-Length: 100\r\n\r\n{\"class\":",
        )
        .await
        .unwrap();
    let started = Instant::now();
    let (took, response) = closed_within(&mut stream, Duration::from_secs(10))
        .await
        .expect("the server gives up on a body that does not arrive");
    assert!(
        response.starts_with("HTTP/1.1 408"),
        "after {took:?}: {response}"
    );
    assert!(started.elapsed() >= Duration::from_millis(800));
    assert_eq!(api.queue("default").size().await.unwrap(), 0);

    // A body sent in time is served.
    let body = br#"{"class":"Ping","queue":"q"}"#;
    let mut stream = server.connect().await;
    stream
        .write_all(
            format!(
                "POST /api/v1/jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
    let (status, body) = read_response(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1 201"), "{status}: {body}");
    assert_eq!(api.queue("q").size().await.unwrap(), 1);

    // Too large: 413, unread.
    let mut stream = server.connect().await;
    stream
        .write_all(
            b"POST /api/v1/jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
              Content-Length: 3000000\r\n\r\n",
        )
        .await
        .unwrap();
    let (status, _) = read_response(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1 413"), "{status}");
}

#[tokio::test]
async fn bodies_are_read_only_after_the_credentials_are_checked() {
    let server = Server::start(
        &["--username", "u", "--password", "p", "--api"],
        no_redis(),
        |limits| limits.body_timeout = Duration::from_secs(30),
    )
    .await;
    let mut stream = server.connect().await;
    // No credentials, and a body that never comes: refused at once, not after 30 s.
    stream
        .write_all(
            b"POST /api/v1/jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
              Content-Length: 100\r\n\r\n",
        )
        .await
        .unwrap();
    let started = Instant::now();
    let (status, _) = read_response(&mut stream).await;
    assert!(status.starts_with("HTTP/1.1 401"), "{status}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn the_connection_limit_holds_further_clients_until_one_closes() {
    let server = Server::start(&[], no_redis(), |limits| {
        limits.max_connections = 2;
        limits.header_timeout = Duration::from_secs(60);
    })
    .await;
    let first = server.connect().await;
    let mut second = server.connect().await;
    second.write_all(HALF_HEAD).await.unwrap();
    // Give the server time to accept both.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // A third client connects (the kernel queues it) but is not served...
    let mut third = server.connect().await;
    third
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0_u8; 64];
    let early = tokio::time::timeout(Duration::from_millis(700), third.read(&mut buf)).await;
    assert!(early.is_err(), "served beyond the limit: {early:?}");
    // ...until a connection closes.
    drop(first);
    let (status, _) = tokio::time::timeout(Duration::from_secs(10), read_response(&mut third))
        .await
        .expect("served once a slot is free");
    assert!(status.starts_with("HTTP/1.1 503"), "{status}");
}

#[tokio::test]
async fn shutdown_lets_requests_in_flight_finish() {
    let redis = TestRedis::start().await;
    let api = Api::new(redis.pool().await);
    let server = Server::start(&["--api"], api.clone(), |_| {}).await;
    // An idle keep-alive connection, and a request whose body is still on its way.
    let mut idle = server.connect().await;
    idle.write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .unwrap();
    let (status, _) = read_response(&mut idle).await;
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    let body = br#"{"class":"Ping","queue":"q"}"#;
    let mut busy = server.connect().await;
    busy.write_all(
        format!(
            "POST /api/v1/jobs HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    busy.write_all(&body[..5]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let address = server.address;
    server.signal();
    // The idle connection is closed at once, no new connection is accepted...
    closed_within(&mut idle, Duration::from_secs(5))
        .await
        .expect("idle connections close at the shutdown");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(TcpStream::connect(address).await.is_err());
    // ...and the request in flight finishes.
    busy.write_all(&body[5..]).await.unwrap();
    let (status, body) = read_response(&mut busy).await;
    assert!(status.starts_with("HTTP/1.1 201"), "{status}: {body}");
    assert_eq!(api.queue("q").size().await.unwrap(), 1);
    server.stopped_within(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn shutdown_closes_what_is_left_after_its_timeout() {
    let server = Server::start(&[], no_redis(), |limits| {
        limits.header_timeout = Duration::from_secs(60);
        limits.shutdown_timeout = Duration::from_secs(1);
    })
    .await;
    let mut stalled = server.connect().await;
    stalled.write_all(HALF_HEAD).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    server.signal();
    let took = server.stopped_within(Duration::from_secs(10)).await;
    assert!(took >= Duration::from_millis(800), "stopped after {took:?}");
    closed_within(&mut stalled, Duration::from_secs(5))
        .await
        .expect("the stalled connection is closed");
}

#[tokio::test]
async fn a_second_signal_stops_the_server_at_once() {
    let server = Server::start(&[], no_redis(), |limits| {
        limits.header_timeout = Duration::from_secs(60);
        limits.shutdown_timeout = Duration::from_secs(60);
    })
    .await;
    let mut stalled = server.connect().await;
    stalled.write_all(HALF_HEAD).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    server.signal();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!server.task.is_finished(), "the first signal waits");
    server.signal();
    server.stopped_within(Duration::from_secs(5)).await;
    closed_within(&mut stalled, Duration::from_secs(5))
        .await
        .expect("the stalled connection is closed");
}
