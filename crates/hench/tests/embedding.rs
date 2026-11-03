//! Workers embedded next to an axum server with the wiring of the guide's "Or embed them next
//! to your web application" (`docs/guide/getting-started.md`, step 5; also
//! `examples/axum_embedded.rs`): one token stops the HTTP server and the workers, whichever side
//! stops first. Keep `serve_and_work` in sync with the guide.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use hench::CancellationToken;
use hench::api::Api;
use hench::prelude::*;
use hench::test_util::TestRedis;
use support::{WAIT, wait_for};

/// The guide's wiring: the server and the workers each cancel `shutdown` when they stop, so
/// that a signal (which cancels it) or either side stopping on its own stops both.
async fn serve_and_work(
    hench: Hench,
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> (std::io::Result<()>, hench::Result<()>) {
    let server = {
        let shutdown = shutdown.clone();
        async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.clone().cancelled_owned())
                .await;
            shutdown.cancel();
            served
        }
    };
    let workers = {
        let shutdown = shutdown.clone();
        async move {
            let ran = hench.run_until(shutdown.clone().cancelled_owned()).await;
            shutdown.cancel();
            ran
        }
    };
    tokio::join!(server, workers)
}

/// The response to `GET /` on `address`.
async fn get_root(address: SocketAddr) -> String {
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    })
    .await
    .unwrap()
}

/// An application with one route, its workers and its listener.
async fn application(redis: &TestRedis) -> (Hench, tokio::net::TcpListener, Router) {
    let hench = redis.builder().await.build().await.unwrap();
    let app = Router::new().route("/", get(|| async { "the application" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    (hench, listener, app)
}

/// The dashboard's per-process Stop (or `hench stop <identity>`) ends the embedded workers:
/// the server stops too, so that the application exits and its process manager restarts it,
/// instead of serving on without anyone processing its jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_stop_of_the_workers_stops_the_server_too() {
    let redis = TestRedis::start().await;
    let (hench, listener, app) = application(&redis).await;
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let application = tokio::spawn(serve_and_work(hench, listener, app, shutdown.clone()));
    assert!(get_root(address).await.ends_with("the application"));

    let api = Api::new(redis.pool().await);
    wait_for("the process to register", WAIT, || async {
        !api.processes().list().await.unwrap().is_empty()
    })
    .await;
    let process = api.processes().list().await.unwrap().remove(0);
    assert!(process.is_embedded());
    process.stop().await.unwrap();

    let (served, ran) = tokio::time::timeout(WAIT, application)
        .await
        .expect("the application stops")
        .unwrap();
    served.unwrap();
    ran.unwrap();
    assert!(shutdown.is_cancelled());
    assert!(api.processes().list().await.unwrap().is_empty());
}

/// A shutdown signal (here the token cancelled directly) stops both sides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shutdown_stops_both() {
    let redis = TestRedis::start().await;
    let (hench, listener, app) = application(&redis).await;
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let application = tokio::spawn(serve_and_work(hench, listener, app, shutdown.clone()));
    assert!(get_root(address).await.ends_with("the application"));
    let api = Api::new(redis.pool().await);
    wait_for("the process to register", WAIT, || async {
        !api.processes().list().await.unwrap().is_empty()
    })
    .await;

    shutdown.cancel();
    let (served, ran) = tokio::time::timeout(WAIT, application)
        .await
        .expect("the application stops")
        .unwrap();
    served.unwrap();
    ran.unwrap();
    assert!(api.processes().list().await.unwrap().is_empty());
    // Give a moment for the listener to close before checking nothing is served.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}
