//! A TCP proxy between the workers and Redis that can delay or drop Redis's replies.
//!
//! * `Pass`: bytes flow both ways.
//! * `Delay(ms)`: every reply chunk is held `ms` before it is forwarded (order kept), so
//!   commands still run on time but their replies arrive late (timeouts when `ms` exceeds the
//!   client's response timeout).
//! * `Blackhole`: replies are swallowed (commands still reach Redis and run); when the mode
//!   ends, every connection that lost a reply is closed. A TCP stream cannot lose bytes in
//!   the middle, so this is how a real network loses replies: the connection dies with them.
//! * `reset()`: closes every connection now (replies in flight are lost).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Pass,
    Delay(u64),
    Blackhole,
}

pub struct Proxy {
    mode: watch::Sender<Mode>,
    /// Incremented to close every connection.
    generation: watch::Sender<u64>,
    pub connections: Arc<AtomicU64>,
    pub accepted: Arc<AtomicU64>,
}

impl Proxy {
    pub async fn start(listen: u16, upstream: u16) -> anyhow::Result<Arc<Self>> {
        let listener = TcpListener::bind(("127.0.0.1", listen)).await?;
        let (mode, _) = watch::channel(Mode::Pass);
        let (generation, _) = watch::channel(0_u64);
        let proxy = Arc::new(Self {
            mode,
            generation,
            connections: Arc::new(AtomicU64::new(0)),
            accepted: Arc::new(AtomicU64::new(0)),
        });
        let accept = proxy.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    continue;
                };
                accept.accepted.fetch_add(1, Ordering::Relaxed);
                let proxy = accept.clone();
                tokio::spawn(async move {
                    proxy.connections.fetch_add(1, Ordering::Relaxed);
                    let _ = proxy.serve(client, upstream).await;
                    proxy.connections.fetch_sub(1, Ordering::Relaxed);
                });
            }
        });
        Ok(proxy)
    }

    pub fn set(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }

    pub fn reset(&self) {
        self.generation.send_modify(|generation| *generation += 1);
    }

    async fn serve(&self, mut client: TcpStream, upstream: u16) -> anyhow::Result<()> {
        let mut server = match tokio::time::timeout(
            Duration::from_secs(2),
            TcpStream::connect(("127.0.0.1", upstream)),
        )
        .await
        {
            Ok(Ok(server)) => server,
            _ => return Ok(()),
        };
        client.set_nodelay(true)?;
        server.set_nodelay(true)?;
        let (mut client_read, mut client_write) = client.split();
        let (mut server_read, mut server_write) = server.split();
        let mut generation = self.generation.subscribe();
        generation.borrow_and_update();
        let mut mode = self.mode.subscribe();

        // Requests: forwarded as they come.
        let requests = async {
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let read = client_read.read(&mut buffer).await?;
                if read == 0 {
                    return Ok::<(), std::io::Error>(());
                }
                server_write.write_all(&buffer[..read]).await?;
            }
        };

        // Replies: forwarded, delayed or swallowed depending on the mode.
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
        let mut tainted = false;
        let read_replies = async {
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let read = tokio::select! {
                    read = server_read.read(&mut buffer) => read?,
                    // A mode change ends a blackhole: close a connection that lost replies.
                    changed = mode.changed() => {
                        if changed.is_err() {
                            return Ok(());
                        }
                        if tainted && *mode.borrow() != Mode::Blackhole {
                            return Ok::<(), std::io::Error>(());
                        }
                        continue;
                    }
                };
                if read == 0 {
                    return Ok(());
                }
                let current = *mode.borrow();
                match current {
                    Mode::Blackhole => tainted = true,
                    Mode::Pass => {
                        let _ = sender.send((tokio::time::Instant::now(), buffer[..read].to_vec()));
                    }
                    Mode::Delay(ms) => {
                        let due = tokio::time::Instant::now() + Duration::from_millis(ms);
                        let _ = sender.send((due, buffer[..read].to_vec()));
                    }
                }
            }
        };
        let write_replies = async {
            while let Some((due, bytes)) = receiver.recv().await {
                tokio::time::sleep_until(due).await;
                client_write.write_all(&bytes).await?;
            }
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {
            _ = requests => {}
            _ = read_replies => {}
            _ = write_replies => {}
            _ = generation.changed() => {}
        }
        Ok(())
    }
}
