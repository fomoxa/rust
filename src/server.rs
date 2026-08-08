//! Listens for and manages many client connections - the Rust counterpart
//! of Cyclone.Unity's `CycloneServer`.

use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Duration;

use crate::client::connection_from_stream;
use crate::connection::{ConnectionEvent, CycloneConnection};
use crate::protocol::CycloneMessage;

/// Identifies one accepted connection for the lifetime of a
/// [`CycloneServer`]. Never reused, even after the connection it named
/// disconnects - stable enough to key a `HashMap` of per-player state on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

/// What [`CycloneServer::poll`] hands back.
#[derive(Debug)]
pub enum ServerEvent {
    ClientConnected(ConnectionId),
    ClientDisconnected(ConnectionId),
    MessageReceived(ConnectionId, CycloneMessage),
    PongReceived(ConnectionId),
}

struct Slot {
    id: ConnectionId,
    connection: CycloneConnection,
}

pub struct CycloneServer {
    incoming_rx: Option<Receiver<TcpStream>>,
    connections: Vec<Slot>,
    next_id: u64,
    running: bool,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    local_addr: Option<SocketAddr>,
}

impl CycloneServer {
    pub fn new() -> Self {
        Self::with_heartbeat(Duration::from_secs(5), Duration::from_secs(15))
    }

    /// Like [`CycloneServer::new`], but with a heartbeat interval/timeout
    /// applied to every connection this server accepts (the defaults match
    /// [`CycloneServer::new`] and Cyclone.Unity's own).
    pub fn with_heartbeat(heartbeat_interval: Duration, heartbeat_timeout: Duration) -> Self {
        Self {
            incoming_rx: None,
            connections: Vec::new(),
            next_id: 0,
            running: false,
            heartbeat_interval,
            heartbeat_timeout,
            local_addr: None,
        }
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    /// The address actually bound, once [`CycloneServer::start`] has
    /// succeeded - in particular, the real port the OS chose when
    /// `start`'s `addr` asked for port `0`. Binding to port `0` and
    /// reading it back here is the robust way to pick a port for a test or
    /// an ephemeral server: no fixed port number can collide with
    /// something else already using it.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Binds and starts accepting connections on a background thread -
    /// see [`crate::connection`]'s own docs for why this crate uses a
    /// thread instead of async I/O. Blocks only long enough to bind; it
    /// does not wait for a connection.
    pub fn start(&mut self, addr: impl ToSocketAddrs) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        self.local_addr = Some(listener.local_addr()?);
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        if tx.send(stream).is_err() {
                            return; // the CycloneServer was dropped
                        }
                    }
                    Err(_) => return, // the listener itself errored; stop accepting
                }
            }
        });

        self.incoming_rx = Some(rx);
        self.running = true;
        Ok(())
    }

    /// Disconnects every open connection and stops accepting new ones.
    pub fn stop(&mut self) {
        self.running = false;
        self.incoming_rx = None;
        self.local_addr = None;
        for slot in &mut self.connections {
            slot.connection.disconnect();
        }
        self.connections.clear();
    }

    pub fn connection_ids(&self) -> impl Iterator<Item = ConnectionId> + '_ {
        self.connections.iter().map(|slot| slot.id)
    }

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn send_to(&mut self, id: ConnectionId, message: &CycloneMessage) -> std::io::Result<()> {
        match self.connections.iter_mut().find(|slot| slot.id == id) {
            Some(slot) => slot.connection.send(message),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no connection with that id",
            )),
        }
    }

    pub fn broadcast(&mut self, message: &CycloneMessage) {
        for slot in &mut self.connections {
            if slot.connection.is_connected() {
                let _ = slot.connection.send(message);
            }
        }
    }

    /// Accepts any waiting connections, then polls every open one - never
    /// blocks. Call this once a tick/frame.
    pub fn poll(&mut self) -> Vec<ServerEvent> {
        let mut events = Vec::new();

        if let Some(rx) = &self.incoming_rx {
            loop {
                match rx.try_recv() {
                    Ok(stream) => {
                        // Err: the accepted socket died before setup finished.
                        if let Ok(connection) = connection_from_stream(
                            stream,
                            self.heartbeat_interval,
                            self.heartbeat_timeout,
                        ) {
                            let id = ConnectionId(self.next_id);
                            self.next_id += 1;
                            self.connections.push(Slot { id, connection });
                            events.push(ServerEvent::ClientConnected(id));
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        }

        let mut disconnected = Vec::new();
        for slot in &mut self.connections {
            for event in slot.connection.poll() {
                match event {
                    ConnectionEvent::MessageReceived(message) => {
                        events.push(ServerEvent::MessageReceived(slot.id, message));
                    }
                    ConnectionEvent::PongReceived => {
                        events.push(ServerEvent::PongReceived(slot.id));
                    }
                    ConnectionEvent::PingReceived => {}
                    ConnectionEvent::Disconnected => {
                        events.push(ServerEvent::ClientDisconnected(slot.id));
                        disconnected.push(slot.id);
                    }
                }
            }
        }

        if !disconnected.is_empty() {
            self.connections
                .retain(|slot| !disconnected.contains(&slot.id));
        }

        events
    }
}

impl Default for CycloneServer {
    fn default() -> Self {
        Self::new()
    }
}
