//! One client connection, plus typed message routing - the Rust
//! counterpart of Cyclone.Unity's `CycloneClient`.
//!
//! Unlike cyclone-godot (GDScript has no generics), [`CycloneClient::on`]
//! is fully generic, the same shape Cyclone.Unity's `On<T>` has:
//!
//! ```no_run
//! # use cyclone_net::client::CycloneClient;
//! # use std::time::Duration;
//! # struct Player { hp: u32 }
//! # fn decode_player(_payload: &[u8]) -> Player { Player { hp: 0 } }
//! let mut client = CycloneClient::new();
//! client.on(1, decode_player, |player: Player| println!("{}", player.hp));
//! client.connect("127.0.0.1:9000", Duration::from_secs(5), Duration::from_secs(15)).unwrap();
//! ```
//!
//! `decode` is `Fn(&[u8]) -> T` - typically a one-line adapter that builds a
//! generated codec's `Reader` and calls its `decode`. Multiple handlers on
//! one message id all run, in registration order, and nothing here catches
//! a panic either one raises.

use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::connection::{ConnectionEvent, CycloneConnection};
use crate::protocol::CycloneMessage;

/// What [`CycloneClient::poll`] hands back.
#[derive(Debug)]
pub enum ClientEvent {
    Connected,
    /// A message arrived - whether or not an [`CycloneClient::on`] handler
    /// was also registered for its id; both this and the matching
    /// handler(s), if any, always run.
    MessageReceived(CycloneMessage),
    PongReceived,
    Disconnected,
}

type ClientHandler = Box<dyn FnMut(&[u8])>;

pub struct CycloneClient {
    connection: Option<CycloneConnection>,
    handlers: HashMap<u32, Vec<ClientHandler>>,
}

impl CycloneClient {
    pub fn new() -> Self {
        Self {
            connection: None,
            handlers: HashMap::new(),
        }
    }

    /// Connects to `addr`. Blocks until the OS completes (or rejects) the
    /// TCP handshake - there is no non-blocking connect here, the same way
    /// there is no async runtime in this crate at all (see the crate root
    /// docs). Returns once connected; the background reader thread and
    /// heartbeat start immediately after.
    pub fn connect(
        &mut self,
        addr: impl ToSocketAddrs,
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
    ) -> std::io::Result<()> {
        let stream = TcpStream::connect(addr)?;
        self.connection = Some(CycloneConnection::new(
            stream,
            heartbeat_interval,
            heartbeat_timeout,
        )?);
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.connection
            .as_ref()
            .is_some_and(CycloneConnection::is_connected)
    }

    pub fn send(&mut self, message: &CycloneMessage) -> std::io::Result<()> {
        match &mut self.connection {
            Some(connection) => connection.send(message),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "CycloneClient is not connected",
            )),
        }
    }

    pub fn disconnect(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.disconnect();
        }
    }

    /// Registers a typed handler for `message_id`. See this module's own
    /// docs for the full shape.
    pub fn on<T: 'static>(
        &mut self,
        message_id: u32,
        decode: impl Fn(&[u8]) -> T + 'static,
        mut handler: impl FnMut(T) + 'static,
    ) {
        self.handlers
            .entry(message_id)
            .or_default()
            .push(Box::new(move |payload| handler(decode(payload))));
    }

    /// Drains the underlying connection, dispatches [`CycloneClient::on`]
    /// handlers, and returns the raw events - never blocks. Call this once
    /// a tick/frame; see [`crate::connection`]'s own docs for why nothing
    /// here needs the caller to be async.
    pub fn poll(&mut self) -> Vec<ClientEvent> {
        let Some(connection) = &mut self.connection else {
            return Vec::new();
        };

        let raw_events = connection.poll();
        let mut events = Vec::new();

        for event in raw_events {
            match event {
                ConnectionEvent::Connected => events.push(ClientEvent::Connected),
                ConnectionEvent::MessageReceived(message) => {
                    if let Some(handlers) = self.handlers.get_mut(&message.id) {
                        for handler in handlers {
                            handler(&message.payload);
                        }
                    }
                    events.push(ClientEvent::MessageReceived(message));
                }
                ConnectionEvent::PingReceived => {}
                ConnectionEvent::PongReceived => events.push(ClientEvent::PongReceived),
                ConnectionEvent::Disconnected => events.push(ClientEvent::Disconnected),
            }
        }

        events
    }
}

impl Default for CycloneClient {
    fn default() -> Self {
        Self::new()
    }
}

// Used by CycloneServer to build a CycloneConnection directly from an
// accepted TcpStream, without going through `connect`.
pub(crate) fn connection_from_stream(
    stream: TcpStream,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) -> std::io::Result<CycloneConnection> {
    CycloneConnection::new(stream, heartbeat_interval, heartbeat_timeout)
}
