//! Wraps one `TcpStream`, doing frame buffering and heartbeat on it - used
//! by both [`crate::client::CycloneClient`] (one connection) and
//! [`crate::server::CycloneServer`] (one per accepted peer), the same split
//! Cyclone.Unity's `CycloneConnection` has and for the same reason: client
//! and server share everything past "how the socket was obtained".
//!
//! # A background thread, not async
//!
//! This crate has no async runtime dependency (see the crate root docs for
//! why), so a blocking `TcpStream::read` cannot simply be awaited between
//! [`CycloneConnection::poll`] calls the way Cyclone.Unity's C# or
//! cyclone-godot's poll-based sockets can. Instead, [`CycloneConnection::new`]
//! spawns one background thread that blocks on reads and pushes decoded
//! events through an [`mpsc`] channel; [`poll`](CycloneConnection::poll)
//! only ever drains that channel and ticks the heartbeat, so it never
//! blocks and is safe to call from a single-threaded game loop the same way
//! Cyclone.Unity's `Pump()` and cyclone-godot's `poll()` are.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::heartbeat::CycloneHeartbeat;
use crate::protocol::{self, system_message, CycloneMessage};

/// What [`CycloneConnection::poll`] hands back - the Rust counterpart of
/// Cyclone.Unity's `MessageReceived`/`PingReceived`/`PongReceived`/
/// `Disconnected` events.
#[derive(Debug)]
pub enum ConnectionEvent {
    MessageReceived(CycloneMessage),
    PingReceived,
    PongReceived,
    /// The peer disconnected, the socket errored, or
    /// [`CycloneConnection::disconnect`] was called. Fires at most once.
    Disconnected,
}

/// What the background reader thread hands to [`CycloneConnection::poll`],
/// before Ping/Pong have been intercepted.
enum RawEvent {
    Message(CycloneMessage),
    Ping,
    Pong,
    Disconnected,
}

pub struct CycloneConnection {
    write_stream: TcpStream,
    heartbeat: CycloneHeartbeat,
    events_rx: Receiver<RawEvent>,
    connected: Arc<AtomicBool>,
    disconnected_reported: bool,
}

impl CycloneConnection {
    /// Takes ownership of an already-connected `stream` and starts its
    /// background reader thread.
    pub fn new(
        stream: TcpStream,
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
    ) -> std::io::Result<Self> {
        let read_stream = stream.try_clone()?;
        let connected = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel();

        let reader_connected = Arc::clone(&connected);
        thread::spawn(move || reader_loop(read_stream, tx, reader_connected));

        Ok(Self {
            write_stream: stream,
            heartbeat: CycloneHeartbeat::new(heartbeat_interval, heartbeat_timeout),
            events_rx: rx,
            connected,
            disconnected_reported: false,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Sends `message` immediately - not queued, not batched. Blocks only
    /// on the local socket buffer filling up, the same as any other
    /// blocking `write`.
    pub fn send(&mut self, message: &CycloneMessage) -> std::io::Result<()> {
        let frame = protocol::encode(message);
        self.write_stream.write_all(&frame)
    }

    pub fn disconnect(&mut self) {
        self.connected.store(false, Ordering::SeqCst);
        let _ = self.write_stream.shutdown(std::net::Shutdown::Both);
    }

    /// Drains whatever the background reader thread has queued since the
    /// last call, replies to any Ping with a Pong, and ticks the heartbeat.
    /// Never blocks. Call this once a tick/frame - see this module's own
    /// docs for why nothing here needs the caller to be async.
    pub fn poll(&mut self) -> Vec<ConnectionEvent> {
        let mut events = Vec::new();

        loop {
            match self.events_rx.try_recv() {
                Ok(RawEvent::Message(message)) => {
                    events.push(ConnectionEvent::MessageReceived(message));
                }
                Ok(RawEvent::Ping) => {
                    events.push(ConnectionEvent::PingReceived);
                    let _ = self.send(&CycloneMessage::new(system_message::PONG, Vec::new()));
                }
                Ok(RawEvent::Pong) => {
                    self.heartbeat.mark_pong();
                    events.push(ConnectionEvent::PongReceived);
                }
                Ok(RawEvent::Disconnected) => {
                    self.connected.store(false, Ordering::SeqCst);
                    if !self.disconnected_reported {
                        self.disconnected_reported = true;
                        events.push(ConnectionEvent::Disconnected);
                    }
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        if self.is_connected() {
            if self.heartbeat.is_timeout() {
                self.disconnect();
                if !self.disconnected_reported {
                    self.disconnected_reported = true;
                    events.push(ConnectionEvent::Disconnected);
                }
            } else if self.heartbeat.should_ping() {
                let _ = self.send(&CycloneMessage::new(system_message::PING, Vec::new()));
            }
        }

        events
    }
}

/// Runs on its own thread for the lifetime of one connection: blocks on
/// reads, extracts frames from the accumulated buffer (resyncing on the
/// magic bytes the same way Cyclone.Unity's `ProcessReceiveBufferAsync` and
/// cyclone-godot's `_process_receive_buffer` do), and forwards decoded
/// messages - never touching anything the caller's own thread owns except
/// through `tx` and `connected`.
fn reader_loop(mut stream: TcpStream, tx: Sender<RawEvent>, connected: Arc<AtomicBool>) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 64 * 1024];

    'read: loop {
        let read = match stream.read(&mut read_buf) {
            Ok(0) => break, // peer closed cleanly
            Ok(n) => n,
            Err(_) => break, // reset, timeout, or anything else: treat as disconnected
        };
        buffer.extend_from_slice(&read_buf[..read]);

        loop {
            if buffer.len() < protocol::HEADER_SIZE {
                break;
            }

            let magic_index = find_magic(&buffer);
            let Some(magic_index) = magic_index else {
                keep_possible_magic_prefix(&mut buffer);
                break;
            };
            if magic_index > 0 {
                buffer.drain(0..magic_index);
            }
            if buffer.len() < protocol::HEADER_SIZE {
                break;
            }

            let payload_length = u32::from_le_bytes(
                buffer[protocol::MAGIC_SIZE + protocol::MESSAGE_ID_SIZE..protocol::HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            if payload_length > protocol::MAX_PAYLOAD_LENGTH {
                break 'read; // peer violated the size limit: drop the connection
            }

            let frame_length = protocol::HEADER_SIZE + payload_length;
            if buffer.len() < frame_length {
                break; // not a full frame yet
            }

            let frame: Vec<u8> = buffer.drain(0..frame_length).collect();
            let Some(message) = protocol::try_decode(&frame) else {
                break 'read; // internally inconsistent: cannot happen from the checks above, but never guess
            };

            let event = match message.id {
                system_message::PING => RawEvent::Ping,
                system_message::PONG => RawEvent::Pong,
                _ => RawEvent::Message(message),
            };
            if tx.send(event).is_err() {
                return; // the CycloneConnection was dropped; nothing left to report to
            }
        }
    }

    connected.store(false, Ordering::SeqCst);
    let _ = tx.send(RawEvent::Disconnected);
}

fn find_magic(buffer: &[u8]) -> Option<usize> {
    if buffer.len() < 2 {
        return None;
    }
    buffer
        .windows(2)
        .position(|window| window == protocol::MAGIC)
}

fn keep_possible_magic_prefix(buffer: &mut Vec<u8>) {
    let last = buffer.last().copied();
    buffer.clear();
    if last == Some(protocol::MAGIC[0]) {
        buffer.push(protocol::MAGIC[0]);
    }
}
