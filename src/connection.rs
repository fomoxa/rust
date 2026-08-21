use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::event::{Disconnect, Event, EventSink, Events, PeerId};
use crate::frame::{self, StreamDecoder};
use crate::schema::Schema;
use crate::session::{Config, Out, Reaction, Role, Session, SessionState};
use crate::transport::{RecvOutcome, SendOutcome, Transport, TransportKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    NotReady,
    Congested,
    TooLarge,
    Closed,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            SendError::NotReady => "the handshake has not been accepted yet",
            SendError::Congested => "a frame is still waiting to go out",
            SendError::TooLarge => "the transport cannot carry a frame this large",
            SendError::Closed => "the session is closed",
        };
        f.write_str(text)
    }
}

impl std::error::Error for SendError {}

#[derive(Debug)]
struct Outbox {
    bytes: Vec<u8>,
    offset: usize,
}

#[derive(Debug)]
struct Wire<T: Transport> {
    transport: T,
    outbox: Option<Outbox>,
    dead: Option<Disconnect>,
    released: bool,
}

impl<T: Transport> Wire<T> {
    fn new(transport: T) -> Wire<T> {
        Wire { transport, outbox: None, dead: None, released: false }
    }

    fn is_dead(&self) -> bool {
        self.dead.is_some()
    }

    fn is_congested(&self) -> bool {
        self.outbox.is_some()
    }

    fn kill(&mut self, reason: Disconnect) {
        if self.dead.is_none() {
            self.dead = Some(reason);
        }
    }

    fn shutdown(&mut self) {
        if self.dead.is_none() {
            self.dead = Some(Disconnect::Local);
        }
        self.outbox = None;
        self.transport.close_soft();
    }

    fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.transport.close_hard();
        }
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        self.transport.recv(buffer)
    }

    fn flush(&mut self) {
        let Wire { transport, outbox, dead, .. } = self;
        loop {
            let Some(pending) = outbox.as_mut() else { return };
            if pending.offset >= pending.bytes.len() {
                *outbox = None;
                return;
            }
            match transport.send(&pending.bytes[pending.offset..]) {
                SendOutcome::Sent => {
                    *outbox = None;
                    return;
                }
                SendOutcome::Partial(0) | SendOutcome::WouldBlock => return,
                SendOutcome::Partial(count) => pending.offset += count,
                SendOutcome::TooLarge => {
                    *outbox = None;
                    return;
                }
                SendOutcome::Closed => {
                    if dead.is_none() {
                        *dead = Some(Disconnect::PeerClosed);
                    }
                    return;
                }
                SendOutcome::Error(_) => {
                    if dead.is_none() {
                        *dead = Some(Disconnect::TransportError);
                    }
                    return;
                }
            }
        }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), SendError> {
        match self.transport.send(bytes) {
            SendOutcome::Sent => Ok(()),
            SendOutcome::Partial(count) if count < bytes.len() => {
                self.outbox = Some(Outbox { bytes: bytes[count..].to_vec(), offset: 0 });
                Ok(())
            }
            SendOutcome::Partial(_) => Ok(()),
            SendOutcome::WouldBlock => {
                self.outbox = Some(Outbox { bytes: bytes.to_vec(), offset: 0 });
                Ok(())
            }
            SendOutcome::TooLarge => Err(SendError::TooLarge),
            SendOutcome::Closed => {
                self.kill(Disconnect::PeerClosed);
                Err(SendError::Closed)
            }
            SendOutcome::Error(_) => {
                self.kill(Disconnect::TransportError);
                Err(SendError::Closed)
            }
        }
    }

    fn send_message(&mut self, bytes: &[u8]) -> Result<(), SendError> {
        if self.is_dead() {
            return Err(SendError::Closed);
        }
        if self.is_congested() {
            return Err(SendError::Congested);
        }
        self.write(bytes)
    }

    fn send_control(&mut self, bytes: &[u8]) {
        if self.is_dead() {
            return;
        }
        if let Some(pending) = self.outbox.as_mut() {
            pending.bytes.extend_from_slice(bytes);
            return;
        }
        let _ = self.write(bytes);
    }
}

#[derive(Debug)]
enum Intake {
    Stream(StreamDecoder),
    Message,
}

enum Pump {
    Idle,
    Fed,
    Handled { out: Option<Out>, failed: bool },
}

#[derive(Debug)]
pub(crate) struct Core<T: Transport> {
    wire: Wire<T>,
    intake: Intake,
    session: Session,
    recv_buf: Vec<u8>,
    scratch: Vec<u8>,
    announced: bool,
}

impl<T: Transport> Core<T> {
    pub(crate) fn new(
        transport: T,
        schema: Arc<Schema>,
        config: Config,
        role: Role,
        now: Instant,
    ) -> Core<T> {
        let intake = match transport.kind() {
            TransportKind::Stream => Intake::Stream(StreamDecoder::new()),
            TransportKind::Message => Intake::Message,
        };
        let (session, opening) = Session::new(role, schema, config, now);
        let mut core = Core {
            wire: Wire::new(transport),
            intake,
            session,
            recv_buf: vec![0; config.recv_buffer_size.max(frame::DATA_HEADER_LEN)],
            scratch: Vec::new(),
            announced: false,
        };
        if let Some(out) = opening {
            core.emit(&out);
        }
        core
    }

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    pub(crate) fn transport(&self) -> &T {
        &self.wire.transport
    }

    pub(crate) fn transport_mut(&mut self) -> &mut T {
        &mut self.wire.transport
    }

    pub(crate) fn is_congested(&self) -> bool {
        self.wire.is_congested()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.session.is_closed() && self.wire.is_dead()
    }

    pub(crate) fn close(&mut self) {
        self.session.close();
        self.wire.shutdown();
    }

    pub(crate) fn send(&mut self, message_id: u32, payload: &[u8]) -> Result<(), SendError> {
        if !self.session.is_ready() {
            return Err(SendError::NotReady);
        }
        self.scratch.clear();
        frame::encode_data(message_id, payload, &mut self.scratch)
            .map_err(|_| SendError::TooLarge)?;
        self.wire.send_message(&self.scratch)
    }

    pub(crate) fn tick(&mut self, now: Instant, peer: PeerId, sink: &mut EventSink) {
        if !self.announced {
            self.announced = true;
            sink.push(peer, Event::Connected);
        }

        if !self.wire.is_dead() {
            self.wire.flush();
        }
        if !self.wire.is_dead() {
            self.drain(now, peer, sink);
        }
        if !self.wire.is_dead() {
            let reaction = self.session.tick(now);
            self.apply(reaction, peer, sink);
        }
        if let Some(reason) = self.wire.dead {
            if self.session.state() != SessionState::Closed {
                let reaction = self.session.on_transport_closed(reason);
                self.apply(reaction, peer, sink);
            }
        }
    }

    fn apply(&mut self, reaction: Reaction<'_>, peer: PeerId, sink: &mut EventSink) {
        if let Some(out) = reaction.out {
            self.emit(&out);
        }
        if let Some(event) = reaction.event {
            sink.push(peer, event);
            if matches!(event, Event::HandshakeFailed(_)) {
                self.wire.shutdown();
            }
        }
    }

    fn emit(&mut self, out: &Out) {
        self.scratch.clear();
        match out {
            Out::Probe => frame::encode_probe(&mut self.scratch),
            Out::Ack => frame::encode_ack(&mut self.scratch),
            Out::Handshake(payload) => {
                if frame::encode_handshake(payload, &mut self.scratch).is_err() {
                    return;
                }
            }
        }
        self.wire.send_control(&self.scratch);
    }

    fn drain(&mut self, now: Instant, peer: PeerId, sink: &mut EventSink) {
        let mut frames = self.session.config().max_frames_per_tick;
        let mut reads = frames;
        while frames > 0 && reads > 0 && !self.wire.is_dead() {
            let pump = {
                let Core { wire, intake, session, recv_buf, .. } = self;
                match intake {
                    Intake::Stream(decoder) => {
                        pump_stream(wire, decoder, session, recv_buf, sink, peer, now)
                    }
                    Intake::Message => pump_message(wire, session, recv_buf, sink, peer, now),
                }
            };
            match pump {
                Pump::Idle => break,
                Pump::Fed => reads -= 1,
                Pump::Handled { out, failed } => {
                    if let Some(out) = out {
                        self.emit(&out);
                    }
                    if failed {
                        self.wire.shutdown();
                    }
                    frames -= 1;
                }
            }
        }
    }
}

impl<T: Transport> Drop for Core<T> {
    fn drop(&mut self) {
        self.wire.release();
    }
}

fn pump_stream<T: Transport>(
    wire: &mut Wire<T>,
    decoder: &mut StreamDecoder,
    session: &mut Session,
    recv_buf: &mut Vec<u8>,
    sink: &mut EventSink,
    peer: PeerId,
    now: Instant,
) -> Pump {
    match decoder.next_len() {
        Err(_) => {
            wire.kill(Disconnect::TransportError);
            return Pump::Idle;
        }
        Ok(Some(len)) => {
            let Ok(frame) = decoder.frame(len) else {
                wire.kill(Disconnect::TransportError);
                return Pump::Idle;
            };
            let Reaction { out, event } = session.on_frame(frame, now);
            let mut failed = false;
            if let Some(event) = event {
                failed = matches!(event, Event::HandshakeFailed(_));
                sink.push(peer, event);
            }
            decoder.advance(len);
            return Pump::Handled { out, failed };
        }
        Ok(None) => {}
    }

    match wire.recv(recv_buf) {
        RecvOutcome::Received(count) => {
            decoder.feed(&recv_buf[..count]);
            Pump::Fed
        }
        RecvOutcome::WouldBlock => Pump::Idle,
        RecvOutcome::NeedCapacity(needed) => {
            recv_buf.resize(needed, 0);
            Pump::Fed
        }
        RecvOutcome::Closed => {
            wire.kill(Disconnect::PeerClosed);
            Pump::Idle
        }
        RecvOutcome::Error(_) => {
            wire.kill(Disconnect::TransportError);
            Pump::Idle
        }
    }
}

fn pump_message<T: Transport>(
    wire: &mut Wire<T>,
    session: &mut Session,
    recv_buf: &mut Vec<u8>,
    sink: &mut EventSink,
    peer: PeerId,
    now: Instant,
) -> Pump {
    match wire.recv(recv_buf) {
        RecvOutcome::Received(count) => {
            let Ok(frame) = frame::decode_packet(&recv_buf[..count]) else {
                return Pump::Handled { out: None, failed: false };
            };
            let Reaction { out, event } = session.on_frame(frame, now);
            let mut failed = false;
            if let Some(event) = event {
                failed = matches!(event, Event::HandshakeFailed(_));
                sink.push(peer, event);
            }
            Pump::Handled { out, failed }
        }
        RecvOutcome::WouldBlock => Pump::Idle,
        RecvOutcome::NeedCapacity(needed) => {
            recv_buf.resize(needed, 0);
            Pump::Fed
        }
        RecvOutcome::Closed => {
            wire.kill(Disconnect::PeerClosed);
            Pump::Idle
        }
        RecvOutcome::Error(_) => {
            wire.kill(Disconnect::TransportError);
            Pump::Idle
        }
    }
}

#[derive(Debug)]
pub struct Connection<T: Transport> {
    core: Core<T>,
    sink: EventSink,
}

impl<T: Transport> Connection<T> {
    pub fn new(transport: T, schema: Arc<Schema>, config: Config) -> Connection<T> {
        Connection::at(transport, schema, config, Instant::now())
    }

    pub fn at(transport: T, schema: Arc<Schema>, config: Config, now: Instant) -> Connection<T> {
        Connection {
            core: Core::new(transport, schema, config, Role::Client, now),
            sink: EventSink::new(),
        }
    }

    pub fn tick(&mut self, now: Instant) -> Events<'_> {
        let Connection { core, sink } = self;
        sink.clear();
        core.tick(now, PeerId::LOCAL, sink);
        sink.events()
    }

    pub fn tick_now(&mut self) -> Events<'_> {
        self.tick(Instant::now())
    }

    pub fn send(&mut self, message_id: u32, payload: &[u8]) -> Result<(), SendError> {
        self.core.send(message_id, payload)
    }

    pub fn state(&self) -> SessionState {
        self.core.session().state()
    }

    pub fn is_ready(&self) -> bool {
        self.core.session().is_ready()
    }

    pub fn is_congested(&self) -> bool {
        self.core.is_congested()
    }

    pub fn schema(&self) -> &Schema {
        self.core.session().schema()
    }

    pub fn transport(&self) -> &T {
        self.core.transport()
    }

    pub fn transport_mut(&mut self) -> &mut T {
        self.core.transport_mut()
    }

    pub fn close(&mut self) {
        self.core.close();
    }
}
