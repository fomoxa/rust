use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::event::{Disconnect, Event};
use crate::frame::Frame;
use crate::handshake::{
    self, Decision, HandshakeFailure, QueryItem, Verdict, PROTOCOL_VERSION, QUERY_TAG,
};
use crate::schema::Schema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Handshaking,
    Ready,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub handshake_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub max_frames_per_tick: usize,
    pub recv_buffer_size: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            handshake_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
            max_frames_per_tick: 64,
            recv_buffer_size: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Out {
    Probe,
    Ack,
    Handshake(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction<'a> {
    pub out: Option<Out>,
    pub event: Option<Event<'a>>,
}

impl<'a> Reaction<'a> {
    fn idle() -> Reaction<'a> {
        Reaction { out: None, event: None }
    }

    fn out(out: Out) -> Reaction<'a> {
        Reaction { out: Some(out), event: None }
    }

    fn event(event: Event<'a>) -> Reaction<'a> {
        Reaction { out: None, event: Some(event) }
    }

    fn both(out: Out, event: Event<'a>) -> Reaction<'a> {
        Reaction { out: Some(out), event: Some(event) }
    }
}

#[derive(Debug)]
pub struct Session {
    role: Role,
    schema: Arc<Schema>,
    config: Config,
    state: SessionState,
    started: Instant,
    last_activity: Instant,
    probe_sent: Option<Instant>,
    terminated: bool,
    query_seen: bool,
    asked: Option<Vec<QueryItem>>,
}

impl Session {
    pub fn new(
        role: Role,
        schema: Arc<Schema>,
        config: Config,
        now: Instant,
    ) -> (Session, Option<Out>) {
        let opening = match role {
            Role::Client => Some(Out::Handshake(handshake::encode_hello(&schema))),
            Role::Server => None,
        };
        let session = Session {
            role,
            schema,
            config,
            state: SessionState::Handshaking,
            started: now,
            last_activity: now,
            probe_sent: None,
            terminated: false,
            query_seen: false,
            asked: None,
        };
        (session, opening)
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn is_ready(&self) -> bool {
        self.state == SessionState::Ready
    }

    pub fn is_closed(&self) -> bool {
        self.state == SessionState::Closed
    }

    pub fn close(&mut self) {
        self.state = SessionState::Closed;
        self.terminated = true;
    }

    pub fn on_frame<'a>(&mut self, frame: Frame<'a>, now: Instant) -> Reaction<'a> {
        if self.state == SessionState::Closed {
            return Reaction::idle();
        }

        self.last_activity = now;
        self.probe_sent = None;

        match frame {
            Frame::Probe => {
                Reaction { out: Some(Out::Ack), event: self.is_ready().then_some(Event::Probe) }
            }
            Frame::Ack => Reaction { out: None, event: self.is_ready().then_some(Event::Ack) },
            Frame::Data { message_id, payload } => Reaction {
                out: None,
                event: self.is_ready().then_some(Event::Message { id: message_id, payload }),
            },
            Frame::Handshake(payload) => {
                if self.state != SessionState::Handshaking {
                    return Reaction::idle();
                }
                match self.role {
                    Role::Client => self.client_handshake(payload),
                    Role::Server => self.server_handshake(payload),
                }
            }
        }
    }

    pub fn tick(&mut self, now: Instant) -> Reaction<'static> {
        if self.state == SessionState::Closed {
            return Reaction::idle();
        }

        if self.role == Role::Client && self.state == SessionState::Handshaking {
            if now.saturating_duration_since(self.started) >= self.config.handshake_timeout {
                return self.fail(HandshakeFailure::Timeout);
            }
            return Reaction::idle();
        }

        if let Some(sent) = self.probe_sent {
            if now.saturating_duration_since(sent) >= self.config.heartbeat_timeout {
                self.state = SessionState::Closed;
                self.terminated = true;
                return Reaction::event(Event::Disconnected(Disconnect::Unresponsive));
            }
            return Reaction::idle();
        }

        let silence = match (self.role, self.state) {
            (Role::Server, SessionState::Handshaking) => self.config.handshake_timeout,
            _ => self.config.heartbeat_interval,
        };
        if now.saturating_duration_since(self.last_activity) >= silence {
            self.probe_sent = Some(now);
            return Reaction::out(Out::Probe);
        }

        Reaction::idle()
    }

    pub fn on_transport_closed(&mut self, reason: Disconnect) -> Reaction<'static> {
        self.state = SessionState::Closed;
        if self.terminated {
            return Reaction::idle();
        }
        self.terminated = true;
        Reaction::event(Event::Disconnected(reason))
    }

    fn client_handshake(&mut self, payload: &[u8]) -> Reaction<'static> {
        if payload.first() == Some(&QUERY_TAG) {
            if self.query_seen {
                return self.fail(HandshakeFailure::MalformedPeer);
            }
            self.query_seen = true;
            let Some(items) = handshake::decode_query(payload) else {
                return self.fail(HandshakeFailure::MalformedPeer);
            };
            let Some(reply) = handshake::answer_query(&self.schema, &items) else {
                return self.fail(HandshakeFailure::MalformedPeer);
            };
            return Reaction::out(Out::Handshake(handshake::encode_reply(&reply)));
        }

        if payload.len() != 1 {
            return self.fail(HandshakeFailure::MalformedPeer);
        }
        let Some(verdict) = Verdict::from_byte(payload[0]) else {
            return self.fail(HandshakeFailure::MalformedPeer);
        };
        match HandshakeFailure::from_verdict(verdict) {
            None => {
                self.state = SessionState::Ready;
                Reaction::event(Event::Ready)
            }
            Some(reason) => self.fail(reason),
        }
    }

    fn server_handshake(&mut self, payload: &[u8]) -> Reaction<'static> {
        if let Some(asked) = self.asked.take() {
            let Some(reply) = handshake::decode_reply(payload) else {
                return self.reject(Verdict::MalformedHello);
            };
            return match handshake::check_reply(&self.schema, &asked, &reply) {
                Verdict::Accept => self.accept(),
                other => self.reject(other),
            };
        }

        let hello = match handshake::decode_hello(payload) {
            Ok(hello) => hello,
            Err(verdict) => return self.reject(verdict),
        };
        if hello.version != PROTOCOL_VERSION {
            return self.reject(Verdict::WrongVersion);
        }
        match handshake::decide(&self.schema, &hello) {
            Decision::Accept => self.accept(),
            Decision::Reject(verdict) => self.reject(verdict),
            Decision::Query(items) => {
                let query = handshake::encode_query(&items);
                self.asked = Some(items);
                Reaction::out(Out::Handshake(query))
            }
        }
    }

    fn accept(&mut self) -> Reaction<'static> {
        self.state = SessionState::Ready;
        Reaction::both(Out::Handshake(handshake::encode_verdict(Verdict::Accept)), Event::Ready)
    }

    fn reject(&mut self, verdict: Verdict) -> Reaction<'static> {
        self.state = SessionState::Closed;
        self.terminated = true;
        let reason =
            HandshakeFailure::from_verdict(verdict).unwrap_or(HandshakeFailure::MalformedPeer);
        Reaction::both(
            Out::Handshake(handshake::encode_verdict(verdict)),
            Event::HandshakeFailed(reason),
        )
    }

    fn fail(&mut self, reason: HandshakeFailure) -> Reaction<'static> {
        self.state = SessionState::Closed;
        self.terminated = true;
        Reaction::event(Event::HandshakeFailed(reason))
    }
}
