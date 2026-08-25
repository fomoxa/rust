use std::fmt;
use std::slice;

use crate::handshake::HandshakeFailure;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub u64);

impl PeerId {
    pub const LOCAL: PeerId = PeerId(0);
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer#{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disconnect {
    PeerClosed,
    TransportError,
    Unresponsive,
    Local,
}

impl fmt::Display for Disconnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Disconnect::PeerClosed => "the peer closed the connection",
            Disconnect::TransportError => "the connection broke",
            Disconnect::Unresponsive => "the peer stopped answering probes",
            Disconnect::Local => "the session was closed locally",
        };
        f.write_str(text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event<'a> {
    Connected,
    Ready,
    HandshakeFailed(HandshakeFailure),
    Message { id: u32, payload: &'a [u8] },
    Probe,
    Ack,
    Disconnected(Disconnect),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerEvent<'a> {
    pub peer: PeerId,
    pub event: Event<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Record {
    Connected(PeerId),
    Ready(PeerId),
    HandshakeFailed(PeerId, HandshakeFailure),
    Message { peer: PeerId, id: u32, start: usize, len: usize },
    Probe(PeerId),
    Ack(PeerId),
    Disconnected(PeerId, Disconnect),
}

#[derive(Debug, Default)]
pub struct EventSink {
    arena: Vec<u8>,
    records: Vec<Record>,
}

impl EventSink {
    pub fn new() -> EventSink {
        EventSink { arena: Vec::new(), records: Vec::new() }
    }

    pub fn clear(&mut self) {
        self.arena.clear();
        self.records.clear();
    }

    pub fn shrink_to_fit(&mut self) {
        self.arena.shrink_to_fit();
        self.records.shrink_to_fit();
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn push(&mut self, peer: PeerId, event: Event<'_>) {
        let record = match event {
            Event::Connected => Record::Connected(peer),
            Event::Ready => Record::Ready(peer),
            Event::HandshakeFailed(reason) => Record::HandshakeFailed(peer, reason),
            Event::Probe => Record::Probe(peer),
            Event::Ack => Record::Ack(peer),
            Event::Disconnected(reason) => Record::Disconnected(peer, reason),
            Event::Message { id, payload } => {
                let start = self.arena.len();
                self.arena.extend_from_slice(payload);
                Record::Message { peer, id, start, len: payload.len() }
            }
        };
        self.records.push(record);
    }

    pub fn peer_events(&self) -> PeerEvents<'_> {
        PeerEvents { records: self.records.iter(), arena: &self.arena }
    }

    pub fn events(&self) -> Events<'_> {
        Events { inner: self.peer_events() }
    }
}

#[derive(Debug, Clone)]
pub struct PeerEvents<'a> {
    records: slice::Iter<'a, Record>,
    arena: &'a [u8],
}

impl<'a> Iterator for PeerEvents<'a> {
    type Item = PeerEvent<'a>;

    fn next(&mut self) -> Option<PeerEvent<'a>> {
        let record = *self.records.next()?;
        let (peer, event) = match record {
            Record::Connected(peer) => (peer, Event::Connected),
            Record::Ready(peer) => (peer, Event::Ready),
            Record::HandshakeFailed(peer, reason) => (peer, Event::HandshakeFailed(reason)),
            Record::Probe(peer) => (peer, Event::Probe),
            Record::Ack(peer) => (peer, Event::Ack),
            Record::Disconnected(peer, reason) => (peer, Event::Disconnected(reason)),
            Record::Message { peer, id, start, len } => {
                (peer, Event::Message { id, payload: &self.arena[start..start + len] })
            }
        };
        Some(PeerEvent { peer, event })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.records.size_hint()
    }
}

impl ExactSizeIterator for PeerEvents<'_> {}

#[derive(Debug, Clone)]
pub struct Events<'a> {
    inner: PeerEvents<'a>,
}

impl<'a> Iterator for Events<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Event<'a>> {
        self.inner.next().map(|peer_event| peer_event.event)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for Events<'_> {}
