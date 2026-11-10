use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fomoxa_net::connection::Connection;
use fomoxa_net::event::{Disconnect, Event, PeerId};
use fomoxa_net::frame::{
    self, Frame, MAX_HANDSHAKE_PARTS, MAX_HANDSHAKE_PAYLOAD, PACKET_HANDSHAKE_BUDGET,
    PART_CONTENT_LEN, PART_HEADER_LEN, TYPE_HANDSHAKE, TYPE_HANDSHAKE_PART, TYPE_PROBE,
};
use fomoxa_net::handshake::{self, HandshakeFailure, Verdict, PROTOCOL_VERSION};
use fomoxa_net::schema::{MessageSchema, Schema};
use fomoxa_net::server::Server;
use fomoxa_net::session::{Config, SessionState};
use fomoxa_net::transport::{
    Accept, RecvOutcome, SendOutcome, ServerTransport, Transport, TransportKind,
};

#[derive(Debug)]
struct State {
    kind: TransportKind,
    sent: Vec<Vec<u8>>,
    incoming: VecDeque<Vec<u8>>,
    blocked: bool,
}

#[derive(Debug, Clone)]
struct Fake {
    state: Rc<RefCell<State>>,
}

impl Fake {
    fn of(kind: TransportKind) -> Fake {
        Fake {
            state: Rc::new(RefCell::new(State {
                kind,
                sent: Vec::new(),
                incoming: VecDeque::new(),
                blocked: false,
            })),
        }
    }

    fn packet() -> Fake {
        Fake::of(TransportKind::Message)
    }

    fn deliver<'a>(&self, packets: impl IntoIterator<Item = &'a Vec<u8>>) {
        let mut state = self.state.borrow_mut();
        for packet in packets {
            state.incoming.push_back(packet.clone());
        }
    }

    fn take_sent(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.state.borrow_mut().sent)
    }

    fn sent_count(&self) -> usize {
        self.state.borrow().sent.len()
    }

    fn block(&self, blocked: bool) {
        self.state.borrow_mut().blocked = blocked;
    }
}

impl Transport for Fake {
    fn kind(&self) -> TransportKind {
        self.state.borrow().kind
    }

    fn send(&mut self, bytes: &[u8]) -> SendOutcome {
        let mut state = self.state.borrow_mut();
        if state.blocked {
            return SendOutcome::WouldBlock;
        }
        state.sent.push(bytes.to_vec());
        SendOutcome::Sent
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        let mut state = self.state.borrow_mut();
        let Some(front) = state.incoming.front() else {
            return RecvOutcome::WouldBlock;
        };
        if front.len() > buffer.len() {
            return RecvOutcome::NeedCapacity(front.len());
        }
        let count = front.len();
        buffer[..count].copy_from_slice(front);
        state.incoming.pop_front();
        RecvOutcome::Received(count)
    }

    fn close_soft(&mut self) {}

    fn close_hard(&mut self) {}
}

struct Listener {
    waiting: Option<Fake>,
}

impl ServerTransport for Listener {
    type Peer = Fake;

    fn accept(&mut self) -> Accept<Fake> {
        match self.waiting.take() {
            Some(peer) => Accept::Accepted(peer),
            None => Accept::Pending,
        }
    }

    fn close(&mut self) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Connected,
    Ready,
    Failed(HandshakeFailure),
    Probe,
    Ack,
    Message(u32),
    Disconnected(Disconnect),
}

fn seen(event: Event<'_>) -> Seen {
    match event {
        Event::Connected => Seen::Connected,
        Event::Ready => Seen::Ready,
        Event::HandshakeFailed(reason) => Seen::Failed(reason),
        Event::Probe => Seen::Probe,
        Event::Ack => Seen::Ack,
        Event::Message { id, .. } => Seen::Message(id),
        Event::Disconnected(reason) => Seen::Disconnected(reason),
    }
}

fn messages(count: u32) -> Arc<Schema> {
    let messages: Vec<_> =
        (1..=count).map(|id| MessageSchema::new(id, u64::from(id), vec![u64::from(id)])).collect();
    Arc::new(Schema::new(0xC0FF_EE00 + u64::from(count), messages).unwrap())
}

fn hello_len(count: usize) -> usize {
    handshake::HELLO_HEADER_LEN + handshake::HELLO_ENTRY_LEN * count
}

fn parts_for(content: usize) -> usize {
    (content + PART_CONTENT_LEN - 1) / PART_CONTENT_LEN
}

fn part(index: usize, count: usize, content: &[u8]) -> Vec<u8> {
    let mut out = vec![TYPE_HANDSHAKE_PART];
    out.extend_from_slice(&(index as u16).to_le_bytes());
    out.extend_from_slice(&(count as u16).to_le_bytes());
    out.extend_from_slice(&(content.len() as u32).to_le_bytes());
    out.extend_from_slice(content);
    out
}

fn split(body: &[u8]) -> Vec<Vec<u8>> {
    let count = parts_for(body.len());
    body.chunks(PART_CONTENT_LEN)
        .enumerate()
        .map(|(index, chunk)| part(index, count, chunk))
        .collect()
}

fn whole(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::encode_handshake(body, &mut out).unwrap();
    out
}

fn u16_at(bytes: &[u8], at: usize) -> usize {
    usize::from(u16::from_le_bytes([bytes[at], bytes[at + 1]]))
}

fn u32_at(bytes: &[u8], at: usize) -> usize {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize
}

fn client(schema: Arc<Schema>, wire: &Fake, now: Instant) -> Connection<Fake> {
    let mut connection = Connection::at(wire.clone(), schema, Config::default(), now);
    connection.tick(now).for_each(drop);
    connection
}

fn hello_parts(schema: &Arc<Schema>) -> Vec<Vec<u8>> {
    let wire = Fake::packet();
    let _connection = client(schema.clone(), &wire, Instant::now());
    wire.take_sent()
}

struct Peer {
    server: Server<Listener>,
    wire: Fake,
    id: PeerId,
}

impl Peer {
    fn tick(&mut self, now: Instant) -> Vec<Seen> {
        self.server.tick(now).map(|event| seen(event.event)).collect()
    }

    fn state(&self) -> Option<SessionState> {
        self.server.state(self.id)
    }
}

fn server_with(schema: Arc<Schema>, config: Config, kind: TransportKind, now: Instant) -> Peer {
    let wire = Fake::of(kind);
    let mut server = Server::new(Listener { waiting: Some(wire.clone()) }, schema, config);
    let first: Vec<_> = server.tick(now).collect();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event, Event::Connected);
    let id = first[0].peer;
    Peer { server, wire, id }
}

fn server(schema: Arc<Schema>, now: Instant) -> Peer {
    server_with(schema, Config::default(), TransportKind::Message, now)
}

fn last_verdict(wire: &Fake) -> Verdict {
    let sent = wire.take_sent();
    let last = sent.last().expect("nothing was sent");
    match frame::decode_packet(last) {
        Ok(Frame::Handshake(&[byte])) => Verdict::from_byte(byte).unwrap(),
        other => panic!("the last packet is {other:?}, not a one-byte verdict"),
    }
}

fn refused_with(schema: Arc<Schema>, packets: &[Vec<u8>]) {
    let now = Instant::now();
    let mut peer = server(schema, now);
    peer.wire.deliver(packets);
    assert_eq!(peer.tick(now), vec![Seen::Failed(HandshakeFailure::MalformedHello)]);
    assert_eq!(last_verdict(&peer.wire), Verdict::MalformedHello);
}

#[test]
fn a_hello_of_62_messages_leaves_as_one_handshake_frame() {
    let sent = hello_parts(&messages(62));

    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0][0], TYPE_HANDSHAKE);
    assert_eq!(sent[0].len(), frame::HANDSHAKE_HEADER_LEN + hello_len(62));
    assert!(sent[0].len() <= PACKET_HANDSHAKE_BUDGET);
}

#[test]
fn a_hello_of_63_messages_on_a_packet_transport_leaves_in_two_parts() {
    let schema = messages(63);
    let sent = hello_parts(&schema);

    assert_eq!(sent.len(), 2);
    let (first, second) = (&sent[0], &sent[1]);
    assert_eq!((first[0], second[0]), (TYPE_HANDSHAKE_PART, TYPE_HANDSHAKE_PART));
    assert_eq!((u16_at(first, 1), u16_at(second, 1)), (0, 1));
    assert_eq!((u16_at(first, 3), u16_at(second, 3)), (2, 2));
    assert_eq!((u32_at(first, 5), u32_at(second, 5)), (891, 7));

    let joined = [&first[PART_HEADER_LEN..], &second[PART_HEADER_LEN..]].concat();
    assert_eq!(joined, handshake::encode_hello(&schema));
}

#[test]
fn every_part_is_at_most_900_bytes_and_only_the_last_is_short() {
    let sent = hello_parts(&messages(500));

    assert_eq!(sent.len(), parts_for(hello_len(500)));
    for (index, bytes) in sent.iter().enumerate() {
        assert!(bytes.len() <= PACKET_HANDSHAKE_BUDGET, "part {index} is {} bytes", bytes.len());
        if index < sent.len() - 1 {
            assert_eq!(bytes.len(), PACKET_HANDSHAKE_BUDGET, "part {index} is not the last");
        }
    }
}

#[test]
fn a_stream_transport_sends_a_large_hello_as_one_handshake_frame() {
    let wire = Fake::of(TransportKind::Stream);
    let _connection = client(messages(200), &wire, Instant::now());
    let sent = wire.take_sent();

    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0][0], TYPE_HANDSHAKE);
    assert_eq!(sent[0].len(), frame::HANDSHAKE_HEADER_LEN + hello_len(200));
}

#[test]
fn a_server_assembles_a_hello_sent_in_parts_and_accepts() {
    let now = Instant::now();
    let schema = messages(200);
    let client_wire = Fake::packet();
    let mut connection = client(schema.clone(), &client_wire, now);
    let mut peer = server(schema, now);

    peer.wire.deliver(&client_wire.take_sent());
    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    client_wire.deliver(&peer.wire.take_sent());
    let events: Vec<_> = connection.tick(now).map(seen).collect();
    assert_eq!(events, vec![Seen::Ready]);
}

#[test]
fn parts_that_arrive_in_reverse_order_are_assembled() {
    let now = Instant::now();
    let schema = messages(200);
    let mut parts = hello_parts(&schema);
    parts.reverse();
    let mut peer = server(schema, now);
    peer.wire.deliver(&parts);

    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    assert_eq!(last_verdict(&peer.wire), Verdict::Accept);
}

#[test]
fn a_repeated_identical_part_is_ignored() {
    let now = Instant::now();
    let schema = messages(200);
    let parts = hello_parts(&schema);
    let mut peer = server(schema, now);
    peer.wire.deliver([&parts[0], &parts[0]]);
    peer.wire.deliver(&parts[1..]);

    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    assert_eq!(last_verdict(&peer.wire), Verdict::Accept);
}

#[test]
fn a_part_that_differs_from_an_earlier_copy_is_refused() {
    let schema = messages(200);
    let parts = hello_parts(&schema);
    let mut altered = parts[0].clone();
    altered[20] ^= 0xFF;

    refused_with(schema, &[parts[0].clone(), altered]);
}

#[test]
fn a_part_whose_count_changes_is_refused() {
    refused_with(messages(200), &[part(0, 3, &[0; 891]), part(1, 4, &[0; 891])]);
}

#[test]
fn a_short_part_before_the_last_is_refused() {
    refused_with(messages(200), &[part(0, 3, &[0; 890])]);
}

#[test]
fn an_empty_last_part_is_refused() {
    refused_with(messages(200), &[part(0, 2, &[0; 891]), part(1, 2, &[])]);
}

#[test]
fn a_part_count_of_one_is_refused() {
    let schema = messages(62);
    let hello = handshake::encode_hello(&schema);
    refused_with(schema, &[part(0, 1, &hello)]);
}

#[test]
fn a_part_count_past_the_ceiling_is_refused() {
    refused_with(messages(200), &[part(0, MAX_HANDSHAKE_PARTS + 1, &[0; 891])]);
}

#[test]
fn a_part_index_past_the_count_is_refused() {
    refused_with(messages(200), &[part(2, 2, &[0; 891])]);
}

#[test]
fn a_part_carrying_more_than_891_bytes_is_dropped_as_a_broken_packet() {
    let now = Instant::now();
    let schema = messages(200);
    let mut peer = server(schema.clone(), now);
    peer.wire.deliver(&[part(0, 2, &[0; 892])]);

    assert_eq!(peer.tick(now), vec![]);
    assert_eq!(peer.wire.sent_count(), 0);

    peer.wire.deliver(&hello_parts(&schema));
    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    assert_eq!(last_verdict(&peer.wire), Verdict::Accept);
}

#[test]
fn a_handshake_frame_while_parts_are_pending_is_refused() {
    let schema = messages(200);
    let parts = hello_parts(&schema);
    let hello = whole(&handshake::encode_hello(&schema));

    refused_with(schema, &[parts[0].clone(), hello]);
}

#[test]
fn content_assembled_past_1_mib_is_refused() {
    let entries = 74_900usize;
    let schema = messages(1);
    let mut hello = vec![0u8; hello_len(entries)];
    hello[0..4].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    hello[4..12].copy_from_slice(&schema.fingerprint().to_le_bytes());
    hello[12..16].copy_from_slice(&(entries as u32).to_le_bytes());
    assert!(hello.len() > MAX_HANDSHAKE_PAYLOAD);
    let parts = split(&hello);
    assert!(parts.len() <= MAX_HANDSHAKE_PARTS);

    let now = Instant::now();
    let config = Config { max_frames_per_tick: 2048, ..Config::default() };
    let mut peer = server_with(schema, config, TransportKind::Message, now);
    peer.wire.deliver(&parts);

    assert_eq!(peer.tick(now), vec![Seen::Failed(HandshakeFailure::MalformedHello)]);
    assert_eq!(last_verdict(&peer.wire), Verdict::MalformedHello);
}

#[test]
fn a_part_after_the_handshake_is_dropped_and_is_not_activity() {
    let now = Instant::now();
    let schema = messages(1);
    let mut peer = server(schema.clone(), now);
    peer.wire.deliver(&[whole(&handshake::encode_hello(&schema))]);
    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    peer.wire.take_sent();

    let interval = Config::default().heartbeat_interval;
    peer.wire.deliver(&[part(0, 2, &[0; 891])]);
    assert_eq!(peer.tick(now + interval - Duration::from_millis(1)), vec![]);
    assert_eq!(peer.wire.sent_count(), 0);
    assert_eq!(peer.state(), Some(SessionState::Ready));

    peer.tick(now + interval);
    let sent = peer.wire.take_sent();
    assert_eq!(sent, vec![vec![TYPE_PROBE]], "the dropped part counted as activity");
}

#[test]
fn a_part_on_a_byte_stream_is_a_fatal_framing_error() {
    let now = Instant::now();
    let mut peer = server_with(messages(1), Config::default(), TransportKind::Stream, now);
    peer.wire.deliver(&[part(0, 2, &[0; 891])]);

    assert_eq!(peer.tick(now), vec![Seen::Disconnected(Disconnect::TransportError)]);
}

#[test]
fn a_part_type_on_a_stream_is_named_in_the_framing_error() {
    let mut decoder = frame::StreamDecoder::new();
    decoder.feed(&part(0, 2, &[0; 891]));
    let error = decoder.next_len().unwrap_err();

    assert_eq!(error, frame::FrameError::UnknownType(TYPE_HANDSHAKE_PART));
    assert!(error.to_string().contains("packet transport"), "{error}");
}

#[test]
fn the_client_fails_on_a_broken_answer_sent_in_parts() {
    let now = Instant::now();
    let wire = Fake::packet();
    let mut connection = client(messages(1), &wire, now);
    wire.deliver(&[part(0, 2, &[0; 891]), part(1, 2, &[])]);

    let events: Vec<_> = connection.tick(now).map(seen).collect();
    assert_eq!(events, vec![Seen::Failed(HandshakeFailure::MalformedPeer)]);
}

#[test]
fn a_query_and_its_reply_that_exceed_the_budget_both_travel_in_parts() {
    let now = Instant::now();
    let client_messages: Vec<_> = (1..=160u64)
        .map(|id| MessageSchema::new(id as u32, id * 10 + 1, vec![id * 10, id * 10 + 1]))
        .collect();
    let server_messages: Vec<_> =
        (1..=160u64).map(|id| MessageSchema::new(id as u32, id * 10, vec![id * 10])).collect();
    let client_wire = Fake::packet();
    let mut connection =
        client(Arc::new(Schema::new(1, client_messages).unwrap()), &client_wire, now);
    let mut peer = server(Arc::new(Schema::new(2, server_messages).unwrap()), now);

    peer.wire.deliver(&client_wire.take_sent());
    assert_eq!(peer.tick(now), vec![]);
    let query = peer.wire.take_sent();
    assert_eq!(query.len(), 2);
    assert!(query.iter().all(|bytes| bytes[0] == TYPE_HANDSHAKE_PART));

    client_wire.deliver(&query);
    connection.tick(now).for_each(drop);
    let reply = client_wire.take_sent();
    assert_eq!(reply.len(), 3);
    assert!(reply.iter().all(|bytes| bytes[0] == TYPE_HANDSHAKE_PART));

    peer.wire.deliver(&reply);
    assert_eq!(peer.tick(now), vec![Seen::Ready]);
    assert_eq!(last_verdict(&peer.wire), Verdict::Accept);
}

#[test]
fn parts_held_back_by_a_blocked_socket_leave_later_as_separate_packets() {
    let now = Instant::now();
    let wire = Fake::packet();
    wire.block(true);
    let mut connection = client(messages(200), &wire, now);
    assert_eq!(wire.sent_count(), 0);
    assert!(connection.is_congested(), "the parts are not waiting");

    wire.block(false);
    connection.tick(now).for_each(drop);
    let sent = wire.take_sent();
    assert_eq!(sent.len(), parts_for(hello_len(200)));
    for (index, bytes) in sent.iter().enumerate() {
        assert_eq!(bytes[0], TYPE_HANDSHAKE_PART, "packet {index}");
        assert_eq!(bytes.len(), PART_HEADER_LEN + u32_at(bytes, 5), "packet {index}");
    }
    assert!(!connection.is_congested(), "parts are still waiting");
}

#[test]
fn a_hello_larger_than_64_kib_held_by_a_blocked_socket_does_not_hit_the_ceiling() {
    assert!(hello_len(5000) > 64 * 1024);
    let now = Instant::now();
    let wire = Fake::packet();
    wire.block(true);
    let mut connection = Connection::at(wire.clone(), messages(5000), Config::default(), now);
    let events: Vec<_> = connection.tick(now).map(seen).collect();
    assert_eq!(events, vec![Seen::Connected]);
    assert_eq!(connection.state(), SessionState::Handshaking);

    wire.block(false);
    connection.tick(now).for_each(drop);
    assert_eq!(wire.sent_count(), parts_for(hello_len(5000)));
}

#[test]
fn a_pending_part_counts_as_activity_for_the_server() {
    let now = Instant::now();
    let schema = messages(200);
    let parts = hello_parts(&schema);
    let mut peer = server(schema, now);

    let almost = Config::default().handshake_timeout - Duration::from_millis(1);
    peer.wire.deliver(&parts[..1]);
    peer.tick(now + almost);
    peer.tick(now + almost + almost);
    assert_eq!(peer.wire.sent_count(), 0, "a probe went out while parts kept the peer active");
}

#[test]
fn a_probe_queued_behind_a_blocked_message_leaves_as_its_own_packet() {
    let now = Instant::now();
    let schema = messages(1);
    let wire = Fake::packet();
    let mut connection = client(schema, &wire, now);
    wire.deliver(&[whole(&[Verdict::Accept.as_byte()])]);
    let events: Vec<_> = connection.tick(now).map(seen).collect();
    assert_eq!(events, vec![Seen::Ready]);
    wire.take_sent();

    wire.block(true);
    connection.send(1, b"held").unwrap();
    let interval = Config::default().heartbeat_interval;
    connection.tick(now + interval).for_each(drop);

    wire.block(false);
    connection.tick(now + interval).for_each(drop);
    let mut data = Vec::new();
    frame::encode_data(1, b"held", &mut data).unwrap();
    assert_eq!(wire.take_sent(), vec![data, vec![TYPE_PROBE]]);
}
