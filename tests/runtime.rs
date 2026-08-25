use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fomoxa_net::connection::{Connection, SendError};
use fomoxa_net::event::{Disconnect, Event};
use fomoxa_net::frame;
use fomoxa_net::schema::{MessageSchema, Schema};
use fomoxa_net::session::{Config, SessionState};
use fomoxa_net::transport::{RecvOutcome, SendOutcome, Transport, TransportKind};

#[derive(Debug)]
struct State {
    kind: TransportKind,
    sent: Vec<u8>,
    incoming: VecDeque<Vec<u8>>,
    blocked: bool,
    partial: Option<usize>,
    too_large_over: Option<usize>,
    closed: bool,
    errored: bool,
    soft_closes: usize,
    hard_closes: usize,
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
                partial: None,
                too_large_over: None,
                closed: false,
                errored: false,
                soft_closes: 0,
                hard_closes: 0,
            })),
        }
    }

    fn stream() -> Fake {
        Fake::of(TransportKind::Stream)
    }

    fn message() -> Fake {
        Fake::of(TransportKind::Message)
    }

    fn deliver(&self, bytes: &[u8]) {
        self.state.borrow_mut().incoming.push_back(bytes.to_vec());
    }

    fn sent(&self) -> Vec<u8> {
        self.state.borrow().sent.clone()
    }

    fn clear_sent(&self) {
        self.state.borrow_mut().sent.clear();
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
        if state.closed {
            return SendOutcome::Closed;
        }
        if state.errored {
            return SendOutcome::Error(io::Error::new(io::ErrorKind::Other, "fake"));
        }
        if state.too_large_over.is_some_and(|limit| bytes.len() > limit) {
            return SendOutcome::TooLarge;
        }
        if state.blocked {
            return SendOutcome::WouldBlock;
        }
        if let Some(chunk) = state.partial {
            if bytes.len() > chunk {
                state.sent.extend_from_slice(&bytes[..chunk]);
                return SendOutcome::Partial(chunk);
            }
        }
        state.sent.extend_from_slice(bytes);
        SendOutcome::Sent
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        let mut state = self.state.borrow_mut();
        let kind = state.kind;
        let Some(front) = state.incoming.front_mut() else {
            return if state.closed { RecvOutcome::Closed } else { RecvOutcome::WouldBlock };
        };
        match kind {
            TransportKind::Message => {
                if front.len() > buffer.len() {
                    return RecvOutcome::NeedCapacity(front.len());
                }
                let count = front.len();
                buffer[..count].copy_from_slice(front);
                state.incoming.pop_front();
                RecvOutcome::Received(count)
            }
            TransportKind::Stream => {
                let count = front.len().min(buffer.len());
                buffer[..count].copy_from_slice(&front[..count]);
                if count == front.len() {
                    state.incoming.pop_front();
                } else {
                    front.drain(..count);
                }
                RecvOutcome::Received(count)
            }
        }
    }

    fn close_soft(&mut self) {
        self.state.borrow_mut().soft_closes += 1;
    }

    fn close_hard(&mut self) {
        let mut state = self.state.borrow_mut();
        state.hard_closes += 1;
        state.closed = true;
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(0x1234, vec![MessageSchema::new(1, 10, vec![10])]).unwrap())
}

fn handshake_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::encode_handshake(payload, &mut out).unwrap();
    out
}

fn data_frame(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::encode_data(id, payload, &mut out).unwrap();
    out
}

fn ready(fake: &Fake, config: Config, now: Instant) -> Connection<Fake> {
    let mut connection = Connection::at(fake.clone(), schema(), config, now);
    fake.deliver(&handshake_frame(&[0]));
    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Connected, Event::Ready]);
    connection
}

#[test]
fn connected_comes_first_and_the_hello_goes_out_at_once() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = Connection::at(fake.clone(), schema(), Config::default(), now);

    assert_eq!(fake.sent()[0], 0x03);
    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Connected]);
    assert_eq!(connection.state(), SessionState::Handshaking);
}

#[test]
fn sending_before_ready_is_refused() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = Connection::at(fake.clone(), schema(), Config::default(), now);
    assert_eq!(connection.send(1, b"hi"), Err(SendError::NotReady));
}

#[test]
fn a_blocked_transport_holds_one_frame_and_sends_it_once() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);
    fake.clear_sent();

    fake.block(true);
    assert_eq!(connection.send(1, b"first"), Ok(()));
    assert!(connection.is_congested());
    assert!(fake.sent().is_empty());

    assert_eq!(connection.send(1, b"second"), Err(SendError::Congested));

    fake.block(false);
    connection.tick(now).for_each(drop);
    assert_eq!(fake.sent(), data_frame(1, b"first"));
    assert!(!connection.is_congested());

    fake.clear_sent();
    connection.tick(now).for_each(drop);
    assert!(fake.sent().is_empty(), "a flushed frame is never sent twice");
}

#[test]
fn a_partial_write_is_finished_before_anything_else() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);
    fake.clear_sent();

    fake.state.borrow_mut().partial = Some(3);
    assert_eq!(connection.send(1, b"payload"), Ok(()));
    assert_eq!(fake.sent().len(), 3);

    fake.state.borrow_mut().partial = None;
    connection.tick(now).for_each(drop);
    assert_eq!(fake.sent(), data_frame(1, b"payload"));
}

#[test]
fn a_frame_over_the_transport_cap_does_not_kill_the_session() {
    let fake = Fake::message();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    fake.state.borrow_mut().too_large_over = Some(16);
    assert_eq!(connection.send(1, &[0u8; 64]), Err(SendError::TooLarge));
    assert_eq!(connection.state(), SessionState::Ready);
    assert!(!connection.is_congested(), "an oversized frame is not retried");

    fake.state.borrow_mut().too_large_over = None;
    assert_eq!(connection.send(1, b"small"), Ok(()));
}

#[test]
fn a_packet_that_does_not_fit_is_not_lost() {
    let fake = Fake::message();
    let now = Instant::now();
    let config = Config { recv_buffer_size: 16, ..Config::default() };
    let mut connection = ready(&fake, config, now);

    let payload = vec![7u8; 512];
    fake.deliver(&data_frame(1, &payload));
    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Message { id: 1, payload: &payload }]);
}

#[test]
fn shrinking_after_a_stream_burst_still_delivers_the_next_message() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    let big_payload = vec![7u8; 5000];
    fake.deliver(&data_frame(1, &big_payload));
    let received: Vec<u8> = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { payload, .. } => Some(payload.to_vec()),
            _ => None,
        })
        .unwrap();
    assert_eq!(received, big_payload);

    connection.shrink_to_fit();

    let small_payload = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    fake.deliver(&data_frame(2, &small_payload));
    let (id, received): (u32, Vec<u8>) = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { id, payload } => Some((id, payload.to_vec())),
            _ => None,
        })
        .unwrap();
    assert_eq!(id, 2);
    assert_eq!(received, small_payload);
}

#[test]
fn shrinking_after_a_packet_burst_still_delivers_the_next_message() {
    let fake = Fake::message();
    let now = Instant::now();
    let config = Config { recv_buffer_size: 16, ..Config::default() };
    let mut connection = ready(&fake, config, now);

    let big_payload = vec![7u8; 5000];
    fake.deliver(&data_frame(1, &big_payload));
    let received: Vec<u8> = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { payload, .. } => Some(payload.to_vec()),
            _ => None,
        })
        .unwrap();
    assert_eq!(received, big_payload);

    connection.shrink_to_fit();

    let small_payload = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    fake.deliver(&data_frame(2, &small_payload));
    let (id, received): (u32, Vec<u8>) = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { id, payload } => Some((id, payload.to_vec())),
            _ => None,
        })
        .unwrap();
    assert_eq!(id, 2);
    assert_eq!(received, small_payload);
}

#[test]
fn a_flood_of_frames_stops_at_the_budget() {
    let fake = Fake::stream();
    let now = Instant::now();
    let config = Config { max_frames_per_tick: 4, ..Config::default() };
    let mut connection = ready(&fake, config, now);

    let mut wire = Vec::new();
    for index in 0..10u32 {
        wire.extend_from_slice(&data_frame(index, b"x"));
    }
    fake.deliver(&wire);

    let first: Vec<u32> = connection
        .tick(now)
        .filter_map(|event| match event {
            Event::Message { id, .. } => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(first, vec![0, 1, 2, 3]);

    let second: Vec<u32> = connection
        .tick(now)
        .filter_map(|event| match event {
            Event::Message { id, .. } => Some(id),
            _ => None,
        })
        .collect();
    assert_eq!(second, vec![4, 5, 6, 7]);
}

#[test]
fn a_probe_is_answered_from_inside_the_tick() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);
    fake.clear_sent();

    let mut probe = Vec::new();
    frame::encode_probe(&mut probe);
    fake.deliver(&probe);

    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Probe]);
    assert_eq!(fake.sent(), [0x02]);
}

#[test]
fn a_rejected_handshake_raises_one_terminal_event() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = Connection::at(fake.clone(), schema(), Config::default(), now);
    fake.deliver(&handshake_frame(&[2]));

    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(
        events,
        vec![
            Event::Connected,
            Event::HandshakeFailed(fomoxa_net::HandshakeFailure::SchemaConflict)
        ]
    );
    assert_eq!(fake.state.borrow().soft_closes, 1);

    fake.state.borrow_mut().closed = true;
    for _ in 0..3 {
        let later: Vec<_> = connection.tick(now).collect();
        assert!(later.is_empty(), "a closed session says nothing more");
    }
}

#[test]
fn a_closed_transport_disconnects_exactly_once() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    fake.state.borrow_mut().closed = true;
    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Disconnected(Disconnect::PeerClosed)]);
    assert!(connection.tick(now).next().is_none());
}

#[test]
fn a_broken_stream_ends_the_session() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    fake.deliver(&[0x09]);
    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Disconnected(Disconnect::TransportError)]);
}

#[test]
fn a_broken_packet_is_dropped_and_the_session_survives() {
    let fake = Fake::message();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    fake.deliver(&[0x09, 0x09]);
    fake.deliver(&data_frame(1, b"still here"));

    let events: Vec<_> = connection.tick(now).collect();
    assert_eq!(events, vec![Event::Message { id: 1, payload: b"still here" }]);
    assert_eq!(connection.state(), SessionState::Ready);
}

#[test]
fn heartbeat_runs_over_a_transport_without_waiting() {
    let fake = Fake::stream();
    let start = Instant::now();
    let mut connection = ready(&fake, Config::default(), start);
    fake.clear_sent();

    connection.tick(start + Duration::from_secs(5)).for_each(drop);
    assert_eq!(fake.sent(), [0x01]);

    let events: Vec<_> = connection.tick(start + Duration::from_secs(20)).collect();
    assert_eq!(events, vec![Event::Disconnected(Disconnect::Unresponsive)]);
}

#[test]
fn closing_locally_raises_no_event_and_releases_the_transport() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    connection.close();
    assert!(connection.tick(now).next().is_none());
    assert_eq!(fake.state.borrow().soft_closes, 1);

    drop(connection);
    assert_eq!(fake.state.borrow().hard_closes, 1);
}

#[test]
fn message_payloads_only_live_until_the_next_tick() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);

    fake.deliver(&data_frame(1, b"one"));
    let copied: Vec<u8> = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { payload, .. } => Some(payload.to_vec()),
            _ => None,
        })
        .unwrap();
    assert_eq!(copied, b"one");

    fake.deliver(&data_frame(2, b"two"));
    let next: Vec<u8> = connection
        .tick(now)
        .find_map(|event| match event {
            Event::Message { payload, .. } => Some(payload.to_vec()),
            _ => None,
        })
        .unwrap();
    assert_eq!(next, b"two");
}

/// 02 §8: the pending queue must have a ceiling. A peer that probes every tick
/// while never reading keeps our silence clock alive, so the heartbeat never
/// ends the session - only the ceiling does. The reason is Unresponsive, not
/// TransportError: the link is fine, the peer is simply not keeping up.
#[test]
fn a_blocked_link_and_a_probing_peer_stop_at_the_outbox_ceiling() {
    let fake = Fake::stream();
    let now = Instant::now();
    let mut connection = ready(&fake, Config::default(), now);
    fake.block(true);

    let mut ending = None;
    for tick in 0..200_000u64 {
        fake.deliver(&[0x01]);
        for event in connection.tick(now + Duration::from_millis(tick)) {
            if let Event::Disconnected(reason) = event {
                ending = Some(reason);
            }
        }
        if ending.is_some() {
            break;
        }
    }

    assert_eq!(
        ending,
        Some(Disconnect::Unresponsive),
        "the ceiling ends the session rather than letting the queue grow"
    );
}
