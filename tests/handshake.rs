use std::sync::Arc;
use std::time::{Duration, Instant};

use cyclone_net::event::Event;
use cyclone_net::frame::Frame;
use cyclone_net::handshake::{HandshakeFailure, MAX_MESSAGES, PROTOCOL_VERSION, QUERY_TAG};
use cyclone_net::schema::{MessageSchema, Schema};
use cyclone_net::session::{Config, Out, Role, Session, SessionState};

#[derive(Debug, PartialEq, Eq)]
enum End {
    Ready,
    Failed(HandshakeFailure),
}

fn message(id: u32, prefixes: &[u64]) -> MessageSchema {
    let fingerprint = prefixes.last().copied().unwrap_or(0xE000_0000_0000_0000 | u64::from(id));
    MessageSchema::new(id, fingerprint, prefixes)
}

fn schema(fingerprint: u64, messages: Vec<MessageSchema>) -> Schema {
    Schema::new(fingerprint, messages).expect("a well-formed schema")
}

fn payload_of(out: Out) -> Vec<u8> {
    match out {
        Out::Handshake(payload) => payload,
        other => panic!("expected a handshake payload, got {other:?}"),
    }
}

fn end_of(event: Event<'_>) -> End {
    match event {
        Event::Ready => End::Ready,
        Event::HandshakeFailed(reason) => End::Failed(reason),
        other => panic!("expected a handshake outcome, got {other:?}"),
    }
}

fn client_of(schema: Schema, now: Instant) -> (Session, Vec<u8>) {
    let (session, opening) = Session::new(Role::Client, Arc::new(schema), Config::default(), now);
    (session, payload_of(opening.expect("a client opens with a hello")))
}

fn server_of(schema: Schema, now: Instant) -> Session {
    let (session, quiet) = Session::new(Role::Server, Arc::new(schema), Config::default(), now);
    assert!(quiet.is_none(), "a server says nothing until it is spoken to");
    session
}

fn run(client_schema: Schema, server_schema: Schema) -> (End, End, usize) {
    let now = Instant::now();
    let (mut client, mut up) = client_of(client_schema, now);
    let mut server = server_of(server_schema, now);

    let mut rounds = 0;
    let mut server_end = None;
    loop {
        rounds += 1;
        assert!(rounds <= 2, "a handshake is never more than two rounds");

        let reaction = server.on_frame(Frame::Handshake(&up), now);
        if let Some(event) = reaction.event {
            server_end = Some(end_of(event));
        }
        let down = payload_of(reaction.out.expect("the server always answers"));

        let reaction = client.on_frame(Frame::Handshake(&down), now);
        if let Some(event) = reaction.event {
            let client_end = end_of(event);
            let server_end = server_end.expect("the server decided in the same round");
            return (client_end, server_end, rounds);
        }
        up = payload_of(reaction.out.expect("the client answers a query"));
    }
}

#[test]
fn identical_schema_fingerprints_are_accepted_without_reading_an_entry() {
    let conflicting = message(1, &[0xAA, 0xBB]);
    let other = message(1, &[0x11, 0x22]);
    let (client, server, rounds) =
        run(schema(0x5EED, vec![conflicting]), schema(0x5EED, vec![other]));
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 1);
}

#[test]
fn branch_a_equal_message_fingerprints_are_accepted() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20, 30]), message(2, &[40])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 1);
}

#[test]
fn branch_b_same_field_count_different_content_is_rejected() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20, 99])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(server, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(rounds, 1);
}

#[test]
fn branch_c_client_with_fewer_fields_is_accepted_without_a_query() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 1);
}

#[test]
fn branch_c_with_a_diverging_prefix_is_rejected_without_a_query() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 77])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(server, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(rounds, 1);
}

#[test]
fn branch_d_client_appending_a_field_is_accepted_after_a_query() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20, 30, 40])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 2);
}

#[test]
fn branch_d_server_dropping_a_trailing_field_is_accepted_after_a_query() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20, 30])]),
        schema(0x2222, vec![message(1, &[10, 20])]),
    );
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 2);
}

#[test]
fn branch_d_with_a_diverging_prefix_is_rejected_after_the_query() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10, 20, 99, 40])]),
        schema(0x2222, vec![message(1, &[10, 20, 30])]),
    );
    assert_eq!(client, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(server, End::Failed(HandshakeFailure::SchemaConflict));
    assert_eq!(rounds, 2);
}

#[test]
fn branch_d_with_an_empty_local_message_needs_no_query() {
    let (client, server, rounds) =
        run(schema(0x1111, vec![message(1, &[10, 20])]), schema(0x2222, vec![message(1, &[])]));
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 1);
}

#[test]
fn messages_only_one_side_knows_do_not_block_the_session() {
    let (client, server, rounds) = run(
        schema(0x1111, vec![message(1, &[10]), message(9, &[90])]),
        schema(0x2222, vec![message(1, &[10]), message(7, &[70])]),
    );
    assert_eq!(client, End::Ready);
    assert_eq!(server, End::Ready);
    assert_eq!(rounds, 1);
}

#[test]
fn one_conflicting_message_rejects_the_whole_session() {
    let (client, _, _) = run(
        schema(0x1111, vec![message(1, &[10]), message(2, &[20, 21])]),
        schema(0x2222, vec![message(1, &[10]), message(2, &[20, 99])]),
    );
    assert_eq!(client, End::Failed(HandshakeFailure::SchemaConflict));
}

fn hello_bytes(version: u32, fingerprint: u64, entries: &[(u32, u16, u64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&fingerprint.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, field_count, entry_fingerprint) in entries {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&field_count.to_le_bytes());
        out.extend_from_slice(&entry_fingerprint.to_le_bytes());
    }
    out
}

fn server_verdict(hello: &[u8]) -> (u8, Option<HandshakeFailure>) {
    let now = Instant::now();
    let mut server = server_of(schema(0x2222, vec![message(1, &[10, 20, 30])]), now);
    let reaction = server.on_frame(Frame::Handshake(hello), now);
    let payload = payload_of(reaction.out.expect("the server always answers"));
    assert_eq!(payload.len(), 1, "a verdict is one byte");
    let failure = reaction.event.map(|event| match end_of(event) {
        End::Failed(reason) => reason,
        End::Ready => panic!("expected a rejection"),
    });
    (payload[0], failure)
}

#[test]
fn a_hello_from_another_protocol_version_is_rejected_with_one() {
    let (byte, failure) = server_verdict(&hello_bytes(1, 0x1111, &[(1, 3, 30)]));
    assert_eq!(byte, 1);
    assert_eq!(failure, Some(HandshakeFailure::WrongVersion));
}

#[test]
fn a_hello_one_byte_off_is_rejected_with_three() {
    let mut hello = hello_bytes(PROTOCOL_VERSION, 0x1111, &[(1, 3, 30)]);
    hello.push(0x00);
    assert_eq!(server_verdict(&hello).0, 3);

    let mut hello = hello_bytes(PROTOCOL_VERSION, 0x1111, &[(1, 3, 30)]);
    hello.pop();
    assert_eq!(server_verdict(&hello).0, 3);
}

#[test]
fn a_hello_shorter_than_its_header_is_rejected_with_three() {
    assert_eq!(server_verdict(&[0u8; 15]).0, 3);
    assert_eq!(server_verdict(&[]).0, 3);
}

#[test]
fn a_hello_declaring_more_messages_than_the_cap_is_rejected_without_allocating() {
    let mut hello = Vec::new();
    hello.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    hello.extend_from_slice(&0x1111u64.to_le_bytes());
    hello.extend_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(server_verdict(&hello).0, 3);

    let mut hello = Vec::new();
    hello.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    hello.extend_from_slice(&0x1111u64.to_le_bytes());
    hello.extend_from_slice(&((MAX_MESSAGES + 1) as u32).to_le_bytes());
    assert_eq!(server_verdict(&hello).0, 3);
}

fn query_round(client_schema: Schema) -> (Session, Vec<u8>, Instant) {
    let now = Instant::now();
    let (client, hello) = client_of(client_schema, now);
    (client, hello, now)
}

#[test]
fn a_second_query_is_treated_as_broken() {
    let (mut client, _, now) = query_round(schema(0x1111, vec![message(1, &[10, 20, 30])]));
    let query = {
        let mut out = vec![QUERY_TAG];
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&2u16.to_le_bytes());
        out
    };
    let reaction = client.on_frame(Frame::Handshake(&query), now);
    assert!(reaction.event.is_none());
    assert!(reaction.out.is_some());

    let reaction = client.on_frame(Frame::Handshake(&query), now);
    assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::MalformedPeer)));
}

#[test]
fn a_query_for_an_undeclared_message_is_treated_as_broken() {
    let (mut client, _, now) = query_round(schema(0x1111, vec![message(1, &[10, 20, 30])]));
    let mut query = vec![QUERY_TAG];
    query.extend_from_slice(&1u32.to_le_bytes());
    query.extend_from_slice(&99u32.to_le_bytes());
    query.extend_from_slice(&1u16.to_le_bytes());
    let reaction = client.on_frame(Frame::Handshake(&query), now);
    assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::MalformedPeer)));
}

#[test]
fn a_query_outside_one_to_field_count_is_treated_as_broken() {
    for asked in [0u16, 3, 4] {
        let (mut client, _, now) = query_round(schema(0x1111, vec![message(1, &[10, 20, 30])]));
        let mut query = vec![QUERY_TAG];
        query.extend_from_slice(&1u32.to_le_bytes());
        query.extend_from_slice(&1u32.to_le_bytes());
        query.extend_from_slice(&asked.to_le_bytes());
        let reaction = client.on_frame(Frame::Handshake(&query), now);
        assert_eq!(
            reaction.event.map(end_of),
            Some(End::Failed(HandshakeFailure::MalformedPeer)),
            "index {asked} must be refused"
        );
    }
}

#[test]
fn a_verdict_above_three_is_treated_as_broken() {
    for byte in [5u8, 6, 200] {
        let (mut client, _, now) = query_round(schema(0x1111, vec![message(1, &[10])]));
        let verdict = [byte];
        let reaction = client.on_frame(Frame::Handshake(&verdict), now);
        assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::MalformedPeer)));
    }
}

#[test]
fn a_verdict_that_is_not_one_byte_is_treated_as_broken() {
    let (mut client, _, now) = query_round(schema(0x1111, vec![message(1, &[10])]));
    let reaction = client.on_frame(Frame::Handshake(&[0, 0]), now);
    assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::MalformedPeer)));
}

fn reply_bytes(items: &[(u32, u64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for (id, fingerprint) in items {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&fingerprint.to_le_bytes());
    }
    out
}

fn server_after_query(reply: &[u8]) -> u8 {
    let now = Instant::now();
    let mut server = server_of(schema(0x2222, vec![message(1, &[10, 20]), message(2, &[30])]), now);
    let hello = hello_bytes(PROTOCOL_VERSION, 0x1111, &[(1, 3, 99), (2, 2, 88)]);
    let reaction = server.on_frame(Frame::Handshake(&hello), now);
    let query = payload_of(reaction.out.expect("the server asks"));
    assert_eq!(query[0], QUERY_TAG);
    assert!(reaction.event.is_none(), "a query is a frame without an event");

    let reaction = server.on_frame(Frame::Handshake(reply), now);
    payload_of(reaction.out.expect("the server decides after the reply"))[0]
}

#[test]
fn a_correct_reply_is_accepted() {
    assert_eq!(server_after_query(&reply_bytes(&[(1, 20), (2, 30)])), 0);
}

#[test]
fn a_reply_with_a_diverging_prefix_is_rejected_with_two() {
    assert_eq!(server_after_query(&reply_bytes(&[(1, 20), (2, 77)])), 2);
}

#[test]
fn a_reply_that_is_short_long_or_out_of_order_is_rejected_with_three() {
    assert_eq!(server_after_query(&reply_bytes(&[(1, 20)])), 3);
    assert_eq!(server_after_query(&reply_bytes(&[(1, 20), (2, 30), (3, 40)])), 3);
    assert_eq!(server_after_query(&reply_bytes(&[(2, 30), (1, 20)])), 3);
    assert_eq!(server_after_query(&[0x00]), 3);
}

#[test]
fn the_client_deadline_covers_both_rounds() {
    let start = Instant::now();
    let (mut client, _) = client_of(schema(0x1111, vec![message(1, &[10, 20, 30])]), start);

    let mut query = vec![QUERY_TAG];
    query.extend_from_slice(&1u32.to_le_bytes());
    query.extend_from_slice(&1u32.to_le_bytes());
    query.extend_from_slice(&2u16.to_le_bytes());

    let midway = start + Duration::from_secs(3);
    let reaction = client.on_frame(Frame::Handshake(&query), midway);
    assert!(reaction.out.is_some());
    assert!(reaction.event.is_none());

    let reaction = client.tick(start + Duration::from_secs(5));
    assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::Timeout)));
    assert_eq!(client.state(), SessionState::Closed);
}

#[test]
fn a_client_that_never_hears_back_times_out() {
    let start = Instant::now();
    let (mut client, _) = client_of(schema(0x1111, vec![message(1, &[10])]), start);
    assert!(client.tick(start + Duration::from_secs(4)).event.is_none());
    let reaction = client.tick(start + Duration::from_secs(5));
    assert_eq!(reaction.event.map(end_of), Some(End::Failed(HandshakeFailure::Timeout)));
}

#[test]
fn a_client_that_never_hellos_but_answers_probes_holds_its_slot() {
    let start = Instant::now();
    let mut server = server_of(schema(0x2222, vec![message(1, &[10])]), start);

    let mut now = start;
    for _ in 0..20 {
        now += Duration::from_secs(5);
        let reaction = server.tick(now);
        assert_eq!(reaction.out, Some(Out::Probe));
        assert!(reaction.event.is_none());

        now += Duration::from_secs(1);
        let reaction = server.on_frame(Frame::Ack, now);
        assert!(reaction.event.is_none(), "an ack before ready raises no event");
    }
    assert_eq!(server.state(), SessionState::Handshaking);
}
