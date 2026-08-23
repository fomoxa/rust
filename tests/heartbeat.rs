use std::sync::Arc;
use std::time::{Duration, Instant};

use fomoxa_net::event::{Disconnect, Event};
use fomoxa_net::frame::Frame;
use fomoxa_net::schema::{MessageSchema, Schema};
use fomoxa_net::session::{Config, Out, Role, Session, SessionState};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(0x1234, vec![MessageSchema::new(1, 10, vec![10])]).unwrap())
}

fn ready_client(now: Instant) -> Session {
    let (mut session, _) = Session::new(Role::Client, schema(), Config::default(), now);
    let reaction = session.on_frame(Frame::Handshake(&[0]), now);
    assert_eq!(reaction.event, Some(Event::Ready));
    session
}

fn ready_server(now: Instant) -> Session {
    let (mut session, _) = Session::new(Role::Server, schema(), Config::default(), now);
    let hello = fomoxa_net::handshake::encode_hello(&schema());
    let reaction = session.on_frame(Frame::Handshake(&hello), now);
    assert_eq!(reaction.event, Some(Event::Ready));
    session
}

#[test]
fn traffic_keeps_the_probe_away() {
    let start = Instant::now();
    let mut session = ready_client(start);

    let mut now = start;
    for _ in 0..20 {
        now += Duration::from_secs(1);
        let reaction = session.on_frame(Frame::Data { message_id: 1, payload: &[] }, now);
        assert!(reaction.out.is_none());
        assert!(session.tick(now).out.is_none(), "a talking peer is never probed");
    }
}

#[test]
fn silence_sends_exactly_one_probe() {
    let start = Instant::now();
    let mut session = ready_client(start);

    assert!(session.tick(start + Duration::from_secs(4)).out.is_none());
    assert_eq!(session.tick(start + Duration::from_secs(5)).out, Some(Out::Probe));

    for extra in 6..15 {
        let reaction = session.tick(start + Duration::from_secs(extra));
        assert_eq!(reaction.out, None, "the probe is sent once, not once a tick");
        assert_eq!(reaction.event, None);
    }
}

#[test]
fn an_ack_puts_the_session_back_to_normal() {
    let start = Instant::now();
    let mut session = ready_client(start);
    assert_eq!(session.tick(start + Duration::from_secs(5)).out, Some(Out::Probe));

    let answered = start + Duration::from_secs(6);
    let reaction = session.on_frame(Frame::Ack, answered);
    assert_eq!(reaction.event, Some(Event::Ack));

    assert!(session.tick(answered + Duration::from_secs(4)).out.is_none());
    assert_eq!(session.tick(answered + Duration::from_secs(5)).out, Some(Out::Probe));
}

#[test]
fn any_frame_clears_the_probe_not_only_an_ack() {
    for frame in [Frame::Data { message_id: 1, payload: &[] }, Frame::Probe, Frame::Ack] {
        let start = Instant::now();
        let mut session = ready_client(start);
        assert_eq!(session.tick(start + Duration::from_secs(5)).out, Some(Out::Probe));

        session.on_frame(frame, start + Duration::from_secs(6));
        let reaction = session.tick(start + Duration::from_secs(21));
        assert_eq!(reaction.event, None, "{frame:?} should have cleared the probe");
    }
}

#[test]
fn a_probe_is_always_answered_with_an_ack() {
    let start = Instant::now();
    let mut session = ready_client(start);
    let reaction = session.on_frame(Frame::Probe, start);
    assert_eq!(reaction.out, Some(Out::Ack));
    assert_eq!(reaction.event, Some(Event::Probe));
}

#[test]
fn a_client_still_handshaking_answers_probes_without_raising_an_event() {
    let start = Instant::now();
    let (mut session, _) = Session::new(Role::Client, schema(), Config::default(), start);
    let reaction = session.on_frame(Frame::Probe, start);
    assert_eq!(reaction.out, Some(Out::Ack));
    assert_eq!(reaction.event, None);
}

#[test]
fn a_client_still_handshaking_never_probes() {
    let start = Instant::now();
    let (mut session, _) = Session::new(Role::Client, schema(), Config::default(), start);
    assert!(session.tick(start + Duration::from_secs(4)).out.is_none());
}

#[test]
fn silence_past_the_response_deadline_declares_the_peer_dead() {
    let start = Instant::now();
    let mut session = ready_client(start);
    assert_eq!(session.tick(start + Duration::from_secs(5)).out, Some(Out::Probe));

    assert!(session.tick(start + Duration::from_secs(19)).event.is_none());
    let reaction = session.tick(start + Duration::from_secs(20));
    assert_eq!(reaction.event, Some(Event::Disconnected(Disconnect::Unresponsive)));
    assert_eq!(session.state(), SessionState::Closed);
}

#[test]
fn the_whole_expiry_cycle_runs_without_waiting() {
    let start = Instant::now();
    let mut session = ready_server(start);
    let reaction = session.tick(start + Duration::from_secs(5));
    assert_eq!(reaction.out, Some(Out::Probe));
    let reaction = session.tick(start + Duration::from_secs(20));
    assert_eq!(reaction.event, Some(Event::Disconnected(Disconnect::Unresponsive)));
}

#[test]
fn a_server_widens_its_silence_window_while_a_peer_is_still_handshaking() {
    let start = Instant::now();
    let (mut handshaking, _) = Session::new(Role::Server, schema(), Config::default(), start);
    let mut ready = ready_server(start);

    let at = start + Duration::from_secs(5);
    assert_eq!(ready.tick(at).out, Some(Out::Probe));
    assert_eq!(handshaking.tick(at).out, Some(Out::Probe));

    let config = Config { handshake_timeout: Duration::from_secs(30), ..Config::default() };
    let (mut patient, _) = Session::new(Role::Server, schema(), config, start);
    assert!(patient.tick(start + Duration::from_secs(29)).out.is_none());
    assert_eq!(patient.tick(start + Duration::from_secs(30)).out, Some(Out::Probe));
}

#[test]
fn only_one_terminal_event_is_ever_raised() {
    let start = Instant::now();
    let mut session = ready_client(start);
    session.tick(start + Duration::from_secs(5));
    let reaction = session.tick(start + Duration::from_secs(20));
    assert_eq!(reaction.event, Some(Event::Disconnected(Disconnect::Unresponsive)));

    assert!(session.on_transport_closed(Disconnect::PeerClosed).event.is_none());
    assert!(session.tick(start + Duration::from_secs(60)).event.is_none());
}
