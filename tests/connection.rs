//! Real sockets, real threads, real TCP on localhost - not a simulated
//! transport. `CycloneClient`/`CycloneServer` talking to each other,
//! including a real Ping/Pong heartbeat exchange observed within a short
//! timeout, not just inspected in source.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use cyclone_net::{ClientEvent, CycloneClient, CycloneMessage, CycloneServer};

/// Calls `poll()` on both sides and hands the events to `done`, in a loop,
/// until `done` returns true or `timeout` elapses. `done` is the only
/// caller of `poll()` inside the loop - polling again outside it would
/// drain events `done` never got to see.
fn pump_until(
    client: &mut CycloneClient,
    server: &mut CycloneServer,
    timeout: Duration,
    mut done: impl FnMut(&mut CycloneClient, &mut CycloneServer) -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done(client, server) {
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    false
}

#[test]
fn client_and_server_round_trip_a_message() {
    let mut server = CycloneServer::new();
    // Port 0: the OS picks a free port, read back via local_addr() - avoids
    // the entire class of "the port I hardcoded happens to already be in
    // use" bug the cyclone-godot SDK hit during its own testing.
    server.start("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = server.local_addr().expect("server reports its bound address");

    let mut client = CycloneClient::new();
    client
        .connect(addr, Duration::from_secs(5), Duration::from_secs(15))
        .expect("connect to the server");

    let (tx, rx) = mpsc::channel();
    client.on(99, |payload: &[u8]| payload.to_vec(), move |payload| {
        let _ = tx.send(payload);
    });

    let connected = pump_until(&mut client, &mut server, Duration::from_secs(5), |c, s| {
        s.poll();
        c.poll();
        s.connection_count() > 0
    });
    assert!(connected, "server never saw the connection");

    server.broadcast(&CycloneMessage::new(99, b"hello".to_vec()));

    let mut received_via_event: Option<Vec<u8>> = None;
    let done = pump_until(&mut client, &mut server, Duration::from_secs(5), |c, s| {
        s.poll();
        for event in c.poll() {
            if let ClientEvent::MessageReceived(message) = event {
                received_via_event = Some(message.payload.clone());
            }
        }
        received_via_event.is_some()
    });

    assert!(done, "client never received the broadcast message");
    assert_eq!(received_via_event.as_deref(), Some(&b"hello"[..]));
    assert_eq!(rx.try_recv().as_deref(), Ok(&b"hello"[..]));
}

#[test]
fn a_real_ping_pong_heartbeat_exchange_happens() {
    let mut server = CycloneServer::new();
    server.start("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = server.local_addr().unwrap();

    // A short interval so a real Ping/Pong round trip happens inside this
    // test's own timeout, not just something inspected in source.
    let mut client = CycloneClient::new();
    client
        .connect(addr, Duration::from_millis(30), Duration::from_secs(5))
        .expect("connect to the server");

    let saw_pong = pump_until(&mut client, &mut server, Duration::from_secs(5), |c, s| {
        s.poll();
        c.poll()
            .into_iter()
            .any(|event| matches!(event, ClientEvent::PongReceived))
    });

    assert!(saw_pong, "no Pong arrived within the timeout");
}

#[test]
fn disconnect_is_observed_by_both_sides() {
    let mut server = CycloneServer::new();
    server.start("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = server.local_addr().unwrap();

    let mut client = CycloneClient::new();
    client
        .connect(addr, Duration::from_secs(5), Duration::from_secs(15))
        .expect("connect to the server");

    pump_until(&mut client, &mut server, Duration::from_secs(5), |c, s| {
        s.poll();
        c.poll();
        s.connection_count() > 0
    });

    client.disconnect();

    let server_saw_it = pump_until(&mut client, &mut server, Duration::from_secs(5), |c, s| {
        c.poll();
        s.poll();
        s.connection_count() == 0
    });
    assert!(server_saw_it, "server never noticed the client disconnecting");
}

#[test]
fn broadcast_reaches_every_connected_client() {
    let mut server = CycloneServer::new();
    server.start("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = server.local_addr().unwrap();

    let mut client_a = CycloneClient::new();
    let mut client_b = CycloneClient::new();
    client_a
        .connect(addr, Duration::from_secs(5), Duration::from_secs(15))
        .unwrap();
    client_b
        .connect(addr, Duration::from_secs(5), Duration::from_secs(15))
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && server.connection_count() < 2 {
        server.poll();
        client_a.poll();
        client_b.poll();
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(server.connection_count(), 2);

    server.broadcast(&CycloneMessage::new(7, b"to-all".to_vec()));

    let mut a_got = false;
    let mut b_got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !(a_got && b_got) {
        server.poll();
        for event in client_a.poll() {
            if matches!(event, ClientEvent::MessageReceived(m) if m.payload == b"to-all") {
                a_got = true;
            }
        }
        for event in client_b.poll() {
            if matches!(event, ClientEvent::MessageReceived(m) if m.payload == b"to-all") {
                b_got = true;
            }
        }
        thread::sleep(Duration::from_millis(5));
    }

    assert!(a_got && b_got, "broadcast did not reach both clients");
}
