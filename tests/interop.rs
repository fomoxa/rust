use std::sync::Arc;
use std::thread;
use std::time::Duration;

use fomoxa_net::connection::Connection;
use fomoxa_net::event::{Disconnect, Event, PeerId};
use fomoxa_net::handshake::HandshakeFailure;
use fomoxa_net::schema::{MessageSchema, Schema};
use fomoxa_net::server::Server;
use fomoxa_net::session::Config;
use fomoxa_net::transport::{
    ServerTransport, TcpListenerTransport, TcpTransport, Transport, UdpServerTransport,
    UdpTransport,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Note {
    Connected,
    Ready,
    Failed(HandshakeFailure),
    Message(u32, Vec<u8>),
    Probe,
    Ack,
    Disconnected(Disconnect),
}

fn note(event: Event<'_>) -> Note {
    match event {
        Event::Connected => Note::Connected,
        Event::Ready => Note::Ready,
        Event::HandshakeFailed(reason) => Note::Failed(reason),
        Event::Message { id, payload } => Note::Message(id, payload.to_vec()),
        Event::Probe => Note::Probe,
        Event::Ack => Note::Ack,
        Event::Disconnected(reason) => Note::Disconnected(reason),
    }
}

fn pump<C: Transport, L: ServerTransport>(
    client: &mut Connection<C>,
    server: &mut Server<L>,
    client_notes: &mut Vec<Note>,
    server_notes: &mut Vec<(PeerId, Note)>,
    rounds: usize,
) {
    for _ in 0..rounds {
        client_notes.extend(client.tick_now().map(note));
        server_notes.extend(server.tick_now().map(|seen| (seen.peer, note(seen.event))));
        thread::sleep(Duration::from_millis(1));
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            0xC1C1_0E00,
            vec![MessageSchema::new(7, 70, vec![10, 70]), MessageSchema::new(9, 90, vec![90])],
        )
        .unwrap(),
    )
}

fn conflicting_schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            0xDEAD_BEEF,
            vec![MessageSchema::new(7, 71, vec![11, 71]), MessageSchema::new(9, 90, vec![90])],
        )
        .unwrap(),
    )
}

struct Exchange<C: Transport, L: ServerTransport> {
    client_notes: Vec<Note>,
    server_notes: Vec<(PeerId, Note)>,
    client: Connection<C>,
    server: Server<L>,
}

fn exchange<C: Transport, L: ServerTransport>(
    mut client: Connection<C>,
    mut server: Server<L>,
) -> Exchange<C, L> {
    let mut client_notes = Vec::new();
    let mut server_notes = Vec::new();

    pump(&mut client, &mut server, &mut client_notes, &mut server_notes, 40);
    assert_eq!(client_notes.first(), Some(&Note::Connected));
    assert!(client_notes.contains(&Note::Ready), "client notes: {client_notes:?}");
    assert!(
        server_notes.iter().any(|(_, seen)| *seen == Note::Ready),
        "server notes: {server_notes:?}"
    );

    let peer = server.peers().next().expect("one peer");
    server.send(peer, 7, b"down").expect("the server can send");
    client.send(9, b"up").expect("the client can send");

    pump(&mut client, &mut server, &mut client_notes, &mut server_notes, 40);
    assert!(client_notes.contains(&Note::Message(7, b"down".to_vec())));
    assert!(server_notes.iter().any(|(_, seen)| *seen == Note::Message(9, b"up".to_vec())));

    Exchange { client_notes, server_notes, client, server }
}

#[test]
fn tcp_client_and_server_talk_both_ways() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = Server::new(listener, schema(), Config::default());
    let transport = TcpTransport::connect(address).unwrap();
    let client = Connection::new(transport, schema(), Config::default());

    let Exchange { mut server_notes, mut client, mut server, .. } = exchange(client, server);

    client.close();
    let mut client_notes = Vec::new();
    pump(&mut client, &mut server, &mut client_notes, &mut server_notes, 40);
    assert!(
        server_notes.iter().any(|(_, seen)| *seen == Note::Disconnected(Disconnect::PeerClosed)),
        "server notes: {server_notes:?}"
    );
    assert_eq!(server.peer_count(), 0);
}

#[test]
fn udp_client_and_server_talk_both_ways() {
    let listener = UdpServerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = Server::new(listener, schema(), Config::default());
    let transport = UdpTransport::connect(address).unwrap();
    let client = Connection::new(transport, schema(), Config::default());

    let exchanged = exchange(client, server);
    assert_eq!(exchanged.client_notes.first(), Some(&Note::Connected));
}

#[test]
fn a_schema_conflict_is_refused_over_a_real_socket() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut server = Server::new(listener, schema(), Config::default());
    let transport = TcpTransport::connect(address).unwrap();
    let mut client = Connection::new(transport, conflicting_schema(), Config::default());

    let mut client_notes = Vec::new();
    let mut server_notes = Vec::new();
    pump(&mut client, &mut server, &mut client_notes, &mut server_notes, 40);

    assert!(
        client_notes.contains(&Note::Failed(HandshakeFailure::SchemaConflict)),
        "client notes: {client_notes:?}"
    );
    assert!(
        server_notes
            .iter()
            .any(|(_, seen)| *seen == Note::Failed(HandshakeFailure::SchemaConflict)),
        "server notes: {server_notes:?}"
    );
    assert!(!client_notes.contains(&Note::Ready));
    assert_eq!(server.peer_count(), 0);
}

#[test]
fn a_peer_that_appends_a_field_still_connects_over_a_real_socket() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut server = Server::new(listener, schema(), Config::default());

    let extended = Arc::new(
        Schema::new(
            0x0BEE_F000,
            vec![MessageSchema::new(7, 71, vec![10, 70, 71]), MessageSchema::new(9, 90, vec![90])],
        )
        .unwrap(),
    );
    let transport = TcpTransport::connect(address).unwrap();
    let mut client = Connection::new(transport, extended, Config::default());

    let mut client_notes = Vec::new();
    let mut server_notes = Vec::new();
    pump(&mut client, &mut server, &mut client_notes, &mut server_notes, 40);

    assert!(client_notes.contains(&Note::Ready), "client notes: {client_notes:?}");
    assert!(server_notes.iter().any(|(_, seen)| *seen == Note::Ready));
}

#[test]
fn after_a_peer_in_the_middle_leaves_a_send_by_id_still_reaches_the_right_peer() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut server = Server::new(listener, schema(), Config::default());
    let mut clients: Vec<Connection<TcpTransport>> = (0..3)
        .map(|_| {
            Connection::new(TcpTransport::connect(address).unwrap(), schema(), Config::default())
        })
        .collect();

    let mut ready = 0;
    for _ in 0..400 {
        if ready == 3 {
            break;
        }
        for client in clients.iter_mut() {
            client.tick_now().for_each(drop);
        }
        ready += server.tick_now().filter(|seen| matches!(seen.event, Event::Ready)).count();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(ready, 3);

    let ids: Vec<PeerId> = server.peers().collect();
    for id in &ids {
        server.send(*id, 7, &[id.0 as u8]).expect("the server sends to each peer");
    }
    let id_of_client: Vec<PeerId> = clients
        .iter_mut()
        .map(|client| PeerId(u64::from(receive_one(&mut server, client)[0])))
        .collect();

    let leaving = ids[1];
    let leaving_client = id_of_client.iter().position(|id| *id == leaving).unwrap();
    let mut gone = clients.remove(leaving_client);
    gone.close();
    drop(gone);
    let mut remaining_ids = id_of_client.clone();
    remaining_ids.remove(leaving_client);

    for _ in 0..400 {
        if server.peer_count() == 2 {
            break;
        }
        server.tick_now().for_each(drop);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(server.peer_count(), 2);
    assert!(server.send(leaving, 7, &[0]).is_err());

    for (index, id) in remaining_ids.iter().enumerate().rev() {
        server.send(*id, 7, &[0x80 | index as u8]).expect("the server sends to a remaining peer");
    }
    for (index, client) in clients.iter_mut().enumerate() {
        assert_eq!(receive_one(&mut server, client), vec![0x80 | index as u8]);
    }
}

fn receive_one<L: ServerTransport>(
    server: &mut Server<L>,
    client: &mut Connection<TcpTransport>,
) -> Vec<u8> {
    let mut received = Vec::new();
    for _ in 0..400 {
        if !received.is_empty() {
            break;
        }
        server.tick_now().for_each(drop);
        received.extend(client.tick_now().filter_map(|event| match event {
            Event::Message { payload, .. } => Some(payload.to_vec()),
            _ => None,
        }));
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received.len(), 1, "the client got one message");
    received.remove(0)
}
