#[path = "../examples/demo/src/models/mod.rs"]
mod models;

#[path = "../examples/demo/src/schema.rs"]
mod schema;

macro_rules! mount_the_generated_tree {
    () => {
        #[path = "../examples/demo/src/generated/mod.rs"]
        mod generated;
    };
}

mount_the_generated_tree!();

use std::thread;
use std::time::Duration;

use fomoxa_net::connection::Connection;
use fomoxa_net::event::Event;
use fomoxa_net::server::Server;
use fomoxa_net::session::Config;
use fomoxa_net::transport::{TcpListenerTransport, TcpTransport};

use generated::{
    GameMessageStateCodec, PlayerInputInputCodec, Reader, Writer, GAME_MESSAGE_STATE_MESSAGE_ID,
    PLAYER_INPUT_INPUT_MESSAGE_ID,
};
use models::game::{GameMessage, PlayerInput, Vector3};

fn state() -> GameMessage {
    GameMessage {
        player_id: 42,
        player_name: "Xin ch\u{E0}o".to_owned(),
        position: Vector3 { x: 10.5, y: 20.3, z: -5.1 },
        health: 100,
        is_alive: true,
        last_seen_at: 1_700_000_000,
    }
}

fn input() -> PlayerInput {
    PlayerInput {
        tick: u64::MAX,
        direction: Vector3 { x: -0.0, y: f32::INFINITY, z: 1.5 },
        firing: true,
    }
}

#[test]
fn a_generated_codec_travels_over_a_real_socket_in_both_directions() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut server = Server::new(listener, schema::schema(), Config::default());
    let transport = TcpTransport::connect(address).unwrap();
    let mut client = Connection::new(transport, schema::schema(), Config::default());

    let mut client_ready = false;
    let mut server_peer = None;
    let mut received_state: Option<GameMessage> = None;
    let mut received_input: Option<PlayerInput> = None;
    let mut sent = false;

    for _ in 0..120 {
        for event in client.tick_now() {
            match event {
                Event::Ready => client_ready = true,
                Event::Message { id: GAME_MESSAGE_STATE_MESSAGE_ID, payload } => {
                    let mut decoded = GameMessage::default();
                    GameMessageStateCodec::decode(&mut Reader::new(payload), &mut decoded)
                        .expect("a frame the peer's own codec wrote");
                    received_state = Some(decoded);
                }
                Event::HandshakeFailed(reason) => panic!("handshake refused: {reason}"),
                _ => {}
            }
        }

        for seen in server.tick_now() {
            match seen.event {
                Event::Ready => server_peer = Some(seen.peer),
                Event::Message { id: PLAYER_INPUT_INPUT_MESSAGE_ID, payload } => {
                    let mut decoded = PlayerInput::default();
                    PlayerInputInputCodec::decode(&mut Reader::new(payload), &mut decoded)
                        .expect("a frame the peer's own codec wrote");
                    received_input = Some(decoded);
                }
                Event::HandshakeFailed(reason) => panic!("handshake refused: {reason}"),
                _ => {}
            }
        }

        if let (true, false, Some(peer)) = (client_ready, sent, server_peer) {
            let mut writer = Writer::new();
            PlayerInputInputCodec::encode(&mut writer, &input());
            client.send(PLAYER_INPUT_INPUT_MESSAGE_ID, writer.as_slice()).unwrap();

            let mut writer = Writer::new();
            GameMessageStateCodec::encode(&mut writer, &state());
            server.send(peer, GAME_MESSAGE_STATE_MESSAGE_ID, writer.as_slice()).unwrap();
            sent = true;
        }

        if received_state.is_some() && received_input.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    let received_input = received_input.expect("the server decoded the input");
    assert_eq!(received_input, input());
    assert_eq!(received_input.direction.x.to_bits(), (-0.0f32).to_bits());

    let received_state = received_state.expect("the client decoded the state");
    assert_eq!(received_state.player_name, "Xin ch\u{E0}o");
    assert_eq!(received_state.position, state().position);
    assert!(received_state.is_alive);

    assert_eq!(received_state.last_seen_at, 0, "a field outside the codec never goes on the wire");
}
