//! Standalone server example - a real, separate OS process. Run this and
//! `client.rs` as two different processes to see them talk over a real TCP
//! socket, not a simulated one in a single process.
//!
//!   cargo run --example server
//!   cargo run --example client   # in another terminal, once this prints "listening"

include!("shared/player.rs");
include!("shared/cyclone.codec.rs");

use std::thread;
use std::time::Duration;

use cyclone_net::{ConnectionId, CycloneMessage, CycloneServer, ServerEvent};

const PORT: u16 = 9321;
const PLAYER_EDGE: u32 = 1;
const PLAYER_INPUT: u32 = 2;

fn main() {
    let mut server = CycloneServer::new();
    server
        .start(("127.0.0.1", PORT))
        .expect("bind 127.0.0.1:9321 - is something else already using this port?");

    println!("cyclone-rust server example listening on port {PORT}");

    server.on(
        PLAYER_INPUT,
        |payload: &[u8]| {
            let mut reader = Reader::new(payload);
            let mut value = PlayerInput { x: 0.0, z: 0.0 };
            PlayerInputClientCodec::decode(&mut reader, &mut value)
                .expect("valid PlayerInput frame");
            value
        },
        |id: ConnectionId, input: PlayerInput| {
            println!(
                "received PlayerInput {{connection = {:?}, x = {}, z = {} }}",
                id, input.x, input.z
            );
        },
    );

    loop {
        for event in server.poll() {
            match event {
                ServerEvent::ClientConnected(id) => {
                    println!("client connected - broadcasting a Player");

                    let outgoing = Player {
                        hp: 100,
                        name: "sensor-1".to_owned(),
                    };
                    let mut writer = Writer::new();
                    PlayerEdgeCodec::encode(&mut writer, &outgoing);

                    let _ = server.send_to(
                        id,
                        &CycloneMessage::new(PLAYER_EDGE, writer.into_bytes()),
                    );
                }
                ServerEvent::ClientDisconnected(_) => println!("client disconnected"),
                ServerEvent::MessageReceived(id, _) => {
                    println!("received PlayerInput {{connection = {:?} }}", id);
                }
                ServerEvent::PongReceived(_) => {}
            }
        }
        thread::sleep(Duration::from_millis(16));
    }
}
