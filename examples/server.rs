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

use cyclone_net::{CycloneMessage, CycloneServer, ServerEvent};

const PORT: u16 = 9321;
const PLAYER_EDGE: u32 = 1;

fn main() {
    let mut server = CycloneServer::new();
    server
        .start(("127.0.0.1", PORT))
        .expect("bind 127.0.0.1:9321 - is something else already using this port?");

    println!("cyclone-rust server example listening on port {PORT}");

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
                ServerEvent::MessageReceived(_, _) | ServerEvent::PongReceived(_) => {}
            }
        }
        thread::sleep(Duration::from_millis(16));
    }
}
