//! Standalone client example - a real, separate OS process. See `server.rs`'s
//! own header for how to run both together.
//!
//!   cargo run --example client

include!("shared/player.rs");
include!("shared/cyclone.codec.rs");

use std::thread;
use std::time::Duration;

use cyclone_net::{ClientEvent, CycloneClient, CycloneMessage};

const PORT: u16 = 9321;
const PLAYER_EDGE: u32 = 1;
const PLAYER_INPUT: u32 = 2;

// The one-line adapter CycloneClient::on() needs: the project's own
// generated Reader is bridged from &[u8] -> Player here, the same seam
// Cyclone.Unity's CycloneDecoder<T> and cyclone-godot's `decode` Callable
// are.
fn decode_player(payload: &[u8]) -> Player {
    let mut reader = Reader::new(payload);
    let mut value = Player {
        hp: 0,
        name: String::new(),
    };
    PlayerEdgeCodec::decode(&mut reader, &mut value).expect("valid Player frame");
    value
}

fn main() {
    let mut client = CycloneClient::new();

    client.on(
        PLAYER_EDGE,
        decode_player,
        |player: Player| {
            println!(
                "received Player {{ hp = {}, name = {:?} }}",
                player.hp, player.name
            );
        },
    );



    println!("cyclone-rust client example connecting to 127.0.0.1:{PORT}");
    client
        .connect(("127.0.0.1", PORT), Duration::from_secs(5), Duration::from_secs(15))
        .expect("connect to 127.0.0.1:9321 - is the server example running?");

    let mut reported_connected = false;
    loop {
        for event in client.poll() {
            match event {
                ClientEvent::Connected => {
                    if !reported_connected {
                        reported_connected = true;
                        println!("connected to server");

                        let outgoing = PlayerInput { x: 42.0, z: 3.14 };
                        let mut writer = Writer::new();
                        PlayerInputClientCodec::encode(&mut writer, &outgoing);

                        println!("[Client] Sending PlayerInput to server: x=42.0, z=3.14");
                        let _ = client
                            .send(&CycloneMessage::new(PLAYER_INPUT, writer.into_bytes()));
                    }
                }
                ClientEvent::Disconnected => {
                    println!("disconnected");
                    return;
                }
                ClientEvent::MessageReceived(_) | ClientEvent::PongReceived => {}
            }
        }

        thread::sleep(Duration::from_millis(16));
    }
}
