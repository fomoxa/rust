#[path = "demo/src/models/mod.rs"]
mod models;

#[path = "demo/src/schema.rs"]
mod schema;

macro_rules! mount_the_generated_tree {
    () => {
        #[path = "demo/src/generated/mod.rs"]
        mod generated;
    };
}

mount_the_generated_tree!();

use std::thread;
use std::time::Duration;

use cyclone_net::connection::Connection;
use cyclone_net::event::Event;
use cyclone_net::session::Config;
use cyclone_net::transport::TcpTransport;

use generated::{
    GameMessageStateCodec, PlayerInputInputCodec, Reader, Writer, GAME_MESSAGE_STATE_MESSAGE_ID,
    PLAYER_INPUT_INPUT_MESSAGE_ID,
};
use models::game::{GameMessage, PlayerInput, Vector3};

const ADDRESS: &str = "127.0.0.1:9321";

fn encode_input(input: &PlayerInput) -> Vec<u8> {
    let mut writer = Writer::new();
    PlayerInputInputCodec::encode(&mut writer, input);
    writer.into_bytes()
}

fn main() {
    let transport = TcpTransport::connect(ADDRESS).expect("the server example must be running");
    let mut connection = Connection::new(transport, schema::schema(), Config::default());
    println!("cyclone-net client connected to {ADDRESS}");

    let mut tick = 0u64;
    let mut ready = false;
    let mut done = false;
    while !done {
        for event in connection.tick_now() {
            match event {
                Event::Connected => println!("transport up, handshaking"),
                Event::Ready => {
                    println!("handshake accepted");
                    ready = true;
                }
                Event::Message { id: GAME_MESSAGE_STATE_MESSAGE_ID, payload } => {
                    let mut state = GameMessage::default();
                    let mut reader = Reader::new(payload);
                    match GameMessageStateCodec::decode(&mut reader, &mut state) {
                        Ok(()) => println!(
                            "{} at ({:.1}, {:.1}, {:.1}) hp {} alive {}",
                            state.player_name,
                            state.position.x,
                            state.position.y,
                            state.position.z,
                            state.health,
                            state.is_alive
                        ),
                        Err(error) => println!("undecodable state: {error}"),
                    }
                }
                Event::Message { id, .. } => println!("unexpected message 0x{id:08X}"),
                Event::HandshakeFailed(reason) => {
                    println!("handshake refused: {reason}");
                    done = true;
                }
                Event::Disconnected(reason) => {
                    println!("disconnected: {reason}");
                    done = true;
                }
                Event::Probe | Event::Ack => {}
            }
        }

        if ready && tick % 60 == 0 {
            let input = PlayerInput {
                tick,
                direction: Vector3 { x: 1.0, y: 0.0, z: -0.5 },
                firing: tick % 120 == 0,
            };
            if let Err(error) =
                connection.send(PLAYER_INPUT_INPUT_MESSAGE_ID, &encode_input(&input))
            {
                println!("send refused: {error}");
            }
        }

        tick += 1;
        thread::sleep(Duration::from_millis(16));
    }
}
