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

use cyclone_net::event::Event;
use cyclone_net::server::Server;
use cyclone_net::session::Config;
use cyclone_net::transport::TcpListenerTransport;

use generated::{
    GameMessageStateCodec, PlayerInputInputCodec, Reader, Writer, GAME_MESSAGE_STATE_MESSAGE_ID,
    PLAYER_INPUT_INPUT_MESSAGE_ID,
};
use models::game::{GameMessage, PlayerInput, Vector3};

const ADDRESS: &str = "127.0.0.1:9321";

fn encode_state(state: &GameMessage) -> Vec<u8> {
    let mut writer = Writer::new();
    GameMessageStateCodec::encode(&mut writer, state);
    writer.into_bytes()
}

fn step(state: &mut GameMessage, input: &PlayerInput) {
    state.position.x += input.direction.x;
    state.position.y += input.direction.y;
    state.position.z += input.direction.z;
    if input.firing {
        state.health = state.health.saturating_sub(10);
        state.is_alive = state.health > 0;
    }
}

fn main() {
    let listener = TcpListenerTransport::bind(ADDRESS).expect("bind 127.0.0.1:9321");
    let mut server = Server::new(listener, schema::schema(), Config::default());
    println!("cyclone-net server listening on {ADDRESS}");

    let mut state = GameMessage {
        player_id: 1,
        player_name: "Knight".to_owned(),
        position: Vector3 { x: 10.5, y: 20.3, z: -5.1 },
        health: 100,
        is_alive: true,
        last_seen_at: 0,
    };

    let mut replies = Vec::new();
    loop {
        replies.clear();
        for seen in server.tick_now() {
            match seen.event {
                Event::Connected => println!("{} connected", seen.peer),
                Event::Ready => {
                    println!("{} handshake accepted", seen.peer);
                    replies.push(seen.peer);
                }
                Event::Message { id: PLAYER_INPUT_INPUT_MESSAGE_ID, payload } => {
                    let mut input = PlayerInput::default();
                    let mut reader = Reader::new(payload);
                    match PlayerInputInputCodec::decode(&mut reader, &mut input) {
                        Ok(()) => {
                            step(&mut state, &input);
                            println!(
                                "{} input at tick {} -> hp {}",
                                seen.peer, input.tick, state.health
                            );
                            replies.push(seen.peer);
                        }
                        Err(error) => println!("{} sent an undecodable input: {error}", seen.peer),
                    }
                }
                Event::Message { id, .. } => {
                    println!("{} sent an unexpected message 0x{id:08X}", seen.peer)
                }
                Event::HandshakeFailed(reason) => println!("{} refused: {reason}", seen.peer),
                Event::Disconnected(reason) => println!("{} gone: {reason}", seen.peer),
                Event::Probe | Event::Ack => {}
            }
        }

        if !replies.is_empty() {
            let payload = encode_state(&state);
            for peer in &replies {
                let _ = server.send(*peer, GAME_MESSAGE_STATE_MESSAGE_ID, &payload);
            }
        }

        thread::sleep(Duration::from_millis(16));
    }
}
