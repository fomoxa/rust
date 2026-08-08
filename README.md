# cyclone-rust

Minimal, platform-agnostic Rust runtime for Cyclone. The Rust counterpart of
[cyclone-unity](https://github.com/cyclone-protocol/cyclone-unity) and
[cyclone-godot](https://github.com/cyclone-protocol/cyclone-godot) - same
wire format (Cyclone's frame: `Magic + MessageId + PayloadLength + Payload`),
same heartbeat, same idea, with nothing tying it to a specific engine,
runtime, or platform.

V1 responsibilities:
- TCP transport (`std::net`, no async runtime)
- Message framing
- MessageId + PayloadLength
- Payload bounded decoding
- Generated codec registration
- Typed message handlers

Generated codecs are expected to be produced by `cyclonec`.

## No async runtime

This crate depends on nothing but `std` - the same "no dependencies, nothing
here could need one" rule `cyclonec` itself follows. A blocking
`TcpStream::read` cannot simply be awaited between `poll()` calls the way an
async runtime (or cyclone-godot's poll-based engine sockets) would allow, so
[`CycloneConnection`](src/connection.rs) spawns one background thread per
connection that blocks on reads and forwards decoded events through a
channel. `poll()` only ever drains that channel - it never blocks, and is
safe to call once a tick/frame from a single-threaded loop, the same shape
Cyclone.Unity's `Pump()` and cyclone-godot's `poll()` both have.

## Generics, unlike cyclone-godot

GDScript has no generics, so cyclone-godot's `on()` takes two `Callable`s and
loses compile-time type checking. Rust has generics, so `CycloneClient::on`
is fully generic - the same shape Cyclone.Unity's `On<T>` has:

```rust
fn decode_player(payload: &[u8]) -> Player {
    let mut reader = Reader::new(payload);
    let mut value = Player { hp: 0, name: String::new() };
    PlayerEdgeCodec::decode(&mut reader, &mut value).expect("valid frame");
    value
}

client.on(PLAYER_EDGE, decode_player, |player: Player| {
    println!("{}", player.hp);
});
```

`decode` is `Fn(&[u8]) -> T`; `handler` is `FnMut(T)`. Multiple handlers on
one message id all run, in registration order, and nothing here catches a
panic either one raises.

## Usage

```rust
let mut server = CycloneServer::new();
server.start("0.0.0.0:9000")?;

let mut client = CycloneClient::new();
client.on(PLAYER_EDGE, decode_player, |player| println!("{}", player.hp));
client.connect("127.0.0.1:9000", Duration::from_secs(5), Duration::from_secs(15))?;

loop {
    for event in server.poll() { /* ServerEvent::ClientConnected(id), ... */ }
    for event in client.poll() { /* ClientEvent::MessageReceived(msg), ... */ }
    std::thread::sleep(Duration::from_millis(16));
}
```

## Picking a port: `local_addr()`

`CycloneServer::start` accepts port `0` to let the OS pick a free port,
readable back via `local_addr()`. This is the robust way to get a port for a
test or an ephemeral server - no fixed port number can collide with
something else already using it (see `tests/connection.rs`, and
cyclone-godot's own README for the real bug a hardcoded port caused there).

## Layout

```
cyclone-rust/
├── src/
│   ├── protocol.rs    CycloneMessage, system_message, frame encode/decode
│   ├── heartbeat.rs   CycloneHeartbeat
│   ├── connection.rs  CycloneConnection - background reader thread + poll()
│   ├── client.rs      CycloneClient - typed on::<T>
│   ├── server.rs      CycloneServer
│   └── lib.rs
├── tests/             real TCP integration tests (connect, heartbeat, disconnect, broadcast)
└── examples/
    ├── shared/        a cyclonec-generated codec (from player.rs's #[network]/#[codec])
    ├── server.rs      standalone, runnable server (its own process)
    └── client.rs      standalone, runnable client (its own process)
```

## Running the tests

```
cargo test
```

10 unit tests (frame encode/decode, heartbeat timing) + 4 integration tests
against real sockets (connect, a real Ping/Pong exchange observed within a
timeout, disconnect, broadcast to multiple clients).

## Running the examples

Two separate OS processes, talking over a real TCP socket on localhost -
confirmed working this way:

```
cargo run --example server   # terminal A
cargo run --example client   # terminal B, once A prints "listening"
```

Terminal A:
```
cyclone-rust server example listening on port 9321
client connected - broadcasting a Player
```

Terminal B:
```
cyclone-rust client example connecting to 127.0.0.1:9321
connected to server
received Player { hp = 100, name = "sensor-1" }
```

## License

Apache-2.0
