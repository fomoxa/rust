//! Minimal, platform-agnostic Rust runtime for Cyclone. The Rust counterpart
//! of `cyclone-unity` and `cyclone-godot` - same wire format (Cyclone's
//! frame: `Magic + MessageId + PayloadLength + Payload`), same heartbeat,
//! same idea, with nothing tying it to a specific engine or platform.
//!
//! V1 responsibilities:
//! - TCP transport (`std::net`, no async runtime)
//! - Message framing
//! - MessageId + PayloadLength
//! - Payload bounded decoding
//! - Generated codec registration
//! - Typed message handlers
//!
//! Generated codecs are expected to be produced by `cyclonec`.
//!
//! # No async runtime
//!
//! This crate depends on nothing but `std`. A blocking socket read cannot
//! simply be awaited between `poll()` calls the way an async runtime (or
//! cyclone-godot's poll-based engine sockets) would allow, so
//! [`connection::CycloneConnection`] spawns one background thread per
//! connection instead - see that module's own docs. The result is the same
//! shape every Cyclone SDK in this family has: call `poll()` once a
//! tick/frame, on the one thread you want events delivered on, and nothing
//! here ever blocks that thread.
//!
//! # Generics, unlike cyclone-godot
//!
//! GDScript has no generics, so cyclone-godot's `on()` takes two
//! `Callable`s and loses compile-time type checking. Rust has generics, so
//! [`client::CycloneClient::on`] is fully generic - the same shape
//! Cyclone.Unity's `On<T>` has.

pub mod client;
pub mod connection;
pub mod heartbeat;
pub mod protocol;
pub mod server;

pub use client::{ClientEvent, CycloneClient};
pub use connection::{ConnectionEvent, CycloneConnection};
pub use heartbeat::CycloneHeartbeat;
pub use protocol::{system_message, CycloneMessage};
pub use server::{ConnectionId, CycloneServer, ServerEvent};
