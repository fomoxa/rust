//! Wire framing and the message shape it carries - the Rust counterpart of
//! Cyclone.Unity's `CycloneFrame`/`CycloneMessage`/`CycloneSystemMessage`.
//!
//! Layout: `Magic(2) + MessageId(u32 LE) + PayloadLength(u32 LE) + Payload`.
//! Exactly what the C# and GDScript SDKs write and read, so a Rust peer
//! talks to either one byte-for-byte.

/// A decoded Cyclone message: an id and its raw payload.
///
/// Not itself decoded into a model - see `cyclonec`'s generated `Reader`/
/// `Writer` for that. This is the transport's unit, the same way
/// Cyclone.Unity's `CycloneMessage` struct is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycloneMessage {
    pub id: u32,
    pub payload: Vec<u8>,
}

impl CycloneMessage {
    pub fn new(id: u32, payload: Vec<u8>) -> Self {
        Self { id, payload }
    }
}

/// Message ids [`crate::connection::CycloneConnection`] reserves for its own
/// heartbeat - never a message a game defines. The exact values the C# and
/// GDScript SDKs use.
pub mod system_message {
    pub const PING: u32 = 0xFFFF_0001;
    pub const PONG: u32 = 0xFFFF_0002;
}

pub const MAGIC: [u8; 2] = [0x43, 0x59]; // "CY"
pub const MAGIC_SIZE: usize = 2;
pub const MESSAGE_ID_SIZE: usize = 4;
pub const PAYLOAD_LENGTH_SIZE: usize = 4;
pub const HEADER_SIZE: usize = MAGIC_SIZE + MESSAGE_ID_SIZE + PAYLOAD_LENGTH_SIZE;

/// 16 MiB - the same ceiling the C# and GDScript SDKs refuse to encode or
/// decode past.
pub const MAX_PAYLOAD_LENGTH: usize = 16 * 1024 * 1024;

/// Encodes `message` as one frame.
///
/// # Panics
///
/// If the payload exceeds [`MAX_PAYLOAD_LENGTH`] - the same refusal
/// Cyclone.Unity's `CycloneFrame.Encode` throws for. A payload this large is
/// a caller mistake (RFC-0002's own `string`/`bytes` length prefix cannot
/// even address it), not a runtime condition to recover from.
pub fn encode(message: &CycloneMessage) -> Vec<u8> {
    assert!(
        message.payload.len() <= MAX_PAYLOAD_LENGTH,
        "Cyclone payload is too large: {} bytes exceeds the {} byte limit",
        message.payload.len(),
        MAX_PAYLOAD_LENGTH
    );

    let mut frame = Vec::with_capacity(HEADER_SIZE + message.payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&message.id.to_le_bytes());
    frame.extend_from_slice(&(message.payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&message.payload);
    frame
}

/// Decodes exactly one frame's worth of bytes - `data` must be exactly one
/// frame, no more and no less (the caller, [`crate::connection::CycloneConnection`],
/// is what finds where a frame starts and ends in a streamed buffer).
///
/// Returns `None` for anything that does not satisfy the layout: bad magic,
/// a declared payload length over [`MAX_PAYLOAD_LENGTH`], or a declared
/// length that does not match the bytes actually given.
pub fn try_decode(data: &[u8]) -> Option<CycloneMessage> {
    if data.len() < HEADER_SIZE {
        return None;
    }
    if data[0] != MAGIC[0] || data[1] != MAGIC[1] {
        return None;
    }

    let id = u32::from_le_bytes(data[MAGIC_SIZE..MAGIC_SIZE + MESSAGE_ID_SIZE].try_into().unwrap());
    let payload_length = u32::from_le_bytes(
        data[MAGIC_SIZE + MESSAGE_ID_SIZE..HEADER_SIZE]
            .try_into()
            .unwrap(),
    ) as usize;

    if payload_length > MAX_PAYLOAD_LENGTH {
        return None;
    }

    let actual_payload_length = data.len() - HEADER_SIZE;
    if payload_length != actual_payload_length {
        return None;
    }

    Some(CycloneMessage::new(id, data[HEADER_SIZE..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let message = CycloneMessage::new(42, b"hello-cyclone".to_vec());
        let frame = encode(&message);

        assert_eq!(frame.len(), HEADER_SIZE + message.payload.len());
        assert_eq!(&frame[0..2], &MAGIC);

        let decoded = try_decode(&frame).expect("round-trip decodes");
        assert_eq!(decoded, message);
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut frame = encode(&CycloneMessage::new(1, vec![]));
        frame[0] = 0x00;
        assert!(try_decode(&frame).is_none());
    }

    #[test]
    fn truncated_is_rejected() {
        let frame = encode(&CycloneMessage::new(1, b"abc".to_vec()));
        assert!(try_decode(&frame[..frame.len() - 1]).is_none());
        assert!(try_decode(&[]).is_none());
    }

    #[test]
    fn wrong_length_is_rejected() {
        let mut frame = encode(&CycloneMessage::new(1, b"abc".to_vec()));
        frame[MAGIC_SIZE + MESSAGE_ID_SIZE..HEADER_SIZE].copy_from_slice(&999u32.to_le_bytes());
        assert!(try_decode(&frame).is_none());
    }

    #[test]
    fn empty_payload_round_trips() {
        let message = CycloneMessage::new(system_message::PING, vec![]);
        let frame = encode(&message);
        assert_eq!(frame.len(), HEADER_SIZE);

        let decoded = try_decode(&frame).unwrap();
        assert_eq!(decoded.id, system_message::PING);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    #[should_panic(expected = "too large")]
    fn oversized_payload_panics() {
        let _ = encode(&CycloneMessage::new(1, vec![0u8; MAX_PAYLOAD_LENGTH + 1]));
    }
}
