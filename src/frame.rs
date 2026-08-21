use std::fmt;

pub const TYPE_DATA: u8 = 0;
pub const TYPE_PROBE: u8 = 1;
pub const TYPE_ACK: u8 = 2;
pub const TYPE_HANDSHAKE: u8 = 3;

pub const MAGIC: [u8; 2] = [0x43, 0x59];

pub const DATA_HEADER_LEN: usize = 11;
pub const HANDSHAKE_HEADER_LEN: usize = 5;

pub const MAX_MESSAGE_PAYLOAD: usize = 16 * 1024 * 1024;
pub const MAX_HANDSHAKE_PAYLOAD: usize = 1024 * 1024;
pub const MAX_FRAME_LEN: usize = DATA_HEADER_LEN + MAX_MESSAGE_PAYLOAD;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame<'a> {
    Data { message_id: u32, payload: &'a [u8] },
    Probe,
    Ack,
    Handshake(&'a [u8]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    UnknownType(u8),
    BadMagic([u8; 2]),
    MessageTooLarge(u64),
    HandshakeTooLarge(u64),
    Truncated,
    Trailing(usize),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            FrameError::UnknownType(byte) => write!(f, "frame type 0x{byte:02X} is not 0..=3"),
            FrameError::BadMagic(magic) => {
                write!(f, "data frame magic {:02X?} is not 'C' 'Y'", magic)
            }
            FrameError::MessageTooLarge(length) => {
                write!(f, "message payload of {length} bytes exceeds {MAX_MESSAGE_PAYLOAD}")
            }
            FrameError::HandshakeTooLarge(length) => {
                write!(f, "handshake payload of {length} bytes exceeds {MAX_HANDSHAKE_PAYLOAD}")
            }
            FrameError::Truncated => f.write_str("packet ended before the frame it declared"),
            FrameError::Trailing(count) => {
                write!(f, "{count} bytes left over after a complete frame")
            }
        }
    }
}

impl std::error::Error for FrameError {}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub fn decode(bytes: &[u8]) -> Result<Option<(Frame<'_>, usize)>, FrameError> {
    let Some(&kind) = bytes.first() else {
        return Ok(None);
    };
    match kind {
        TYPE_PROBE => Ok(Some((Frame::Probe, 1))),
        TYPE_ACK => Ok(Some((Frame::Ack, 1))),
        TYPE_DATA => {
            if bytes.len() < DATA_HEADER_LEN {
                if bytes.len() >= 3 && bytes[1..3] != MAGIC {
                    return Err(FrameError::BadMagic([bytes[1], bytes[2]]));
                }
                return Ok(None);
            }
            if bytes[1..3] != MAGIC {
                return Err(FrameError::BadMagic([bytes[1], bytes[2]]));
            }
            let message_id = read_u32(&bytes[3..7]);
            let length = u64::from(read_u32(&bytes[7..11]));
            if length > MAX_MESSAGE_PAYLOAD as u64 {
                return Err(FrameError::MessageTooLarge(length));
            }
            let length = length as usize;
            let total = DATA_HEADER_LEN + length;
            if bytes.len() < total {
                return Ok(None);
            }
            let payload = &bytes[DATA_HEADER_LEN..total];
            Ok(Some((Frame::Data { message_id, payload }, total)))
        }
        TYPE_HANDSHAKE => {
            if bytes.len() < HANDSHAKE_HEADER_LEN {
                return Ok(None);
            }
            let length = u64::from(read_u32(&bytes[1..5]));
            if length > MAX_HANDSHAKE_PAYLOAD as u64 {
                return Err(FrameError::HandshakeTooLarge(length));
            }
            let length = length as usize;
            let total = HANDSHAKE_HEADER_LEN + length;
            if bytes.len() < total {
                return Ok(None);
            }
            Ok(Some((Frame::Handshake(&bytes[HANDSHAKE_HEADER_LEN..total]), total)))
        }
        other => Err(FrameError::UnknownType(other)),
    }
}

pub fn decode_packet(bytes: &[u8]) -> Result<Frame<'_>, FrameError> {
    match decode(bytes)? {
        None => Err(FrameError::Truncated),
        Some((frame, used)) if used == bytes.len() => Ok(frame),
        Some((_, used)) => Err(FrameError::Trailing(bytes.len() - used)),
    }
}

pub fn encode_data(message_id: u32, payload: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
    if payload.len() > MAX_MESSAGE_PAYLOAD {
        return Err(FrameError::MessageTooLarge(payload.len() as u64));
    }
    out.reserve(DATA_HEADER_LEN + payload.len());
    out.push(TYPE_DATA);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&message_id.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

pub fn encode_probe(out: &mut Vec<u8>) {
    out.push(TYPE_PROBE);
}

pub fn encode_ack(out: &mut Vec<u8>) {
    out.push(TYPE_ACK);
}

pub fn encode_handshake(payload: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
    if payload.len() > MAX_HANDSHAKE_PAYLOAD {
        return Err(FrameError::HandshakeTooLarge(payload.len() as u64));
    }
    out.reserve(HANDSHAKE_HEADER_LEN + payload.len());
    out.push(TYPE_HANDSHAKE);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

#[derive(Debug, Default)]
pub struct StreamDecoder {
    buf: Vec<u8>,
    start: usize,
    poisoned: Option<FrameError>,
}

impl StreamDecoder {
    pub fn new() -> StreamDecoder {
        StreamDecoder { buf: Vec::new(), start: 0, poisoned: None }
    }

    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        if self.start > 0 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_len(&mut self) -> Result<Option<usize>, FrameError> {
        if let Some(error) = self.poisoned {
            return Err(error);
        }
        match decode(&self.buf[self.start..]) {
            Ok(Some((_, used))) => Ok(Some(used)),
            Ok(None) => Ok(None),
            Err(error) => {
                self.poisoned = Some(error);
                Err(error)
            }
        }
    }

    pub fn frame(&self, len: usize) -> Result<Frame<'_>, FrameError> {
        decode_packet(&self.buf[self.start..self.start + len])
    }

    pub fn advance(&mut self, len: usize) {
        self.start += len;
        if self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
    }
}
