use std::fmt;

use crate::schema::Schema;

pub const PROTOCOL_VERSION: u32 = 2;
pub const MAX_MESSAGES: usize = 1_000_000;

pub const HELLO_HEADER_LEN: usize = 16;
pub const HELLO_ENTRY_LEN: usize = 14;
pub const QUERY_TAG: u8 = 4;
pub const QUERY_HEADER_LEN: usize = 5;
pub const QUERY_ENTRY_LEN: usize = 6;
pub const REPLY_HEADER_LEN: usize = 4;
pub const REPLY_ENTRY_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Accept,
    WrongVersion,
    SchemaConflict,
    MalformedHello,
}

impl Verdict {
    pub fn as_byte(self) -> u8 {
        match self {
            Verdict::Accept => 0,
            Verdict::WrongVersion => 1,
            Verdict::SchemaConflict => 2,
            Verdict::MalformedHello => 3,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Verdict> {
        match byte {
            0 => Some(Verdict::Accept),
            1 => Some(Verdict::WrongVersion),
            2 => Some(Verdict::SchemaConflict),
            3 => Some(Verdict::MalformedHello),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeFailure {
    WrongVersion,
    SchemaConflict,
    MalformedHello,
    MalformedPeer,
    Timeout,
}

impl HandshakeFailure {
    pub fn from_verdict(verdict: Verdict) -> Option<HandshakeFailure> {
        match verdict {
            Verdict::Accept => None,
            Verdict::WrongVersion => Some(HandshakeFailure::WrongVersion),
            Verdict::SchemaConflict => Some(HandshakeFailure::SchemaConflict),
            Verdict::MalformedHello => Some(HandshakeFailure::MalformedHello),
        }
    }
}

impl fmt::Display for HandshakeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            HandshakeFailure::WrongVersion => "the two peers speak different handshake versions",
            HandshakeFailure::SchemaConflict => {
                "a message both peers know puts different fields at a shared index"
            }
            HandshakeFailure::MalformedHello => "the hello was rejected as malformed",
            HandshakeFailure::MalformedPeer => "the peer sent unreadable handshake traffic",
            HandshakeFailure::Timeout => "the handshake did not finish in time",
        };
        f.write_str(text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloEntry {
    pub id: u32,
    pub field_count: u16,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u32,
    pub fingerprint: u64,
    pub entries: Vec<HelloEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryItem {
    pub id: u32,
    pub field_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyItem {
    pub id: u32,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Accept,
    Reject(Verdict),
    Query(Vec<QueryItem>),
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]])
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

pub fn encode_hello(schema: &Schema) -> Vec<u8> {
    let messages = schema.messages();
    let mut out = Vec::with_capacity(HELLO_HEADER_LEN + HELLO_ENTRY_LEN * messages.len());
    out.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    out.extend_from_slice(&schema.fingerprint().to_le_bytes());
    out.extend_from_slice(&(messages.len() as u32).to_le_bytes());
    for message in messages {
        out.extend_from_slice(&message.id().to_le_bytes());
        out.extend_from_slice(&message.field_count().to_le_bytes());
        out.extend_from_slice(&message.fingerprint().to_le_bytes());
    }
    out
}

pub fn decode_hello(payload: &[u8]) -> Result<Hello, Verdict> {
    if payload.len() < HELLO_HEADER_LEN {
        return Err(Verdict::MalformedHello);
    }
    let version = u32_at(payload, 0);
    let fingerprint = u64_at(payload, 4);
    let count = u32_at(payload, 12) as usize;
    if count > MAX_MESSAGES {
        return Err(Verdict::MalformedHello);
    }
    if payload.len() != HELLO_HEADER_LEN + HELLO_ENTRY_LEN * count {
        return Err(Verdict::MalformedHello);
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let at = HELLO_HEADER_LEN + HELLO_ENTRY_LEN * index;
        entries.push(HelloEntry {
            id: u32_at(payload, at),
            field_count: u16_at(payload, at + 4),
            fingerprint: u64_at(payload, at + 6),
        });
    }
    Ok(Hello { version, fingerprint, entries })
}

pub fn decide(local: &Schema, hello: &Hello) -> Decision {
    if hello.fingerprint == local.fingerprint() {
        return Decision::Accept;
    }

    let mut queries = Vec::new();
    for entry in &hello.entries {
        let Some(known) = local.message(entry.id) else {
            continue;
        };
        if entry.fingerprint == known.fingerprint() {
            continue;
        }
        let peer_fields = entry.field_count;
        let local_fields = known.field_count();
        if peer_fields == 0 || local_fields == 0 {
            continue;
        }
        if peer_fields == local_fields {
            return Decision::Reject(Verdict::SchemaConflict);
        }
        if peer_fields < local_fields {
            match known.prefix(peer_fields) {
                Some(prefix) if prefix == entry.fingerprint => continue,
                _ => return Decision::Reject(Verdict::SchemaConflict),
            }
        }
        queries.push(QueryItem { id: entry.id, field_count: local_fields });
    }

    if queries.is_empty() {
        Decision::Accept
    } else {
        Decision::Query(queries)
    }
}

pub fn encode_verdict(verdict: Verdict) -> Vec<u8> {
    vec![verdict.as_byte()]
}

pub fn encode_query(items: &[QueryItem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(QUERY_HEADER_LEN + QUERY_ENTRY_LEN * items.len());
    out.push(QUERY_TAG);
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        out.extend_from_slice(&item.id.to_le_bytes());
        out.extend_from_slice(&item.field_count.to_le_bytes());
    }
    out
}

pub fn decode_query(payload: &[u8]) -> Option<Vec<QueryItem>> {
    if payload.len() < QUERY_HEADER_LEN || payload[0] != QUERY_TAG {
        return None;
    }
    let count = u32_at(payload, 1) as usize;
    if count > MAX_MESSAGES {
        return None;
    }
    if payload.len() != QUERY_HEADER_LEN + QUERY_ENTRY_LEN * count {
        return None;
    }
    let mut items = Vec::with_capacity(count);
    for index in 0..count {
        let at = QUERY_HEADER_LEN + QUERY_ENTRY_LEN * index;
        items.push(QueryItem { id: u32_at(payload, at), field_count: u16_at(payload, at + 4) });
    }
    Some(items)
}

pub fn encode_reply(items: &[ReplyItem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(REPLY_HEADER_LEN + REPLY_ENTRY_LEN * items.len());
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        out.extend_from_slice(&item.id.to_le_bytes());
        out.extend_from_slice(&item.fingerprint.to_le_bytes());
    }
    out
}

pub fn decode_reply(payload: &[u8]) -> Option<Vec<ReplyItem>> {
    if payload.len() < REPLY_HEADER_LEN {
        return None;
    }
    let count = u32_at(payload, 0) as usize;
    if count > MAX_MESSAGES {
        return None;
    }
    if payload.len() != REPLY_HEADER_LEN + REPLY_ENTRY_LEN * count {
        return None;
    }
    let mut items = Vec::with_capacity(count);
    for index in 0..count {
        let at = REPLY_HEADER_LEN + REPLY_ENTRY_LEN * index;
        items.push(ReplyItem { id: u32_at(payload, at), fingerprint: u64_at(payload, at + 4) });
    }
    Some(items)
}

pub fn answer_query(local: &Schema, items: &[QueryItem]) -> Option<Vec<ReplyItem>> {
    let mut reply = Vec::with_capacity(items.len());
    for item in items {
        let known = local.message(item.id)?;
        if item.field_count == 0 || item.field_count >= known.field_count() {
            return None;
        }
        reply.push(ReplyItem { id: item.id, fingerprint: known.prefix(item.field_count)? });
    }
    Some(reply)
}

pub fn check_reply(local: &Schema, asked: &[QueryItem], reply: &[ReplyItem]) -> Verdict {
    if asked.len() != reply.len() {
        return Verdict::MalformedHello;
    }
    for (item, answer) in asked.iter().zip(reply) {
        if item.id != answer.id {
            return Verdict::MalformedHello;
        }
        let expected = local.message(item.id).and_then(|known| known.prefix(item.field_count));
        match expected {
            Some(prefix) if prefix == answer.fingerprint => {}
            _ => return Verdict::SchemaConflict,
        }
    }
    Verdict::Accept
}
