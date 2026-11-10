use crate::frame::{Part, MAX_HANDSHAKE_PARTS, MAX_HANDSHAKE_PAYLOAD, PART_CONTENT_LEN};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Assembly {
    Pending,
    Complete,
    Broken,
}

#[derive(Debug, Default)]
pub(crate) struct Assembler {
    content: Vec<u8>,
    received: Vec<bool>,
    count: usize,
    arrived: usize,
    last_len: usize,
}

impl Assembler {
    pub(crate) fn is_pending(&self) -> bool {
        self.count > 0
    }

    pub(crate) fn body(&self) -> &[u8] {
        &self.content[..(self.count - 1) * PART_CONTENT_LEN + self.last_len]
    }

    pub(crate) fn add(&mut self, part: Part<'_>) -> Assembly {
        if part.count < 2 || part.count > MAX_HANDSHAKE_PARTS || part.index >= part.count {
            return Assembly::Broken;
        }
        let last = part.index == part.count - 1;
        let misfit =
            if last { part.bytes.is_empty() } else { part.bytes.len() != PART_CONTENT_LEN };
        if misfit {
            return Assembly::Broken;
        }

        if self.count == 0 {
            self.begin(part.count);
        } else if part.count != self.count {
            return Assembly::Broken;
        }

        let offset = part.index * PART_CONTENT_LEN;
        if self.received[part.index] {
            if last && part.bytes.len() != self.last_len {
                return Assembly::Broken;
            }
            if part.bytes != &self.content[offset..offset + part.bytes.len()] {
                return Assembly::Broken;
            }
            return Assembly::Pending;
        }

        self.content[offset..offset + part.bytes.len()].copy_from_slice(part.bytes);
        self.received[part.index] = true;
        self.arrived += 1;
        if last {
            self.last_len = part.bytes.len();
        }

        if self.arrived < self.count {
            return Assembly::Pending;
        }
        if self.body().len() > MAX_HANDSHAKE_PAYLOAD {
            return Assembly::Broken;
        }
        Assembly::Complete
    }

    pub(crate) fn reset(&mut self) {
        self.received[..self.count].fill(false);
        self.count = 0;
        self.arrived = 0;
        self.last_len = 0;
    }

    pub(crate) fn shrink(&mut self) {
        if self.count == 0 {
            self.content = Vec::new();
            self.received = Vec::new();
        }
    }

    fn begin(&mut self, count: usize) {
        let capacity = count * PART_CONTENT_LEN;
        if self.content.len() < capacity {
            self.content = vec![0; capacity];
        }
        if self.received.len() < count {
            self.received = vec![false; count];
        }
        self.count = count;
    }
}
