use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::rc::Rc;

use crate::transport::{
    Accept, RecvOutcome, SendOutcome, ServerTransport, Transport, TransportKind,
};

const HEADER_LEN: usize = 8;

pub const MAX_DATAGRAM: usize = 65_507 - HEADER_LEN;

const SCRATCH_LEN: usize = 65_535;
const MAX_QUEUED_PER_PEER: usize = 64;
const MAX_TRACKED_PEERS: usize = 4096;
const MAX_READS_PER_PUMP: usize = 256;

fn retryable(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const MESSAGE_TOO_LONG: i32 = 90;
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const MESSAGE_TOO_LONG: i32 = 40;
#[cfg(windows)]
const MESSAGE_TOO_LONG: i32 = 10040;
#[cfg(not(any(unix, windows)))]
const MESSAGE_TOO_LONG: i32 = i32::MIN;

fn too_large(error: &io::Error) -> bool {
    error.raw_os_error() == Some(MESSAGE_TOO_LONG) || error.kind() == io::ErrorKind::InvalidInput
}

fn random_bits() -> u64 {
    let mut bytes = [0u8; 8];
    if fill_from_os(&mut bytes) {
        return u64::from_le_bytes(bytes);
    }
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0x6f6d_6f66);
    hasher.finish()
}

#[cfg(unix)]
fn fill_from_os(bytes: &mut [u8]) -> bool {
    use std::io::Read;
    std::fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(bytes)).is_ok()
}

#[cfg(windows)]
fn fill_from_os(bytes: &mut [u8]) -> bool {
    #[link(name = "advapi32")]
    extern "system" {
        fn SystemFunction036(buffer: *mut u8, length: u32) -> u8;
    }
    unsafe { SystemFunction036(bytes.as_mut_ptr(), bytes.len() as u32) != 0 }
}

#[cfg(not(any(unix, windows)))]
fn fill_from_os(_bytes: &mut [u8]) -> bool {
    false
}

#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
    inbox: Vec<u8>,
    inbox_len: usize,
    holding: bool,
    session: u64,
    closed: bool,
}

impl UdpTransport {
    pub fn connect<A: ToSocketAddrs>(address: A) -> io::Result<UdpTransport> {
        UdpTransport::bind_and_connect("0.0.0.0:0", address)
    }

    pub fn bind_and_connect<L: ToSocketAddrs, R: ToSocketAddrs>(
        local: L,
        remote: R,
    ) -> io::Result<UdpTransport> {
        let socket = UdpSocket::bind(local)?;
        socket.connect(remote)?;
        socket.set_nonblocking(true)?;
        Ok(UdpTransport {
            socket,
            inbox: vec![0; SCRATCH_LEN],
            inbox_len: 0,
            holding: false,
            session: 0,
            closed: false,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
    }
}

impl Transport for UdpTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Message
    }

    fn send(&mut self, bytes: &[u8]) -> SendOutcome {
        if self.closed {
            return SendOutcome::Closed;
        }
        if bytes.len() > MAX_DATAGRAM {
            return SendOutcome::TooLarge;
        }
        let mut datagram = Vec::with_capacity(HEADER_LEN + bytes.len());
        datagram.extend_from_slice(&self.session.to_le_bytes());
        datagram.extend_from_slice(bytes);
        match self.socket.send(&datagram) {
            Ok(count) if count == datagram.len() => SendOutcome::Sent,
            Ok(_) => SendOutcome::TooLarge,
            Err(error) if retryable(&error) => SendOutcome::WouldBlock,
            Err(error) if too_large(&error) => SendOutcome::TooLarge,
            Err(error) => {
                self.closed = true;
                SendOutcome::Error(error)
            }
        }
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        while !self.holding {
            if self.closed {
                return RecvOutcome::Closed;
            }
            match self.socket.recv(&mut self.inbox) {
                Ok(count) if count > HEADER_LEN => {
                    let mut header = [0u8; HEADER_LEN];
                    header.copy_from_slice(&self.inbox[..HEADER_LEN]);
                    let received = u64::from_le_bytes(header);
                    if received == 0 || (self.session != 0 && received != self.session) {
                        continue;
                    }
                    self.session = received;
                    self.inbox_len = count - HEADER_LEN;
                    self.holding = true;
                }
                Ok(_) => continue,
                Err(error) if retryable(&error) => return RecvOutcome::WouldBlock,
                Err(error) => {
                    self.closed = true;
                    return RecvOutcome::Error(error);
                }
            }
        }
        if self.inbox_len > buffer.len() {
            return RecvOutcome::NeedCapacity(self.inbox_len);
        }
        buffer[..self.inbox_len].copy_from_slice(&self.inbox[HEADER_LEN..HEADER_LEN + self.inbox_len]);
        self.holding = false;
        RecvOutcome::Received(self.inbox_len)
    }

    fn close_soft(&mut self) {}

    fn close_hard(&mut self) {
        self.closed = true;
        self.holding = false;
        self.inbox = Vec::new();
        self.inbox_len = 0;
    }
}

#[derive(Debug)]
struct Slot {
    address: SocketAddr,
    queue: VecDeque<Vec<u8>>,
}

impl Slot {
    fn push(&mut self, frame: &[u8]) {
        if self.queue.len() >= MAX_QUEUED_PER_PEER {
            self.queue.pop_front();
        }
        self.queue.push_back(frame.to_vec());
    }
}

#[derive(Debug)]
struct Hub {
    socket: UdpSocket,
    scratch: Vec<u8>,
    slots: HashMap<u64, Slot>,
    by_address: HashMap<SocketAddr, u64>,
    arrivals: VecDeque<u64>,
    closed: bool,
}

impl Hub {
    fn new_session(&self) -> u64 {
        loop {
            let candidate = random_bits();
            if candidate != 0 && !self.slots.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    fn pump(&mut self) {
        if self.closed {
            return;
        }
        for _ in 0..MAX_READS_PER_PUMP {
            let (count, address) = match self.socket.recv_from(&mut self.scratch) {
                Ok(received) => received,
                Err(_) => return,
            };
            if count <= HEADER_LEN {
                continue;
            }
            let mut header = [0u8; HEADER_LEN];
            header.copy_from_slice(&self.scratch[..HEADER_LEN]);
            let session = u64::from_le_bytes(header);
            if session != 0 {
                let Some(slot) = self.slots.get_mut(&session) else {
                    continue;
                };
                if slot.address != address {
                    if self.by_address.contains_key(&address) {
                        continue;
                    }
                    self.by_address.remove(&slot.address);
                    slot.address = address;
                    self.by_address.insert(address, session);
                }
                slot.push(&self.scratch[HEADER_LEN..count]);
                continue;
            }
            if let Some(known) = self.by_address.get(&address).copied() {
                if let Some(slot) = self.slots.get_mut(&known) {
                    slot.push(&self.scratch[HEADER_LEN..count]);
                }
                continue;
            }
            if self.slots.len() >= MAX_TRACKED_PEERS {
                continue;
            }
            let created = self.new_session();
            let mut slot = Slot { address, queue: VecDeque::new() };
            slot.push(&self.scratch[HEADER_LEN..count]);
            self.slots.insert(created, slot);
            self.by_address.insert(address, created);
            self.arrivals.push_back(created);
        }
    }

    fn forget(&mut self, session: u64) {
        if let Some(slot) = self.slots.remove(&session) {
            self.by_address.remove(&slot.address);
        }
    }
}

#[derive(Debug, Clone)]
pub struct UdpServerTransport {
    hub: Rc<RefCell<Hub>>,
}

impl UdpServerTransport {
    pub fn bind<A: ToSocketAddrs>(address: A) -> io::Result<UdpServerTransport> {
        let socket = UdpSocket::bind(address)?;
        socket.set_nonblocking(true)?;
        Ok(UdpServerTransport {
            hub: Rc::new(RefCell::new(Hub {
                socket,
                scratch: vec![0; SCRATCH_LEN],
                slots: HashMap::new(),
                by_address: HashMap::new(),
                arrivals: VecDeque::new(),
                closed: false,
            })),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.hub.borrow().socket.local_addr()
    }
}

impl ServerTransport for UdpServerTransport {
    type Peer = UdpPeerTransport;

    fn accept(&mut self) -> Accept<UdpPeerTransport> {
        let mut hub = self.hub.borrow_mut();
        hub.pump();
        match hub.arrivals.pop_front() {
            Some(session) => Accept::Accepted(UdpPeerTransport {
                hub: Rc::clone(&self.hub),
                session,
                closed: false,
            }),
            None => Accept::Pending,
        }
    }

    fn close(&mut self) {
        let mut hub = self.hub.borrow_mut();
        hub.closed = true;
        hub.slots.clear();
        hub.by_address.clear();
        hub.arrivals.clear();
    }
}

#[derive(Debug)]
pub struct UdpPeerTransport {
    hub: Rc<RefCell<Hub>>,
    session: u64,
    closed: bool,
}

impl UdpPeerTransport {
    pub fn address(&self) -> Option<SocketAddr> {
        self.hub.borrow().slots.get(&self.session).map(|slot| slot.address)
    }
}

impl Transport for UdpPeerTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Message
    }

    fn send(&mut self, bytes: &[u8]) -> SendOutcome {
        if self.closed {
            return SendOutcome::Closed;
        }
        if bytes.len() > MAX_DATAGRAM {
            return SendOutcome::TooLarge;
        }
        let hub = self.hub.borrow();
        if hub.closed {
            return SendOutcome::Closed;
        }
        let Some(slot) = hub.slots.get(&self.session) else {
            return SendOutcome::Closed;
        };
        let mut datagram = Vec::with_capacity(HEADER_LEN + bytes.len());
        datagram.extend_from_slice(&self.session.to_le_bytes());
        datagram.extend_from_slice(bytes);
        match hub.socket.send_to(&datagram, slot.address) {
            Ok(count) if count == datagram.len() => SendOutcome::Sent,
            Ok(_) => SendOutcome::TooLarge,
            Err(error) if retryable(&error) => SendOutcome::WouldBlock,
            Err(error) if too_large(&error) => SendOutcome::TooLarge,
            Err(error) => SendOutcome::Error(error),
        }
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        if self.closed {
            return RecvOutcome::Closed;
        }
        let mut hub = self.hub.borrow_mut();
        hub.pump();
        let Some(slot) = hub.slots.get_mut(&self.session) else {
            return RecvOutcome::Closed;
        };
        let Some(datagram) = slot.queue.front() else {
            return RecvOutcome::WouldBlock;
        };
        if datagram.len() > buffer.len() {
            return RecvOutcome::NeedCapacity(datagram.len());
        }
        buffer[..datagram.len()].copy_from_slice(datagram);
        let count = datagram.len();
        slot.queue.pop_front();
        RecvOutcome::Received(count)
    }

    fn close_soft(&mut self) {}

    fn close_hard(&mut self) {
        self.closed = true;
        self.hub.borrow_mut().forget(self.session);
    }
}
