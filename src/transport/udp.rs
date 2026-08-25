use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::rc::Rc;

use crate::transport::{
    Accept, RecvOutcome, SendOutcome, ServerTransport, Transport, TransportKind,
};

pub const MAX_DATAGRAM: usize = 65_507;

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

#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
    inbox: Vec<u8>,
    inbox_len: usize,
    holding: bool,
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
        match self.socket.send(bytes) {
            Ok(count) if count == bytes.len() => SendOutcome::Sent,
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
        if !self.holding {
            if self.closed {
                return RecvOutcome::Closed;
            }
            match self.socket.recv(&mut self.inbox) {
                Ok(count) => {
                    self.inbox_len = count;
                    self.holding = true;
                }
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
        buffer[..self.inbox_len].copy_from_slice(&self.inbox[..self.inbox_len]);
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
struct Hub {
    socket: UdpSocket,
    scratch: Vec<u8>,
    peers: HashMap<SocketAddr, VecDeque<Vec<u8>>>,
    arrivals: VecDeque<SocketAddr>,
    closed: bool,
}

impl Hub {
    fn pump(&mut self) {
        if self.closed {
            return;
        }
        let Hub { socket, scratch, peers, arrivals, .. } = self;
        for _ in 0..MAX_READS_PER_PUMP {
            match socket.recv_from(scratch) {
                Ok((count, address)) => {
                    if !peers.contains_key(&address) {
                        if peers.len() >= MAX_TRACKED_PEERS {
                            continue;
                        }
                        peers.insert(address, VecDeque::new());
                        arrivals.push_back(address);
                    }
                    let queue = peers.get_mut(&address).expect("just inserted");
                    if queue.len() >= MAX_QUEUED_PER_PEER {
                        queue.pop_front();
                    }
                    queue.push_back(scratch[..count].to_vec());
                }
                Err(error) if retryable(&error) => return,
                Err(_) => return,
            }
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
                peers: HashMap::new(),
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
            Some(address) => Accept::Accepted(UdpPeerTransport {
                hub: Rc::clone(&self.hub),
                address,
                closed: false,
            }),
            None => Accept::Pending,
        }
    }

    fn close(&mut self) {
        let mut hub = self.hub.borrow_mut();
        hub.closed = true;
        hub.peers.clear();
        hub.arrivals.clear();
    }
}

#[derive(Debug)]
pub struct UdpPeerTransport {
    hub: Rc<RefCell<Hub>>,
    address: SocketAddr,
    closed: bool,
}

impl UdpPeerTransport {
    pub fn address(&self) -> SocketAddr {
        self.address
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
        match hub.socket.send_to(bytes, self.address) {
            Ok(count) if count == bytes.len() => SendOutcome::Sent,
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
        let Some(queue) = hub.peers.get_mut(&self.address) else {
            return RecvOutcome::Closed;
        };
        let Some(datagram) = queue.front() else {
            return RecvOutcome::WouldBlock;
        };
        if datagram.len() > buffer.len() {
            return RecvOutcome::NeedCapacity(datagram.len());
        }
        buffer[..datagram.len()].copy_from_slice(datagram);
        let count = datagram.len();
        queue.pop_front();
        RecvOutcome::Received(count)
    }

    fn close_soft(&mut self) {}

    fn close_hard(&mut self) {
        self.closed = true;
        self.hub.borrow_mut().peers.remove(&self.address);
    }
}
