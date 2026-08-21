use std::io;

pub mod tcp;
pub mod udp;

pub use tcp::{TcpListenerTransport, TcpTransport};
pub use udp::{UdpPeerTransport, UdpServerTransport, UdpTransport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Stream,
    Message,
}

#[derive(Debug)]
pub enum SendOutcome {
    Sent,
    Partial(usize),
    WouldBlock,
    TooLarge,
    Closed,
    Error(io::Error),
}

#[derive(Debug)]
pub enum RecvOutcome {
    Received(usize),
    WouldBlock,
    NeedCapacity(usize),
    Closed,
    Error(io::Error),
}

pub trait Transport {
    fn kind(&self) -> TransportKind;
    fn send(&mut self, bytes: &[u8]) -> SendOutcome;
    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome;
    fn close_soft(&mut self);
    fn close_hard(&mut self);
}

#[derive(Debug)]
pub enum Accept<P> {
    Accepted(P),
    Pending,
    Error(io::Error),
}

pub trait ServerTransport {
    type Peer: Transport;

    fn accept(&mut self) -> Accept<Self::Peer>;
    fn close(&mut self);
}
