use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};

use crate::transport::{
    Accept, RecvOutcome, SendOutcome, ServerTransport, Transport, TransportKind,
};

fn retryable(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted)
}

#[derive(Debug)]
pub struct TcpTransport {
    socket: TcpStream,
    closed: bool,
}

impl TcpTransport {
    pub fn connect<A: ToSocketAddrs>(address: A) -> io::Result<TcpTransport> {
        TcpTransport::from_stream(TcpStream::connect(address)?)
    }

    pub fn from_stream(socket: TcpStream) -> io::Result<TcpTransport> {
        socket.set_nonblocking(true)?;
        let _ = socket.set_nodelay(true);
        Ok(TcpTransport { socket, closed: false })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
    }
}

impl Transport for TcpTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Stream
    }

    fn send(&mut self, bytes: &[u8]) -> SendOutcome {
        if self.closed {
            return SendOutcome::Closed;
        }
        if bytes.is_empty() {
            return SendOutcome::Sent;
        }
        match self.socket.write(bytes) {
            Ok(0) => SendOutcome::WouldBlock,
            Ok(count) if count == bytes.len() => SendOutcome::Sent,
            Ok(count) => SendOutcome::Partial(count),
            Err(error) if retryable(&error) => SendOutcome::WouldBlock,
            Err(error) => {
                self.closed = true;
                SendOutcome::Error(error)
            }
        }
    }

    fn recv(&mut self, buffer: &mut [u8]) -> RecvOutcome {
        if self.closed {
            return RecvOutcome::Closed;
        }
        match self.socket.read(buffer) {
            Ok(0) => {
                self.closed = true;
                RecvOutcome::Closed
            }
            Ok(count) => RecvOutcome::Received(count),
            Err(error) if retryable(&error) => RecvOutcome::WouldBlock,
            Err(error) => {
                self.closed = true;
                RecvOutcome::Error(error)
            }
        }
    }

    fn close_soft(&mut self) {
        if !self.closed {
            let _ = self.socket.shutdown(Shutdown::Write);
        }
    }

    fn close_hard(&mut self) {
        self.closed = true;
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

#[derive(Debug)]
pub struct TcpListenerTransport {
    listener: TcpListener,
}

impl TcpListenerTransport {
    pub fn bind<A: ToSocketAddrs>(address: A) -> io::Result<TcpListenerTransport> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(TcpListenerTransport { listener })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl ServerTransport for TcpListenerTransport {
    type Peer = TcpTransport;

    fn accept(&mut self) -> Accept<TcpTransport> {
        match self.listener.accept() {
            Ok((stream, _)) => match TcpTransport::from_stream(stream) {
                Ok(transport) => Accept::Accepted(transport),
                Err(error) => Accept::Error(error),
            },
            Err(error) if retryable(&error) => Accept::Pending,
            Err(error) => Accept::Error(error),
        }
    }

    fn close(&mut self) {}
}
