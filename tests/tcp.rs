use std::io::{self, Write};
use std::thread;
use std::time::{Duration, Instant};

use fomoxa_net::transport::{Accept, ServerTransport, TcpListenerTransport, TcpTransport};

fn accept_within(listener: &mut TcpListenerTransport, limit: Duration) -> TcpTransport {
    let deadline = Instant::now() + limit;
    loop {
        match listener.accept() {
            Accept::Accepted(peer) => return peer,
            Accept::Pending if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
            Accept::Pending => panic!("no connection was accepted within {limit:?}"),
            Accept::Error(error) => panic!("accept failed: {error}"),
        }
    }
}

fn peek_within(transport: &TcpTransport, limit: Duration) -> usize {
    let deadline = Instant::now() + limit;
    let mut probe = [0u8; 16];
    loop {
        match transport.socket().peek(&mut probe) {
            Ok(count) => return count,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1))
            }
            Err(error) => panic!("peek failed: {error}"),
        }
    }
}

#[test]
fn the_listener_socket_is_the_bound_listener() {
    let listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();

    assert_eq!(listener.socket().local_addr().unwrap(), listener.local_addr().unwrap());
}

#[test]
fn the_transport_socket_is_the_connected_non_blocking_stream() {
    let mut listener = TcpListenerTransport::bind("127.0.0.1:0").unwrap();
    let client = TcpTransport::connect(listener.local_addr().unwrap()).unwrap();
    let server = accept_within(&mut listener, Duration::from_secs(5));

    assert_eq!(server.socket().peer_addr().unwrap(), client.local_addr().unwrap());
    let mut probe = [0u8; 1];
    let idle = server.socket().peek(&mut probe).unwrap_err();
    assert_eq!(idle.kind(), io::ErrorKind::WouldBlock);

    let mut writer = client.socket().try_clone().unwrap();
    writer.write_all(b"ready").unwrap();
    assert!(peek_within(&server, Duration::from_secs(5)) > 0);
}
