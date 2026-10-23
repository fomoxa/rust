use std::net::UdpSocket;
use std::thread::sleep;
use std::time::Duration;

use fomoxa_net::{
    Accept, RecvOutcome, SendOutcome, ServerTransport, Transport, UdpPeerTransport,
    UdpServerTransport, UdpTransport,
};

fn datagram(session: u64, frame: &[u8]) -> Vec<u8> {
    let mut bytes = session.to_le_bytes().to_vec();
    bytes.extend_from_slice(frame);
    bytes
}

fn sender() -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    socket
}

fn settle() {
    sleep(Duration::from_millis(50));
}

fn accept(listener: &mut UdpServerTransport) -> UdpPeerTransport {
    for _ in 0..200 {
        if let Accept::Accepted(peer) = listener.accept() {
            return peer;
        }
        sleep(Duration::from_millis(5));
    }
    panic!("no peer was accepted");
}

fn drain(listener: &mut UdpServerTransport) {
    while !matches!(listener.accept(), Accept::Pending) {}
}

fn received(peer: &mut UdpPeerTransport) -> Option<Vec<u8>> {
    let mut buffer = [0u8; 64];
    match peer.recv(&mut buffer) {
        RecvOutcome::Received(count) => Some(buffer[..count].to_vec()),
        _ => None,
    }
}

fn session_of(socket: &UdpSocket) -> (u64, usize) {
    let mut reply = [0u8; 64];
    let count = socket.recv(&mut reply).unwrap();
    (u64::from_le_bytes(reply[..8].try_into().unwrap()), count)
}

#[test]
fn a_peer_keeps_its_session_when_its_source_address_changes() {
    let mut listener = UdpServerTransport::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap();
    let before = sender();
    let after = sender();

    before.send_to(&datagram(0, &[1, 2]), target).unwrap();
    let mut peer = accept(&mut listener);
    assert_eq!(received(&mut peer), Some(vec![1, 2]));
    assert_eq!(peer.address(), Some(before.local_addr().unwrap()));

    assert!(matches!(peer.send(&[9]), SendOutcome::Sent));
    let (session, count) = session_of(&before);
    assert_ne!(session, 0);
    assert_eq!(count, 9);

    after.send_to(&datagram(session, &[3]), target).unwrap();
    settle();
    drain(&mut listener);
    assert_eq!(peer.address(), Some(after.local_addr().unwrap()));
    assert_eq!(received(&mut peer), Some(vec![3]));

    assert!(matches!(peer.send(&[10]), SendOutcome::Sent));
    assert_eq!(session_of(&after), (session, 9));
}

#[test]
fn unknown_ids_short_datagrams_and_taken_addresses_move_nobody() {
    let mut listener = UdpServerTransport::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap();
    let first = sender();
    let second = sender();

    first.send_to(&datagram(0, &[1]), target).unwrap();
    let mut peer = accept(&mut listener);
    assert_eq!(received(&mut peer), Some(vec![1]));
    assert!(matches!(peer.send(&[0]), SendOutcome::Sent));
    let (session, _) = session_of(&first);

    first.send_to(&[0u8; 8], target).unwrap();
    second.send_to(&datagram(session ^ 1, &[5]), target).unwrap();
    settle();
    drain(&mut listener);
    assert_eq!(received(&mut peer), None);
    assert_eq!(peer.address(), Some(first.local_addr().unwrap()));

    second.send_to(&datagram(0, &[6]), target).unwrap();
    let mut other = accept(&mut listener);
    assert_eq!(received(&mut other), Some(vec![6]));
    assert!(matches!(other.send(&[0]), SendOutcome::Sent));
    let (other_session, _) = session_of(&second);

    first.send_to(&datagram(other_session, &[7]), target).unwrap();
    settle();
    drain(&mut listener);
    assert_eq!(other.address(), Some(second.local_addr().unwrap()));
}

#[test]
fn a_client_sends_zero_first_and_keeps_the_first_id_it_receives() {
    let server = sender();
    let mut client = UdpTransport::connect(server.local_addr().unwrap()).unwrap();

    assert!(matches!(client.send(&[1]), SendOutcome::Sent));
    let mut request = [0u8; 64];
    let (count, from) = server.recv_from(&mut request).unwrap();
    assert_eq!(count, 9);
    assert_eq!(u64::from_le_bytes(request[..8].try_into().unwrap()), 0);

    server.send_to(&datagram(0, &[2]), from).unwrap();
    server.send_to(&datagram(11, &[3]), from).unwrap();
    server.send_to(&datagram(22, &[4]), from).unwrap();
    settle();

    let mut buffer = [0u8; 64];
    assert!(matches!(client.recv(&mut buffer), RecvOutcome::Received(1)));
    assert_eq!(buffer[0], 3);
    assert!(matches!(client.recv(&mut buffer), RecvOutcome::WouldBlock));

    assert!(matches!(client.send(&[8]), SendOutcome::Sent));
    server.recv_from(&mut request).unwrap();
    assert_eq!(u64::from_le_bytes(request[..8].try_into().unwrap()), 11);
}
