use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::connection::{Core, SendError};
use crate::event::{EventSink, PeerEvents, PeerId};
use crate::schema::Schema;
use crate::session::{Config, Role, SessionState};
use crate::transport::{Accept, ServerTransport, Transport};

#[derive(Debug)]
struct Peer<T: Transport> {
    id: PeerId,
    core: Core<T>,
}

#[derive(Debug)]
pub struct Server<L: ServerTransport> {
    listener: L,
    schema: Arc<Schema>,
    config: Config,
    peers: Vec<Peer<L::Peer>>,
    positions: HashMap<PeerId, usize>,
    next_id: u64,
    sink: EventSink,
}

impl<L: ServerTransport> Server<L> {
    pub fn new(listener: L, schema: Arc<Schema>, config: Config) -> Server<L> {
        Server {
            listener,
            schema,
            config,
            peers: Vec::new(),
            positions: HashMap::new(),
            next_id: 1,
            sink: EventSink::new(),
        }
    }

    pub fn listener(&self) -> &L {
        &self.listener
    }

    pub fn listener_mut(&mut self) -> &mut L {
        &mut self.listener
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().map(|peer| peer.id)
    }

    pub fn state(&self, peer: PeerId) -> Option<SessionState> {
        self.find(peer).map(|found| found.core.session().state())
    }

    pub fn is_ready(&self, peer: PeerId) -> bool {
        self.state(peer) == Some(SessionState::Ready)
    }

    pub fn transport(&self, peer: PeerId) -> Option<&L::Peer> {
        self.find(peer).map(|found| found.core.transport())
    }

    pub fn tick(&mut self, now: Instant) -> PeerEvents<'_> {
        let Server { listener, schema, config, peers, positions, next_id, sink } = self;
        sink.clear();

        let mut room = config.max_frames_per_tick;
        while room > 0 {
            match listener.accept() {
                Accept::Accepted(transport) => {
                    let id = PeerId(*next_id);
                    *next_id += 1;
                    let core = Core::new(transport, schema.clone(), *config, Role::Server, now);
                    positions.insert(id, peers.len());
                    peers.push(Peer { id, core });
                    room -= 1;
                }
                Accept::Pending | Accept::Error(_) => break,
            }
        }

        for peer in peers.iter_mut() {
            peer.core.tick(now, peer.id, sink);
        }
        let before = peers.len();
        peers.retain(|peer| !peer.core.is_finished());
        if peers.len() != before {
            positions.clear();
            positions.extend(peers.iter().enumerate().map(|(position, peer)| (peer.id, position)));
        }

        sink.peer_events()
    }

    pub fn tick_now(&mut self) -> PeerEvents<'_> {
        self.tick(Instant::now())
    }

    pub fn send(&mut self, peer: PeerId, message_id: u32, payload: &[u8]) -> Result<(), SendError> {
        match self.find_mut(peer) {
            Some(found) => found.core.send(message_id, payload),
            None => Err(SendError::Closed),
        }
    }

    pub fn broadcast(&mut self, message_id: u32, payload: &[u8]) {
        for peer in self.peers.iter_mut() {
            let _ = peer.core.send(message_id, payload);
        }
    }

    pub fn disconnect(&mut self, peer: PeerId) {
        if let Some(found) = self.find_mut(peer) {
            found.core.close();
        }
    }

    pub fn close(&mut self) {
        for peer in self.peers.iter_mut() {
            peer.core.close();
        }
        self.peers.clear();
        self.positions.clear();
        self.listener.close();
    }

    pub fn shrink_to_fit(&mut self) {
        self.sink.shrink_to_fit();
        for peer in self.peers.iter_mut() {
            peer.core.shrink();
        }
    }

    fn find(&self, peer: PeerId) -> Option<&Peer<L::Peer>> {
        self.positions.get(&peer).map(|&position| &self.peers[position])
    }

    fn find_mut(&mut self, peer: PeerId) -> Option<&mut Peer<L::Peer>> {
        self.positions.get(&peer).map(|&position| &mut self.peers[position])
    }
}
