//! WebSockets. A client request can upgrade: its handler opens a socket on
//! the node it runs on, of one of the three kinds the core knows. Each
//! message on the socket is a request of its own, which reaches the core
//! as `WebSocketRequestAt`, runs a handler and answers through the output
//! gate like any other. The client closes the socket after a while.
//!
//! The shell also closes a socket when the core asks (`CloseWebSocket`),
//! when a handoff cancels the cell's activity (`CancelCellActivity`), and
//! when its process dies. Every close but one on a process's death is
//! reported to the core (`WebSocketClosed`), as `actor.rs` does: a dead
//! process has no core to tell.

use std::collections::VecDeque;

use celld_logic::{Event, WebSocketId, WebSocketKind};

use super::{Work, World};
use crate::Mutation;

/// How long a client keeps a socket open, and how often it sends on it.
const SOCKET_LIFETIME_MS: (u64, u64) = (200, 5_000);
const MESSAGE_GAP_MS: (u64, u64) = (20, 500);

/// One open socket, on the node whose process holds it.
pub(super) struct Socket {
    pub(super) cell: String,
    pub(super) kind: WebSocketKind,
}

/// A client's work on its socket.
pub(super) enum SocketWork {
    /// The client sends on its WebSocket.
    Message { websocket: WebSocketId },
    /// The client closes its WebSocket.
    Closed { websocket: WebSocketId },
}

impl World {
    pub(super) fn socket_work(&mut self, node: usize, work: SocketWork) {
        match work {
            SocketWork::Message { websocket } => self.socket_message(node, websocket),
            SocketWork::Closed { websocket } => self.socket_closed(node, websocket),
        }
    }

    /// A handler upgrades its request: open a socket of a drawn kind for
    /// its cell.
    pub(super) fn open_socket(&mut self, node: usize, cell: String) {
        let websocket = self.next_socket;
        self.next_socket += 1;
        let kind = *self
            .work_rng
            .pick(&[
                WebSocketKind::Hibernatable,
                WebSocketKind::Regular,
                WebSocketKind::Outbound,
            ])
            .unwrap();
        let socket = Socket {
            cell: cell.clone(),
            kind,
        };
        self.machines[node]
            .proc_mut()
            .sockets
            .insert(websocket, socket);
        self.stats.sockets += 1;
        let event = Event::WebSocketOpened {
            cell,
            websocket,
            kind,
        };
        self.drive(node, event);
        let (lo, hi) = SOCKET_LIFETIME_MS;
        let lifetime = self.work_rng.range(lo, hi);
        self.work(
            node,
            lifetime,
            Work::Socket(SocketWork::Closed { websocket }),
        );
        self.next_message(node, websocket);
    }

    fn next_message(&mut self, node: usize, websocket: WebSocketId) {
        let (lo, hi) = MESSAGE_GAP_MS;
        let gap = self.work_rng.range(lo, hi);
        self.work(node, gap, Work::Socket(SocketWork::Message { websocket }));
    }

    /// The client sends on its socket: a request for the socket's cell, on
    /// this node.
    pub(super) fn socket_message(&mut self, node: usize, websocket: WebSocketId) {
        let Some(socket) = self.machines[node].proc().sockets.get(&websocket) else {
            return;
        };
        let cell = socket.cell.clone();
        self.touch(node, &cell);
        let write = self.work_rng.chance(self.config.write_probability);
        self.send_on_socket(cell, write, node, websocket);
        self.next_message(node, websocket);
    }

    /// The client closes its socket.
    pub(super) fn socket_closed(&mut self, node: usize, websocket: WebSocketId) {
        if let Some(socket) = self.machines[node].proc_mut().sockets.remove(&websocket) {
            let event = Event::WebSocketClosed {
                cell: socket.cell,
                websocket,
            };
            self.drive(node, event);
        }
    }

    /// The core declined a socket, or a handoff cancelled it: close it, and
    /// tell the core at once.
    pub(super) fn close_sockets(
        &mut self,
        node: usize,
        cell: &str,
        websockets: Vec<WebSocketId>,
        immediate: &mut VecDeque<Event>,
    ) {
        for websocket in websockets {
            // The planted bug reports the close without closing anything.
            if self.config.mutation != Some(Mutation::PhantomSocketClose) {
                self.machines[node].proc_mut().sockets.remove(&websocket);
            }
            immediate.push_back(Event::WebSocketClosed {
                cell: cell.to_string(),
                websocket,
            });
        }
    }

    /// Whether `cell` holds a socket open here that pins its runtime: any
    /// kind but a hibernatable one.
    pub(super) fn pinned(&self, node: usize, cell: &str) -> bool {
        self.machines[node]
            .proc()
            .sockets
            .values()
            .any(|socket| socket.cell == cell && socket.kind != WebSocketKind::Hibernatable)
    }
}
