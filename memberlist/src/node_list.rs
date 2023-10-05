use autosurgeon::{hydrate, reconcile, Hydrate, Reconcile};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Deserialize, Serialize, Reconcile, Hydrate, Clone, Debug)]
pub struct Node {
    pub name: Option<String>,
    #[key]
    #[autosurgeon(with = "with_ip_addr")]
    pub addr: std::net::IpAddr,
    pub port: u16,
    pub state: NodeState,
    pub custom_info: Option<Vec<u8>>,
}

impl Node {
    pub fn new(name: Option<String>, addr: std::net::IpAddr, port: u16, custom_info: Option<Vec<u8>>) -> Self {
        Self {
            name,
            addr,
            port,
            state: NodeState::Alive,
            custom_info,
        }
    }
    pub fn is_alive(&self) -> bool {
        matches!(self.state, NodeState::Alive)
    }

    pub fn mark_as_dead(&mut self) {
        self.state = NodeState::Dead;
    }
}
impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node {}, {}:{} ({})",
            self.name.clone().unwrap_or("unknown".to_string()),
            self.addr,
            self.port,
            self.state
        )
    }
}

#[derive(Deserialize, Serialize, Reconcile, Hydrate, Clone, Debug)]
pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

impl Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Alive => write!(f, "Alive"),
            NodeState::Suspect => write!(f, "Suspect"),
            NodeState::Dead => write!(f, "Dead"),
        }
    }
}

// reconcile and hydrate for std::net::IpAddr
mod with_ip_addr {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    pub(super) fn reconcile<R: Reconciler>(
        addr: &std::net::IpAddr,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.str(addr.to_string())
    }

    pub(super) fn hydrate<D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'_>,
    ) -> Result<std::net::IpAddr, HydrateError> {
        let inner = String::hydrate(doc, obj, prop)?;
        inner.parse().map_err(|e| {
            HydrateError::unexpected(
                "a valid ip address",
                format!("a ip address which failed to parse due to {e}"),
            )
        })
    }
}

#[derive(Reconcile, Hydrate, Clone)]
pub(crate) struct NodeMap {
    nodes: HashMap<String, Node>,
}

impl NodeMap {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    fn upsert(&mut self, node: Node) {
        let addr = SocketAddr::new(node.addr, node.port);
        self.nodes.insert(addr.to_string(), node);
    }

    fn remove(&mut self, node: Node) {
        let addr = SocketAddr::new(node.addr, node.port);
        self.nodes.remove(&addr.to_string());
    }

    fn get_all(&mut self) -> Vec<Node> {
        self.nodes.iter_mut().map(|(_, v)| v.clone()).collect()
    }
}

// NodeList is a CRDT implementation of a list of nodes
pub(crate) struct NodeList {
    doc: automerge::AutoCommit,
    node_map: NodeMap,
    self_node: Node,
}

impl NodeList {
    pub(crate) fn new(self_node: Node) -> Self {
        Self {
            doc: automerge::AutoCommit::new(),
            node_map: NodeMap::new(),
            self_node,
        }
    }

    fn set_self_alive(&mut self) {
        let self_addr = SocketAddr::new(self.self_node.addr, self.self_node.port);
        match self
            .node_map
            .nodes
            .get_mut(&self_addr.to_string()) {
                Some(node) => node.state = NodeState::Alive,
                None => { // may removed or get lost due to reconciliation
                    self.node_map.upsert(self.self_node.clone());
                }
            }
    }

    pub(crate) fn merge(&mut self, other: Vec<u8>) -> Result<(), CRDTError> {
        let mut doc2 = automerge::AutoCommit::load(&other)?;
        self.doc.merge(&mut doc2)?;
        self.node_map = hydrate(&self.doc)?;
        self.set_self_alive();
        Ok(())
    }

    pub(crate) fn get_doc(&mut self) -> Result<Vec<u8>, CRDTError> {
        reconcile(&mut self.doc, self.node_map.clone())?;
        Ok(self.doc.save())
    }

    pub(crate) fn upsert(&mut self, node: Node) {
        self.node_map.upsert(node);
    }

    pub(crate) fn remove(&mut self, node: Node) {
        self.node_map.remove(node);
    }

    pub(crate) fn get_all(&mut self) -> Vec<Node> {
        self.node_map.get_all()
    }
}

#[derive(Error, Debug)]
pub(crate) enum CRDTError {
    #[error("failed to load doc")]
    DocumentFailToLoad(#[from] automerge::AutomergeError),
    #[error("failed to reconcile")]
    ReconcileError(#[from] autosurgeon::ReconcileError),
    #[error("failed to hydrate")]
    HydrateError(#[from] autosurgeon::HydrateError),
}