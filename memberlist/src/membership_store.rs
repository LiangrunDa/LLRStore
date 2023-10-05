use crate::config::Config;
use crate::node_list::{Node, NodeList};
use rand::seq::SliceRandom;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(crate) struct Request {
    pub answer: oneshot::Sender<Response>,
    pub cmd: Command,
}

#[derive(Debug)]
pub(crate) enum Response {
    Nodes(Vec<Node>),
    Node(Node),
    Doc(Vec<u8>),
    Success,
    Failure(String),
}

#[derive(Debug)]
pub(crate) enum Command {
    GetAllNodes,
    Upsert(Node),
    #[allow(dead_code)]
    Remove(Node),
    GetDoc,
    MergeDoc(Vec<u8>),
    GetAnAliveNode,
}

pub(crate) struct MembershipStore {
    nodes: NodeList,
    rx: mpsc::Receiver<Request>,
    config: Arc<Config>,
}

impl MembershipStore {
    pub(crate) fn new(rx: mpsc::Receiver<Request>, config: Arc<Config>) -> Self {
        Self {
            nodes: NodeList::new(Node::new(config.name(), config.addr(),config.port(), config.custom_info())),
            rx,
            config,
        }
    }

    fn initialize(&mut self) {
        let node = Node::new(self.config.name(), self.config.addr(), self.config.port(), self.config.custom_info());
        self.nodes.upsert(node);
    }

    pub(crate) async fn run(&mut self) {
        self.initialize();
        loop {
            let Request { answer, cmd } = match self.rx.recv().await {
                Some(req) => req,
                None => break,
            };
            match cmd {
                Command::GetAllNodes => {
                    let nodes = self.nodes.get_all();
                    answer
                        .send(Response::Nodes(nodes.clone()))
                        .expect("failed to send response")
                }
                Command::Upsert(node) => {
                    self.nodes.upsert(node);
                    answer
                        .send(Response::Success)
                        .expect("failed to send response");
                }
                Command::Remove(node) => {
                    self.nodes.remove(node);
                    answer
                        .send(Response::Success)
                        .expect("failed to send response");
                }
                Command::GetDoc => {
                    let doc = self
                        .nodes
                        .get_doc()
                        .expect("doc is corrupted due to unknown reason");
                    answer
                        .send(Response::Doc(doc))
                        .expect("failed to send response")
                }
                Command::MergeDoc(doc) => match self.nodes.merge(doc) {
                    Ok(_) => {
                        let doc = self
                            .nodes
                            .get_doc()
                            .expect("doc is corrupted due to unknown reason");
                        answer
                            .send(Response::Doc(doc))
                            .expect("failed to send response")
                    }
                    Err(e) => answer
                        .send(Response::Failure(e.to_string()))
                        .expect("failed to send response"),
                },
                Command::GetAnAliveNode => {
                    let nodes = self.nodes.get_all();
                    // randomly select an alive node, except itself
                    let self_name = self.config.name();
                    let alive_nodes: Vec<Node> = nodes
                        .into_iter()
                        .filter(|node| node.is_alive() && node.name != self_name)
                        .collect();
                    match alive_nodes.choose(&mut rand::thread_rng()).cloned() {
                        Some(node) => answer
                            .send(Response::Node(node))
                            .expect("failed to send response"),
                        None => answer
                            .send(Response::Failure("No alive node".to_string()))
                            .expect("failed to send response"),
                    }
                }
            }
        }
    }
}
