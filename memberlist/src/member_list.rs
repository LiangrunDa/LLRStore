use std::net::SocketAddr;
use crate::config::Config;
use crate::error::MemberListError;
use crate::failure_detector::FailureDetector;
use crate::membership_store::MembershipStore;
use crate::node_list::Node;
use crate::{membership_store, message_queue};
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub struct MemberList {
    started: bool,
    config: Config,
    membership_store: Option<mpsc::Sender<membership_store::Request>>,
    message_queue: Option<mpsc::Sender<message_queue::Request>>,
}

impl MemberList {
    pub fn new_with_config(config: Config) -> Self {
        Self {
            config,
            started: false,
            membership_store: None,
            message_queue: None,
        }
    }

    pub async fn async_join(&self, peer: SocketAddr) -> Result<(), MemberListError> {
        if !self.started {
            warn!("MemberList does not start");
            return Err(MemberListError::NotStartError);
        }
        let node = Node::new(None, peer.ip(), peer.port(), None);
        match self.message_queue {
            Some(ref message_queue) => {
                message_queue.send(message_queue::Request::Join(0, node)).await.map_err(|_| MemberListError::UnexpectedInternalError)
            }
            None => Err(MemberListError::UnexpectedInternalError),
        }
    }

    pub fn join(&self, peer: SocketAddr) -> Result<(), MemberListError> {
        if !self.started {
            warn!("MemberList does not start");
            return Err(MemberListError::NotStartError);
        }
        let node = Node::new(None, peer.ip(), peer.port(), None);
        self.message_queue
            .as_ref()
            .and_then(|message_queue| {
                match message_queue.blocking_send(message_queue::Request::Join(0, node)) {
                    Ok(_) => Some(()),
                    Err(_) => None,
                }
            })
            .ok_or(MemberListError::UnexpectedInternalError)
    }

    pub async fn async_get_nodes(&self) -> Result<Vec<Node>, MemberListError> {
        if !self.started {
            warn!("MemberList does not start");
            return Err(MemberListError::NotStartError);
        }
        match self.membership_store {
            Some(ref membership_store) => {
                let (nodes_tx, nodes_rx) = oneshot::channel();
                let _ = membership_store.send(membership_store::Request {
                    answer: nodes_tx,
                    cmd: membership_store::Command::GetAllNodes,
                }).await;
                match nodes_rx.await.expect("Failed to receive answer from membership_store") {
                    membership_store::Response::Nodes(nodes) => Ok(nodes),
                    _ => Err(MemberListError::UnexpectedInternalError),
                }
            },
            None => Err(MemberListError::UnexpectedInternalError),
        }
    }

    pub fn get_nodes(&self) -> Result<Vec<Node>, MemberListError> {
        if !self.started {
            warn!("MemberList does not start");
            return Err(MemberListError::NotStartError);
        }
        self.membership_store
            .as_ref()
            .and_then(|membership_store| {
                let (nodes_tx, nodes_rx) = oneshot::channel();
                let _ = membership_store.blocking_send(membership_store::Request {
                    answer: nodes_tx,
                    cmd: membership_store::Command::GetAllNodes,
                });
                match nodes_rx
                    .blocking_recv()
                    .expect("Failed to receive answer from membership_store")
                {
                    membership_store::Response::Nodes(nodes) => Some(nodes),
                    _ => None,
                }
            })
            .ok_or(MemberListError::UnexpectedInternalError)
    }

    pub fn start(&mut self) {
        info!("Starting MemberList");
        if self.started {
            warn!("MemberList already started");
            return;
        } else {
            self.started = true;
        }
        let (message_tx, message_rx) = mpsc::channel::<message_queue::Request>(100);
        let (membership_tx, membership_rx) = mpsc::channel::<membership_store::Request>(100);
        self.message_queue = Some(message_tx.clone());
        self.membership_store = Some(membership_tx.clone());
        let config: Arc<Config> = Arc::new(self.config.clone());
        // spawn a new thread to run the tokio runtime
        thread::spawn(move || Self::run(config, membership_rx, membership_tx, message_rx, message_tx));
    }

    pub(crate) fn run(
        config: Arc<Config>,
        membership_rx: mpsc::Receiver<membership_store::Request>,
        membership_tx: mpsc::Sender<membership_store::Request>,
        message_rx: mpsc::Receiver<message_queue::Request>,
        message_tx: mpsc::Sender<message_queue::Request>,
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (failure_tx, failure_rx) =
                    mpsc::channel::<crate::failure_detector::Request>(100);
                let mut membership_store = MembershipStore::new(membership_rx, config.clone());
                let _membership_store_handle = tokio::spawn(async move {
                    membership_store.run().await;
                });

                let mut failure_detector = FailureDetector::new(
                    membership_tx.clone(),
                    message_tx.clone(),
                    failure_tx.clone(),
                    failure_rx,
                );
                let _failure_detector_handle = tokio::spawn(async move {
                    failure_detector.run().await;
                });

                let mut message_queue = message_queue::MessageQueue::new(
                    message_rx,
                    membership_tx.clone(),
                    failure_tx.clone(),
                    config.clone(),
                );
                let _message_queue_handle = tokio::spawn(async move { message_queue.run().await });

                let mut gossip = crate::gossip::Gossip::new(
                    membership_tx.clone(),
                    message_tx.clone(),
                    config.clone(),
                );
                let _ = gossip.run().await;
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use tracing_test::traced_test;

    #[traced_test]
    #[test]
    fn test_member_list() {
        let mut member_list1 = MemberList::new_with_config(Config::new(
            Some("node1".to_string()),
            IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            8080,
            None,
        ));
        let mut member_list2 = MemberList::new_with_config(Config::new(
            Some("node2".to_string()),
            IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            8081,
            None,
        ));
        member_list1.start();
        member_list2.start();

        let peer1 = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 8080);
        member_list2.join(peer1).expect("Failed to join node1");

        thread::sleep(std::time::Duration::from_secs(10));
        let nodes = member_list1.get_nodes().unwrap();
        assert_eq!(nodes.len(), 2);
        for node in nodes {
            println!("{}", node);
        }
    }
}
