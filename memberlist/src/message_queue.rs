use crate::config::Config;
use crate::failure_detector;
use crate::membership_store;
use crate::network::{Communicator, Message, ProtocolError};
use crate::node_list::Node;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc};
use tracing::{debug, warn};
use crate::internal_service::{get_doc, merge_doc, merge_doc_and_push};

// Ping or PingReq from peers are not forwarded to the failure detector
// PushPull from peers are forwarded to the membership_store via membership_store_tx by GetDoc Request

pub(crate) enum Request {
    Ping(u32, Node),
    #[allow(dead_code)]
    PingReq(u32, Vec<Node>, Node),
    Pull(u32, Node, Vec<u8>),
    Join(u32, Node),
}

impl Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Ping(seq, node) => write!(f, "Ping({seq:?}, {node:?})"),
            Request::PingReq(seq, nodes, node) => {
                write!(f, "PingReq({seq:?}, {nodes:?}, {node:?})")
            }
            Request::Pull(seq, node, _) => write!(f, "Pull({seq:?}, {node:?})"),
            Request::Join(seq, node) => write!(f, "Join({seq:?}, {node:?})"),
        }
    }
}

pub(crate) struct MessageQueue {
    membership_store_tx: mpsc::Sender<membership_store::Request>,
    failure_detector_tx: mpsc::Sender<failure_detector::Request>,
    rx: mpsc::Receiver<Request>,
    config: Arc<Config>,
}

impl MessageQueue {
    pub(crate) fn new(
        rx: mpsc::Receiver<Request>,
        membership_store_tx: mpsc::Sender<membership_store::Request>,
        failure_detector_tx: mpsc::Sender<failure_detector::Request>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            membership_store_tx,
            failure_detector_tx,
            rx,
            config,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ProtocolError> {
        let addr = SocketAddr::new(self.config.addr(), self.config.port());
        let listener = UdpSocket::bind(addr).await?;
        let mut communicator = Communicator::new(listener);

        loop {
            tokio::select! {
                Some(request) = self.rx.recv() => {
                    let debug_info = format!("Request {request:?}");
                    match self.handle_request(&mut communicator, request).await {
                        Ok(_) => {
                            debug!("Request {:?} handled successfully", debug_info);
                        }
                        Err(err) => {
                            warn!("Error handling request {:?}: {:?}", debug_info, err);
                        }
                    }
                }
                Ok((message, addr)) = communicator.receive_message() => {
                    let debug_info = format!("Message {message:?} from {addr:?}");
                    match self.handle_message(&mut communicator, message, addr).await {
                        Ok(_) => {
                            debug!("Message {:?} handled successfully", debug_info);
                        }
                        Err(err) => {
                            warn!("Error handling message {:?}: {:?}", debug_info, err);
                        }
                    }
                }
                else => {
                    warn!("MessageQueue channel closed");
                    break Ok(())
                }
            }
        }
    }

    async fn handle_request(
        &mut self,
        communicator: &mut Communicator,
        request: Request,
    ) -> Result<(), ProtocolError> {
        match request {
            Request::Ping(seq, node) => {
                // we don't need to send Ping to the failure detector, just send Ack back
                communicator
                    .send_ping(node.clone(), SocketAddr::new(node.addr, node.port), seq)
                    .await
            }
            Request::PingReq(_seq, _nodes, _node) => {
                // TODO: send PingReq to all nodes in `nodes`
                Ok(())
            }
            Request::Pull(seq, node, doc) => {
                communicator
                    .send_pull(doc, SocketAddr::new(node.addr, node.port), seq)
                    .await
            }
            Request::Join(seq, node) => {
                communicator
                    .send_join(SocketAddr::new(node.addr, node.port), seq)
                    .await
            }
        }
    }

    async fn handle_message(
        &self,
        communicator: &mut Communicator,
        message: Message,
        addr: SocketAddr,
    ) -> Result<(), ProtocolError> {
        match message {
            Message::Ping(seq, node) => {
                // we don't need to send Ping to the failure detector, just send Ack back
                communicator.send_ack(node.clone(), addr, seq).await
            }
            Message::Pull(seq, doc) => {
                let doc = merge_doc_and_push(&self.membership_store_tx, doc)
                    .await?;
                communicator.send_push(doc, addr, seq).await
            }
            Message::Push(_seq, doc) => {
                merge_doc(&self.membership_store_tx, doc)
                    .await.map_err(|e| e.into())
            }
            Message::Ack(seq, _node) => {
                self.failure_detector_tx
                    .send(failure_detector::Request::Ack(seq))
                    .await
                    .expect("Failed to send Ack to failure_detector");
                Ok(())
            }
            Message::JoinRequest(seq) => {
                let doc = get_doc(&self.membership_store_tx)
                    .await
                    .expect("Failed to get doc from membership_store");
                communicator.send_join_reply(doc, addr, seq).await
            }
            Message::JoinReply(_seq, doc) => {
                merge_doc(&self.membership_store_tx, doc)
                    .await.map_err(|e| e.into())
            }
        }
    }
}
