use crate::internal_service::InternalRequestError::{NoAliveNode, UnknownError};
use crate::node_list::Node;
use crate::{membership_store, message_queue};
use rand::Rng;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::warn;

pub(crate) async fn get_alive_node(
    membership_tx: &Sender<membership_store::Request>,
) -> Result<Node, InternalRequestError> {
    let (nodes_tx, nodes_rx) = oneshot::channel();
    membership_tx
        .send(membership_store::Request {
            answer: nodes_tx,
            cmd: membership_store::Command::GetAnAliveNode,
        })
        .await
        .expect("Failed to send GetAllNodes to membership_store");
    match nodes_rx
        .await
        .expect("Failed to receive answer from membership_store")
    {
        membership_store::Response::Node(node) => Ok(node),
        membership_store::Response::Failure(_) => Err(NoAliveNode),
        _ => Err(UnknownError),
    }
}

pub(crate) async fn upsert_node(
    membership_store: &Sender<membership_store::Request>,
    node: Node,
) -> Result<(), InternalRequestError> {
    let (answer_tx, answer_rx) = oneshot::channel();
    membership_store
        .send(membership_store::Request {
            answer: answer_tx,
            cmd: membership_store::Command::Upsert(node),
        })
        .await
        .expect("Failed to send MarkNodeAsDead to membership_store");
    match answer_rx
        .await
        .expect("Failed to receive answer from membership_store")
    {
        membership_store::Response::Success => Ok(()),
        _ => Err(UnknownError),
    }
}

pub(crate) async fn send_pull_to_node(
    membership_tx: &Sender<membership_store::Request>,
    message_tx: &Sender<message_queue::Request>,
    node: Node,
) -> Result<(), InternalRequestError> {
    let (answer_tx, answer_rx) = oneshot::channel();
    membership_tx
        .send(membership_store::Request {
            answer: answer_tx,
            cmd: membership_store::Command::GetDoc,
        })
        .await
        .expect("Failed to send GetDoc to membership_store");
    let answer = answer_rx
        .await
        .expect("Failed to receive answer from membership_store");
    match answer {
        membership_store::Response::Doc(doc) => {
            // generate random seq number
            let seq = rand::thread_rng().gen_range(1..=u32::MAX);
            message_tx
                .send(message_queue::Request::Pull(seq, node, doc))
                .await
                .expect("Failed to send Pull to message_queue");
            Ok(())
        }
        _ => {
            warn!("Received unexpected answer from membership_store");
            Err(UnknownError)
        }
    }
}

pub(crate) async fn get_doc(
    membership_tx: &Sender<membership_store::Request>,
) -> Result<Vec<u8>, InternalRequestError> {
    let (answer_tx, answer_rx) = oneshot::channel();
    membership_tx
        .send(membership_store::Request {
            answer: answer_tx,
            cmd: membership_store::Command::GetDoc,
        })
        .await
        .expect("Failed to send GetDoc to membership_store");
    let answer = answer_rx
        .await
        .expect("Failed to receive answer from membership_store");
    match answer {
        membership_store::Response::Doc(doc) => Ok(doc),
        _ => {
            warn!("Received unexpected answer from membership_store");
            Err(UnknownError)
        }
    }
}

pub(crate) async fn merge_doc_and_push(
    membership_tx: &Sender<membership_store::Request>,
    doc: Vec<u8>,
) -> Result<Vec<u8>, InternalRequestError> {
    let (answer_tx, answer_rx) = oneshot::channel();
    membership_tx
        .send(membership_store::Request {
            answer: answer_tx,
            cmd: membership_store::Command::MergeDoc(doc),
        })
        .await
        .expect("Failed to send MergeDoc to membership_store");
    let answer = answer_rx
        .await
        .expect("Failed to receive answer from membership_store");
    match answer {
        membership_store::Response::Doc(doc) => {
            Ok(doc)
        }
        membership_store::Response::Failure(e) => {
            warn!("Failed to merge doc: {}", e);
            Err(InternalRequestError::MergeError)
        }
        _ => {
            warn!("Received unexpected answer from membership_store");
            Err(UnknownError)
        }
    }
}

pub(crate) async fn merge_doc(
    membership_tx: &Sender<membership_store::Request>,
    doc: Vec<u8>,
) -> Result<(), InternalRequestError> {
    let result = merge_doc_and_push(membership_tx, doc).await;
    result.and(Ok(()))
}

#[derive(Error, Debug)]
pub(crate) enum InternalRequestError {
    #[error("Merge error")]
    MergeError,
    #[error("No alive node")]
    NoAliveNode,
    #[error("Unknown error")]
    UnknownError,
}
