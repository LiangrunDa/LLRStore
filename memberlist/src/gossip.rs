use crate::config::Config;
use crate::internal_service::{get_alive_node, send_pull_to_node, InternalRequestError};
use crate::{membership_store, message_queue};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::warn;

pub(crate) struct Gossip {
    membership_tx: mpsc::Sender<membership_store::Request>,
    message_tx: mpsc::Sender<message_queue::Request>,
    #[allow(dead_code)]
    config: Arc<Config>,
}

impl Gossip {
    pub(crate) fn new(
        membership_tx: mpsc::Sender<membership_store::Request>,
        message_tx: mpsc::Sender<message_queue::Request>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            membership_tx,
            message_tx,
            config,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), GossipError> {
        loop {
            // wait for 1sec
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            // get an alive node
            match get_alive_node(&self.membership_tx).await {
                Ok(node) => {
                    send_pull_to_node(&self.membership_tx, &self.message_tx, node).await?;
                }
                Err(err) => {
                    warn!("Failed to choose a node to gossip: {:?}", err);
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum GossipError {
    #[error(transparent)]
    InternalRequestError(#[from] InternalRequestError),
}
