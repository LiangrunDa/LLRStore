use crate::failure_detector::DetectorError::AckNotExpected;
use crate::internal_service::{get_alive_node, upsert_node, InternalRequestError};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

#[derive(Debug)]
pub(crate) enum Request {
    Ack(u32),
    Ping,
}

pub(crate) struct FailureDetector {
    membership_tx: mpsc::Sender<crate::membership_store::Request>,
    message_tx: mpsc::Sender<crate::message_queue::Request>,
    failure_tx: mpsc::Sender<Request>,
    rx: mpsc::Receiver<Request>,
    ping_channel: Arc<Mutex<HashMap<u32, mpsc::Sender<Request>>>>,
}

impl FailureDetector {
    pub(crate) fn new(
        membership_tx: mpsc::Sender<crate::membership_store::Request>,
        message_tx: mpsc::Sender<crate::message_queue::Request>,
        failure_tx: mpsc::Sender<Request>,
        rx: mpsc::Receiver<Request>,
    ) -> Self {
        Self {
            membership_tx,
            message_tx,
            failure_tx,
            rx,
            ping_channel: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn run(&mut self) {
        // info!("FailureDetector started");
        let tick_tx = self.failure_tx.clone();
        let _tick_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                tick_tx
                    .send(Request::Ping)
                    .await
                    .expect("FailureDetector channel send failed");
            }
        });

        loop {
            tokio::select! {
                Some(request) = self.rx.recv() => {
                    match request {
                        Request::Ack(seq) => {
                            match self.handle_ack(seq).await {
                                Ok(_) => {}
                                Err(err) => {
                                    warn!("Failed to handle ack: {:?}", err);
                                }
                            }
                        }
                        Request::Ping => {
                            let membership_tx = self.membership_tx.clone();
                            let message_tx = self.message_tx.clone();
                            let ping_channel = self.ping_channel.clone();
                            tokio::spawn(async move {
                                match Self::handle_ping(message_tx, membership_tx, ping_channel).await {
                                    Ok(_) => {}
                                    Err(err) => {
                                        warn!("Failed to handle ping: {:?}", err);
                                    }
                                }
                            });
                        }
                    }
                }
                else => {
                    warn!("FailureDetector channel closed");
                    break
                }
            }
        }
    }

    async fn handle_ack(&mut self, seq: u32) -> Result<(), DetectorError> {
        info!("Received Ack {}", seq);
        // print all channels

        let insert_ping_channel = self.ping_channel.lock().await;
        // print all channels
        for (key, value) in insert_ping_channel.iter() {
            info!("before getting {}: {:?}", key, value);
        }
        let channel = insert_ping_channel.get(&seq).ok_or(AckNotExpected)?;
        channel
            .send(Request::Ack(seq))
            .await
            .expect("Failed to send Ack");
        Ok(())
    }

    // TODO: suspect node if no ack received
    async fn handle_ping(
        message_tx: mpsc::Sender<crate::message_queue::Request>,
        membership_tx: mpsc::Sender<crate::membership_store::Request>,
        ping_channel: Arc<Mutex<HashMap<u32, mpsc::Sender<Request>>>>,
    ) -> Result<(), DetectorError> {
        let seq = rand::random::<u32>();
        let mut chosen_node = get_alive_node(&membership_tx).await?;
        let (ack_tx, mut ack_rx) = mpsc::channel::<Request>(1);
        let mut insert_ping_channel = ping_channel.lock().await;
        insert_ping_channel.insert(seq, ack_tx);
        // print all channels
        for (key, value) in insert_ping_channel.iter() {
            info!("after inserting {}: {:?}", key, value);
        }
        drop(insert_ping_channel);
        info!("Sending Ping {}", seq);
        message_tx
            .send(crate::message_queue::Request::Ping(
                seq,
                chosen_node.clone(),
            ))
            .await
            .expect("MessageQueue channel sent failed");

        let timeout = time::sleep(Duration::from_secs(4));
        tokio::pin!(timeout);

        tokio::select! {
            _ = &mut timeout => {
                info!("No Ack received for Ping {}", seq);
                chosen_node.mark_as_dead();
                upsert_node(&membership_tx, chosen_node).await?;
            }
            result = ack_rx.recv() => {
                match result {
                    Some(Request::Ack(seq)) => {
                        info!("Receive Ack for ping {}", seq);
                        let mut insert_ping_channel = ping_channel.lock().await;
                        insert_ping_channel.remove(&seq);
                        drop(insert_ping_channel);
                    }
                    _ => {
                        warn!("Received unexpected message");
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Error, Debug)]
pub(crate) enum DetectorError {
    #[error(transparent)]
    InternalRequestError(#[from] InternalRequestError),
    #[error("Ack not expected")]
    AckNotExpected,
}
