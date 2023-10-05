//! A connection to a client.
use crate::replication::ReplicationManager;
use crate::server_state::{ServerMode, SharedServerState};
use crate::store::Command as StoreCommand;
use crate::store::Request as StoreRequest;
use anyhow::Result;
use anyhow::{anyhow, bail};
use protocol::kv_kv::kvkv_client::KvkvClient;
use protocol::kv_kv::ClientRequest;
use rand::seq::IteratorRandom;
use shared::key::{Key, RingPos};
use shared::protocol::{DeleteErrorResponse, ServerWriteLockResponse};
use shared::protocol::{DeleteRequest, KeyRangeSuccessResponse};
use shared::protocol::{DeleteSuccessResponse, InnerSuccessResponse, PutSuccessResponse};
use shared::protocol::{DelimCodec, Request, Response};
use shared::protocol::{GetErrorResponse, ProtocolError};
use shared::protocol::{GetRequest, PutErrorResponse};
use shared::protocol::{GetSuccessResponse, PutRequest};
use shared::protocol::{InnerErrResponse, ServerStoppedResponse};
use shared::value::DvvSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, instrument, warn};

#[allow(dead_code)]
pub struct Connection {
    state: SharedServerState,
    server_id: RingPos,
    local: SocketAddr,
    peer: SocketAddr,
    socket: DelimCodec,
    shutdown_rx: broadcast::Receiver<()>,
    store_tx: mpsc::Sender<StoreRequest>,
    replication_manager: ReplicationManager,
    kv_kv_port: u16,
}

impl Connection {
    pub fn new(
        server_id: RingPos,
        state: SharedServerState,
        local: SocketAddr,
        peer: SocketAddr,
        socket: TcpStream,
        shutdown_rx: broadcast::Receiver<()>,
        store_tx: mpsc::Sender<StoreRequest>,
        replication_manager: ReplicationManager,
        kv_kv_port: u16,
    ) -> Self {
        Self {
            state,
            server_id,
            local,
            peer,
            socket: DelimCodec::new(socket, Some(Duration::from_secs(64))),
            shutdown_rx,
            store_tx,
            replication_manager,
            kv_kv_port,
        }
    }
    async fn read_quorum(
        &self,
        ring_pos: RingPos,
        replicas: impl Iterator<Item = SocketAddr>,
    ) -> Result<DvvSet> {
        let dvvss = self.replication_manager.read(replicas, ring_pos).await?;
        match dvvss.into_iter().reduce(|mut a, b| {
            a.sync(b);
            a
        }) {
            Some(dvvs) => Ok(dvvs),
            None => bail!("No entry found for key {}", ring_pos),
        }
        // TODO: read repair?
    }
    async fn forward_and_init_write_quorum(
        key: &Key,
        replicas: impl Iterator<Item = SocketAddr>,
        store_cmd: &StoreCommand,
    ) -> Result<DvvSet> {
        // forward to any one replica and await result
        let selected_replica = replicas.choose(&mut rand::thread_rng());
        let Some(selected_replica) = selected_replica else {
            bail!("No replicas available for key {}", key);
        };
        let mut client = KvkvClient::connect(format!("http://{}", selected_replica))
            .await
            .map_err(|e| anyhow!("Could not connect to replica: {}", e))?;
        let response = client
            .forward_request(ClientRequest {
                request: bincode::serialize(store_cmd)?,
            })
            .await?;
        let dvvs = bincode::deserialize::<DvvSet>(&response.into_inner().response)?;
        Ok(dvvs)
    }
    #[instrument(
        name = "Connection::handle",
        skip(self),
        fields(
            // `%` serializes the peer IP addr with `Display`
            peer_addr = %self.peer
        ),
    )]
    pub async fn handle(&mut self) -> Result<()> {
        info!("New connection");
        // no stupid welcome message anymore

        loop {
            let request = tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("Shutting down connection");
                    break;
                }
                read = self.socket.read_msg::<Request>() => {
                    match read {
                        Ok(request) => request,
                        Err(e) => match e {
                            ProtocolError::Parsing { .. } | ProtocolError::Violation(_) => {
                                let response = Response::Error(e.to_string());
                                self.socket.send_msg(response).await?;
                                continue;
                            }
                            ProtocolError::Closed => break,
                            _ => return Err(e.into()),
                        },
                    }
                }
            };
            info!(%request, "Processing new request");
            let response = 'response: {
                let state = self.state.read().await;
                match (state.mode(), &request) {
                    (ServerMode::Serving(..), _) => {}
                    (ServerMode::WriteLocked(..), Request::Get(_)) => {}
                    (ServerMode::WriteLocked(..), _) => {
                        break 'response Response::ServerWriteLock(ServerWriteLockResponse {});
                    }
                    (ServerMode::Stopped, _) => {
                        break 'response Response::ServerStopped(ServerStoppedResponse {});
                    }
                }
                // a client is allowed to still use the KeyRange command
                // to directly talk to a responsible server for a given key
                if let Request::KeyRange = request {
                    break 'response Response::KeyRangeSuccess(KeyRangeSuccessResponse {
                        server_ranges: state.all_replicas(),
                    });
                }

                let (key, ring_pos, store_cmd): (Key, RingPos, StoreCommand) = match request {
                    Request::Get(GetRequest { key }) => {
                        let ring_pos = RingPos::from(&key);
                        (key, ring_pos, StoreCommand::Get(ring_pos))
                    }
                    Request::Put(PutRequest {
                        key,
                        context,
                        value,
                    }) => {
                        let ring_pos = RingPos::from(&key);
                        (key, ring_pos, StoreCommand::Put(ring_pos, context, value))
                    }
                    Request::Delete(DeleteRequest { key, context }) => {
                        let ring_pos = RingPos::from(&key);
                        (key, ring_pos, StoreCommand::Delete(ring_pos, context))
                    }
                    Request::KeyRange => unreachable!("handled above"),
                };

                let replicas = state
                    .replicas_for_key(ring_pos)
                    .into_iter()
                    .map(|info| info.kv_addr(self.kv_kv_port));
                drop(state); // don't hold the lock across an await point!

                match store_cmd {
                    StoreCommand::Put(..) => {
                        match Self::forward_and_init_write_quorum(&key, replicas, &store_cmd).await
                        {
                            Ok(dvvs) => {
                                break 'response Response::PutSuccess(PutSuccessResponse(
                                    InnerSuccessResponse::new(key, dvvs),
                                ))
                            }
                            Err(e) => {
                                warn!("Could not replicate put: {}", e);
                                break 'response Response::PutError(PutErrorResponse(
                                    InnerErrResponse::new(key),
                                ));
                            }
                        }
                    }
                    StoreCommand::Delete(..) => {
                        match Self::forward_and_init_write_quorum(&key, replicas, &store_cmd).await
                        {
                            Ok(dvvs) => {
                                break 'response Response::DeleteSuccess(DeleteSuccessResponse(
                                    InnerSuccessResponse::new(key, dvvs),
                                ))
                            }
                            Err(e) => {
                                warn!("Could not replicate delete: {}", e);
                                break 'response Response::DeleteError(DeleteErrorResponse(
                                    InnerErrResponse::new(key),
                                ));
                            }
                        }
                    }
                    StoreCommand::Get(ring_pos) => {
                        match self.read_quorum(ring_pos, replicas).await {
                            Ok(dvvs) => {
                                break 'response Response::GetSuccess(GetSuccessResponse(
                                    InnerSuccessResponse::new(key, dvvs),
                                ));
                            }
                            Err(e) => {
                                warn!("Could not perform read quorum: {}", e);
                                break 'response Response::GetError(GetErrorResponse(
                                    InnerErrResponse::new(key),
                                ));
                            }
                        }
                    }
                    _ => unreachable!(
                        "client cannot cause any other store command than get|put|delete"
                    ),
                }
            };
            info!(%response, "sending respose");
            self.socket.send_msg(response).await?;
        }
        info!("Connection closed");
        Ok(())
    }
}
