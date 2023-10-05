use crate::{
    replication::ReplicationManager,
    server_state::SharedServerState,
    store::{Command as StoreCommand, Request as StoreRequest, Response as StoreResponse},
};
use anyhow::Result as AnyhowResult;
use protocol::kv_kv::{
    kvkv_server::Kvkv, ClientRequest, DeleteKeyResponse, ForwardRequestResponse, Key, KeyDvvsPair,
    KeyDvvsPairs, KeyRange, PushKeyRangeResponse, SetWriteLockRequest, SetWriteLockResponse,
};
use shared::{
    key::{RingPos, RingRange},
    value::DvvSet,
};
use std::str::FromStr;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Debug)]
pub struct KvKvServer {
    server_id: RingPos,
    state: SharedServerState,
    store_tx: mpsc::Sender<StoreRequest>,
    replication_manager: ReplicationManager,
    kv_kv_port: u16,
}

impl KvKvServer {
    pub fn new(
        server_id: RingPos,
        state: SharedServerState,
        store_tx: mpsc::Sender<StoreRequest>,
        replication_manager: ReplicationManager,
        kv_kv_port: u16,
    ) -> Self {
        Self {
            server_id,
            state,
            store_tx,
            replication_manager,
            kv_kv_port,
        }
    }
}

#[tonic::async_trait]
impl Kvkv for KvKvServer {
    // idea: call store_request and make it generic
    async fn forward_request(
        &self,
        request: Request<ClientRequest>,
    ) -> Result<Response<ForwardRequestResponse>, Status> {
        let store_cmd = bincode::deserialize::<StoreCommand>(&request.into_inner().request)
            .map_err(err_to_invalid_arg)?;

        match store_cmd {
            StoreCommand::Put(..) => {}
            StoreCommand::Delete(..) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "Only client side put and delete operations are supported to be forwarded",
                ))
            }
        }
        let (tx, rx) = oneshot::channel::<StoreResponse>();
        let store_req = StoreRequest {
            answer: tx,
            cmd: store_cmd,
        };
        self.store_tx
            .send(store_req)
            .await
            .map_err(err_to_internal)?;
        let store_resp = rx
            .await
            .map_err(err_to_internal)?
            .map_err(err_to_internal)?;
        let (ring_pos, dvvs) = store_resp
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("No response from store"))?;

        let responsible_servers = self.state.read().await.replicas_for_key(ring_pos);
        let replicas = responsible_servers
            .into_iter()
            .filter(|info| !info.range.contains(&self.server_id))
            .map(|info| info.kv_addr(self.kv_kv_port));
        self.replication_manager
            .replicate(replicas, ring_pos, &dvvs)
            .await
            .map_err(err_to_internal)?;

        Ok(Response::new(ForwardRequestResponse {
            response: bincode::serialize(&dvvs)
                .map_err(|_e| Status::internal("serialization error"))?,
        }))
    }
    async fn set_write_lock(
        &self,
        request: Request<SetWriteLockRequest>,
    ) -> Result<Response<SetWriteLockResponse>, Status> {
        let locked = request.into_inner().locked;
        if locked {
            self.state.write().await.mode_mut().acquire_write_lock();
        } else {
            self.state.write().await.mode_mut().release_write_lock();
        }

        let response = SetWriteLockResponse { locked };
        Ok(Response::new(response))
    }
    async fn pull_key_range(
        &self,
        request: Request<KeyRange>,
    ) -> Result<Response<KeyDvvsPairs>, Status> {
        let key_range = request.into_inner();
        let ring_range = RingPos::try_from(key_range.from)
            .and_then(|from| RingPos::try_from(key_range.to).map(|to| RingRange::new(from, to)))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        info!("Processing pull key range request for {}", ring_range);

        let (reply_to_tx, reply_to_rx) = oneshot::channel();
        self.store_tx
            .send(StoreRequest {
                answer: reply_to_tx,
                cmd: StoreCommand::GetWithinRange(ring_range),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let key_dvvs_pairs = reply_to_rx
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(convert_ring_pos_dvvs_to_key_dvvs_pair)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::internal(e.to_string()))?;

        info!("Sending pull key range response: {:?}", key_dvvs_pairs);
        let response = KeyDvvsPairs { key_dvvs_pairs };

        Ok(Response::new(response))
    }
    async fn push_key_range(
        &self,
        request: Request<KeyDvvsPairs>,
    ) -> Result<Response<PushKeyRangeResponse>, Status> {
        info!("Processing push key range request");

        let ringpos_value_pairs = request
            .into_inner()
            .key_dvvs_pairs
            .into_iter()
            .map(convert_key_dvvs_pair_to_ring_pos_dvvs)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let (reply_to_tx, reply_to_rx) = oneshot::channel();
        self.store_tx
            .send(StoreRequest {
                answer: reply_to_tx,
                cmd: StoreCommand::MergeBulk(ringpos_value_pairs),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        reply_to_rx
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PushKeyRangeResponse {}))
    }
    async fn delete_key(
        &self,
        request: Request<Key>,
    ) -> Result<Response<DeleteKeyResponse>, Status> {
        let key = <RingPos as FromStr>::from_str(&request.into_inner().key)
            .map_err(|e| Status::invalid_argument(format!("Invalid key: {}", e.to_string())))?;
        info!("Processing delete key request for '{}'", key);

        todo!("Get rid of delete key");
        // let (reply_to_tx, reply_to_rx) = oneshot::channel();
        // self.store_tx
        //     .send(StoreRequest {
        //         answer: reply_to_tx,
        //         cmd: StoreCommand::Delete(key),
        //     })
        //     .await
        //     .map_err(|e| Status::internal(e.to_string()))?;
        // reply_to_rx
        //     .await
        //     .map_err(|e| Status::internal(e.to_string()))?
        //     .map_err(|e| Status::internal(e.to_string()))?;

        // Ok(Response::new(DeleteKeyResponse {}))
    }
}

fn err_to_invalid_arg<E: ToString>(e: E) -> Status {
    Status::invalid_argument(e.to_string())
}

fn err_to_internal<E: ToString>(e: E) -> Status {
    Status::internal(e.to_string())
}

pub fn convert_key_dvvs_pair_to_ring_pos_dvvs(
    pair: KeyDvvsPair,
) -> AnyhowResult<(RingPos, DvvSet)> {
    Ok((
        pair.key.parse::<RingPos>()?,
        bincode::deserialize(&pair.dvvs)?,
    ))
}

pub fn convert_ring_pos_dvvs_to_key_dvvs_pair(
    (ring_pos, dvvs): (RingPos, DvvSet),
) -> AnyhowResult<KeyDvvsPair> {
    Ok(KeyDvvsPair {
        key: ring_pos.to_string(),
        dvvs: bincode::serialize(&dvvs)?,
    })
}
