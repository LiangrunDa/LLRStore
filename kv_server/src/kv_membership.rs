use crate::kv_internal::convert_key_dvvs_pair_to_ring_pos_dvvs;
use crate::server_state::SharedServerState;
use crate::store::{Command as StoreCommand, Request as StoreRequest};
use anyhow::Result;
use futures::future::join_all;
use protocol::{
    kv_kv::{kvkv_client::KvkvClient, KeyRange},
    kv_membership::{
        kv_membership_server::KvMembership, UpdateTopologyRequest, UpdateTopologyResponse,
    },
};
use shared::key::MetaData;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};
use tracing::{debug, error};

#[derive(Debug)]
pub struct KvMembershipService {
    #[allow(dead_code)]
    socket_addr: SocketAddr,
    kv_kv_port: u16,
    state: SharedServerState,
    store_tx: mpsc::Sender<StoreRequest>,
}

impl KvMembershipService {
    pub fn new(
        state: SharedServerState,
        socket_addr: SocketAddr,
        store_tx: mpsc::Sender<StoreRequest>,
        kv_kv_port: u16,
    ) -> Self {
        Self {
            state,
            socket_addr,
            store_tx,
            kv_kv_port,
        }
    }
}

#[tonic::async_trait]
impl KvMembership for KvMembershipService {
    async fn update_topology(
        &self,
        request: Request<UpdateTopologyRequest>,
    ) -> Result<Response<UpdateTopologyResponse>, Status> {
        debug!("Processing update topology request");
        let new = MetaData::try_from(request.into_inner().updated_network_topology)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let (_old, new_replicas) = {
            let mut state = self.state.write().await;
            let old = state.update_metadata(new);
            let new_replicas = state.replicas();
            (old, new_replicas)
        };

        let tasks = new_replicas
            .into_iter()
            .map(|info| {
                let store_tx = self.store_tx.clone();
                let kv_kv_port = self.kv_kv_port;
                return async move {
                    let mut client =
                        KvkvClient::connect(format!("http://{}", info.kv_addr(kv_kv_port))).await?;
                    let ringpos_value_pairs = client
                        .pull_key_range(KeyRange::from(&info.range))
                        .await?
                        .into_inner()
                        .key_dvvs_pairs
                        .into_iter()
                        .map(convert_key_dvvs_pair_to_ring_pos_dvvs)
                        .collect::<Result<Vec<_>, _>>()?;

                    let (reply_to_tx, reply_to_rx) = oneshot::channel();
                    store_tx
                        .send(StoreRequest {
                            answer: reply_to_tx,
                            cmd: StoreCommand::MergeBulk(ringpos_value_pairs),
                        })
                        .await?;
                    reply_to_rx.await??;

                    Ok(()) as Result<()>
                };
            })
            .collect::<Vec<_>>();

        // the replication happens asynchronously here and we do not await
        // the results
        let _ = tokio::task::spawn(async move {
            let res = join_all(tasks)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>();
            if let Err(e) = res {
                error!("Error while pulling key ranges from peers: {}", e);
            }
        });

        return Ok(Response::new(UpdateTopologyResponse {}));
    }
}
