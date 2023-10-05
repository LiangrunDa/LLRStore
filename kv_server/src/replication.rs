use crate::kv_internal::convert_key_dvvs_pair_to_ring_pos_dvvs;
use crate::kv_internal::convert_ring_pos_dvvs_to_key_dvvs_pair;
use anyhow::anyhow;
use anyhow::Result;
use futures::future::join_all;
use futures::future::select_ok;
use protocol::kv_kv::kvkv_client::KvkvClient;
use protocol::kv_kv::KeyDvvsPairs;
use protocol::kv_kv::KeyRange;
use shared::key::RingPos;
use shared::key::RingRange;
use shared::value::DvvSet;
use std::future::Future;
use std::{net::SocketAddr, num::NonZeroUsize};
use tracing::info;
use tracing::warn;

#[derive(Debug, Copy, Clone)]
pub struct ReplicationManager {
    replication_factor: NonZeroUsize,
    durability_param: NonZeroUsize,
    consistency_param: NonZeroUsize,
}

impl ReplicationManager {
    pub fn new(
        replication_factor: NonZeroUsize,
        durability_param: NonZeroUsize,
        consistency_param: NonZeroUsize,
    ) -> Self {
        Self {
            durability_param,
            replication_factor,
            consistency_param,
        }
    }
    async fn run<T: Send>(
        &self,
        tasks: impl Iterator<Item = impl Future<Output = Result<T>> + Unpin + Send + 'static>,
        desired_sync_acks: NonZeroUsize,
    ) -> Result<Vec<T>> {
        let mut tasks = Vec::from_iter(tasks);
        let mut results: Vec<T> = Vec::with_capacity(usize::from(self.replication_factor));
        let required_sync_acks = usize::min(usize::from(desired_sync_acks), tasks.len());
        // here we do sync replication first
        loop {
            if results.len() >= required_sync_acks {
                break;
            }
            if tasks.len() == 0 {
                break;
            }
            match select_ok(tasks).await {
                Ok((res, remaining)) => {
                    results.push(res);
                    tasks = remaining;
                }
                Err(_e) => {
                    return Err(anyhow!("Sync replication error"));
                }
            }
        }
        // we might have to do async replication after the sync replication
        if tasks.len() > 0 {
            let _async_results = tokio::task::spawn(async move {
                let res = join_all(tasks).await;
                if let Err(e) = res.into_iter().collect::<Result<Vec<_>>>() {
                    warn!("Tolerating async replication error: {}", e);
                } else {
                    info!("Async replication succeeded");
                }
            });
        }
        Ok(results)
    }
    pub async fn replicate(
        &self,
        to: impl Iterator<Item = SocketAddr>,
        key: impl Into<RingPos>,
        dvvs: &DvvSet,
    ) -> Result<()> {
        let key: RingPos = key.into();
        let task = |addr: SocketAddr, key: RingPos, dvvs: DvvSet| async move {
            let _ = KvkvClient::connect(format!("http://{}", addr))
                .await?
                .push_key_range(KeyDvvsPairs {
                    key_dvvs_pairs: vec![convert_ring_pos_dvvs_to_key_dvvs_pair((key, dvvs))?],
                })
                .await?;
            Ok(()) as Result<()>
        };

        let tasks = to.map(|addr| Box::pin(task(addr, key, dvvs.clone())));

        self.run(tasks, self.durability_param).await.map(|_| ())
    }
    pub async fn read(
        &self,
        from: impl Iterator<Item = SocketAddr>,
        key: impl Into<RingPos>,
    ) -> Result<Vec<DvvSet>> {
        let key: RingPos = key.into();
        let task = |addr: SocketAddr, key: RingPos| async move {
            let mut client = KvkvClient::connect(format!("http://{}", addr)).await?;
            let response = client
                .pull_key_range(KeyRange::from(&RingRange::new(key, key)))
                .await?
                .into_inner()
                .key_dvvs_pairs
                .into_iter()
                .next()
                .map_or(Ok(None), |pair| {
                    Ok(Some(
                        convert_key_dvvs_pair_to_ring_pos_dvvs(pair).map(|(_, dvvs)| dvvs)?,
                    ))
                });
            response
        };

        let tasks = from.map(|addr| Box::pin(task(addr, key)));

        self.run(tasks, self.consistency_param)
            .await
            .map(|results| {
                results
                    .into_iter()
                    .filter_map(|option| option)
                    .collect::<Vec<_>>()
            })
    }
}
