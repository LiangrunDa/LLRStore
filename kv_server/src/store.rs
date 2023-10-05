//! This module combines the cache and the storage to provide a unified interface.

use crate::cache::Cache;
use crate::storage::Storage;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared::key::{RingPos, RingRange};
use shared::value::{DvvSet, Value, VersionVec};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

#[derive(Debug)]
pub struct Request {
    pub answer: oneshot::Sender<Response>,
    pub cmd: Command,
}
pub type Response = Result<Vec<(RingPos, DvvSet)>>;

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Get(RingPos),
    Put(RingPos, VersionVec, Value),
    Delete(RingPos, VersionVec),
    GetWithinRange(RingRange),
    MergeBulk(Vec<(RingPos, DvvSet)>),
    ResetWithinRange(RingRange),
}

pub trait KeyValueStore {
    fn get(&mut self, key: &RingPos) -> Result<Option<DvvSet>>;
    fn get_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>>;
    fn put(&mut self, key: &RingPos, vvec: &VersionVec, value: Value) -> Result<Option<DvvSet>>;
    fn delete(&mut self, key: &RingPos, vvec: &VersionVec) -> Result<Option<DvvSet>>;
    fn merge_bulk(
        &mut self,
        key_value_pairs: impl Iterator<Item = (RingPos, DvvSet)>,
    ) -> Result<()>;
    fn reset_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>>;
}

pub struct Store<C: Cache, S: Storage> {
    actor: RingPos,
    requests: mpsc::Receiver<Request>,
    cache: C,
    storage: S,
}

impl<C: Cache, S: Storage> Store<C, S> {
    pub fn new(actor: RingPos, cache: C, storage: S, requests: mpsc::Receiver<Request>) -> Self {
        Self {
            actor,
            requests,
            cache,
            storage,
        }
    }
    pub fn run(&mut self) -> Result<()> {
        loop {
            let Request { answer, cmd } = match self.requests.blocking_recv() {
                Some(req) => req,
                // all senders have been dropped and there is no more use of the
                // store
                None => break,
            };
            let map_to_result = |key: RingPos, res: Result<Option<DvvSet>>| -> Response {
                res.map(|value| value.map_or(Vec::new(), |value| vec![(key, value)]))
            };
            let res: Response = match cmd {
                Command::Get(key) => {
                    debug!("Get request for key: {}", key);
                    map_to_result(key, self.get(&key))
                }
                Command::Put(key, vvec, value) => {
                    debug!("Put request for key: {} and value: {}", key, value);
                    map_to_result(key, self.put(&key, &vvec, value))
                }
                Command::Delete(key, vvec) => {
                    debug!("Delete request for key: {}", key);
                    self.delete(&key, &vvec)
                        .map(|value| value.map_or_else(Vec::new, |value| vec![(key, value)]))
                }
                Command::GetWithinRange(range) => {
                    debug!("GetWithinRange request for range: {}", range);
                    self.get_within_range(&range)
                }
                Command::MergeBulk(key_value_pairs) => {
                    debug!(
                        "PutBulk request for {} key-value pairs",
                        key_value_pairs.len()
                    );
                    self.merge_bulk(key_value_pairs.into_iter())
                        .map(|_| Vec::new())
                }
                Command::ResetWithinRange(range) => {
                    debug!("DeleteWithinRange request for range: {}", range);
                    self.reset_within_range(&range)
                }
            };
            debug!("Response: {:?}", res);
            // how to handle send error? just swallow and log?
            let _res = answer.send(res);
        }
        trace!("Store terminating gracefully");
        Ok(())
    }
}

impl<C: Cache, S: Storage> KeyValueStore for Store<C, S> {
    fn get(self: &mut Store<C, S>, ring_pos: &RingPos) -> Result<Option<DvvSet>> {
        self.cache
            .get(ring_pos)
            .map_or_else(|| self.storage.get(ring_pos), |val| Ok(Some(val)))
    }
    fn put(
        &mut self,
        ring_pos: &RingPos,
        vvec: &VersionVec,
        value: Value,
    ) -> Result<Option<DvvSet>> {
        self.get(ring_pos).and_then(|option| {
            let mut dvvs = option.unwrap_or_else(DvvSet::default);
            let _ = dvvs.handle_put(&self.actor, vvec, value);
            self.cache.put(ring_pos, &dvvs);
            self.storage.store(ring_pos, &dvvs)?;
            Ok(Some(dvvs))
        })
    }
    fn delete(&mut self, ring_pos: &RingPos, vvec: &VersionVec) -> Result<Option<DvvSet>> {
        self.get(ring_pos).and_then(|option| {
            option.map_or(Ok(None), |mut dvvs| {
                let _ = dvvs.handle_delete(&self.actor, vvec);
                self.cache.put(ring_pos, &dvvs);
                self.storage.store(ring_pos, &dvvs)?;
                Ok(Some(dvvs))
            })
        })
    }

    fn get_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>> {
        if range.is_empty() {
            // to allow a query with (from == to) to just return that the value
            // for one key
            return self
                .get(range.from())
                .map(|option| option.map_or_else(Vec::new, |value| vec![(*range.from(), value)]));
        }
        self.storage.get_within_range(range)
    }
    fn merge_bulk(
        &mut self,
        key_value_pairs: impl Iterator<Item = (RingPos, DvvSet)>,
    ) -> Result<()> {
        for (ring_pos, other_dvvs) in key_value_pairs {
            self.get(&ring_pos).and_then(|option| match option {
                Some(mut dvvs) => {
                    dvvs.sync(other_dvvs);
                    self.cache.put(&ring_pos, &dvvs);
                    self.storage.store(&ring_pos, &dvvs)
                }
                None => {
                    self.cache.put(&ring_pos, &other_dvvs);
                    self.storage.store(&ring_pos, &other_dvvs)
                }
            })?;
        }
        Ok(())
    }
    fn reset_within_range(&mut self, range: &RingRange) -> Result<Vec<(RingPos, DvvSet)>> {
        let ring_pos_value_pairs = self.storage.reset_within_range(range)?;
        for (ring_pos, _value) in ring_pos_value_pairs.iter() {
            self.cache.delete(ring_pos);
        }
        Ok(ring_pos_value_pairs)
    }
}
