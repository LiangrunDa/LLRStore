use anyhow::Result;
use shared::key::{MetaData, RangeServerInfo, RingPos, RingRange};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{fmt::Display, net::SocketAddr};
use tokio::sync::RwLock;
use tracing::{info, warn};

pub type SharedServerState = Arc<RwLock<ServerState>>;

pub fn init_shared_state(addr: SocketAddr, replication_factor: NonZeroUsize) -> SharedServerState {
    Arc::new(RwLock::new(ServerState::new(addr, replication_factor)))
}

#[derive(Debug, Clone)]
pub struct ServerState {
    addr: SocketAddr,
    replication_factor: NonZeroUsize,
    mode: ServerMode,
}

impl ServerState {
    pub fn new(addr: SocketAddr, replication_factor: NonZeroUsize) -> Self {
        Self {
            addr,
            replication_factor,
            mode: ServerMode::default(),
        }
    }
    pub fn init_serving(&mut self, metadata: MetaData) -> Result<()> {
        self.mode = ServerMode::new_serving(metadata, &self.addr, self.replication_factor)?;
        Ok(())
    }
    /// Get the ring's coordinators for each range
    pub fn all_coordinators(&self) -> Vec<RangeServerInfo> {
        self.mode
            .get_metadata()
            .map_or_else(Vec::new, |metadata| metadata.all_coordinators())
    }
    /// Get the ring's coordinators and replicas for each range
    pub fn all_replicas(&self) -> Vec<RangeServerInfo> {
        self.mode.get_metadata().map_or_else(Vec::new, |metadata| {
            metadata.all_replicas(self.replication_factor)
        })
    }
    /// Get this server's replicas
    pub fn replicas(&self) -> Vec<RangeServerInfo> {
        self.mode.get_metadata().map_or_else(Vec::new, |metadata| {
            metadata
                .get_key_servers(&self.addr, self.replication_factor)
                .get(1..)
                .map_or_else(Vec::new, |s| s.to_vec())
        })
    }
    pub fn replicas_for_key(&self, key: impl Into<RingPos>) -> Vec<RangeServerInfo> {
        self.mode.get_metadata().map_or_else(Vec::new, |metadata| {
            metadata.get_key_servers(key, self.replication_factor)
        })
    }
    pub fn update_metadata(&mut self, metadata: MetaData) -> Option<MetaData> {
        match Responsibility::from_metadata(&metadata, &self.addr, self.replication_factor) {
            Ok(responsibility) => self.mode.update_metadata(metadata, responsibility),
            Err(_e) => {
                warn!(
                    "Server {} not found in updated metadata, switching to stopped mode",
                    self.addr
                );
                self.mode = ServerMode::Stopped;
                None
            }
        }
    }
    pub fn mode(&self) -> &ServerMode {
        &self.mode
    }
    pub fn mode_mut(&mut self) -> &mut ServerMode {
        &mut self.mode
    }
}

#[derive(Debug, Clone)]
pub enum ServerMode {
    Stopped,
    WriteLocked(WriteLocked),
    Serving(Serving),
}

impl Default for ServerMode {
    fn default() -> Self {
        Self::Stopped
    }
}

impl ServerMode {
    pub fn new_serving(
        metadata: MetaData,
        addr: &SocketAddr,
        replication_factor: NonZeroUsize,
    ) -> Result<Self> {
        let responsibility = Responsibility::from_metadata(&metadata, addr, replication_factor)?;
        info!("Server is now {}", responsibility);
        Ok(Self::Serving(Serving {
            responsibility,
            metadata,
        }))
    }
    pub fn get_metadata(&self) -> Option<&MetaData> {
        match self {
            ServerMode::Stopped => None,
            ServerMode::WriteLocked(state) => Some(&state.metadata),
            ServerMode::Serving(state) => Some(&state.metadata),
        }
    }
    pub fn update_metadata(
        &mut self,
        metadata: MetaData,
        responsibility: Responsibility,
    ) -> Option<MetaData> {
        match self {
            ServerMode::Stopped => None,
            ServerMode::WriteLocked(state) => {
                info!("Server is now {}", responsibility);
                let old = std::mem::replace(&mut state.metadata, metadata);
                state.responsibility = responsibility;
                Some(old)
            }
            ServerMode::Serving(state) => {
                info!("Server is now {}", responsibility);
                let old = std::mem::replace(&mut state.metadata, metadata);
                state.responsibility = responsibility;
                Some(old)
            }
        }
    }
    pub fn acquire_write_lock(&mut self) {
        match self {
            ServerMode::Stopped => {}
            ServerMode::WriteLocked(_) => {}
            ServerMode::Serving(state) => {
                *self = ServerMode::WriteLocked(WriteLocked {
                    metadata: std::mem::take(&mut state.metadata),
                    responsibility: std::mem::take(&mut state.responsibility),
                });
            }
        }
    }
    pub fn release_write_lock(&mut self) {
        match self {
            ServerMode::Stopped => {}
            ServerMode::Serving(_) => {}
            ServerMode::WriteLocked(state) => {
                *self = ServerMode::Serving(Serving {
                    metadata: std::mem::take(&mut state.metadata),
                    responsibility: std::mem::take(&mut state.responsibility),
                });
            }
        }
    }
    pub fn is_read_responsible(&self, key: impl Into<RingPos>) -> bool {
        match self {
            ServerMode::Stopped => false,
            ServerMode::WriteLocked(state) => state.responsibility.is_read_responsible(key),
            ServerMode::Serving(state) => state.responsibility.is_read_responsible(key),
        }
    }
    pub fn is_write_responsible(&self, key: impl Into<RingPos>) -> bool {
        match self {
            ServerMode::Stopped => false,
            ServerMode::WriteLocked(state) => state.responsibility.is_write_responsible(key),
            ServerMode::Serving(state) => state.responsibility.is_write_responsible(key),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriteLocked {
    responsibility: Responsibility,
    metadata: MetaData,
}

#[derive(Debug, Clone)]
pub struct Serving {
    responsibility: Responsibility,
    metadata: MetaData,
}

// default is necessary for std::mem::take but semantically useless
#[derive(Debug, Clone, Default)]
pub struct Responsibility {
    primary: RingRange,
    // we assume it is sorted
    secondaries: Vec<RingRange>,
}

impl Responsibility {
    pub fn from_metadata(
        metadata: &MetaData,
        addr: &SocketAddr,
        replication_factor: NonZeroUsize,
    ) -> Result<Self> {
        let ranges = metadata.get_server_ranges(addr, replication_factor);

        let primary = ranges
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Primary range for server not found in metadata"))?
            .range;
        let secondaries = ranges[1..].iter().map(|info| info.range).collect();

        Ok(Self {
            primary,
            secondaries,
        })
    }
    pub fn is_write_responsible(&self, key: impl Into<RingPos>) -> bool {
        self.primary.contains(&key.into())
    }
    pub fn is_read_responsible(&self, key: impl Into<RingPos>) -> bool {
        let key = key.into();
        self.primary.contains(&key) || self.secondaries.iter().any(|range| range.contains(&key))
    }
}

impl Display for Responsibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let primary = format!("primary for {}", self.primary);
        if self.secondaries.is_empty() {
            return write!(f, "{}", primary);
        }
        let with_secondaries = self
            .secondaries
            .iter()
            .fold(format!("{} and secondary for", primary), |acc, range| {
                format!("{} {},", acc, range)
            });
        write!(f, "{}", with_secondaries.trim_end_matches(","))
    }
}
