//! This module defines the command line interface (CLI) of the server.
use crate::cache::CacheEvictionStrategy;
use clap::error::ErrorKind;
use clap::CommandFactory;
use clap::Parser;
use protocol::{
    KV_CLIENT_DEFAULT_PORT, KV_KV_DEFAULT_PORT, KV_MEM_DEFAULT_PORT, MEM_KV_DEFAULT_PORT,
};
use shared::logger::LogLevel;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Ip address of a known peer.
    #[arg(short = 'y', long)]
    peer_addr: Option<String>,

    /// Port of a known peer.
    #[arg(short = 'x', long)]
    peer_port: Option<u16>,

    /// Ip address of the server.
    #[arg(short = 'a', long, default_value = "localhost")]
    address: String,

    /// Port of the server to listen for clients' requests.
    #[arg(short = 'p', long, default_value_t = KV_CLIENT_DEFAULT_PORT)]
    port: u16,

    #[arg(short = 'd', long, default_value = "./data/kv_server/storage")]
    directory: PathBuf,

    /// Relative path to the server's log file.
    #[arg(short = 'l', long, default_value = "./data/kv_server/kv_server.log")]
    log_file: PathBuf,

    /// Set the log level.
    #[arg(long = "ll", value_enum, default_value_t)]
    log_level: LogLevel,

    /// Set the cache size in number of entries.
    #[arg(short = 'c', long, default_value_t = 30)]
    cache_size: usize,

    /// Set the cache eviction strategy.
    #[arg(short = 's', long, value_enum, default_value_t)]
    cache_eviction_strategy: CacheEvictionStrategy,

    /// The listening socket address of the ecs server.
    #[arg(short = 'b', long, default_value_t = format!("localhost:{}", MEM_KV_DEFAULT_PORT))]
    membership_address: String,

    /// The listening port of the kv server for other kv servers' requests.
    #[arg(long, default_value_t = KV_KV_DEFAULT_PORT)]
    kv_port: u16,

    /// The listening port of the kv server for the ecs server's requests.
    #[arg(long, default_value_t = KV_MEM_DEFAULT_PORT)]
    membership_port: u16,

    /// Replication factor. Make sure to be the same across the server fleet.
    /// Must be bigger than 0.
    /// When talking about quorums, this is the number of replicas for a key,
    /// usually denoted by N.
    #[arg(long, default_value_t = NonZeroUsize::new(3).unwrap())]
    replication_factor: NonZeroUsize,

    /// Durability parameter. Make sure to be the same across the server fleet.
    /// Must be bigger than 0 and less than or equal to the replication factor.
    /// When talking about quorums, this is the number of replicas that must
    /// acknowledge a write before acking a client's write request,
    /// usually denoted by W.
    #[arg(long, default_value_t = NonZeroUsize::new(2).unwrap())]
    durability_param: NonZeroUsize,

    /// Consistency parameter. Make sure to be the same across the server fleet.
    /// Must be bigger than 0 and less than or equal to the replication factor.
    /// When talking about quorums, this is the number of replicas that are required
    /// to respond to a read before answering a client's read request,
    /// usually denoted by R.
    #[arg(long, default_value_t = NonZeroUsize::new(2).unwrap())]
    consistency_param: NonZeroUsize,
}

impl Args {
    pub fn validate(self) -> Self {
        if self.durability_param > self.replication_factor {
            let mut cmd = Self::command();
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "durability parameter ({}) must be less than or equal to the replication factor ({})",
                    self.durability_param, self.replication_factor
                ),
            )
            .exit();
        }
        if self.consistency_param > self.replication_factor {
            let mut cmd = Self::command();
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "consistency parameter ({}) must be less than or equal to the replication factor ({})",
                    self.consistency_param, self.replication_factor
                ),
            )
            .exit();
        }
        self
    }
    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }
    pub fn log_dir(&self) -> &Path {
        self.log_file.parent().unwrap_or(Path::new("."))
    }
    pub fn log_file(&self) -> &Path {
        self.log_file
            .file_name()
            .map(|f| f.as_ref())
            .unwrap_or(Path::new("server.log"))
    }
    fn resolve_addr(string: &String) -> SocketAddr {
        string
            .to_socket_addrs()
            .expect(
                format!(
                    "Could not resolve server address by looking up DNS {}",
                    string
                )
                .as_str(),
            )
            .filter(|addr| match addr {
                SocketAddr::V4(_) => true,
                SocketAddr::V6(_) => false,
            })
            .next()
            .expect(
                format!(
                    "Could not resolve server address by filtering V4 {}",
                    string
                )
                .as_str(),
            )
    }
    pub fn socket_addr(&self) -> SocketAddr {
        Self::resolve_addr(&format!("{}:{}", self.address, self.port))
    }
    pub fn membership_kv_addr(&self) -> SocketAddr {
        Self::resolve_addr(&self.membership_address)
    }
    pub fn kv_kv_addr(&self) -> SocketAddr {
        Self::resolve_addr(&format!("{}:{}", self.address, self.kv_port))
    }
    pub fn kv_membership_addr(&self) -> SocketAddr {
        Self::resolve_addr(&format!("{}:{}", self.address, self.membership_port))
    }
    pub fn cache_size(&self) -> usize {
        self.cache_size
    }
    pub fn cache_eviction_strategy(&self) -> CacheEvictionStrategy {
        self.cache_eviction_strategy
    }
    pub fn data_dir(&self) -> &Path {
        self.directory.as_path()
    }
    pub fn replication_factor(&self) -> NonZeroUsize {
        self.replication_factor
    }
    pub fn durability_param(&self) -> NonZeroUsize {
        self.durability_param
    }
    pub fn consistency_param(&self) -> NonZeroUsize {
        self.consistency_param
    }
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr.clone().and_then(|ip| {
            self.peer_port.clone().map(|port| {
                let addr = format!("{}:{}", ip, port);
                info!("Resolving peer address {}", addr);
                Self::resolve_addr(&addr)
            })
        })
    }
}

pub fn parse_args() -> Args {
    Args::parse()
}
