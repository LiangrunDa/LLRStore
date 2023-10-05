use clap::Parser;
use protocol::{KV_MEM_DEFAULT_PORT, MEM_KV_DEFAULT_PORT, GOSSIP_DEFAULT_PORT};
use shared::logger::LogLevel;
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Name of the server
    #[arg(short = 'n', long, default_value = "node")]
    name: String,

    /// Gossip port
    #[arg(short = 'g', long, default_value_t = GOSSIP_DEFAULT_PORT)]
    gossip_port: u16,

    /// Domain name or ip address of the server to connect to
    #[arg(short = 'a', long, default_value = "localhost")]
    address: String,

    /// Port of the server to connect to
    #[arg(short = 'p', long, default_value_t = MEM_KV_DEFAULT_PORT)]
    port: u16,

    /// Relative path to the server's log file
    #[arg(short = 'l', long, default_value = "./data/membership/membership.log")]
    log_file: PathBuf,

    /// Set the log level
    #[arg(long = "ll", value_enum, default_value = "TRACE")]
    log_level: LogLevel,

    /// Set the port of kv server for membership
    #[arg(short = 'k', long, default_value_t = KV_MEM_DEFAULT_PORT)]
    kv_port: u16,
}

impl Args {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn gossip_port(&self) -> u16 {
        self.gossip_port
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
            .unwrap_or(Path::new("membership.log"))
    }

    pub fn socket_addr(&self) -> SocketAddr {
        let addrs_iter = format!("{}:{}", self.address, self.port)
            .to_socket_addrs()
            .unwrap();
        for addr in addrs_iter {
            if let IpAddr::V4(_ipv4) = addr.ip() {
                return addr;
            }
        }
        panic!("Could not resolve membership address");
    }

    pub fn kv_addr(&self) -> SocketAddr {
        let addrs_iter = format!("{}:{}", self.address, self.kv_port)
            .to_socket_addrs()
            .unwrap();
        for addr in addrs_iter {
            if let IpAddr::V4(_ipv4) = addr.ip() {
                return addr;
            }
        }
        panic!("Could not resolve kv address");
    }
}

pub fn parse_args() -> Args {
    Args::parse()
}
