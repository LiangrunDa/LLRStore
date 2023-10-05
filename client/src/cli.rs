//! This module defines the command line interface (CLI) of the program. It is
//! responsible for parsing the command line arguments and options and _not_
//! for the interactive REPL. See [repl module](crate::repl) for that.
use clap::Parser;
use protocol::KV_CLIENT_DEFAULT_PORT;
use shared::logger::LogLevel;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Domain name or ip address of the server to connect to
    #[arg(short, long)]
    address: Option<String>,

    /// Port of the server to connect to
    #[arg(short, long, default_value_t = KV_CLIENT_DEFAULT_PORT)]
    port: u16,

    /// Set the log level.
    #[arg(long = "ll", value_enum, default_value_t)]
    log_level: LogLevel,

    /// Relative path to the server's log file.
    #[arg(short = 'l', long, default_value = "./data/client/client.log")]
    log_file: PathBuf,
}

impl Args {
    pub fn server_addr(&self) -> Option<SocketAddr> {
        let Some(addr) = &self.address else {
            return None;
        };
        Some(
            to_ipv4_socket_addr(addr, &self.port.to_string()).expect(
                format!("Could not resolve server address {} {}", addr, self.port).as_str(),
            ),
        )
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
}

pub fn parse_args() -> Args {
    Args::parse()
}

pub fn to_ipv4_socket_addr(addr: &str, port: &str) -> Result<SocketAddr, ()> {
    format!("{}:{}", addr, port)
        .to_socket_addrs()
        .map_err(|_e| ())?
        .filter(|addr| match addr {
            SocketAddr::V4(_) => true,
            SocketAddr::V6(_) => false,
        })
        .next()
        .ok_or(())
}
