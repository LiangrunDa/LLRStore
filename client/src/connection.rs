use anyhow::{Context, Result};
use shared::{
    key::MetaData,
    protocol::{DelimCodec, ProtocolError, Response},
};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct ConnectionPool {
    pool: HashMap<SocketAddr, ConnectionState>,
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionPool {
    pub fn new() -> Self {
        ConnectionPool {
            pool: HashMap::new(),
        }
    }

    // Get a connection from the pool (create one if not exists)
    pub async fn get_connection(
        &mut self,
        addr: SocketAddr,
    ) -> Result<&mut ConnectionState, ProtocolError> {
        match self.pool.entry(addr) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let connection_state = ConnectionState::new(addr).await?;
                Ok(entry.insert(connection_state))
            }
        }
    }

    async fn release_connection(&mut self, addr: SocketAddr) -> Result<(), ProtocolError> {
        if let Some(connection_state) = self.pool.get_mut(&addr) {
            connection_state.disconnect().await?;
        }
        self.pool.remove(&addr);
        Ok(())
    }
}

#[derive(Debug)]
pub enum ConnectionState {
    Connected(SocketAddr, DelimCodec, MetaData),
    Disconnected(),
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::Disconnected()
    }
}

impl ConnectionState {
    // immediately connects, use default if you want to connect later
    pub async fn new(addr: SocketAddr) -> Result<Self, ProtocolError> {
        let mut connection_state = Self::default();
        connection_state.connect(addr).await?;
        Ok(connection_state)
    }
    pub async fn connect(&mut self, addr: SocketAddr) -> Result<(), ProtocolError> {
        match self {
            Self::Connected(..) => Ok(()),
            Self::Disconnected() => {
                let stream = TcpStream::connect(addr)
                    .await
                    .with_context(|| format!("Connecting to server at {}", addr))
                    .map_err(|_e| ProtocolError::NotConnected)?;
                let stream = DelimCodec::new(stream, None);
                *self = Self::Connected(addr, stream, MetaData::default());
                Ok(())
            }
        }
    }
    pub fn is_connected(&self) -> Option<SocketAddr> {
        match self {
            Self::Connected(addr, _, _) => Some(*addr),
            Self::Disconnected() => None,
        }
    }
    pub async fn disconnect(&mut self) -> Result<(), ProtocolError> {
        match self {
            Self::Connected(_, _codec, _meta) => {
                // drop(codec); // might not be required..
                *self = Self::Disconnected();
                Ok(())
            }
            Self::Disconnected() => Err(ProtocolError::NotConnected),
        }
    }
    pub async fn send(&mut self, msg: impl Display + Debug) -> Result<(), ProtocolError> {
        match self {
            Self::Connected(_, codec, _meta) => codec.send_msg(msg).await,
            Self::Disconnected() => Err(ProtocolError::NotConnected),
        }
    }
    pub async fn receive(&mut self) -> Result<ResponseWrapper, ProtocolError> {
        match self {
            Self::Connected(_, codec, _meta) => codec.read_msg().await,
            Self::Disconnected() => Err(ProtocolError::NotConnected),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResponseWrapper(Response);

impl ResponseWrapper {
    pub fn into_inner(self) -> Response {
        self.0
    }
}

impl TryFrom<Vec<u8>> for ResponseWrapper {
    type Error = ProtocolError;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let value = String::from_utf8(value)
            .map_err(|_| ProtocolError::Violation("String is not valid utf8".to_string()))?;
        let value = value.trim_start();

        let response = value.parse::<Response>()?;
        Ok(Self(response))
        // if value == "Welcome to group 16's key-value store" {
        //     return Ok(Self(None));
        // }

        // let mut parts = value.splitn(3, ' ');
        // let result = parts
        //     .next()
        //     .ok_or(ProtocolError::Violation("Result is missing".to_string()))?;
        // match result {
        //     "put_success" => parts
        //         .next()
        //         .map(|_| Self(Ok("SUCCESS".to_string())))
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string())),
        //     "put_update" => parts
        //         .next()
        //         .map(|_| Self(Ok("SUCCESS".to_string())))
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string())),
        //     "put_error" => Ok(Self(Ok("ERROR".to_string()))),
        //     "get_success" => parts
        //         .next()
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string()))
        //         .and_then(|_| {
        //             parts
        //                 .next()
        //                 .map(|value| Self(Ok(value.to_string())))
        //                 .ok_or(ProtocolError::Violation("Value is missing".to_string()))
        //         }),
        //     "get_error" => parts
        //         .next()
        //         .map(|_| Self(Ok("ERROR".to_string())))
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string())),
        //     "delete_success" => parts
        //         .next()
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string()))
        //         .and_then(|_| {
        //             parts
        //                 .next()
        //                 .map(|_| Self(Ok("SUCCESS".to_string())))
        //                 .ok_or(ProtocolError::Violation("Value is missing".to_string()))
        //         }),
        //     "delete_error" => parts
        //         .next()
        //         .map(|_| Self(Ok("ERROR".to_string())))
        //         .ok_or(ProtocolError::Violation("Key is missing".to_string())),

        //     "server_not_responsible" => Ok(Self(Ok(result.to_string()))),
        //     "server_stopped" => Ok(Self(Ok(result.to_string()))),
        //     "server_write_lock" => Ok(Self(Ok(result.to_string()))),
        //     "keyrange_success" => parts
        //         .next()
        //         .map(|v| Self(Ok(v.to_string())))
        //         .ok_or(ProtocolError::Violation("keyrange is missing".to_string())),
        //     _ => Err(ProtocolError::Violation(format!(
        //         "Unknown result {}",
        //         result
        //     ))),
        // }
    }
}
