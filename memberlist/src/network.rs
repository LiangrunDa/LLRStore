use crate::node_list::Node;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::UdpSocket;
use tracing::info;

// Message format:
// MAGIC (4 bytes) | content_identifier (1 byte) | seq (4 bytes) | msg_len (4 bytes) | content (variable length)

#[derive(Serialize, Deserialize)]
pub(crate) enum Message {
    Ping(u32, Node),
    Ack(u32, Node),
    Push(u32, Vec<u8>),
    Pull(u32, Vec<u8>),
    JoinRequest(u32),
    JoinReply(u32, Vec<u8>)
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Message::Ping(seq, node) => write!(f, "Ping(seq:{seq}, {node})"),
            Message::Ack(seq, node) => write!(f, "Ack(seq:{seq}, {node})"),
            Message::Push(seq, data) => write!(f, "Push(seq:{seq}, {} bytes)", data.len()),
            Message::Pull(seq, data) => write!(f, "Pull(seq:{seq}, {} bytes)", data.len()),
            Message::JoinRequest(seq) => write!(f, "JoinRequest(seq:{seq})"),
            Message::JoinReply(seq, data) => write!(f, "JoinReply(seq:{seq}, {} bytes)", data.len()),
        }
    }
}

impl Message {
    const MAGIC: &'static [u8] = b"MEMB";
    pub(crate) fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut encoded_bytes = Vec::new();

        // Encode magic
        encoded_bytes.extend(Self::MAGIC);

        match &self {
            Message::Ping(seq, node) => {
                encoded_bytes.push(0x01); // Identifier for Ping
                encoded_bytes.extend(seq.to_le_bytes());
                let node_bytes = bincode::serialize(&node)?;
                // Encode length of node_bytes
                let node_bytes_len = node_bytes.len() as u32;
                encoded_bytes.extend(node_bytes_len.to_le_bytes());
                // Encode node_bytes
                encoded_bytes.extend(node_bytes);
            }
            Message::Ack(seq, node) => {
                encoded_bytes.push(0x02); // Identifier for Ack
                encoded_bytes.extend(seq.to_le_bytes());
                let node_bytes = bincode::serialize(&node)?;
                // Encode length of node_bytes
                let node_bytes_len = node_bytes.len() as u32;
                encoded_bytes.extend(node_bytes_len.to_le_bytes());
                // Encode node_bytes
                encoded_bytes.extend(node_bytes);
            }
            Message::Push(seq, data) => {
                encoded_bytes.push(0x03); // Identifier for Push
                encoded_bytes.extend(seq.to_le_bytes());
                let data_len = data.len() as u32;
                encoded_bytes.extend(data_len.to_le_bytes());
                // Encode data
                encoded_bytes.extend(data);
            }
            Message::Pull(seq, data) => {
                encoded_bytes.push(0x04); // Identifier for Pull
                encoded_bytes.extend(seq.to_le_bytes());
                let data_len = data.len() as u32;
                encoded_bytes.extend(data_len.to_le_bytes());
                // Encode data
                encoded_bytes.extend(data);
            }
            Message::JoinRequest(seq) => {
                encoded_bytes.push(0x05); // Identifier for JoinRequest
                encoded_bytes.extend(seq.to_le_bytes());
                let data_len = 0 as u32;
                encoded_bytes.extend(data_len.to_le_bytes());
            }
            Message::JoinReply(seq, data) => {
                encoded_bytes.push(0x06); // Identifier for JoinReply
                encoded_bytes.extend(seq.to_le_bytes());
                let data_len = data.len() as u32;
                encoded_bytes.extend(data_len.to_le_bytes());
                // Encode data
                encoded_bytes.extend(data);
            }
        }
        Ok(encoded_bytes)
    }

    pub(crate) fn decode(bytes: Vec<u8>) -> Result<Self, ProtocolError> {
        let mut cursor = 0;

        // Decode magic
        let magic_size = 4; // Assuming magic is always 4 bytes
        let magic = bytes[..magic_size].to_vec();
        if magic != Self::MAGIC {
            return Err(ProtocolError::InvalidMagicNumber);
        }
        cursor += magic_size;

        // Decode content_identifier
        let content_identifier = bytes[cursor];
        cursor += 1;

        // decode seq
        let seq_bytes = &bytes[cursor..cursor + 4];
        let seq = u32::from_le_bytes(seq_bytes.try_into()?);
        cursor += 4;

        // Decode msg_len
        let msg_len_bytes = &bytes[cursor..cursor + 4];
        let msg_len = u32::from_le_bytes(msg_len_bytes.try_into().unwrap());
        cursor += 4;

        let content = match content_identifier {
            0x01 => {
                let node = bincode::deserialize(&bytes[cursor..cursor + msg_len as usize])?;
                Message::Ping(seq, node)
            }
            0x02 => {
                let node = bincode::deserialize(&bytes[cursor..cursor + msg_len as usize])?;
                Message::Ack(seq, node)
            }
            0x03 => {
                if cursor + msg_len as usize > bytes.len() {
                    return Err(ProtocolError::ExceededMaximumMessageLength);
                }
                let data = bytes[cursor..cursor + msg_len as usize].to_vec();
                Message::Push(seq, data)
            }
            0x04 => {
                if cursor + msg_len as usize > bytes.len() {
                    return Err(ProtocolError::ExceededMaximumMessageLength);
                }
                let data = bytes[cursor..cursor + msg_len as usize].to_vec();
                Message::Pull(seq, data)
            }
            0x05 => Message::JoinRequest(seq),
            0x06 => {
                if cursor + msg_len as usize > bytes.len() {
                    return Err(ProtocolError::ExceededMaximumMessageLength);
                }
                let data = bytes[cursor..cursor + msg_len as usize].to_vec();
                Message::JoinReply(seq, data)
            }
            _ => return Err(ProtocolError::InvalidMessageIdentifier),
        };
        Ok(content)
    }
}

pub(crate) struct Communicator {
    socket: UdpSocket,
}

impl Communicator {
    const MAX_MESSAGE_LENGTH: usize = 65536;

    pub(crate) fn new(socket: UdpSocket) -> Self {
        Self { socket }
    }

    pub(crate) async fn send_ping(
        &mut self,
        node: Node,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::Ping(seq, node);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        info!("Sent ping {:?} to {}", msg, peer_addr);
        Ok(())
    }

    pub(crate) async fn send_ack(
        &mut self,
        node: Node,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::Ack(seq, node);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        info!("Sent ack {:?} to {}", msg, peer_addr);
        Ok(())
    }

    pub(crate) async fn send_push(
        &mut self,
        data: Vec<u8>,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::Push(seq, data);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        Ok(())
    }

    pub(crate) async fn send_pull(
        &mut self,
        data: Vec<u8>,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::Pull(seq, data);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        Ok(())
    }

    pub(crate) async fn send_join(
        &mut self,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::JoinRequest(seq);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        Ok(())
    }

    pub(crate) async fn send_join_reply(
        &mut self,
        data: Vec<u8>,
        peer_addr: SocketAddr,
        seq: u32,
    ) -> Result<(), ProtocolError> {
        let msg = Message::JoinReply(seq, data);
        let msg_bytes = msg.encode()?;
        self.socket.send_to(&msg_bytes, peer_addr).await?;
        Ok(())
    }

    pub(crate) async fn receive_message(&mut self) -> Result<(Message, SocketAddr), ProtocolError> {
        let mut buf = vec![0; Self::MAX_MESSAGE_LENGTH];
        let (len, peer_addr) = self.socket.recv_from(&mut buf).await?;
        let decoded = Message::decode(buf[..len].to_vec());
        match decoded {
            Ok(msg) => Ok((msg, peer_addr)),
            Err(e) => match e {
                ProtocolError::ExceededMaximumMessageLength => {
                    let mut buf2 = vec![0; Self::MAX_MESSAGE_LENGTH * 8]; // try a larger buffer
                                                                          // TODO: make this more robust
                    let (len, peer_addr) = self.socket.recv_from(&mut buf2).await?;
                    buf.extend(buf2[..len].to_vec());
                    let decoded = Message::decode(buf[..len].to_vec())?;
                    info!("Received message {:?} from {}", decoded, peer_addr);
                    Ok((decoded, peer_addr))
                }
                _ => Err(e),
            },
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum ProtocolError {
    #[error(transparent)]
    EncodingError(#[from] bincode::Error),
    #[error("Invalid magic number")]
    InvalidMagicNumber,
    #[error("Invalid message identifier")]
    InvalidMessageIdentifier,
    #[error("Exceeded maximum message length")]
    ExceededMaximumMessageLength,
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Invalid sequence number")]
    SeqError(#[from] std::array::TryFromSliceError),
    #[error(transparent)]
    InternalRequestError(#[from] crate::internal_service::InternalRequestError),
}
