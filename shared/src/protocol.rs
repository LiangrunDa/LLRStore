// #![allow(unused)]

use crate::key::{Key, RangeServerInfo, RingPos};
use crate::value::{DvvSet, Siblings, Value, VersionVec};
use anyhow::Result;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    time::timeout,
};
use tracing::trace;

const PARAM_DELIMITER: &'static str = &" ";

/// Message sent from a client to a server.
#[derive(Debug, Clone)]
pub enum Request {
    Put(PutRequest),
    Get(GetRequest),
    Delete(DeleteRequest),
    KeyRange,
}

impl Request {
    pub fn key(&self) -> Option<&Key> {
        match self {
            Request::Put(request) => Some(&request.key),
            Request::Get(request) => Some(&request.key),
            Request::Delete(request) => Some(&request.key),
            Request::KeyRange => None,
        }
    }
    pub fn is_read_request(&self) -> bool {
        match self {
            Request::Put(_) | Request::Delete(_) => false,
            Request::Get(_) | Request::KeyRange => true,
        }
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let codec = ListCodec::new(PARAM_DELIMITER);
        match self {
            Request::Put(request) => {
                f.write_str(
                    &codec.encode([&"put", &request.key, &request.context, &request.value]
                        as [&dyn ToString; 4]),
                )
            }
            Request::Delete(request) => {
                f.write_str(
                    &codec
                        .encode([&"delete", &request.key, &request.context] as [&dyn ToString; 3]),
                )
            }
            Request::Get(request) => {
                f.write_str(&codec.encode([&"get", &request.key] as [&dyn ToString; 2]))
            }
            Request::KeyRange => f.write_str("keyrange"),
        }
    }
}

/// This implements deserialize for Request
impl TryFrom<Vec<u8>> for Request {
    type Error = ProtocolError;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let value = String::from_utf8(value)
            .map_err(|_| ProtocolError::Violation("String is not valid utf8".to_string()))?;
        let value = value.trim_start();
        let mut parts = value.splitn(2, PARAM_DELIMITER);
        let command = parts
            .next()
            .ok_or(ProtocolError::Violation("Command missing".to_string()))?;
        let args = parts.next().unwrap_or("");
        match command {
            "put" => {
                let mut parts = args.splitn(3, PARAM_DELIMITER);
                let key = parts
                    .next()
                    .ok_or(ProtocolError::Violation("Key missing".to_string()))?;
                let context = parts
                    .next()
                    .and_then(|s| s.parse::<VersionVec>().ok())
                    .ok_or(ProtocolError::Violation("Context missing".to_string()))?;
                let value = parts
                    .next()
                    .ok_or(ProtocolError::Violation("Value missing".to_string()))?;
                Ok(Self::Put(PutRequest {
                    key: Key::new(key.to_string()),
                    context,
                    value: Value::new(value.to_string()),
                }))
            }
            "get" => {
                let key = args;
                if key.is_empty() {
                    return Err(ProtocolError::Violation("Key missing".to_string()));
                }
                Ok(Self::Get(GetRequest {
                    key: Key::new(key.to_string()),
                }))
            }
            "delete" => {
                let mut parts = args.splitn(2, PARAM_DELIMITER);
                let key = parts
                    .next()
                    .ok_or(ProtocolError::Violation("Key missing".to_string()))?;
                let context = parts
                    .next()
                    .and_then(|s| s.parse::<VersionVec>().ok())
                    .ok_or(ProtocolError::Violation("Context missing".to_string()))?;
                Ok(Self::Delete(DeleteRequest {
                    key: Key::new(key.to_string()),
                    context,
                }))
            }
            "keyrange" => Ok(Self::KeyRange),
            _ => Err(ProtocolError::Parsing {
                expected: "command (get|put|delete|keyrange)".to_string(),
                found: command.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutRequest {
    pub key: Key,
    pub context: VersionVec,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct GetRequest {
    pub key: Key,
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub key: Key,
    pub context: VersionVec,
}

/// Message sent from a server back to a client in response to a Request.
#[derive(Debug, Clone)]
pub enum Response {
    PutSuccess(PutSuccessResponse),
    PutError(PutErrorResponse),
    GetSuccess(GetSuccessResponse),
    GetError(GetErrorResponse),
    DeleteSuccess(DeleteSuccessResponse),
    DeleteError(DeleteErrorResponse),
    ServerStopped(ServerStoppedResponse),
    ServerWriteLock(ServerWriteLockResponse),
    KeyRangeSuccess(KeyRangeSuccessResponse),
    Error(String),
}

impl Response {
    pub fn from_request_and_store_resp(
        request: Request,
        store_resp: Result<Vec<(RingPos, DvvSet)>>,
    ) -> Self {
        let store_resp = store_resp.map(|resp| match resp.len() {
            0 => None,
            1 => Some(resp.into_iter().next().unwrap().1),
            _ => panic!(
                "Unexpected response from store with more than one item for a get/put/delete"
            ),
        });
        match (request, store_resp) {
            (Request::Put(request), Ok(Some(dvvs))) => Response::PutSuccess(PutSuccessResponse(
                InnerSuccessResponse::new(request.key, dvvs),
            )),
            (Request::Put(request), Err(_e)) => {
                Response::PutError(PutErrorResponse(InnerErrResponse::new(request.key)))
            }
            (Request::Get(request), Ok(Some(dvvs))) => Response::GetSuccess(GetSuccessResponse(
                InnerSuccessResponse::new(request.key, dvvs),
            )),
            (Request::Get(request), Err(_)) | (Request::Get(request), Ok(None)) => {
                Response::GetError(GetErrorResponse(InnerErrResponse::new(request.key)))
            }
            (Request::Delete(request), Ok(Some(dvvs))) => Response::DeleteSuccess(
                DeleteSuccessResponse(InnerSuccessResponse::new(request.key, dvvs)),
            ),
            (Request::Delete(request), Err(_)) | (Request::Delete(request), Ok(None)) => {
                Response::DeleteError(DeleteErrorResponse(InnerErrResponse::new(request.key)))
            }
            _ => unreachable!(),
        }
    }
}

impl Display for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let codec = ListCodec::new(PARAM_DELIMITER);
        match self {
            Response::PutSuccess(response) => {
                f.write_str(&codec.encode(["put_success", &response.0.to_string()]))
            }
            Response::PutError(response) => {
                f.write_str(&codec.encode(["put_error", &response.0.to_string()]))
            }
            Response::GetSuccess(response) => {
                f.write_str(&codec.encode(["get_success", &response.0.to_string()]))
            }
            Response::GetError(response) => {
                f.write_str(&codec.encode(["get_error", &response.0.to_string()]))
            }
            Response::DeleteSuccess(response) => {
                f.write_str(&codec.encode(["delete_success", &response.0.to_string()]))
            }
            Response::DeleteError(response) => {
                f.write_str(&codec.encode(["delete_error", &response.0.to_string()]))
            }
            Response::ServerStopped(_) => f.write_str("server_stopped"),
            Response::ServerWriteLock(_) => f.write_str("server_write_lock"),
            Response::KeyRangeSuccess(response) => f.write_str(&codec.encode([
                "keyrange_success",
                &ListCodec::default().encode(&response.server_ranges),
            ])),
            Response::Error(description) => write!(f, "error {}", description),
        }
    }
}

impl FromStr for Response {
    type Err = ProtocolError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parse_error = || -> ProtocolError {
            ProtocolError::Parsing {
                expected: "Response".to_string(),
                found: s.to_string(),
            }
        };
        let mut parts = s.splitn(2, PARAM_DELIMITER);
        let command = parts.next().ok_or_else(parse_error)?;
        let remainder = parts.next().ok_or_else(parse_error)?;
        match command {
            "put_success" => Ok(Response::PutSuccess(PutSuccessResponse(
                InnerSuccessResponse::from_str(remainder)?,
            ))),
            "delete_success" => Ok(Response::DeleteSuccess(DeleteSuccessResponse(
                InnerSuccessResponse::from_str(remainder)?,
            ))),
            "get_success" => Ok(Response::GetSuccess(GetSuccessResponse(
                InnerSuccessResponse::from_str(remainder)?,
            ))),
            "put_error" => Ok(Response::PutError(PutErrorResponse(
                InnerErrResponse::from_str(remainder)?,
            ))),
            "delete_error" => Ok(Response::DeleteError(DeleteErrorResponse(
                InnerErrResponse::from_str(remainder)?,
            ))),
            "get_error" => Ok(Response::GetError(GetErrorResponse(
                InnerErrResponse::from_str(remainder)?,
            ))),
            "server_stopped" => Ok(Response::ServerStopped(ServerStoppedResponse {})),
            "server_write_lock" => Ok(Response::ServerWriteLock(ServerWriteLockResponse {})),
            "keyrange_success" => Ok(Response::KeyRangeSuccess(KeyRangeSuccessResponse {
                server_ranges: ListCodec::default()
                    .decode(remainder)
                    .map_err(|_e| parse_error())?,
            })),
            "error" => Ok(Response::Error(remainder.to_string())),
            _ => Ok(Response::Error("unexpected error".to_string())),
        }
    }
}

/// This essentially provides serialize() for Response.
impl From<Response> for String {
    fn from(value: Response) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct InnerSuccessResponse {
    pub key: Key,
    pub context: VersionVec,
    pub siblings: Siblings,
}

impl InnerSuccessResponse {
    pub fn new(key: Key, dvvs: DvvSet) -> Self {
        Self {
            key,
            context: dvvs.join(),
            siblings: Siblings::from(&mut dvvs.into_values() as &mut dyn Iterator<Item = Value>),
        }
    }
}

impl Display for InnerSuccessResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let codec = ListCodec::new(PARAM_DELIMITER);
        f.write_str(&codec.encode([&self.key, &self.context, &self.siblings] as [&dyn ToString; 3]))
    }
}

impl FromStr for InnerSuccessResponse {
    type Err = ProtocolError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parse_error = || -> ProtocolError {
            ProtocolError::Parsing {
                expected: "InnerSuccessResponse".to_string(),
                found: s.to_string(),
            }
        };
        let mut parts = s.splitn(3, PARAM_DELIMITER);
        Ok(Self {
            key: Key::from(parts.next().ok_or_else(parse_error)?),
            context: parts
                .next()
                .ok_or_else(parse_error)?
                .parse::<VersionVec>()
                .map_err(|_e| parse_error())?,
            siblings: parts
                .next()
                .ok_or_else(parse_error)?
                .parse::<Siblings>()
                .map_err(|_e| parse_error())?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct InnerErrResponse {
    key: Key,
}

impl InnerErrResponse {
    pub fn new(key: Key) -> Self {
        Self { key }
    }
}

impl Display for InnerErrResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.key.to_string())
    }
}

impl FromStr for InnerErrResponse {
    type Err = ProtocolError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self { key: Key::from(s) })
    }
}

#[derive(Debug, Clone)]
pub struct PutSuccessResponse(pub InnerSuccessResponse);

#[derive(Debug, Clone)]
pub struct PutErrorResponse(pub InnerErrResponse);

#[derive(Debug, Clone)]
pub struct GetSuccessResponse(pub InnerSuccessResponse);

#[derive(Debug, Clone)]
pub struct GetErrorResponse(pub InnerErrResponse);

#[derive(Debug, Clone)]
pub struct DeleteSuccessResponse(pub InnerSuccessResponse);

#[derive(Debug, Clone)]
pub struct DeleteErrorResponse(pub InnerErrResponse);

#[derive(Debug, Clone)]
pub struct ServerStoppedResponse;

#[derive(Debug, Clone)]
pub struct ServerWriteLockResponse;

#[derive(Debug, Clone)]
pub struct KeyRangeSuccessResponse {
    pub server_ranges: Vec<RangeServerInfo>,
}

/// A codec for encoding and decoding messages that are delimited by a carriage
/// return and newline `\r\n`.
#[derive(Debug)]
pub struct DelimCodec {
    /// The timeout for reading from the stream.
    timeout: Option<Duration>,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl DelimCodec {
    const MSG_DELIMITER: &'static str = "\r\n";
    const MAX_LEN: usize = 128 * 1 << 10; // 128 KiB

    pub fn new(stream: TcpStream, timeout: Option<Duration>) -> Self {
        let (reader, writer) = stream.into_split();
        let reader = BufReader::new(reader);
        let writer = BufWriter::new(writer);
        Self {
            timeout,
            reader,
            writer,
        }
    }
    pub async fn send_msg<Msg>(&mut self, msg: Msg) -> Result<(), ProtocolError>
    where
        Msg: ToString,
    {
        let msg = format!("{}{}", msg.to_string(), Self::MSG_DELIMITER);
        let bytes = msg.into_bytes();
        // trace!("Sending encoded message '{:?}'", bytes);
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }
    pub async fn read_msg<Msg>(&mut self) -> Result<Msg, ProtocolError>
    where
        Msg: TryFrom<Vec<u8>, Error = ProtocolError>,
    {
        let mut buf = [0 as u8; Self::MAX_LEN];
        let mut read = 0;
        loop {
            let read_stream = async {
                match self.reader.read(&mut buf[read..]).await {
                    Ok(len) => Ok(len),
                    Err(e) => return Err(e.into()),
                }
            };
            let len = match self.timeout {
                Some(duration) => {
                    trace!(
                        "Awaiting read from stream with timeout of {}s...",
                        duration.as_secs()
                    );
                    match timeout(duration, read_stream).await {
                        Ok(Ok(len)) => len,
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(ProtocolError::Timeout(duration)),
                    }
                }
                None => {
                    trace!("Awaiting read from stream indefinitely...");
                    read_stream.await?
                }
            };
            let lower = read;
            let upper = read + len;
            if upper >= Self::MAX_LEN {
                return Err(ProtocolError::Violation(String::from(
                    "Max message length exceeded",
                )));
            }
            if len == 0 {
                trace!("EOF of stream and did not encounter message delimiter");
                return Err(ProtocolError::Closed);
            }
            // trace!("Received bytes {:?}", &buf[..upper]);
            for i in lower..=(upper - Self::MSG_DELIMITER.len()) {
                if &buf[i..i + Self::MSG_DELIMITER.len()] == Self::MSG_DELIMITER.as_bytes() {
                    let msg = Msg::try_from(buf[..i].to_vec())?;
                    return Ok(msg);
                }
            }
            read = upper;
        }
    }
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("timeout of `{0:?}` exceeded while reading from stream")]
    Timeout(Duration),
    #[error("{0}")]
    TcpStream(#[from] std::io::Error),
    #[error("could not parse {found:?} as {expected}")]
    Parsing { expected: String, found: String },
    #[error("protocol violation: {0}")]
    Violation(String),
    #[error("EOF of stream; connection probably closed by client")]
    Closed,
    #[error("Not connected to server")]
    NotConnected,
}

pub struct ListCodec {
    delimiter: &'static str,
}

impl Default for ListCodec {
    fn default() -> Self {
        Self { delimiter: ";" }
    }
}

impl ListCodec {
    pub fn new(delimiter: &'static str) -> Self {
        Self { delimiter }
    }
    pub fn encode<'a>(
        &self,
        entries: impl IntoIterator<Item = &'a (impl ToString + 'a + ?Sized)>,
    ) -> String {
        entries
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(self.delimiter)
    }
    pub fn decode<T: FromStr>(&self, msg: &str) -> Result<Vec<T>> {
        msg.split(self.delimiter)
            .enumerate()
            .map(|(_i, s)| {
                s.parse().map_err(|_e| {
                    anyhow::anyhow!("Could not parse '{}' as {}", s, std::any::type_name::<T>())
                })
            })
            .collect()
    }
}
