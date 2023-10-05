//! This is the actual App.

use crate::connection::{ConnectionPool, ConnectionState};
use anyhow::Result;
use shared::key::{Key, RangeServerInfo, RingPos};
use shared::protocol::DeleteSuccessResponse;
use shared::protocol::GetSuccessResponse;
use shared::protocol::InnerSuccessResponse;
use shared::protocol::PutSuccessResponse;
use shared::protocol::Response;
use shared::protocol::{DeleteRequest, GetRequest, ProtocolError, PutRequest, Request};
use shared::value::{Value, VersionVec};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::info;
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Client {
    /// used for get the metadata of the system
    touch_point: ConnectionState,
    connections: ConnectionPool,
    /// keyring
    metadata: Option<Vec<RangeServerInfo>>,
    /// the contexts (version vectors) for each key
    contexts: HashMap<String, VersionVec>,
}

impl Client {
    pub async fn with_touchpoint(touch_point: SocketAddr) -> Result<Self, ProtocolError> {
        let client = Self {
            touch_point: ConnectionState::new(touch_point).await?,
            connections: ConnectionPool::new(),
            metadata: None,
            contexts: HashMap::new(),
        };
        Ok(client)
    }
    pub async fn connect(&mut self, addr: &SocketAddr) -> Result<(), ProtocolError> {
        let requested_conn = ConnectionState::new(*addr).await?;
        self.touch_point = requested_conn;
        self.metadata = None; // clear metadata
        Ok(())
    }
    pub fn is_connected(&self) -> Option<SocketAddr> {
        self.touch_point.is_connected()
    }
    pub async fn disconnect(&mut self) -> Result<(), ProtocolError> {
        self.touch_point.disconnect().await?;
        self.metadata = None; // clear metadata
        Ok(())
    }
    fn resolve_context(&self, key: &String, context: Option<VersionVec>) -> VersionVec {
        context
            .or_else(|| self.contexts.get(key).cloned())
            .unwrap_or_default()
    }

    async fn ensure_metadata(&mut self) -> Result<(), ProtocolError> {
        match self.metadata {
            Some(_) => Ok(()),
            None => {
                let request = Request::KeyRange;
                self.touch_point.send(request).await?;
                let response: Response = self.touch_point.receive().await?.into_inner();
                match response {
                    Response::KeyRangeSuccess(r) => {
                        self.metadata = Some(r.server_ranges);
                        Ok(())
                    }
                    _ => unreachable!("should receive KeyRangeSuccess"),
                }
            }
        }
    }

    fn get_server_addr(&self, key: &Key) -> SocketAddr {
        for server in self.metadata.as_ref().unwrap() {
            if server.range.contains(&RingPos::from_key(key)) {
                println!("server: {:?}", server);
                return server.client_addr()
            }
        }
        panic!("no server found for key: {}", key)
    }

    #[instrument(
        name = "put",
        skip(self),
        fields(
            key = %key,
            context = ?context,
            value = %value
        ),
    )]
    pub async fn put(
        &mut self,
        key: String,
        context: Option<VersionVec>,
        value: String,
    ) -> Result<Response, ProtocolError> {
        self.ensure_metadata().await?;
        let request_key = Key::from(key.clone());
        let server_addr = self.get_server_addr(&request_key);
        let request = Request::Put(PutRequest {
            key: request_key,
            context: self.resolve_context(&key, context),
            value: Value::from(value),
        });
        let connection = self.connections.get_connection(server_addr).await?;

        info!(request = %request, "sending");
        connection.send(request).await?;
        let response = connection.receive().await?.into_inner();
        info!(response = %response, "received");
        self.update_from_response(&key, &response);
        Ok(response)
    }
    #[instrument(
        name = "delete",
        skip(self),
        fields(
            key = %key,
            context = ?context,
        ),
    )]
    pub async fn delete(
        &mut self,
        key: String,
        context: Option<VersionVec>,
    ) -> Result<Response, ProtocolError> {
        self.ensure_metadata().await?;
        let request_key = Key::from(key.clone());
        let server_addr = self.get_server_addr(&request_key);
        let request = Request::Delete(DeleteRequest {
            key: request_key,
            context: self.resolve_context(&key, context),
        });
        info!(request = %request, "sending");
        let connection = self.connections.get_connection(server_addr).await?;
        connection.send(request).await?;
        let response = connection.receive().await?.into_inner();
        info!(response = %response, "received");
        self.update_from_response(&key, &response);
        Ok(response)
    }
    #[instrument(
        name = "get",
        skip(self),
        fields(
            key = %key,
        ),
    )]
    pub async fn get(&mut self, key: String) -> Result<Response, ProtocolError> {
        self.ensure_metadata().await?;
        let request_key = Key::from(key.clone());
        let server_addr = self.get_server_addr(&request_key);
        let request = Request::Get(GetRequest {
            key: request_key,
        });
        info!(request = %request, "sending");
        let connection = self.connections.get_connection(server_addr).await?;
        connection.send(request).await?;
        let response = connection.receive().await?.into_inner();
        info!(response = %response, "received");
        self.update_from_response(&key, &response);
        Ok(response)
    }
    pub async fn keyrange(&mut self) -> Result<Response, ProtocolError> {
        let request = Request::KeyRange;
        self.touch_point.send(request).await?;
        let response = self.touch_point.receive().await?.into_inner();
        Ok(response)
    }
    fn update_from_response(&mut self, key: &String, response: &Response) {
        match response {
            Response::GetSuccess(GetSuccessResponse(InnerSuccessResponse { context, .. }))
            | Response::DeleteSuccess(DeleteSuccessResponse(InnerSuccessResponse {
                context,
                ..
            }))
            | Response::PutSuccess(PutSuccessResponse(InnerSuccessResponse { context, .. })) => {
                self.contexts.insert(key.clone(), context.clone());
            }
            _ => {}
        }
    }
}
