use protocol::membership_kv::membership_kv_server::MembershipKv;
use protocol::membership_kv::{RegisterRequest, RegisterResponse};
use shared::key::MetaData;
use std::net::{SocketAddr};
use std::str::FromStr;
use tokio::sync::{mpsc};
use tonic::{Request, Response, Status};
use tracing::{info};

pub struct MembershipService {
    pub start_channel: mpsc::Sender<(SocketAddr, Option<SocketAddr>)>,
}

impl MembershipService {
    pub fn new(start_channel: mpsc::Sender<(SocketAddr, Option<SocketAddr>)>, ) -> Self {
        Self { start_channel }
    }
}

#[tonic::async_trait]
impl MembershipKv for MembershipService {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        info!(
            "Receive register from {}",
            request.remote_addr().unwrap().to_string()
        );
        let slf = request.get_ref().socket_addr.clone();
        let peer = request.get_ref().peer_addr.clone();
        let self_addr = SocketAddr::from_str(&slf).unwrap();
        let peer_addr = SocketAddr::from_str(&peer).ok();


        let mut single_server_meta = MetaData::new();
        single_server_meta.add_server(self_addr).expect("Failed to add server");
        self.start_channel.send((self_addr, peer_addr)).await.expect("Failed to send start channel");
        Ok(Response::new(RegisterResponse{updated_network_topology: single_server_meta.into()}))
    }
}