use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;
use memberlist::{Config, MemberList};
use protocol::kv_membership::kv_membership_client::KvMembershipClient;
use protocol::kv_membership::UpdateTopologyRequest;
use shared::key::MetaData;
use crate::cli::Args;

pub(crate) struct MembershipMetadata {
    pub(crate) members: MemberList,
    pub(crate) meta: MetaData,
    pub(crate) kv_addr: SocketAddr,
}

pub(crate) struct CustomInfo {
    addr_for_client: SocketAddr,
}

impl From<Vec<u8>> for CustomInfo {
    fn from(bytes: Vec<u8>) -> Self {
        let addr_for_client = SocketAddr::from_str(&String::from_utf8(bytes).unwrap()).unwrap();
        Self { addr_for_client }
    }
}

impl From<CustomInfo> for Vec<u8> {
    fn from(custom_info: CustomInfo) -> Self {
        custom_info.addr_for_client.to_string().into_bytes()
    }
}

impl MembershipMetadata {

    pub(crate) fn new(args: Arc<Args>, self_addr: SocketAddr) -> Self {
        let custom_info = CustomInfo{addr_for_client: self_addr.clone()};
        let config = Config::new(Some(args.name().to_string()), args.socket_addr().ip(), args.gossip_port(), Some(custom_info.into()));
        let members = MemberList::new_with_config(config);
        let mut meta = MetaData::new();
        meta.add_server(self_addr).expect("Failed to add server");
        Self { members, meta, kv_addr: args.kv_addr() }
    }

    pub(crate) async fn start(&mut self, peer_addr: Option<SocketAddr>) {
        self.members.start();
        match peer_addr {
            Some(peer_addr) => self.join(peer_addr).await,
            None => {
                info!("Starting as the first node in the cluster");
            }
        }
        self.start_membership().await;
    }

    async fn join(&mut self, peer_addr: SocketAddr) {
        self.members.async_join(peer_addr).await.expect("Failed to join cluster");
    }

    async fn send_metadata_update(&self, addr: SocketAddr, new_topology: String) {
        let mut client =
            KvMembershipClient::connect(format!("http://{}:{}", addr.ip(), addr.port()))
                .await
                .unwrap();
        client.update_topology(UpdateTopologyRequest {
            updated_network_topology: new_topology,
        }).await.unwrap();
    }

    // TODO: implement delegate for memberlist. Whenever a node joins or leaves, the function will be called.
    async fn start_membership(&mut self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut changed = false;
            let members = self.members.async_get_nodes().await.expect("Failed to get nodes");
            for member in members {
                let custom_info: CustomInfo = member.custom_info.clone().expect("should have custom info").into();
                let server = SocketAddr::new(custom_info.addr_for_client.ip(), custom_info.addr_for_client.port());
                if member.is_alive() {
                    match self.meta.upsert_server(server) {
                        Ok(c) => {
                            if c {
                                info!("Server {} is up", server);
                                changed = c;
                            }
                        },
                        Err(_) => {}
                    }
                } else {
                    match self.meta.remove_server(server) {
                        Ok(_) => {
                            changed = true;
                            info!("Server {} is down", server)
                        },
                        Err(_) => {}
                    }
                }
            }
            if changed {
                let new_topology = self.meta.clone().into();
                self.send_metadata_update(self.kv_addr, new_topology).await;
            }
        }
    }
}