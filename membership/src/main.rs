mod cli;
mod server;
mod member_meta;

use shared::logger::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc};
use tracing::info;
use protocol::membership_kv::membership_kv_server::MembershipKvServer;
use crate::member_meta::MembershipMetadata;

fn main() {
    let args = Arc::new(cli::parse_args());

    let _logger = Logger::init(args.log_level(), args.log_dir(), args.log_file())
        .expect("Could not initialize logger");

    info!("Starting with args: {:?}", args);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Could not load tokio runtime")
        .block_on(async move {
            let address = args.socket_addr();
            let (start_channel, mut start_receiver) = mpsc::channel::<(SocketAddr, Option<SocketAddr>)>(1);
            let membership_service = server::MembershipService::new(
                start_channel
            );

            let membership_server = tonic::transport::Server::builder()
                .add_service(MembershipKvServer::new(membership_service))
                .serve(address);

            tokio::spawn(async move {
                let (self_addr, peer_addr) = start_receiver.recv().await.expect("Failed to receive start channel");
                let mut membership_checker = MembershipMetadata::new(args, self_addr);
                membership_checker.start(peer_addr).await;
            });
            membership_server.await.unwrap();
        })
}
