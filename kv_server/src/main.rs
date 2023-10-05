#![allow(dead_code)]
mod cache;
mod cli;
mod connection;
mod kv_internal;
mod kv_membership;
mod replication;
mod server;
mod server_state;
mod storage;
mod store;

use crate::{
    cache::SupportedCache,
    kv_internal::KvKvServer,
    kv_membership::KvMembershipService,
    replication::ReplicationManager,
    server::Server,
    server_state::init_shared_state,
    storage::LogIndexStorage,
    store::{Request, Store},
};
use anyhow::Result;
use protocol::{
    kv_kv::kvkv_server::KvkvServer, kv_membership::kv_membership_server::KvMembershipServer,
};
use shared::{key::RingPos, logger::Logger};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tonic::transport::Server as TonicServer;
use tracing::{debug, error, info};

fn main() {
    let args = Arc::new(cli::parse_args().validate());

    let _logger = Logger::init(args.log_level(), args.log_dir(), args.log_file())
        .expect("Could not initialize logger");

    info!("Starting with args: {:?}", args);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Could not load tokio runtime")
        .block_on(async move {
            let signal_rx = setup_signal_handler().expect("Could not setup signal handler");

            let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

            let (store_tx, store_rx) = mpsc::channel::<Request>(100);

            let actor_id = RingPos::from_socket_addr(&args.socket_addr());
            let state = init_shared_state(args.socket_addr(), args.replication_factor());
            let replication_manager = ReplicationManager::new(
                args.replication_factor(),
                args.durability_param(),
                args.consistency_param(),
            );

            let mut kvkv_shutdown_rx = shutdown_tx.subscribe();
            let kvkv_server = TonicServer::builder()
                .add_service(KvkvServer::new(KvKvServer::new(
                    actor_id,
                    Arc::clone(&state),
                    store_tx.clone(),
                    replication_manager,
                    args.kv_kv_addr().port(),
                )))
                .serve_with_shutdown(args.kv_kv_addr(), async move {
                    let _ = kvkv_shutdown_rx.recv().await;
                    info!("Shutting down kvkv grpc server");
                });
            let _kvkv_server_handle = tokio::spawn(async move {
                kvkv_server.await.expect("Cannot launch kvkv grpc server");
            });

            let mut kvecs_shutdown_rx = shutdown_tx.subscribe();
            let kvecs_server = TonicServer::builder()
                .add_service(KvMembershipServer::new(KvMembershipService::new(
                    Arc::clone(&state),
                    args.socket_addr(),
                    store_tx.clone(),
                    args.kv_kv_addr().port(),
                )))
                .serve_with_shutdown(args.kv_membership_addr(), async move {
                    let _ = kvecs_shutdown_rx.recv().await;
                    info!("Shutting down kvecs grpc server");
                });
            let _kvecs_server_handle = tokio::spawn(async move {
                kvecs_server.await.expect("Cannot launch kvecs grpc server");
            });

            let args_clone = Arc::clone(&args);
            let _store_handle = tokio::task::spawn_blocking(move || {
                let args = args_clone;
                let storage =
                    LogIndexStorage::new(args.data_dir()).expect("Could not initialize storage");
                let cache = SupportedCache::new(args.cache_eviction_strategy(), args.cache_size());
                let mut store = Store::new(actor_id, cache, storage, store_rx);
                let res = store.run();
                if res.is_err() {
                    error!("Store error: {}", res.unwrap_err());
                } else {
                    debug!("Store stopped");
                }
            });

            let mut server = Server::with_args(
                args,
                actor_id,
                state,
                (shutdown_tx, shutdown_rx),
                signal_rx,
                store_tx,
                replication_manager,
            );
            let res = server.run().await;
            if res.is_err() {
                error!("Server error: {}", res.unwrap_err());
            } else {
                debug!("Server stopped");
            }
        });

    info!("KV server with all of its tasks stopped");
}

#[derive(Clone, Debug)]
pub enum ProcessSignal {
    CtrlCPressed,
}

fn setup_signal_handler() -> Result<broadcast::Receiver<ProcessSignal>> {
    // happens when ctrl-c is pressed
    let (tx, rx) = broadcast::channel::<ProcessSignal>(8);
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigquit = signal::unix::signal(signal::unix::SignalKind::quit())?;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    debug!("Received SIGINT");
                    let _ = tx.send(ProcessSignal::CtrlCPressed);
                }
                _ = sigterm.recv() => {
                    debug!("Received SIGTERM");
                    let _ = tx.send(ProcessSignal::CtrlCPressed);
                }
                _ = sigquit.recv() => {
                    debug!("Received SIGQUIT");
                    let _ = tx.send(ProcessSignal::CtrlCPressed);
                }
            }
        }
    });
    Ok(rx)
}
