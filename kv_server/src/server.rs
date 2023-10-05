use crate::replication::ReplicationManager;
use crate::server_state::SharedServerState;
use crate::ProcessSignal;
use crate::{cli::Args, connection::Connection, store::Request};
use anyhow::{Context, Result};
use protocol::membership_kv::membership_kv_client::MembershipKvClient;
use protocol::membership_kv::RegisterRequest;
use shared::key::{MetaData, RingPos};
use shared::retry::{ExponentialBackoff, Retry};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use tracing::{debug, info, instrument, trace, warn};

pub struct Server {
    args: Arc<Args>,
    server_id: RingPos,
    local_addr: SocketAddr,
    state: SharedServerState,
    shutdown_tx: Option<broadcast::Sender<()>>,
    shutdown_rx: broadcast::Receiver<()>,
    signal_rx: broadcast::Receiver<ProcessSignal>,
    store_tx: mpsc::Sender<Request>,
    replication_manager: ReplicationManager,
}

impl Server {
    pub fn with_args(
        args: Arc<Args>,
        server_id: RingPos,
        state: SharedServerState,
        shutdown_ch: (broadcast::Sender<()>, broadcast::Receiver<()>),
        signal_rx: broadcast::Receiver<ProcessSignal>,
        store_tx: mpsc::Sender<Request>,
        replication_manager: ReplicationManager,
    ) -> Self {
        Self {
            local_addr: args.socket_addr(),
            server_id,
            args,
            state,
            shutdown_tx: Some(shutdown_ch.0),
            shutdown_rx: shutdown_ch.1,
            signal_rx,
            store_tx,
            replication_manager,
        }
    }
    #[instrument(
        name = "Server::run",
        skip(self),
        fields(
            listening_addr = %self.local_addr,
            server_id = %self.server_id
        ),
    )]
    pub async fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.local_addr).await?;
        let listening_addr = listener.local_addr()?;
        // just for subscribing to shutdown signal for new clients
        let shutdown_tx = self.shutdown_tx.as_ref().expect("unreachable").clone();

        let state = Arc::clone(&self.state);
        let store_tx = self.store_tx.clone();
        let args = Arc::clone(&self.args);
        let register_factory = || async {
            Self::register(args.clone(), state.clone(), store_tx.clone())
                .await
                .context("Registering with membership service")
        };
        let sleep_interrupt_factory = || async {
            let _ = shutdown_tx.subscribe().recv().await;
        };

        let retry: Retry<ExponentialBackoff> = Retry::default();
        let register = retry.with_sleep_interrupt(&register_factory, &sleep_interrupt_factory);

        let event_loop = async {
            loop {
                debug!("Waiting for incoming connections on {} ...", listening_addr);
                tokio::select! {
                    _ = self.shutdown_rx.recv() => {
                        info!("Shutting down server");
                        break;
                    },
                    signal = self.signal_rx.recv() => {
                        match signal {
                            Ok(signal) => {
                                self.handle_process_signal(signal).await;
                            },
                            Err(e) => {
                                info!("Error receiving signal: {}", e);
                            }
                        }
                    },
                    result = listener.accept() => {
                        match result {
                            Ok((socket, peer_addr)) => {
                                self.handle_new_client(socket, peer_addr, shutdown_tx.subscribe()).await;
                            }
                            Err(e) => {
                                info!("Error accepting connection: {}", e);
                            }
                        }
                    }
                };
            }
        };

        tokio::join!(register, event_loop);

        Ok(())
    }
    async fn handle_new_client(
        &mut self,
        socket: tokio::net::TcpStream,
        peer_addr: SocketAddr,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> () {
        // here we spawn one task per client
        let mut connection = Connection::new(
            self.server_id,
            Arc::clone(&self.state),
            self.local_addr,
            peer_addr,
            socket,
            shutdown_rx,
            self.store_tx.clone(),
            self.replication_manager,
            self.args.kv_kv_addr().port(),
        );
        let _handle = tokio::spawn(async move {
            connection.handle().await.unwrap_or_else(|e| {
                warn!("Connection {} error: {}", peer_addr, e);
            });
        });
    }
    async fn handle_process_signal(&mut self, command: ProcessSignal) -> () {
        match command {
            ProcessSignal::CtrlCPressed => {
                debug!("Server received ctrl-c signal, initiating shutdown");
                match self.shutdown_tx.take() {
                    None => {
                        trace!("Shutdown already in progress");
                    }
                    Some(shutdown_tx) => {
                        let args = Arc::clone(&self.args);
                        let state = Arc::clone(&self.state);
                        let store_tx = self.store_tx.clone();
                        let shutdown_tx_clone = shutdown_tx.clone();
                        let _handle = tokio::spawn(async move {
                            let retry: Retry<ExponentialBackoff> = Retry::default();
                            let shutdown_factory = || async {
                                Self::shutdown(
                                    Arc::clone(&args),
                                    Arc::clone(&state),
                                    store_tx.clone(),
                                    shutdown_tx_clone.clone(),
                                )
                                .await
                                .context("Shutting down server")
                            };
                            let shutdown = retry.with_max_attempts(&shutdown_factory, 2);
                            if let Err(e) = shutdown.await {
                                warn!(
                                    "Error shutting down server, doing dirty shutdown instead: {:?}",
                                    e
                                );
                                shutdown_tx.send(()).expect("Shutdown channel closed");
                            }
                        });
                    }
                }
            }
        }
    }

    pub async fn register(
        args: Arc<Args>,
        state: SharedServerState,
        _store_tx: mpsc::Sender<Request>,
    ) -> Result<()> {
        let addr = args.socket_addr();
        let peer = args
            .peer_addr()
            .and_then(|addr| Some(addr.to_string()))
            .unwrap_or("".to_string());
        let membership_kv_addr = args.membership_kv_addr();

        info!(
            "Contacting membership service at {} to register {}",
            membership_kv_addr, addr
        );
        let mut client =
            MembershipKvClient::connect(format!("http://{}", membership_kv_addr.to_string()))
                .await?;
        let request = RegisterRequest {
            socket_addr: addr.to_string(),
            peer_addr: peer,
        };
        let response = client.register(request).await?;
        let metadata = MetaData::try_from(response.into_inner().updated_network_topology)?;
        state.write().await.init_serving(metadata)?;
        info!("Successfully registered with membership service");
        Ok(())
    }

    pub async fn shutdown(
        _args: Arc<Args>,
        _state: SharedServerState,
        _store_tx: mpsc::Sender<Request>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        info!("Shutting down server");
        // finally, blow the horn to inform all tasks to shutdown
        shutdown_tx.send(()).expect("Shutdown channel closed");
        Ok(())
    }
}
