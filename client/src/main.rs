#![allow(dead_code)]
mod cli;
mod client;
mod connection;
mod repl;
use client::Client;
use repl::Repl;
use shared::logger::Logger;
use tracing::trace;

fn main() {
    let args = cli::parse_args();

    let logger = Logger::init(args.log_level(), args.log_dir(), args.log_file())
        .expect("Could not initialize logger");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Could not load tokio runtime")
        .block_on(async {
            let client = if let Some(addr) = args.server_addr() {
                Client::with_touchpoint(addr).await
            } else {
                async { Ok(Client::default()) }.await
            }
            .expect("Could not create client");

            let mut repl = Repl::new(logger, client);
            repl.run().await;
            trace!("Client stopped");
        })
}
