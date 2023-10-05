//! This module contains code for the REPL (Read-Eval-Print-Loop) of the
//! cli. It is responsible for reading _and parsing_ user input, evaluating it,
//! and printing the result.

use crate::cli::to_ipv4_socket_addr;
use crate::client::Client;
use shared::logger::{LogLevel, Logger};
use shared::value::VersionVec;
use std::io::Write;
use tracing::trace;

pub struct Repl {
    quit_requested: bool,
    logger: Logger,
    last_input: String,
    client: Client,
}

impl Repl {
    const PROMPT: &'static str = "Gr16Client>";
    const HELP_MSG: &'static str = r#"Available commands:
help: print this help message
quit: quit the program
connect <address> <port>: connect to the server at <address> <port>
disconnect: disconnect from the server
put <key> <context> <val>: put <key> <val> into the KV store with <context> being optional
delete <key> <context>: delete <key> from the KV store with <context> being optional
get <key>: get the value for <key> from the KV store
logLevel <level>: set the log level to <level>"#;

    pub fn new(logger: Logger, client: Client) -> Self {
        Self {
            quit_requested: false,
            logger,
            last_input: String::with_capacity(30),
            client,
        }
    }
    pub async fn run(&mut self) {
        trace!("REPL running and awaiting user input");
        if let Some(addr) = self.client.is_connected() {
            self.print(&format!("Connected to {}", addr));
        }
        loop {
            if self.quit_requested {
                break;
            }
            self.read_line();

            let feedback: String = match self.parse_line().await {
                Ok(response) => response,
                Err(reason) => reason,
            };

            self.print(&feedback);
        }
        trace!("REPL stopped");
    }
    fn read_line(&mut self) {
        std::io::stdout().flush().unwrap();
        self.last_input.clear();
        std::io::stdin()
            .read_line(&mut self.last_input)
            .expect("Failed to read line");
    }
    async fn parse_line(&mut self) -> Result<String, String> {
        let trimmed = self.last_input.trim_start();
        let mut tokens = trimmed.split_whitespace();
        let command = tokens.next();
        match command.ok_or("Command missing, type 'help' for usage information")? {
            "help" => Ok(Self::HELP_MSG.to_string()),
            "quit" => {
                self.quit_requested = true;
                Ok("Quitting...".to_string())
            }
            "logLevel" => tokens
                .next()
                .and_then(|level| LogLevel::try_from(level).ok())
                .map(|level| {
                    let old = self.logger.set_log_level(level);
                    format!("Log level set from {} to {}", old, level)
                })
                .ok_or(format!("Could not parse argument as a valid log level")),
            "connect" => {
                let addr = tokens
                    .next()
                    .and_then(|addr| {
                        tokens
                            .next()
                            .and_then(|port| to_ipv4_socket_addr(addr, port).ok())
                    })
                    .ok_or(format!(
                        "Could not parse arguments to an ip address and a port"
                    ))?;
                self.client
                    .connect(&addr)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(format!("Connected to {}", addr))
            }
            "disconnect" => {
                self.client.disconnect().await.map_err(|e| e.to_string())?;
                Ok("Disconnected".to_string())
            }
            "get" => {
                let key = tokens.next().ok_or(format!("Key argument is missing"))?;
                let response = self
                    .client
                    .get(key.to_string())
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(response.to_string())
            }
            "put" => {
                let args = tokens.collect::<Vec<&str>>();
                match args.len() {
                    2 => {
                        let key = args[0];
                        let value = args[1];
                        let response = self
                            .client
                            .put(key.to_string(), None, value.to_string())
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok(response.to_string())
                    }
                    3 => {
                        let key = args[0];
                        let context = args[1].parse::<VersionVec>().map_err(|e| e.to_string())?;
                        let value = args[2];
                        let response = self
                            .client
                            .put(key.to_string(), Some(context), value.to_string())
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok(response.to_string())
                    }
                    _ => Err(format!("Wrong number of arguments for put command")),
                }
            }
            "delete" => {
                let args = tokens.collect::<Vec<&str>>();
                match args.len() {
                    1 => {
                        let key = args[0];
                        let response = self
                            .client
                            .delete(key.to_string(), None)
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok(response.to_string())
                    }
                    2 => {
                        let key = args[0];
                        let context = args[1].parse::<VersionVec>().map_err(|e| e.to_string())?;
                        let response = self
                            .client
                            .delete(key.to_string(), Some(context))
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok(response.to_string())
                    }
                    _ => Err(format!("Wrong number of arguments for delete command")),
                }
            }
            "keyrange" => {
                let response = self.client.keyrange().await.map_err(|e| e.to_string())?;
                Ok(response.to_string())
            }
            _cmd => Ok(Self::HELP_MSG.to_string()),
        }
    }
    fn print(&self, msg: &str) {
        println!("{} {}", Self::PROMPT, msg);
    }
}
