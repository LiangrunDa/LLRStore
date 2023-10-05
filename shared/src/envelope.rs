/// This module comes in handy for communication via channels between an mpsc
/// sender and an oneshot receiver. The sender sends a request via an mpsc channel
/// and awaits the response. The receiver receives the request, processes it
/// and sends the response back to the sender via the oneshot channel.
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

// TODO: use with store

#[derive(Debug)]
pub struct Envelope<Request, Response> {
    pub request: Request,
    pub reply_to: oneshot::Sender<Response>,
}

impl<Request, Response> Envelope<Request, Response> {
    pub async fn send(
        request: Request,
        to: &mpsc::Sender<Self>,
    ) -> Result<Response, EnvelopeError<Request, Response>> {
        let (tx, rx) = oneshot::channel();
        let envelope = Self {
            request,
            reply_to: tx,
        };
        to.send(envelope).await?;
        Ok(rx.await?)
    }
}

#[derive(Error, Debug)]
pub enum EnvelopeError<Request, Response> {
    #[error("Send error")]
    SendError(#[from] mpsc::error::SendError<Envelope<Request, Response>>),
    #[error("Receive error")]
    RecvError(#[from] oneshot::error::RecvError),
}
