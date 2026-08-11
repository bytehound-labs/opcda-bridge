//! Shared test doubles for this crate's own unit tests: a configurable mock
//! `Bridge` gRPC service plus a helper to run it on an ephemeral localhost
//! port.
//!
//! This mirrors the pattern `client/src/test_support.rs` established in the
//! `opcda-bridge-client` crate, but is kept as this crate's own copy rather
//! than a shared dependency: `bridge-client-core` must be testable
//! standalone (it has no dependency on, and must not gain a dev-dependency
//! on, the `client` crate that in turn depends on it), and this crate's
//! tests additionally need to force RPC-level errors (via the `*_error`
//! fields below) to exercise [`crate::Error::Rpc`], which the CLI's mock
//! never needed since its tests only cover successful RPCs end-to-end.

use bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
use bridge_proto::bridge::{
    BrowseRequest, BrowseResponse, ListServersRequest, ListServersResponse, ReadRequest,
    ReadResponse, WriteRequest, WriteResponse,
};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// A configurable mock `Bridge` gRPC service.
///
/// Each `*_response` field controls the successful response for its
/// corresponding RPC, defaulting to the proto message's zero value. Setting
/// the matching `*_error` field instead makes that RPC return the given
/// `Status` (checked before the success response). `browse_stream_error`,
/// if set, is sent as one final stream item after every item in
/// `browse_responses`, to exercise a `Browse` response stream that starts
/// successfully but fails partway through.
#[derive(Default)]
pub(crate) struct MockBridgeService {
    pub(crate) list_servers_response: ListServersResponse,
    pub(crate) list_servers_error: Option<Status>,
    pub(crate) browse_responses: Vec<BrowseResponse>,
    pub(crate) browse_initial_error: Option<Status>,
    pub(crate) browse_stream_error: Option<Status>,
    pub(crate) read_response: ReadResponse,
    pub(crate) read_error: Option<Status>,
    pub(crate) write_response: WriteResponse,
    pub(crate) write_error: Option<Status>,
}

#[tonic::async_trait]
impl Bridge for MockBridgeService {
    async fn list_servers(
        &self,
        _request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        if let Some(status) = self.list_servers_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.list_servers_response.clone()))
    }

    type BrowseStream = ReceiverStream<Result<BrowseResponse, Status>>;

    async fn browse(
        &self,
        _request: Request<BrowseRequest>,
    ) -> Result<Response<Self::BrowseStream>, Status> {
        if let Some(status) = self.browse_initial_error.clone() {
            return Err(status);
        }
        let (tx, rx) = mpsc::channel(4);
        let items = self.browse_responses.clone();
        let stream_error = self.browse_stream_error.clone();
        tokio::spawn(async move {
            for item in items {
                if tx.send(Ok(item)).await.is_err() {
                    break;
                }
            }
            if let Some(status) = stream_error {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read(&self, _request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        if let Some(status) = self.read_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.read_response.clone()))
    }

    async fn write(
        &self,
        _request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        if let Some(status) = self.write_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.write_response.clone()))
    }
}

/// Starts `service` on an ephemeral localhost port and returns its
/// `host:port` address, ready to be passed to [`crate::Client::connect`].
pub(crate) async fn start_mock_server(service: MockBridgeService) -> String {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        Server::builder()
            .add_service(BridgeServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("127.0.0.1:{port}")
}
