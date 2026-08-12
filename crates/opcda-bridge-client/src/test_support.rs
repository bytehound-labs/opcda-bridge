//! Shared test doubles for the `cli` and `commands` unit test modules.
//!
//! Both modules exercise their code against a real gRPC server backed by a
//! configurable mock `Bridge` implementation, rather than mocking the
//! generated client directly. Keeping a single mock here avoids the two
//! modules drifting into slightly different (and easy to forget to keep in
//! sync) copies of the same scaffolding.

use opcda_bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
use opcda_bridge_proto::bridge::{
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
/// Each field controls the response for its corresponding RPC; fields left
/// at their default produce the proto message's zero value (e.g. an empty
/// server list or an empty tag stream).
#[derive(Default)]
pub(crate) struct MockBridgeService {
    pub(crate) list_servers_response: ListServersResponse,
    pub(crate) browse_responses: Vec<BrowseResponse>,
    pub(crate) read_response: ReadResponse,
    pub(crate) write_response: WriteResponse,
}

#[tonic::async_trait]
impl Bridge for MockBridgeService {
    async fn list_servers(
        &self,
        _request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        Ok(Response::new(self.list_servers_response.clone()))
    }

    type BrowseStream = ReceiverStream<Result<BrowseResponse, Status>>;

    async fn browse(
        &self,
        _request: Request<BrowseRequest>,
    ) -> Result<Response<Self::BrowseStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let items = self.browse_responses.clone();
        tokio::spawn(async move {
            for item in items {
                if tx.send(Ok(item)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read(&self, _request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        Ok(Response::new(self.read_response.clone()))
    }

    async fn write(
        &self,
        _request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        Ok(Response::new(self.write_response.clone()))
    }
}

/// Starts `service` on an ephemeral localhost port and returns its
/// `host:port` address, ready to be passed to `BridgeClient::connect`.
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
