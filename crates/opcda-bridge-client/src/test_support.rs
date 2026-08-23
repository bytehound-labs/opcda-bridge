//! In-process gRPC service shared by CLI unit tests.

use opcda_bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
use opcda_bridge_proto::bridge::{
    BrowsePage, BrowseRequest, CloseBrowseSessionRequest, GetCapabilitiesRequest,
    GetCapabilitiesResponse, ListServersRequest, ListServersResponse, ReadRequest, ReadResponse,
    SearchEvent, SearchRequest, WriteRequest, WriteResponse,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub(crate) struct MockBridgeService {
    pub(crate) capabilities_response: GetCapabilitiesResponse,
    pub(crate) list_servers_response: ListServersResponse,
    pub(crate) browse_responses: Vec<BrowsePage>,
    pub(crate) browse_requests: Arc<Mutex<Vec<BrowseRequest>>>,
    pub(crate) browse_calls: AtomicUsize,
    pub(crate) close_requests: Arc<Mutex<Vec<CloseBrowseSessionRequest>>>,
    pub(crate) search_events: Vec<SearchEvent>,
    pub(crate) search_requests: Arc<Mutex<Vec<SearchRequest>>>,
    pub(crate) read_response: ReadResponse,
    pub(crate) write_response: WriteResponse,
    pub(crate) server_shutdown: Arc<Notify>,
    pub(crate) server_stopped: Arc<Notify>,
}

impl Default for MockBridgeService {
    fn default() -> Self {
        Self {
            capabilities_response: GetCapabilitiesResponse::default(),
            list_servers_response: ListServersResponse::default(),
            browse_responses: vec![BrowsePage {
                complete: true,
                ..Default::default()
            }],
            browse_requests: Arc::default(),
            browse_calls: AtomicUsize::new(0),
            close_requests: Arc::default(),
            search_events: Vec::new(),
            search_requests: Arc::default(),
            read_response: ReadResponse::default(),
            write_response: WriteResponse::default(),
            server_shutdown: Arc::default(),
            server_stopped: Arc::default(),
        }
    }
}

#[tonic::async_trait]
impl Bridge for MockBridgeService {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        Ok(Response::new(self.capabilities_response.clone()))
    }

    async fn browse(
        &self,
        request: Request<BrowseRequest>,
    ) -> Result<Response<BrowsePage>, Status> {
        self.browse_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        let index = self.browse_calls.fetch_add(1, Ordering::Relaxed);
        let last = self.browse_responses.len().saturating_sub(1);
        let page = self.browse_responses[index.min(last)].clone();
        Ok(Response::new(page))
    }

    async fn close_browse_session(
        &self,
        request: Request<CloseBrowseSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.close_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        Ok(Response::new(()))
    }

    type SearchStream = ReceiverStream<Result<SearchEvent, Status>>;

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        self.search_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        let (tx, rx) = mpsc::channel(4);
        let events = self.search_events.clone();
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(Ok(event)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_servers(
        &self,
        _request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        Ok(Response::new(self.list_servers_response.clone()))
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

pub(crate) async fn start_mock_server(service: MockBridgeService) -> String {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_shutdown = Arc::clone(&service.server_shutdown);
    let server_stopped = Arc::clone(&service.server_stopped);
    tokio::spawn(async move {
        Server::builder()
            .add_service(BridgeServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                server_shutdown.notified(),
            )
            .await
            .unwrap();
        server_stopped.notify_one();
    });
    format!("127.0.0.1:{port}")
}
