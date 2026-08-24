//! Configurable in-process gRPC service used by the client-library tests.

use opcda_bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
use opcda_bridge_proto::bridge::{
    BrowsePage, BrowseRequest, CloseBrowseSessionRequest, ControlSearchIndexRequest,
    GetCapabilitiesRequest, GetCapabilitiesResponse, GetGatewayInfoRequest, GetGatewayInfoResponse,
    GetSearchIndexStatusRequest, ListServersRequest, ListServersResponse, ReadRequest,
    ReadResponse, RefreshSearchIndexRequest, SearchEvent, SearchIndexRequest, SearchIndexResponse,
    SearchIndexStatus, SearchRequest, WriteRequest, WriteResponse,
};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub(crate) struct MockBridgeService {
    pub(crate) gateway_info_response: GetGatewayInfoResponse,
    pub(crate) gateway_info_error: Option<Status>,
    pub(crate) gateway_info_requests: Arc<Mutex<Vec<GetGatewayInfoRequest>>>,
    pub(crate) capabilities_response: GetCapabilitiesResponse,
    pub(crate) capabilities_error: Option<Status>,
    pub(crate) capabilities_requests: Arc<Mutex<Vec<GetCapabilitiesRequest>>>,
    pub(crate) list_servers_response: ListServersResponse,
    pub(crate) list_servers_error: Option<Status>,
    pub(crate) browse_response: BrowsePage,
    pub(crate) browse_error: Option<Status>,
    pub(crate) browse_requests: Arc<Mutex<Vec<BrowseRequest>>>,
    pub(crate) close_error: Option<Status>,
    pub(crate) close_requests: Arc<Mutex<Vec<CloseBrowseSessionRequest>>>,
    pub(crate) search_events: Vec<SearchEvent>,
    pub(crate) search_initial_error: Option<Status>,
    pub(crate) search_stream_error: Option<Status>,
    pub(crate) search_requests: Arc<Mutex<Vec<SearchRequest>>>,
    pub(crate) search_index_status_response: SearchIndexStatus,
    pub(crate) search_index_status_error: Option<Status>,
    pub(crate) search_index_status_requests: Arc<Mutex<Vec<GetSearchIndexStatusRequest>>>,
    pub(crate) refresh_search_index_response: SearchIndexStatus,
    pub(crate) refresh_search_index_error: Option<Status>,
    pub(crate) refresh_search_index_requests: Arc<Mutex<Vec<RefreshSearchIndexRequest>>>,
    pub(crate) control_search_index_response: SearchIndexStatus,
    pub(crate) control_search_index_error: Option<Status>,
    pub(crate) control_search_index_requests: Arc<Mutex<Vec<ControlSearchIndexRequest>>>,
    pub(crate) search_index_response: SearchIndexResponse,
    pub(crate) search_index_error: Option<Status>,
    pub(crate) search_index_requests: Arc<Mutex<Vec<SearchIndexRequest>>>,
    pub(crate) read_response: ReadResponse,
    pub(crate) read_error: Option<Status>,
    pub(crate) write_response: WriteResponse,
    pub(crate) write_error: Option<Status>,
    pub(crate) server_shutdown: Arc<Notify>,
    pub(crate) server_stopped: Arc<Notify>,
}

impl Default for MockBridgeService {
    fn default() -> Self {
        Self {
            gateway_info_response: GetGatewayInfoResponse::default(),
            gateway_info_error: None,
            gateway_info_requests: Arc::default(),
            capabilities_response: GetCapabilitiesResponse::default(),
            capabilities_error: None,
            capabilities_requests: Arc::default(),
            list_servers_response: ListServersResponse::default(),
            list_servers_error: None,
            browse_response: BrowsePage {
                complete: true,
                ..Default::default()
            },
            browse_error: None,
            browse_requests: Arc::default(),
            close_error: None,
            close_requests: Arc::default(),
            search_events: Vec::new(),
            search_initial_error: None,
            search_stream_error: None,
            search_requests: Arc::default(),
            search_index_status_response: SearchIndexStatus::default(),
            search_index_status_error: None,
            search_index_status_requests: Arc::default(),
            refresh_search_index_response: SearchIndexStatus::default(),
            refresh_search_index_error: None,
            refresh_search_index_requests: Arc::default(),
            control_search_index_response: SearchIndexStatus::default(),
            control_search_index_error: None,
            control_search_index_requests: Arc::default(),
            search_index_response: SearchIndexResponse {
                status: Some(SearchIndexStatus::default()),
                ..Default::default()
            },
            search_index_error: None,
            search_index_requests: Arc::default(),
            read_response: ReadResponse::default(),
            read_error: None,
            write_response: WriteResponse::default(),
            write_error: None,
            server_shutdown: Arc::default(),
            server_stopped: Arc::default(),
        }
    }
}

#[tonic::async_trait]
impl Bridge for MockBridgeService {
    async fn get_gateway_info(
        &self,
        request: Request<GetGatewayInfoRequest>,
    ) -> Result<Response<GetGatewayInfoResponse>, Status> {
        self.gateway_info_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.gateway_info_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.gateway_info_response.clone()))
    }

    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        self.capabilities_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.capabilities_error.clone() {
            return Err(status);
        }
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
        if let Some(status) = self.browse_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.browse_response.clone()))
    }

    async fn close_browse_session(
        &self,
        request: Request<CloseBrowseSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.close_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.close_error.clone() {
            return Err(status);
        }
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
        if let Some(status) = self.search_initial_error.clone() {
            return Err(status);
        }
        let (tx, rx) = mpsc::channel(4);
        let events = self.search_events.clone();
        let stream_error = self.search_stream_error.clone();
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(Ok(event)).await;
            }
            if let Some(status) = stream_error {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_servers(
        &self,
        _request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        if let Some(status) = self.list_servers_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.list_servers_response.clone()))
    }

    async fn get_search_index_status(
        &self,
        request: Request<GetSearchIndexStatusRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        self.search_index_status_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.search_index_status_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.search_index_status_response.clone()))
    }

    async fn refresh_search_index(
        &self,
        request: Request<RefreshSearchIndexRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        self.refresh_search_index_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.refresh_search_index_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.refresh_search_index_response.clone()))
    }

    async fn control_search_index(
        &self,
        request: Request<ControlSearchIndexRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        self.control_search_index_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.control_search_index_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.control_search_index_response.clone()))
    }

    async fn search_index(
        &self,
        request: Request<SearchIndexRequest>,
    ) -> Result<Response<SearchIndexResponse>, Status> {
        self.search_index_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        if let Some(status) = self.search_index_error.clone() {
            return Err(status);
        }
        Ok(Response::new(self.search_index_response.clone()))
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
