use super::map::{
    gateway_info, index_error, internal, map_browse_page, map_capabilities, map_index_match,
    map_index_status, map_to_proto_tag_values, map_to_write_response, resolve_host,
    typed_value_to_opc_value,
};
use super::search::{run_search, validate_search};
use crate::browse::BrowseManager;
use crate::config::{GatewayConfig, resolve_index_config};
use crate::index::{IndexControlAction, IndexManager, SearchMode, normalize_query};
use crate::opc::OpcClient;
use opcda_bridge_proto::bridge::{
    BrowsePage as ProtoBrowsePage, CloseBrowseSessionRequest, ControlSearchIndexRequest,
    GetCapabilitiesRequest, GetCapabilitiesResponse, GetGatewayInfoRequest, GetGatewayInfoResponse,
    GetSearchIndexStatusRequest, ListServersRequest, ListServersResponse, ReadRequest,
    ReadResponse, RefreshSearchIndexRequest, SearchEvent, SearchIndexControlAction,
    SearchIndexRequest, SearchIndexResponse, SearchIndexStatus, SearchRequest, WriteRequest,
    WriteResponse, bridge_server::Bridge,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

pub struct BridgeService<C: OpcClient> {
    client: Arc<C>,
    browse: Arc<BrowseManager<C>>,
    index: Arc<IndexManager<C>>,
}

impl<C: OpcClient> Clone for BridgeService<C> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            browse: Arc::clone(&self.browse),
            index: Arc::clone(&self.index),
        }
    }
}

impl<C: OpcClient> BridgeService<C> {
    pub fn new(client: C) -> Self {
        Self::with_index_config(client, &GatewayConfig::default())
    }

    pub fn with_index_config(client: C, config: &GatewayConfig) -> Self {
        let client = Arc::new(client);
        Self {
            browse: Arc::new(BrowseManager::new(Arc::clone(&client))),
            index: Arc::new(IndexManager::new(
                Arc::clone(&client),
                resolve_index_config(&config.index),
            )),
            client,
        }
    }

    pub fn start_background_indexing(&self) {
        self.index.start_background_indexing();
    }

    pub async fn shutdown_background_indexing(&self) {
        self.index.shutdown_background_indexing().await;
    }
}

#[cfg(target_os = "windows")]
impl Default for BridgeService<crate::opc_da_adapter::OpcDaAdapter> {
    fn default() -> Self {
        Self::new(crate::opc_da_adapter::OpcDaAdapter::default())
    }
}

#[tonic::async_trait]
impl<C: OpcClient> Bridge for BridgeService<C> {
    #[tracing::instrument(skip(self, _request))]
    async fn get_gateway_info(
        &self,
        _request: Request<GetGatewayInfoRequest>,
    ) -> Result<Response<GetGatewayInfoResponse>, Status> {
        Ok(Response::new(gateway_info()))
    }

    #[tracing::instrument(skip(self, request))]
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        let _foreground = self.index.foreground_guard(&req.server);
        let started = std::time::Instant::now();
        let result = self.client.get_capabilities(&req.server).await;
        self.index.record_foreground_operation_with_health(
            &req.server,
            started.elapsed(),
            result.is_err(),
            false,
            result.is_err(),
        );
        let capabilities = result.map_err(internal)?;
        let index_status = self.index.status(&req.server).await.map_err(internal)?;
        Ok(Response::new(map_capabilities(
            capabilities,
            &index_status,
            self.index.max_results(),
        )))
    }

    #[tracing::instrument(skip(self, request))]
    async fn browse(
        &self,
        request: Request<opcda_bridge_proto::bridge::BrowseRequest>,
    ) -> Result<Response<ProtoBrowsePage>, Status> {
        let req = request.into_inner();
        let _foreground = self.index.foreground_guard(&req.server);
        let started = std::time::Instant::now();
        let result = self
            .browse
            .browse(
                &req.server,
                req.session_id.as_deref(),
                req.parent_node_key.as_deref(),
                req.page_token.as_deref(),
                req.page_size,
                req.refresh,
            )
            .await;
        self.index.record_foreground_operation_with_health(
            &req.server,
            started.elapsed(),
            result.is_err(),
            false,
            result.as_ref().is_err_and(|status| {
                matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
            }),
        );
        let (session_id, page) = result?;
        tracing::info!(
            server = %req.server,
            session = %session_id,
            count = page.nodes.len(),
            complete = page.complete,
            "browsed OPC DA page"
        );
        Ok(Response::new(map_browse_page(session_id, page)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn close_browse_session(
        &self,
        request: Request<CloseBrowseSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.browse
            .close_session(&request.into_inner().session_id)
            .await?;
        Ok(Response::new(()))
    }

    #[tracing::instrument(skip(self, request))]
    async fn get_search_index_status(
        &self,
        request: Request<GetSearchIndexStatusRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        let status = self
            .index
            .status(&request.into_inner().server)
            .await
            .map_err(internal)?;
        Ok(Response::new(map_index_status(status)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn refresh_search_index(
        &self,
        request: Request<RefreshSearchIndexRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        let request = request.into_inner();
        let status = self
            .index
            .refresh(&request.server, request.force)
            .await
            .map_err(index_error)?;
        Ok(Response::new(map_index_status(status)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn control_search_index(
        &self,
        request: Request<ControlSearchIndexRequest>,
    ) -> Result<Response<SearchIndexStatus>, Status> {
        let request = request.into_inner();
        let action = SearchIndexControlAction::try_from(request.action)
            .map_err(|_| Status::invalid_argument("unknown index control action"))?;
        let action = match action {
            SearchIndexControlAction::Pause => IndexControlAction::Pause,
            SearchIndexControlAction::Resume => IndexControlAction::Resume,
            SearchIndexControlAction::Cancel => IndexControlAction::Cancel,
            SearchIndexControlAction::EnableAutoRefresh => IndexControlAction::EnableAutoRefresh,
            SearchIndexControlAction::DisableAutoRefresh => IndexControlAction::DisableAutoRefresh,
            SearchIndexControlAction::Delete => IndexControlAction::Delete,
            SearchIndexControlAction::Unspecified => {
                return Err(Status::invalid_argument("index control action is required"));
            }
        };
        self.index
            .control(&request.server, action)
            .await
            .map_err(index_error)?;
        let status = self.index.status(&request.server).await.map_err(internal)?;
        Ok(Response::new(map_index_status(status)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn search_index(
        &self,
        request: Request<SearchIndexRequest>,
    ) -> Result<Response<SearchIndexResponse>, Status> {
        let request = request.into_inner();
        let mode = SearchMode::try_from(request.match_mode)
            .map_err(|_| Status::invalid_argument("unknown search match mode"))?;
        let mode = match mode {
            SearchMode::Exact => SearchMode::Exact,
            SearchMode::Prefix => SearchMode::Prefix,
            SearchMode::Contains | SearchMode::Unspecified => SearchMode::Contains,
        };
        let normalized_query = normalize_query(&request.query);
        if normalized_query.is_empty() {
            return Err(Status::invalid_argument("search query must not be empty"));
        }
        let minimum = match mode {
            SearchMode::Exact | SearchMode::Prefix => 2,
            SearchMode::Contains | SearchMode::Unspecified => 3,
        };
        if normalized_query.chars().count() < minimum {
            return Err(Status::invalid_argument(format!(
                "indexed {mode:?} searches require at least {minimum} characters"
            )));
        }
        let result = self
            .index
            .search(
                &request.server,
                &request.query,
                mode as i32,
                request.max_results,
            )
            .await
            .map_err(internal)?;
        let status = result.status;
        let matches = result.matches;
        Ok(Response::new(SearchIndexResponse {
            matches: matches.into_iter().map(map_index_match).collect(),
            has_more: result.has_more,
            status: Some(map_index_status(status)),
        }))
    }

    type SearchStream = ReceiverStream<Result<SearchEvent, Status>>;

    #[tracing::instrument(skip(self, request))]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        let request = request.into_inner();
        let foreground = self.index.foreground_guard(&request.server);
        let (mode, max_results) = validate_search(&request)?;
        let temporary_session = request.session_id.is_none();
        let session_id = match request.session_id.as_deref() {
            Some(session_id) => session_id.to_string(),
            None => self.browse.open_session(&request.server).await?,
        };
        let (tx, rx) = mpsc::channel(32);
        let manager = Arc::clone(&self.browse);
        tokio::spawn(run_search(
            manager,
            foreground,
            request.server.clone(),
            session_id,
            request,
            mode,
            max_results,
            temporary_session,
            tx,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn list_servers(
        &self,
        request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        let req = request.into_inner();
        let host = resolve_host(&req.host);
        let servers = self.client.list_servers(host).await.map_err(internal)?;
        Ok(Response::new(ListServersResponse { servers }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        let _foreground = self.index.foreground_guard(&req.server);
        let started = std::time::Instant::now();
        let result = self.client.read_tag_values(&req.server, req.tag_ids).await;
        let bad_quality = result.as_ref().is_ok_and(|values| {
            values
                .iter()
                .any(|value| !value.quality.eq_ignore_ascii_case("good"))
        });
        self.index.record_foreground_operation_with_health(
            &req.server,
            started.elapsed(),
            result.is_err(),
            bad_quality,
            result.is_err(),
        );
        let values = result.map_err(internal)?;
        Ok(Response::new(ReadResponse {
            values: map_to_proto_tag_values(values),
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let _foreground = self.index.foreground_guard(&req.server);
        let value = typed_value_to_opc_value(req.typed_value)?;
        let started = std::time::Instant::now();
        let result = self
            .client
            .write_tag_value(&req.server, &req.tag_id, value)
            .await;
        self.index.record_foreground_operation_with_health(
            &req.server,
            started.elapsed(),
            result.is_err() || result.as_ref().is_ok_and(|value| !value.success),
            false,
            result.is_err(),
        );
        let result = result.map_err(internal)?;
        Ok(Response::new(map_to_write_response(result)))
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
