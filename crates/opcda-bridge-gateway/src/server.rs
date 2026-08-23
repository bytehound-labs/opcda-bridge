use crate::browse::{BrowseManager, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::opc::{
    BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, BrowseSource,
    NamespaceOrganization, OpcClient, OpcValue, TagValue, WriteResult,
};
use opcda_bridge_proto::bridge::{
    BrowseBreadcrumb, BrowseNode as ProtoBrowseNode, BrowsePage as ProtoBrowsePage,
    BrowseSource as ProtoBrowseSource, CloseBrowseSessionRequest, GetCapabilitiesRequest,
    GetCapabilitiesResponse, ListServersRequest, ListServersResponse,
    NamespaceOrganization as ProtoNamespaceOrganization, ReadRequest, ReadResponse,
    SearchCompleted, SearchEvent, SearchMatch, SearchMatchMode, SearchProgress, SearchRequest,
    TagValue as ProtoTagValue, WriteRequest, WriteResponse, bridge_server::Bridge,
    search_event::Event, write_request::TypedValue as ProtoTypedValue,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const PROTOCOL_VERSION: &str = "2";
const DEFAULT_SEARCH_RESULTS: u32 = 200;
const MAX_SEARCH_RESULTS: u32 = 1_000;
const MAX_SEARCH_VISITED: u32 = 50_000;

pub struct BridgeService<C: OpcClient> {
    client: Arc<C>,
    browse: Arc<BrowseManager<C>>,
}

impl<C: OpcClient> BridgeService<C> {
    pub fn new(client: C) -> Self {
        let client = Arc::new(client);
        Self {
            browse: Arc::new(BrowseManager::new(Arc::clone(&client))),
            client,
        }
    }
}

#[cfg(target_os = "windows")]
impl Default for BridgeService<crate::opc_da_adapter::OpcDaAdapter> {
    fn default() -> Self {
        Self::new(crate::opc_da_adapter::OpcDaAdapter::default())
    }
}

fn internal(error: impl std::fmt::Display) -> Status {
    let message = error.to_string();
    tracing::error!(error = %message, "OPC operation failed");
    Status::internal(message)
}

fn resolve_host(host: &str) -> &str {
    if host.is_empty() { "localhost" } else { host }
}

fn map_namespace_organization(value: NamespaceOrganization) -> ProtoNamespaceOrganization {
    match value {
        NamespaceOrganization::Unspecified => ProtoNamespaceOrganization::Unspecified,
        NamespaceOrganization::Flat => ProtoNamespaceOrganization::Flat,
        NamespaceOrganization::Hierarchical => ProtoNamespaceOrganization::Hierarchical,
    }
}

fn map_browse_source(value: BrowseSource) -> ProtoBrowseSource {
    match value {
        BrowseSource::Da3 => ProtoBrowseSource::Da3,
        BrowseSource::Da2 => ProtoBrowseSource::Da2,
        BrowseSource::Flat => ProtoBrowseSource::Flat,
        BrowseSource::Derived => ProtoBrowseSource::Derived,
    }
}

fn map_browse_node(node: BrowseNode) -> ProtoBrowseNode {
    ProtoBrowseNode {
        node_key: node.node_key,
        display_name: node.display_name,
        kind: match node.kind {
            BrowseNodeKind::Branch => opcda_bridge_proto::bridge::BrowseNodeKind::Branch,
            BrowseNodeKind::Item => opcda_bridge_proto::bridge::BrowseNodeKind::Item,
            BrowseNodeKind::BranchAndItem => {
                opcda_bridge_proto::bridge::BrowseNodeKind::BranchAndItem
            }
        } as i32,
        item_id: node.item_id,
    }
}

fn map_browse_page(session_id: String, page: BrowsePage) -> ProtoBrowsePage {
    ProtoBrowsePage {
        session_id,
        nodes: page.nodes.into_iter().map(map_browse_node).collect(),
        next_page_token: page.next_page_token,
        complete: page.complete,
        organization: map_namespace_organization(page.organization) as i32,
        source: map_browse_source(page.source) as i32,
        warning: page.warning,
    }
}

fn map_capabilities(capabilities: BrowseCapabilities) -> GetCapabilitiesResponse {
    GetCapabilitiesResponse {
        application_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        max_page_size: capabilities.max_page_size.min(MAX_PAGE_SIZE),
        supports_browse_sessions: capabilities.supports_browse_sessions,
        supports_search: capabilities.supports_search,
        organization: map_namespace_organization(capabilities.organization) as i32,
        source: map_browse_source(capabilities.source) as i32,
    }
}

fn map_to_proto_tag_values(values: Vec<TagValue>) -> Vec<ProtoTagValue> {
    values
        .into_iter()
        .map(|value| ProtoTagValue {
            tag_id: value.tag_id,
            value: value.value,
            quality: value.quality,
            timestamp: value.timestamp,
        })
        .collect()
}

fn typed_value_to_opc_value(typed_value: Option<ProtoTypedValue>) -> Result<OpcValue, Status> {
    let typed_value =
        typed_value.ok_or_else(|| Status::invalid_argument("no typed_value provided"))?;
    Ok(match typed_value {
        ProtoTypedValue::StringValue(value) => OpcValue::String(value),
        ProtoTypedValue::IntValue(value) => OpcValue::Int(value),
        ProtoTypedValue::FloatValue(value) => OpcValue::Float(value),
        ProtoTypedValue::BoolValue(value) => OpcValue::Bool(value),
    })
}

fn map_to_write_response(result: WriteResult) -> WriteResponse {
    WriteResponse {
        tag_id: result.tag_id,
        success: result.success,
        error: result.error,
    }
}

fn search_mode(mode: i32) -> Result<SearchMatchMode, Status> {
    let mode = SearchMatchMode::try_from(mode)
        .map_err(|_| Status::invalid_argument("unknown search match mode"))?;
    if mode == SearchMatchMode::Unspecified {
        Ok(SearchMatchMode::Contains)
    } else {
        Ok(mode)
    }
}

fn validate_search(request: &SearchRequest) -> Result<(SearchMatchMode, u32), Status> {
    if request.query.trim().is_empty() {
        return Err(Status::invalid_argument("search query must not be empty"));
    }
    let mode = search_mode(request.match_mode)?;
    if mode == SearchMatchMode::Contains && request.query.chars().count() < 2 {
        return Err(Status::invalid_argument(
            "contains searches require at least two characters",
        ));
    }
    let max_results = if request.max_results == 0 {
        DEFAULT_SEARCH_RESULTS
    } else {
        request.max_results
    };
    if max_results > MAX_SEARCH_RESULTS {
        return Err(Status::invalid_argument(format!(
            "max_results must not exceed {MAX_SEARCH_RESULTS}"
        )));
    }
    Ok((mode, max_results))
}

fn search_matches(node: &BrowseNode, query: &str, mode: SearchMatchMode) -> bool {
    let matches = |value: &str| match mode {
        SearchMatchMode::Exact => value == query,
        SearchMatchMode::Prefix => value.starts_with(query),
        SearchMatchMode::Contains | SearchMatchMode::Unspecified => value.contains(query),
    };
    matches(&node.display_name) || node.item_id.as_deref().is_some_and(matches)
}

fn is_expandable(kind: BrowseNodeKind) -> bool {
    matches!(kind, BrowseNodeKind::Branch | BrowseNodeKind::BranchAndItem)
}

fn search_event(event: Event) -> SearchEvent {
    SearchEvent { event: Some(event) }
}

#[allow(clippy::too_many_arguments)]
async fn run_search<C: OpcClient>(
    manager: Arc<BrowseManager<C>>,
    server: String,
    session_id: String,
    request: SearchRequest,
    mode: SearchMatchMode,
    max_results: u32,
    temporary_session: bool,
    tx: mpsc::Sender<Result<SearchEvent, Status>>,
) {
    let result = run_search_inner(
        Arc::clone(&manager),
        &server,
        &session_id,
        &request,
        mode,
        max_results,
        &tx,
    )
    .await;

    if let Err(error) = result {
        let _ = tx.send(Err(error)).await;
    }
    if temporary_session && let Err(error) = manager.close_session(&session_id).await {
        tracing::debug!(error = %error, "temporary search session was already closed");
    }
}

async fn run_search_inner<C: OpcClient>(
    manager: Arc<BrowseManager<C>>,
    server: &str,
    session_id: &str,
    request: &SearchRequest,
    mode: SearchMatchMode,
    max_results: u32,
    tx: &mpsc::Sender<Result<SearchEvent, Status>>,
) -> Result<(), Status> {
    if tx
        .send(Ok(search_event(Event::Progress(SearchProgress {
            visited_nodes: 0,
            matches: 0,
            partial: false,
        }))))
        .await
        .is_err()
    {
        return Ok(());
    }

    let mut scopes = VecDeque::from([(
        request.scope_node_key.clone(),
        request
            .scope_node_key
            .as_ref()
            .map(|node_key| {
                vec![BrowseBreadcrumb {
                    node_key: node_key.clone(),
                    display_name: String::new(),
                }]
            })
            .unwrap_or_default(),
        request.refresh,
    )]);
    let mut matched_item_ids = HashSet::new();
    let mut visited_nodes = 0_u32;
    let mut matches = 0_u32;

    while let Some((parent_node_key, breadcrumbs, refresh)) = scopes.pop_front() {
        let mut page_token = None;
        let mut first_page = true;
        loop {
            let (_, page) = manager
                .browse(
                    server,
                    Some(session_id),
                    parent_node_key.as_deref(),
                    page_token.as_deref(),
                    DEFAULT_PAGE_SIZE,
                    refresh && first_page,
                )
                .await?;
            first_page = false;

            for node in page.nodes {
                visited_nodes = visited_nodes.saturating_add(1);
                let item_match = search_matches(&node, &request.query, mode);
                let branch_match = request.include_branches && item_match;
                let has_new_item = node
                    .item_id
                    .as_ref()
                    .is_none_or(|item_id| !matched_item_ids.contains(item_id));

                if item_match && has_new_item && (node.item_id.is_some() || branch_match) {
                    if let Some(item_id) = node.item_id.as_ref() {
                        matched_item_ids.insert(item_id.clone());
                    }
                    matches = matches.saturating_add(1);
                    let mut result_breadcrumbs = breadcrumbs.clone();
                    result_breadcrumbs.push(BrowseBreadcrumb {
                        node_key: node.node_key.clone(),
                        display_name: node.display_name.clone(),
                    });
                    if tx
                        .send(Ok(search_event(Event::Match(SearchMatch {
                            node: Some(map_browse_node(node.clone())),
                            breadcrumbs: result_breadcrumbs,
                        }))))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    if matches >= max_results {
                        tx.send(Ok(search_event(Event::Completed(SearchCompleted {
                            complete: false,
                            cancelled: false,
                            truncated: true,
                            warning: Some("search result limit reached".to_string()),
                        }))))
                        .await
                        .map_err(|_| Status::cancelled("search stream closed"))?;
                        return Ok(());
                    }
                }

                if is_expandable(node.kind) {
                    let mut child_breadcrumbs = breadcrumbs.clone();
                    child_breadcrumbs.push(BrowseBreadcrumb {
                        node_key: node.node_key.clone(),
                        display_name: node.display_name.clone(),
                    });
                    scopes.push_back((Some(node.node_key), child_breadcrumbs, false));
                }

                if visited_nodes >= MAX_SEARCH_VISITED {
                    tx.send(Ok(search_event(Event::Completed(SearchCompleted {
                        complete: false,
                        cancelled: false,
                        truncated: true,
                        warning: Some("search visit limit reached".to_string()),
                    }))))
                    .await
                    .map_err(|_| Status::cancelled("search stream closed"))?;
                    return Ok(());
                }
            }

            if tx
                .send(Ok(search_event(Event::Progress(SearchProgress {
                    visited_nodes,
                    matches,
                    partial: page.next_page_token.is_some(),
                }))))
                .await
                .is_err()
            {
                return Ok(());
            }

            match page.next_page_token {
                Some(next) => page_token = Some(next),
                None => break,
            }
        }
    }

    tx.send(Ok(search_event(Event::Completed(SearchCompleted {
        complete: true,
        cancelled: false,
        truncated: false,
        warning: None,
    }))))
    .await
    .map_err(|_| Status::cancelled("search stream closed"))?;
    Ok(())
}

#[tonic::async_trait]
impl<C: OpcClient> Bridge for BridgeService<C> {
    #[tracing::instrument(skip(self, request))]
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        let capabilities = self
            .client
            .get_capabilities(&req.server)
            .await
            .map_err(internal)?;
        Ok(Response::new(map_capabilities(capabilities)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn browse(
        &self,
        request: Request<opcda_bridge_proto::bridge::BrowseRequest>,
    ) -> Result<Response<ProtoBrowsePage>, Status> {
        let req = request.into_inner();
        let (session_id, page) = self
            .browse
            .browse(
                &req.server,
                req.session_id.as_deref(),
                req.parent_node_key.as_deref(),
                req.page_token.as_deref(),
                req.page_size,
                req.refresh,
            )
            .await?;
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

    type SearchStream = ReceiverStream<Result<SearchEvent, Status>>;

    #[tracing::instrument(skip(self, request))]
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStream>, Status> {
        let request = request.into_inner();
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
        let values = self
            .client
            .read_tag_values(&req.server, req.tag_ids)
            .await
            .map_err(internal)?;
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
        let value = typed_value_to_opc_value(req.typed_value)?;
        let result = self
            .client
            .write_tag_value(&req.server, &req.tag_id, value)
            .await
            .map_err(internal)?;
        Ok(Response::new(map_to_write_response(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opc::BrowseCapabilities;
    use crate::test_support::MockOpcClient;
    use opcda_bridge_proto::bridge::{
        BrowseRequest, BrowseSource as ProtoBrowseSource, GetCapabilitiesRequest,
        ListServersRequest, NamespaceOrganization as ProtoNamespaceOrganization, ReadRequest,
        SearchMatchMode, SearchRequest, WriteRequest, bridge_server::Bridge,
        write_request::TypedValue as ProtoTypedValue,
    };

    fn service() -> BridgeService<MockOpcClient> {
        BridgeService::new(MockOpcClient::default())
    }

    #[test]
    fn maps_types_and_defaults() {
        assert_eq!(resolve_host(""), "localhost");
        assert_eq!(resolve_host("nas"), "nas");
        assert_eq!(
            map_namespace_organization(NamespaceOrganization::Hierarchical),
            ProtoNamespaceOrganization::Hierarchical
        );
        assert_eq!(
            map_namespace_organization(NamespaceOrganization::Flat),
            ProtoNamespaceOrganization::Flat
        );
        assert_eq!(
            map_namespace_organization(NamespaceOrganization::Unspecified),
            ProtoNamespaceOrganization::Unspecified
        );
        assert_eq!(map_browse_source(BrowseSource::Da3), ProtoBrowseSource::Da3);
        assert_eq!(map_browse_source(BrowseSource::Da2), ProtoBrowseSource::Da2);
        assert_eq!(
            map_browse_source(BrowseSource::Flat),
            ProtoBrowseSource::Flat
        );
        assert_eq!(
            map_browse_source(BrowseSource::Derived),
            ProtoBrowseSource::Derived
        );
        assert!(is_expandable(BrowseNodeKind::Branch));
        assert!(is_expandable(BrowseNodeKind::BranchAndItem));
        assert!(!is_expandable(BrowseNodeKind::Item));
    }

    #[test]
    fn maps_wire_shapes_and_search_matching() {
        let nodes = [
            (BrowseNodeKind::Branch, "branch"),
            (BrowseNodeKind::Item, "item"),
            (BrowseNodeKind::BranchAndItem, "both"),
        ]
        .into_iter()
        .map(|(kind, node_key)| BrowseNode {
            node_key: node_key.into(),
            display_name: node_key.into(),
            kind,
            item_id: Some(format!("{node_key}.item")),
        })
        .collect();
        let page = map_browse_page(
            "session".into(),
            BrowsePage {
                nodes,
                next_page_token: Some("next".into()),
                complete: false,
                organization: NamespaceOrganization::Flat,
                source: BrowseSource::Derived,
                warning: Some("partial".into()),
            },
        );
        assert_eq!(page.session_id, "session");
        assert_eq!(page.nodes.len(), 3);
        assert_eq!(page.next_page_token.as_deref(), Some("next"));
        assert_eq!(page.organization, ProtoNamespaceOrganization::Flat as i32);
        assert_eq!(page.source, ProtoBrowseSource::Derived as i32);
        assert_eq!(page.warning.as_deref(), Some("partial"));

        let values = map_to_proto_tag_values(vec![TagValue {
            tag_id: "tag".into(),
            value: "42".into(),
            quality: "good".into(),
            timestamp: "now".into(),
        }]);
        assert_eq!(values[0].tag_id, "tag");
        assert_eq!(values[0].value, "42");
        assert_eq!(values[0].quality, "good");
        assert_eq!(values[0].timestamp, "now");

        let response = map_to_write_response(WriteResult {
            tag_id: "tag".into(),
            success: false,
            error: Some("failed".into()),
        });
        assert_eq!(response.tag_id, "tag");
        assert!(!response.success);
        assert_eq!(response.error.as_deref(), Some("failed"));

        let node = BrowseNode {
            node_key: "opaque".into(),
            display_name: "Temperature".into(),
            kind: BrowseNodeKind::Item,
            item_id: Some("device.temperature".into()),
        };
        assert!(search_matches(&node, "Temp", SearchMatchMode::Prefix));
        assert!(search_matches(
            &node,
            "device",
            SearchMatchMode::Unspecified
        ));
        assert!(!search_matches(
            &node,
            "pressure",
            SearchMatchMode::Contains
        ));
        assert_eq!(
            search_mode(SearchMatchMode::Exact as i32).unwrap(),
            SearchMatchMode::Exact
        );
        assert_eq!(
            search_mode(i32::MAX).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(internal("operation failed").message(), "operation failed");
    }

    #[test]
    fn validates_search_modes_and_limits() {
        let mut request = SearchRequest {
            query: String::new(),
            ..Default::default()
        };
        assert_eq!(
            validate_search(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        request.query = "x".into();
        request.match_mode = SearchMatchMode::Contains as i32;
        assert_eq!(
            validate_search(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        request.query = "xy".into();
        request.max_results = MAX_SEARCH_RESULTS + 1;
        assert_eq!(
            validate_search(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        request.max_results = 0;
        request.match_mode = SearchMatchMode::Unspecified as i32;
        assert_eq!(
            validate_search(&request).unwrap(),
            (SearchMatchMode::Contains, DEFAULT_SEARCH_RESULTS)
        );
        request.match_mode = SearchMatchMode::Prefix as i32;
        request.max_results = 12;
        assert_eq!(
            validate_search(&request).unwrap(),
            (SearchMatchMode::Prefix, 12)
        );
    }

    #[test]
    fn search_matching_preserves_exact_item_ids() {
        let node = BrowseNode {
            node_key: "opaque".into(),
            display_name: "PV".into(),
            kind: BrowseNodeKind::Item,
            item_id: Some("FCS0201!204FI00510.PV".into()),
        };
        assert!(search_matches(&node, "PV", SearchMatchMode::Exact));
        assert!(search_matches(&node, "204FI", SearchMatchMode::Contains));
        assert!(!search_matches(&node, "MV", SearchMatchMode::Exact));
    }

    #[tokio::test]
    async fn capabilities_are_typed() {
        let response = service()
            .get_capabilities(Request::new(GetCapabilitiesRequest { server: "S".into() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
        assert!(response.supports_browse_sessions);
        assert_eq!(response.max_page_size, MAX_PAGE_SIZE);
    }

    #[tokio::test]
    async fn browse_returns_session_page_and_metadata() {
        let response = service()
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                page_size: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.session_id.is_empty());
        assert!(response.complete);
    }

    #[tokio::test]
    async fn browse_rejects_invalid_page_size() {
        let result = service()
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                page_size: MAX_PAGE_SIZE + 1,
                ..Default::default()
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn browse_rejects_unknown_parent() {
        let response = service()
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let result = service()
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                session_id: Some(response.session_id),
                parent_node_key: Some("unknown".into()),
                ..Default::default()
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn search_stream_emits_progress_matches_and_completion() {
        let service = service();
        let response = service
            .search(Request::new(SearchRequest {
                server: "S".into(),
                query: "tag".into(),
                match_mode: SearchMatchMode::Contains as i32,
                ..Default::default()
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let mut events = Vec::new();
        while let Some(event) = tokio_stream::StreamExt::next(&mut stream).await {
            events.push(event.unwrap().event.unwrap());
        }
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::Progress(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::Completed(_)))
        );
    }

    #[tokio::test]
    async fn search_traverses_pages_scopes_and_deduplicates_items() {
        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![BrowseNode {
                node_key: "scope-native".into(),
                display_name: "scope".into(),
                kind: BrowseNodeKind::Item,
                item_id: None,
            }],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        let service = BridgeService::new(mock);
        let initial = service
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let scope_node_key = initial.nodes[0].node_key.clone();

        let root_page = BrowsePage {
            nodes: vec![
                BrowseNode {
                    node_key: "branch-native".into(),
                    display_name: "tag-area".into(),
                    kind: BrowseNodeKind::Branch,
                    item_id: None,
                },
                BrowseNode {
                    node_key: "branch-item-native".into(),
                    display_name: "tag".into(),
                    kind: BrowseNodeKind::BranchAndItem,
                    item_id: Some("tag.item".into()),
                },
                BrowseNode {
                    node_key: "duplicate-native".into(),
                    display_name: "tag-duplicate".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: Some("tag.item".into()),
                },
            ],
            next_page_token: Some("native-next".into()),
            complete: false,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        };
        let empty_page = || BrowsePage {
            nodes: Vec::new(),
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        };
        service.client.browse_page_results.lock().unwrap().extend([
            Ok(root_page),
            Ok(empty_page()),
            Ok(empty_page()),
            Ok(empty_page()),
        ]);

        let response = service
            .search(Request::new(SearchRequest {
                server: "S".into(),
                session_id: Some(initial.session_id),
                scope_node_key: Some(scope_node_key),
                query: "tag".into(),
                match_mode: SearchMatchMode::Contains as i32,
                include_branches: true,
                refresh: true,
                max_results: 10,
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let mut events = Vec::new();
        while let Some(event) = tokio_stream::StreamExt::next(&mut stream).await {
            events.push(event.unwrap().event.unwrap());
        }
        let matches: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Match(value) => Some(value),
                _ => None,
            })
            .collect();
        assert_eq!(matches.len(), 2);
        assert!(matches.iter().any(|value| value.breadcrumbs.len() == 2));
        assert!(events.iter().any(|event| {
            matches!(event, Event::Progress(SearchProgress { partial: true, .. }))
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                Event::Completed(SearchCompleted {
                    complete: true,
                    truncated: false,
                    ..
                })
            )
        }));
    }

    #[tokio::test]
    async fn search_truncates_at_result_limit_and_closes_temporary_session() {
        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![BrowseNode {
                node_key: "native".into(),
                display_name: "tag".into(),
                kind: BrowseNodeKind::Item,
                item_id: Some("tag.item".into()),
            }],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        let service = BridgeService::new(mock);
        let mut stream = service
            .search(Request::new(SearchRequest {
                server: "S".into(),
                query: "tag".into(),
                match_mode: SearchMatchMode::Prefix as i32,
                max_results: 1,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut completed = None;
        while let Some(event) = tokio_stream::StreamExt::next(&mut stream).await {
            if let Event::Completed(value) = event.unwrap().event.unwrap() {
                completed = Some(value);
            }
        }
        let completed = completed.unwrap();
        assert!(!completed.complete);
        assert!(completed.truncated);
        assert_eq!(
            completed.warning.as_deref(),
            Some("search result limit reached")
        );
    }

    #[tokio::test]
    async fn search_reports_browse_errors_and_close_errors() {
        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Err("browse failed".into());
        *mock.close_browse_session_result.lock().unwrap() = Err("close failed".into());
        let service = BridgeService::new(mock);
        let mut stream = service
            .search(Request::new(SearchRequest {
                server: "S".into(),
                query: "tag".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut errors = 0;
        while let Some(event) = tokio_stream::StreamExt::next(&mut stream).await {
            if event.is_err() {
                errors += 1;
            }
        }
        assert_eq!(errors, 1);
    }

    #[tokio::test]
    async fn search_handles_closed_streams_and_visit_limit() {
        let make_request = || SearchRequest {
            query: "tag".into(),
            match_mode: SearchMatchMode::Contains as i32,
            ..Default::default()
        };

        let service = service();
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert!(
            run_search_inner(
                Arc::clone(&service.browse),
                "S",
                &session,
                &make_request(),
                SearchMatchMode::Contains,
                10,
                &tx,
            )
            .await
            .is_ok()
        );

        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![BrowseNode {
                node_key: "native".into(),
                display_name: "tag".into(),
                kind: BrowseNodeKind::Item,
                item_id: Some("tag.item".into()),
            }],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        let service = BridgeService::new(mock);
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let manager = Arc::clone(&service.browse);
        let request = make_request();
        let handle = tokio::spawn(async move {
            run_search_inner(
                manager,
                "S",
                &session,
                &request,
                SearchMatchMode::Contains,
                10,
                &tx,
            )
            .await
        });
        let _ = rx.recv().await.unwrap();
        drop(rx);
        assert!(handle.await.unwrap().is_ok());

        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![BrowseNode {
                node_key: "native".into(),
                display_name: "tag".into(),
                kind: BrowseNodeKind::Item,
                item_id: Some("tag.item".into()),
            }],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        let service = BridgeService::new(mock);
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let manager = Arc::clone(&service.browse);
        let request = make_request();
        let handle = tokio::spawn(async move {
            run_search_inner(
                manager,
                "S",
                &session,
                &request,
                SearchMatchMode::Contains,
                1,
                &tx,
            )
            .await
        });
        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();
        drop(rx);
        assert_eq!(
            handle.await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );

        let service = BridgeService::new(MockOpcClient::default());
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let manager = Arc::clone(&service.browse);
        let request = make_request();
        let handle = tokio::spawn(async move {
            run_search_inner(
                manager,
                "S",
                &session,
                &request,
                SearchMatchMode::Contains,
                10,
                &tx,
            )
            .await
        });
        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();
        drop(rx);
        assert_eq!(
            handle.await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );

        let service = BridgeService::new(MockOpcClient::default());
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let manager = Arc::clone(&service.browse);
        let request = make_request();
        let handle = tokio::spawn(async move {
            run_search_inner(
                manager,
                "S",
                &session,
                &request,
                SearchMatchMode::Contains,
                10,
                &tx,
            )
            .await
        });
        let _ = rx.recv().await.unwrap();
        drop(rx);
        assert!(handle.await.unwrap().is_ok());

        let service = BridgeService::new(MockOpcClient::default());
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let manager = Arc::clone(&service.browse);
        let request = make_request();
        let handle = tokio::spawn(async move {
            run_search_inner(
                manager,
                "S",
                &session,
                &request,
                SearchMatchMode::Contains,
                10,
                &tx,
            )
            .await
        });
        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();
        drop(rx);
        assert_eq!(
            handle.await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );

        let mock = MockOpcClient::default();
        *mock.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: (0..MAX_SEARCH_VISITED)
                .map(|index| BrowseNode {
                    node_key: format!("node-{index}"),
                    display_name: "not-a-match".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: None,
                })
                .collect(),
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        let service = BridgeService::new(mock);
        let session = service.browse.open_session("S").await.unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        run_search_inner(
            Arc::clone(&service.browse),
            "S",
            &session,
            &make_request(),
            SearchMatchMode::Contains,
            10,
            &tx,
        )
        .await
        .unwrap();
        let _ = rx.recv().await.unwrap();
        let completed = rx.recv().await.unwrap().unwrap().event.unwrap();
        assert!(matches!(
            completed,
            Event::Completed(SearchCompleted {
                truncated: true,
                warning: Some(_),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn search_rejects_invalid_query() {
        let result = service()
            .search(Request::new(SearchRequest::default()))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn close_session_and_read_write_paths_work() {
        let service = service();
        let page = service
            .browse(Request::new(BrowseRequest {
                server: "S".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        service
            .close_browse_session(Request::new(CloseBrowseSessionRequest {
                session_id: page.session_id,
            }))
            .await
            .unwrap();

        let read = service
            .read(Request::new(ReadRequest {
                server: "S".into(),
                tag_ids: vec!["tag".into()],
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(read.values.is_empty());
        let write = service
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "tag".into(),
                typed_value: Some(ProtoTypedValue::BoolValue(true)),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(write.success);
    }

    #[tokio::test]
    async fn handlers_map_values_and_surface_client_errors() {
        let mock = MockOpcClient::default();
        *mock.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Unspecified,
            source: BrowseSource::Flat,
            supports_browse_sessions: false,
            supports_search: true,
            max_page_size: 10,
        });
        *mock.list_servers_result.lock().unwrap() = Ok(vec!["one".into(), "two".into()]);
        *mock.read_tag_values_result.lock().unwrap() = Ok(vec![TagValue {
            tag_id: "tag".into(),
            value: "value".into(),
            quality: "good".into(),
            timestamp: "timestamp".into(),
        }]);
        *mock.write_tag_value_result.lock().unwrap() = Ok(WriteResult {
            tag_id: "tag".into(),
            success: false,
            error: Some("bad value".into()),
        });
        let service = BridgeService::new(mock);

        let capabilities = service
            .get_capabilities(Request::new(GetCapabilitiesRequest { server: "S".into() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            capabilities.organization,
            ProtoNamespaceOrganization::Unspecified as i32
        );
        assert_eq!(capabilities.source, ProtoBrowseSource::Flat as i32);
        assert!(!capabilities.supports_browse_sessions);

        let servers = service
            .list_servers(Request::new(ListServersRequest {
                host: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(servers.servers, vec!["one", "two"]);

        let read = service
            .read(Request::new(ReadRequest {
                server: "S".into(),
                tag_ids: vec!["tag".into()],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(read.values[0].tag_id, "tag");

        for typed_value in [
            ProtoTypedValue::StringValue("text".into()),
            ProtoTypedValue::IntValue(1),
            ProtoTypedValue::FloatValue(1.5),
            ProtoTypedValue::BoolValue(true),
        ] {
            let write = service
                .write(Request::new(WriteRequest {
                    server: "S".into(),
                    tag_id: "tag".into(),
                    typed_value: Some(typed_value),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(write.error.as_deref(), Some("bad value"));
        }

        let mock = MockOpcClient::default();
        *mock.capabilities_result.lock().unwrap() = Err("capabilities failed".into());
        assert_eq!(
            BridgeService::new(mock)
                .get_capabilities(Request::new(GetCapabilitiesRequest { server: "S".into() }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        let mock = MockOpcClient::default();
        *mock.list_servers_result.lock().unwrap() = Err("list failed".into());
        assert_eq!(
            BridgeService::new(mock)
                .list_servers(Request::new(ListServersRequest {
                    host: "host".into(),
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        let mock = MockOpcClient::default();
        *mock.read_tag_values_result.lock().unwrap() = Err("read failed".into());
        assert_eq!(
            BridgeService::new(mock)
                .read(Request::new(ReadRequest {
                    server: "S".into(),
                    tag_ids: vec![],
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        let mock = MockOpcClient::default();
        *mock.write_tag_value_result.lock().unwrap() = Err("write failed".into());
        assert_eq!(
            BridgeService::new(mock)
                .write(Request::new(WriteRequest {
                    server: "S".into(),
                    tag_id: "tag".into(),
                    typed_value: Some(ProtoTypedValue::StringValue("value".into())),
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        let mock = MockOpcClient::default();
        *mock.open_browse_session_result.lock().unwrap() = Err("open failed".into());
        assert_eq!(
            BridgeService::new(mock)
                .browse(Request::new(BrowseRequest {
                    server: "S".into(),
                    ..Default::default()
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let mock = MockOpcClient::default();
        mock.browse_page_results
            .lock()
            .unwrap()
            .push_back(Err("queued browse failed".into()));
        assert!(
            mock.browse_page("native", None, None, 10, false)
                .await
                .is_err()
        );
    }

    #[test]
    fn map_capabilities_clamps_page_size() {
        let response = map_capabilities(BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            supports_browse_sessions: true,
            supports_search: false,
            max_page_size: u32::MAX,
        });
        assert_eq!(response.max_page_size, MAX_PAGE_SIZE);
    }

    #[test]
    fn typed_value_missing_is_invalid() {
        assert_eq!(
            typed_value_to_opc_value(None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            typed_value_to_opc_value(Some(ProtoTypedValue::IntValue(-1))).unwrap(),
            OpcValue::Int(-1)
        );
    }
}
