//! The connected gRPC client and typed search stream.

use crate::error::{Error, Result};
use crate::types::{
    BrowsePage, BrowsePageRequest, Capabilities, SearchEvent, SearchIndexControlAction,
    SearchIndexRequest, SearchIndexResponse, SearchIndexStatus, SearchRequest, TagValue, Value,
    WriteResult,
};
use crate::{
    CompatibilityReport, GatewayInfo, current_client_profile, evaluate_compatibility,
    legacy_gateway_profile, unknown_compatibility_report,
};
use opcda_bridge_proto::bridge::bridge_client::BridgeClient;
use opcda_bridge_proto::bridge::write_request::TypedValue;
use opcda_bridge_proto::bridge::{
    CloseBrowseSessionRequest, ControlSearchIndexRequest, GetCapabilitiesRequest,
    GetGatewayInfoRequest, GetSearchIndexStatusRequest, ListServersRequest, ReadRequest,
    RefreshSearchIndexRequest, WriteRequest,
};
use tonic::Code;
use tonic::codec::Streaming;
use tonic::transport::Channel;

/// A connected client for an opcda-bridge gateway's gRPC API.
#[derive(Debug)]
pub struct Client {
    inner: BridgeClient<Channel>,
}

/// A cancellable stream of typed namespace-search events.
///
/// Dropping this value drops the underlying gRPC stream, allowing the gateway
/// to stop scheduling further search work.
#[derive(Debug)]
pub struct SearchStream {
    inner: Streaming<opcda_bridge_proto::bridge::SearchEvent>,
}

impl SearchStream {
    /// Wait for the next event. `None` means the server closed the stream.
    pub async fn message(&mut self) -> Result<Option<SearchEvent>> {
        self.inner
            .message()
            .await?
            .map(SearchEvent::try_from)
            .transpose()
    }
}

impl Client {
    /// Connect to a plaintext gateway at `host` (for example, `localhost:7600`).
    pub async fn connect(host: &str) -> Result<Self> {
        let inner = BridgeClient::connect(format!("http://{host}")).await?;
        Ok(Self { inner })
    }

    /// Report protocol, paging, browse-session, search, and namespace support.
    pub async fn capabilities(&mut self, server: impl Into<String>) -> Result<Capabilities> {
        self.inner
            .get_capabilities(GetCapabilitiesRequest {
                server: server.into(),
            })
            .await
            .map_err(|status| feature_error("capability discovery", status))?
            .into_inner()
            .try_into()
    }

    /// Report gateway-wide protocol ranges without contacting an OPC server.
    pub async fn gateway_info(&mut self) -> Result<GatewayInfo> {
        self.inner
            .get_gateway_info(GetGatewayInfoRequest {})
            .await
            .map_err(Error::Rpc)?
            .into_inner()
            .try_into()
    }

    /// Compare this reusable client with a gateway, using the library version
    /// as the local application version.
    pub async fn compatibility(&mut self, server: Option<&str>) -> Result<CompatibilityReport> {
        self.compatibility_with_client_version(server, env!("CARGO_PKG_VERSION"))
            .await
    }

    /// Compare this client application version with a gateway.
    ///
    /// New gateways answer without an OPC server. Older gateways can be
    /// inspected with `server` through their legacy per-server capabilities
    /// response; without it, the result is honestly reported as unknown.
    pub async fn compatibility_with_client_version(
        &mut self,
        server: Option<&str>,
        client_version: impl Into<String>,
    ) -> Result<CompatibilityReport> {
        let client_profile = current_client_profile(client_version);
        match self.gateway_info().await {
            Ok(info) => {
                let gateway_profile = crate::ProtocolProfile::from_gateway_info(&info);
                Ok(evaluate_compatibility(&client_profile, &gateway_profile))
            }
            Err(Error::Rpc(status)) if status.code() == Code::Unimplemented => match server {
                Some(server) => {
                    let capabilities = self.capabilities(server).await?;
                    let gateway_profile = legacy_gateway_profile(&capabilities);
                    Ok(evaluate_compatibility(&client_profile, &gateway_profile))
                }
                None => Ok(unknown_compatibility_report(
                    client_profile.application_version.unwrap_or_default(),
                )),
            },
            Err(error) => Err(error),
        }
    }

    /// List the OPC DA servers registered on the gateway's host.
    pub async fn list_servers(&mut self) -> Result<Vec<String>> {
        let response = self
            .inner
            .list_servers(ListServersRequest {
                host: "localhost".to_string(),
            })
            .await?;
        Ok(response.into_inner().servers)
    }

    /// Open a browse session and return only its first root page.
    pub async fn browse(
        &mut self,
        server: impl Into<String>,
        page_size: u32,
    ) -> Result<BrowsePage> {
        self.open_browse(server, page_size).await
    }

    /// Open a browse session and return only its first root page.
    pub async fn open_browse(
        &mut self,
        server: impl Into<String>,
        page_size: u32,
    ) -> Result<BrowsePage> {
        self.browse_page(BrowsePageRequest::root(server, page_size))
            .await
    }

    /// Request exactly one root, child, or continuation page.
    ///
    /// This method never follows `next_page_token` automatically.
    pub async fn browse_page(&mut self, request: BrowsePageRequest) -> Result<BrowsePage> {
        self.inner
            .browse(opcda_bridge_proto::bridge::BrowseRequest::from(request))
            .await
            .map_err(|status| feature_error("paged browse", status))?
            .into_inner()
            .try_into()
    }

    /// Explicitly release a gateway browse session.
    pub async fn close_browse_session(&mut self, session_id: impl Into<String>) -> Result<()> {
        self.inner
            .close_browse_session(CloseBrowseSessionRequest {
                session_id: session_id.into(),
            })
            .await
            .map_err(|status| feature_error("browse-session close", status))?;
        Ok(())
    }

    /// Start a bounded search and return its progressive event stream.
    pub async fn search_stream(&mut self, request: SearchRequest) -> Result<SearchStream> {
        let inner = self
            .inner
            .search(opcda_bridge_proto::bridge::SearchRequest::from(request))
            .await
            .map_err(|status| feature_error("namespace search", status))?
            .into_inner();
        Ok(SearchStream { inner })
    }

    /// Explicitly collect a complete search stream into memory.
    pub async fn search(&mut self, request: SearchRequest) -> Result<Vec<SearchEvent>> {
        let mut stream = self.search_stream(request).await?;
        let mut events = Vec::new();
        while let Some(event) = stream.message().await? {
            events.push(event);
        }
        Ok(events)
    }

    /// Return the persistent namespace-index status for `server`.
    pub async fn search_index_status(
        &mut self,
        server: impl Into<String>,
    ) -> Result<SearchIndexStatus> {
        self.inner
            .get_search_index_status(GetSearchIndexStatusRequest {
                server: server.into(),
            })
            .await
            .map_err(|status| feature_error("indexed-search status", status))?
            .into_inner()
            .try_into()
    }

    /// Start or coalesce a persistent namespace-index refresh.
    pub async fn refresh_search_index(
        &mut self,
        server: impl Into<String>,
        force: bool,
    ) -> Result<SearchIndexStatus> {
        self.inner
            .refresh_search_index(RefreshSearchIndexRequest {
                server: server.into(),
                force,
            })
            .await
            .map_err(|status| feature_error("indexed-search refresh", status))?
            .into_inner()
            .try_into()
    }

    /// Pause, resume, or cancel a persistent namespace-index build.
    pub async fn control_search_index(
        &mut self,
        server: impl Into<String>,
        action: SearchIndexControlAction,
    ) -> Result<SearchIndexStatus> {
        self.inner
            .control_search_index(ControlSearchIndexRequest {
                server: server.into(),
                action: opcda_bridge_proto::bridge::SearchIndexControlAction::from(action) as i32,
            })
            .await
            .map_err(|status| feature_error("indexed-search control", status))?
            .into_inner()
            .try_into()
    }

    /// Search the gateway-owned persistent namespace index.
    ///
    /// This never falls back to live namespace traversal.
    pub async fn search_index(
        &mut self,
        request: SearchIndexRequest,
    ) -> Result<SearchIndexResponse> {
        self.inner
            .search_index(opcda_bridge_proto::bridge::SearchIndexRequest::from(
                request,
            ))
            .await
            .map_err(|status| feature_error("indexed search", status))?
            .into_inner()
            .try_into()
    }

    /// Read one or more exact OPC DA ItemIDs from `server`.
    pub async fn read(&mut self, server: String, tags: Vec<String>) -> Result<Vec<TagValue>> {
        let response = self
            .inner
            .read(ReadRequest {
                server,
                tag_ids: tags,
            })
            .await?;
        Ok(response
            .into_inner()
            .values
            .into_iter()
            .map(|v| TagValue {
                tag_id: v.tag_id,
                value: v.value,
                quality: v.quality,
                timestamp: v.timestamp,
            })
            .collect())
    }

    /// Write `value` to one exact OPC DA ItemID on `server`.
    pub async fn write(
        &mut self,
        server: String,
        tag: String,
        value: Value,
    ) -> Result<WriteResult> {
        let typed_value = match value {
            Value::String(s) => TypedValue::StringValue(s),
            Value::Int(i) => TypedValue::IntValue(i),
            Value::Float(f) => TypedValue::FloatValue(f),
            Value::Bool(b) => TypedValue::BoolValue(b),
        };
        let response = self
            .inner
            .write(WriteRequest {
                server,
                tag_id: tag,
                typed_value: Some(typed_value),
            })
            .await?;
        let result = response.into_inner();
        Ok(WriteResult {
            tag_id: result.tag_id,
            success: result.success,
            error: result.error,
        })
    }
}

fn feature_error(operation: &'static str, status: tonic::Status) -> Error {
    if status.code() == Code::Unimplemented {
        Error::IncompatibleGateway { operation }
    } else {
        Error::Rpc(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use crate::{
        BrowseNodeKind, BrowseSource, NamespaceOrganization, SearchIndexControlAction,
        SearchIndexRequest, SearchIndexState, SearchMatchMode,
    };
    use opcda_bridge_proto::bridge::search_event;
    use opcda_bridge_proto::bridge::{
        BrowseNode as ProtoBrowseNode, BrowsePage as ProtoBrowsePage,
        BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse, GetGatewayInfoResponse,
        IndexedSearchMatch, IndexedSearchProgress, ListServersResponse,
        NamespaceOrganization as ProtoOrganization, ProtocolFeature, ProtocolFeatureKind,
        ReadResponse, SearchCompleted, SearchEvent as ProtoSearchEvent, SearchIndexResponse,
        SearchIndexState as ProtoSearchIndexState, SearchIndexStatus, SearchProgress,
        TagValue as ProtoTagValue, WriteResponse,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tonic::Status;

    fn item_node() -> ProtoBrowseNode {
        ProtoBrowseNode {
            node_key: "node".into(),
            display_name: "PV".into(),
            kind: opcda_bridge_proto::bridge::BrowseNodeKind::Item as i32,
            item_id: Some("FCS!TAG.PV".into()),
        }
    }

    #[tokio::test]
    async fn connect_success_and_failure_are_typed() {
        let host = start_mock_server(MockBridgeService::default()).await;
        Client::connect(&host).await.unwrap();
        assert!(matches!(
            Client::connect("127.0.0.1:1").await.unwrap_err(),
            Error::Connect(_)
        ));
    }

    #[tokio::test]
    async fn mock_server_shutdown_completes() {
        let service = MockBridgeService::default();
        let shutdown = Arc::clone(&service.server_shutdown);
        let stopped = Arc::clone(&service.server_stopped);
        let _host = start_mock_server(service).await;
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(1), stopped.notified())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn capabilities_maps_fields_and_request() {
        let service = MockBridgeService {
            capabilities_response: GetCapabilitiesResponse {
                application_version: "0.3.0".into(),
                protocol_version: "0.3".into(),
                max_page_size: 1000,
                supports_browse_sessions: true,
                supports_search: true,
                organization: ProtoOrganization::Hierarchical as i32,
                source: ProtoBrowseSource::Da2 as i32,
                supports_indexed_search: true,
                indexed_search_protocol_version: "1".into(),
                max_indexed_search_results: 50,
                search_index_state: ProtoSearchIndexState::Ready as i32,
            },
            ..Default::default()
        };
        let requests = Arc::clone(&service.capabilities_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let capabilities = client.capabilities("S").await.unwrap();
        assert_eq!(
            capabilities.organization,
            NamespaceOrganization::Hierarchical
        );
        assert_eq!(capabilities.source, BrowseSource::Da2);
        assert!(capabilities.supports_indexed_search);
        assert_eq!(capabilities.indexed_search_protocol_version, "1");
        assert_eq!(capabilities.max_indexed_search_results, 50);
        assert_eq!(capabilities.search_index_state, SearchIndexState::Ready);
        assert_eq!(requests.lock().unwrap()[0].server, "S");
    }

    #[tokio::test]
    async fn gateway_info_and_compatibility_report_are_typed() {
        let service = MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.4.3".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::IndexedSearch as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                ],
            },
            ..Default::default()
        };
        let requests = Arc::clone(&service.gateway_info_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let info = client.gateway_info().await.unwrap();
        assert_eq!(info.application_version, "0.4.3");
        let report = client
            .compatibility_with_client_version(None, "0.4.3")
            .await
            .unwrap();
        assert_eq!(report.status, crate::CompatibilityStatus::Full);
        assert_eq!(report.library_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn compatibility_wrapper_and_gateway_info_errors_are_typed() {
        let host = start_mock_server(MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.4.3".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::IndexedSearch as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                ],
            },
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert_eq!(
            client.compatibility(None).await.unwrap().status,
            crate::CompatibilityStatus::Full
        );

        let host = start_mock_server(MockBridgeService {
            gateway_info_error: Some(Status::internal("gateway unavailable")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.compatibility(None).await.unwrap_err(),
            Error::Rpc(_)
        ));
    }

    #[tokio::test]
    async fn compatibility_falls_back_to_legacy_or_reports_unknown() {
        let service = MockBridgeService {
            gateway_info_error: Some(Status::unimplemented("old gateway")),
            capabilities_response: GetCapabilitiesResponse {
                application_version: "0.3.2".into(),
                protocol_version: "2".into(),
                supports_indexed_search: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let report = client
            .compatibility_with_client_version(Some("S"), "0.4.3")
            .await
            .unwrap();
        assert_eq!(
            report.source,
            crate::CompatibilitySource::LegacyCapabilities
        );
        assert_eq!(report.status, crate::CompatibilityStatus::Partial);

        let host = start_mock_server(MockBridgeService {
            gateway_info_error: Some(Status::unimplemented("old gateway")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        let report = client
            .compatibility_with_client_version(None, "0.4.3")
            .await
            .unwrap();
        assert_eq!(report.status, crate::CompatibilityStatus::Unknown);
    }

    #[tokio::test]
    async fn capabilities_rpc_error_is_typed() {
        let host = start_mock_server(MockBridgeService {
            capabilities_error: Some(Status::unimplemented("old gateway")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.capabilities("S").await.unwrap_err(),
            Error::IncompatibleGateway { .. }
        ));
    }

    #[tokio::test]
    async fn list_servers_maps_data_and_errors() {
        let host = start_mock_server(MockBridgeService {
            list_servers_response: ListServersResponse {
                servers: vec!["S1".into(), "S2".into()],
            },
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert_eq!(client.list_servers().await.unwrap(), ["S1", "S2"]);

        let host = start_mock_server(MockBridgeService {
            list_servers_error: Some(Status::internal("boom")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.list_servers().await.unwrap_err(),
            Error::Rpc(_)
        ));
    }

    #[tokio::test]
    async fn browse_returns_one_typed_page_without_draining() {
        let service = MockBridgeService {
            browse_response: ProtoBrowsePage {
                session_id: "session".into(),
                nodes: vec![item_node()],
                next_page_token: Some("next".into()),
                complete: false,
                organization: ProtoOrganization::Hierarchical as i32,
                source: ProtoBrowseSource::Da3 as i32,
                warning: None,
            },
            ..Default::default()
        };
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let page = client.browse("S", 25).await.unwrap();
        assert_eq!(page.nodes[0].kind, BrowseNodeKind::Item);
        assert_eq!(page.next_page_token.as_deref(), Some("next"));
        let request = &requests.lock().unwrap()[0];
        assert_eq!(request.page_size, 25);
        assert!(request.session_id.is_none());
    }

    #[tokio::test]
    async fn browse_page_forwards_session_parent_token_and_refresh() {
        let service = MockBridgeService::default();
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        client
            .browse_page(
                BrowsePageRequest::next("S", "session", Some("parent".into()), "token", 10)
                    .with_refresh(true),
            )
            .await
            .unwrap();
        let request = &requests.lock().unwrap()[0];
        assert_eq!(request.session_id.as_deref(), Some("session"));
        assert_eq!(request.parent_node_key.as_deref(), Some("parent"));
        assert_eq!(request.page_token.as_deref(), Some("token"));
        assert!(request.refresh);
    }

    #[tokio::test]
    async fn browse_rpc_and_protocol_errors_are_typed() {
        let host = start_mock_server(MockBridgeService {
            browse_error: Some(Status::failed_precondition("expired")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.open_browse("S", 20).await.unwrap_err(),
            Error::Rpc(_)
        ));

        let host = start_mock_server(MockBridgeService {
            browse_response: ProtoBrowsePage::default(),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.open_browse("S", 20).await.unwrap_err(),
            Error::Protocol(_)
        ));

        let host = start_mock_server(MockBridgeService {
            browse_error: Some(Status::unimplemented("old gateway")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.open_browse("S", 20).await.unwrap_err(),
            Error::IncompatibleGateway { .. }
        ));
    }

    #[tokio::test]
    async fn close_browse_session_forwards_id_and_error() {
        let service = MockBridgeService::default();
        let requests = Arc::clone(&service.close_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        client.close_browse_session("session").await.unwrap();
        assert_eq!(requests.lock().unwrap()[0].session_id, "session");

        let host = start_mock_server(MockBridgeService {
            close_error: Some(Status::not_found("missing")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.close_browse_session("missing").await.unwrap_err(),
            Error::Rpc(_)
        ));
    }

    #[tokio::test]
    async fn search_stream_and_collect_map_events_and_request() {
        let events = vec![
            ProtoSearchEvent {
                event: Some(search_event::Event::Progress(SearchProgress {
                    visited_nodes: 5,
                    matches: 0,
                    partial: true,
                })),
            },
            ProtoSearchEvent {
                event: Some(search_event::Event::Completed(SearchCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                })),
            },
        ];
        let service = MockBridgeService {
            search_events: events,
            ..Default::default()
        };
        let requests = Arc::clone(&service.search_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let mut request = SearchRequest::new("S", "PV", SearchMatchMode::Prefix);
        request.session_id = Some("session".into());
        request.scope_node_key = Some("scope".into());
        request.max_results = 50;
        request.include_branches = true;
        request.refresh = true;
        let found = client.search(request).await.unwrap();
        assert_eq!(found.len(), 2);
        let request = &requests.lock().unwrap()[0];
        assert_eq!(request.query, "PV");
        assert_eq!(
            request.match_mode,
            opcda_bridge_proto::bridge::SearchMatchMode::Prefix as i32
        );
        assert!(request.include_branches);
        assert!(request.refresh);
    }

    #[tokio::test]
    async fn search_initial_stream_and_protocol_errors_are_typed() {
        let host = start_mock_server(MockBridgeService {
            search_initial_error: Some(Status::unavailable("down")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client
                .search_stream(SearchRequest::new("S", "PV", SearchMatchMode::Exact))
                .await
                .unwrap_err(),
            Error::Rpc(_)
        ));

        let host = start_mock_server(MockBridgeService {
            search_stream_error: Some(Status::deadline_exceeded("slow")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client
                .search(SearchRequest::new("S", "PV", SearchMatchMode::Exact))
                .await
                .unwrap_err(),
            Error::Rpc(_)
        ));

        let host = start_mock_server(MockBridgeService {
            search_events: vec![ProtoSearchEvent::default()],
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client
                .search(SearchRequest::new("S", "PV", SearchMatchMode::Exact))
                .await
                .unwrap_err(),
            Error::Protocol(_)
        ));

        assert!(matches!(
            feature_error("test", Status::unimplemented("old")),
            Error::IncompatibleGateway { .. }
        ));
    }

    fn index_status(state: ProtoSearchIndexState) -> SearchIndexStatus {
        SearchIndexStatus {
            server: "S".into(),
            state: state as i32,
            configured: true,
            active_generation: 4,
            entry_count: 100,
            unique_item_count: 99,
            started_at: Some("start".into()),
            completed_at: Some("complete".into()),
            last_error: None,
            database_bytes: 1024,
            organization: ProtoOrganization::Hierarchical as i32,
            source: ProtoBrowseSource::Da3 as i32,
            progress: Some(IndexedSearchProgress {
                branches_visited: 2,
                entries_seen: 3,
                unique_items: 3,
                active_time_ms: 4,
                paused_time_ms: 5,
                items_per_second: 6.0,
                estimated_remaining_ms: Some(7),
            }),
        }
    }

    #[tokio::test]
    async fn indexed_search_methods_map_requests_responses_and_errors() {
        let service = MockBridgeService {
            search_index_status_response: index_status(ProtoSearchIndexState::Ready),
            refresh_search_index_response: index_status(ProtoSearchIndexState::Refreshing),
            control_search_index_response: index_status(ProtoSearchIndexState::Partial),
            search_index_response: SearchIndexResponse {
                matches: vec![IndexedSearchMatch {
                    item_id: "Exact.ItemID".into(),
                    display_name: "PV".into(),
                    kind: opcda_bridge_proto::bridge::BrowseNodeKind::Item as i32,
                    breadcrumbs: vec!["Area".into()],
                }],
                has_more: true,
                status: Some(index_status(ProtoSearchIndexState::Stale)),
            },
            ..Default::default()
        };
        let status_requests = Arc::clone(&service.search_index_status_requests);
        let refresh_requests = Arc::clone(&service.refresh_search_index_requests);
        let control_requests = Arc::clone(&service.control_search_index_requests);
        let search_requests = Arc::clone(&service.search_index_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();

        assert_eq!(
            client.search_index_status("S").await.unwrap().state,
            SearchIndexState::Ready
        );
        assert_eq!(
            client.refresh_search_index("S", true).await.unwrap().state,
            SearchIndexState::Refreshing
        );
        assert_eq!(
            client
                .control_search_index("S", SearchIndexControlAction::Pause)
                .await
                .unwrap()
                .state,
            SearchIndexState::Partial
        );
        let mut request = SearchIndexRequest::new("S", "PV", SearchMatchMode::Contains);
        request.max_results = 25;
        let response = client.search_index(request).await.unwrap();
        assert_eq!(response.matches[0].item_id, "Exact.ItemID");
        assert!(response.has_more);

        assert_eq!(status_requests.lock().unwrap()[0].server, "S");
        assert!(refresh_requests.lock().unwrap()[0].force);
        assert_eq!(
            control_requests.lock().unwrap()[0].action,
            opcda_bridge_proto::bridge::SearchIndexControlAction::Pause as i32
        );
        assert_eq!(search_requests.lock().unwrap()[0].max_results, 25);

        for (field, operation) in [
            ("search_index_status_error", "status"),
            ("refresh_search_index_error", "refresh"),
            ("control_search_index_error", "control"),
            ("search_index_error", "search"),
        ] {
            let mut service = MockBridgeService::default();
            let error = Some(Status::unimplemented("old gateway"));
            match field {
                "search_index_status_error" => service.search_index_status_error = error,
                "refresh_search_index_error" => service.refresh_search_index_error = error,
                "control_search_index_error" => service.control_search_index_error = error,
                "search_index_error" => service.search_index_error = error,
                _ => unreachable!(),
            }
            let host = start_mock_server(service).await;
            let mut client = Client::connect(&host).await.unwrap();
            let error = match operation {
                "status" => client.search_index_status("S").await.unwrap_err(),
                "refresh" => client.refresh_search_index("S", false).await.unwrap_err(),
                "control" => client
                    .control_search_index("S", SearchIndexControlAction::Cancel)
                    .await
                    .unwrap_err(),
                "search" => client
                    .search_index(SearchIndexRequest::new(
                        "S",
                        "PV",
                        SearchMatchMode::Contains,
                    ))
                    .await
                    .unwrap_err(),
                _ => unreachable!(),
            };
            assert!(matches!(error, Error::IncompatibleGateway { .. }));
        }
    }

    #[tokio::test]
    async fn read_maps_data_and_errors() {
        let host = start_mock_server(MockBridgeService {
            read_response: ReadResponse {
                values: ["AUT", "", "A\"B", "\"AUT\""]
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| ProtoTagValue {
                        tag_id: format!("t{index}"),
                        value: value.into(),
                        quality: "Good".into(),
                        timestamp: "now".into(),
                    })
                    .collect(),
            },
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        let values = client.read("S".into(), vec![]).await.unwrap();
        assert_eq!(
            values
                .iter()
                .map(|value| value.value.as_str())
                .collect::<Vec<_>>(),
            vec!["AUT", "", "A\"B", "\"AUT\""]
        );

        let host = start_mock_server(MockBridgeService {
            read_error: Some(Status::internal("boom")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client.read("S".into(), vec![]).await.unwrap_err(),
            Error::Rpc(_)
        ));
    }

    #[tokio::test]
    async fn write_maps_every_value_and_result_or_error() {
        for value in [
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.5),
            Value::String("text".into()),
        ] {
            let host = start_mock_server(MockBridgeService {
                write_response: WriteResponse {
                    tag_id: "t".into(),
                    success: true,
                    error: None,
                },
                ..Default::default()
            })
            .await;
            let mut client = Client::connect(&host).await.unwrap();
            assert!(
                client
                    .write("S".into(), "t".into(), value)
                    .await
                    .unwrap()
                    .success
            );
        }

        let host = start_mock_server(MockBridgeService {
            write_error: Some(Status::internal("boom")),
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert!(matches!(
            client
                .write("S".into(), "t".into(), Value::Int(1))
                .await
                .unwrap_err(),
            Error::Rpc(_)
        ));
    }
}
