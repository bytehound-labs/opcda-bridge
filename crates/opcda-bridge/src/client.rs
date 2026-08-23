//! The connected gRPC client and typed search stream.

use crate::error::{Error, Result};
use crate::types::{
    BrowsePage, BrowsePageRequest, Capabilities, SearchEvent, SearchRequest, TagValue, Value,
    WriteResult,
};
use opcda_bridge_proto::bridge::bridge_client::BridgeClient;
use opcda_bridge_proto::bridge::write_request::TypedValue;
use opcda_bridge_proto::bridge::{
    CloseBrowseSessionRequest, GetCapabilitiesRequest, ListServersRequest, ReadRequest,
    WriteRequest,
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
    use crate::{BrowseNodeKind, BrowseSource, NamespaceOrganization, SearchMatchMode};
    use opcda_bridge_proto::bridge::search_event;
    use opcda_bridge_proto::bridge::{
        BrowseNode as ProtoBrowseNode, BrowsePage as ProtoBrowsePage,
        BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse, ListServersResponse,
        NamespaceOrganization as ProtoOrganization, ReadResponse, SearchCompleted,
        SearchEvent as ProtoSearchEvent, SearchProgress, TagValue as ProtoTagValue, WriteResponse,
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
        assert_eq!(requests.lock().unwrap()[0].server, "S");
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

    #[tokio::test]
    async fn read_maps_data_and_errors() {
        let host = start_mock_server(MockBridgeService {
            read_response: ReadResponse {
                values: vec![ProtoTagValue {
                    tag_id: "t1".into(),
                    value: "42".into(),
                    quality: "Good".into(),
                    timestamp: "now".into(),
                }],
            },
            ..Default::default()
        })
        .await;
        let mut client = Client::connect(&host).await.unwrap();
        assert_eq!(
            client.read("S".into(), vec![]).await.unwrap()[0].value,
            "42"
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
