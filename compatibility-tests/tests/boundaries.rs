use async_trait::async_trait;
use current_gateway::config::{GatewayConfig, IndexConfig};
use current_gateway::opc::{
    BrowseCapabilities as CurrentBrowseCapabilities, BrowseNode as CurrentBrowseNode,
    BrowsePage as CurrentBrowsePage, BrowseSource as CurrentBrowseSource, InventoryControl,
    InventoryEvent, InventoryHandle, InventoryStream,
    NamespaceOrganization as CurrentNamespaceOrganization, OpcClient as CurrentOpcClient,
    OpcValue as CurrentOpcValue, TagValue as CurrentTagValue, WriteResult as CurrentWriteResult,
};
use current_gateway::server::BridgeService as CurrentBridgeService;
use current_proto::bridge::bridge_server::BridgeServer as CurrentBridgeServer;
use historical_gateway_032::opc::{
    BrowseCapabilities as HistoricalBrowseCapabilities, BrowseNode as HistoricalBrowseNode,
    BrowseNodeKind as HistoricalBrowseNodeKind, BrowsePage as HistoricalBrowsePage,
    BrowseSource as HistoricalBrowseSource,
    NamespaceOrganization as HistoricalNamespaceOrganization, OpcClient as HistoricalOpcClient,
    OpcValue as HistoricalOpcValue, TagValue as HistoricalTagValue,
    WriteResult as HistoricalWriteResult,
};
use historical_gateway_032::server::BridgeService as HistoricalBridgeService;
use historical_proto_032::bridge::bridge_server::BridgeServer as HistoricalBridgeServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const SERVER: &str = "Mock.Server";
const TAG: &str = "Mock.Tag";

#[derive(Default)]
struct HistoricalMockOpcClient;

#[async_trait]
impl HistoricalOpcClient for HistoricalMockOpcClient {
    async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
        Ok(vec![SERVER.into()])
    }

    async fn get_capabilities(
        &self,
        _server: &str,
    ) -> anyhow::Result<HistoricalBrowseCapabilities> {
        Ok(HistoricalBrowseCapabilities {
            organization: HistoricalNamespaceOrganization::Hierarchical,
            source: HistoricalBrowseSource::Da2,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 200,
        })
    }

    async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
        Ok("historical-session".into())
    }

    async fn browse_page(
        &self,
        _session_id: &str,
        _parent_node_key: Option<&str>,
        _page_token: Option<&str>,
        _page_size: u32,
        _refresh: bool,
    ) -> anyhow::Result<HistoricalBrowsePage> {
        Ok(HistoricalBrowsePage {
            nodes: vec![HistoricalBrowseNode {
                node_key: "historical-node".into(),
                display_name: TAG.into(),
                kind: HistoricalBrowseNodeKind::Item,
                item_id: Some(TAG.into()),
            }],
            next_page_token: None,
            complete: true,
            organization: HistoricalNamespaceOrganization::Hierarchical,
            source: HistoricalBrowseSource::Da2,
            warning: None,
        })
    }

    async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn read_tag_values(
        &self,
        _server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<HistoricalTagValue>> {
        Ok(tag_ids
            .into_iter()
            .map(|tag_id| HistoricalTagValue {
                tag_id,
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "2026-01-01T00:00:00Z".into(),
            })
            .collect())
    }

    async fn write_tag_value(
        &self,
        _server: &str,
        tag_id: &str,
        _value: HistoricalOpcValue,
    ) -> anyhow::Result<HistoricalWriteResult> {
        Ok(HistoricalWriteResult {
            tag_id: tag_id.into(),
            success: true,
            error: None,
        })
    }
}

#[derive(Default)]
struct CurrentMockOpcClient;

#[async_trait]
impl CurrentOpcClient for CurrentMockOpcClient {
    async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
        Ok(vec![SERVER.into()])
    }

    async fn get_capabilities(&self, _server: &str) -> anyhow::Result<CurrentBrowseCapabilities> {
        Ok(CurrentBrowseCapabilities {
            organization: CurrentNamespaceOrganization::Hierarchical,
            source: CurrentBrowseSource::Da2,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 200,
        })
    }

    async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
        Ok("current-session".into())
    }

    async fn browse_page(
        &self,
        _session_id: &str,
        _parent_node_key: Option<&str>,
        _page_token: Option<&str>,
        _page_size: u32,
        _refresh: bool,
    ) -> anyhow::Result<CurrentBrowsePage> {
        Ok(CurrentBrowsePage {
            nodes: vec![CurrentBrowseNode {
                node_key: "current-node".into(),
                display_name: TAG.into(),
                kind: current_gateway::opc::BrowseNodeKind::Item,
                item_id: Some(TAG.into()),
            }],
            next_page_token: None,
            complete: true,
            organization: CurrentNamespaceOrganization::Hierarchical,
            source: CurrentBrowseSource::Da2,
            warning: None,
        })
    }

    async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn start_inventory(
        &self,
        _server: &str,
        _batch_size: u32,
    ) -> anyhow::Result<InventoryHandle> {
        Ok(InventoryHandle {
            stream: Box::new(EmptyInventoryStream),
            control: Arc::new(EmptyInventoryControl),
        })
    }

    async fn read_tag_values(
        &self,
        _server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<CurrentTagValue>> {
        Ok(tag_ids
            .into_iter()
            .map(|tag_id| CurrentTagValue {
                tag_id,
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "2026-01-01T00:00:00Z".into(),
            })
            .collect())
    }

    async fn write_tag_value(
        &self,
        _server: &str,
        tag_id: &str,
        _value: CurrentOpcValue,
    ) -> anyhow::Result<CurrentWriteResult> {
        Ok(CurrentWriteResult {
            tag_id: tag_id.into(),
            success: true,
            error: None,
        })
    }
}

struct EmptyInventoryStream;

#[async_trait]
impl InventoryStream for EmptyInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        None
    }
}

struct EmptyInventoryControl;

impl InventoryControl for EmptyInventoryControl {
    fn pause(&self) {}
    fn resume(&self) {}
    fn cancel(&self) {}
}

async fn start_historical_gateway() -> (String, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, signal) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(HistoricalBridgeServer::new(HistoricalBridgeService::new(
                HistoricalMockOpcClient,
            )))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = signal.await;
            })
            .await
            .unwrap();
    });
    (format_address(address), shutdown)
}

async fn start_current_gateway() -> (String, oneshot::Sender<()>, TempDir) {
    let tempdir = tempfile::tempdir().unwrap();
    let config = GatewayConfig {
        index: IndexConfig {
            database_path: Some(
                tempdir
                    .path()
                    .join("index.sqlite3")
                    .to_string_lossy()
                    .into_owned(),
            ),
            enabled: Some(false),
            ..Default::default()
        },
        ..Default::default()
    };
    let service = CurrentBridgeService::with_index_config(CurrentMockOpcClient, &config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, signal) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(CurrentBridgeServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = signal.await;
            })
            .await
            .unwrap();
    });
    (format_address(address), shutdown, tempdir)
}

fn format_address(address: SocketAddr) -> String {
    format!("127.0.0.1:{}", address.port())
}

#[tokio::test]
async fn current_client_reads_writes_and_browses_historical_gateway() {
    let (host, shutdown) = start_historical_gateway().await;
    let mut client = current_client::Client::connect(&host).await.unwrap();

    let capabilities = client.capabilities(SERVER).await.unwrap();
    assert_eq!(capabilities.protocol_version, "2");
    let page = client.browse(SERVER, 20).await.unwrap();
    assert_eq!(page.nodes[0].item_id.as_deref(), Some(TAG));
    let values = client.read(SERVER.into(), vec![TAG.into()]).await.unwrap();
    assert_eq!(values[0].value, "42");
    assert!(
        client
            .write(SERVER.into(), TAG.into(), current_client::Value::Int(42))
            .await
            .unwrap()
            .success
    );

    let report = client
        .compatibility_with_client_version(Some(SERVER), "0.4.3")
        .await
        .unwrap();
    assert_eq!(
        report.source,
        current_client::CompatibilitySource::LegacyCapabilities
    );
    assert_eq!(report.status, current_client::CompatibilityStatus::Partial);

    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn historical_client_reads_writes_and_browses_current_gateway() {
    let (host, shutdown, _tempdir) = start_current_gateway().await;
    let mut client = historical_client_032::Client::connect(&host).await.unwrap();

    let capabilities = client.capabilities(SERVER).await.unwrap();
    assert_eq!(capabilities.protocol_version, "2");
    let page = client.browse(SERVER, 20).await.unwrap();
    assert_eq!(page.nodes[0].item_id.as_deref(), Some(TAG));
    let values = client.read(SERVER.into(), vec![TAG.into()]).await.unwrap();
    assert_eq!(values[0].value, "42");
    assert!(
        client
            .write(
                SERVER.into(),
                TAG.into(),
                historical_client_032::Value::Int(42)
            )
            .await
            .unwrap()
            .success
    );

    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn indexed_client_reaches_current_index_contract() {
    let (host, shutdown, _tempdir) = start_current_gateway().await;
    let mut client = historical_client_040::Client::connect(&host).await.unwrap();

    let status = client.search_index_status(SERVER).await.unwrap();
    assert!(!status.configured);

    shutdown.send(()).unwrap();
}

#[tokio::test]
async fn current_client_and_gateway_are_exact_pair_tested() {
    let (host, shutdown, _tempdir) = start_current_gateway().await;
    let mut client = current_client::Client::connect(&host).await.unwrap();

    let report = client
        .compatibility_with_client_version(None, "0.4.3")
        .await
        .unwrap();
    assert_eq!(report.status, current_client::CompatibilityStatus::Full);
    assert_eq!(
        report.evidence,
        current_client::CompatibilityEvidence::ExactPairTested
    );

    shutdown.send(()).unwrap();
}
