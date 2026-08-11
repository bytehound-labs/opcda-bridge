//! The connected gRPC client: [`Client`] and its typed methods.

use crate::error::Result;
use crate::types::{BrowseNode, TagValue, Value, WriteResult};
use bridge_proto::bridge::bridge_client::BridgeClient;
use bridge_proto::bridge::write_request::TypedValue;
use bridge_proto::bridge::{BrowseRequest, ListServersRequest, ReadRequest, WriteRequest};
use tonic::transport::Channel;

/// A connected client for an opcda-bridge gateway's gRPC API.
///
/// Every method takes `&mut self`, matching the generated `BridgeClient`'s
/// own requirement (it buffers per-call codec state); the underlying
/// `tonic` channel itself is a cheap-to-reuse, multiplexed HTTP/2
/// connection, so a single `Client` is meant to be held and reused across
/// many calls rather than reconnected per request (unlike
/// `opcda-bridge-client`'s CLI, which is a fresh process per invocation and
/// so never notices the difference).
#[derive(Debug)]
pub struct Client {
    inner: BridgeClient<Channel>,
}

impl Client {
    /// Connect to a gateway at `host` (e.g. `"localhost:7600"`).
    ///
    /// Matches `opcda-bridge-client`'s long-standing `http://{host}` scheme
    /// assumption: the gateway only ever serves plaintext HTTP/2 (no TLS).
    pub async fn connect(host: &str) -> Result<Self> {
        let inner = BridgeClient::connect(format!("http://{host}")).await?;
        Ok(Self { inner })
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

    /// Browse one level of `server`'s tag tree rooted at `path` (empty for
    /// the top level), fully materialized into a `Vec` rather than a raw
    /// stream. Every current caller (the CLI, and any bhtune-style
    /// consumer) wants a complete result before doing anything else, so
    /// this drains the stream internally rather than exposing it, sparing
    /// callers a dependency on `tokio-stream`/`futures` just to consume it.
    ///
    /// `flat` and `max_tags` are forwarded to the gateway unchanged; the
    /// gateway alone decides how they affect what's returned (e.g. `flat`
    /// yielding every descendant tag instead of one level). `path` selects
    /// which branch of the tree to browse.
    pub async fn browse(
        &mut self,
        server: String,
        flat: bool,
        path: String,
        max_tags: u32,
    ) -> Result<Vec<BrowseNode>> {
        let mut stream = self
            .inner
            .browse(BrowseRequest {
                server,
                flat,
                path,
                max_tags,
            })
            .await?
            .into_inner();

        let mut nodes = Vec::new();
        while let Some(response) = stream.message().await? {
            nodes.push(BrowseNode {
                tag_id: response.tag_id,
                node_type: response.node_type,
            });
        }
        Ok(nodes)
    }

    /// Read one or more tag values from `server`.
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

    /// Write `value` to `tag` on `server`.
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
        let r = response.into_inner();
        Ok(WriteResult {
            tag_id: r.tag_id,
            success: r.success,
            error: r.error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use bridge_proto::bridge::{BrowseResponse, bridge_client::BridgeClient as ProtoBridgeClient};
    use bridge_proto::bridge::{
        ListServersResponse, ReadResponse, TagValue as ProtoTagValue, WriteResponse,
    };
    use tonic::Status;

    #[tokio::test]
    async fn test_connect_success() {
        let host = start_mock_server(MockBridgeService::default()).await;
        Client::connect(&host).await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_failure_is_connect_variant() {
        let err = Client::connect("127.0.0.1:1").await.unwrap_err();
        assert!(matches!(err, Error::Connect(_)));
    }

    #[tokio::test]
    async fn test_connect_failure_anyhow_debug_matches_bare_transport_error() {
        // `opcda-bridge-client`'s commands convert this crate's `Error`
        // into `anyhow::Error` via a bare `?`; this must render identically
        // to today's direct `tonic::transport::Error` -> `anyhow::Error`
        // conversion (the connect helper's pre-this-crate implementation),
        // or the CLI's printed error text would silently change.
        let bare_err = ProtoBridgeClient::connect("http://127.0.0.1:1".to_string())
            .await
            .unwrap_err();
        let bare = anyhow::Error::from(bare_err);

        let wrapped_err = Client::connect("127.0.0.1:1").await.unwrap_err();
        let wrapped = anyhow::Error::from(wrapped_err);

        assert_eq!(format!("{bare:?}"), format!("{wrapped:?}"));
        assert_eq!(bare.to_string(), wrapped.to_string());
    }

    #[tokio::test]
    async fn test_list_servers_empty() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        assert_eq!(client.list_servers().await.unwrap(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_list_servers_with_data() {
        let svc = MockBridgeService {
            list_servers_response: ListServersResponse {
                servers: vec!["Server1".into(), "Server2".into()],
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        assert_eq!(
            client.list_servers().await.unwrap(),
            vec!["Server1".to_string(), "Server2".to_string()]
        );
    }

    #[tokio::test]
    async fn test_list_servers_rpc_error() {
        let svc = MockBridgeService {
            list_servers_error: Some(Status::internal("boom")),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let err = client.list_servers().await.unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
    }

    #[tokio::test]
    async fn test_browse_empty() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        let nodes = client
            .browse("S".into(), false, String::new(), 1000)
            .await
            .unwrap();
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_browse_with_data_maps_fields() {
        let svc = MockBridgeService {
            browse_responses: vec![
                BrowseResponse {
                    tag_id: "tag1".into(),
                    node_type: "Leaf".into(),
                },
                BrowseResponse {
                    tag_id: "tag2".into(),
                    node_type: "Branch".into(),
                },
            ],
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let nodes = client
            .browse("S".into(), true, String::new(), 1000)
            .await
            .unwrap();
        assert_eq!(
            nodes,
            vec![
                BrowseNode {
                    tag_id: "tag1".into(),
                    node_type: "Leaf".into(),
                },
                BrowseNode {
                    tag_id: "tag2".into(),
                    node_type: "Branch".into(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_browse_initial_rpc_error() {
        let svc = MockBridgeService {
            browse_initial_error: Some(Status::unavailable("gateway down")),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let err = client
            .browse("S".into(), false, String::new(), 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
    }

    #[tokio::test]
    async fn test_browse_stream_error_after_items() {
        let svc = MockBridgeService {
            browse_responses: vec![BrowseResponse {
                tag_id: "tag1".into(),
                node_type: "Leaf".into(),
            }],
            browse_stream_error: Some(Status::internal("stream broke")),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let err = client
            .browse("S".into(), false, String::new(), 1000)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
    }

    #[tokio::test]
    async fn test_browse_drop_with_many_items_breaks_server_send_loop() {
        // `Client::browse` always fully drains the stream, so exercising
        // the mock server's own early-disconnect handling (the `break` in
        // `test_support::MockBridgeService::browse`'s background task when
        // the receiver goes away) requires bypassing `Client` and talking
        // to the raw generated `BridgeClient` directly, the same way
        // `opcda-bridge-client`'s own `test_cmd_browse_drop_with_many_items`
        // does against its own copy of the mock service.
        let svc = MockBridgeService {
            browse_responses: (0..300)
                .map(|i| BrowseResponse {
                    tag_id: format!("tag{i}"),
                    node_type: "Leaf".into(),
                })
                .collect(),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = ProtoBridgeClient::connect(format!("http://{host}"))
            .await
            .unwrap();
        let mut stream = client
            .browse(BrowseRequest {
                server: "S".into(),
                flat: false,
                path: String::new(),
                max_tags: 1000,
            })
            .await
            .unwrap()
            .into_inner();
        let _first = stream.message().await;
        drop(stream);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn test_read_empty() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        let values = client.read("S".into(), vec![]).await.unwrap();
        assert!(values.is_empty());
    }

    #[tokio::test]
    async fn test_read_with_data_maps_fields() {
        let svc = MockBridgeService {
            read_response: ReadResponse {
                values: vec![ProtoTagValue {
                    tag_id: "t1".into(),
                    value: "42".into(),
                    quality: "Good".into(),
                    timestamp: "now".into(),
                }],
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let values = client.read("S".into(), vec!["t1".into()]).await.unwrap();
        assert_eq!(
            values,
            vec![TagValue {
                tag_id: "t1".into(),
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "now".into(),
            }]
        );
    }

    #[tokio::test]
    async fn test_read_rpc_error() {
        let svc = MockBridgeService {
            read_error: Some(Status::internal("boom")),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let err = client.read("S".into(), vec![]).await.unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
    }

    #[tokio::test]
    async fn test_write_bool_value() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        client
            .write("S".into(), "tag1".into(), Value::Bool(true))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_int_value() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        client
            .write("S".into(), "tag1".into(), Value::Int(42))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_float_value() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        client
            .write("S".into(), "tag1".into(), Value::Float(9.5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_string_value() {
        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        client
            .write(
                "S".into(),
                "tag1".into(),
                Value::String("hello world".into()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_maps_success_result() {
        let svc = MockBridgeService {
            write_response: WriteResponse {
                tag_id: "t1".into(),
                success: true,
                error: None,
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let result = client
            .write("S".into(), "t1".into(), Value::Int(1))
            .await
            .unwrap();
        assert_eq!(
            result,
            WriteResult {
                tag_id: "t1".into(),
                success: true,
                error: None,
            }
        );
    }

    #[tokio::test]
    async fn test_write_maps_failure_result_with_error() {
        let svc = MockBridgeService {
            write_response: WriteResponse {
                tag_id: "bad".into(),
                success: false,
                error: Some("access denied".into()),
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let result = client
            .write("S".into(), "bad".into(), Value::Int(0))
            .await
            .unwrap();
        assert_eq!(
            result,
            WriteResult {
                tag_id: "bad".into(),
                success: false,
                error: Some("access denied".into()),
            }
        );
    }

    #[tokio::test]
    async fn test_write_rpc_error() {
        let svc = MockBridgeService {
            write_error: Some(Status::internal("boom")),
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = Client::connect(&host).await.unwrap();
        let err = client
            .write("S".into(), "t1".into(), Value::Int(1))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Rpc(_)));
    }
}
