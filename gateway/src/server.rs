use crate::opc::OpcValue;
use crate::opc::{OpcClient, TagValue, WriteResult};
use bridge_proto::bridge::{
    BrowseRequest, BrowseResponse, ListServersRequest, ListServersResponse, ReadRequest,
    ReadResponse, TagValue as ProtoTagValue, WriteRequest, WriteResponse, bridge_server::Bridge,
    write_request::TypedValue as ProtoTypedValue,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

fn internal(e: impl std::fmt::Display) -> Status {
    let message = e.to_string();
    tracing::error!(error = %message, "OPC operation failed");
    Status::internal(message)
}

const NODE_TYPE_LEAF: &str = "Leaf";
const NODE_TYPE_BRANCH: &str = "Branch";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeType {
    Branch,
    Leaf,
}

impl NodeType {
    fn as_str(self) -> &'static str {
        match self {
            NodeType::Branch => NODE_TYPE_BRANCH,
            NodeType::Leaf => NODE_TYPE_LEAF,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowseNode {
    tag_id: String,
    node_type: NodeType,
}

pub struct BridgeService<C> {
    client: C,
}

impl<C: OpcClient> BridgeService<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

#[cfg(target_os = "windows")]
impl Default for BridgeService<crate::opc_da_adapter::OpcDaAdapter> {
    fn default() -> Self {
        Self::new(crate::opc_da_adapter::OpcDaAdapter::default())
    }
}

fn resolve_host(host: &str) -> &str {
    if host.is_empty() { "localhost" } else { host }
}

fn effective_max_tags(max_tags: u32) -> usize {
    if max_tags == 0 {
        1000
    } else {
        max_tags as usize
    }
}

fn select_browse_nodes(
    flat: bool,
    path: &str,
    discovered: Vec<String>,
    tags_sink: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) -> Result<Vec<BrowseNode>, Status> {
    if flat {
        // Flat mode streams every fully-qualified tag discovered, ignoring
        // `path` entirely — it reads from the sink rather than `discovered`
        // so it reflects tags as they're incrementally reported during a
        // live browse, not just the final return value.
        tags_sink
            .lock()
            .map(|guard| {
                guard
                    .iter()
                    .map(|tag_id| BrowseNode {
                        tag_id: tag_id.clone(),
                        node_type: NodeType::Leaf,
                    })
                    .collect()
            })
            .map_err(|_| Status::internal("browse lock poisoned"))
    } else {
        Ok(browse_tree(path, &discovered))
    }
}

/// Synthesize one level of tag hierarchy from a flat list of
/// fully-qualified, dot-separated tag IDs (e.g. `"Simulink.Device1.Python.D"`).
/// Upstream OPC DA browsing is flat-only, so hierarchy is synthesized here
/// by splitting on `.` — the same convention most OPC tooling uses.
///
/// Returns the distinct immediate children directly under `path` (each
/// listed exactly once, in first-seen order), classified as `Branch` when
/// at least one discovered tag extends further below that child, or `Leaf`
/// when the child is itself the end of a tag ID. `path` is matched as a
/// dot-separated prefix — `""` means the root level, `"Simulink"` means
/// direct children of `"Simulink"`, and so on. A trailing `.` on `path` is
/// tolerated and stripped.
fn browse_tree(path: &str, discovered: &[String]) -> Vec<BrowseNode> {
    let path = path.strip_suffix('.').unwrap_or(path);
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}.")
    };

    let mut nodes: Vec<BrowseNode> = Vec::new();
    let mut index_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for tag in discovered {
        let Some(rest) = tag.strip_prefix(prefix.as_str()) else {
            continue;
        };
        if rest.is_empty() {
            // `tag` equals `path` itself exactly — nothing to list below a leaf.
            continue;
        }
        let next_segment = rest.split_once('.').map_or(rest, |(seg, _)| seg);
        let is_branch = rest.len() > next_segment.len();
        let child_id = if path.is_empty() {
            next_segment.to_string()
        } else {
            format!("{path}.{next_segment}")
        };

        match index_of.get(&child_id) {
            Some(&i) => {
                if is_branch {
                    // A later tag proves this child has children of its
                    // own too; Branch always wins over Leaf, and once
                    // upgraded a child is never downgraded back.
                    nodes[i].node_type = NodeType::Branch;
                }
            }
            None => {
                index_of.insert(child_id.clone(), nodes.len());
                nodes.push(BrowseNode {
                    tag_id: child_id,
                    node_type: if is_branch {
                        NodeType::Branch
                    } else {
                        NodeType::Leaf
                    },
                });
            }
        }
    }
    nodes
}

fn map_to_proto_tag_values(values: Vec<TagValue>) -> Vec<ProtoTagValue> {
    values
        .into_iter()
        .map(|v| ProtoTagValue {
            tag_id: v.tag_id,
            value: v.value,
            quality: v.quality,
            timestamp: v.timestamp,
        })
        .collect()
}

fn typed_value_to_opc_value(typed_value: Option<ProtoTypedValue>) -> Result<OpcValue, Status> {
    let tv = typed_value.ok_or_else(|| Status::invalid_argument("no typed_value provided"))?;
    Ok(match tv {
        ProtoTypedValue::StringValue(s) => OpcValue::String(s),
        ProtoTypedValue::IntValue(i) => OpcValue::Int(i),
        ProtoTypedValue::FloatValue(f) => OpcValue::Float(f),
        ProtoTypedValue::BoolValue(b) => OpcValue::Bool(b),
    })
}

fn map_to_write_response(result: WriteResult) -> WriteResponse {
    WriteResponse {
        tag_id: result.tag_id,
        success: result.success,
        error: result.error,
    }
}

#[tonic::async_trait]
impl<C: OpcClient> Bridge for BridgeService<C> {
    #[tracing::instrument(skip(self, request))]
    async fn list_servers(
        &self,
        request: Request<ListServersRequest>,
    ) -> Result<Response<ListServersResponse>, Status> {
        let req = request.into_inner();
        let host = resolve_host(&req.host);
        let servers = self.client.list_servers(host).await.map_err(internal)?;
        tracing::info!(host, count = servers.len(), "listed OPC DA servers");
        Ok(Response::new(ListServersResponse { servers }))
    }

    type BrowseStream = ReceiverStream<std::result::Result<BrowseResponse, Status>>;

    #[tracing::instrument(skip(self, request))]
    async fn browse(
        &self,
        request: Request<BrowseRequest>,
    ) -> Result<Response<Self::BrowseStream>, Status> {
        let req = request.into_inner();
        let max_tags = effective_max_tags(req.max_tags);

        let tags_sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let discovered = self
            .client
            .browse_tags(&req.server, max_tags, progress.clone(), tags_sink.clone())
            .await
            .map_err(internal)?;

        let tags = select_browse_nodes(req.flat, &req.path, discovered, &tags_sink)?;
        tracing::info!(server = %req.server, path = %req.path, count = tags.len(), "browsed OPC DA tags");

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for node in tags {
                if tx
                    .send(Ok(BrowseResponse {
                        tag_id: node.tag_id,
                        node_type: node.node_type.as_str().to_string(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        let values = self
            .client
            .read_tag_values(&req.server, req.tag_ids)
            .await
            .map_err(internal)?;

        tracing::info!(server = %req.server, count = values.len(), "read OPC DA tag values");
        let proto_values = map_to_proto_tag_values(values);

        Ok(Response::new(ReadResponse {
            values: proto_values,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();

        let opc_value = typed_value_to_opc_value(req.typed_value)?;

        let result = self
            .client
            .write_tag_value(&req.server, &req.tag_id, opc_value)
            .await
            .map_err(internal)?;

        tracing::info!(
            server = %req.server,
            tag_id = %req.tag_id,
            success = result.success,
            "wrote OPC DA tag value"
        );
        Ok(Response::new(map_to_write_response(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opc::{OpcValue, TagValue, WriteResult};
    use crate::test_support::MockOpcClient;
    use bridge_proto::bridge::write_request::TypedValue as ProtoTypedValue;
    use std::sync::{Arc, Mutex};

    fn new_bridge_service(
        list_servers: Result<Vec<String>, String>,
        browse_tags: Result<Vec<String>, String>,
        read_tag_values: Result<Vec<TagValue>, String>,
        write_tag_value: Result<WriteResult, String>,
    ) -> BridgeService<MockOpcClient> {
        BridgeService::new(MockOpcClient {
            list_servers_result: Mutex::new(list_servers),
            browse_tags_result: Mutex::new(browse_tags),
            read_tag_values_result: Mutex::new(read_tag_values),
            write_tag_value_result: Mutex::new(write_tag_value),
            ..MockOpcClient::default()
        })
    }

    #[test]
    fn test_internal_converts_error_to_status() {
        let status = internal("test error");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("test error"));
    }

    #[test]
    fn test_internal_with_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let status = internal(io_err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("file not found"));
    }

    #[test]
    fn test_resolve_host_empty() {
        assert_eq!(resolve_host(""), "localhost");
    }

    #[test]
    fn test_resolve_host_provided() {
        assert_eq!(resolve_host("myhost"), "myhost");
    }

    #[test]
    fn test_resolve_host_localhost() {
        assert_eq!(resolve_host("localhost"), "localhost");
    }

    #[test]
    fn test_effective_max_tags_zero() {
        assert_eq!(effective_max_tags(0), 1000);
    }

    #[test]
    fn test_effective_max_tags_provided() {
        assert_eq!(effective_max_tags(42), 42);
    }

    #[test]
    fn test_effective_max_tags_large() {
        assert_eq!(effective_max_tags(u32::MAX), u32::MAX as usize);
    }

    #[test]
    fn test_select_browse_nodes_flat_returns_sink_as_leaves() {
        let discovered = vec!["A.B".to_string()];
        let sink = Arc::new(Mutex::new(vec!["sink1".to_string(), "sink2".to_string()]));
        let result = select_browse_nodes(true, "ignored", discovered, &sink).unwrap();
        assert_eq!(
            result,
            vec![
                BrowseNode {
                    tag_id: "sink1".to_string(),
                    node_type: NodeType::Leaf
                },
                BrowseNode {
                    tag_id: "sink2".to_string(),
                    node_type: NodeType::Leaf
                },
            ]
        );
    }

    #[test]
    fn test_select_browse_nodes_flat_poisoned() {
        let discovered = vec!["discovered".to_string()];
        let sink = Arc::new(Mutex::new(vec![]));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = sink.lock().unwrap();
            panic!("intentional poison");
        }));
        let result = select_browse_nodes(true, "", discovered, &sink);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("poisoned"));
    }

    #[test]
    fn test_select_browse_nodes_not_flat_uses_browse_tree() {
        let discovered = vec!["A.B".to_string(), "A.C".to_string(), "D".to_string()];
        let sink = Arc::new(Mutex::new(vec![]));
        let result = select_browse_nodes(false, "", discovered, &sink).unwrap();
        assert_eq!(
            result,
            vec![
                BrowseNode {
                    tag_id: "A".to_string(),
                    node_type: NodeType::Branch
                },
                BrowseNode {
                    tag_id: "D".to_string(),
                    node_type: NodeType::Leaf
                },
            ]
        );
    }

    #[test]
    fn test_browse_tree_root_single_segment_tags_are_leaves() {
        let discovered = vec!["Foo".to_string(), "Bar".to_string()];
        let nodes = browse_tree("", &discovered);
        assert_eq!(
            nodes,
            vec![
                BrowseNode {
                    tag_id: "Foo".to_string(),
                    node_type: NodeType::Leaf
                },
                BrowseNode {
                    tag_id: "Bar".to_string(),
                    node_type: NodeType::Leaf
                },
            ]
        );
    }

    #[test]
    fn test_browse_tree_root_groups_multi_segment_tags_into_branches() {
        let discovered = vec![
            "Simulink.Device1.Python.D".to_string(),
            "Simulink.Device1.Python.E".to_string(),
            "System.Time".to_string(),
        ];
        let nodes = browse_tree("", &discovered);
        assert_eq!(
            nodes,
            vec![
                BrowseNode {
                    tag_id: "Simulink".to_string(),
                    node_type: NodeType::Branch
                },
                BrowseNode {
                    tag_id: "System".to_string(),
                    node_type: NodeType::Branch
                },
            ]
        );
    }

    #[test]
    fn test_browse_tree_nested_path() {
        let discovered = vec![
            "Simulink.Device1.Python.D".to_string(),
            "Simulink.Device1.Python.E".to_string(),
            "Simulink.Device2".to_string(),
        ];
        let nodes = browse_tree("Simulink.Device1", &discovered);
        assert_eq!(
            nodes,
            vec![BrowseNode {
                tag_id: "Simulink.Device1.Python".to_string(),
                node_type: NodeType::Branch
            }]
        );
    }

    #[test]
    fn test_browse_tree_leaf_at_deepest_level() {
        let discovered = vec!["Simulink.Device1.Python.D".to_string()];
        let nodes = browse_tree("Simulink.Device1.Python", &discovered);
        assert_eq!(
            nodes,
            vec![BrowseNode {
                tag_id: "Simulink.Device1.Python.D".to_string(),
                node_type: NodeType::Leaf
            }]
        );
    }

    #[test]
    fn test_browse_tree_non_existent_path_returns_empty() {
        let discovered = vec!["Simulink.Device1".to_string()];
        let nodes = browse_tree("DoesNotExist", &discovered);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_browse_tree_tolerates_trailing_dot_on_path() {
        let discovered = vec!["Simulink.Device1".to_string()];
        let with_dot = browse_tree("Simulink.", &discovered);
        let without_dot = browse_tree("Simulink", &discovered);
        assert_eq!(with_dot, without_dot);
    }

    #[test]
    fn test_browse_tree_dedups_repeated_branches() {
        let discovered = vec![
            "A.B.C".to_string(),
            "A.B.D".to_string(),
            "A.B.E".to_string(),
        ];
        let nodes = browse_tree("", &discovered);
        assert_eq!(
            nodes,
            vec![BrowseNode {
                tag_id: "A".to_string(),
                node_type: NodeType::Branch
            }]
        );
    }

    #[test]
    fn test_browse_tree_branch_classification_is_order_independent() {
        // A leaf-looking tag ("A.B") and a tag proving "A.B" actually has
        // children ("A.B.C") can arrive in either order; either way "A.B"
        // must end up classified as Branch, never downgraded back to Leaf.
        let leaf_first = vec!["A.B".to_string(), "A.B.C".to_string()];
        let branch_first = vec!["A.B.C".to_string(), "A.B".to_string()];
        let expected = vec![BrowseNode {
            tag_id: "A.B".to_string(),
            node_type: NodeType::Branch,
        }];
        assert_eq!(browse_tree("A", &leaf_first), expected);
        assert_eq!(browse_tree("A", &branch_first), expected);
    }

    #[test]
    fn test_browse_tree_ignores_blank_tag_ids() {
        let discovered = vec!["".to_string(), "Foo".to_string()];
        let nodes = browse_tree("", &discovered);
        assert_eq!(
            nodes,
            vec![BrowseNode {
                tag_id: "Foo".to_string(),
                node_type: NodeType::Leaf
            }]
        );
    }

    #[test]
    fn test_browse_tree_empty_discovered_returns_empty() {
        assert!(browse_tree("", &[]).is_empty());
    }

    #[test]
    fn test_map_to_proto_tag_values_empty() {
        let result = map_to_proto_tag_values(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_map_to_proto_tag_values_single() {
        let tag = TagValue {
            tag_id: "tag1".into(),
            value: "42.5".into(),
            quality: "Good".into(),
            timestamp: "2026-01-01".into(),
        };
        let result = map_to_proto_tag_values(vec![tag]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tag_id, "tag1");
        assert_eq!(result[0].value, "42.5");
        assert_eq!(result[0].quality, "Good");
        assert_eq!(result[0].timestamp, "2026-01-01");
    }

    #[test]
    fn test_map_to_proto_tag_values_multiple() {
        let tags = vec![
            TagValue {
                tag_id: "a".into(),
                value: "1".into(),
                quality: "G".into(),
                timestamp: "t1".into(),
            },
            TagValue {
                tag_id: "b".into(),
                value: "2".into(),
                quality: "B".into(),
                timestamp: "t2".into(),
            },
        ];
        let result = map_to_proto_tag_values(tags);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].tag_id, "a");
        assert_eq!(result[1].tag_id, "b");
    }

    #[test]
    fn test_typed_value_to_opc_missing_value() {
        let result = typed_value_to_opc_value(None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_typed_value_to_opc_string() {
        let result =
            typed_value_to_opc_value(Some(ProtoTypedValue::StringValue("hello".into()))).unwrap();
        assert_eq!(result, OpcValue::String("hello".into()));
    }

    #[test]
    fn test_typed_value_to_opc_int() {
        let result = typed_value_to_opc_value(Some(ProtoTypedValue::IntValue(42))).unwrap();
        assert_eq!(result, OpcValue::Int(42));
    }

    #[test]
    fn test_typed_value_to_opc_negative_int() {
        let result = typed_value_to_opc_value(Some(ProtoTypedValue::IntValue(-1))).unwrap();
        assert_eq!(result, OpcValue::Int(-1));
    }

    #[test]
    fn test_typed_value_to_opc_float() {
        let result = typed_value_to_opc_value(Some(ProtoTypedValue::FloatValue(9.5))).unwrap();
        assert_eq!(result, OpcValue::Float(9.5));
    }

    #[test]
    fn test_typed_value_to_opc_bool_true() {
        let result = typed_value_to_opc_value(Some(ProtoTypedValue::BoolValue(true))).unwrap();
        assert_eq!(result, OpcValue::Bool(true));
    }

    #[test]
    fn test_typed_value_to_opc_bool_false() {
        let result = typed_value_to_opc_value(Some(ProtoTypedValue::BoolValue(false))).unwrap();
        assert_eq!(result, OpcValue::Bool(false));
    }

    #[test]
    fn test_map_to_write_response_success() {
        let wr = WriteResult {
            tag_id: "tag1".into(),
            success: true,
            error: None,
        };
        let response = map_to_write_response(wr);
        assert_eq!(response.tag_id, "tag1");
        assert!(response.success);
        assert_eq!(response.error, None);
    }

    #[test]
    fn test_map_to_write_response_failure() {
        let wr = WriteResult {
            tag_id: "tag1".into(),
            success: false,
            error: Some("write error".into()),
        };
        let response = map_to_write_response(wr);
        assert_eq!(response.tag_id, "tag1");
        assert!(!response.success);
        assert_eq!(response.error, Some("write error".into()));
    }

    #[tokio::test]
    async fn test_list_servers_empty_host_defaults_to_localhost() {
        let svc = new_bridge_service(
            Ok(vec!["Server1".into(), "Server2".into()]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .list_servers(Request::new(ListServersRequest {
                host: String::new(),
            }))
            .await
            .unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.servers, vec!["Server1", "Server2"]);
    }

    #[tokio::test]
    async fn test_list_servers_with_host() {
        let svc = new_bridge_service(
            Ok(vec!["RemoteServer".into()]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .list_servers(Request::new(ListServersRequest {
                host: "192.168.1.1".into(),
            }))
            .await
            .unwrap();
        assert_eq!(response.into_inner().servers, vec!["RemoteServer"]);
    }

    #[tokio::test]
    async fn test_list_servers_error_propagates() {
        let svc = new_bridge_service(
            Err("COM failed".into()),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let result = svc
            .list_servers(Request::new(ListServersRequest {
                host: String::new(),
            }))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("COM failed"));
    }

    #[tokio::test]
    async fn test_browse_flat_returns_sink_tags() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TestServer".into(),
                flat: true,
                path: String::new(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let stream = response.into_inner();
        let items: Vec<_> = stream.collect::<Vec<_>>().await;
        for item in items {
            assert!(item.is_ok());
        }
    }

    #[tokio::test]
    async fn test_browse_not_flat_returns_discovered() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec!["tag1".into(), "tag2".into()]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TestServer".into(),
                flat: false,
                path: String::new(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = response.into_inner();
        let mut tags = Vec::new();
        while let Some(item) = stream.next().await {
            tags.push(item.unwrap().tag_id);
        }
        assert_eq!(tags, vec!["tag1", "tag2"]);
    }

    #[tokio::test]
    async fn test_browse_max_tags_defaults_to_1000() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec!["only".into()]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TS".into(),
                flat: false,
                path: String::new(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = response.into_inner();
        let tag = stream.next().await.unwrap().unwrap();
        assert_eq!(tag.tag_id, "only");
    }

    #[tokio::test]
    async fn test_browse_error_propagates() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Err("connection refused".into()),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let result = svc
            .browse(Request::new(BrowseRequest {
                server: "BadServer".into(),
                flat: false,
                path: String::new(),
                max_tags: 100,
            }))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("connection refused"));
    }

    #[tokio::test]
    async fn test_browse_not_flat_synthesizes_branch_for_dotted_tags() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec!["A.B".into(), "A.C".into()]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TestServer".into(),
                flat: false,
                path: String::new(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = response.into_inner();
        let node = stream.next().await.unwrap().unwrap();
        assert_eq!(node.tag_id, "A");
        assert_eq!(node.node_type, "Branch");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_browse_not_flat_honours_non_root_path() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec!["A.B.C".into(), "A.D".into()]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TestServer".into(),
                flat: false,
                path: "A".into(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = response.into_inner();
        let mut nodes = Vec::new();
        while let Some(item) = stream.next().await {
            let node = item.unwrap();
            nodes.push((node.tag_id, node.node_type));
        }
        assert_eq!(
            nodes,
            vec![
                ("A.B".to_string(), "Branch".to_string()),
                ("A.D".to_string(), "Leaf".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn test_read_single_tag() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![TagValue {
                tag_id: "t1".into(),
                value: "123".into(),
                quality: "Good".into(),
                timestamp: "now".into(),
            }]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .read(Request::new(ReadRequest {
                server: "S".into(),
                tag_ids: vec!["t1".into()],
            }))
            .await
            .unwrap();
        let values = response.into_inner().values;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].tag_id, "t1");
        assert_eq!(values[0].value, "123");
    }

    #[tokio::test]
    async fn test_read_multiple_tags() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![
                TagValue {
                    tag_id: "a".into(),
                    value: "1".into(),
                    quality: "G".into(),
                    timestamp: "t".into(),
                },
                TagValue {
                    tag_id: "b".into(),
                    value: "2".into(),
                    quality: "B".into(),
                    timestamp: "t2".into(),
                },
            ]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .read(Request::new(ReadRequest {
                server: "S".into(),
                tag_ids: vec!["a".into(), "b".into()],
            }))
            .await
            .unwrap();
        let values = response.into_inner().values;
        assert_eq!(values.len(), 2);
    }

    #[tokio::test]
    async fn test_read_error_propagates() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Err("tag not found".into()),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let result = svc
            .read(Request::new(ReadRequest {
                server: "S".into(),
                tag_ids: vec!["nonexistent".into()],
            }))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("tag not found"));
    }

    #[tokio::test]
    async fn test_write_string_value() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: "tag1".into(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "tag1".into(),
                typed_value: Some(ProtoTypedValue::StringValue("hello".into())),
            }))
            .await
            .unwrap();
        let wr = response.into_inner();
        assert_eq!(wr.tag_id, "tag1");
        assert!(wr.success);
    }

    #[tokio::test]
    async fn test_write_int_value() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: "tag_int".into(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "tag_int".into(),
                typed_value: Some(ProtoTypedValue::IntValue(42)),
            }))
            .await
            .unwrap();
        let wr = response.into_inner();
        assert_eq!(wr.tag_id, "tag_int");
        assert!(wr.success);
    }

    #[tokio::test]
    async fn test_write_float_value() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: "tag_f".into(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "tag_f".into(),
                typed_value: Some(ProtoTypedValue::FloatValue(9.5)),
            }))
            .await
            .unwrap();
        let wr = response.into_inner();
        assert_eq!(wr.tag_id, "tag_f");
        assert!(wr.success);
    }

    #[tokio::test]
    async fn test_write_bool_value() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: "tag_bool".into(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "tag_bool".into(),
                typed_value: Some(ProtoTypedValue::BoolValue(false)),
            }))
            .await
            .unwrap();
        let wr = response.into_inner();
        assert_eq!(wr.tag_id, "tag_bool");
        assert!(wr.success);
    }

    #[tokio::test]
    async fn test_write_missing_typed_value() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let result = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "t".into(),
                typed_value: None,
            }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_write_error_propagates() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Err("write failed".into()),
        );
        let result = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "t".into(),
                typed_value: Some(ProtoTypedValue::StringValue("x".into())),
            }))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("write failed"));
    }

    #[tokio::test]
    async fn test_write_failure_with_error_message() {
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: "bad_tag".into(),
                success: false,
                error: Some("access denied".into()),
            }),
        );
        let response = svc
            .write(Request::new(WriteRequest {
                server: "S".into(),
                tag_id: "bad_tag".into(),
                typed_value: Some(ProtoTypedValue::StringValue("x".into())),
            }))
            .await
            .unwrap();
        let wr = response.into_inner();
        assert_eq!(wr.tag_id, "bad_tag");
        assert!(!wr.success);
        assert_eq!(wr.error, Some("access denied".into()));
    }

    #[tokio::test]
    async fn test_browse_stream_break_on_drop() {
        let many_tags: Vec<String> = (0..200).map(|i| format!("tag{i}")).collect();
        let svc = new_bridge_service(
            Ok(vec![]),
            Ok(many_tags),
            Ok(vec![]),
            Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            }),
        );
        let response = svc
            .browse(Request::new(BrowseRequest {
                server: "TS".into(),
                flat: false,
                path: String::new(),
                max_tags: 0,
            }))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
        let mut stream = response.into_inner();
        let _first = stream.next().await;
        drop(stream);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }
}
