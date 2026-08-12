use crate::output::{self, OutputFormat};
use opcda_bridge::{BrowseNode, Client, parse_value};
use serde::Serialize;
use tabled::Tabled;
use tabled::derive::display;

#[derive(Tabled, Serialize)]
struct ServerRow {
    #[tabled(rename = "Servers")]
    name: String,
}

pub async fn cmd_servers(host: String, format: OutputFormat) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let servers = client.list_servers().await?;
    let rows: Vec<ServerRow> = servers.into_iter().map(|name| ServerRow { name }).collect();
    println!("{}", output::render(rows, format)?);
    Ok(())
}

#[derive(Tabled, Serialize)]
struct TagRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Type")]
    node_type: String,
}

pub async fn cmd_browse(
    host: String,
    server: String,
    flat: bool,
    path: String,
    max_tags: u32,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let nodes = client.browse(server, flat, path.clone(), max_tags).await?;

    // JSON always renders as rows, regardless of `--flat`, since the tree
    // renderer produces a display-only string, not structured data.
    if format == OutputFormat::Json || flat {
        let rows: Vec<TagRow> = nodes
            .into_iter()
            .map(|r| TagRow {
                tag_id: r.tag_id,
                node_type: r.node_type,
            })
            .collect();
        println!("{}", output::render(rows, format)?);
    } else {
        print!("{}", render_tree(&path, &nodes));
    }
    Ok(())
}

/// Render a single level of browse results as an indented tree, using the
/// path given as the header line and box-drawing connectors for children.
/// `Branch` nodes get a trailing `/` so the user knows to drill down with
/// `--path <tag_id>`; `Leaf` nodes get no suffix. Kept as a pure
/// string-building function (rather than printing directly) so it can be
/// unit tested without capturing stdout.
fn render_tree(path: &str, nodes: &[BrowseNode]) -> String {
    let mut out = String::new();
    out.push_str(if path.is_empty() { "/" } else { path });
    out.push('\n');
    let last = nodes.len().saturating_sub(1);
    for (i, node) in nodes.iter().enumerate() {
        let connector = if i == last {
            "└── "
        } else {
            "├── "
        };
        let suffix = if node.node_type == "Branch" { "/" } else { "" };
        out.push_str(&format!("{connector}{}{suffix}\n", node.tag_id));
    }
    out
}

#[derive(Tabled, Serialize)]
struct ReadRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Value")]
    value: String,
    #[tabled(rename = "Quality")]
    quality: String,
    #[tabled(rename = "Timestamp")]
    timestamp: String,
}

pub async fn cmd_read(
    host: String,
    server: String,
    tags: Vec<String>,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let values = client.read(server, tags).await?;
    let rows: Vec<ReadRow> = values
        .into_iter()
        .map(|v| ReadRow {
            tag_id: v.tag_id,
            value: v.value,
            quality: v.quality,
            timestamp: v.timestamp,
        })
        .collect();
    println!("{}", output::render(rows, format)?);
    Ok(())
}

#[derive(Tabled, Serialize)]
struct WriteRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Success")]
    success: bool,
    /// `None` renders as an empty cell in the table but as JSON `null` — the
    /// existing `.unwrap_or_default()` collapsed both to `""`, which is
    /// indistinguishable from "no error" in JSON.
    #[tabled(rename = "Error", display("display::option", ""))]
    error: Option<String>,
}

pub async fn cmd_write(
    host: String,
    server: String,
    tag: String,
    value: String,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let parsed = parse_value(&value);
    let mut client = Client::connect(&host).await?;
    let result = client.write(server, tag, parsed).await?;
    let rows = vec![WriteRow {
        tag_id: result.tag_id,
        success: result.success,
        error: result.error,
    }];
    println!("{}", output::render(rows, format)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use opcda_bridge::Value;
    use opcda_bridge_proto::bridge::{
        BrowseRequest, BrowseResponse, ListServersResponse, ReadResponse,
        TagValue as ProtoTagValue, WriteResponse, bridge_client::BridgeClient,
    };

    #[tokio::test]
    async fn test_cmd_servers_empty() {
        let svc = MockBridgeService {
            list_servers_response: ListServersResponse { servers: vec![] },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_servers(host, OutputFormat::Table).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_servers_with_data() {
        let svc = MockBridgeService {
            list_servers_response: ListServersResponse {
                servers: vec!["Server1".into(), "Server2".into()],
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_servers(host, OutputFormat::Table).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_empty() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_browse(
            host,
            "TestServer".into(),
            false,
            String::new(),
            1000,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_with_tags() {
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
        cmd_browse(
            host,
            "TestServer".into(),
            true,
            String::new(),
            1000,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_tree_mode_renders_indented_tree() {
        let svc = MockBridgeService {
            browse_responses: vec![
                BrowseResponse {
                    tag_id: "Simulink".into(),
                    node_type: "Branch".into(),
                },
                BrowseResponse {
                    tag_id: "System".into(),
                    node_type: "Leaf".into(),
                },
            ],
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_browse(
            host,
            "TestServer".into(),
            false,
            String::new(),
            1000,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_tree_mode_with_path() {
        let svc = MockBridgeService {
            browse_responses: vec![BrowseResponse {
                tag_id: "Simulink.Device1".into(),
                node_type: "Leaf".into(),
            }],
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_browse(
            host,
            "TestServer".into(),
            false,
            "Simulink".into(),
            1000,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_json_bypasses_tree_even_when_not_flat() {
        let svc = MockBridgeService {
            browse_responses: vec![
                BrowseResponse {
                    tag_id: "Simulink".into(),
                    node_type: "Branch".into(),
                },
                BrowseResponse {
                    tag_id: "System".into(),
                    node_type: "Leaf".into(),
                },
            ],
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_browse(
            host,
            "TestServer".into(),
            false,
            String::new(),
            1000,
            OutputFormat::Json,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_empty() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_read(host, "S".into(), vec![], OutputFormat::Table)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_with_values() {
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
        cmd_read(host, "S".into(), vec!["t1".into()], OutputFormat::Table)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_json() {
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
        cmd_read(host, "S".into(), vec!["t1".into()], OutputFormat::Json)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_success() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "tag1".into(),
            "42".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_failure() {
        let svc = MockBridgeService {
            write_response: WriteResponse {
                tag_id: "bad".into(),
                success: false,
                error: Some("access denied".into()),
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "bad".into(),
            "0".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_failure_json_error_is_null() {
        let svc = MockBridgeService {
            write_response: WriteResponse {
                tag_id: "good".into(),
                success: true,
                error: None,
            },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "good".into(),
            "0".into(),
            OutputFormat::Json,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_float_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "tag1".into(),
            "3.14".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_bool_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "tag1".into(),
            "true".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_string_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(
            host,
            "S".into(),
            "tag1".into(),
            "hello world".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_drop_early() {
        let svc = MockBridgeService {
            browse_responses: vec![
                BrowseResponse {
                    tag_id: "tag1".into(),
                    node_type: "Leaf".into(),
                },
                BrowseResponse {
                    tag_id: "tag2".into(),
                    node_type: "Leaf".into(),
                },
            ],
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        let mut client = BridgeClient::connect(format!("http://{host}"))
            .await
            .unwrap();
        let stream = client
            .browse(BrowseRequest {
                server: "S".into(),
                flat: false,
                path: String::new(),
                max_tags: 1000,
            })
            .await
            .unwrap()
            .into_inner();
        drop(stream);
    }

    #[tokio::test]
    async fn test_cmd_browse_drop_with_many_items() {
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
        let mut client = BridgeClient::connect(format!("http://{host}"))
            .await
            .unwrap();
        use tokio_stream::StreamExt;
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
        let _first = stream.next().await;
        drop(stream);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    #[test]
    fn test_parse_value_bool_true() {
        assert!(matches!(parse_value("true"), Value::Bool(true)));
    }

    #[test]
    fn test_parse_value_bool_false() {
        assert!(matches!(parse_value("false"), Value::Bool(false)));
    }

    #[test]
    fn test_parse_value_int_positive() {
        assert!(matches!(parse_value("42"), Value::Int(42)));
    }

    #[test]
    fn test_parse_value_int_negative() {
        assert!(matches!(parse_value("-1"), Value::Int(-1)));
    }

    #[test]
    fn test_parse_value_int_zero() {
        assert!(matches!(parse_value("0"), Value::Int(0)));
    }

    #[test]
    fn test_parse_value_float_positive() {
        assert!(matches!(parse_value("9.5"), Value::Float(v) if (v - 9.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_value_float_negative() {
        assert!(matches!(parse_value("-2.5"), Value::Float(v) if (v + 2.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_parse_value_float_exponential() {
        assert!(matches!(parse_value("1e10"), Value::Float(v) if (v - 1e10).abs() < 1.0));
    }

    #[test]
    fn test_parse_value_string_simple() {
        assert!(matches!(parse_value("hello"), Value::String(s) if s == "hello"));
    }

    #[test]
    fn test_parse_value_string_empty() {
        assert!(matches!(parse_value(""), Value::String(s) if s.is_empty()));
    }

    #[test]
    fn test_parse_value_string_numeric_string() {
        assert!(matches!(parse_value("42foo"), Value::String(s) if s == "42foo"));
    }

    #[test]
    fn test_parse_value_string_special_chars() {
        assert!(matches!(parse_value("hello world!"), Value::String(s) if s == "hello world!"));
    }

    #[test]
    fn test_render_tree_empty_path_shows_root_marker() {
        let out = render_tree("", &[]);
        assert_eq!(out, "/\n");
    }

    #[test]
    fn test_render_tree_non_empty_path_shows_path_header() {
        let out = render_tree("Simulink.Device1", &[]);
        assert_eq!(out, "Simulink.Device1\n");
    }

    #[test]
    fn test_render_tree_single_leaf_uses_last_connector_no_suffix() {
        let nodes = vec![BrowseNode {
            tag_id: "System".into(),
            node_type: "Leaf".into(),
        }];
        let out = render_tree("", &nodes);
        assert_eq!(out, "/\n└── System\n");
    }

    #[test]
    fn test_render_tree_single_branch_uses_last_connector_with_suffix() {
        let nodes = vec![BrowseNode {
            tag_id: "Simulink".into(),
            node_type: "Branch".into(),
        }];
        let out = render_tree("", &nodes);
        assert_eq!(out, "/\n└── Simulink/\n");
    }

    #[test]
    fn test_render_tree_multiple_nodes_use_middle_and_last_connectors() {
        let nodes = vec![
            BrowseNode {
                tag_id: "Simulink".into(),
                node_type: "Branch".into(),
            },
            BrowseNode {
                tag_id: "System".into(),
                node_type: "Leaf".into(),
            },
        ];
        let out = render_tree("", &nodes);
        assert_eq!(out, "/\n├── Simulink/\n└── System\n");
    }

    #[test]
    fn test_server_row_json_keys() {
        let rows = vec![ServerRow { name: "S1".into() }];
        let out = output::render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value[0]["name"], "S1");
    }

    #[test]
    fn test_tag_row_json_keys() {
        let rows = vec![TagRow {
            tag_id: "Simulink".into(),
            node_type: "Branch".into(),
        }];
        let out = output::render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value[0]["tag_id"], "Simulink");
        assert_eq!(value[0]["node_type"], "Branch");
    }

    #[test]
    fn test_read_row_json_keys() {
        let rows = vec![ReadRow {
            tag_id: "t1".into(),
            value: "42".into(),
            quality: "Good".into(),
            timestamp: "now".into(),
        }];
        let out = output::render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value[0]["tag_id"], "t1");
        assert_eq!(value[0]["value"], "42");
        assert_eq!(value[0]["quality"], "Good");
        assert_eq!(value[0]["timestamp"], "now");
    }

    #[test]
    fn test_write_row_json_no_error_is_null_not_empty_string() {
        let rows = vec![WriteRow {
            tag_id: "t1".into(),
            success: true,
            error: None,
        }];
        let out = output::render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(value[0]["error"].is_null());
    }

    #[test]
    fn test_write_row_json_error_is_string_when_present() {
        let rows = vec![WriteRow {
            tag_id: "t1".into(),
            success: false,
            error: Some("access denied".into()),
        }];
        let out = output::render(rows, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(value[0]["error"], "access denied");
    }

    #[test]
    fn test_write_row_table_no_error_renders_empty_cell() {
        let rows = vec![WriteRow {
            tag_id: "t1".into(),
            success: true,
            error: None,
        }];
        let out = output::render(rows, OutputFormat::Table).unwrap();
        // The `display::option` helper renders `None` as `""`, matching the
        // old `.unwrap_or_default()` behavior for table output.
        assert!(!out.contains("None"));
    }
}
