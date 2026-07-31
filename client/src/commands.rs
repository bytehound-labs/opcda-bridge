use bridge_proto::bridge::{
    BrowseRequest, ListServersRequest, ReadRequest, WriteRequest, bridge_client::BridgeClient,
};
use tabled::{Table, Tabled};

#[derive(Tabled)]
struct ServerRow {
    #[tabled(rename = "Servers")]
    name: String,
}

pub async fn cmd_servers(host: String) -> anyhow::Result<()> {
    let mut client = BridgeClient::connect(format!("http://{host}")).await?;
    let response = client
        .list_servers(ListServersRequest {
            host: "localhost".to_string(),
        })
        .await?;
    let servers = response.into_inner().servers;
    let rows: Vec<ServerRow> = servers.into_iter().map(|name| ServerRow { name }).collect();
    println!("{}", Table::new(rows));
    Ok(())
}

#[derive(Tabled)]
struct TagRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Type")]
    node_type: String,
}

pub async fn cmd_browse(host: String, server: String, flat: bool) -> anyhow::Result<()> {
    let mut client = BridgeClient::connect(format!("http://{host}")).await?;
    let mut stream = client
        .browse(BrowseRequest {
            server,
            flat,
            path: String::new(),
            max_tags: 1000,
        })
        .await?
        .into_inner();

    use tokio_stream::StreamExt;
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await {
        let r = response?;
        rows.push(TagRow {
            tag_id: r.tag_id,
            node_type: r.node_type,
        });
    }
    println!("{}", Table::new(rows));
    Ok(())
}

#[derive(Tabled)]
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

pub async fn cmd_read(host: String, server: String, tags: Vec<String>) -> anyhow::Result<()> {
    let mut client = BridgeClient::connect(format!("http://{host}")).await?;
    let response = client
        .read(ReadRequest {
            server,
            tag_ids: tags,
        })
        .await?;
    let values = response.into_inner().values;
    let rows: Vec<ReadRow> = values
        .into_iter()
        .map(|v| ReadRow {
            tag_id: v.tag_id,
            value: v.value,
            quality: v.quality,
            timestamp: v.timestamp,
        })
        .collect();
    println!("{}", Table::new(rows));
    Ok(())
}

#[derive(Tabled)]
struct WriteRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Success")]
    success: bool,
    #[tabled(rename = "Error")]
    error: String,
}

pub async fn cmd_write(
    host: String,
    server: String,
    tag: String,
    value: String,
) -> anyhow::Result<()> {
    let parsed = parse_value(&value);
    let typed_value = match parsed {
        Value::String(s) => bridge_proto::bridge::write_request::TypedValue::StringValue(s),
        Value::Int(i) => bridge_proto::bridge::write_request::TypedValue::IntValue(i),
        Value::Float(f) => bridge_proto::bridge::write_request::TypedValue::FloatValue(f),
        Value::Bool(b) => bridge_proto::bridge::write_request::TypedValue::BoolValue(b),
    };

    let mut client = BridgeClient::connect(format!("http://{host}")).await?;
    let response = client
        .write(WriteRequest {
            server,
            tag_id: tag,
            typed_value: Some(typed_value),
        })
        .await?;
    let r = response.into_inner();
    let rows = vec![WriteRow {
        tag_id: r.tag_id,
        success: r.success,
        error: r.error.unwrap_or_default(),
    }];
    println!("{}", Table::new(rows));
    Ok(())
}

enum Value {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

fn parse_value(raw: &str) -> Value {
    if let Ok(b) = raw.parse::<bool>() {
        return Value::Bool(b);
    }
    if let Ok(i) = raw.parse::<i32>() {
        return Value::Int(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return Value::Float(f);
    }
    Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use bridge_proto::bridge::{
        BrowseResponse, ListServersResponse, ReadResponse, TagValue as ProtoTagValue, WriteResponse,
    };

    #[tokio::test]
    async fn test_cmd_servers_empty() {
        let svc = MockBridgeService {
            list_servers_response: ListServersResponse { servers: vec![] },
            ..Default::default()
        };
        let host = start_mock_server(svc).await;
        cmd_servers(host).await.unwrap();
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
        cmd_servers(host).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_empty() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_browse(host, "TestServer".into(), false).await.unwrap();
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
        cmd_browse(host, "TestServer".into(), true).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_empty() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_read(host, "S".into(), vec![]).await.unwrap();
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
        cmd_read(host, "S".into(), vec!["t1".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_success() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "42".into())
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
        cmd_write(host, "S".into(), "bad".into(), "0".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_float_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "3.14".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_bool_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "true".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_string_value() {
        let svc = MockBridgeService::default();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "hello world".into())
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
}
