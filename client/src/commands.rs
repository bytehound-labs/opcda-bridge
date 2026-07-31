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
    use bridge_proto::bridge::bridge_server::{Bridge, BridgeServer};
    use bridge_proto::bridge::{
        BrowseResponse, ListServersResponse, ReadResponse, TagValue as ProtoTagValue, WriteResponse,
    };
    use std::net::SocketAddr;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    struct MockBridgeService {
        list_servers_response: ListServersResponse,
        browse_responses: Vec<BrowseResponse>,
        read_response: ReadResponse,
        write_response: WriteResponse,
    }

    #[tonic::async_trait]
    impl Bridge for MockBridgeService {
        async fn list_servers(
            &self,
            _request: Request<ListServersRequest>,
        ) -> Result<Response<ListServersResponse>, Status> {
            Ok(Response::new(self.list_servers_response.clone()))
        }

        type BrowseStream = ReceiverStream<Result<BrowseResponse, Status>>;

        async fn browse(
            &self,
            _request: Request<BrowseRequest>,
        ) -> Result<Response<Self::BrowseStream>, Status> {
            let (tx, rx) = mpsc::channel(4);
            let items = self.browse_responses.clone();
            tokio::spawn(async move {
                for item in items {
                    if tx.send(Ok(item)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn read(
            &self,
            _request: Request<ReadRequest>,
        ) -> Result<Response<ReadResponse>, Status> {
            Ok(Response::new(self.read_response.clone()))
        }

        async fn write(
            &self,
            _request: Request<WriteRequest>,
        ) -> Result<Response<WriteResponse>, Status> {
            Ok(Response::new(self.write_response.clone()))
        }
    }

    async fn start_mock_server(service: MockBridgeService) -> String {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            Server::builder()
                .add_service(BridgeServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("127.0.0.1:{port}")
    }

    fn base_svc() -> MockBridgeService {
        MockBridgeService {
            list_servers_response: ListServersResponse { servers: vec![] },
            browse_responses: vec![],
            read_response: ReadResponse { values: vec![] },
            write_response: WriteResponse {
                tag_id: String::new(),
                success: true,
                error: None,
            },
        }
    }

    #[tokio::test]
    async fn test_cmd_servers_empty() {
        let mut svc = base_svc();
        svc.list_servers_response = ListServersResponse { servers: vec![] };
        let host = start_mock_server(svc).await;
        cmd_servers(host).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_servers_with_data() {
        let mut svc = base_svc();
        svc.list_servers_response = ListServersResponse {
            servers: vec!["Server1".into(), "Server2".into()],
        };
        let host = start_mock_server(svc).await;
        cmd_servers(host).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_empty() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_browse(host, "TestServer".into(), false).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_with_tags() {
        let mut svc = base_svc();
        svc.browse_responses = vec![
            BrowseResponse {
                tag_id: "tag1".into(),
                node_type: "Leaf".into(),
            },
            BrowseResponse {
                tag_id: "tag2".into(),
                node_type: "Branch".into(),
            },
        ];
        let host = start_mock_server(svc).await;
        cmd_browse(host, "TestServer".into(), true).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_empty() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_read(host, "S".into(), vec![]).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_read_with_values() {
        let mut svc = base_svc();
        svc.read_response = ReadResponse {
            values: vec![ProtoTagValue {
                tag_id: "t1".into(),
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "now".into(),
            }],
        };
        let host = start_mock_server(svc).await;
        cmd_read(host, "S".into(), vec!["t1".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_success() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "42".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_failure() {
        let mut svc = base_svc();
        svc.write_response = WriteResponse {
            tag_id: "bad".into(),
            success: false,
            error: Some("access denied".into()),
        };
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "bad".into(), "0".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_float_value() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "3.14".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_bool_value() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "true".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_write_string_value() {
        let svc = base_svc();
        let host = start_mock_server(svc).await;
        cmd_write(host, "S".into(), "tag1".into(), "hello world".into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cmd_browse_drop_early() {
        let mut svc = base_svc();
        svc.browse_responses = vec![
            BrowseResponse {
                tag_id: "tag1".into(),
                node_type: "Leaf".into(),
            },
            BrowseResponse {
                tag_id: "tag2".into(),
                node_type: "Leaf".into(),
            },
        ];
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
        let mut svc = base_svc();
        svc.browse_responses = (0..300)
            .map(|i| BrowseResponse {
                tag_id: format!("tag{i}"),
                node_type: "Leaf".into(),
            })
            .collect();
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
