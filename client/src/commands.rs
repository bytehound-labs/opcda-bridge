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
    let mut client = BridgeClient::connect(format!("http://{}", host)).await?;
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
    let mut client = BridgeClient::connect(format!("http://{}", host)).await?;
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
    let mut client = BridgeClient::connect(format!("http://{}", host)).await?;
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

    let mut client = BridgeClient::connect(format!("http://{}", host)).await?;
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
