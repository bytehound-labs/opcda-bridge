use crate::output::{self, OutputFormat};
use opcda_bridge::Client;
use serde::Serialize;
use tabled::Tabled;

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
        .map(|value| ReadRow {
            tag_id: value.tag_id,
            value: value.value,
            quality: value.quality,
            timestamp: value.timestamp,
        })
        .collect();
    println!("{}", output::render(rows, format)?);
    Ok(())
}
