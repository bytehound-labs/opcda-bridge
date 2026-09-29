use crate::output::{self, OutputFormat};
use opcda_bridge::Client;
use serde::Serialize;
use tabled::Tabled;

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
