use crate::output::{self, OutputFormat};
use opcda_bridge::{Capabilities, Client};
use serde::Serialize;
use tabled::Tabled;

#[derive(Tabled, Serialize)]
struct CapabilitiesRow {
    #[tabled(rename = "Application Version")]
    application_version: String,
    #[tabled(rename = "Protocol Version")]
    protocol_version: String,
    #[tabled(rename = "Max Page Size")]
    max_page_size: u32,
    #[tabled(rename = "Browse Sessions")]
    supports_browse_sessions: bool,
    #[tabled(rename = "Search")]
    supports_search: bool,
    #[tabled(rename = "Organization")]
    organization: String,
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Indexed Search")]
    supports_indexed_search: bool,
    #[tabled(rename = "Index Protocol")]
    indexed_search_protocol_version: String,
    #[tabled(rename = "Index Max Results")]
    max_indexed_search_results: u32,
    #[tabled(rename = "Index State")]
    search_index_state: String,
    #[tabled(rename = "Index Promoting")]
    search_index_promoting: bool,
}

impl From<Capabilities> for CapabilitiesRow {
    fn from(value: Capabilities) -> Self {
        Self {
            application_version: value.application_version,
            protocol_version: value.protocol_version,
            max_page_size: value.max_page_size,
            supports_browse_sessions: value.supports_browse_sessions,
            supports_search: value.supports_search,
            organization: value.organization.to_string(),
            source: value.source.to_string(),
            supports_indexed_search: value.supports_indexed_search,
            indexed_search_protocol_version: value.indexed_search_protocol_version,
            max_indexed_search_results: value.max_indexed_search_results,
            search_index_state: value.search_index_state.to_string(),
            search_index_promoting: value.search_index_promoting,
        }
    }
}

pub async fn cmd_capabilities(
    host: String,
    server: String,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let row = CapabilitiesRow::from(client.capabilities(server).await?);
    println!("{}", output::render(vec![row], format)?);
    Ok(())
}
