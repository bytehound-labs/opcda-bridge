use crate::output::{self, OutputFormat};
use opcda_bridge::{BrowseNode, BrowsePage, BrowsePageRequest, Client};
use serde::Serialize;
use tabled::Tabled;
use tabled::derive::display;

#[derive(Debug, Clone, Tabled, Serialize)]
struct BrowseNodeRow {
    #[tabled(rename = "Name")]
    display_name: String,
    #[tabled(rename = "Kind")]
    kind: String,
    #[tabled(rename = "Item ID", display("display::option", ""))]
    item_id: Option<String>,
    #[tabled(rename = "Node Key")]
    node_key: String,
}

impl From<BrowseNode> for BrowseNodeRow {
    fn from(value: BrowseNode) -> Self {
        Self {
            node_key: value.node_key,
            display_name: value.display_name,
            kind: value.kind.to_string(),
            item_id: value.item_id,
        }
    }
}

#[derive(Serialize)]
struct BrowseOutput {
    session_id: String,
    nodes: Vec<BrowseNodeRow>,
    next_page_token: Option<String>,
    complete: bool,
    organization: String,
    source: String,
    warning: Option<String>,
    pages: u32,
}

pub(super) fn render_browse(
    page: BrowsePage,
    pages: u32,
    format: OutputFormat,
) -> anyhow::Result<String> {
    let output = BrowseOutput {
        session_id: page.session_id,
        nodes: page.nodes.into_iter().map(BrowseNodeRow::from).collect(),
        next_page_token: page.next_page_token,
        complete: page.complete,
        organization: page.organization.to_string(),
        source: page.source.to_string(),
        warning: page.warning,
        pages,
    };
    match format {
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&output)?),
        OutputFormat::Table => {
            let table = output::render(output.nodes, OutputFormat::Table)?;
            let continuation = output.next_page_token.as_deref().unwrap_or("none");
            let mut rendered = format!(
                "{table}\nSession: {}\nOrganization: {}\nSource: {}\nComplete: {}\nMore children available: {}\nPages: {}\nNext page token: {continuation}",
                output.session_id,
                output.organization,
                output.source,
                output.complete,
                !output.complete,
                output.pages
            );
            if let Some(warning) = output.warning {
                rendered.push_str(&format!("\nWarning: {warning}"));
            }
            Ok(rendered)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn cmd_browse(
    host: String,
    server: String,
    session_id: Option<String>,
    parent_node_key: Option<String>,
    page_token: Option<String>,
    page_size: u32,
    all: bool,
    max_results: u32,
    refresh: bool,
    format: OutputFormat,
) -> anyhow::Result<()> {
    if page_size == 0 {
        anyhow::bail!("--page-size must be greater than zero");
    }
    if all && max_results == 0 {
        anyhow::bail!("--max-results must be greater than zero with --all");
    }
    if all {
        eprintln!("Warning: --all may be expensive; stopping after at most {max_results} results");
    }

    let first_size = if all {
        page_size.min(max_results)
    } else {
        page_size
    };
    let request = BrowsePageRequest {
        server: server.clone(),
        session_id,
        parent_node_key: parent_node_key.clone(),
        page_token,
        page_size: first_size,
        refresh,
    };
    let mut client = Client::connect(&host).await?;
    let mut combined = client.browse_page(request).await?;
    ensure_page_bound(&combined, first_size)?;
    let mut pages = 1;

    while should_fetch_browse_page(all, combined.complete, combined.nodes.len(), max_results) {
        let token = combined.next_page_token.clone().unwrap_or_default();
        let remaining = max_results - combined.nodes.len() as u32;
        let request_size = page_size.min(remaining);
        let request = BrowsePageRequest::next(
            server.clone(),
            combined.session_id.clone(),
            parent_node_key.clone(),
            token,
            request_size,
        );
        let page = client.browse_page(request).await?;
        ensure_page_bound(&page, request_size)?;
        if page.session_id != combined.session_id {
            anyhow::bail!("gateway changed browse session ID while paging");
        }
        if page.organization != combined.organization || page.source != combined.source {
            anyhow::bail!("gateway changed namespace metadata while paging");
        }
        combined.nodes.extend(page.nodes);
        combined.next_page_token = page.next_page_token;
        combined.complete = page.complete;
        combined.warning = merge_warnings(combined.warning, page.warning);
        pages = next_browse_page_count(pages);
    }

    if stopped_at_browse_safety_cap(all, combined.complete, combined.nodes.len(), max_results) {
        combined.warning = merge_warnings(
            combined.warning,
            Some(format!(
                "stopped at the --all safety cap of {max_results} results; more children are available"
            )),
        );
    }

    // Stdout is the command result required to continue or close the session.
    // codeql[rust/cleartext-logging]: browse session id is the operator continuation handle, not a secret
    println!("{}", render_browse(combined, pages, format)?);
    Ok(())
}

pub(super) fn next_browse_page_count(pages: u32) -> u32 {
    pages + 1
}

pub(super) fn should_fetch_browse_page(
    all: bool,
    complete: bool,
    node_count: usize,
    max_results: u32,
) -> bool {
    all && !complete && node_count < max_results as usize
}

pub(super) fn stopped_at_browse_safety_cap(
    all: bool,
    complete: bool,
    node_count: usize,
    max_results: u32,
) -> bool {
    all && !complete && node_count >= max_results as usize
}

pub(super) fn ensure_page_bound(page: &BrowsePage, requested: u32) -> anyhow::Result<()> {
    if page.nodes.len() > requested as usize {
        anyhow::bail!(
            "gateway returned {} nodes for a requested page size of {requested}",
            page.nodes.len()
        );
    }
    if !page.complete && page.nodes.is_empty() {
        anyhow::bail!("gateway returned an empty incomplete browse page");
    }
    Ok(())
}

pub(super) fn merge_warnings(existing: Option<String>, next: Option<String>) -> Option<String> {
    match (existing, next) {
        (Some(existing), Some(next)) => Some(format!("{existing}; {next}")),
        (Some(existing), None) => Some(existing),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

#[derive(Tabled, Serialize)]
struct CloseSessionRow {
    #[tabled(rename = "Closed Session")]
    session_id: String,
}

pub async fn cmd_close_browse_session(
    host: String,
    session_id: String,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    client.close_browse_session(session_id.clone()).await?;
    println!(
        "{}",
        output::render(vec![CloseSessionRow { session_id }], format)?
    );
    Ok(())
}
