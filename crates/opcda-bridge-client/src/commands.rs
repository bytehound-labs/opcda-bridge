use crate::output::{self, OutputFormat};
use opcda_bridge::{
    BrowseNode, BrowsePage, BrowsePageRequest, Capabilities, Client, SearchEvent, SearchMatchMode,
    SearchRequest, parse_value,
};
use serde::Serialize;
use std::io::Write;
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

fn render_browse(page: BrowsePage, pages: u32, format: OutputFormat) -> anyhow::Result<String> {
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

    while all && !combined.complete && combined.nodes.len() < max_results as usize {
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
        pages += 1;
    }

    if all && !combined.complete && combined.nodes.len() >= max_results as usize {
        combined.warning = merge_warnings(
            combined.warning,
            Some(format!(
                "stopped at the --all safety cap of {max_results} results; more children are available"
            )),
        );
    }

    println!("{}", render_browse(combined, pages, format)?);
    Ok(())
}

fn ensure_page_bound(page: &BrowsePage, requested: u32) -> anyhow::Result<()> {
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

fn merge_warnings(existing: Option<String>, next: Option<String>) -> Option<String> {
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

#[derive(Debug, Clone, Serialize)]
struct SearchNodeOutput {
    node_key: String,
    display_name: String,
    kind: String,
    item_id: Option<String>,
}

impl From<BrowseNode> for SearchNodeOutput {
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
struct BreadcrumbOutput {
    node_key: String,
    display_name: String,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum SearchOutputEvent {
    Match {
        node: SearchNodeOutput,
        breadcrumbs: Vec<BreadcrumbOutput>,
    },
    Progress {
        visited_nodes: u32,
        matches: u32,
        partial: bool,
    },
    Completed {
        complete: bool,
        cancelled: bool,
        truncated: bool,
        warning: Option<String>,
    },
}

fn search_output_event(event: SearchEvent) -> SearchOutputEvent {
    match event {
        SearchEvent::Match(found) => SearchOutputEvent::Match {
            node: found.node.into(),
            breadcrumbs: found
                .breadcrumbs
                .into_iter()
                .map(|part| BreadcrumbOutput {
                    node_key: part.node_key,
                    display_name: part.display_name,
                })
                .collect(),
        },
        SearchEvent::Progress(progress) => SearchOutputEvent::Progress {
            visited_nodes: progress.visited_nodes,
            matches: progress.matches,
            partial: progress.partial,
        },
        SearchEvent::Completed(completed) => SearchOutputEvent::Completed {
            complete: completed.complete,
            cancelled: completed.cancelled,
            truncated: completed.truncated,
            warning: completed.warning,
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn cmd_search(
    host: String,
    server: String,
    query: String,
    match_mode: SearchMatchMode,
    session_id: Option<String>,
    scope_node_key: Option<String>,
    max_results: u32,
    include_branches: bool,
    refresh: bool,
    format: OutputFormat,
) -> anyhow::Result<()> {
    if max_results == 0 {
        anyhow::bail!("--max-results must be greater than zero");
    }
    let mut request = SearchRequest::new(server, query, match_mode);
    request.session_id = session_id;
    request.scope_node_key = scope_node_key;
    request.max_results = max_results;
    request.include_branches = include_branches;
    request.refresh = refresh;

    let mut client = Client::connect(&host).await?;
    let mut stream = client.search_stream(request).await?;
    while let Some(event) = stream.message().await? {
        match format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string(&search_output_event(event.clone()))?
                );
                std::io::stdout().flush()?;
            }
            OutputFormat::Table => {
                if let SearchEvent::Match(found) = &event {
                    let breadcrumb = found
                        .breadcrumbs
                        .iter()
                        .map(|part| part.display_name.as_str())
                        .collect::<Vec<_>>()
                        .join(" / ");
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        breadcrumb,
                        found.node.display_name,
                        found.node.kind,
                        found.node.item_id.as_deref().unwrap_or(""),
                        found.node.node_key
                    );
                    std::io::stdout().flush()?;
                }
            }
        }

        match event {
            SearchEvent::Progress(progress) => eprintln!(
                "Search progress: visited={}, matches={}, partial={}",
                progress.visited_nodes, progress.matches, progress.partial
            ),
            SearchEvent::Completed(completed) if format == OutputFormat::Table => {
                eprintln!(
                    "Search complete: complete={}, cancelled={}, truncated={}",
                    completed.complete, completed.cancelled, completed.truncated
                );
                if let Some(warning) = completed.warning {
                    eprintln!("Warning: {warning}");
                }
            }
            SearchEvent::Match(_) | SearchEvent::Completed(_) => {}
        }
    }
    Ok(())
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

#[derive(Tabled, Serialize)]
struct WriteRow {
    #[tabled(rename = "Tag")]
    tag_id: String,
    #[tabled(rename = "Success")]
    success: bool,
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
    let rendered = output::render(
        vec![WriteRow {
            tag_id: result.tag_id,
            success: result.success,
            error: result.error,
        }],
        format,
    )
    .expect("write rows contain only infallibly serializable scalar fields");
    println!("{rendered}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use opcda_bridge::{BrowseNodeKind, BrowseSource, NamespaceOrganization};
    use opcda_bridge_proto::bridge::search_event;
    use opcda_bridge_proto::bridge::{
        BrowseBreadcrumb, BrowseNode as ProtoBrowseNode, BrowseNodeKind as ProtoBrowseNodeKind,
        BrowsePage as ProtoBrowsePage, BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse,
        ListServersResponse, NamespaceOrganization as ProtoOrganization, ReadResponse,
        SearchCompleted, SearchEvent as ProtoSearchEvent, SearchMatch, SearchProgress,
        TagValue as ProtoTagValue, WriteResponse,
    };
    use std::sync::Arc;

    fn page(complete: bool, token: Option<&str>, name: &str) -> ProtoBrowsePage {
        ProtoBrowsePage {
            session_id: "session".into(),
            nodes: vec![ProtoBrowseNode {
                node_key: format!("key-{name}"),
                display_name: name.into(),
                kind: ProtoBrowseNodeKind::Item as i32,
                item_id: Some(format!("Item.{name}")),
            }],
            next_page_token: token.map(str::to_string),
            complete,
            organization: ProtoOrganization::Hierarchical as i32,
            source: ProtoBrowseSource::Da2 as i32,
            warning: None,
        }
    }

    #[tokio::test]
    async fn basic_commands_render_table_and_json() {
        let service = MockBridgeService {
            capabilities_response: GetCapabilitiesResponse {
                application_version: "0.3".into(),
                protocol_version: "0.3".into(),
                max_page_size: 1000,
                supports_browse_sessions: true,
                supports_search: true,
                organization: ProtoOrganization::Flat as i32,
                source: ProtoBrowseSource::Flat as i32,
            },
            list_servers_response: ListServersResponse {
                servers: vec!["S".into()],
            },
            read_response: ReadResponse {
                values: vec![ProtoTagValue {
                    tag_id: "t".into(),
                    value: "1".into(),
                    quality: "Good".into(),
                    timestamp: "now".into(),
                }],
            },
            write_response: WriteResponse {
                tag_id: "t".into(),
                success: true,
                error: None,
            },
            ..Default::default()
        };
        let host = start_mock_server(service).await;
        cmd_servers(host.clone(), OutputFormat::Table)
            .await
            .unwrap();
        cmd_capabilities(host.clone(), "S".into(), OutputFormat::Json)
            .await
            .unwrap();
        cmd_read(
            host.clone(),
            "S".into(),
            vec!["t".into()],
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let err = cmd_search(
            "unused".into(),
            "S".into(),
            "PV".into(),
            SearchMatchMode::Exact,
            None,
            None,
            0,
            false,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("greater than zero"));
        cmd_write(
            host.clone(),
            "S".into(),
            "t".into(),
            "1".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
        cmd_write(
            host,
            "S".into(),
            "t".into(),
            "text".into(),
            OutputFormat::Json,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn browse_one_page_preserves_metadata_and_request() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A")],
            ..Default::default()
        };
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        cmd_browse(
            host,
            "S".into(),
            Some("session".into()),
            Some("parent".into()),
            None,
            20,
            false,
            100,
            true,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].page_size, 20);
        assert!(requests[0].refresh);
    }

    #[tokio::test]
    async fn browse_all_follows_pages_and_honors_safety_cap() {
        let service = MockBridgeService {
            browse_responses: vec![
                page(false, Some("next-1"), "A"),
                page(false, Some("next-2"), "B"),
            ],
            ..Default::default()
        };
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            2,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].page_token.as_deref(), Some("next-1"));
    }

    #[tokio::test]
    async fn browse_all_rejects_invalid_limits_and_session_changes() {
        let err = cmd_browse(
            "unused".into(),
            "S".into(),
            None,
            None,
            None,
            0,
            false,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("page-size"));

        let err = cmd_browse(
            "unused".into(),
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            0,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("greater than zero"));

        let mut second = page(true, None, "B");
        second.session_id = "other".into();
        let host = start_mock_server(MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), second],
            ..Default::default()
        })
        .await;
        let err = cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("changed browse session"));

        let mut second = page(true, None, "B");
        second.source = ProtoBrowseSource::Da3 as i32;
        let host = start_mock_server(MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), second],
            ..Default::default()
        })
        .await;
        let err = cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("namespace metadata"));
    }

    #[tokio::test]
    async fn close_and_search_stream_events() {
        let service = MockBridgeService {
            search_events: vec![
                ProtoSearchEvent {
                    event: Some(search_event::Event::Match(SearchMatch {
                        node: Some(ProtoBrowseNode {
                            node_key: "n".into(),
                            display_name: "PV".into(),
                            kind: ProtoBrowseNodeKind::Item as i32,
                            item_id: Some("FCS!TAG.PV".into()),
                        }),
                        breadcrumbs: vec![BrowseBreadcrumb {
                            node_key: "b".into(),
                            display_name: "FCS".into(),
                        }],
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Progress(SearchProgress {
                        visited_nodes: 5,
                        matches: 1,
                        partial: true,
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Completed(SearchCompleted {
                        complete: false,
                        cancelled: false,
                        truncated: true,
                        warning: Some("cap reached".into()),
                    })),
                },
            ],
            ..Default::default()
        };
        let close_requests = Arc::clone(&service.close_requests);
        let search_requests = Arc::clone(&service.search_requests);
        let host = start_mock_server(service).await;
        cmd_close_browse_session(host.clone(), "session".into(), OutputFormat::Json)
            .await
            .unwrap();
        cmd_search(
            host.clone(),
            "S".into(),
            "PV".into(),
            SearchMatchMode::Contains,
            Some("session".into()),
            Some("scope".into()),
            20,
            true,
            true,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(close_requests.lock().unwrap()[0].session_id, "session");
        {
            let requests = search_requests.lock().unwrap();
            assert_eq!(requests[0].max_results, 20);
            assert!(requests[0].include_branches);
        }

        let host = start_mock_server(MockBridgeService {
            search_events: vec![
                ProtoSearchEvent {
                    event: Some(search_event::Event::Match(SearchMatch {
                        node: Some(ProtoBrowseNode {
                            node_key: "n".into(),
                            display_name: "PV".into(),
                            kind: ProtoBrowseNodeKind::Branch as i32,
                            item_id: None,
                        }),
                        breadcrumbs: vec![BrowseBreadcrumb {
                            node_key: "b".into(),
                            display_name: "FCS".into(),
                        }],
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Progress(SearchProgress {
                        visited_nodes: 5,
                        matches: 1,
                        partial: true,
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Completed(SearchCompleted {
                        complete: false,
                        cancelled: false,
                        truncated: true,
                        warning: Some("cap reached".into()),
                    })),
                },
            ],
            ..Default::default()
        })
        .await;
        cmd_search(
            host,
            "S".into(),
            "PV".into(),
            SearchMatchMode::Contains,
            None,
            None,
            20,
            false,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    #[test]
    fn rendering_helpers_include_metadata_and_all_warning_combinations() {
        let typed = BrowsePage {
            session_id: "session".into(),
            nodes: vec![BrowseNode {
                node_key: "key".into(),
                display_name: "Branch".into(),
                kind: BrowseNodeKind::BranchAndItem,
                item_id: Some("Exact.ItemID".into()),
            }],
            next_page_token: Some("next".into()),
            complete: false,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Derived,
            warning: Some("partial".into()),
        };
        let json = render_browse(typed.clone(), 1, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["session_id"], "session");
        assert_eq!(value["nodes"][0]["item_id"], "Exact.ItemID");
        assert_eq!(value["complete"], false);
        let table = render_browse(typed, 1, OutputFormat::Table).unwrap();
        assert!(table.contains("Next page token: next"));
        assert!(table.contains("Warning: partial"));

        assert_eq!(merge_warnings(None, None), None);
        assert_eq!(merge_warnings(Some("a".into()), None).as_deref(), Some("a"));
        assert_eq!(merge_warnings(None, Some("b".into())).as_deref(), Some("b"));
        assert_eq!(
            merge_warnings(Some("a".into()), Some("b".into())).as_deref(),
            Some("a; b")
        );

        let oversized = BrowsePage {
            nodes: vec![
                BrowseNode {
                    node_key: "1".into(),
                    display_name: "1".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: Some("1".into()),
                },
                BrowseNode {
                    node_key: "2".into(),
                    display_name: "2".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: Some("2".into()),
                },
            ],
            ..typed_page()
        };
        assert!(ensure_page_bound(&oversized, 1).is_err());

        let empty_incomplete = BrowsePage {
            complete: false,
            next_page_token: Some("next".into()),
            ..typed_page()
        };
        assert!(ensure_page_bound(&empty_incomplete, 1).is_err());
    }

    #[test]
    fn search_event_json_is_tagged() {
        let event = search_output_event(SearchEvent::Progress(opcda_bridge::SearchProgress {
            visited_nodes: 3,
            matches: 1,
            partial: true,
        }));
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["event"], "progress");
        assert_eq!(value["visited_nodes"], 3);
    }

    fn typed_page() -> BrowsePage {
        BrowsePage {
            session_id: "session".into(),
            nodes: Vec::new(),
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        }
    }
}
