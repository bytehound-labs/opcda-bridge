use crate::output::OutputFormat;
use opcda_bridge::{BrowseNode, Client, SearchEvent, SearchMatchMode, SearchRequest};
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Clone, Serialize)]
pub(super) struct SearchNodeOutput {
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
pub(super) struct BreadcrumbOutput {
    node_key: String,
    display_name: String,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(super) enum SearchOutputEvent {
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

pub(super) fn search_completion_messages(
    format: OutputFormat,
    complete: bool,
    cancelled: bool,
    truncated: bool,
    warning: Option<&str>,
) -> Vec<String> {
    if format == OutputFormat::Table {
        let mut messages = vec![format!(
            "Search complete: complete={complete}, cancelled={cancelled}, truncated={truncated}"
        )];
        if let Some(warning) = warning {
            messages.push(format!("Warning: {warning}"));
        }
        messages
    } else {
        Vec::new()
    }
}

pub(super) fn search_output_event(event: SearchEvent) -> SearchOutputEvent {
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
            SearchEvent::Completed(completed) => {
                for message in search_completion_messages(
                    format,
                    completed.complete,
                    completed.cancelled,
                    completed.truncated,
                    completed.warning.as_deref(),
                ) {
                    eprintln!("{message}");
                }
            }
            SearchEvent::Match(_) => {}
        }
    }
    Ok(())
}
