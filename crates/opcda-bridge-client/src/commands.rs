use crate::output::{self, OutputFormat};
use opcda_bridge::{
    BrowseNode, BrowsePage, BrowsePageRequest, Capabilities, Client, CompatibilityFeature,
    CompatibilityReport, FeatureCompatibilityStatus, IndexForegroundDiagnostics,
    IndexHealthDiagnostics, IndexHostDiagnostics, IndexInventoryLimits, IndexSchedulerDiagnostics,
    IndexStorageDiagnostics, IndexedSearchProgress, SearchEvent, SearchIndexControlAction,
    SearchIndexRequest, SearchIndexResponse, SearchIndexStatus, SearchMatchMode, SearchRequest,
    parse_value,
};
use serde::Serialize;
use std::fmt::Write as _;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use tabled::Tabled;
use tabled::derive::display;

#[derive(Tabled, Serialize)]
struct ServerRow {
    #[tabled(rename = "Servers")]
    name: String,
}

#[derive(Tabled, Serialize)]
struct CompatibilityRow {
    #[tabled(rename = "Client")]
    client_version: String,
    #[tabled(rename = "Library")]
    library_version: String,
    #[tabled(rename = "Gateway")]
    gateway_version: String,
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Overall")]
    status: String,
    #[tabled(rename = "Evidence")]
    evidence: String,
    #[tabled(rename = "Feature")]
    feature: String,
    #[tabled(rename = "Feature Status")]
    feature_status: String,
    #[tabled(rename = "Client Versions")]
    client_versions: String,
    #[tabled(rename = "Gateway Versions")]
    gateway_versions: String,
    #[tabled(rename = "Negotiated")]
    negotiated_version: String,
    #[tabled(rename = "Reason")]
    reason: String,
}

fn version_range(range: Option<opcda_bridge::ProtocolVersionRange>) -> String {
    match range {
        Some(range) if range.min == range.max => range.min.to_string(),
        Some(range) => format!("{}-{}", range.min, range.max),
        None => "unknown".into(),
    }
}

fn render_compatibility(
    report: &CompatibilityReport,
    format: OutputFormat,
) -> anyhow::Result<String> {
    if format == OutputFormat::Json {
        return Ok(serde_json::to_string_pretty(report)?);
    }

    let rows = if report.features.is_empty() {
        vec![CompatibilityRow {
            client_version: report.client_version.clone(),
            library_version: report.library_version.clone(),
            gateway_version: report
                .gateway_version
                .as_deref()
                .unwrap_or("unknown")
                .into(),
            source: report.source.to_string(),
            status: report.status.to_string(),
            evidence: report.evidence.to_string(),
            feature: "none".into(),
            feature_status: "unknown".into(),
            client_versions: "unknown".into(),
            gateway_versions: "unknown".into(),
            negotiated_version: "none".into(),
            reason: "gateway did not provide a compatibility profile".into(),
        }]
    } else {
        report
            .features
            .iter()
            .map(|feature| CompatibilityRow {
                client_version: report.client_version.clone(),
                library_version: report.library_version.clone(),
                gateway_version: report
                    .gateway_version
                    .as_deref()
                    .unwrap_or("unknown")
                    .into(),
                source: report.source.to_string(),
                status: report.status.to_string(),
                evidence: report.evidence.to_string(),
                feature: feature.feature.to_string(),
                feature_status: feature.status.to_string(),
                client_versions: version_range(Some(feature.client_versions)),
                gateway_versions: version_range(feature.gateway_versions),
                negotiated_version: feature
                    .negotiated_version
                    .map_or_else(|| "none".into(), |version| version.to_string()),
                reason: feature.reason.clone(),
            })
            .collect()
    };
    output::render(rows, format)
}

/// Print the negotiated compatibility profile and enforce requested features.
pub async fn cmd_compatibility(
    host: String,
    server: Option<String>,
    required: Vec<CompatibilityFeature>,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let report = client
        .compatibility_with_client_version(server.as_deref(), env!("CARGO_PKG_VERSION"))
        .await?;
    println!("{}", render_compatibility(&report, format)?);

    let required = if required.is_empty() {
        vec![CompatibilityFeature::Core]
    } else {
        required
    };
    let failures = required
        .iter()
        .filter(|feature| {
            report
                .feature(**feature)
                .is_none_or(|result| result.status != FeatureCompatibilityStatus::Compatible)
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return Ok(());
    }
    let mut message = String::from("gateway compatibility check failed for ");
    let _ = write!(message, "{}", failures.join(", "));
    Err(anyhow::anyhow!(message))
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

    println!("{}", render_browse(combined, pages, format)?);
    Ok(())
}

fn next_browse_page_count(pages: u32) -> u32 {
    pages + 1
}

fn should_fetch_browse_page(
    all: bool,
    complete: bool,
    node_count: usize,
    max_results: u32,
) -> bool {
    all && !complete && node_count < max_results as usize
}

fn stopped_at_browse_safety_cap(
    all: bool,
    complete: bool,
    node_count: usize,
    max_results: u32,
) -> bool {
    all && !complete && node_count >= max_results as usize
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

fn search_completion_messages(
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

#[derive(Debug, Clone, Serialize)]
struct IndexProgressOutput {
    branches_visited: u64,
    entries_seen: u64,
    unique_items: u64,
    active_time_ms: u64,
    paused_time_ms: u64,
    items_per_second: f64,
    estimated_remaining_ms: Option<u64>,
}

impl From<IndexedSearchProgress> for IndexProgressOutput {
    fn from(value: IndexedSearchProgress) -> Self {
        Self {
            branches_visited: value.branches_visited,
            entries_seen: value.entries_seen,
            unique_items: value.unique_items,
            active_time_ms: value.active_time_ms,
            paused_time_ms: value.paused_time_ms,
            items_per_second: value.items_per_second,
            estimated_remaining_ms: value.estimated_remaining_ms,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexLimitsOutput {
    item_rate_per_second: u32,
    batch_size: u32,
    duty_cycle_percent: u32,
}

impl From<IndexInventoryLimits> for IndexLimitsOutput {
    fn from(value: IndexInventoryLimits) -> Self {
        Self {
            item_rate_per_second: value.item_rate_per_second,
            batch_size: value.batch_size,
            duty_cycle_percent: value.duty_cycle_percent,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexForegroundOutput {
    active_count: u64,
    operations: u64,
    errors: u64,
    bad_quality: u64,
    latency_p50_ms: Option<u64>,
    latency_p95_ms: Option<u64>,
    latency_max_ms: Option<u64>,
    last_error: bool,
    last_bad_quality: bool,
}

impl From<IndexForegroundDiagnostics> for IndexForegroundOutput {
    fn from(value: IndexForegroundDiagnostics) -> Self {
        Self {
            active_count: value.active_count,
            operations: value.operations,
            errors: value.errors,
            bad_quality: value.bad_quality,
            latency_p50_ms: value.latency_p50_ms,
            latency_p95_ms: value.latency_p95_ms,
            latency_max_ms: value.latency_max_ms,
            last_error: value.last_error,
            last_bad_quality: value.last_bad_quality,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexHostOutput {
    cpu_percent: Option<f64>,
    available_memory_percent: Option<f64>,
    disk_active_percent: Option<f64>,
    disk_queue: Option<f64>,
    process_working_set_bytes: Option<u64>,
    process_private_bytes: Option<u64>,
    process_read_bytes_per_second: Option<u64>,
    process_write_bytes_per_second: Option<u64>,
    disk_free_bytes: Option<u64>,
}

impl From<IndexHostDiagnostics> for IndexHostOutput {
    fn from(value: IndexHostDiagnostics) -> Self {
        Self {
            cpu_percent: value.cpu_percent,
            available_memory_percent: value.available_memory_percent,
            disk_active_percent: value.disk_active_percent,
            disk_queue: value.disk_queue,
            process_working_set_bytes: value.process_working_set_bytes,
            process_private_bytes: value.process_private_bytes,
            process_read_bytes_per_second: value.process_read_bytes_per_second,
            process_write_bytes_per_second: value.process_write_bytes_per_second,
            disk_free_bytes: value.disk_free_bytes,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexStorageOutput {
    main_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
    free_bytes: Option<u64>,
    last_commit_latency_ms: Option<u64>,
}

impl From<IndexStorageDiagnostics> for IndexStorageOutput {
    fn from(value: IndexStorageDiagnostics) -> Self {
        Self {
            main_bytes: value.main_bytes,
            wal_bytes: value.wal_bytes,
            shm_bytes: value.shm_bytes,
            free_bytes: value.free_bytes,
            last_commit_latency_ms: value.last_commit_latency_ms,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexSchedulerOutput {
    next_refresh_at: Option<String>,
    last_attempt_at: Option<String>,
    last_success_at: Option<String>,
    last_success_duration_ms: Option<u64>,
    retry_after: Option<String>,
    consecutive_failures: u32,
    circuit_open: bool,
}

impl From<IndexSchedulerDiagnostics> for IndexSchedulerOutput {
    fn from(value: IndexSchedulerDiagnostics) -> Self {
        Self {
            next_refresh_at: value.next_refresh_at,
            last_attempt_at: value.last_attempt_at,
            last_success_at: value.last_success_at,
            last_success_duration_ms: value.last_success_duration_ms,
            retry_after: value.retry_after,
            consecutive_failures: value.consecutive_failures,
            circuit_open: value.circuit_open,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexHealthOutput {
    state: String,
    sentinel_configured: bool,
}

impl From<IndexHealthDiagnostics> for IndexHealthOutput {
    fn from(value: IndexHealthDiagnostics) -> Self {
        Self {
            state: value.state.to_string(),
            sentinel_configured: value.sentinel_configured,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct IndexStatusOutput {
    server: String,
    state: String,
    configured: bool,
    active_generation: u64,
    entry_count: u64,
    unique_item_count: u64,
    started_at: Option<String>,
    completed_at: Option<String>,
    last_error: Option<String>,
    database_bytes: u64,
    organization: String,
    source: String,
    progress: Option<IndexProgressOutput>,
    effective_limits: Option<IndexLimitsOutput>,
    controller_state: String,
    pause_reason: Option<String>,
    pause_reason_detail: Option<String>,
    recovery_deadline: Option<String>,
    foreground: IndexForegroundOutput,
    host: IndexHostOutput,
    storage: IndexStorageOutput,
    scheduler: IndexSchedulerOutput,
    health: IndexHealthOutput,
    promoting: bool,
}

impl From<SearchIndexStatus> for IndexStatusOutput {
    fn from(value: SearchIndexStatus) -> Self {
        Self {
            server: value.server,
            state: value.state.to_string(),
            configured: value.configured,
            active_generation: value.active_generation,
            entry_count: value.entry_count,
            unique_item_count: value.unique_item_count,
            started_at: value.started_at,
            completed_at: value.completed_at,
            last_error: value.last_error,
            database_bytes: value.database_bytes,
            organization: value.organization.to_string(),
            source: value.source.to_string(),
            progress: value.progress.map(Into::into),
            effective_limits: value.effective_limits.map(Into::into),
            controller_state: value.controller_state.to_string(),
            pause_reason: value.pause_reason.map(|reason| reason.to_string()),
            pause_reason_detail: value.pause_reason_detail,
            recovery_deadline: value.recovery_deadline,
            foreground: value.foreground.into(),
            host: value.host.into(),
            storage: value.storage.into(),
            scheduler: value.scheduler.into(),
            health: value.health.into(),
            promoting: value.promoting,
        }
    }
}

#[derive(Tabled, Serialize)]
struct IndexStatusRow {
    #[tabled(rename = "Metric")]
    metric: String,
    #[tabled(rename = "Value")]
    value: String,
}

fn index_status_rows(status: &IndexStatusOutput) -> Vec<IndexStatusRow> {
    let diagnostic_label = if status.state != "failed" && status.last_error.is_some() {
        "Last warning"
    } else {
        "Last error"
    };
    let mut rows = vec![
        ("Server", status.server.clone()),
        ("State", status.state.clone()),
        ("Promoting", status.promoting.to_string()),
        ("Configured", status.configured.to_string()),
        ("Active generation", status.active_generation.to_string()),
        ("Entries", status.entry_count.to_string()),
        ("Unique items", status.unique_item_count.to_string()),
        ("Database bytes", status.database_bytes.to_string()),
        ("Organization", status.organization.clone()),
        ("Source", status.source.clone()),
        ("Controller state", status.controller_state.clone()),
        (
            "Pause reason",
            status.pause_reason.clone().unwrap_or_else(|| "-".into()),
        ),
        (
            "Pause detail",
            status
                .pause_reason_detail
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Recovery deadline",
            status
                .recovery_deadline
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Started",
            status.started_at.clone().unwrap_or_else(|| "-".into()),
        ),
        (
            "Completed",
            status.completed_at.clone().unwrap_or_else(|| "-".into()),
        ),
        (
            diagnostic_label,
            status.last_error.clone().unwrap_or_else(|| "-".into()),
        ),
    ];
    if let Some(limits) = &status.effective_limits {
        rows.extend([
            (
                "Effective item rate/s",
                limits.item_rate_per_second.to_string(),
            ),
            ("Effective batch size", limits.batch_size.to_string()),
            (
                "Effective duty cycle %",
                limits.duty_cycle_percent.to_string(),
            ),
        ]);
    }
    if let Some(progress) = &status.progress {
        rows.extend([
            ("Branches visited", progress.branches_visited.to_string()),
            ("Entries seen", progress.entries_seen.to_string()),
            ("Build unique items", progress.unique_items.to_string()),
            ("Active time ms", progress.active_time_ms.to_string()),
            ("Paused time ms", progress.paused_time_ms.to_string()),
            ("Items per second", progress.items_per_second.to_string()),
            (
                "Estimated remaining ms",
                progress
                    .estimated_remaining_ms
                    .map_or_else(|| "-".into(), |value| value.to_string()),
            ),
        ]);
    }
    rows.extend([
        (
            "Foreground active",
            status.foreground.active_count.to_string(),
        ),
        (
            "Foreground operations",
            status.foreground.operations.to_string(),
        ),
        ("Foreground errors", status.foreground.errors.to_string()),
        (
            "Foreground bad quality",
            status.foreground.bad_quality.to_string(),
        ),
        (
            "Foreground p50 ms",
            status
                .foreground
                .latency_p50_ms
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Foreground p95 ms",
            status
                .foreground
                .latency_p95_ms
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Foreground max ms",
            status
                .foreground
                .latency_max_ms
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        ("Health", status.health.state.clone()),
        (
            "Sentinel configured",
            status.health.sentinel_configured.to_string(),
        ),
        (
            "Host CPU %",
            status
                .host
                .cpu_percent
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Available memory %",
            status
                .host
                .available_memory_percent
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Disk active %",
            status
                .host
                .disk_active_percent
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Disk queue",
            status
                .host
                .disk_queue
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Process working set bytes",
            status
                .host
                .process_working_set_bytes
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Process private bytes",
            status
                .host
                .process_private_bytes
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Process read bytes/s",
            status
                .host
                .process_read_bytes_per_second
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Process write bytes/s",
            status
                .host
                .process_write_bytes_per_second
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Index free bytes",
            status
                .host
                .disk_free_bytes
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        ("SQLite main bytes", status.storage.main_bytes.to_string()),
        ("SQLite WAL bytes", status.storage.wal_bytes.to_string()),
        ("SQLite SHM bytes", status.storage.shm_bytes.to_string()),
        (
            "SQLite free bytes",
            status
                .storage
                .free_bytes
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "SQLite last commit ms",
            status
                .storage
                .last_commit_latency_ms
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Next refresh",
            status
                .scheduler
                .next_refresh_at
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Last attempt",
            status
                .scheduler
                .last_attempt_at
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Last success",
            status
                .scheduler
                .last_success_at
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Last success duration ms",
            status
                .scheduler
                .last_success_duration_ms
                .map_or_else(|| "-".into(), |value| value.to_string()),
        ),
        (
            "Retry after",
            status
                .scheduler
                .retry_after
                .clone()
                .unwrap_or_else(|| "-".into()),
        ),
        (
            "Consecutive failures",
            status.scheduler.consecutive_failures.to_string(),
        ),
        ("Circuit open", status.scheduler.circuit_open.to_string()),
    ]);
    rows.into_iter()
        .map(|(metric, value)| IndexStatusRow {
            metric: metric.into(),
            value,
        })
        .collect()
}

fn render_index_status(status: SearchIndexStatus, format: OutputFormat) -> anyhow::Result<String> {
    let output = IndexStatusOutput::from(status);
    match format {
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&output)?),
        OutputFormat::Table => output::render(index_status_rows(&output), OutputFormat::Table),
    }
}

enum WatchStatusOutput {
    Table(String),
    Json(String),
}

fn render_watch_status(
    status: SearchIndexStatus,
    format: OutputFormat,
) -> anyhow::Result<WatchStatusOutput> {
    match format {
        OutputFormat::Table => {
            let rendered = render_index_status(status, format)?;
            Ok(WatchStatusOutput::Table(rendered))
        }
        OutputFormat::Json => {
            let rendered = serde_json::to_string(&IndexStatusOutput::from(status))?;
            Ok(WatchStatusOutput::Json(rendered))
        }
    }
}

pub async fn cmd_index_status(
    host: String,
    server: String,
    format: OutputFormat,
    watch_seconds: Option<u64>,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let Some(watch_seconds) = watch_seconds else {
        let status = client.search_index_status(server).await?;
        println!("{}", render_index_status(status, format)?);
        return Ok(());
    };
    let ctrl_c = Box::pin(tokio::signal::ctrl_c());
    watch_index_status(&mut client, server, format, watch_seconds, ctrl_c).await
}

async fn watch_index_status(
    client: &mut Client,
    server: String,
    format: OutputFormat,
    watch_seconds: u64,
    mut ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>,
) -> anyhow::Result<()> {
    loop {
        let status = tokio::select! {
            result = client.search_index_status(server.clone()) => result?,
            result = &mut ctrl_c => {
                result?;
                break;
            }
        };
        match render_watch_status(status, format)? {
            WatchStatusOutput::Table(rendered) => {
                print!("\x1b[2J\x1b[H");
                println!("{rendered}");
                std::io::stdout().flush()?;
            }
            WatchStatusOutput::Json(rendered) => {
                println!("{rendered}");
            }
        }
        tokio::select! {
            result = &mut ctrl_c => {
                result?;
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(watch_seconds.max(1))) => {}
        }
    }
    Ok(())
}

pub async fn cmd_index_refresh(
    host: String,
    server: String,
    force: bool,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let status = client.refresh_search_index(server, force).await?;
    println!("{}", render_index_status(status, format)?);
    Ok(())
}

pub async fn cmd_index_control(
    host: String,
    server: String,
    action: SearchIndexControlAction,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let mut client = Client::connect(&host).await?;
    let status = client.control_search_index(server, action).await?;
    println!("{}", render_index_status(status, format)?);
    Ok(())
}

#[derive(Serialize)]
struct IndexedSearchOutput {
    matches: Vec<IndexedSearchMatchOutput>,
    has_more: bool,
    status: IndexStatusOutput,
}

#[derive(Serialize)]
struct IndexedSearchMatchOutput {
    breadcrumbs: Vec<String>,
    display_name: String,
    kind: String,
    item_id: String,
}

#[derive(Debug, Clone, Tabled, Serialize)]
struct IndexedSearchMatchTableRow {
    #[tabled(rename = "Breadcrumbs")]
    breadcrumbs: String,
    #[tabled(rename = "Name")]
    display_name: String,
    #[tabled(rename = "Kind")]
    kind: String,
    #[tabled(rename = "Item ID")]
    item_id: String,
}

fn indexed_search_output(response: SearchIndexResponse) -> IndexedSearchOutput {
    IndexedSearchOutput {
        matches: response
            .matches
            .into_iter()
            .map(|found| IndexedSearchMatchOutput {
                breadcrumbs: found.breadcrumbs,
                display_name: found.display_name,
                kind: found.kind.to_string(),
                item_id: found.item_id,
            })
            .collect(),
        has_more: response.has_more,
        status: response.status.into(),
    }
}

fn render_indexed_search(
    response: SearchIndexResponse,
    format: OutputFormat,
) -> anyhow::Result<String> {
    let output = indexed_search_output(response);
    match format {
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&output)?),
        OutputFormat::Table => {
            let matches = output::render(
                output
                    .matches
                    .iter()
                    .map(|found| IndexedSearchMatchTableRow {
                        breadcrumbs: found.breadcrumbs.join(" / "),
                        display_name: found.display_name.clone(),
                        kind: found.kind.clone(),
                        item_id: found.item_id.clone(),
                    })
                    .collect::<Vec<_>>(),
                OutputFormat::Table,
            )?;
            let status = output::render(index_status_rows(&output.status), OutputFormat::Table)?;
            Ok(format!(
                "{matches}\nHas more: {}\n{status}",
                output.has_more
            ))
        }
    }
}

pub async fn cmd_index_search(
    host: String,
    server: String,
    query: String,
    match_mode: SearchMatchMode,
    max_results: u32,
    format: OutputFormat,
) -> anyhow::Result<()> {
    if max_results == 0 {
        anyhow::bail!("--max-results must be greater than zero");
    }
    let query = query.trim();
    if query.is_empty() {
        anyhow::bail!("indexed search query must not be empty");
    }
    let minimum = match match_mode {
        SearchMatchMode::Exact | SearchMatchMode::Prefix => 2,
        SearchMatchMode::Contains => 3,
    };
    if query.chars().count() < minimum {
        anyhow::bail!("indexed {match_mode} searches require at least {minimum} characters");
    }
    let mut request = SearchIndexRequest::new(server, query, match_mode);
    request.max_results = max_results;
    let mut client = Client::connect(&host).await?;
    let response = client.search_index(request).await?;
    println!("{}", render_indexed_search(response, format)?);
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
    use opcda_bridge::{
        BrowseNodeKind, BrowseSource, CompatibilityEvidence, CompatibilitySource,
        CompatibilityStatus, FeatureCompatibility, NamespaceOrganization, ProtocolVersionRange,
        SearchIndexControlAction, SearchIndexState,
    };
    use opcda_bridge_proto::bridge::search_event;
    use opcda_bridge_proto::bridge::write_request::TypedValue;
    use opcda_bridge_proto::bridge::{
        BrowseBreadcrumb, BrowseNode as ProtoBrowseNode, BrowseNodeKind as ProtoBrowseNodeKind,
        BrowsePage as ProtoBrowsePage, BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse,
        GetGatewayInfoResponse, IndexedSearchMatch, IndexedSearchProgress, ListServersResponse,
        NamespaceOrganization as ProtoOrganization, ProtocolFeature, ProtocolFeatureKind,
        ReadResponse, SearchCompleted, SearchEvent as ProtoSearchEvent, SearchIndexResponse,
        SearchIndexState as ProtoSearchIndexState, SearchIndexStatus, SearchMatch, SearchProgress,
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
                supports_indexed_search: true,
                indexed_search_protocol_version: "1".into(),
                max_indexed_search_results: 50,
                search_index_state: ProtoSearchIndexState::Ready as i32,
                search_index_promoting: false,
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
        let capabilities_requests = Arc::clone(&service.capabilities_requests);
        let read_requests = Arc::clone(&service.read_requests);
        let write_requests = Arc::clone(&service.write_requests);
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
        assert_eq!(capabilities_requests.lock().unwrap()[0].server, "S");
        {
            let requests = read_requests.lock().unwrap();
            assert_eq!(requests[0].server, "S");
            assert_eq!(requests[0].tag_ids, ["t"]);
        }
        {
            let requests = write_requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].server, "S");
            assert_eq!(requests[0].tag_id, "t");
            assert!(matches!(
                requests[0].typed_value,
                Some(TypedValue::IntValue(1))
            ));
            assert!(matches!(
                requests[1].typed_value,
                Some(TypedValue::StringValue(ref value)) if value == "text"
            ));
        }
    }

    #[tokio::test]
    async fn compatibility_command_reports_full_and_rejects_missing_requirements() {
        let host = start_mock_server(MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.4.3".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::IndexedSearch as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                ],
            },
            ..Default::default()
        })
        .await;
        cmd_compatibility(
            host,
            None,
            vec![
                CompatibilityFeature::Core,
                CompatibilityFeature::IndexedSearch,
            ],
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let host = start_mock_server(MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.3.2".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                ],
            },
            ..Default::default()
        })
        .await;
        let error = cmd_compatibility(
            host,
            None,
            vec![CompatibilityFeature::IndexedSearch],
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("indexed-search"));
    }

    #[test]
    fn compatibility_rendering_handles_empty_features_and_version_ranges() {
        let empty = CompatibilityReport {
            client_version: "client".into(),
            library_version: "library".into(),
            gateway_version: None,
            source: CompatibilitySource::Unknown,
            status: CompatibilityStatus::Unknown,
            evidence: CompatibilityEvidence::Unverified,
            features: Vec::new(),
        };
        let rendered = render_compatibility(&empty, OutputFormat::Table).unwrap();
        assert!(rendered.contains("none"));
        assert!(rendered.contains("gateway did not provide"));

        let ranged = CompatibilityReport {
            gateway_version: Some("gateway".into()),
            source: CompatibilitySource::GatewayInfo,
            status: CompatibilityStatus::Partial,
            evidence: CompatibilityEvidence::ContractBoundaryTested,
            features: vec![FeatureCompatibility {
                feature: CompatibilityFeature::Namespace,
                status: FeatureCompatibilityStatus::Incompatible,
                client_versions: ProtocolVersionRange { min: 1, max: 2 },
                gateway_versions: Some(ProtocolVersionRange { min: 3, max: 4 }),
                negotiated_version: None,
                reason: "ranges do not overlap".into(),
            }],
            ..empty
        };
        let rendered = render_compatibility(&ranged, OutputFormat::Table).unwrap();
        assert!(rendered.contains("1-2"));
        assert!(rendered.contains("3-4"));
        assert!(rendered.contains("none"));

        let exact = CompatibilityReport {
            features: vec![FeatureCompatibility {
                feature: CompatibilityFeature::Core,
                status: FeatureCompatibilityStatus::Compatible,
                client_versions: ProtocolVersionRange::exact(1),
                gateway_versions: None,
                negotiated_version: Some(1),
                reason: "exact".into(),
            }],
            ..ranged
        };
        let rendered = render_compatibility(&exact, OutputFormat::Table).unwrap();
        assert!(rendered.contains("unknown"));
        assert!(rendered.contains('1'));
        assert_eq!(version_range(Some(ProtocolVersionRange::exact(1))), "1");
        assert_eq!(
            version_range(Some(ProtocolVersionRange { min: 1, max: 2 })),
            "1-2"
        );
        assert_eq!(version_range(None), "unknown");
    }

    #[tokio::test]
    async fn browse_one_page_preserves_metadata_and_request() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), page(true, None, "B")],
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
    async fn browse_all_follows_pages_and_honors_remaining_limit() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next-1"), "A"), page(true, None, "B")],
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
            3,
            true,
            3,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].page_token.as_deref(), Some("next-1"));
        assert_eq!(requests[1].page_size, 2);
    }

    #[tokio::test]
    async fn browse_all_reports_exhausted_mock_response_sequence() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A")],
            ..Default::default()
        };
        let host = start_mock_server(service).await;
        let error = cmd_browse(
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
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("mock browse response sequence exhausted")
        );
    }

    #[test]
    fn browse_paging_helpers_preserve_page_counts_and_cap_conditions() {
        assert_eq!(next_browse_page_count(1), 2);
        assert!(should_fetch_browse_page(true, false, 1, 2));
        assert!(!should_fetch_browse_page(false, false, 1, 2));
        assert!(!should_fetch_browse_page(true, true, 1, 2));
        assert!(!should_fetch_browse_page(true, false, 2, 2));
        assert!(stopped_at_browse_safety_cap(true, false, 2, 2));
        assert!(!stopped_at_browse_safety_cap(false, false, 2, 2));
        assert!(!stopped_at_browse_safety_cap(true, true, 2, 2));
        assert!(!stopped_at_browse_safety_cap(true, false, 1, 2));
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

    fn proto_index_status(state: ProtoSearchIndexState) -> SearchIndexStatus {
        SearchIndexStatus {
            server: "S".into(),
            state: state as i32,
            configured: true,
            active_generation: 3,
            entry_count: 101,
            unique_item_count: 100,
            started_at: Some("start".into()),
            completed_at: Some("complete".into()),
            last_error: Some("warning".into()),
            database_bytes: 2048,
            organization: ProtoOrganization::Hierarchical as i32,
            source: ProtoBrowseSource::Da2 as i32,
            effective_limits: Some(opcda_bridge_proto::bridge::IndexInventoryLimits {
                item_rate_per_second: 100,
                batch_size: 25,
                duty_cycle_percent: 5,
            }),
            controller_state: opcda_bridge_proto::bridge::IndexControllerState::Throttled as i32,
            pause_reason: Some(opcda_bridge_proto::bridge::IndexPauseReason::Database as i32),
            recovery_deadline: Some("recover".into()),
            pause_reason_detail: Some("commit latency".into()),
            foreground: Some(opcda_bridge_proto::bridge::IndexForegroundDiagnostics {
                active_count: 1,
                operations: 2,
                errors: 3,
                bad_quality: 4,
                latency_p50_ms: Some(5),
                latency_p95_ms: Some(6),
                latency_max_ms: Some(7),
                last_error: true,
                last_bad_quality: true,
            }),
            host: Some(opcda_bridge_proto::bridge::IndexHostDiagnostics {
                cpu_percent: Some(8.0),
                available_memory_percent: Some(9.0),
                disk_active_percent: Some(10.0),
                disk_queue: Some(11.0),
                process_working_set_bytes: Some(12),
                process_private_bytes: Some(13),
                process_read_bytes_per_second: Some(14),
                process_write_bytes_per_second: Some(15),
                disk_free_bytes: Some(16),
            }),
            storage: Some(opcda_bridge_proto::bridge::IndexStorageDiagnostics {
                main_bytes: 17,
                wal_bytes: 18,
                shm_bytes: 19,
                free_bytes: Some(20),
                last_commit_latency_ms: Some(21),
            }),
            scheduler: Some(opcda_bridge_proto::bridge::IndexSchedulerDiagnostics {
                next_refresh_at: Some("next".into()),
                last_attempt_at: Some("attempt".into()),
                last_success_at: Some("success".into()),
                last_success_duration_ms: Some(22),
                retry_after: Some("retry".into()),
                consecutive_failures: 23,
                circuit_open: true,
            }),
            health: Some(opcda_bridge_proto::bridge::IndexHealthDiagnostics {
                state: opcda_bridge_proto::bridge::IndexHealthState::Healthy as i32,
                sentinel_configured: true,
            }),
            promoting: true,
            progress: Some(IndexedSearchProgress {
                branches_visited: 4,
                entries_seen: 5,
                unique_items: 5,
                active_time_ms: 6,
                paused_time_ms: 7,
                items_per_second: 8.5,
                estimated_remaining_ms: Some(9),
            }),
        }
    }

    #[tokio::test]
    async fn indexed_search_commands_render_and_forward_requests() {
        let service = MockBridgeService {
            search_index_status_response: proto_index_status(ProtoSearchIndexState::Ready),
            refresh_search_index_response: proto_index_status(ProtoSearchIndexState::Refreshing),
            control_search_index_response: proto_index_status(ProtoSearchIndexState::Partial),
            search_index_response: SearchIndexResponse {
                matches: vec![IndexedSearchMatch {
                    item_id: "FCS0201!204FI00510.PV".into(),
                    display_name: "PV".into(),
                    kind: ProtoBrowseNodeKind::Item as i32,
                    breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
                }],
                has_more: true,
                status: Some(proto_index_status(ProtoSearchIndexState::Stale)),
            },
            ..Default::default()
        };
        let status_requests = Arc::clone(&service.search_index_status_requests);
        let refresh_requests = Arc::clone(&service.refresh_search_index_requests);
        let control_requests = Arc::clone(&service.control_search_index_requests);
        let search_requests = Arc::clone(&service.search_index_requests);
        let host = start_mock_server(service).await;

        cmd_index_status(host.clone(), "S".into(), OutputFormat::Table, None)
            .await
            .unwrap();
        cmd_index_refresh(host.clone(), "S".into(), true, OutputFormat::Json)
            .await
            .unwrap();
        cmd_index_control(
            host.clone(),
            "S".into(),
            SearchIndexControlAction::Pause,
            OutputFormat::Table,
        )
        .await
        .unwrap();
        cmd_index_search(
            host.clone(),
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            25,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        cmd_index_search(
            host,
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            25,
            OutputFormat::Table,
        )
        .await
        .unwrap();

        assert_eq!(status_requests.lock().unwrap()[0].server, "S");
        assert!(refresh_requests.lock().unwrap()[0].force);
        assert_eq!(
            control_requests.lock().unwrap()[0].action,
            opcda_bridge_proto::bridge::SearchIndexControlAction::Pause as i32
        );
        let search_requests = search_requests.lock().unwrap();
        assert_eq!(search_requests[0].query, "PV1");
        assert_eq!(search_requests[0].max_results, 25);
    }

    #[test]
    fn indexed_status_rendering_includes_diagnostics_and_failure_labels() {
        let status = opcda_bridge::SearchIndexStatus::try_from(proto_index_status(
            ProtoSearchIndexState::Ready,
        ))
        .unwrap();
        let table = render_index_status(status.clone(), OutputFormat::Table).unwrap();
        for value in [
            "Effective item rate/s",
            "Foreground p95 ms",
            "Host CPU %",
            "SQLite WAL bytes",
            "Next refresh",
            "Health",
            "warning",
        ] {
            assert!(table.contains(value), "missing {value} in {table}");
        }
        let json = render_index_status(status, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["effective_limits"]["batch_size"], 25);
        assert_eq!(value["foreground"]["latency_p95_ms"], 6);
        assert_eq!(value["host"]["disk_free_bytes"], 16);
        assert_eq!(value["storage"]["last_commit_latency_ms"], 21);
        assert_eq!(value["scheduler"]["consecutive_failures"], 23);
        assert_eq!(value["health"]["state"], "healthy");
        assert_eq!(value["promoting"], true);

        let mut failed = proto_index_status(ProtoSearchIndexState::Failed);
        failed.last_error = Some("failure".into());
        let table = render_index_status(failed.try_into().unwrap(), OutputFormat::Table).unwrap();
        assert!(table.contains("Last error"));
        assert!(table.contains("failure"));
    }

    #[test]
    fn watched_status_rendering_preserves_table_and_json_shapes() {
        let status: opcda_bridge::SearchIndexStatus =
            proto_index_status(ProtoSearchIndexState::Ready)
                .try_into()
                .unwrap();
        let table = render_watch_status(status.clone(), OutputFormat::Table).unwrap();
        assert!(matches!(&table, WatchStatusOutput::Table(_)));
        let rendered = match table {
            WatchStatusOutput::Table(rendered) | WatchStatusOutput::Json(rendered) => rendered,
        };
        assert!(rendered.contains("Health"));

        let json = render_watch_status(status, OutputFormat::Json).unwrap();
        assert!(matches!(&json, WatchStatusOutput::Json(_)));
        let rendered = match json {
            WatchStatusOutput::Table(rendered) | WatchStatusOutput::Json(rendered) => rendered,
        };
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["state"], "ready");
    }

    #[tokio::test]
    async fn index_status_watch_renders_and_stops_on_ctrl_c() {
        let service = MockBridgeService {
            search_index_status_response: proto_index_status(ProtoSearchIndexState::Ready),
            ..Default::default()
        };
        let requests = Arc::clone(&service.search_index_status_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(())
        });
        watch_index_status(&mut client, "S".into(), OutputFormat::Table, 60, ctrl_c)
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);

        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(())
        });
        watch_index_status(&mut client, "S".into(), OutputFormat::Json, 60, ctrl_c)
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);

        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> =
            Box::pin(async { Ok(()) });
        watch_index_status(&mut client, "S".into(), OutputFormat::Json, 60, ctrl_c)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn indexed_search_validates_query_and_limit_before_connecting() {
        for (query, mode, expected) in [
            ("PV", SearchMatchMode::Contains, "at least 3"),
            ("P", SearchMatchMode::Exact, "at least 2"),
            ("P", SearchMatchMode::Prefix, "at least 2"),
            (" ", SearchMatchMode::Contains, "must not be empty"),
        ] {
            let error = cmd_index_search(
                "unused".into(),
                "S".into(),
                query.into(),
                mode,
                50,
                OutputFormat::Table,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
        let error = cmd_index_search(
            "unused".into(),
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            0,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("greater than zero"));
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

    #[test]
    fn search_completion_messages_only_render_table_diagnostics() {
        let table =
            search_completion_messages(OutputFormat::Table, true, false, true, Some("cap reached"));
        assert_eq!(
            table,
            [
                "Search complete: complete=true, cancelled=false, truncated=true",
                "Warning: cap reached",
            ]
        );
        assert!(
            search_completion_messages(OutputFormat::Json, true, false, true, Some("cap reached"),)
                .is_empty()
        );
    }

    #[test]
    fn indexed_search_rendering_exposes_status_without_node_keys() {
        let status = opcda_bridge::SearchIndexStatus {
            server: "S".into(),
            state: SearchIndexState::Ready,
            configured: true,
            active_generation: 2,
            entry_count: 1,
            unique_item_count: 1,
            started_at: None,
            completed_at: Some("done".into()),
            last_error: None,
            database_bytes: 512,
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            progress: None,
            effective_limits: None,
            controller_state: opcda_bridge::IndexControllerState::Unspecified,
            pause_reason: None,
            pause_reason_detail: None,
            recovery_deadline: None,
            foreground: Default::default(),
            host: Default::default(),
            storage: Default::default(),
            scheduler: Default::default(),
            health: Default::default(),
            promoting: false,
        };
        let response = opcda_bridge::SearchIndexResponse {
            matches: vec![opcda_bridge::IndexedSearchMatch {
                item_id: "Exact.ItemID".into(),
                display_name: "Tag".into(),
                kind: BrowseNodeKind::Item,
                breadcrumbs: vec!["Area".into()],
            }],
            has_more: false,
            status: status.clone(),
        };
        let json = render_indexed_search(response.clone(), OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["matches"][0]["item_id"], "Exact.ItemID");
        assert_eq!(
            value["matches"][0]["breadcrumbs"],
            serde_json::json!(["Area"])
        );
        assert!(value["matches"][0].get("node_key").is_none());
        assert_eq!(value["status"]["state"], "ready");
        let table = render_indexed_search(response, OutputFormat::Table).unwrap();
        assert!(table.contains("Exact.ItemID"));
        assert!(table.contains("Has more: false"));
        assert!(
            render_index_status(status, OutputFormat::Json)
                .unwrap()
                .contains("\"progress\": null")
        );
    }

    #[test]
    fn indexed_search_status_renders_nonfailed_diagnostics_as_warnings() {
        let status = IndexStatusOutput {
            server: "S".into(),
            state: "ready".into(),
            configured: true,
            active_generation: 1,
            entry_count: 1,
            unique_item_count: 1,
            started_at: None,
            completed_at: None,
            last_error: Some("partial inventory".into()),
            database_bytes: 1,
            organization: "hierarchical".into(),
            source: "da2".into(),
            progress: None,
            ..Default::default()
        };

        let table = output::render(index_status_rows(&status), OutputFormat::Table).unwrap();
        assert!(table.contains("Last warning"));
        assert!(table.contains("partial inventory"));
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
