use crate::output::{self, OutputFormat};
use opcda_bridge::{
    Client, IndexForegroundDiagnostics, IndexHealthDiagnostics, IndexHostDiagnostics,
    IndexInventoryLimits, IndexSchedulerDiagnostics, IndexStorageDiagnostics,
    IndexedSearchProgress, SearchIndexControlAction, SearchIndexRequest, SearchIndexResponse,
    SearchIndexStatus, SearchMatchMode,
};
use serde::Serialize;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use tabled::Tabled;

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
    auto_refresh_enabled: bool,
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
            auto_refresh_enabled: value.auto_refresh_enabled,
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
        (
            "Auto refresh enabled",
            status.auto_refresh_enabled.to_string(),
        ),
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

pub(super) fn render_index_status(
    status: SearchIndexStatus,
    format: OutputFormat,
) -> anyhow::Result<String> {
    let output = IndexStatusOutput::from(status);
    match format {
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&output)?),
        OutputFormat::Table => output::render(index_status_rows(&output), OutputFormat::Table),
    }
}

pub(super) enum WatchStatusOutput {
    Table(String),
    Json(String),
}

pub(super) fn render_watch_status(
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

pub(super) async fn watch_index_status(
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

pub(super) fn render_indexed_search(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_search_status_renders_nonfailed_diagnostics_as_warnings() {
        let status = IndexStatusOutput {
            server: "S".into(),
            state: "ready".into(),
            auto_refresh_enabled: true,
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
}
