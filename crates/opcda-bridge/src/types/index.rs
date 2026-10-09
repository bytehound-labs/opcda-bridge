use super::browse::{BrowseNodeKind, BrowseSource, NamespaceOrganization};
use super::search::SearchMatchMode;
use std::fmt;

/// Default maximum number of matches requested from the persistent index.
pub const DEFAULT_INDEX_SEARCH_MAX_RESULTS: u32 = 50;
/// Readiness of a gateway-owned persistent namespace index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexState {
    Unspecified,
    NotIndexed,
    Partial,
    Ready,
    Stale,
    Refreshing,
    Promoting,
    Failed,
    Deleting,
}

impl fmt::Display for SearchIndexState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::NotIndexed => "not-indexed",
            Self::Partial => "partial",
            Self::Ready => "ready",
            Self::Stale => "stale",
            Self::Refreshing => "refreshing",
            Self::Promoting => "promoting",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
        })
    }
}

/// Effective inventory limits currently applied by the gateway controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexInventoryLimits {
    pub item_rate_per_second: u32,
    pub batch_size: u32,
    pub duty_cycle_percent: u32,
}

/// Adaptive controller state for a namespace-index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexControllerState {
    Unspecified,
    Ramping,
    Steady,
    Throttled,
    Paused,
}

impl fmt::Display for IndexControllerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Ramping => "ramping",
            Self::Steady => "steady",
            Self::Throttled => "throttled",
            Self::Paused => "paused",
        })
    }
}

/// Typed reason for a controller pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPauseReason {
    Unspecified,
    Foreground,
    OpcHealth,
    HostCpu,
    Memory,
    Disk,
    Database,
    Operator,
    Circuit,
}

impl fmt::Display for IndexPauseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Foreground => "foreground",
            Self::OpcHealth => "opc-health",
            Self::HostCpu => "host-cpu",
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::Database => "database",
            Self::Operator => "operator",
            Self::Circuit => "circuit",
        })
    }
}

/// Rolling foreground operation measurements.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexForegroundDiagnostics {
    pub active_count: u64,
    pub operations: u64,
    pub errors: u64,
    pub bad_quality: u64,
    pub latency_p50_ms: Option<u64>,
    pub latency_p95_ms: Option<u64>,
    pub latency_max_ms: Option<u64>,
    pub last_error: bool,
    pub last_bad_quality: bool,
}

/// Host and gateway-process resource measurements.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexHostDiagnostics {
    pub cpu_percent: Option<f64>,
    pub available_memory_percent: Option<f64>,
    pub disk_active_percent: Option<f64>,
    pub disk_queue: Option<f64>,
    pub process_working_set_bytes: Option<u64>,
    pub process_private_bytes: Option<u64>,
    pub process_read_bytes_per_second: Option<u64>,
    pub process_write_bytes_per_second: Option<u64>,
    pub disk_free_bytes: Option<u64>,
}

/// SQLite file and commit measurements.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexStorageDiagnostics {
    pub main_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub free_bytes: Option<u64>,
    pub last_commit_latency_ms: Option<u64>,
}

/// Gateway configuration policy for automatic refresh of usable indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAutoRefreshPolicy {
    Allowed,
    Disabled,
    Paused,
}

impl fmt::Display for IndexAutoRefreshPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Allowed => "allowed",
            Self::Disabled => "disabled",
            Self::Paused => "paused",
        })
    }
}

/// Scheduler, retry, and circuit-breaker measurements.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexSchedulerDiagnostics {
    /// `None` means the gateway has not reported its configuration policy.
    pub auto_refresh_policy: Option<IndexAutoRefreshPolicy>,
    pub next_refresh_at: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_success_duration_ms: Option<u64>,
    pub retry_after: Option<String>,
    pub consecutive_failures: u32,
    pub circuit_open: bool,
}

/// Health-probe state reported for the indexed server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexHealthState {
    Unspecified,
    Healthy,
    Unhealthy,
    #[default]
    Unavailable,
}

impl fmt::Display for IndexHealthState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Healthy => "healthy",
            Self::Unhealthy => "unhealthy",
            Self::Unavailable => "unavailable",
        })
    }
}

/// Health-probe availability and result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHealthDiagnostics {
    pub state: IndexHealthState,
    pub sentinel_configured: bool,
}

impl Default for IndexHealthDiagnostics {
    fn default() -> Self {
        Self {
            state: IndexHealthState::Unavailable,
            sentinel_configured: false,
        }
    }
}

/// Operator action applied to an active namespace-index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexControlAction {
    Pause,
    Resume,
    Cancel,
    Delete,
}
/// Parameters for one persistent-index query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexRequest {
    pub server: String,
    pub query: String,
    pub match_mode: SearchMatchMode,
    pub max_results: u32,
}

impl SearchIndexRequest {
    pub fn new(
        server: impl Into<String>,
        query: impl Into<String>,
        match_mode: SearchMatchMode,
    ) -> Self {
        Self {
            server: server.into(),
            query: query.into(),
            match_mode,
            max_results: DEFAULT_INDEX_SEARCH_MAX_RESULTS,
        }
    }
}

/// Progress reported for a running persistent namespace inventory.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedSearchProgress {
    pub branches_visited: u64,
    pub entries_seen: u64,
    pub unique_items: u64,
    pub active_time_ms: u64,
    pub paused_time_ms: u64,
    pub items_per_second: f64,
    pub estimated_remaining_ms: Option<u64>,
}

/// Persistent namespace-index state and build metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexStatus {
    pub server: String,
    pub state: SearchIndexState,
    pub active_generation: u64,
    pub entry_count: u64,
    pub unique_item_count: u64,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    pub database_bytes: u64,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub progress: Option<IndexedSearchProgress>,
    pub effective_limits: Option<IndexInventoryLimits>,
    pub controller_state: IndexControllerState,
    pub pause_reason: Option<IndexPauseReason>,
    pub recovery_deadline: Option<String>,
    pub pause_reason_detail: Option<String>,
    pub foreground: IndexForegroundDiagnostics,
    pub host: IndexHostDiagnostics,
    pub storage: IndexStorageDiagnostics,
    pub scheduler: IndexSchedulerDiagnostics,
    pub health: IndexHealthDiagnostics,
    pub promoting: bool,
}

/// One selectable result from the persistent namespace index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSearchMatch {
    pub item_id: String,
    pub display_name: String,
    pub kind: BrowseNodeKind,
    pub breadcrumbs: Vec<String>,
}

/// Ranked persistent-index matches plus snapshot readiness metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexResponse {
    pub matches: Vec<IndexedSearchMatch>,
    pub has_more: bool,
    pub status: SearchIndexStatus,
}
