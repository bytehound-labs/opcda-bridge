//! Persistent, gateway-owned namespace index and refresh coordinator.

use crate::config::ResolvedIndexConfig;
use crate::controller::HostMetricsProvider;
use crate::opc::OpcClient;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use self::query::QueryCache;
#[cfg(test)]
use self::query::SearchGate;
#[cfg(test)]
use self::scheduler::BuildReservationHook;
use self::scheduler::{
    BackgroundTasks, BuildFileLock, CleanupTaskState, DatabaseCoordination, MaintenanceWindow,
    RuntimeBuild, RuntimeState, maintenance_window_active, parse_maintenance_windows,
    wait_with_cancellation,
};
use self::status::{ForegroundMetricState, PauseOverlayState, percentile};
use self::store::{DbStatus, IndexDb, StatusRows, index_profile_is_compatible};

pub use self::enrollment::IndexOperationError;
pub(crate) use self::query::normalize_query;
pub use self::query::{IndexedMatch, IndexedSearch, SearchMode};
pub use self::status::{
    AutoRefreshPolicy, ForegroundGuard, ForegroundMetrics, HealthProbeState, IndexState,
    IndexStatus, SchedulerDiagnostics, StorageDiagnostics,
};

mod enrollment;
mod query;
mod scheduler;
mod status;
mod store;
mod traversal;

pub struct IndexManager<C: OpcClient> {
    client: Arc<C>,
    settings: ResolvedIndexConfig,
    database: Arc<Mutex<Option<IndexDb>>>,
    coordination: Arc<DatabaseCoordination>,
    writer_gate: Arc<Mutex<()>>,
    build_changed: Arc<tokio::sync::Notify>,
    build_locks: Arc<Mutex<HashMap<String, BuildFileLock>>>,
    runtime: Arc<Mutex<HashMap<String, RuntimeState>>>,
    active_builds: Arc<Mutex<HashSet<String>>>,
    pending_cancels: Arc<Mutex<HashSet<String>>>,
    promoting: Arc<Mutex<HashSet<String>>>,
    deleting: Arc<Mutex<HashSet<String>>>,
    deletion_errors: Arc<Mutex<HashMap<String, String>>>,
    foreground_users: Arc<Mutex<HashMap<String, usize>>>,
    pause_overlays: Arc<Mutex<HashMap<String, PauseOverlayState>>>,
    foreground_metrics: Arc<Mutex<HashMap<String, ForegroundMetricState>>>,
    commit_latency_recorded_at: Arc<Mutex<HashMap<String, Instant>>>,
    cache: Arc<Mutex<QueryCache>>,
    host_metrics: Arc<dyn HostMetricsProvider>,
    background_tasks: Arc<BackgroundTasks>,
    cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    cleanup_worker_active: Arc<AtomicBool>,
    background_started: AtomicBool,
    #[cfg(test)]
    reject_next_build_spawn: AtomicBool,
    #[cfg(test)]
    reject_next_cleanup_spawn: AtomicBool,
    #[cfg(test)]
    build_reservation_hook: Mutex<Option<Arc<BuildReservationHook>>>,
    #[cfg(test)]
    search_gate: Arc<Mutex<SearchGate>>,
}

impl<C: OpcClient> IndexManager<C> {
    #[cfg(test)]
    pub(super) fn take_build_spawn_rejection(&self) -> bool {
        self.reject_next_build_spawn.swap(false, Ordering::AcqRel)
    }

    #[cfg(not(test))]
    pub(super) fn take_build_spawn_rejection(&self) -> bool {
        false
    }

    #[cfg(test)]
    pub(super) fn take_cleanup_spawn_rejection(&self) -> bool {
        self.reject_next_cleanup_spawn.swap(false, Ordering::AcqRel)
    }

    #[cfg(not(test))]
    pub(super) fn take_cleanup_spawn_rejection(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexControlAction {
    Pause,
    Resume,
    Cancel,
    Delete,
}

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    use super::{IndexDb, IndexedMatch};
    use crate::opc::{BrowseSource, InventoryEntry, NamespaceOrganization};
    use std::path::Path;

    #[derive(Debug)]
    pub enum SearchAllModesError {
        Setup(anyhow::Error),
        QueryRejected(anyhow::Error),
    }

    pub fn search_all_modes(
        query: &str,
        entries: &[InventoryEntry],
        limit: u32,
    ) -> Result<[Vec<IndexedMatch>; 4], SearchAllModesError> {
        const SERVER: &str = "fuzz";

        let mut database =
            IndexDb::open(Path::new(":memory:")).map_err(SearchAllModesError::Setup)?;
        let generation = database
            .start_generation(
                SERVER,
                NamespaceOrganization::Unspecified,
                BrowseSource::Unspecified,
                "0",
            )
            .map_err(SearchAllModesError::Setup)?;
        database
            .insert_entries(SERVER, generation, entries)
            .map_err(SearchAllModesError::Setup)?;
        let search = |mode| {
            database
                .search(SERVER, generation, query, mode, limit)
                .map_err(SearchAllModesError::QueryRejected)
        };

        Ok([search(0)?, search(1)?, search(2)?, search(3)?])
    }

    pub fn parse_breadcrumbs(value: String) -> rusqlite::Result<Vec<String>> {
        super::query::parse_indexed_breadcrumbs(value)
    }
}

fn timestamp_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

fn system_time_timestamp(value: SystemTime) -> String {
    value
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().to_string())
        .unwrap_or_default()
}

fn instant_timestamp(value: Instant) -> String {
    system_time_timestamp(SystemTime::now() + value.saturating_duration_since(Instant::now()))
}

fn parse_timestamp(value: &str) -> Option<SystemTime> {
    value.parse::<u128>().ok().and_then(|millis| {
        u64::try_from(millis)
            .ok()
            .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
    })
}

#[cfg(test)]
mod tests;
