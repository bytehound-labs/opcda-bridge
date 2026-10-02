//! Persistent, gateway-owned namespace index and refresh coordinator.

use crate::config::ResolvedIndexConfig;
use crate::controller::{
    AdaptiveIndexController, HostMetrics, HostMetricsProvider, InventoryLimits,
    default_host_metrics_provider,
};
use crate::opc::{
    BrowseSource, InventoryCompleted, InventoryControl, InventoryEntry, InventoryEvent,
    InventoryNodeKind, InventoryPacing, InventoryProgress, InventorySliceBackend,
    InventorySliceObservation, NamespaceOrganization, OpcClient,
};
use chrono::{DateTime, Local, Timelike};
use fs2::FileExt;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

mod enrollment;
mod query;
mod scheduler;
mod status;
mod store;
mod traversal;

const SCHEMA_VERSION: i64 = 4;
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(300);
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(86_400);
const CLEANUP_BATCH_SIZE: usize = 10_000;
const CLEANUP_BATCH_PAUSE: Duration = Duration::from_millis(1);
const CLEANUP_RETRY_LIMIT: u32 = 3;
const CLEANUP_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    NotIndexed,
    Partial,
    Ready,
    Stale,
    Refreshing,
    Promoting,
    Failed,
    Deleting,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexStatus {
    pub server: String,
    pub state: IndexState,
    pub auto_refresh_enabled: bool,
    pub active_generation: u64,
    pub entry_count: u64,
    pub unique_item_count: u64,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    pub database_bytes: u64,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub progress: Option<InventoryProgress>,
    pub effective_limits: Option<InventoryLimits>,
    pub controller_state: Option<crate::controller::ControllerState>,
    pub pause_reason: Option<crate::controller::PauseReason>,
    pub recovery_deadline: Option<String>,
    pub foreground_metrics: ForegroundMetrics,
    pub host_metrics: HostMetrics,
    pub health: HealthProbeState,
    pub sentinel_configured: bool,
    pub storage: StorageDiagnostics,
    pub scheduler: SchedulerDiagnostics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ForegroundMetrics {
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HealthProbeState {
    #[default]
    Unavailable,
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageDiagnostics {
    pub main_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub free_bytes: Option<u64>,
    pub last_commit_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerDiagnostics {
    pub next_refresh_at: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_success_duration_ms: Option<u64>,
    pub retry_after: Option<String>,
    pub consecutive_failures: u32,
    pub circuit_open: bool,
}

#[derive(Default)]
struct ForegroundMetricState {
    latencies_ms: VecDeque<u64>,
    operations: u64,
    errors: u64,
    bad_quality: u64,
    last_error: bool,
    last_bad_quality: bool,
    last_health_failure_at: Option<Instant>,
    last_bad_quality_at: Option<Instant>,
}

impl ForegroundMetricState {
    fn record_health_at(
        &mut self,
        now: Instant,
        latency_ms: u64,
        error: bool,
        bad_quality: bool,
        health_failure: bool,
    ) {
        const WINDOW: usize = 128;
        self.latencies_ms.push_back(latency_ms);
        if self.latencies_ms.len() > WINDOW {
            self.latencies_ms.pop_front();
        }
        self.operations = self.operations.saturating_add(1);
        self.errors += u64::from(error);
        self.bad_quality += u64::from(bad_quality);
        self.last_error = error;
        self.last_bad_quality = bad_quality;
        if health_failure {
            self.last_health_failure_at = Some(now);
        }
        if bad_quality {
            self.last_bad_quality_at = Some(now);
        }
    }

    fn recent_health_failure(&self, now: Instant, max_age: Duration) -> bool {
        self.last_health_failure_at
            .is_some_and(|recorded| now.saturating_duration_since(recorded) <= max_age)
    }

    fn recent_bad_quality(&self, now: Instant, max_age: Duration) -> bool {
        self.last_bad_quality_at
            .is_some_and(|recorded| now.saturating_duration_since(recorded) <= max_age)
    }

    fn snapshot(&self, active_count: u64) -> ForegroundMetrics {
        let mut sorted = self.latencies_ms.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        ForegroundMetrics {
            active_count,
            operations: self.operations,
            errors: self.errors,
            bad_quality: self.bad_quality,
            latency_p50_ms: percentile(&sorted, 50),
            latency_p95_ms: percentile(&sorted, 95),
            latency_max_ms: sorted.last().copied(),
            last_error: self.last_error,
            last_bad_quality: self.last_bad_quality,
        }
    }
}

fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let rank = (values.len() * percentile).div_ceil(100).max(1);
    let index = (rank - 1).min(values.len() - 1);
    values.get(index).copied()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedMatch {
    pub item_id: String,
    pub display_name: String,
    pub kind: InventoryNodeKind,
    pub breadcrumbs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexedSearch {
    pub matches: Vec<IndexedMatch>,
    pub has_more: bool,
    pub status: IndexStatus,
}

#[derive(Clone)]
struct RuntimeBuild {
    control: Option<Arc<dyn InventoryControl>>,
    progress: Option<InventoryProgress>,
    started_at: String,
    foreground_users: usize,
    operator_paused: bool,
    quiet_until: Option<Instant>,
    effective_limits: Option<InventoryLimits>,
    controller_state: Option<crate::controller::ControllerState>,
    pause_reason: Option<crate::controller::PauseReason>,
    recovery_deadline: Option<Instant>,
    last_commit_latency_ms: Option<u64>,
}

#[derive(Default)]
struct RuntimeState {
    build: Option<RuntimeBuild>,
    retry_after: Option<SystemTime>,
    last_error: Option<String>,
    consecutive_failures: u32,
    circuit_open: bool,
    health: HealthProbeState,
    sentinel_checked_at: Option<Instant>,
}

#[derive(Default)]
struct RuntimeStatus {
    build: Option<RuntimeBuild>,
    last_error: Option<String>,
    retry_after: Option<SystemTime>,
    consecutive_failures: u32,
    circuit_open: bool,
    health: HealthProbeState,
}

#[derive(Debug, Clone, Copy, Default)]
struct PauseOverlayState {
    maintenance: bool,
    health: bool,
}

struct HealthProbeObservation {
    healthy: bool,
    failure_reason: String,
    sentinel_configured: bool,
}

struct HealthSentinelObservation {
    healthy: bool,
    failure_reason: Option<String>,
}

struct BuildRunState {
    pending: Vec<InventoryEntry>,
    last_progress: InventoryProgress,
    telemetry: BuildTelemetry,
    completed: bool,
    cancelled: bool,
    failed: Option<String>,
    completion_warning: Option<String>,
    completion_profile: Option<(NamespaceOrganization, BrowseSource)>,
    terminal: bool,
    accounted_active_time_ms: u64,
    persisted_item_count: u64,
    drained_event_count: u64,
    received_entry_count: u64,
    rate_limiter: ItemRateLimiter,
    controller: Option<AdaptiveIndexController>,
    effective_duty_cycle_percent: u8,
    last_commit_at: Instant,
    next_health_probe: Instant,
    health_backoff: Duration,
}

#[derive(Debug, Default)]
struct BuildTelemetry {
    slice_count: u64,
    slice_nodes_returned: u64,
    slice_native_operations: u64,
    slice_elapsed_ms: u64,
    slice_elapsed_max_ms: u64,
    slice_entries_delta: u64,
    slice_entries_delta_max: u64,
    slice_unique_items_delta: u64,
    da2_slices: u64,
    da3_slices: u64,
    last_slice_entries_seen: u64,
    last_slice_unique_items: u64,
    progress_events: u64,
    item_entries: u64,
    branch_and_item_entries: u64,
    commit_attempts: u64,
    commit_failures: u64,
    committed_entries: u64,
    commit_elapsed_ms: u64,
    commit_elapsed_max_ms: u64,
    commit_latency_samples_ms: VecDeque<u64>,
    terminal_event_ms: Option<u64>,
}

impl BuildTelemetry {
    fn record_entry(&mut self, kind: InventoryNodeKind) {
        match kind {
            InventoryNodeKind::Item => self.item_entries += 1,
            InventoryNodeKind::BranchAndItem => self.branch_and_item_entries += 1,
        }
    }

    fn record_progress(&mut self) {
        self.progress_events += 1;
    }

    fn record_slice(&mut self, slice: &InventorySliceObservation) {
        let entries_delta = slice
            .entries_seen
            .saturating_sub(self.last_slice_entries_seen);
        let unique_items_delta = slice
            .unique_items
            .saturating_sub(self.last_slice_unique_items);
        self.last_slice_entries_seen = slice.entries_seen;
        self.last_slice_unique_items = slice.unique_items;
        self.slice_count += 1;
        self.slice_nodes_returned += slice.nodes_returned;
        self.slice_native_operations += slice.native_operations;
        self.slice_elapsed_ms += slice.elapsed_ms;
        self.slice_elapsed_max_ms = self.slice_elapsed_max_ms.max(slice.elapsed_ms);
        self.slice_entries_delta += entries_delta;
        self.slice_entries_delta_max = self.slice_entries_delta_max.max(entries_delta);
        self.slice_unique_items_delta += unique_items_delta;
        match slice.backend {
            InventorySliceBackend::Da2 => self.da2_slices += 1,
            InventorySliceBackend::Da3 => self.da3_slices += 1,
        }
    }

    fn record_commit(&mut self, inserted: u64, elapsed: Duration, failed: bool) {
        let elapsed_ms = elapsed.as_millis().try_into().unwrap_or(u64::MAX);
        self.commit_attempts += 1;
        self.commit_failures += u64::from(failed);
        self.committed_entries += inserted;
        self.commit_elapsed_ms = self.commit_elapsed_ms.saturating_add(elapsed_ms);
        self.commit_elapsed_max_ms = self.commit_elapsed_max_ms.max(elapsed_ms);
        if self.commit_latency_samples_ms.len() == 256 {
            self.commit_latency_samples_ms.pop_front();
        }
        self.commit_latency_samples_ms.push_back(elapsed_ms);
    }

    fn commit_latency_percentile(&self, percentile_value: usize) -> Option<u64> {
        let mut values = self
            .commit_latency_samples_ms
            .iter()
            .copied()
            .collect::<Vec<_>>();
        values.sort_unstable();
        percentile(&values, percentile_value)
    }

    fn record_terminal_event(&mut self, elapsed: Duration) {
        self.terminal_event_ms = Some(elapsed.as_millis().try_into().unwrap_or(u64::MAX));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TerminalBuildCounts {
    last_progress_entries_seen: u64,
    last_progress_unique_items: u64,
    persisted_items: u64,
    drained_events: u64,
    received_entry_events: u64,
    pending_entries: u64,
    pending_unique_items: u64,
}

impl BuildRunState {
    fn new(settings: &ResolvedIndexConfig, controller: Option<AdaptiveIndexController>) -> Self {
        Self {
            pending: Vec::new(),
            last_progress: InventoryProgress {
                branches_visited: 0,
                entries_seen: 0,
                unique_items: 0,
                active_time_ms: 0,
                paused_time_ms: 0,
                items_per_second: 0.0,
                estimated_remaining_ms: None,
            },
            telemetry: BuildTelemetry::default(),
            completed: false,
            cancelled: false,
            failed: None,
            completion_warning: None,
            completion_profile: None,
            terminal: false,
            accounted_active_time_ms: 0,
            persisted_item_count: 0,
            drained_event_count: 0,
            received_entry_count: 0,
            rate_limiter: ItemRateLimiter::new(settings.item_rate_limit, settings.burst_size),
            controller,
            effective_duty_cycle_percent: settings.duty_cycle_percent,
            last_commit_at: Instant::now(),
            next_health_probe: Instant::now(),
            health_backoff: Duration::from_secs(1),
        }
    }

    fn record_completion(&mut self, result: InventoryCompleted, elapsed: Duration) {
        self.terminal = true;
        self.telemetry.record_terminal_event(elapsed);
        self.completed = result.complete;
        self.cancelled = result.cancelled;
        self.completion_profile = Some((result.organization, result.source));
        if result.truncated {
            self.failed = Some(
                result
                    .warning
                    .unwrap_or_else(|| "inventory was truncated".to_string()),
            );
        } else {
            self.completion_warning = result.warning;
        }
    }

    fn terminal_counts(&self) -> TerminalBuildCounts {
        let pending_unique_items = self
            .pending
            .iter()
            .map(|entry| entry.item_id.as_str())
            .collect::<HashSet<_>>()
            .len() as u64;
        TerminalBuildCounts {
            last_progress_entries_seen: self.last_progress.entries_seen,
            last_progress_unique_items: self.last_progress.unique_items,
            persisted_items: self.persisted_item_count,
            drained_events: self.drained_event_count,
            received_entry_events: self.received_entry_count,
            pending_entries: self.pending.len() as u64,
            pending_unique_items,
        }
    }
}

struct BuildFinalizationContext<'a> {
    server: &'a str,
    generation: u64,
    control: &'a Arc<dyn InventoryControl>,
    control_was_cancelled_before_cleanup: bool,
    ownership: &'a Arc<()>,
    build_started: Instant,
}

enum BuildReadiness {
    Ready,
    Cancelled,
    Failed(String),
}

enum HealthProbeAction {
    Ready,
    Wait(Duration),
    Probe,
}

enum BuildEventOutcome {
    Continue,
    Stop,
    Cancelled,
    Failed(String),
}

enum BuildLoopOutcome {
    Finished,
    Failed(String),
}

struct CoordinatedInventoryControl {
    state: Arc<CoordinatedInventoryControlState>,
}

struct CoordinatedInventoryControlState {
    controls: Mutex<HashMap<usize, Arc<dyn InventoryControl>>>,
    cancelled: AtomicBool,
    worker_stop_requested: AtomicBool,
    paused: AtomicBool,
    pacing: Mutex<InventoryPacing>,
}

struct CoordinatedInventoryStream {
    receiver: UnboundedReceiver<anyhow::Result<InventoryEvent>>,
    control: Arc<CoordinatedInventoryControl>,
    coordinator: Option<tokio::task::JoinHandle<()>>,
    terminal_event_seen: bool,
}

struct InventoryRootPlan {
    root_entries: Vec<InventoryEntry>,
    worker_roots: Vec<String>,
    organization: NamespaceOrganization,
    source: BrowseSource,
}

enum WorkerInventoryMessage {
    Started {
        worker_id: usize,
    },
    Entry(InventoryEntry),
    Progress {
        worker_id: usize,
        progress: InventoryProgress,
    },
    Slice(InventorySliceObservation),
    Completed {
        worker_id: usize,
        result: InventoryCompleted,
    },
    Failed {
        worker_id: usize,
        error: String,
    },
    Finished {
        _worker_id: usize,
    },
}

enum WorkerEventAction {
    Continue,
    Completed,
    Stop,
}

struct WorkerFinishedGuard {
    sender: UnboundedSender<WorkerInventoryMessage>,
    worker_id: usize,
}

impl Drop for WorkerFinishedGuard {
    fn drop(&mut self) {
        let _ = self.sender.send(WorkerInventoryMessage::Finished {
            _worker_id: self.worker_id,
        });
    }
}

impl CoordinatedInventoryControl {
    fn new(initial_pacing: InventoryPacing) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(CoordinatedInventoryControlState {
                controls: Mutex::new(HashMap::new()),
                cancelled: AtomicBool::new(false),
                worker_stop_requested: AtomicBool::new(false),
                paused: AtomicBool::new(false),
                pacing: Mutex::new(initial_pacing),
            }),
        })
    }

    fn register(
        &self,
        worker_id: usize,
        control: Arc<dyn InventoryControl>,
    ) -> anyhow::Result<bool> {
        if self.should_stop_workers() {
            control.cancel();
            return Ok(false);
        }
        let pacing = self
            .state
            .pacing
            .lock()
            .map_err(|_| anyhow::anyhow!("coordinated inventory pacing lock poisoned"))?
            .to_owned();
        control.set_pacing(pacing)?;
        if self.state.paused.load(Ordering::Acquire) {
            control.pause();
        }
        let mut controls = self
            .state
            .controls
            .lock()
            .map_err(|_| anyhow::anyhow!("coordinated inventory control lock poisoned"))?;
        if self.should_stop_workers() {
            control.cancel();
            return Ok(false);
        }
        controls.insert(worker_id, Arc::clone(&control));
        Ok(true)
    }

    fn unregister(&self, worker_id: usize) {
        if let Ok(mut controls) = self.state.controls.lock() {
            controls.remove(&worker_id);
        }
    }

    fn snapshot_controls(&self) -> Vec<Arc<dyn InventoryControl>> {
        self.state
            .controls
            .lock()
            .map(|controls| controls.values().cloned().collect())
            .unwrap_or_default()
    }

    fn set_pacing(&self, pacing: InventoryPacing) -> anyhow::Result<()> {
        *self
            .state
            .pacing
            .lock()
            .map_err(|_| anyhow::anyhow!("coordinated inventory pacing lock poisoned"))? = pacing;
        for control in self.snapshot_controls() {
            if let Err(error) = control.set_pacing(pacing) {
                self.cancel();
                return Err(error);
            }
        }
        Ok(())
    }

    fn pause_all(&self) {
        self.state.paused.store(true, Ordering::Release);
        for control in self.snapshot_controls() {
            control.pause();
        }
    }

    fn resume_all(&self) {
        self.state.paused.store(false, Ordering::Release);
        for control in self.snapshot_controls() {
            control.resume();
        }
    }

    fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state
            .worker_stop_requested
            .store(true, Ordering::Release);
        self.stop_registered_workers();
    }

    fn stop_workers(&self) {
        self.state
            .worker_stop_requested
            .store(true, Ordering::Release);
        self.stop_registered_workers();
    }

    fn stop_registered_workers(&self) {
        for control in self.snapshot_controls() {
            control.cancel();
        }
    }

    fn should_stop_workers(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
            || self.state.worker_stop_requested.load(Ordering::Acquire)
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }
}

impl InventoryControl for CoordinatedInventoryControl {
    fn pause(&self) {
        self.pause_all();
    }

    fn resume(&self) {
        self.resume_all();
    }

    fn cancel(&self) {
        self.cancel();
    }

    fn set_pacing(&self, pacing: InventoryPacing) -> anyhow::Result<()> {
        self.set_pacing(pacing)
    }

    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

#[async_trait::async_trait]
impl crate::opc::InventoryStream for CoordinatedInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        let event = self.receiver.recv().await;
        if matches!(event.as_ref(), Some(Ok(InventoryEvent::Completed(_)))) {
            self.terminal_event_seen = true;
        }
        event
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        if !self.terminal_event_seen {
            self.control.cancel();
        }
        let Some(coordinator) = self.coordinator.take() else {
            return Ok(());
        };
        coordinator
            .await
            .map_err(|error| anyhow::anyhow!("coordinated inventory task failed: {error}"))
    }
}

impl Drop for CoordinatedInventoryStream {
    fn drop(&mut self) {
        self.control.cancel();
    }
}

struct BackgroundTasks {
    state: Mutex<BackgroundTaskState>,
    shutdown: tokio::sync::watch::Sender<bool>,
    idle: tokio::sync::Notify,
    #[cfg(test)]
    panic_next_cleanup_worker: AtomicBool,
    #[cfg(test)]
    cleanup_batch_hook: Mutex<Option<Arc<CleanupBatchHook>>>,
    #[cfg(test)]
    cleanup_writer_gate_hook: Mutex<Option<Arc<CleanupBatchHook>>>,
    #[cfg(test)]
    cleanup_notification_hook: Mutex<Option<Arc<CleanupNotificationHook>>>,
}

#[derive(Default)]
struct BackgroundTaskState {
    active: usize,
    shutting_down: bool,
}

struct BackgroundTaskGuard {
    tasks: Arc<BackgroundTasks>,
}

#[derive(Default)]
struct CleanupTaskState {
    running: bool,
    requested: bool,
    #[cfg(test)]
    failures: usize,
}

struct DatabaseCoordination {
    writer_gate: Arc<Mutex<()>>,
    active_builds: Arc<Mutex<HashSet<String>>>,
    build_owners: Arc<Mutex<HashMap<String, Arc<()>>>>,
    build_changed: Arc<tokio::sync::Notify>,
}

static DATABASE_COORDINATIONS: OnceLock<Mutex<HashMap<PathBuf, Weak<DatabaseCoordination>>>> =
    OnceLock::new();

#[cfg(test)]
struct CleanupBatchHook {
    started: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    fired: AtomicBool,
}

#[cfg(test)]
struct BuildReservationHook {
    started: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    fired: AtomicBool,
}

#[cfg(test)]
struct CleanupNotificationHook {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    fired: AtomicBool,
}

#[cfg(test)]
type SearchGate = Option<(
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
)>;

impl BackgroundTasks {
    fn new() -> Self {
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Self {
            state: Mutex::new(BackgroundTaskState::default()),
            shutdown,
            idle: tokio::sync::Notify::new(),
            #[cfg(test)]
            panic_next_cleanup_worker: AtomicBool::new(false),
            #[cfg(test)]
            cleanup_batch_hook: Mutex::new(None),
            #[cfg(test)]
            cleanup_writer_gate_hook: Mutex::new(None),
            #[cfg(test)]
            cleanup_notification_hook: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn install_cleanup_notification_hook(
        &self,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.cleanup_notification_hook.lock().unwrap() = Some(Arc::new(CleanupNotificationHook {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            fired: AtomicBool::new(false),
        }));
        (started, release)
    }

    #[cfg(test)]
    async fn wait_for_cleanup_notification_hook(&self) {
        let hook = self
            .cleanup_notification_hook
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        let Some(hook) = hook else {
            return;
        };
        if !hook.fired.swap(true, Ordering::AcqRel) {
            hook.started.notify_one();
            hook.release.notified().await;
        }
    }

    fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    fn is_shutting_down(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.shutting_down)
            .unwrap_or(true)
    }

    fn request_shutdown(&self) {
        let should_notify = self
            .state
            .lock()
            .map(|mut state| {
                if state.shutting_down {
                    false
                } else {
                    state.shutting_down = true;
                    true
                }
            })
            .unwrap_or(true);
        if should_notify {
            let _ = self.shutdown.send(true);
        }
    }

    #[cfg(test)]
    fn panic_next_cleanup_worker(&self) {
        self.panic_next_cleanup_worker
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn install_cleanup_batch_hook(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (started, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.cleanup_batch_hook.lock().unwrap() = Some(Arc::new(CleanupBatchHook {
            started,
            release: Mutex::new(release_rx),
            fired: AtomicBool::new(false),
        }));
        (started_rx, release)
    }

    #[cfg(test)]
    fn wait_for_cleanup_batch_hook(&self) {
        let hook = self
            .cleanup_batch_hook
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        let Some(hook) = hook else {
            return;
        };
        if !hook.fired.swap(true, Ordering::AcqRel) {
            let _ = hook.started.send(());
            if let Ok(release) = hook.release.lock() {
                let _ = release.recv();
            }
        }
    }

    #[cfg(test)]
    fn install_cleanup_writer_gate_hook(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (started, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.cleanup_writer_gate_hook.lock().unwrap() = Some(Arc::new(CleanupBatchHook {
            started,
            release: Mutex::new(release_rx),
            fired: AtomicBool::new(false),
        }));
        (started_rx, release)
    }

    #[cfg(test)]
    fn wait_for_cleanup_writer_gate_hook(&self) {
        let hook = self
            .cleanup_writer_gate_hook
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        let Some(hook) = hook else {
            return;
        };
        if !hook.fired.swap(true, Ordering::AcqRel) {
            let _ = hook.started.send(());
            if let Ok(release) = hook.release.lock() {
                let _ = release.recv();
            }
        }
    }

    fn spawn<F>(self: &Arc<Self>, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.shutting_down {
            return false;
        }
        state.active = state.active.saturating_add(1);
        drop(state);

        let tasks = Arc::clone(self);
        tokio::spawn(async move {
            let _guard = BackgroundTaskGuard { tasks };
            future.await;
        });
        true
    }

    async fn wait_for_idle(&self) {
        loop {
            let notified = self.idle.notified();
            let active = self.state.lock().map(|state| state.active).unwrap_or(0);
            if active == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for BackgroundTaskGuard {
    fn drop(&mut self) {
        let became_idle = self
            .tasks
            .state
            .lock()
            .map(|mut state| {
                state.active = state.active.saturating_sub(1);
                state.active == 0
            })
            .unwrap_or(true);
        if became_idle {
            self.tasks.idle.notify_one();
        }
    }
}

struct QueryCache {
    values: HashMap<CacheKey, IndexedSearch>,
    order: VecDeque<CacheKey>,
    capacity: usize,
}

struct ItemRateLimiter {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
}

impl ItemRateLimiter {
    fn new(rate: u32, burst_size: u32) -> Self {
        let capacity = f64::from(burst_size.max(1));
        Self {
            rate: f64::from(rate),
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    async fn acquire(&mut self, control: &Arc<dyn InventoryControl>) -> bool {
        if self.rate <= 0.0 {
            return !control.is_cancelled();
        }
        loop {
            if control.is_cancelled() {
                return false;
            }
            let now = Instant::now();
            let elapsed = now.duration_since(self.last_refill).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
            self.last_refill = now;
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return true;
            }
            let wait = Duration::from_secs_f64((1.0 - self.tokens) / self.rate);
            if !wait_with_cancellation(control, wait).await {
                return false;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaintenanceWindow {
    start_minute: u16,
    end_minute: u16,
}

impl MaintenanceWindow {
    fn parse(value: &str) -> anyhow::Result<Self> {
        let (start, end) = value
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("maintenance window must use HH:MM-HH:MM"))?;
        Ok(Self {
            start_minute: parse_clock(start)?,
            end_minute: parse_clock(end)?,
        })
    }

    fn contains(self, minute: u16) -> bool {
        if self.start_minute == self.end_minute {
            return true;
        }
        if self.start_minute < self.end_minute {
            (self.start_minute..self.end_minute).contains(&minute)
        } else {
            minute >= self.start_minute || minute < self.end_minute
        }
    }
}

fn parse_clock(value: &str) -> anyhow::Result<u16> {
    let (hour, minute) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("maintenance window clock must use HH:MM"))?;
    let hour = hour
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("maintenance window hour is invalid"))?;
    let minute = minute
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("maintenance window minute is invalid"))?;
    if hour >= 24 || minute >= 60 {
        anyhow::bail!("maintenance window clock is outside 00:00-23:59");
    }
    Ok(hour * 60 + minute)
}

fn parse_maintenance_windows(values: &[String]) -> anyhow::Result<Vec<MaintenanceWindow>> {
    values
        .iter()
        .map(|value| MaintenanceWindow::parse(value))
        .collect()
}

fn maintenance_window_active(windows: &[MaintenanceWindow], now: DateTime<Local>) -> bool {
    if windows.is_empty() {
        return false;
    }
    let minute = (now.hour() * 60 + now.minute()) as u16;
    windows
        .iter()
        .copied()
        .any(|window| window.contains(minute))
}

async fn wait_with_cancellation(control: &Arc<dyn InventoryControl>, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        if control.is_cancelled() {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
}

impl QueryCache {
    fn get(&mut self, key: &CacheKey) -> Option<IndexedSearch> {
        let value = self.values.get(key).cloned()?;
        self.order.retain(|existing| existing != key);
        self.order.push_back(key.clone());
        Some(value)
    }

    fn insert(&mut self, key: CacheKey, value: IndexedSearch) {
        self.values.insert(key.clone(), value);
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }

    fn clear_server(&mut self, server: &str) {
        self.values.retain(|key, _| key.server != server);
        self.order.retain(|key| key.server != server);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    server: String,
    generation: u64,
    query: String,
    mode: i32,
    limit: u32,
}

struct IndexDb {
    path: PathBuf,
    connection: Connection,
    #[cfg(test)]
    reject_next_prefix_query_map: AtomicBool,
}

impl IndexDb {
    #[cfg(test)]
    pub(super) fn take_prefix_query_map_rejection(&self) -> bool {
        self.reject_next_prefix_query_map
            .swap(false, Ordering::AcqRel)
    }
}

#[derive(Clone, Copy)]
struct Enrollment {
    auto_refresh_enabled: bool,
}

/// A typed index-operation failure suitable for stable gRPC status mapping.
#[derive(Debug)]
pub enum IndexOperationError {
    UnknownServer { server: String },
    NotEnrolled { server: String },
    Deleting { server: String },
    Internal(anyhow::Error),
}

impl std::fmt::Display for IndexOperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownServer { server } => {
                write!(formatter, "OPC DA server {server:?} is not registered")
            }
            Self::NotEnrolled { server } => {
                write!(
                    formatter,
                    "namespace index for OPC DA server {server:?} is not enrolled"
                )
            }
            Self::Deleting { server } => {
                write!(
                    formatter,
                    "namespace index for OPC DA server {server:?} is being deleted"
                )
            }
            Self::Internal(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for IndexOperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Internal(error) => Some(error.root_cause()),
            Self::UnknownServer { .. } | Self::NotEnrolled { .. } | Self::Deleting { .. } => None,
        }
    }
}

#[derive(Debug)]
struct BuildFileLock {
    file: Option<fs::File>,
    #[cfg(windows)]
    owner_path: Option<PathBuf>,
}

impl BuildFileLock {
    fn acquire(database_path: &Path, server: &str) -> anyhow::Result<Self> {
        if database_path == Path::new(":memory:") {
            return Ok(Self {
                file: None,
                #[cfg(windows)]
                owner_path: None,
            });
        }
        Self::acquire_with(database_path, server, |file, metadata| {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(metadata)?;
            file.sync_all()
        })
    }

    fn acquire_with<F>(database_path: &Path, server: &str, initialize: F) -> anyhow::Result<Self>
    where
        F: FnOnce(&mut fs::File, &[u8]) -> std::io::Result<()>,
    {
        let lock_path = build_lock_path(database_path, server);
        lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(fs::create_dir_all)
            .transpose()?;
        let mut file = match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) if is_lock_conflict(&error) => {
                let owner = read_lock_owner(&lock_path, database_path, server);
                anyhow::bail!(
                    "namespace index build lock is already held at {} ({})",
                    lock_path.display(),
                    if owner.trim().is_empty() {
                        error.to_string()
                    } else {
                        owner.trim().to_string()
                    }
                );
            }
            Err(error) => return Err(error.into()),
        };
        if let Err(error) = file.try_lock_exclusive() {
            let owner = read_lock_owner(&lock_path, database_path, server);
            anyhow::bail!(
                "namespace index build lock is already held at {} ({})",
                lock_path.display(),
                if owner.trim().is_empty() {
                    error.to_string()
                } else {
                    owner.trim().to_string()
                }
            );
        }
        let metadata = format!("process_id={}\nserver={server}\n", std::process::id());
        if let Err(error) = initialize(&mut file, metadata.as_bytes()) {
            let _ = FileExt::unlock(&file);
            return Err(error.into());
        }
        #[cfg(windows)]
        if let Err(error) = fs::write(build_owner_path(database_path, server), metadata.as_bytes())
        {
            let _ = FileExt::unlock(&file);
            return Err(error.into());
        }
        Ok(Self {
            file: Some(file),
            #[cfg(windows)]
            owner_path: Some(build_owner_path(database_path, server)),
        })
    }

    fn is_held(database_path: &Path, server: &str) -> anyhow::Result<bool> {
        if database_path == Path::new(":memory:") {
            return Ok(false);
        }
        let lock_path = build_lock_path(database_path, server);
        let file = match OpenOptions::new().read(true).write(true).open(&lock_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) if is_lock_conflict(&error) => return Ok(true),
            Err(error) => return Err(error.into()),
        };
        match file.try_lock_exclusive() {
            Ok(()) => {
                FileExt::unlock(&file)?;
                Ok(false)
            }
            Err(error) if is_lock_conflict(&error) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }
}

fn is_lock_conflict(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock || matches!(error.raw_os_error(), Some(32 | 33))
}

impl Drop for BuildFileLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            #[cfg(windows)]
            if let Some(owner_path) = self.owner_path.take() {
                match fs::remove_file(&owner_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => tracing::warn!(
                        process_id = std::process::id(),
                        owner = %owner_path.display(),
                        error = %error,
                        "unable to remove namespace index build owner metadata"
                    ),
                }
            }
            let _ = FileExt::unlock(&file);
            drop(file);
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SearchCandidate {
    rank: SearchRank,
    item_id: String,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SearchRank {
    tier: u8,
    display_name_len: usize,
    display_name_norm: String,
    item_id_norm: String,
}

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    use super::{BrowseSource, IndexDb, IndexedMatch, NamespaceOrganization};
    use crate::opc::InventoryEntry;
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

#[derive(Default)]
struct CleanupStats {
    batches: u64,
    entries: u64,
    fts_entries: u64,
    generations: u64,
    stopped_for_shutdown: bool,
    deferred_for_build: bool,
}

struct CleanupBatch {
    entries: u64,
    fts_entries: u64,
    generations: u64,
}

enum CleanupBatchResult {
    Shutdown,
    Deferred,
    NoObsoleteGenerations,
    Deleted(CleanupBatch),
}

enum CleanupAttempt {
    Retry,
    Return,
    Finished,
}

struct CleanupWorkerGuard {
    active: Arc<AtomicBool>,
    path: PathBuf,
    background_tasks: Arc<BackgroundTasks>,
    cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    coordination: Arc<DatabaseCoordination>,
}

impl Drop for CleanupWorkerGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        scheduler::spawn_cleanup_worker_if_idle(
            Arc::clone(&self.active),
            self.path.clone(),
            Arc::clone(&self.background_tasks),
            Arc::clone(&self.cleanup_tasks),
            Arc::clone(&self.coordination),
            false,
        );
    }
}

#[derive(Clone)]
struct DbStatus {
    generation: u64,
    state: String,
    organization: NamespaceOrganization,
    source: BrowseSource,
    started_at: String,
    completed_at: Option<String>,
    entry_count: u64,
    unique_item_count: u64,
    last_error: Option<String>,
}

struct StatusRows {
    active: Option<DbStatus>,
    staging: Option<DbStatus>,
    failed: Option<DbStatus>,
    failed_after_active: Option<DbStatus>,
}

impl StatusRows {
    fn from_rows(rows: &[DbStatus]) -> Self {
        let active = rows.iter().find(|row| row.state == "active").cloned();
        let staging = rows.iter().find(|row| row.state == "staging").cloned();
        let failed = rows.iter().find(|row| row.state == "failed").cloned();
        let failed_after_active = active.as_ref().and_then(|active| {
            failed
                .as_ref()
                .filter(|failed| failed.generation > active.generation)
                .cloned()
        });
        Self {
            active,
            staging,
            failed,
            failed_after_active,
        }
    }
}

#[derive(Clone, Copy)]
struct StoredIndexProfile {
    organization: NamespaceOrganization,
    source: BrowseSource,
    compatibility_fallback: bool,
}

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

struct BuildFinalizationGuard<C: OpcClient> {
    manager: Arc<IndexManager<C>>,
    server: String,
    generation: u64,
    control: Arc<dyn InventoryControl>,
    ownership: Arc<()>,
    armed: bool,
}

impl<C: OpcClient> BuildFinalizationGuard<C> {
    fn new(
        manager: Arc<IndexManager<C>>,
        server: String,
        generation: u64,
        control: Arc<dyn InventoryControl>,
        ownership: Arc<()>,
    ) -> Self {
        Self {
            manager,
            server,
            generation,
            control,
            ownership,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<C: OpcClient> Drop for BuildFinalizationGuard<C> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.control.cancel();
        self.manager.fail_generation_and_schedule_cleanup(
            &self.server,
            self.generation,
            "namespace index build unwound unexpectedly",
        );
        self.manager.finish_build_for_control_owned(
            &self.server,
            &self.control,
            &self.ownership,
            Some("namespace index build unwound unexpectedly".into()),
        );
        tracing::error!(
            process_id = std::process::id(),
            database = %self.manager.settings.database_path.display(),
            server = %self.server,
            generation = self.generation,
            "namespace index build unwound unexpectedly; ownership was released"
        );
    }
}

pub struct ForegroundGuard<C: OpcClient> {
    manager: Arc<IndexManager<C>>,
    server: String,
}

impl<C: OpcClient> Drop for ForegroundGuard<C> {
    fn drop(&mut self) {
        self.manager.foreground_end(&self.server);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexControlAction {
    Pause,
    Resume,
    Cancel,
    EnableAutoRefresh,
    DisableAutoRefresh,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Unspecified,
    Exact,
    Prefix,
    Contains,
}

impl TryFrom<i32> for SearchMode {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Exact),
            2 => Ok(Self::Prefix),
            3 => Ok(Self::Contains),
            _ => Err(()),
        }
    }
}

pub(crate) fn normalize_query(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
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

fn build_lock_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = scheduler::canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), std::ffi::OsStr::to_os_string);
    database_path.with_file_name(format!(
        "{}.{}.build.lock",
        file_name.to_string_lossy(),
        scheduler::stable_server_hash(server)
    ))
}

#[cfg(windows)]
fn build_owner_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = scheduler::canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), std::ffi::OsStr::to_os_string);
    database_path.with_file_name(format!(
        "{}.{}.build.owner",
        file_name.to_string_lossy(),
        scheduler::stable_server_hash(server)
    ))
}

#[cfg(windows)]
fn read_lock_owner(lock_path: &Path, database_path: &Path, server: &str) -> String {
    fs::read_to_string(build_owner_path(database_path, server))
        .or_else(|_| fs::read_to_string(lock_path))
        .unwrap_or_else(|_| "owner details unavailable".to_string())
}

#[cfg(not(windows))]
fn read_lock_owner(lock_path: &Path, _database_path: &Path, _server: &str) -> String {
    fs::read_to_string(lock_path).unwrap_or_else(|_| "owner details unavailable".to_string())
}

fn namespace_string(value: NamespaceOrganization) -> &'static str {
    match value {
        NamespaceOrganization::Unspecified => "unspecified",
        NamespaceOrganization::Flat => "flat",
        NamespaceOrganization::Hierarchical => "hierarchical",
    }
}

fn parse_namespace(value: &str) -> NamespaceOrganization {
    match value {
        "flat" => NamespaceOrganization::Flat,
        "hierarchical" => NamespaceOrganization::Hierarchical,
        _ => NamespaceOrganization::Unspecified,
    }
}

fn source_string(value: BrowseSource) -> &'static str {
    match value {
        BrowseSource::Unspecified => "unspecified",
        BrowseSource::Da3 => "da3",
        BrowseSource::Da2 => "da2",
        BrowseSource::Flat => "flat",
        BrowseSource::Derived => "derived",
    }
}

fn parse_source(value: &str) -> BrowseSource {
    match value {
        "da3" => BrowseSource::Da3,
        "da2" => BrowseSource::Da2,
        "flat" => BrowseSource::Flat,
        "derived" => BrowseSource::Derived,
        _ => BrowseSource::Unspecified,
    }
}

fn index_profile_is_compatible(
    indexed_organization: NamespaceOrganization,
    indexed_source: BrowseSource,
    compatibility_fallback: bool,
    raw_organization: NamespaceOrganization,
    raw_source: BrowseSource,
) -> bool {
    indexed_organization == raw_organization
        && (indexed_source == raw_source
            || (compatibility_fallback
                && indexed_source == BrowseSource::Da2
                && raw_source == BrowseSource::Da3))
}

fn node_kind_number(value: InventoryNodeKind) -> i64 {
    match value {
        InventoryNodeKind::Item => 1,
        InventoryNodeKind::BranchAndItem => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{query::*, scheduler::*, status::*, store::*, traversal::*};
    use crate::controller::ControllerObservation;
    use crate::opc::{
        BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, InventoryCompleted,
        InventoryEntry, InventoryEvent, InventoryHandle, InventorySliceBackend,
        InventorySliceObservation, InventoryStream, MAX_NATIVE_INVENTORY_BATCH_SIZE, OpcValue,
        TagValue, WriteResult,
    };
    use crate::test_support::MockOpcClient;
    use chrono::TimeZone;
    use proptest::prelude::*;
    use rusqlite::params;
    use std::collections::{HashMap, VecDeque};
    use std::error::Error;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::sync::Notify;

    fn settings(path: PathBuf) -> ResolvedIndexConfig {
        ResolvedIndexConfig {
            database_path: path,
            enabled: true,
            refresh_interval_seconds: 604_800,
            startup_grace_period_seconds: 0,
            schedule_jitter_seconds: 0,
            inventory_batch_size: 100,
            inventory_root: None,
            commit_batch_size: 100,
            commit_interval_ms: 1_000,
            batch_size: 100,
            item_rate_limit: 0,
            burst_size: 100,
            duty_cycle_percent: 100,
            adaptive: false,
            minimum_item_rate: 10,
            minimum_batch_size: 1,
            minimum_duty_cycle_percent: 1,
            canary_item_rate: 50,
            canary_batch_size: 25,
            canary_duty_cycle_percent: 5,
            adaptive_healthy_window_seconds: 30,
            adaptive_recovery_delay_seconds: 30,
            adaptive_max_recovery_delay_seconds: 300,
            sentinel_tag: None,
            sentinel_probe_interval_seconds: 30,
            minimum_free_space_bytes: 0,
            storage_headroom_bytes: 0,
            circuit_failure_threshold: 3,
            circuit_open_seconds: 300,
            quiet_period_seconds: 0,
            health_probe_interval_seconds: 30,
            health_latency_threshold_ms: 500,
            adaptive_foreground_soft_latency_ms: 1_000,
            adaptive_foreground_hard_latency_ms: 2_000,
            operation_timeout_seconds: 30,
            maintenance_windows: Vec::new(),
            concurrency: 1,
            worker_count: 1,
            query_cache_capacity: 256,
            paused: false,
            max_results: 50,
        }
    }

    fn completed_progress(count: u64) -> InventoryProgress {
        InventoryProgress {
            entries_seen: count,
            unique_items: count,
            ..zero_progress()
        }
    }

    #[test]
    fn controller_observation_expires_stale_commit_latency() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            ResolvedIndexConfig {
                adaptive_recovery_delay_seconds: 1,
                ..settings(PathBuf::from(":memory:"))
            },
        );
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: "test".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: Some(2_000),
                }),
                ..RuntimeState::default()
            },
        );
        manager
            .commit_latency_recorded_at
            .lock()
            .unwrap()
            .insert("S".into(), Instant::now() - Duration::from_secs(2));

        let stale = manager.controller_observation("S", false);
        assert_eq!(stale.database_commit_p95_ms, None);

        manager
            .commit_latency_recorded_at
            .lock()
            .unwrap()
            .insert("S".into(), Instant::now());
        let fresh = manager.controller_observation("S", false);
        assert_eq!(fresh.database_commit_p95_ms, Some(2_000));
    }

    #[test]
    fn reserving_a_new_build_clears_previous_commit_latency_timestamp() {
        let directory = tempdir().unwrap();
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        );
        manager
            .commit_latency_recorded_at
            .lock()
            .unwrap()
            .insert("S".into(), Instant::now());

        let ownership = manager.reserve_refresh_build("S", true).unwrap().unwrap();
        assert!(
            !manager
                .commit_latency_recorded_at
                .lock()
                .unwrap()
                .contains_key("S")
        );
        manager.finish_build_owned("S", &ownership, None);
    }

    fn synthetic_entries(prefix: &str, count: usize) -> Vec<InventoryEntry> {
        (0..count)
            .map(|index| {
                inventory_entry(&format!("{prefix}-{index}"), &format!("{prefix}.{index}"))
            })
            .collect()
    }

    fn root_node(display_name: &str, kind: BrowseNodeKind, item_id: Option<&str>) -> BrowseNode {
        BrowseNode {
            node_key: display_name.into(),
            display_name: display_name.into(),
            kind,
            item_id: item_id.map(str::to_owned),
        }
    }

    fn completed_inventory() -> InventoryEvent {
        InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            truncated: false,
            warning: None,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        })
    }

    #[tokio::test]
    async fn configured_inventory_root_uses_root_scoped_start() {
        let client = Arc::new(MockOpcClient::default());
        client.inventory_root_events.lock().unwrap().insert(
            "FCS0201".into(),
            VecDeque::from([Ok(completed_inventory())]),
        );
        let mut config = settings(PathBuf::from(":memory:"));
        config.inventory_root = Some("FCS0201".into());
        config.worker_count = 4;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        let handle = manager
            .start_refresh_inventory(
                "S",
                &Arc::new(()),
                InventoryLimits {
                    item_rate_per_second: 0,
                    batch_size: 17,
                    duty_cycle_percent: 100,
                },
            )
            .await
            .unwrap()
            .expect("configured root should start an inventory");
        let InventoryHandle {
            mut stream,
            control,
        } = handle;
        assert!(matches!(
            stream.next().await,
            Some(Ok(InventoryEvent::Completed(_)))
        ));
        stream.shutdown().await.unwrap();
        assert!(!control.is_cancelled());
        assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 0);
        assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 1);
        assert_eq!(
            client.inventory_started_roots.lock().unwrap().as_slice(),
            ["FCS0201"]
        );
        assert_eq!(client.inventory_batch_size.load(Ordering::Acquire), 17);
    }

    #[test]
    fn coordinated_control_handles_registration_and_pacing_edges() {
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        control.cancel();
        let cancelled_worker = Arc::new(RecordingInventoryControl::default());
        assert!(
            !control
                .register(
                    0,
                    Arc::clone(&cancelled_worker) as Arc<dyn InventoryControl>
                )
                .unwrap()
        );
        assert!(cancelled_worker.is_cancelled());

        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        control.pause();
        let paused_worker = Arc::new(RecordingInventoryControl::default());
        assert!(
            control
                .register(1, Arc::clone(&paused_worker) as Arc<dyn InventoryControl>)
                .unwrap()
        );
        assert_eq!(paused_worker.pause_count.load(Ordering::Acquire), 1);

        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let racing_worker = Arc::new(RegisterCancellingControl {
            parent: Arc::clone(&control),
            cancelled: AtomicBool::new(false),
        });
        racing_worker.pause();
        racing_worker.resume();
        assert!(
            !control
                .register(2, Arc::clone(&racing_worker) as Arc<dyn InventoryControl>)
                .unwrap()
        );
        assert!(racing_worker.is_cancelled());

        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let pacing_worker = Arc::new(RecordingInventoryControl::default());
        control
            .register(3, Arc::clone(&pacing_worker) as Arc<dyn InventoryControl>)
            .unwrap();
        let trait_control: &dyn InventoryControl = &*control;
        trait_control
            .set_pacing(InventoryPacing {
                min_interval: Duration::from_millis(5),
                item_rate_per_second: Some(10),
                batch_size: Some(20),
            })
            .unwrap();
        assert_eq!(pacing_worker.pacing_calls.load(Ordering::Acquire), 2);
        control.stop_workers();
        assert!(control.should_stop_workers());

        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let failing_worker = Arc::new(RecordingInventoryControl::default());
        failing_worker.fail_pacing_on_call(2);
        control
            .register(4, Arc::clone(&failing_worker) as Arc<dyn InventoryControl>)
            .unwrap();
        let error = control
            .set_pacing(InventoryPacing {
                min_interval: Duration::from_millis(1),
                item_rate_per_second: None,
                batch_size: None,
            })
            .unwrap_err();
        assert!(error.to_string().contains("test pacing update failure"));
        assert!(control.is_cancelled());
    }

    #[tokio::test]
    async fn coordinated_channel_close_reports_worker_failure_or_completion() {
        let plan = InventoryRootPlan {
            root_entries: Vec::new(),
            worker_roots: Vec::new(),
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        };

        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let worker = tokio::spawn(async {
            panic!("injected coordinated worker task failure");
        });
        IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
            &plan,
            &control,
            vec![worker],
            &sender,
        )
        .await;
        let error = receiver
            .recv()
            .await
            .expect("worker failure should produce an error")
            .expect_err("worker task failure must be reported");
        assert!(error.to_string().contains("worker task failed"));

        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
            &plan,
            &control,
            Vec::new(),
            &sender,
        )
        .await;
        assert!(matches!(
            receiver.recv().await,
            Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: true,
                cancelled: false,
                warning: None,
                ..
            })))
        ));

        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        control.cancel();
        IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
            &plan,
            &control,
            Vec::new(),
            &sender,
        )
        .await;
        assert!(matches!(
            receiver.recv().await,
            Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: true,
                warning: Some(_),
                ..
            })))
        ));
    }

    #[tokio::test]
    async fn coordinated_stream_shutdown_reports_coordinator_join_failure() {
        let (_sender, receiver) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let coordinator = tokio::spawn(async {
            panic!("injected coordinator panic");
        });
        let mut stream = CoordinatedInventoryStream {
            receiver,
            control: Arc::clone(&control),
            coordinator: Some(coordinator),
            terminal_event_seen: false,
        };
        let error = stream.shutdown().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("coordinated inventory task failed")
        );
        assert!(control.is_cancelled());
    }

    #[test]
    fn aggregate_inventory_progress_handles_zero_active_time() {
        let mut progress = HashMap::new();
        progress.insert(0, zero_inventory_progress());
        let aggregate = aggregate_inventory_progress(&progress, 0);
        assert_eq!(aggregate.items_per_second, 0.0);
        assert_eq!(
            aggregate_inventory_progress(&HashMap::new(), 0).items_per_second,
            0.0
        );
    }

    #[test]
    fn accumulate_inventory_progress_handles_zero_active_time() {
        let mut cumulative = zero_inventory_progress();
        accumulate_inventory_progress(
            &mut cumulative,
            None,
            &InventoryProgress {
                unique_items: 3,
                ..zero_inventory_progress()
            },
        );
        assert_eq!(cumulative.unique_items, 3);
        assert_eq!(cumulative.items_per_second, 0.0);
    }

    #[tokio::test]
    async fn coordinated_inventory_deduplicates_roots_and_aggregates_progress() {
        let client = Arc::new(MockOpcClient::default());
        *client.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![
                root_node("Branch A", BrowseNodeKind::Branch, Some("Root.A")),
                root_node("Branch B", BrowseNodeKind::BranchAndItem, Some("Root.B")),
                root_node("Root leaf", BrowseNodeKind::Item, Some("Root.Leaf")),
            ],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        *client.inventory_root_events.lock().unwrap() = HashMap::from([
            (
                "Root.A".into(),
                VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry("A", "A.Item"))),
                    Ok(InventoryEvent::Progress(InventoryProgress {
                        branches_visited: 1,
                        entries_seen: 1,
                        unique_items: 1,
                        active_time_ms: 10,
                        paused_time_ms: 2,
                        items_per_second: 100.0,
                        estimated_remaining_ms: Some(50),
                    })),
                    Ok(InventoryEvent::Slice(InventorySliceObservation {
                        sequence: 11,
                        backend: InventorySliceBackend::Da2,
                        nodes_returned: 1,
                        has_more: true,
                        native_operations: 3,
                        elapsed_ms: 20,
                        entries_seen: 1,
                        unique_items: 1,
                    })),
                    Ok(completed_inventory()),
                ]),
            ),
            (
                "Root.B".into(),
                VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry(
                        "A duplicate",
                        "A.Item",
                    ))),
                    Ok(InventoryEvent::Entry(inventory_entry("B", "B.Item"))),
                    Ok(InventoryEvent::Progress(InventoryProgress {
                        branches_visited: 2,
                        entries_seen: 2,
                        unique_items: 2,
                        active_time_ms: 20,
                        paused_time_ms: 3,
                        items_per_second: 100.0,
                        estimated_remaining_ms: None,
                    })),
                    Ok(InventoryEvent::Slice(InventorySliceObservation {
                        sequence: 22,
                        backend: InventorySliceBackend::Da2,
                        nodes_returned: 2,
                        has_more: false,
                        native_operations: 4,
                        elapsed_ms: 15,
                        entries_seen: 2,
                        unique_items: 2,
                    })),
                    Ok(completed_inventory()),
                ]),
            ),
        ]);
        let mut config = settings(PathBuf::from(":memory:"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        let InventoryHandle {
            mut stream,
            control,
        } = manager
            .start_refresh_inventory(
                "S",
                &Arc::new(()),
                InventoryLimits {
                    item_rate_per_second: 100,
                    batch_size: 25,
                    duty_cycle_percent: 100,
                },
            )
            .await
            .unwrap()
            .expect("two expandable roots should use coordinated inventory");
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event.unwrap());
        }
        stream.shutdown().await.unwrap();
        assert!(!control.is_cancelled());

        let mut entries: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                InventoryEvent::Entry(entry) => Some(entry),
                _ => None,
            })
            .collect();
        entries.sort_by(|left, right| left.item_id.cmp(&right.item_id));
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["A.Item", "B.Item", "Root.B", "Root.Leaf"]
        );
        assert_eq!(entries[2].breadcrumbs, Vec::<String>::new());
        assert_eq!(entries[2].kind, InventoryNodeKind::BranchAndItem);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                InventoryEvent::Progress(progress)
                    if progress.branches_visited == 3
                        && progress.entries_seen == 3
                        && progress.active_time_ms == 30
                        && progress.paused_time_ms == 5
                        && (3..=4).contains(&progress.unique_items)
            )
        }));
        let slices: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                InventoryEvent::Slice(slice) => Some(slice),
                _ => None,
            })
            .collect();
        assert_eq!(
            slices.iter().map(|slice| slice.nodes_returned).sum::<u64>(),
            3
        );
        assert_eq!(
            slices
                .iter()
                .map(|slice| slice.native_operations)
                .sum::<u64>(),
            7
        );
        assert_eq!(slices.iter().map(|slice| slice.elapsed_ms).max(), Some(20));
        assert!(slices.iter().any(|slice| slice.entries_seen == 2));
        assert!(
            slices
                .iter()
                .all(|slice| (3..=4).contains(&slice.unique_items))
        );
        assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 2);
        let mut started_roots = client.inventory_started_roots.lock().unwrap().clone();
        started_roots.sort();
        assert_eq!(started_roots, vec!["Root.A", "Root.B"]);
        assert!(matches!(
            events.last(),
            Some(InventoryEvent::Completed(InventoryCompleted {
                complete: true,
                cancelled: false,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn configured_inventory_root_failure_is_fatal() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_inventories(vec![Err("configured root failed".into())]),
        );
        let mut config = settings(directory.path().join("configured-root-failure.sqlite3"));
        config.inventory_root = Some("FCS0201".into());
        let manager = Arc::new(IndexManager::new(client, config));
        manager.with_database(|_| Ok(())).unwrap();
        let ownership = Arc::new(());
        let error = manager
            .start_refresh_inventory("S", &ownership, coordinator_limits())
            .await
            .err()
            .expect("configured root failure should be returned");
        assert!(error.to_string().contains("configured root failed"));
        assert!(manager.status("S").await.is_ok());
    }

    #[tokio::test]
    async fn configured_inventory_root_failure_is_cleanly_cancelled_when_pending() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_inventories(vec![Err("configured root failed".into())]),
        );
        let mut config = settings(directory.path().join("configured-root-cancelled.sqlite3"));
        config.inventory_root = Some("FCS0201".into());
        let manager = Arc::new(IndexManager::new(client, config));
        manager.with_database(|_| Ok(())).unwrap();
        manager.pending_cancels.lock().unwrap().insert("S".into());

        let ownership = Arc::new(());
        let result = manager
            .start_refresh_inventory("S", &ownership, coordinator_limits())
            .await
            .unwrap();
        assert!(result.is_none());
        assert!(manager.pending_cancels.lock().unwrap().is_empty());
        assert!(manager.status("S").await.is_ok());
    }

    #[tokio::test]
    async fn generation_start_failure_is_recorded_and_reported() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Ok(default_capabilities())],
        ));
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("generation-lock-poisoned.sqlite3")),
        ));
        manager.with_database(|_| Ok(())).unwrap();
        let ownership = manager
            .reserve_refresh_build("S", true)
            .unwrap()
            .expect("build reservation should succeed");
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        manager
            .with_database(|database| {
                database
                    .connection
                    .execute_batch(
                        "CREATE TRIGGER reject_staging_generation
                     BEFORE INSERT ON generations
                     WHEN NEW.state = 'staging'
                     BEGIN
                         SELECT RAISE(ABORT, 'staging generation rejected');
                     END;",
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();

        let error = manager
            .start_refresh_generation("S", &control, &ownership, false)
            .await
            .expect_err("the staging generation trigger should fail generation start");
        assert!(error.to_string().contains("staging generation rejected"));
    }

    #[tokio::test]
    async fn lifecycle_client_reports_missing_root_inventory_fixture() {
        let client = LifecycleClient::new(vec![], vec![]);
        let result = client.start_inventory_at_root("S", "Root", 1).await;
        assert!(result.is_err());
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("no root inventory configured"))
        );
    }

    #[tokio::test]
    async fn coordinated_start_falls_back_when_partitioning_is_not_safe() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![Ok(immediate_inventory_handle())], vec![])
                .with_root_browse_page(Ok(BrowsePage {
                    nodes: vec![root_node(
                        "Only branch",
                        BrowseNodeKind::Branch,
                        Some("Root.Only"),
                    )],
                    next_page_token: None,
                    complete: true,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                    warning: None,
                })),
        );
        let mut config = settings(directory.path().join("partition-unsafe.sqlite3"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client.clone(), config));

        let handle = manager
            .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
            .await
            .unwrap()
            .expect("unsafe partitioning should fall back to full-root inventory");

        assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
        assert_eq!(client.root_inventory_start_count.load(Ordering::Acquire), 0);
        drop(handle);
    }

    #[tokio::test]
    async fn coordinated_start_failure_falls_back_to_full_root_inventory() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![Ok(immediate_inventory_handle())], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_close_result(Err("partition discovery failed".into())),
        );
        let mut config = settings(directory.path().join("partition-fallback.sqlite3"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client.clone(), config));
        let handle = manager
            .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
            .await
            .unwrap()
            .expect("fallback should start a full-root inventory");
        assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
        assert_eq!(client.root_inventory_start_count.load(Ordering::Acquire), 0);
        drop(handle);
    }

    #[tokio::test]
    async fn coordinated_inventory_reports_worker_failure() {
        let client = Arc::new(MockOpcClient::default());
        *client.browse_page_result.lock().unwrap() = Ok(BrowsePage {
            nodes: vec![
                root_node("A", BrowseNodeKind::Branch, Some("Root.A")),
                root_node("B", BrowseNodeKind::Branch, Some("Root.B")),
            ],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        });
        *client.inventory_root_events.lock().unwrap() = HashMap::from([
            (
                "Root.A".into(),
                VecDeque::from([Err("worker exploded".into())]),
            ),
            ("Root.B".into(), VecDeque::from([Ok(completed_inventory())])),
        ]);
        let mut config = settings(PathBuf::from(":memory:"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client, config));
        let InventoryHandle { mut stream, .. } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .expect("two roots should use coordinated inventory");

        let error = stream
            .next()
            .await
            .expect("worker failure should produce an event")
            .expect_err("worker failure must be terminal");
        assert!(error.to_string().contains("worker exploded"));
        stream.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coordinated_inventory_reports_worker_registration_failure() {
        let control = Arc::new(RecordingInventoryControl::default());
        control.fail_pacing_on_call(1);
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(BrowsePage {
                    nodes: vec![
                        root_node("A", BrowseNodeKind::Branch, Some("Root.A")),
                        root_node("B", BrowseNodeKind::Branch, Some("Root.B")),
                    ],
                    next_page_token: None,
                    complete: true,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                    warning: None,
                }))
                .with_root_inventories(vec![Ok(handle_with_control(VecDeque::new(), control))]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        let InventoryHandle { mut stream, .. } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .expect("one root should use coordinated inventory");
        let error = stream
            .next()
            .await
            .expect("registration failure should produce an event")
            .expect_err("registration failure must be terminal");
        assert!(error.to_string().contains("test pacing update failure"));
    }

    #[tokio::test]
    async fn coordinated_inventory_suppresses_registration_error_after_stop_request() {
        let parent = CoordinatedInventoryControl::new(InventoryPacing::default());
        let control = Arc::new(RegisterStoppingControl {
            parent: Arc::clone(&parent),
        });
        let control_for_handle = Arc::clone(&control);
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![]).with_root_inventories(vec![Ok(
                handle_with_control(VecDeque::new(), control_for_handle),
            )]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(Arc::clone(&manager).run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            parent,
            sender,
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerInventoryMessage::Finished { _worker_id: 0 })
        ));
        worker.await.unwrap();
        control.pause();
        control.resume();
        control.cancel();
    }

    #[tokio::test]
    async fn coordinated_inventory_cancels_after_outer_entry_send_failure() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![Ok(InventoryHandle {
                    stream: Box::new(BlockingInventoryStream {
                        started: Arc::clone(&started),
                        release: Arc::clone(&release),
                        event: Some(Ok(InventoryEvent::Entry(inventory_entry(
                            "Entry",
                            "Root.A.Entry",
                        )))),
                    }),
                    control: Arc::new(RecordingInventoryControl::default()),
                })]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        let InventoryHandle { stream, control } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .expect("two roots should use coordinated inventory");
        started.notified().await;
        drop(stream);
        release.notify_one();
        for _ in 0..100 {
            if control.is_cancelled() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("coordinator did not observe the closed output channel");
    }

    #[tokio::test]
    async fn coordinated_worker_sender_failures_stop_and_clean_up_the_worker() {
        let events = vec![
            (completed_inventory(), false),
            (
                InventoryEvent::Entry(inventory_entry("Entry", "Entry.Item")),
                true,
            ),
            (InventoryEvent::Progress(zero_inventory_progress()), true),
            (
                InventoryEvent::Slice(InventorySliceObservation {
                    sequence: 1,
                    backend: InventorySliceBackend::Da2,
                    nodes_returned: 1,
                    has_more: false,
                    native_operations: 1,
                    elapsed_ms: 1,
                    entries_seen: 1,
                    unique_items: 1,
                }),
                true,
            ),
            (completed_inventory(), true),
        ];
        for (event, wait_for_started) in events {
            let (stream, gate) = if wait_for_started {
                let started = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                (
                    Box::new(BlockingInventoryStream {
                        started: Arc::clone(&started),
                        release: Arc::clone(&release),
                        event: Some(Ok(event)),
                    }) as Box<dyn InventoryStream>,
                    Some((started, release)),
                )
            } else {
                (
                    Box::new(VecInventoryStream {
                        events: VecDeque::from([Ok(event)]),
                    }) as Box<dyn InventoryStream>,
                    None,
                )
            };
            let client = Arc::new(LifecycleClient::new(vec![], vec![]).with_root_inventories(
                vec![Ok(InventoryHandle {
                    stream,
                    control: Arc::new(RecordingInventoryControl::default()),
                })],
            ));
            let manager = Arc::new(IndexManager::new(
                client,
                settings(PathBuf::from(":memory:")),
            ));
            let control = CoordinatedInventoryControl::new(InventoryPacing::default());
            let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
            let (sender, mut receiver) =
                tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
            let worker = tokio::spawn(Arc::clone(&manager).run_inventory_worker(
                "S".into(),
                0,
                queue,
                coordinator_limits(),
                control,
                sender,
            ));
            if wait_for_started {
                assert!(matches!(
                    receiver.recv().await,
                    Some(WorkerInventoryMessage::Started { worker_id: 0 })
                ));
                let (started, release) = gate.unwrap();
                started.notified().await;
                drop(receiver);
                release.notify_one();
            } else {
                drop(receiver);
            }
            worker.await.unwrap();
        }
    }

    #[tokio::test]
    async fn coordinated_inventory_falls_back_without_safe_roots() {
        let client = Arc::new(MockOpcClient::default());
        *client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            supports_browse_sessions: false,
            supports_search: false,
            max_page_size: 100,
        });
        let mut config = settings(PathBuf::from(":memory:"));
        config.worker_count = 4;
        let manager = Arc::new(IndexManager::new(client.clone(), config));

        assert!(
            manager
                .start_coordinated_inventory("S", coordinator_limits())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 0);

        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("flat-fallback.sqlite3")),
        ));
        let handle = manager
            .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
            .await
            .unwrap()
            .expect("unsafe partitioning should fall back to full-root inventory");
        assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
        drop(handle);
    }

    #[tokio::test]
    async fn coordinated_inventory_falls_back_when_root_page_has_too_few_workers() {
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![]).with_root_browse_page(Ok(BrowsePage {
                nodes: vec![root_node(
                    "Only branch",
                    BrowseNodeKind::Branch,
                    Some("Root"),
                )],
                next_page_token: None,
                complete: true,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            })),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        assert!(
            manager
                .start_coordinated_inventory("S", coordinator_limits())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn coordinated_inventory_rejects_root_continuation_pages() {
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![]).with_root_browse_page(Ok(BrowsePage {
                nodes: Vec::new(),
                next_page_token: Some("next".into()),
                complete: false,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            })),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        let error = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .err()
            .expect("root continuation should be rejected");
        assert!(error.to_string().contains("continuation page"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_forwards_control_to_all_root_workers() {
        let directory = tempdir().unwrap();
        let first_control = Arc::new(RecordingInventoryControl::default());
        let second_control = Arc::new(RecordingInventoryControl::default());
        let started = Arc::new(Notify::new());
        let started_count = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![
                    Ok(InventoryHandle {
                        stream: Box::new(ControlledInventoryStream {
                            started: Arc::clone(&started),
                            started_count: Arc::clone(&started_count),
                            release: Arc::clone(&release),
                            event: Some(Ok(completed_inventory())),
                            shutdowns: Arc::clone(&shutdowns),
                        }),
                        control: Arc::clone(&first_control) as Arc<dyn InventoryControl>,
                    }),
                    Ok(InventoryHandle {
                        stream: Box::new(ControlledInventoryStream {
                            started: Arc::clone(&started),
                            started_count: Arc::clone(&started_count),
                            release: Arc::clone(&release),
                            event: Some(Ok(completed_inventory())),
                            shutdowns: Arc::clone(&shutdowns),
                        }),
                        control: Arc::clone(&second_control) as Arc<dyn InventoryControl>,
                    }),
                ]),
        );
        let mut config = settings(directory.path().join("control-forwarding.sqlite3"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client, config));
        let InventoryHandle {
            stream: mut inventory_stream,
            control,
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        let reader = tokio::spawn(async move { inventory_stream.next().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while started_count.load(Ordering::Acquire) < 2 {
                started.notified().await;
            }
        })
        .await
        .unwrap();
        control.pause();
        control.resume();
        control.cancel();

        assert!(!first_control.paused.load(Ordering::Acquire));
        assert!(!second_control.paused.load(Ordering::Acquire));
        assert_eq!(first_control.pause_count.load(Ordering::Acquire), 1);
        assert_eq!(second_control.pause_count.load(Ordering::Acquire), 1);
        assert_eq!(first_control.resume_count.load(Ordering::Acquire), 1);
        assert_eq!(second_control.resume_count.load(Ordering::Acquire), 1);
        assert!(first_control.is_cancelled());
        assert!(second_control.is_cancelled());
        release.notify_waiters();
        assert!(reader.await.unwrap().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_cancellation_stops_active_workers_and_joins_them() {
        let directory = tempdir().unwrap();
        let started = Arc::new(Notify::new());
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let first_control = Arc::new(CancellationInventoryControl::default());
        let second_control = Arc::new(CancellationInventoryControl::default());
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![
                    Ok(InventoryHandle {
                        stream: Box::new(CancellationAwareInventoryStream {
                            started: Arc::clone(&started),
                            control: Arc::clone(&first_control),
                            shutdowns: Arc::clone(&shutdowns),
                            emitted: false,
                        }),
                        control: Arc::clone(&first_control) as Arc<dyn InventoryControl>,
                    }),
                    Ok(InventoryHandle {
                        stream: Box::new(CancellationAwareInventoryStream {
                            started: Arc::clone(&started),
                            control: Arc::clone(&second_control),
                            shutdowns: Arc::clone(&shutdowns),
                            emitted: false,
                        }),
                        control: Arc::clone(&second_control) as Arc<dyn InventoryControl>,
                    }),
                ]),
        );
        let mut config = settings(directory.path().join("cancellation.sqlite3"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client, config));
        let InventoryHandle {
            stream: mut inventory_stream,
            control,
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        started.notified().await;
        first_control.pause();
        first_control.resume();
        control.cancel();
        let event = inventory_stream.next().await;
        assert!(matches!(
            event,
            Some(Ok(InventoryEvent::Completed(result)))
                if result.cancelled && !result.complete
        ));
        assert_eq!(shutdowns.load(Ordering::Acquire), 2);
        assert!(first_control.is_cancelled());
        assert!(second_control.is_cancelled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_handles_closed_coordinator_senders() {
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        ));
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let (sender, receiver) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
        drop(receiver);
        manager
            .run_coordinated_inventory(
                "S".into(),
                InventoryRootPlan {
                    root_entries: vec![inventory_entry("Root", "Root.Item")],
                    worker_roots: Vec::new(),
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                },
                1,
                coordinator_limits(),
                control,
                sender,
            )
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_worker_handles_registration_cancellation_and_failures() {
        let cases = [
            ("registration-cancelled", true, false),
            ("registration-failed", false, true),
        ];
        for (name, cancel_during_start, fail_registration) in cases {
            let directory = tempdir().unwrap();
            let control = CoordinatedInventoryControl::new(InventoryPacing::default());
            let parent = Arc::clone(&control);
            let inventory_control = Arc::new(RecordingInventoryControl::default());
            if fail_registration {
                inventory_control.fail_pacing_on_call(1);
            }
            let client = LifecycleClient::new(vec![], vec![])
                .with_root_inventory_start_hook(move || {
                    if cancel_during_start {
                        parent.cancel();
                    }
                })
                .with_root_inventories(vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&inventory_control),
                ))]);
            let manager = Arc::new(IndexManager::new(
                Arc::new(client),
                settings(directory.path().join(name)),
            ));
            let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
            let (sender, mut receiver) =
                tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
            let worker = tokio::spawn(manager.run_inventory_worker(
                "S".into(),
                0,
                queue,
                coordinator_limits(),
                control,
                sender,
            ));
            let mut saw_finished = false;
            while let Some(message) = receiver.recv().await {
                match message {
                    WorkerInventoryMessage::Failed { error, .. } => {
                        assert!(fail_registration);
                        assert!(error.contains("test pacing update failure"));
                    }
                    WorkerInventoryMessage::Finished { .. } => {
                        saw_finished = true;
                        break;
                    }
                    _ => {}
                }
            }
            worker.await.unwrap();
            assert!(saw_finished);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_worker_stops_after_cancelled_root_start_failure() {
        let directory = tempdir().unwrap();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let parent = Arc::clone(&control);
        let client = LifecycleClient::new(vec![], vec![])
            .with_root_inventory_start_hook(move || parent.cancel())
            .with_root_inventories(vec![Err("root start failed".into())]);
        let manager = Arc::new(IndexManager::new(
            Arc::new(client),
            settings(directory.path().join("cancelled-root-start.sqlite3")),
        ));
        let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(manager.run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            control,
            sender,
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerInventoryMessage::Finished { _worker_id: 0 })
        ));
        assert!(receiver.recv().await.is_none());
        worker.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_worker_handles_closed_event_senders() {
        let events = vec![
            Some(Ok(InventoryEvent::Entry(inventory_entry(
                "Entry", "S.Entry",
            )))),
            Some(Ok(InventoryEvent::Progress(zero_progress()))),
            Some(Ok(InventoryEvent::Slice(InventorySliceObservation {
                sequence: 1,
                backend: InventorySliceBackend::Da2,
                nodes_returned: 1,
                has_more: false,
                native_operations: 1,
                elapsed_ms: 1,
                entries_seen: 1,
                unique_items: 1,
            }))),
            Some(Ok(completed_inventory())),
            Some(Err(anyhow::anyhow!("stream error"))),
            None,
        ];
        for event in events {
            let directory = tempdir().unwrap();
            let client = LifecycleClient::new(vec![], vec![]).with_root_inventories(vec![Ok(
                handle_with_control(
                    event.into_iter().collect(),
                    Arc::new(RecordingInventoryControl::default()),
                ),
            )]);
            let manager = Arc::new(IndexManager::new(
                Arc::new(client),
                settings(directory.path().join("closed-sender.sqlite3")),
            ));
            let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
            let control = CoordinatedInventoryControl::new(InventoryPacing::default());
            let (sender, mut receiver) =
                tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
            let worker = tokio::spawn(manager.run_inventory_worker(
                "S".into(),
                0,
                queue,
                coordinator_limits(),
                Arc::clone(&control),
                sender,
            ));
            assert!(matches!(
                receiver.recv().await,
                Some(WorkerInventoryMessage::Started { worker_id: 0 })
            ));
            drop(receiver);
            worker.await.unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_worker_reports_a_poisoned_root_queue() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("poisoned-queue.sqlite3")),
        ));
        let queue = Arc::new(Mutex::new(VecDeque::<String>::new()));
        let poison_queue = Arc::clone(&queue);
        let _ = std::thread::spawn(move || {
            let _guard = poison_queue.lock().unwrap();
            panic!("poison root queue");
        })
        .join();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(manager.run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            control,
            sender,
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerInventoryMessage::Failed { error, .. })
                if error.contains("root queue lock poisoned")
        ));
        worker.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_detects_worker_task_panic() {
        let directory = tempdir().unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![
                    Ok(InventoryHandle {
                        stream: Box::new(PanicAfterReleaseInventoryStream {
                            started: Arc::clone(&started),
                            release: Arc::clone(&release),
                        }),
                        control: Arc::new(RecordingInventoryControl::default()),
                    }),
                    Ok(immediate_inventory_handle()),
                ]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("worker-panic.sqlite3")),
        ));
        let InventoryHandle {
            stream: mut inventory_stream,
            ..
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        started.notified().await;
        release.notify_one();
        let event = inventory_stream.next().await;
        assert!(matches!(
            event,
            Some(Err(error)) if error.to_string().contains("panicked")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_reports_root_worker_start_failure() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![
                    Err("root worker failed to start".into()),
                    Ok(immediate_inventory_handle()),
                ]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("worker-start-failure.sqlite3")),
        ));
        let InventoryHandle {
            stream: mut inventory_stream,
            ..
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        let event = inventory_stream.next().await;
        assert!(matches!(
            event,
            Some(Err(error)) if error.to_string().contains("root worker failed to start")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_reports_incomplete_worker_termination() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![
                    Ok(InventoryHandle {
                        stream: Box::new(VecInventoryStream {
                            events: VecDeque::from([Ok(InventoryEvent::Completed(
                                InventoryCompleted {
                                    complete: false,
                                    cancelled: false,
                                    truncated: false,
                                    warning: Some("worker ended early".into()),
                                    organization: NamespaceOrganization::Hierarchical,
                                    source: BrowseSource::Da2,
                                },
                            ))]),
                        }),
                        control: Arc::new(RecordingInventoryControl::default()),
                    }),
                    Ok(immediate_inventory_handle()),
                ]),
        );
        let mut config = settings(directory.path().join("worker-incomplete.sqlite3"));
        config.worker_count = 2;
        let manager = Arc::new(IndexManager::new(client, config));
        let InventoryHandle {
            stream: mut inventory_stream,
            ..
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        let event = inventory_stream.next().await;
        assert!(matches!(
            event,
            Some(Err(error)) if error.to_string().contains("ended before completion")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_reports_root_browse_close_failure() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_close_result(Err("root browse close failed".into())),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("root-close-failure.sqlite3")),
        ));
        let result = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.to_string().contains("root browse close failed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinated_inventory_shutdown_panic_is_reported() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![])
                .with_root_browse_page(Ok(coordinator_root_page()))
                .with_root_inventories(vec![Ok(InventoryHandle {
                    stream: Box::new(CompletedThenShutdownPanicInventoryStream { emitted: false }),
                    control: Arc::new(RecordingInventoryControl::default()),
                })]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("shutdown-panic.sqlite3")),
        ));
        let InventoryHandle {
            stream: mut inventory_stream,
            ..
        } = manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .unwrap();
        let event = inventory_stream.next().await;
        assert!(matches!(
            event,
            Some(Err(error)) if error.to_string().contains("panicked")
        ));
    }

    fn coordinator_limits() -> InventoryLimits {
        InventoryLimits {
            item_rate_per_second: 0,
            batch_size: 25,
            duty_cycle_percent: 100,
        }
    }

    fn coordinator_root_page() -> BrowsePage {
        BrowsePage {
            nodes: vec![
                root_node("Branch A", BrowseNodeKind::Branch, Some("Root.A")),
                root_node("Branch B", BrowseNodeKind::Branch, Some("Root.B")),
            ],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        }
    }

    #[test]
    fn normalization_and_timestamp_helpers_are_safe() {
        assert_eq!(normalize_query("  FCS0201   PV "), "fcs0201 pv");
        assert_eq!(escape_like(r"a%b_c\d"), r"a\%b\_c\\d");
        assert_eq!(prefix_upper_bound("abc"), Some("abd".into()));
        assert_eq!(prefix_upper_bound("a\u{10ffff}"), Some("b".into()));
        assert_eq!(
            prefix_upper_bound("\u{d7ff}\u{10ffff}"),
            Some("\u{e000}".into())
        );
        assert_eq!(prefix_upper_bound("\u{10ffff}"), None);
        assert_eq!(build_fts_query("fcs0201 pv"), "\"fcs0201\" AND \"pv\"");
        assert_eq!(search_rank("219", "219", "display-exact"), 0);
        assert_eq!(search_rank("219", "ordinary", "219"), 1);
        assert_eq!(search_rank("219", "219 block", "ordinary"), 2);
        assert_eq!(search_rank("219", "ordinary", "219.item"), 3);
        assert_eq!(search_rank("219", "block 219", "display-contains"), 4);
        assert_eq!(search_rank("219", "ordinary", "area.219.item"), 5);
        assert_eq!(search_rank("219", "ordinary", "ordinary"), 6);
        assert_eq!(parse_indexed_kind(2), Ok(InventoryNodeKind::BranchAndItem));
        assert!(parse_indexed_kind(99).is_err());
        assert_eq!(
            parse_indexed_breadcrumbs(r#"["Area","Unit"]"#.into()).unwrap(),
            vec!["Area", "Unit"]
        );
        assert!(parse_indexed_breadcrumbs("not-json".into()).is_err());
        assert!(parse_timestamp("not-a-timestamp").is_none());
        assert_eq!(
            parse_timestamp(u128::from(u64::MAX).to_string().as_str()),
            UNIX_EPOCH.checked_add(Duration::from_millis(u64::MAX))
        );
        assert!(parse_timestamp(u128::MAX.to_string().as_str()).is_none());
        assert_eq!(SearchMode::try_from(0), Ok(SearchMode::Unspecified));
        assert_eq!(SearchMode::try_from(1), Ok(SearchMode::Exact));
        assert_eq!(SearchMode::try_from(2), Ok(SearchMode::Prefix));
        assert_eq!(SearchMode::try_from(3), Ok(SearchMode::Contains));
        assert_eq!(SearchMode::try_from(4), Err(()));
    }

    fn in_memory_index_with(entries: &[InventoryEntry]) -> (IndexDb, u64) {
        let mut database = IndexDb::open(Path::new(":memory:")).unwrap();
        let generation = database
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        database.insert_entries("S", generation, entries).unwrap();
        (database, generation)
    }

    #[cfg(feature = "fuzzing")]
    #[test]
    fn fuzzing_search_classifies_fts_syntax_rejections() {
        let query = "fuzz!\0\u{3}";
        let entries = [inventory_entry(query, "0")];

        assert!(matches!(
            super::fuzzing::search_all_modes(query, &entries, 10),
            Err(super::fuzzing::SearchAllModesError::QueryRejected(_))
        ));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn indexed_query_normalization_is_idempotent(value in any::<String>()) {
            let normalized = normalize_query(&value);
            prop_assert_eq!(normalize_query(&normalized), normalized);
        }

        #[test]
        fn indexed_search_preserves_match_tier_order(
            // SQLite FTS5 trigram matching does not support supplementary-plane query scalars.
            value in prop::collection::vec(
                proptest::char::range(' ', '~'),
                0..32,
            )
            .prop_map(|chars| chars.into_iter().collect::<String>()),
        ) {
            let query = format!("fuzz{value}");
            let item_prefix = format!("{query}\0prefix");
            let item_contains = format!("contains:{query}:suffix");
            let entries = [
                inventory_entry(&query, "0"),
                inventory_entry("zzzz", &query),
                inventory_entry(&format!("{query} suffix"), "2"),
                inventory_entry("zzzz", &item_prefix),
                inventory_entry(&format!("prefix {query} suffix"), "4"),
                inventory_entry("zzzz", &item_contains),
                inventory_entry("zzzz", "6"),
            ];
            let (database, generation) = in_memory_index_with(&entries);
            let item_ids = |mode| {
                database
                    .search("S", generation, &query, mode, 10)
                    .unwrap()
                    .into_iter()
                    .map(|entry| entry.item_id)
                    .collect::<Vec<_>>()
            };

            prop_assert_eq!(item_ids(0), item_ids(3));
            prop_assert_eq!(
                item_ids(1),
                vec!["0".to_string(), query.clone()]
            );
            prop_assert_eq!(
                item_ids(2),
                vec![
                    "0".to_string(),
                    query.clone(),
                    "2".to_string(),
                    item_prefix.clone()
                ]
            );
            prop_assert_eq!(
                item_ids(3),
                vec![
                    "0".to_string(),
                    query,
                    "2".to_string(),
                    item_prefix,
                    "4".to_string(),
                    item_contains,
                ]
            );
        }

        #[test]
        fn indexed_record_storage_round_trips_fields_and_breadcrumbs(
            item_id in any::<String>(),
            display_name in any::<String>(),
            breadcrumbs in prop::collection::vec(any::<String>(), 0..8),
            branch_and_item in any::<bool>(),
        ) {
            let item_id = if item_id.is_empty() {
                "fuzz-item".to_string()
            } else {
                item_id
            };
            let display_name = format!("fuzz{display_name}");
            let kind = if branch_and_item {
                InventoryNodeKind::BranchAndItem
            } else {
                InventoryNodeKind::Item
            };
            let entry = InventoryEntry {
                item_id: item_id.clone(),
                display_name: display_name.clone(),
                kind,
                breadcrumbs: breadcrumbs.clone(),
            };
            let expected = IndexedMatch {
                item_id,
                display_name,
                kind,
                breadcrumbs,
            };
            let (database, generation) = in_memory_index_with(std::slice::from_ref(&entry));
            let matches = database
                .search("S", generation, &entry.display_name, 1, 10)
                .unwrap();

            prop_assert_eq!(matches, vec![expected]);
        }
    }

    #[test]
    fn build_telemetry_aggregates_slices_entries_and_commits() {
        let mut telemetry = BuildTelemetry::default();
        assert_eq!(telemetry.commit_latency_percentile(50), None);
        telemetry.record_progress();
        telemetry.record_entry(InventoryNodeKind::Item);
        telemetry.record_entry(InventoryNodeKind::BranchAndItem);
        telemetry.record_slice(&InventorySliceObservation {
            sequence: 1,
            backend: InventorySliceBackend::Da2,
            nodes_returned: 10,
            has_more: true,
            native_operations: 4,
            elapsed_ms: 25,
            entries_seen: 10,
            unique_items: 8,
        });
        telemetry.record_slice(&InventorySliceObservation {
            sequence: 2,
            backend: InventorySliceBackend::Da3,
            nodes_returned: 5,
            has_more: false,
            native_operations: 2,
            elapsed_ms: 40,
            entries_seen: 14,
            unique_items: 11,
        });
        telemetry.record_commit(10, Duration::from_millis(7), false);
        telemetry.record_commit(0, Duration::from_millis(12), true);
        telemetry.record_terminal_event(Duration::from_millis(123));

        assert_eq!(telemetry.progress_events, 1);
        assert_eq!(telemetry.item_entries, 1);
        assert_eq!(telemetry.branch_and_item_entries, 1);
        assert_eq!(telemetry.slice_count, 2);
        assert_eq!(telemetry.slice_nodes_returned, 15);
        assert_eq!(telemetry.slice_native_operations, 6);
        assert_eq!(telemetry.slice_elapsed_ms, 65);
        assert_eq!(telemetry.slice_elapsed_max_ms, 40);
        assert_eq!(telemetry.slice_entries_delta, 14);
        assert_eq!(telemetry.slice_entries_delta_max, 10);
        assert_eq!(telemetry.slice_unique_items_delta, 11);
        assert_eq!(telemetry.da2_slices, 1);
        assert_eq!(telemetry.da3_slices, 1);
        assert_eq!(telemetry.commit_attempts, 2);
        assert_eq!(telemetry.commit_failures, 1);
        assert_eq!(telemetry.committed_entries, 10);
        assert_eq!(telemetry.commit_elapsed_ms, 19);
        assert_eq!(telemetry.commit_elapsed_max_ms, 12);
        assert_eq!(telemetry.commit_latency_percentile(50), Some(7));
        assert_eq!(telemetry.commit_latency_percentile(95), Some(12));
        assert_eq!(telemetry.terminal_event_ms, Some(123));
    }

    #[test]
    fn build_telemetry_keeps_only_the_latest_commit_latency_samples() {
        let mut telemetry = BuildTelemetry::default();
        for elapsed_ms in 0..=256 {
            telemetry.record_commit(0, Duration::from_millis(elapsed_ms), false);
        }

        assert_eq!(telemetry.commit_latency_samples_ms.len(), 256);
        assert_eq!(telemetry.commit_latency_samples_ms.front(), Some(&1));
        assert_eq!(telemetry.commit_latency_samples_ms.back(), Some(&256));
        assert_eq!(telemetry.commit_latency_percentile(50), Some(128));
    }

    #[test]
    fn terminal_counts_distinguish_progress_snapshot_from_persisted_rows() {
        let config = settings(PathBuf::from("test-index.sqlite3"));
        let mut state = BuildRunState::new(&config, None);
        state.last_progress = InventoryProgress {
            entries_seen: 11,
            unique_items: 9,
            ..zero_progress()
        };
        state.persisted_item_count = 10;
        state.drained_event_count = 4;
        state.received_entry_count = 3;
        state.pending = vec![
            inventory_entry("Pending A", "A.pending"),
            inventory_entry("Pending A duplicate", "A.pending"),
            inventory_entry("Pending B", "B.pending"),
        ];

        assert_eq!(
            state.terminal_counts(),
            TerminalBuildCounts {
                last_progress_entries_seen: 11,
                last_progress_unique_items: 9,
                persisted_items: 10,
                drained_events: 4,
                received_entry_events: 3,
                pending_entries: 3,
                pending_unique_items: 2,
            }
        );
    }

    #[tokio::test]
    async fn terminal_counts_reconcile_entries_drained_before_stream_termination() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let mut handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry("A", "A.item"))),
                    Ok(InventoryEvent::Progress(zero_progress())),
                    Ok(InventoryEvent::Entry(inventory_entry("B", "B.item"))),
                ]),
            }),
            control,
        };
        let mut state = BuildRunState::new(&settings(PathBuf::from(":memory:")), None);

        let outcome = manager
            .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
            .await;

        assert!(matches!(
            outcome,
            BuildLoopOutcome::Failed(error)
                if error == "inventory stream ended before completion"
        ));
        let counts = state.terminal_counts();
        assert_eq!(counts.drained_events, 3);
        assert_eq!(counts.received_entry_events, 2);
        assert_eq!(counts.persisted_items, 0);
        assert_eq!(counts.pending_entries, 2);
        assert_eq!(counts.pending_unique_items, 2);
        assert_eq!(
            counts.received_entry_events,
            counts.persisted_items + counts.pending_entries
        );
        assert_eq!(
            namespace_string(NamespaceOrganization::Unspecified),
            "unspecified"
        );
        assert_eq!(namespace_string(NamespaceOrganization::Flat), "flat");
    }

    #[test]
    fn index_operation_errors_have_stable_messages_and_sources() {
        let unknown = IndexOperationError::UnknownServer {
            server: "Typo.Server".into(),
        };
        assert_eq!(
            unknown.to_string(),
            "OPC DA server \"Typo.Server\" is not registered"
        );
        assert!(unknown.source().is_none());

        let not_enrolled = IndexOperationError::NotEnrolled {
            server: "Unenrolled.Server".into(),
        };
        assert_eq!(
            not_enrolled.to_string(),
            "namespace index for OPC DA server \"Unenrolled.Server\" is not enrolled"
        );
        assert!(not_enrolled.source().is_none());

        let deleting = IndexOperationError::Deleting {
            server: "Deleting.Server".into(),
        };
        assert_eq!(
            deleting.to_string(),
            "namespace index for OPC DA server \"Deleting.Server\" is being deleted"
        );
        assert!(deleting.source().is_none());

        let internal = IndexOperationError::Internal(anyhow::anyhow!("database failed"));
        assert_eq!(internal.to_string(), "database failed");
        assert!(internal.source().is_some());
    }

    #[test]
    fn database_coordination_key_handles_relative_and_unresolvable_paths() {
        let directory = tempdir().unwrap();
        let absolute = directory.path().join("index.sqlite3");
        assert_eq!(
            database_coordination_key(Path::new(":memory:"), std::env::current_dir),
            PathBuf::from(":memory:")
        );
        assert_eq!(canonical_database_path(Path::new("")), PathBuf::from(""));
        assert_eq!(
            database_coordination_key(&absolute, std::env::current_dir),
            canonical_database_path(&absolute)
        );
        assert_eq!(
            database_coordination_key(Path::new("index.sqlite3"), || {
                Ok(PathBuf::from("/database"))
            }),
            PathBuf::from("/database/index.sqlite3")
        );
        assert_eq!(
            database_coordination_key(Path::new("index.sqlite3"), || {
                Err(std::io::Error::other("current directory unavailable"))
            }),
            PathBuf::from("index.sqlite3")
        );
    }

    #[test]
    fn database_coordination_reuses_the_same_identity_for_path_aliases() {
        let directory = tempdir().unwrap();
        let nested = directory.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let database = directory.path().join("index.sqlite3");
        fs::write(&database, []).unwrap();
        let alias = nested.join("..").join("index.sqlite3");

        let canonical_coordination = database_coordination(&database);
        let aliased_coordination = database_coordination(&alias);

        assert!(Arc::ptr_eq(&canonical_coordination, &aliased_coordination));
        assert_eq!(
            build_lock_path(&database, "S"),
            build_lock_path(&alias, "S")
        );
    }

    #[test]
    fn in_memory_databases_do_not_share_coordination() {
        let first = database_coordination(Path::new(":memory:"));
        let second = database_coordination(Path::new(":memory:"));

        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn in_memory_build_locks_are_not_file_backed() {
        let lock = BuildFileLock::acquire(Path::new(":memory:"), "S").unwrap();

        assert!(lock.file.is_none());
        assert!(!BuildFileLock::is_held(Path::new(":memory:"), "S").unwrap());
    }

    #[test]
    fn adaptive_limits_translate_to_native_operation_pacing() {
        let pacing = pacing_for_limits(InventoryLimits {
            item_rate_per_second: 100,
            batch_size: 10,
            duty_cycle_percent: 50,
        });
        assert_eq!(pacing.min_interval, Duration::ZERO);
        assert_eq!(pacing.item_rate_per_second, Some(100));
        assert_eq!(pacing.batch_size, Some(10));
        assert_eq!(
            pacing_for_limits(InventoryLimits {
                item_rate_per_second: 3,
                batch_size: 1,
                duty_cycle_percent: 1,
            })
            .min_interval,
            Duration::ZERO
        );
        assert_eq!(
            pacing_for_limits(InventoryLimits {
                item_rate_per_second: 0,
                batch_size: 1,
                duty_cycle_percent: 1,
            })
            .min_interval,
            Duration::ZERO
        );
        assert_eq!(
            pacing_for_limits(InventoryLimits {
                item_rate_per_second: 0,
                batch_size: 1,
                duty_cycle_percent: 1,
            })
            .item_rate_per_second,
            None
        );
        assert_eq!(
            pacing_for_limits(InventoryLimits {
                item_rate_per_second: 100,
                batch_size: MAX_NATIVE_INVENTORY_BATCH_SIZE + 1,
                duty_cycle_percent: 50,
            })
            .batch_size,
            Some(MAX_NATIVE_INVENTORY_BATCH_SIZE)
        );
        assert_ne!(stable_server_hash("S"), stable_server_hash("T"));
    }

    #[test]
    fn slice_observations_feed_adaptive_controller_health_state() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        let slice = InventorySliceObservation {
            sequence: 1,
            backend: InventorySliceBackend::Da2,
            nodes_returned: 0,
            has_more: false,
            native_operations: 0,
            elapsed_ms: 1,
            entries_seen: 0,
            unique_items: 0,
        };
        let observation = manager.controller_observation_for_slice("S", &slice);
        assert!(observation.inventory_error);
        assert!(!observation.foreground_active);
    }

    #[test]
    fn foreground_metrics_keep_rolling_latency_percentiles() {
        let mut metrics = ForegroundMetricState::default();
        metrics.record_health_at(Instant::now(), 30, false, false, false);
        metrics.record_health_at(Instant::now(), 10, true, true, true);
        metrics.record_health_at(Instant::now(), 20, false, false, false);
        let snapshot = metrics.snapshot(2);
        assert_eq!(snapshot.active_count, 2);
        assert_eq!(snapshot.operations, 3);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.bad_quality, 1);
        assert_eq!(snapshot.latency_p50_ms, Some(20));
        assert_eq!(snapshot.latency_p95_ms, Some(30));
        assert_eq!(snapshot.latency_max_ms, Some(30));
        assert!(!snapshot.last_error);
    }

    #[test]
    fn foreground_health_failures_expire_without_a_follow_up_operation() {
        let recorded_at = Instant::now();
        let mut metrics = ForegroundMetricState::default();
        metrics.record_health_at(recorded_at, 10, true, true, true);
        assert!(
            metrics.recent_health_failure(
                recorded_at + Duration::from_secs(1),
                Duration::from_secs(2)
            )
        );
        assert!(
            !metrics.recent_health_failure(
                recorded_at + Duration::from_secs(3),
                Duration::from_secs(2)
            )
        );
    }

    #[test]
    fn foreground_bad_quality_expires_without_a_follow_up_operation() {
        let recorded_at = Instant::now();
        let mut metrics = ForegroundMetricState::default();
        metrics.record_health_at(recorded_at, 10, false, true, false);
        assert!(
            metrics
                .recent_bad_quality(recorded_at + Duration::from_secs(1), Duration::from_secs(2))
        );
        assert!(
            !metrics
                .recent_bad_quality(recorded_at + Duration::from_secs(3), Duration::from_secs(2))
        );
    }

    #[test]
    fn controller_observation_includes_recent_bad_quality() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        manager.record_foreground_operation_with_health(
            "S",
            Duration::from_millis(10),
            false,
            true,
            false,
        );

        let observation = manager.controller_observation("S", false);
        assert!(observation.foreground_bad_quality);
        assert!(!observation.foreground_error);
    }

    #[test]
    fn storage_diagnostics_include_sqlite_sidecars() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let db = IndexDb::open(&path).unwrap();
        drop(db);
        std::fs::write(IndexDb::sqlite_sidecar_path(&path, "-wal"), vec![0_u8; 7]).unwrap();
        std::fs::write(IndexDb::sqlite_sidecar_path(&path, "-shm"), vec![0_u8; 11]).unwrap();
        let storage = storage_diagnostics_for_path(&path);
        assert_eq!(storage.wal_bytes, 7);
        assert_eq!(storage.shm_bytes, 11);
        assert!(storage.free_bytes.is_some());
    }

    #[test]
    fn sqlite_quarantine_preserves_database_and_sidecars_as_one_bundle() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("invalid.sqlite3");
        let quarantine = directory.path().join("invalid.quarantine");
        let database = b"database contents";
        let wal = b"wal contents";
        let shm = b"shm contents";

        fs::write(&path, database).unwrap();
        fs::write(IndexDb::sqlite_sidecar_path(&path, "-wal"), wal).unwrap();
        fs::write(IndexDb::sqlite_sidecar_path(&path, "-shm"), shm).unwrap();

        assert!(quarantine_index_files(&path, &quarantine).unwrap());
        assert!(!path.exists());
        assert!(!IndexDb::sqlite_sidecar_path(&path, "-wal").exists());
        assert!(!IndexDb::sqlite_sidecar_path(&path, "-shm").exists());
        assert_eq!(fs::read(&quarantine).unwrap(), database);
        assert_eq!(
            fs::read(IndexDb::sqlite_sidecar_path(&quarantine, "-wal")).unwrap(),
            wal
        );
        assert_eq!(
            fs::read(IndexDb::sqlite_sidecar_path(&quarantine, "-shm")).unwrap(),
            shm
        );
    }

    #[test]
    fn sqlite_sidecars_append_to_custom_database_names() {
        let path = PathBuf::from("/tmp/custom-index.db");
        assert_eq!(
            IndexDb::sqlite_sidecar_path(&path, "-wal"),
            PathBuf::from("/tmp/custom-index.db-wal")
        );
        assert_eq!(
            IndexDb::sqlite_sidecar_path(&path, "-shm"),
            PathBuf::from("/tmp/custom-index.db-shm")
        );
    }

    #[test]
    fn retry_state_round_trips_through_index_meta() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let db = IndexDb::open(&path).unwrap();
        let retry_after = Some(SystemTime::now() + Duration::from_secs(30));
        db.set_retry_state("S", retry_after, 3, true).unwrap();
        let (_, failures, circuit_open) = db.retry_state("S").unwrap();
        assert_eq!(failures, 3);
        assert!(circuit_open);
    }

    #[test]
    fn failed_attempt_and_enrollment_state_persist_through_index_db() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("enrollment.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();

        db.record_failed_attempt("S", "inventory failed").unwrap();
        let failed = db.status_rows("S").unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].state, "failed");
        assert_eq!(failed[0].last_error.as_deref(), Some("inventory failed"));

        assert!(db.enrollment("S").unwrap().is_none());
        db.enroll("S", "1").unwrap();
        assert!(
            db.enrollment("S")
                .unwrap()
                .expect("enrollment should exist")
                .auto_refresh_enabled
        );
        assert!(!db.set_auto_refresh("missing", false).unwrap());
        assert!(db.set_auto_refresh("S", false).unwrap());
        assert!(
            !db.enrollment("S")
                .unwrap()
                .expect("enrollment should remain")
                .auto_refresh_enabled
        );
        db.enroll("S", "2").unwrap();
        assert!(
            !db.enrollment("S")
                .unwrap()
                .expect("enrollment should remain")
                .auto_refresh_enabled
        );
    }

    #[test]
    fn build_file_lock_is_exclusive_and_reusable() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let lock = BuildFileLock::acquire_with(&database, "S", |_file, _metadata| Ok(())).unwrap();
        assert!(build_lock_path(&database, "S").exists());
        #[cfg(windows)]
        assert!(build_owner_path(&database, "S").exists());
        assert!(BuildFileLock::is_held(&database, "S").unwrap());
        assert!(!BuildFileLock::is_held(&database, "T").unwrap());
        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("build lock is already held"));
        drop(lock);
        assert!(!BuildFileLock::is_held(&database, "S").unwrap());
        assert!(build_lock_path(&database, "S").exists());
        #[cfg(windows)]
        assert!(!build_owner_path(&database, "S").exists());
        let other_server_lock = BuildFileLock::acquire(&database, "T").unwrap();
        assert_ne!(
            build_lock_path(&database, "T"),
            build_lock_path(&database, "S"),
            "different servers must not share a build lock"
        );
        drop(other_server_lock);
        fs::write(build_lock_path(&database, "S"), "stale process metadata\n").unwrap();
        let replacement = BuildFileLock::acquire(&database, "S").unwrap();
        drop(replacement);
    }

    #[test]
    fn build_file_lock_reports_initialization_and_cleanup_errors() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let error = BuildFileLock::acquire_with(&database, "S", |_file, _metadata| {
            Err(std::io::Error::other("lock metadata write failed"))
        })
        .unwrap_err();
        assert!(error.to_string().contains("lock metadata write failed"));
        assert!(build_lock_path(&database, "S").exists());

        #[cfg(unix)]
        {
            let error = BuildFileLock::acquire(Path::new("/proc/opcda-bridge-index.sqlite3"), "S")
                .unwrap_err();
            assert!(!error.to_string().is_empty());
        }

        #[cfg(unix)]
        {
            let lock = BuildFileLock::acquire(&database, "S").unwrap();
            let lock_path = build_lock_path(&database, "S");
            fs::remove_file(&lock_path).unwrap();
            fs::create_dir(&lock_path).unwrap();
            let subscriber = tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::WARN)
                .finish();
            tracing::subscriber::with_default(subscriber, || drop(lock));
            assert!(lock_path.is_dir());
            fs::remove_dir(lock_path).unwrap();
        }
    }

    #[test]
    fn only_corrupt_or_incompatible_index_errors_are_quarantinable() {
        assert!(is_quarantinable_index_error(&anyhow::anyhow!(
            "unsupported namespace index schema version 99"
        )));
        assert!(is_quarantinable_index_error(&anyhow::anyhow!(
            "invalid namespace index schema version \"corrupt\""
        )));
        assert!(is_quarantinable_index_error(&anyhow::anyhow!(
            "SQLite error: file is not a database"
        )));
        assert!(!is_quarantinable_index_error(&anyhow::anyhow!(
            "FOREIGN KEY constraint failed"
        )));
        assert!(!is_quarantinable_index_error(&anyhow::anyhow!(
            "database is locked"
        )));
        assert_eq!(
            parse_namespace("hierarchical"),
            NamespaceOrganization::Hierarchical
        );
        assert_eq!(
            parse_namespace("unknown"),
            NamespaceOrganization::Unspecified
        );
        assert_eq!(source_string(BrowseSource::Unspecified), "unspecified");
        assert_eq!(source_string(BrowseSource::Da3), "da3");
        assert_eq!(source_string(BrowseSource::Derived), "derived");
        assert_eq!(parse_source("unknown"), BrowseSource::Unspecified);
        assert_eq!(node_kind_number(InventoryNodeKind::Item), 1);
        assert_eq!(node_kind_number(InventoryNodeKind::BranchAndItem), 2);
    }

    #[test]
    fn scheduled_refresh_jitter_is_deterministic_and_bounded() {
        assert_eq!(deterministic_jitter("S", 0), Duration::ZERO);
        assert_eq!(
            deterministic_jitter("S", 3600),
            deterministic_jitter("S", 3600)
        );
        assert!(deterministic_jitter("S", 3600) <= Duration::from_secs(3600));
    }

    #[test]
    fn profile_compatibility_preserves_negotiated_da2_fallbacks() {
        assert!(index_profile_is_compatible(
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            true,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da3,
        ));
        assert!(!index_profile_is_compatible(
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            false,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da3,
        ));
        assert!(!index_profile_is_compatible(
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da3,
            false,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
        ));
        assert!(!index_profile_is_compatible(
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            true,
            NamespaceOrganization::Flat,
            BrowseSource::Da2,
        ));
    }

    #[test]
    fn maintenance_windows_parse_and_match_day_boundaries() {
        let daytime = MaintenanceWindow::parse("08:30-17:00").unwrap();
        assert!(daytime.contains(8 * 60 + 30));
        assert!(!daytime.contains(17 * 60));

        let overnight = MaintenanceWindow::parse("22:00-06:00").unwrap();
        assert!(overnight.contains(23 * 60));
        assert!(overnight.contains(5 * 60 + 59));
        assert!(!overnight.contains(12 * 60));

        let all_day = MaintenanceWindow::parse("00:00-00:00").unwrap();
        assert!(all_day.contains(12 * 60));

        assert!(MaintenanceWindow::parse("bad").is_err());
        assert!(MaintenanceWindow::parse("aa:00-01:00").is_err());
        assert!(MaintenanceWindow::parse("01:aa-02:00").is_err());
        assert!(MaintenanceWindow::parse("25:00-01:00").is_err());
        assert!(MaintenanceWindow::parse("01:60-02:00").is_err());
        assert!(parse_maintenance_windows(&["08:00-09:00".into()]).is_ok());

        let now = chrono::Local
            .with_ymd_and_hms(2026, 1, 1, 9, 0, 0)
            .single()
            .unwrap();
        assert!(maintenance_window_active(&[daytime], now));
        assert!(!maintenance_window_active(&[], now));
    }

    #[tokio::test]
    async fn rate_limiter_and_wait_helpers_honor_cancellation() {
        let control_impl = Arc::new(TestInventoryControl::default());
        let control: Arc<dyn InventoryControl> = control_impl.clone();
        control_impl.pause();
        control_impl.resume();

        let mut disabled = ItemRateLimiter::new(0, 0);
        assert!(disabled.acquire(&control).await);
        control_impl.cancel();
        assert!(!disabled.acquire(&control).await);

        let active_impl = Arc::new(TestInventoryControl::default());
        let active: Arc<dyn InventoryControl> = active_impl.clone();
        assert!(wait_with_cancellation(&active, Duration::ZERO).await);
        active_impl.cancel();
        let mut cancelled_limiter = ItemRateLimiter::new(1, 1);
        assert!(!cancelled_limiter.acquire(&active).await);

        let active_impl = Arc::new(TestInventoryControl::default());
        let active: Arc<dyn InventoryControl> = active_impl.clone();
        let mut limited = ItemRateLimiter::new(10_000, 1);
        assert!(limited.acquire(&active).await);
        assert!(limited.acquire(&active).await);

        let waiting_impl = Arc::new(TestInventoryControl::default());
        let waiting: Arc<dyn InventoryControl> = waiting_impl.clone();
        let canceller = Arc::clone(&waiting_impl);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            canceller.cancel();
        });
        assert!(!wait_with_cancellation(&waiting, Duration::from_millis(200)).await);
        task.await.unwrap();
    }

    #[test]
    fn query_cache_evicts_oldest_and_clears_by_server() {
        let mut cache = QueryCache {
            values: HashMap::new(),
            order: VecDeque::new(),
            capacity: 1,
        };
        let first = CacheKey {
            server: "first".into(),
            generation: 1,
            query: "query".into(),
            mode: 3,
            limit: 10,
        };
        let second = CacheKey {
            server: "second".into(),
            generation: 1,
            query: "query".into(),
            mode: 3,
            limit: 10,
        };
        cache.insert(first.clone(), cached_search("first"));
        assert!(cache.get(&first).is_some());
        cache.insert(second.clone(), cached_search("second"));
        assert!(cache.get(&first).is_none());
        assert!(cache.get(&second).is_some());
        cache.clear_server("second");
        assert!(cache.get(&second).is_none());
    }

    #[test]
    fn sqlite_open_quarantines_invalid_schema_and_recovers_interrupted_builds() {
        let directory = tempdir().unwrap();
        let memory = IndexDb::open(Path::new(":memory:")).unwrap();
        assert_eq!(memory.storage_diagnostics().main_bytes, 0);
        drop(memory);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let readonly = directory.path().join("readonly");
            fs::create_dir(&readonly).unwrap();
            fs::set_permissions(&readonly, fs::Permissions::from_mode(0o500)).unwrap();
            let result = IndexDb::open(&readonly.join("index.sqlite3"));
            fs::set_permissions(&readonly, fs::Permissions::from_mode(0o700)).unwrap();
            assert!(result.is_err());
        }

        let invalid_path = directory.path().join("invalid/index.sqlite3");
        fs::create_dir_all(invalid_path.parent().unwrap()).unwrap();
        let invalid = Connection::open(&invalid_path).unwrap();
        invalid
            .execute_batch(
                "CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );
                 INSERT INTO index_meta(key, value)
                 VALUES ('schema_version', '999');",
            )
            .unwrap();
        drop(invalid);

        let invalid_version_path = directory.path().join("invalid-version.sqlite3");
        let invalid_version = Connection::open(&invalid_version_path).unwrap();
        invalid_version
            .execute_batch(
                "CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );
                 INSERT INTO index_meta(key, value)
                 VALUES ('schema_version', 'corrupt');",
            )
            .unwrap();
        drop(invalid_version);
        let error = IndexDb::open_once(&invalid_version_path)
            .err()
            .expect("invalid schema version should fail");
        assert!(
            error
                .to_string()
                .contains("invalid namespace index schema version")
        );

        let schema2_path = directory.path().join("schema2.sqlite3");
        let schema2 = Connection::open(&schema2_path).unwrap();
        schema2
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );
                 CREATE TABLE generations (
                     server TEXT NOT NULL,
                     generation INTEGER NOT NULL,
                     state TEXT NOT NULL,
                     organization TEXT NOT NULL,
                     source TEXT NOT NULL,
                     started_at TEXT NOT NULL,
                     completed_at TEXT,
                     entry_count INTEGER NOT NULL DEFAULT 0,
                     unique_item_count INTEGER NOT NULL DEFAULT 0,
                     last_error TEXT,
                     PRIMARY KEY (server, generation)
                 );
                 CREATE TABLE entries (
                     server TEXT NOT NULL,
                     generation INTEGER NOT NULL,
                     item_id TEXT NOT NULL,
                     item_id_norm TEXT NOT NULL,
                     display_name TEXT NOT NULL,
                     display_name_norm TEXT NOT NULL,
                     kind INTEGER NOT NULL,
                     breadcrumbs TEXT NOT NULL,
                     PRIMARY KEY (server, generation, item_id),
                     FOREIGN KEY (server, generation)
                       REFERENCES generations(server, generation)
                       ON DELETE CASCADE
                 );
                 CREATE INDEX entries_display_prefix
                   ON entries(server, generation, display_name_norm);
                 CREATE INDEX entries_item_prefix
                   ON entries(server, generation, item_id_norm);
                 CREATE VIRTUAL TABLE entries_fts USING fts5(
                     server UNINDEXED,
                     generation UNINDEXED,
                     item_id,
                     display_name,
                     breadcrumbs,
                     tokenize = 'trigram'
                 );
                 INSERT INTO index_meta(key, value)
                 VALUES ('schema_version', '2');
                 INSERT INTO generations (
                     server, generation, state, organization, source, started_at,
                     completed_at, entry_count, unique_item_count, last_error
                 ) VALUES
                     ('S', 1, 'active', 'hierarchical', 'da2', '1', '2', 1, 1, NULL),
                     ('Failed', 1, 'failed', 'flat', 'da2', '3', NULL, 0, 0, 'failed');
                 INSERT INTO entries (
                     server, generation, item_id, item_id_norm, display_name,
                     display_name_norm, kind, breadcrumbs
                 ) VALUES (
                     'S', 1, 'S.Active', 's.active', 'Active', 'active', 1, '[\"Active\"]'
                 );
                 INSERT INTO entries_fts(server, generation, item_id, display_name, breadcrumbs)
                 VALUES ('S', 1, 'S.Active', 'Active', 'Active');",
            )
            .unwrap();
        drop(schema2);

        let rollback_path = directory.path().join("schema2-rollback.sqlite3");
        fs::copy(&schema2_path, &rollback_path).unwrap();
        let rollback = Connection::open(&rollback_path).unwrap();
        rollback
            .execute_batch(
                "CREATE TRIGGER reject_schema_version_update
                 BEFORE INSERT ON index_meta
                 BEGIN
                   SELECT RAISE(FAIL, 'schema migration metadata update rejected');
                 END;",
            )
            .unwrap();
        drop(rollback);
        let migration_error = IndexDb::open_once(&rollback_path)
            .err()
            .expect("schema migration failure should be surfaced");
        assert!(
            migration_error
                .to_string()
                .contains("schema migration metadata update rejected")
        );
        let rolled_back = Connection::open(&rollback_path).unwrap();
        assert_eq!(
            rolled_back
                .query_row(
                    "SELECT value FROM index_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "2"
        );
        let generation_columns = rolled_back
            .prepare("PRAGMA table_info(generations)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            !generation_columns
                .iter()
                .any(|column| column == "compatibility_fallback")
        );
        drop(rolled_back);

        let migrated = IndexDb::open_once(&schema2_path).unwrap();
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT value FROM index_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            SCHEMA_VERSION.to_string()
        );
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT compatibility_fallback FROM generations WHERE server = 'S' AND generation = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM enrolled_servers WHERE server = 'S'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(
            !migrated
                .active_profile("S")
                .unwrap()
                .unwrap()
                .compatibility_fallback
        );
        assert_eq!(migrated.status_rows("S").unwrap().len(), 1);
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(
            migrated
                .enrollment("S")
                .unwrap()
                .expect("active server should be enrolled")
                .auto_refresh_enabled
        );
        assert!(
            !migrated
                .enrollment("Failed")
                .unwrap()
                .expect("failed server should be enrolled")
                .auto_refresh_enabled
        );
        assert_eq!(migrated.scheduled_servers().unwrap(), vec!["S"]);
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries_fts WHERE server = 'S' AND generation = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        drop(migrated);

        let reopened = IndexDb::open_once(&schema2_path).unwrap();
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT value FROM index_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            SCHEMA_VERSION.to_string()
        );
        assert_eq!(
            reopened
                .connection
                .query_row("SELECT COUNT(*) FROM enrolled_servers", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2
        );
        drop(reopened);

        let rejected_metadata_path = directory.path().join("rejected-metadata.sqlite3");
        drop(IndexDb::open(&rejected_metadata_path).unwrap());
        let rejected_metadata = Connection::open(&rejected_metadata_path).unwrap();
        rejected_metadata
            .execute_batch(
                "CREATE TRIGGER reject_index_meta_insert
                 BEFORE INSERT ON index_meta
                 BEGIN
                   SELECT RAISE(FAIL, 'index metadata update rejected');
                 END;",
            )
            .unwrap();
        drop(rejected_metadata);
        assert!(
            IndexDb::open_once(&rejected_metadata_path)
                .err()
                .expect("rejected metadata write should fail")
                .to_string()
                .contains("index metadata update rejected")
        );

        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        let recovered =
            tracing::subscriber::with_default(subscriber, || IndexDb::open(&invalid_path).unwrap());
        assert!(recovered.status_rows("S").unwrap().is_empty());
        assert!(
            directory
                .path()
                .join("invalid")
                .read_dir()
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().contains("quarantine-"))
        );
        drop(recovered);

        let interrupted_path = directory.path().join("interrupted.sqlite3");
        let mut interrupted = IndexDb::open(&interrupted_path).unwrap();
        let active = interrupted
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
            .unwrap();
        interrupted
            .insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
            .unwrap();
        interrupted
            .promote(
                "S",
                active,
                "2",
                &InventoryProgress {
                    entries_seen: 1,
                    unique_items: 1,
                    ..zero_progress()
                },
            )
            .unwrap();
        let generation = interrupted
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
            .unwrap();
        interrupted
            .insert_entries("S", generation, &[inventory_entry("Interrupted", "S.Tag")])
            .unwrap();
        drop(interrupted);

        let reopened = IndexDb::open(&interrupted_path).unwrap();
        let rows = reopened.status_rows("S").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "active");
        assert_eq!(rows[0].generation, active);
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT state FROM generations
                     WHERE server = 'S' AND generation = ?1",
                    [generation as i64],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "superseded"
        );
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT last_error FROM generations
                     WHERE server = 'S' AND generation = ?1",
                    [generation as i64],
                    |row| row.get::<_, Option<String>>(0)
                )
                .unwrap()
                .as_deref(),
            Some("namespace index build interrupted by gateway restart")
        );
        assert_eq!(reopened.search_generation("S").unwrap(), Some(active));
        assert_eq!(
            reopened.search("S", active, "active", 1, 10).unwrap().len(),
            1
        );
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                    [generation as i64],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries_fts WHERE server = 'S' AND generation = ?1",
                    [generation as i64],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );

        let initial_path = directory.path().join("interrupted-initial.sqlite3");
        let mut initial = IndexDb::open(&initial_path).unwrap();
        let initial_generation = initial
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
            .unwrap();
        initial
            .insert_entries(
                "S",
                initial_generation,
                &[inventory_entry("Interrupted", "S.Tag")],
            )
            .unwrap();
        drop(initial);

        let reopened_initial = IndexDb::open(&initial_path).unwrap();
        let initial_rows = reopened_initial.status_rows("S").unwrap();
        assert_eq!(initial_rows.len(), 1);
        assert_eq!(initial_rows[0].state, "failed");
        assert_eq!(
            initial_rows[0].last_error.as_deref(),
            Some("namespace index build interrupted by gateway restart")
        );
    }

    #[test]
    fn sqlite_open_preserves_staging_owned_by_a_live_build_lock() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("live-build.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
            .unwrap();
        db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
            .unwrap();
        db.promote("S", active, "2", &completed_progress(1))
            .unwrap();
        let staging = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "3",
            )
            .unwrap();
        db.insert_entries("S", staging, &[inventory_entry("Staging", "S.Staging")])
            .unwrap();
        drop(db);

        let lock = BuildFileLock::acquire(&path, "S").unwrap();
        let reopened = IndexDb::open(&path).unwrap();
        assert_eq!(
            reopened
                .connection
                .query_row(
                    "SELECT state FROM generations
                     WHERE server = 'S' AND generation = ?1",
                    [staging as i64],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "staging"
        );
        drop(reopened);
        drop(lock);

        let recovered = IndexDb::open(&path).unwrap();
        assert_eq!(
            recovered
                .connection
                .query_row(
                    "SELECT state FROM generations
                     WHERE server = 'S' AND generation = ?1",
                    [staging as i64],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "superseded"
        );
    }

    #[test]
    fn schema_migration_rejects_an_invalid_server_value() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("invalid-server.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );
                 CREATE TABLE generations (
                     server BLOB NOT NULL,
                     state TEXT NOT NULL
                 );
                 INSERT INTO index_meta(key, value) VALUES ('schema_version', '3');
                 INSERT INTO generations(server, state) VALUES (X'00', 'active');",
            )
            .unwrap();
        drop(connection);

        assert!(IndexDb::open_once(&path).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_during_refresh_keeps_active_status_ready() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("restart-status.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                &timestamp_now(),
            )
            .unwrap();
        db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
            .unwrap();
        db.promote("S", active, &timestamp_now(), &completed_progress(1))
            .unwrap();
        let interrupted = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                &timestamp_now(),
            )
            .unwrap();
        db.insert_entries(
            "S",
            interrupted,
            &[inventory_entry("Interrupted", "S.Interrupted")],
        )
        .unwrap();
        drop(db);

        let manager = IndexManager::new(Arc::new(MockOpcClient::default()), settings(path));
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Ready);
        assert_eq!(status.active_generation, active);
        assert!(status.last_error.is_none());
    }

    #[test]
    fn sqlite_migrates_schema_3_and_preserves_indexed_data() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("v3.sqlite3");
        drop(IndexDb::open(&path).unwrap());
        let legacy = Connection::open(&path).unwrap();
        legacy
            .execute_batch(
                "DROP TABLE enrolled_servers;
                 INSERT INTO generations (
                     server, generation, state, organization, source, started_at,
                     completed_at, entry_count, unique_item_count
                 ) VALUES
                     ('Active', 1, 'active', 'hierarchical', 'da2', '1', '2', 1, 1),
                     ('Failed', 1, 'failed', 'flat', 'da2', '3', NULL, 0, 0);
                 INSERT INTO entries (
                     server, generation, item_id, item_id_norm, display_name,
                     display_name_norm, kind, breadcrumbs
                 ) VALUES (
                     'Active', 1, 'Area.Loop.PV', 'area.loop.pv', 'PV',
                     'pv', 1, '[\"Area\",\"Loop\",\"PV\"]'
                 );
                 INSERT INTO entries_fts (
                     server, generation, item_id, display_name, breadcrumbs
                 ) VALUES (
                     'Active', 1, 'Area.Loop.PV', 'PV', 'Area Loop PV'
                 );
                 UPDATE index_meta SET value = '3' WHERE key = 'schema_version';",
            )
            .unwrap();
        drop(legacy);

        let migrated = IndexDb::open(&path).unwrap();
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT value FROM index_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "4"
        );
        assert!(
            migrated
                .enrollment("Active")
                .unwrap()
                .expect("active server should be enrolled")
                .auto_refresh_enabled
        );
        assert!(
            !migrated
                .enrollment("Failed")
                .unwrap()
                .expect("failed server should be enrolled")
                .auto_refresh_enabled
        );
        assert_eq!(migrated.scheduled_servers().unwrap(), vec!["Active"]);
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries
                     WHERE server = 'Active' AND generation = 1",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            migrated
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM entries_fts
                     WHERE server = 'Active' AND generation = 1",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
    #[test]
    fn sqlite_quarantines_inconsistent_full_text_data() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("fts-inconsistent.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
            .unwrap();
        db.promote("S", generation, "2", &completed_progress(1))
            .unwrap();
        db.connection
            .execute("DELETE FROM entries_fts", [])
            .unwrap();
        drop(db);

        let recovered = IndexDb::open(&path).unwrap();
        assert!(recovered.status_rows("S").unwrap().is_empty());
        assert!(
            directory
                .path()
                .read_dir()
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().contains("quarantine-"))
        );
    }

    #[test]
    fn sqlite_validation_failure_discard_and_clear_paths_work() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        assert!(
            db.insert_entries(
                "S",
                generation,
                &[InventoryEntry {
                    display_name: "Invalid".into(),
                    item_id: String::new(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec![],
                }],
            )
            .is_err()
        );
        db.insert_entries("S", generation, &[inventory_entry("Valid", "S.Valid")])
            .unwrap();
        db.update_progress(
            "S",
            generation,
            &InventoryProgress {
                branches_visited: 1,
                entries_seen: 1,
                unique_items: 1,
                active_time_ms: 1,
                paused_time_ms: 0,
                items_per_second: 1.0,
                estimated_remaining_ms: Some(10),
            },
        )
        .unwrap();
        assert_eq!(db.status_rows("S").unwrap()[0].entry_count, 1);
        assert!(
            db.update_progress(
                "S",
                generation,
                &InventoryProgress {
                    unique_items: u64::MAX,
                    ..zero_progress()
                },
            )
            .unwrap_err()
            .to_string()
            .contains("unique item count exceeds SQLite range")
        );

        db.connection
            .execute("UPDATE entries SET kind = 99 WHERE server = 'S'", [])
            .unwrap();
        assert!(db.search("S", generation, "valid", 1, 10).is_err());
        assert!(db.search("S", generation, "valid", 3, 10).is_err());
        db.connection
            .execute(
                "UPDATE entries SET kind = 1, breadcrumbs = 'not-json'
                 WHERE server = 'S'",
                [],
            )
            .unwrap();
        assert!(db.search("S", generation, "valid", 1, 10).is_err());
        assert!(db.search("S", generation, "valid", 3, 10).is_err());
        assert!(
            db.promote("S", generation + 1, "2", &zero_progress())
                .is_err()
        );

        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            db.fail_generation("S", generation, "failed")
        })
        .unwrap();
        let failed = db.status_rows("S").unwrap();
        assert_eq!(failed[0].state, "failed");
        assert_eq!(failed[0].last_error.as_deref(), Some("failed"));

        let replacement = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da3,
                "3",
            )
            .unwrap();
        assert_eq!(db.status_rows("S").unwrap().len(), 2);
        assert!(db.discard_empty_generation("S", replacement).unwrap());
        assert_eq!(db.status_rows("S").unwrap().len(), 1);

        let other = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "4",
            )
            .unwrap();
        db.insert_entries("S", other, &[inventory_entry("Other", "S.Other")])
            .unwrap();
        db.clear_server("S").unwrap();
        assert!(db.status_rows("S").unwrap().is_empty());
        assert_eq!(db.search_generation("S").unwrap(), None);
    }

    #[test]
    fn sqlite_corruption_errors_propagate_from_each_persistence_operation() {
        let directory = tempdir().unwrap();

        let collision_path = directory.path().join("collision.sqlite3");
        let collision = Connection::open(&collision_path).unwrap();
        collision
            .execute_batch(
                "CREATE TABLE seed(value INTEGER);
                 CREATE INDEX index_meta ON seed(value);",
            )
            .unwrap();
        drop(collision);
        assert!(IndexDb::open_once(&collision_path).is_err());

        let malformed_path = directory.path().join("malformed.sqlite3");
        let malformed = Connection::open(&malformed_path).unwrap();
        malformed
            .execute_batch(
                "CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     wrong_column TEXT NOT NULL
                 );",
            )
            .unwrap();
        drop(malformed);
        assert!(IndexDb::open_once(&malformed_path).is_err());

        let schema_path = directory.path().join("schema.sqlite3");
        let schema = Connection::open(&schema_path).unwrap();
        schema
            .execute_batch(
                "CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );
                CREATE TABLE generations (server TEXT NOT NULL);
                CREATE TABLE entries (server TEXT NOT NULL);",
            )
            .unwrap();
        drop(schema);
        assert!(IndexDb::open_once(&schema_path).is_err());

        let cleanup_path = directory.path().join("cleanup.sqlite3");
        let mut cleanup = IndexDb::open(&cleanup_path).unwrap();
        let active = cleanup
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        cleanup
            .promote("S", active, "0", &completed_progress(0))
            .unwrap();
        let cleanup_generation = cleanup
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        cleanup
            .insert_entries(
                "S",
                cleanup_generation,
                &[inventory_entry("Cleanup", "S.Cleanup")],
            )
            .unwrap();
        cleanup
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_cleanup
                 BEFORE DELETE ON entries
                 BEGIN
                   SELECT RAISE(FAIL, 'cleanup failed');
                 END;",
            )
            .unwrap();
        cleanup
            .fail_generation("S", cleanup_generation, "failed")
            .unwrap();
        drop(cleanup);
        assert!(
            cleanup_obsolete_generations(&cleanup_path, "S", &BackgroundTasks::new(),).is_err()
        );

        let rebuild_path = directory.path().join("rebuild.sqlite3");
        let mut rebuild = IndexDb::open(&rebuild_path).unwrap();
        let rebuild_generation = rebuild
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        rebuild
            .insert_entries(
                "S",
                rebuild_generation,
                &[inventory_entry("Rebuild", "S.Rebuild")],
            )
            .unwrap();
        rebuild
            .promote("S", rebuild_generation, "2", &zero_progress())
            .unwrap();
        drop_table(&mut rebuild, "entries_fts");
        rebuild
            .connection
            .execute_batch(
                "CREATE TABLE entries_fts (
                     server TEXT,
                     generation INTEGER CHECK (generation < 0),
                     item_id TEXT,
                     display_name TEXT,
                     breadcrumbs TEXT
                 );",
            )
            .unwrap();
        drop(rebuild);
        assert!(IndexDb::open_once(&rebuild_path).is_err());

        let mut start_db = IndexDb::open(&directory.path().join("start.sqlite3")).unwrap();
        drop_table(&mut start_db, "generations");
        assert!(
            start_db
                .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1",)
                .is_err()
        );

        let mut insert_db = IndexDb::open(&directory.path().join("insert.sqlite3")).unwrap();
        let generation = insert_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        drop_table(&mut insert_db, "entries");
        assert!(
            insert_db
                .insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
                .is_err()
        );

        let mut fts_db = IndexDb::open(&directory.path().join("fts.sqlite3")).unwrap();
        let generation = fts_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        drop_table(&mut fts_db, "entries_fts");
        assert!(
            fts_db
                .insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
                .is_err()
        );

        let mut progress_db = IndexDb::open(&directory.path().join("progress.sqlite3")).unwrap();
        drop_table(&mut progress_db, "generations");
        assert!(
            progress_db
                .update_progress("S", 1, &zero_progress())
                .is_err()
        );

        let mut promote_db = IndexDb::open(&directory.path().join("promote.sqlite3")).unwrap();
        let generation = promote_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        drop_table(&mut promote_db, "entries");
        assert!(
            promote_db
                .promote("S", generation, "2", &zero_progress())
                .is_ok()
        );

        let mut fail_db = IndexDb::open(&directory.path().join("fail.sqlite3")).unwrap();
        drop_table(&mut fail_db, "generations");
        assert!(fail_db.fail_generation("S", 1, "failed").is_err());

        let mut discard_db = IndexDb::open(&directory.path().join("discard.sqlite3")).unwrap();
        drop_table(&mut discard_db, "entries_fts");
        assert!(discard_db.discard_empty_generation("S", 1).is_err());

        let mut clear_db = IndexDb::open(&directory.path().join("clear.sqlite3")).unwrap();
        drop_table(&mut clear_db, "entries_fts");
        assert!(clear_db.clear_server("S").is_err());

        let mut status_db = IndexDb::open(&directory.path().join("status.sqlite3")).unwrap();
        drop_table(&mut status_db, "generations");
        assert!(status_db.status_rows("S").is_err());
        assert!(status_db.search_generation("S").is_err());

        let mut search_db = IndexDb::open(&directory.path().join("search.sqlite3")).unwrap();
        drop_table(&mut search_db, "entries");
        assert!(search_db.search("S", 1, "tag", 1, 10).is_err());
    }

    #[test]
    fn promotion_uses_inventory_metadata_without_scanning_entries() {
        let directory = tempdir().unwrap();
        let mut db = IndexDb::open(&directory.path().join("duplicates.sqlite3")).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        drop_table(&mut db, "entries");
        db.connection
            .execute_batch(
                "CREATE TABLE entries (
                     server TEXT NOT NULL,
                     generation INTEGER NOT NULL,
                     item_id TEXT NOT NULL,
                     item_id_norm TEXT NOT NULL,
                     display_name TEXT NOT NULL,
                     display_name_norm TEXT NOT NULL,
                     kind INTEGER NOT NULL,
                     breadcrumbs TEXT NOT NULL
                 );
                 INSERT INTO entries VALUES
                   ('S', 1, 'duplicate', 'duplicate', 'One', 'one', 1, '[]'),
                   ('S', 1, 'duplicate', 'duplicate', 'Two', 'two', 1, '[]');",
            )
            .unwrap();
        let progress = InventoryProgress {
            entries_seen: 2,
            unique_items: 1,
            ..zero_progress()
        };
        db.promote("S", generation, "2", &progress).unwrap();
        let row = db.status_rows("S").unwrap().remove(0);
        assert_eq!(row.state, "active");
        assert_eq!(row.entry_count, 1);
        assert_eq!(row.unique_item_count, 1);
    }

    #[tokio::test]
    async fn completed_inventory_profile_replaces_startup_capabilities() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
                    Ok(InventoryEvent::Progress(InventoryProgress {
                        branches_visited: 2,
                        entries_seen: 3,
                        unique_items: 1,
                        active_time_ms: 1,
                        paused_time_ms: 0,
                        items_per_second: 1.0,
                        estimated_remaining_ms: None,
                    })),
                    Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    })),
                ]),
                Arc::new(RecordingInventoryControl::default()),
            ))],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da3,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 100,
            })],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("effective-profile.sqlite3")),
        ));

        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Ready).await;
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.organization, NamespaceOrganization::Hierarchical);
        assert_eq!(status.source, BrowseSource::Da2);
        assert_eq!(status.entry_count, 1);
        assert_eq!(status.unique_item_count, 1);
    }

    #[test]
    fn failed_activation_keeps_the_previous_generation_active() {
        let directory = tempdir().unwrap();
        let mut db = IndexDb::open(&directory.path().join("activation.sqlite3")).unwrap();
        let previous = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.insert_entries("S", previous, &[inventory_entry("Previous", "S.Previous")])
            .unwrap();
        db.promote("S", previous, "2", &completed_progress(1))
            .unwrap();

        let target = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
            .unwrap();
        db.insert_entries("S", target, &[inventory_entry("Target", "S.Target")])
            .unwrap();
        db.connection
            .execute_batch(
                "CREATE TRIGGER reject_target_activation
                 BEFORE UPDATE OF state ON generations
                 WHEN NEW.generation = 2 AND NEW.state = 'active'
                 BEGIN
                   SELECT RAISE(FAIL, 'target activation rejected');
                 END;",
            )
            .unwrap();

        assert!(
            db.promote("S", target, "4", &completed_progress(1))
                .unwrap_err()
                .to_string()
                .contains("target activation rejected")
        );
        let rows = db.status_rows("S").unwrap();
        assert_eq!(rows[0].state, "active");
        assert_eq!(rows[0].generation, previous);
        assert_eq!(rows[1].state, "staging");
        assert_eq!(rows[1].generation, target);
        assert_eq!(
            db.search("S", previous, "previous", 1, 10).unwrap().len(),
            1
        );
    }

    #[test]
    fn activation_defers_superseded_data_to_bounded_cleanup() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("deferred-cleanup.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let previous = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        let obsolete_entries = synthetic_entries("Obsolete", CLEANUP_BATCH_SIZE + 1);
        db.insert_entries("S", previous, &obsolete_entries).unwrap();
        db.promote(
            "S",
            previous,
            "2",
            &completed_progress(obsolete_entries.len() as u64),
        )
        .unwrap();

        let active = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
            .unwrap();
        db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
            .unwrap();
        db.promote("S", active, "4", &completed_progress(1))
            .unwrap();
        assert_eq!(
            db.connection
                .query_row(
                    "SELECT state FROM generations WHERE server = 'S' AND generation = ?1",
                    [previous as i64],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "superseded"
        );
        assert_eq!(
            db.connection
                .query_row(
                    "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                    [previous as i64],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            (CLEANUP_BATCH_SIZE + 1) as i64
        );

        let stats = cleanup_obsolete_generations(&path, "S", &BackgroundTasks::new()).unwrap();
        assert!(stats.batches >= 2);
        assert_eq!(stats.entries, (CLEANUP_BATCH_SIZE + 1) as u64);
        assert_eq!(stats.fts_entries, (CLEANUP_BATCH_SIZE + 1) as u64);
        assert_eq!(stats.generations, 1);
        assert_eq!(db.status_rows("S").unwrap().len(), 1);
        assert_eq!(db.search_generation("S").unwrap(), Some(active));
        assert_eq!(db.search("S", active, "active", 1, 10).unwrap().len(), 1);
    }

    #[test]
    fn cleanup_precheck_avoids_a_write_when_no_obsolete_generation_exists() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-precheck.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.promote("S", generation, "2", &zero_progress()).unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let started = Instant::now();
        let stats = cleanup_obsolete_generations(&path, "S", &BackgroundTasks::new()).unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(stats.batches, 0);
        drop(blocker);
    }

    #[test]
    fn cleanup_stops_before_writing_when_shutdown_is_requested() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-shutdown.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        db.promote("S", active, "0", &completed_progress(0))
            .unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.fail_generation("S", generation, "obsolete").unwrap();
        drop(db);

        let background_tasks = BackgroundTasks::new();
        background_tasks.request_shutdown();
        let stats = cleanup_obsolete_generations(&path, "S", &background_tasks).unwrap();

        assert!(stats.stopped_for_shutdown);
        assert_eq!(stats.batches, 0);
        assert_eq!(
            IndexDb::open(&path)
                .unwrap()
                .status_rows("S")
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn cleanup_stops_when_obsolete_data_disappears_before_batch_delete() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-no-progress.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        db.promote("S", active, "0", &completed_progress(0))
            .unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.fail_generation("S", generation, "obsolete").unwrap();
        drop(db);

        let background_tasks = Arc::new(BackgroundTasks::new());
        let (started, release) = background_tasks.install_cleanup_batch_hook();
        let cleanup_path = path.clone();
        let cleanup_tasks = Arc::clone(&background_tasks);
        let cleanup = std::thread::spawn(move || {
            cleanup_obsolete_generations(&cleanup_path, "S", cleanup_tasks.as_ref())
        });

        started.recv().unwrap();
        let remover = Connection::open(&path).unwrap();
        remover
            .execute("DELETE FROM generations WHERE server = 'S'", [])
            .unwrap();
        drop(remover);
        release.send(()).unwrap();

        let stats = cleanup.join().unwrap().unwrap();
        assert_eq!(stats.batches, 1);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.fts_entries, 0);
        assert_eq!(stats.generations, 0);
        assert!(
            IndexDb::open(&path)
                .unwrap()
                .status_rows("S")
                .unwrap()
                .is_empty()
        );
        background_tasks.wait_for_cleanup_batch_hook();
    }

    #[test]
    fn cleanup_checkpoint_defers_for_builds_and_tolerates_poisoned_locks() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-checkpoint.sqlite3");
        let connection = Connection::open(&path).unwrap();
        let writer_gate = Mutex::new(());
        let active_builds = Mutex::new(HashSet::new());

        assert!(cleanup_checkpoint(&connection, &writer_gate, &active_builds, &path, "S",).is_ok());

        active_builds.lock().unwrap().insert("S".into());
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            assert!(
                cleanup_checkpoint(&connection, &writer_gate, &active_builds, &path, "S",).is_err()
            );
        });
        active_builds.lock().unwrap().clear();

        let poisoned_builds = Arc::new(Mutex::new(HashSet::new()));
        let poison_builds = Arc::clone(&poisoned_builds);
        std::thread::spawn(move || {
            let _guard = poison_builds.lock().unwrap();
            panic!("poison cleanup checkpoint active-build lock");
        })
        .join()
        .unwrap_err();
        assert!(
            cleanup_checkpoint(
                &connection,
                &writer_gate,
                poisoned_builds.as_ref(),
                &path,
                "S",
            )
            .is_err()
        );

        let poisoned_gate = Arc::new(Mutex::new(()));
        let poison_gate = Arc::clone(&poisoned_gate);
        std::thread::spawn(move || {
            let _guard = poison_gate.lock().unwrap();
            panic!("poison cleanup checkpoint writer lock");
        })
        .join()
        .unwrap_err();
        assert!(
            cleanup_checkpoint(
                &connection,
                poisoned_gate.as_ref(),
                &active_builds,
                &path,
                "S",
            )
            .is_err()
        );
    }

    #[test]
    fn cleanup_rechecks_build_state_after_waiting_for_the_writer_gate() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-gate-recheck.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        db.promote("S", active, "0", &completed_progress(0))
            .unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.fail_generation("S", generation, "obsolete").unwrap();
        drop(db);

        let background_tasks = Arc::new(BackgroundTasks::new());
        let (started, release) = background_tasks.install_cleanup_writer_gate_hook();
        let writer_gate = Arc::new(Mutex::new(()));
        let active_builds = Arc::new(Mutex::new(HashSet::new()));
        let cleanup_path = path.clone();
        let cleanup_tasks = Arc::clone(&background_tasks);
        let cleanup_writer_gate = Arc::clone(&writer_gate);
        let cleanup_active_builds = Arc::clone(&active_builds);
        let cleanup = std::thread::spawn(move || {
            let subscriber = tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::INFO)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                cleanup_obsolete_generations_coordinated(
                    &cleanup_path,
                    "S",
                    cleanup_tasks.as_ref(),
                    cleanup_writer_gate,
                    cleanup_active_builds,
                )
            })
        });

        started.recv().unwrap();
        active_builds.lock().unwrap().insert("T".into());
        release.send(()).unwrap();
        let stats = cleanup.join().unwrap().unwrap();
        assert_eq!(stats.batches, 0);
        assert!(stats.deferred_for_build);
        assert_eq!(
            IndexDb::open(&path)
                .unwrap()
                .status_rows("S")
                .unwrap()
                .len(),
            2
        );
        background_tasks.wait_for_cleanup_writer_gate_hook();
    }

    #[test]
    fn cleanup_rechecks_obsolete_data_after_waiting_for_the_writer_gate() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-obsolete-recheck.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let active = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        db.promote("S", active, "0", &completed_progress(0))
            .unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.fail_generation("S", generation, "obsolete").unwrap();
        drop(db);

        let background_tasks = Arc::new(BackgroundTasks::new());
        let (started, release) = background_tasks.install_cleanup_writer_gate_hook();
        let writer_gate = Arc::new(Mutex::new(()));
        let active_builds = Arc::new(Mutex::new(HashSet::new()));
        let cleanup_path = path.clone();
        let cleanup_tasks = Arc::clone(&background_tasks);
        let cleanup_writer_gate = Arc::clone(&writer_gate);
        let cleanup_active_builds = Arc::clone(&active_builds);
        let cleanup = std::thread::spawn(move || {
            cleanup_obsolete_generations_coordinated(
                &cleanup_path,
                "S",
                cleanup_tasks.as_ref(),
                cleanup_writer_gate,
                cleanup_active_builds,
            )
        });

        started.recv().unwrap();
        let remover = Connection::open(&path).unwrap();
        remover
            .execute("DELETE FROM generations WHERE server = 'S'", [])
            .unwrap();
        drop(remover);
        release.send(()).unwrap();
        let stats = cleanup.join().unwrap().unwrap();
        assert_eq!(stats.batches, 0);
        assert!(!stats.deferred_for_build);
        assert!(
            IndexDb::open(&path)
                .unwrap()
                .status_rows("S")
                .unwrap()
                .is_empty()
        );
        background_tasks.wait_for_cleanup_writer_gate_hook();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_first_build_waits_for_the_shared_writer_gate() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-build-gate.sqlite3");
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(client, settings(path.clone())));
        manager
            .with_database(|db| {
                let active =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", active, "0", &completed_progress(0))?;
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.fail_generation("S", generation, "obsolete")?;
                Ok(())
            })
            .unwrap();
        let (cleanup_started, cleanup_release) =
            manager.background_tasks.install_cleanup_batch_hook();
        manager.schedule_cleanup("S");
        tokio::task::spawn_blocking(move || cleanup_started.recv().unwrap())
            .await
            .unwrap();

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!refresh.is_finished());

        cleanup_release.send(()).unwrap();
        refresh.await.unwrap().unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        manager.background_tasks.wait_for_cleanup_batch_hook();
        assert_eq!(manager.status("S").await.unwrap().active_generation, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_and_build_share_the_writer_gate_across_servers() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cross-server-gate.sqlite3");
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(client, settings(path.clone())));
        manager
            .with_database(|db| {
                let active =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", active, "0", &completed_progress(0))?;
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.fail_generation("S", generation, "obsolete")?;
                Ok(())
            })
            .unwrap();
        let (cleanup_started, cleanup_release) =
            manager.background_tasks.install_cleanup_batch_hook();
        manager.schedule_cleanup("S");
        tokio::task::spawn_blocking(move || cleanup_started.recv().unwrap())
            .await
            .unwrap();

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("T", true).await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!refresh.is_finished());
        cleanup_release.send(()).unwrap();
        refresh.await.unwrap().unwrap();
        wait_for_state(&manager, "T", IndexState::Ready).await;
    }

    #[test]
    fn build_reservation_hook_blocks_once_and_then_becomes_inert() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("build-reservation-hook.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let (started, release) = manager.install_build_reservation_hook();
        let waiter = Arc::clone(&manager);
        let wait = std::thread::spawn(move || waiter.wait_for_build_reservation_hook());
        started.recv().unwrap();
        release.send(()).unwrap();
        wait.join().unwrap();
        manager.wait_for_build_reservation_hook();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_rejects_a_build_owner_registered_during_reservation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("build-owner-race.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let (started, release) = manager.install_build_reservation_hook();
        let hook_manager = Arc::clone(&manager);
        let hook = std::thread::spawn(move || {
            started.recv().unwrap();
            hook_manager
                .coordination
                .build_owners
                .lock()
                .unwrap()
                .insert("S".into(), Arc::new(()));
            release.send(()).unwrap();
        });

        let error = manager.refresh("S", true).await.unwrap_err();
        hook.join().unwrap();
        assert!(
            error
                .to_string()
                .contains("build owner is already registered in this process")
        );
        assert!(manager.active_builds.lock().unwrap().is_empty());
        assert!(manager.build_locks.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_rechecks_the_concurrency_limit_after_reservation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("build-concurrency-race.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let (started, release) = manager.install_build_reservation_hook();
        let hook_manager = Arc::clone(&manager);
        let hook = std::thread::spawn(move || {
            started.recv().unwrap();
            hook_manager
                .active_builds
                .lock()
                .unwrap()
                .insert("T".into());
            release.send(()).unwrap();
        });

        let error = manager.refresh("S", true).await.unwrap_err();
        hook.join().unwrap();
        assert!(
            error
                .to_string()
                .contains("namespace index build concurrency limit reached")
        );
        assert!(manager.build_locks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cleanup_stays_pending_while_any_build_is_active_and_resumes_after_termination() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-deferred.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let (obsolete, active) = manager
            .with_database(|db| {
                let current =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", current, "0", &completed_progress(0))?;
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.fail_generation("S", obsolete, "failed")?;
                let active =
                    db.start_generation("T", NamespaceOrganization::Flat, BrowseSource::Flat, "2")?;
                db.insert_entries("T", active, &[inventory_entry("Active", "T.Active")])?;
                db.promote("T", active, "3", &completed_progress(1))?;
                Ok((obsolete, active))
            })
            .unwrap();
        let (hook_started, hook_release) =
            manager.background_tasks.install_cleanup_notification_hook();
        manager.active_builds.lock().unwrap().insert("T".into());
        manager.schedule_cleanup("S");
        hook_started.notified().await;
        assert!(
            manager
                .cleanup_tasks
                .lock()
                .unwrap()
                .get("S")
                .is_some_and(|task| task.requested && task.running)
        );

        manager.active_builds.lock().unwrap().remove("T");
        manager.coordination.build_changed.notify_waiters();
        hook_release.notify_one();
        manager.background_tasks.wait_for_idle().await;
        assert_eq!(
            manager
                .with_database(|db| {
                    db.connection
                        .query_row(
                            "SELECT COUNT(*) FROM generations
                             WHERE server = 'S' AND generation = ?1",
                            [obsolete as i64],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(Into::into)
                })
                .unwrap(),
            0
        );
        assert_eq!(manager.status("T").await.unwrap().active_generation, active);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_on_one_manager_resumes_after_a_build_on_another_manager_finishes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cross-manager-cleanup.sqlite3");
        let manager_a = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path.clone()),
        ));
        let manager_b = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let obsolete = manager_a
            .with_database(|db| {
                let current =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", current, "0", &completed_progress(0))?;
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.fail_generation("S", obsolete, "failed")?;
                Ok(obsolete)
            })
            .unwrap();
        manager_a.active_builds.lock().unwrap().insert("T".into());
        let (hook_started, hook_release) = manager_b
            .background_tasks
            .install_cleanup_notification_hook();

        manager_b.schedule_cleanup("S");
        hook_started.notified().await;
        assert!(
            manager_b
                .cleanup_tasks
                .lock()
                .unwrap()
                .get("S")
                .is_some_and(|task| task.requested && task.running)
        );

        manager_a.active_builds.lock().unwrap().remove("T");
        manager_a.coordination.build_changed.notify_waiters();
        hook_release.notify_one();
        tokio::time::timeout(
            Duration::from_secs(2),
            manager_b.background_tasks.wait_for_idle(),
        )
        .await
        .expect("cross-manager cleanup did not resume after build completion");

        assert_eq!(
            manager_b
                .with_database(|db| {
                    db.connection
                        .query_row(
                            "SELECT COUNT(*) FROM generations
                             WHERE server = 'S' AND generation = ?1",
                            [obsolete as i64],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(Into::into)
                })
                .unwrap(),
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deferred_cleanup_exits_when_shutdown_precedes_notification_subscription() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-shutdown-race.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        manager
            .with_database(|db| {
                let current =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", current, "0", &completed_progress(0))?;
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.fail_generation("S", obsolete, "failed")?;
                Ok(())
            })
            .unwrap();
        manager.active_builds.lock().unwrap().insert("T".into());
        let (hook_started, hook_release) =
            manager.background_tasks.install_cleanup_notification_hook();

        manager.schedule_cleanup("S");
        hook_started.notified().await;
        manager.background_tasks.request_shutdown();
        hook_release.notify_one();

        tokio::time::timeout(
            Duration::from_secs(2),
            manager.background_tasks.wait_for_idle(),
        )
        .await
        .expect("deferred cleanup did not stop after shutdown");
        assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_notification_registration_closes_the_lost_wakeup_window() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-notification-race.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        let obsolete = manager
            .with_database(|db| {
                let current =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", current, "0", &completed_progress(0))?;
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.fail_generation("S", obsolete, "failed")?;
                Ok(obsolete)
            })
            .unwrap();
        let (hook_started, hook_release) =
            manager.background_tasks.install_cleanup_notification_hook();
        let writer_guard = manager.coordination.writer_gate.lock().unwrap();
        manager.schedule_cleanup("S");
        manager.active_builds.lock().unwrap().insert("T".into());
        drop(writer_guard);
        hook_started.notified().await;

        manager.active_builds.lock().unwrap().remove("T");
        manager.coordination.build_changed.notify_waiters();
        hook_release.notify_one();

        tokio::time::timeout(
            Duration::from_secs(2),
            manager.background_tasks.wait_for_idle(),
        )
        .await
        .expect("cleanup worker missed the build-completion notification");
        assert_eq!(
            manager
                .with_database(|db| {
                    db.connection
                        .query_row(
                            "SELECT COUNT(*) FROM generations
                             WHERE server = 'S' AND generation = ?1",
                            [obsolete as i64],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(Into::into)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn cleanup_uses_a_separate_connection_without_blocking_primary_reads() {
        use std::thread;

        let directory = tempdir().unwrap();
        let path = directory.path().join("concurrent-cleanup.sqlite3");
        let mut primary = IndexDb::open(&path).unwrap();
        let obsolete = primary
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        let obsolete_entries = synthetic_entries("Obsolete", CLEANUP_BATCH_SIZE + 1);
        primary
            .insert_entries("S", obsolete, &obsolete_entries)
            .unwrap();
        primary
            .promote(
                "S",
                obsolete,
                "2",
                &completed_progress(obsolete_entries.len() as u64),
            )
            .unwrap();
        let active = primary
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
            .unwrap();
        primary
            .insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
            .unwrap();
        primary
            .promote("S", active, "4", &completed_progress(1))
            .unwrap();

        primary.connection.execute_batch("BEGIN").unwrap();
        assert_eq!(
            primary
                .connection
                .query_row("SELECT COUNT(*) FROM entries", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            (CLEANUP_BATCH_SIZE + 2) as i64
        );
        let cleanup_path = path.clone();
        let background_tasks = Arc::new(BackgroundTasks::new());
        let cleanup_tasks = Arc::clone(&background_tasks);
        let cleanup = thread::spawn(move || {
            cleanup_obsolete_generations(&cleanup_path, "S", cleanup_tasks.as_ref())
        });

        for _ in 0..100 {
            assert_eq!(primary.search_generation("S").unwrap(), Some(active));
            assert_eq!(
                primary.search("S", active, "active", 1, 10).unwrap().len(),
                1
            );
            thread::yield_now();
        }
        let stats = cleanup.join().unwrap().unwrap();
        primary.connection.execute_batch("COMMIT").unwrap();
        assert_eq!(stats.entries, (CLEANUP_BATCH_SIZE + 1) as u64);
        assert_eq!(primary.search_generation("S").unwrap(), Some(active));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_errors_do_not_change_a_successfully_activated_generation() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("cleanup-error.sqlite3")),
        ));
        let active = manager
            .with_database(|db| {
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.promote("S", obsolete, "2", &completed_progress(1))?;
                let active =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")?;
                db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])?;
                db.promote("S", active, "4", &completed_progress(1))?;
                db.connection
                    .execute_batch(
                        "CREATE TRIGGER fail_obsolete_cleanup
                     BEFORE DELETE ON entries
                     WHEN OLD.generation = 1
                     BEGIN
                       SELECT RAISE(FAIL, 'obsolete cleanup rejected');
                     END;",
                    )
                    .unwrap();
                Ok(active)
            })
            .unwrap();
        manager.schedule_cleanup("S");
        manager.background_tasks.wait_for_idle().await;

        let status = manager.status("S").await.unwrap();
        assert!(matches!(
            status.state,
            IndexState::Ready | IndexState::Stale
        ));
        assert_eq!(status.active_generation, active);
        assert_eq!(
            manager
                .search("S", "active", 1, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_retries_transient_failures() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("cleanup-retry.sqlite3")),
        ));
        let (obsolete, active) = manager
            .with_database(|db| {
                let obsolete =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
                db.promote("S", obsolete, "2", &completed_progress(1))?;
                let active =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")?;
                db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])?;
                db.promote("S", active, "4", &completed_progress(1))?;
                db.connection
                    .execute_batch(
                        "CREATE TRIGGER fail_obsolete_cleanup_once
                     BEFORE DELETE ON entries
                     WHEN OLD.generation = 1
                     BEGIN
                       SELECT RAISE(FAIL, 'transient obsolete cleanup rejection');
                     END;",
                    )
                    .unwrap();
                Ok((obsolete, active))
            })
            .unwrap();
        manager.schedule_cleanup("S");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let failed_once = manager
                    .cleanup_tasks
                    .lock()
                    .ok()
                    .and_then(|tasks| tasks.get("S").map(|task| task.failures > 0))
                    .unwrap_or(false);
                if failed_once {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        manager
            .with_database(|db| {
                db.connection
                    .execute_batch("DROP TRIGGER fail_obsolete_cleanup_once;")?;
                Ok(())
            })
            .unwrap();
        manager.background_tasks.wait_for_idle().await;

        assert_eq!(
            manager
                .with_database(|db| {
                    db.connection
                        .query_row(
                            "SELECT COUNT(*) FROM generations
                         WHERE server = 'S' AND generation = ?1",
                            [i64::try_from(obsolete)?],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(Into::into)
                })
                .unwrap(),
            0
        );
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.active_generation, active);
        assert!(matches!(
            status.state,
            IndexState::Ready | IndexState::Stale
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_cleanup_stops_cleanly_during_shutdown() {
        let directory = tempdir().unwrap();
        let background_tasks = Arc::new(BackgroundTasks::new());
        let cleanup_tasks = Arc::new(Mutex::new(HashMap::new()));
        background_tasks.request_shutdown();

        run_scheduled_cleanup(
            directory.path().join("shutdown-cleanup.sqlite3"),
            "S".into(),
            Arc::clone(&background_tasks),
            Arc::clone(&cleanup_tasks),
            Arc::new(DatabaseCoordination {
                writer_gate: Arc::new(Mutex::new(())),
                active_builds: Arc::new(Mutex::new(HashSet::new())),
                build_owners: Arc::new(Mutex::new(HashMap::new())),
                build_changed: Arc::new(tokio::sync::Notify::new()),
            }),
        )
        .await;

        assert!(cleanup_tasks.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_cleanup_retries_after_worker_panic() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("cleanup-panic.sqlite3")),
        ));
        manager.background_tasks.panic_next_cleanup_worker();
        manager.schedule_cleanup("S");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let failed_once = manager
                    .cleanup_tasks
                    .lock()
                    .ok()
                    .and_then(|tasks| tasks.get("S").map(|task| task.failures > 0))
                    .unwrap_or(false);
                if failed_once {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        manager.background_tasks.wait_for_idle().await;

        assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn older_failed_generations_do_not_poison_active_status() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("older-failed.sqlite3")),
        ));
        let active = manager
            .with_database(|db| {
                let failed =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", failed, &[inventory_entry("Failed", "S.Failed")])?;
                db.fail_generation("S", failed, "old refresh failed")?;
                let active =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "2")?;
                db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])?;
                db.promote("S", active, &timestamp_now(), &completed_progress(1))?;
                Ok(active)
            })
            .unwrap();

        let status = manager.status("S").await.unwrap();
        assert_eq!(status.active_generation, active);
        assert!(matches!(
            status.state,
            IndexState::Ready | IndexState::Stale
        ));
        assert_eq!(status.last_error, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abandoning_a_first_generation_preserves_its_failure_for_manual_retry() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("abandon.sqlite3")),
        ));
        let generation = manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                let entries = synthetic_entries("Abandoned", CLEANUP_BATCH_SIZE + 1);
                db.insert_entries("S", generation, &entries)?;
                Ok(generation)
            })
            .unwrap();

        manager
            .with_database(|db| {
                db.connection.execute_batch("BEGIN IMMEDIATE")?;
                Ok(())
            })
            .unwrap();
        manager.abandon_generation("S", generation, "inventory cancelled");
        let failed = manager.with_database(|db| db.status_rows("S")).unwrap();
        assert_eq!(failed[0].state, "failed");
        assert_eq!(
            manager
                .with_database(|db| {
                    db.connection
                        .query_row(
                            "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                            [generation as i64],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(Into::into)
                })
                .unwrap(),
            (CLEANUP_BATCH_SIZE + 1) as i64
        );
        manager
            .with_database(|db| {
                db.connection.execute_batch("COMMIT")?;
                Ok(())
            })
            .unwrap();
        manager.background_tasks.wait_for_idle().await;
        assert_eq!(
            manager.with_database(|db| db.status_rows("S")).unwrap()[0].state,
            "failed"
        );
    }

    #[cfg(not(coverage))]
    #[test]
    #[ignore = "production-scale regression: one million rows exercises activation without scans or cleanup"]
    fn large_synthetic_generation_promotes_without_a_validation_scan() {
        let directory = tempdir().unwrap();
        let mut db = IndexDb::open(&directory.path().join("large-generation.sqlite3")).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        const STRESS_ROWS: usize = 1_000_000;
        for offset in (0..STRESS_ROWS).step_by(1_000) {
            let entries = synthetic_entries("Stress", 1_000)
                .into_iter()
                .enumerate()
                .map(|(index, mut entry)| {
                    let sequence = offset + index;
                    entry.display_name = format!("Stress-{sequence}");
                    entry.item_id = format!("Stress.{sequence}");
                    entry
                })
                .collect::<Vec<_>>();
            db.insert_entries("S", generation, &entries).unwrap();
        }
        let promotion_started = Instant::now();
        db.promote(
            "S",
            generation,
            "2",
            &completed_progress(STRESS_ROWS as u64),
        )
        .unwrap();
        assert_eq!(
            db.status_rows("S").unwrap()[0].entry_count,
            STRESS_ROWS as u64
        );
        assert!(
            promotion_started.elapsed() < Duration::from_secs(5),
            "activation should only update generation metadata"
        );
    }

    #[tokio::test]
    async fn background_refresh_delay_uses_persisted_state() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        assert_eq!(
            manager.background_refresh_delay("S").await,
            Duration::from_secs(3600)
        );

        manager
            .with_database(|db| {
                let generation = db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    &timestamp_now(),
                )?;
                db.promote(
                    "S",
                    generation,
                    &timestamp_now(),
                    &InventoryProgress {
                        branches_visited: 0,
                        entries_seen: 0,
                        unique_items: 0,
                        active_time_ms: 0,
                        paused_time_ms: 0,
                        items_per_second: 0.0,
                        estimated_remaining_ms: None,
                    },
                )
            })
            .unwrap();
        let ready_delay = manager.background_refresh_delay("S").await;
        assert!(ready_delay <= Duration::from_secs(604_800));

        manager.deleting.lock().unwrap().insert("S".into());
        assert_eq!(
            manager.background_refresh_delay("S").await,
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn background_indexing_respects_disabled_paused_and_idempotent_start() {
        let directory = tempdir().unwrap();
        let mut disabled = settings(directory.path().join("disabled.sqlite3"));
        disabled.enabled = false;
        let disabled = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            disabled,
        ));
        disabled.start_background_indexing();
        assert!(!disabled.background_started.load(Ordering::Acquire));

        let mut paused = settings(directory.path().join("paused.sqlite3"));
        paused.paused = true;
        let paused = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            paused,
        ));
        paused.start_background_indexing();
        assert!(!paused.background_started.load(Ordering::Acquire));

        let enabled = settings(directory.path().join("enabled.sqlite3"));
        let enabled = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            enabled,
        ));
        enabled.start_background_indexing();
        enabled.start_background_indexing();
        assert!(enabled.background_started.load(Ordering::Acquire));
        enabled.shutdown_background_indexing().await;
    }

    #[tokio::test]
    async fn background_indexing_never_starts_an_unenrolled_server() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.refresh_interval_seconds = 1;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.start_background_indexing();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn background_indexing_does_not_start_first_build() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let config = settings(directory.path().join("index.sqlite3"));
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.start_background_indexing();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
        manager.shutdown_background_indexing().await;
    }

    #[tokio::test]
    async fn background_indexing_continues_after_a_scheduled_server_query_failure() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("scheduled-query-failure.sqlite3")),
        ));
        manager
            .with_database(|db| {
                drop_table(db, "enrolled_servers");
                Ok(())
            })
            .unwrap();

        manager.start_background_indexing();
        tokio::task::yield_now().await;
        manager.shutdown_background_indexing().await;
    }

    #[tokio::test]
    async fn background_indexing_wakes_for_the_next_refresh_check() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.refresh_interval_seconds = 1;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));
        let future = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &future,
        );

        manager.start_background_indexing();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn background_delay_and_refresh_handle_partial_status_errors_and_unconfigured_servers() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        assert_eq!(
            manager.background_refresh_delay("S").await,
            Duration::from_secs(3600)
        );
        manager.refresh_if_due("S").await;

        manager.runtime.lock().unwrap().clear();
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: "2".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        assert_eq!(
            manager.status("S").await.unwrap().state,
            IndexState::Refreshing
        );
        manager.refresh_if_due("S").await;

        manager.runtime.lock().unwrap().clear();
        manager
            .with_database(|db| {
                drop_table(db, "generations");
                Ok(())
            })
            .unwrap();
        assert_eq!(
            manager.background_refresh_delay("S").await,
            retry_delay("S", 1, false, 300)
        );
        manager.refresh_if_due("S").await;

        manager.refresh_if_due("Other").await;
    }

    #[tokio::test]
    async fn manager_status_covers_partial_stale_refreshing_and_runtime_errors() {
        let directory = tempdir().unwrap();
        let mut index_settings = settings(directory.path().join("index.sqlite3"));
        index_settings.sentinel_tag = Some("Health.PV".into());
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            index_settings,
        ));
        let not_indexed = manager.status("S").await.unwrap();
        assert_eq!(not_indexed.state, IndexState::NotIndexed);
        assert!(not_indexed.sentinel_configured);
        assert_eq!(not_indexed.health, HealthProbeState::Unavailable);

        let generation = manager
            .with_database(|db| {
                let generation = db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )?;
                db.update_progress(
                    "S",
                    generation,
                    &InventoryProgress {
                        branches_visited: 2,
                        entries_seen: 3,
                        unique_items: 2,
                        active_time_ms: 4,
                        paused_time_ms: 5,
                        items_per_second: 6.0,
                        estimated_remaining_ms: Some(7),
                    },
                )?;
                Ok(generation)
            })
            .unwrap();
        let partial = manager.status("S").await.unwrap();
        assert_eq!(partial.state, IndexState::Partial);
        assert_eq!(partial.entry_count, 3);

        manager
            .with_database(|db| {
                db.insert_entries("S", generation, &[inventory_entry("Persisted", "S.Tag")])?;
                db.promote("S", generation, "0", &zero_progress())
            })
            .unwrap();
        let stale = manager.status("S").await.unwrap();
        assert_eq!(stale.state, IndexState::Stale);
        assert_eq!(stale.active_generation, generation);

        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: Some(zero_progress()),
                    started_at: "runtime-start".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: Some("obsolete build failure".into()),
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        let refreshing = manager.status("S").await.unwrap();
        assert_eq!(refreshing.state, IndexState::Refreshing);
        assert_eq!(refreshing.started_at.as_deref(), Some("runtime-start"));
        assert!(refreshing.progress.is_some());
        assert_ne!(
            refreshing.last_error.as_deref(),
            Some("obsolete build failure")
        );
        manager.mark_promoting("S").unwrap();
        assert_eq!(
            manager.status("S").await.unwrap().state,
            IndexState::Promoting
        );
        manager.clear_promoting("S");

        {
            let mut runtime = manager.runtime.lock().unwrap();
            let state = runtime.get_mut("S").unwrap();
            state.build = None;
            state.last_error = Some("runtime failure".into());
        }
        let failed = manager.status("S").await.unwrap();
        assert_eq!(failed.state, IndexState::Failed);
        assert_eq!(failed.last_error.as_deref(), Some("runtime failure"));

        manager.with_database(|db| db.clear_server("S")).unwrap();
        {
            let mut runtime = manager.runtime.lock().unwrap();
            let state = runtime.get_mut("S").unwrap();
            state.last_error = None;
            state.build = Some(RuntimeBuild {
                control: None,
                progress: Some(InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 8,
                    unique_items: 7,
                    active_time_ms: 2,
                    paused_time_ms: 3,
                    items_per_second: 4.0,
                    estimated_remaining_ms: None,
                }),
                started_at: "runtime-only".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            });
        }
        let runtime_only = manager.status("S").await.unwrap();
        assert_eq!(runtime_only.state, IndexState::Partial);
        assert_eq!(runtime_only.entry_count, 8);
        assert_eq!(runtime_only.unique_item_count, 7);

        manager.runtime.lock().unwrap().clear();
        manager
            .with_database(|db| {
                let generation = db.start_generation(
                    "S",
                    NamespaceOrganization::Flat,
                    BrowseSource::Flat,
                    "failed-start",
                )?;
                db.fail_generation("S", generation, "persisted failure")
            })
            .unwrap();
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: "failed-runtime".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        let failed_build = manager.status("S").await.unwrap();
        assert_eq!(failed_build.state, IndexState::Failed);
        assert_eq!(
            failed_build.last_error.as_deref(),
            Some("persisted failure")
        );
    }

    #[test]
    fn sqlite_generations_search_and_restart_cleanup_work() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
            .unwrap();
        db.insert_entries(
            "S",
            generation,
            &[
                InventoryEntry {
                    display_name: "PV".into(),
                    item_id: "FCS0201!204FI00510.PV".into(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
                },
                InventoryEntry {
                    display_name: "Pressure".into(),
                    item_id: "FCS0201!204FI00510.PV".into(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec!["FCS0201".into()],
                },
                InventoryEntry {
                    display_name: "Temperature".into(),
                    item_id: "FCS0201!204TI00510.PV".into(),
                    kind: InventoryNodeKind::BranchAndItem,
                    breadcrumbs: vec!["FCS0201".into()],
                },
                InventoryEntry {
                    display_name: "Tag".into(),
                    item_id: "Unique.Tag".into(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec!["Area".into()],
                },
            ],
        )
        .unwrap();
        db.promote(
            "S",
            generation,
            "2",
            &InventoryProgress {
                branches_visited: 2,
                entries_seen: 3,
                unique_items: 2,
                active_time_ms: 1,
                paused_time_ms: 0,
                items_per_second: 2.0,
                estimated_remaining_ms: None,
            },
        )
        .unwrap();
        assert_eq!(db.search("S", generation, "PV", 1, 10).unwrap().len(), 1);
        assert_eq!(
            db.search("S", generation, "FCS0201!204FI00510.PV", 1, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            db.search("S", generation, "FCS0201!204FI", 2, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            db.search("S", generation, "fcs0201!204fi", 3, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.search("S", generation, "pv", 3, 10).unwrap().len(), 2);
        assert_eq!(db.search("S", generation, "area", 3, 10).unwrap().len(), 1);
        assert_eq!(db.search("S", generation, "ar", 3, 10).unwrap().len(), 1);
        assert_eq!(db.search("S", generation, "temp", 2, 10).unwrap().len(), 1);
        let second_generation = db
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "3",
            )
            .unwrap();
        db.insert_entries(
            "S",
            second_generation,
            &[InventoryEntry {
                display_name: "Second".into(),
                item_id: "second".into(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec![],
            }],
        )
        .unwrap();
        db.promote(
            "S",
            second_generation,
            "4",
            &InventoryProgress {
                branches_visited: 1,
                entries_seen: 1,
                unique_items: 1,
                active_time_ms: 1,
                paused_time_ms: 0,
                items_per_second: 1.0,
                estimated_remaining_ms: None,
            },
        )
        .unwrap();
        assert_eq!(db.status_rows("S").unwrap().len(), 1);
        assert_eq!(
            db.status_rows("S").unwrap().first().unwrap().generation,
            second_generation
        );
        assert_eq!(
            db.status_rows("S").unwrap().first().unwrap().state,
            "active"
        );
        drop(db);

        let reopened = IndexDb::open(&path).unwrap();
        assert_eq!(
            reopened.status_rows("S").unwrap().first().unwrap().state,
            "active"
        );
        let read_only = IndexDb::open_read_only(&path).unwrap();
        assert_eq!(
            read_only
                .search("S", second_generation, "second", 3, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(
            read_only
                .connection
                .execute("DELETE FROM entries", [])
                .is_err()
        );
    }

    #[test]
    fn full_text_search_ranks_bounded_candidates_without_join_sort() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ranked-search.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.insert_entries(
            "S",
            generation,
            &[
                inventory_entry("ordinary two", "area.219.item"),
                inventory_entry("block 219", "display-contains"),
                inventory_entry("ordinary", "219.item"),
                inventory_entry("219 block", "display-prefix"),
                inventory_entry("219", "display-exact"),
            ],
        )
        .unwrap();
        db.promote("S", generation, "2", &zero_progress()).unwrap();

        let matches = db.search("S", generation, "219", 3, 10).unwrap();
        assert_eq!(
            matches
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "display-exact",
                "display-prefix",
                "219.item",
                "display-contains",
                "area.219.item",
            ]
        );
        assert_eq!(
            db.search("S", generation, "219", 3, 2)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["display-exact", "display-prefix", "219.item"]
        );
        assert_eq!(
            db.search("S", generation, "ordinary two", 3, 10)
                .unwrap()
                .len(),
            1
        );
        db.connection
            .execute(
                "DELETE FROM entries
                 WHERE server = 'S' AND generation = ?1 AND item_id = 'display-exact'",
                [generation as i64],
            )
            .unwrap();
        assert_eq!(db.search("S", generation, "219", 3, 10).unwrap().len(), 4);
    }

    #[test]
    fn exact_search_uses_ranked_equality_matches_without_duplicates() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("exact-search.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.insert_entries(
            "S",
            generation,
            &[
                inventory_entry("PUMP", "z-display"),
                inventory_entry("pump", "a-display"),
                inventory_entry("Pump output", "PUMP"),
                inventory_entry("PUMP", "pump"),
                inventory_entry("unrelated", "other"),
            ],
        )
        .unwrap();
        db.promote("S", generation, "2", &zero_progress()).unwrap();

        let matches = db.search("S", generation, "PuMp", 1, 10).unwrap();
        assert_eq!(
            matches
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-display", "pump", "z-display", "PUMP"]
        );
        assert_eq!(
            db.search("S", generation, "pump", 1, 2)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-display", "pump", "z-display"]
        );
    }

    #[test]
    fn exact_search_bounds_common_display_name_matches_with_equality_indexes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("broad-exact-search.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        let entries = (0..10_000)
            .map(|index| inventory_entry("PV", &format!("Area.{index:05}.PV")))
            .collect::<Vec<_>>();
        db.insert_entries("S", generation, &entries).unwrap();
        db.promote("S", generation, "2", &zero_progress()).unwrap();

        let plan_for = |sql: &str| {
            db.connection
                .prepare(sql)
                .unwrap()
                .query_map(params!["S", generation as i64, "pv", 4_i64], |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        let display_plan = plan_for(
            "EXPLAIN QUERY PLAN
             SELECT e.item_id, e.display_name, e.display_name_norm,
                    e.item_id_norm, e.kind, e.breadcrumbs
             FROM entries e
             WHERE e.server = ?1 AND e.generation = ?2
               AND e.display_name_norm = ?3
             ORDER BY e.item_id_norm, e.item_id
             LIMIT ?4",
        );
        assert!(
            display_plan
                .iter()
                .any(|detail| detail.contains("entries_display_exact"))
        );
        assert!(
            display_plan
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE"))
        );

        let item_plan = plan_for(
            "EXPLAIN QUERY PLAN
             SELECT e.item_id, e.display_name, e.display_name_norm,
                    e.item_id_norm, e.kind, e.breadcrumbs
             FROM entries e
             WHERE e.server = ?1 AND e.generation = ?2
               AND e.item_id_norm = ?3
               AND e.display_name_norm <> ?3
             ORDER BY length(e.display_name_norm), e.display_name_norm,
                      e.item_id_norm, e.item_id
             LIMIT ?4",
        );
        assert!(
            item_plan
                .iter()
                .any(|detail| detail.contains("entries_item_exact"))
        );
        assert!(
            item_plan
                .iter()
                .all(|detail| !detail.contains("USE TEMP B-TREE"))
        );

        let matches = db.search("S", generation, "PV", 1, 3).unwrap();
        assert_eq!(matches.len(), 4);
        assert_eq!(
            matches
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "Area.00000.PV",
                "Area.00001.PV",
                "Area.00002.PV",
                "Area.00003.PV"
            ]
        );
    }

    #[test]
    fn prefix_search_uses_indexed_ranges_and_preserves_ranking() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("prefix-search.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        let generation = db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        db.insert_entries(
            "S",
            generation,
            &[
                inventory_entry("219 block", "display-prefix"),
                inventory_entry("219", "display-exact"),
                inventory_entry("ordinary", "219.item"),
                inventory_entry("219 both", "219.both"),
                inventory_entry("ordinary", "x219.item"),
                inventory_entry("ordinary", "x%219.item"),
                inventory_entry("éclair", "unicode-display"),
                inventory_entry("ordinary", "\u{10ffff}item"),
                inventory_entry("block 219", "display-contains"),
            ],
        )
        .unwrap();
        db.promote("S", generation, "2", &zero_progress()).unwrap();

        assert_eq!(
            db.search("S", generation, "219", 2, 10)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["display-exact", "219.both", "display-prefix", "219.item"]
        );
        assert_eq!(
            db.search("S", generation, "219", 2, 2)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["display-exact", "219.both", "display-prefix"]
        );
        assert_eq!(
            db.search("S", generation, "x%", 2, 10)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["x%219.item"]
        );
        assert_eq!(
            db.search("S", generation, "É", 2, 10)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["unicode-display"]
        );
        assert_eq!(
            db.search("S", generation, "\u{10ffff}", 2, 10)
                .unwrap()
                .iter()
                .map(|value| value.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["\u{10ffff}item"]
        );
    }

    #[test]
    fn full_text_search_reports_missing_tables() {
        let directory = tempdir().unwrap();
        let fts_path = directory.path().join("missing-fts.sqlite3");
        let mut fts_db = IndexDb::open(&fts_path).unwrap();
        let fts_generation = fts_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        fts_db
            .insert_entries("S", fts_generation, &[inventory_entry("Tag", "S.Tag")])
            .unwrap();
        fts_db
            .promote("S", fts_generation, "2", &zero_progress())
            .unwrap();
        fts_db
            .connection
            .execute("DROP TABLE entries_fts", [])
            .unwrap();
        assert!(fts_db.search("S", fts_generation, "tag", 3, 10).is_err());

        let entries_path = directory.path().join("missing-entries.sqlite3");
        let mut entries_db = IndexDb::open(&entries_path).unwrap();
        let entries_generation = entries_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        entries_db
            .insert_entries("S", entries_generation, &[inventory_entry("Tag", "S.Tag")])
            .unwrap();
        entries_db
            .promote("S", entries_generation, "2", &zero_progress())
            .unwrap();
        entries_db
            .connection
            .execute("DROP TABLE entries", [])
            .unwrap();
        assert!(
            entries_db
                .search("S", entries_generation, "tag", 3, 10)
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_during_promotion_does_not_wait_for_database_mutex() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| {
                let generation = db
                    .start_generation(
                        "S",
                        NamespaceOrganization::Hierarchical,
                        BrowseSource::Da2,
                        "1",
                    )
                    .unwrap();
                db.update_progress("S", generation, &zero_progress())
                    .unwrap();
                Ok(())
            })
            .unwrap();
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: Some(zero_progress()),
                    started_at: "runtime-start".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        manager.mark_promoting("S").unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let lock_manager = Arc::clone(&manager);
        let lock_thread = std::thread::spawn(move || {
            let database_guard = lock_manager.database.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(database_guard);
        });
        locked_rx.recv().unwrap();
        let status_manager = Arc::clone(&manager);
        let status_task = tokio::spawn(async move { status_manager.status("S").await });
        let status = tokio::time::timeout(Duration::from_secs(1), status_task)
            .await
            .expect("promotion status should not wait for the writer mutex")
            .expect("status task should not panic")
            .unwrap();
        assert_eq!(status.state, IndexState::Promoting);
        release_tx.send(()).unwrap();
        lock_thread.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn indexed_search_during_promotion_does_not_wait_for_database_mutex() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "2",
        );
        insert_runtime_build(&manager, Arc::new(RecordingInventoryControl::default()));
        manager.mark_promoting("S").unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let lock_manager = Arc::clone(&manager);
        let lock_thread = std::thread::spawn(move || {
            let database_guard = lock_manager.database.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(database_guard);
        });
        locked_rx.recv().unwrap();

        let search_manager = Arc::clone(&manager);
        let search_task =
            tokio::spawn(async move { search_manager.search("S", "Persisted", 3, 10).await });
        let search = tokio::time::timeout(Duration::from_secs(1), search_task)
            .await
            .expect("indexed search should not wait for the writer mutex")
            .expect("search task should not panic")
            .unwrap();
        assert_eq!(search.status.state, IndexState::Promoting);
        assert_eq!(search.matches.len(), 1);
        assert_eq!(search.matches[0].item_id, "Persisted.Tag");

        release_tx.send(()).unwrap();
        lock_thread.join().unwrap();
    }

    #[tokio::test]
    async fn refresh_start_failure_backs_off_until_forced_retry() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![Err("start failed".into()), Ok(immediate_inventory_handle())],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));

        assert_eq!(
            manager.refresh("S", true).await.unwrap_err().to_string(),
            "start failed"
        );
        let failed = manager.status("S").await.unwrap();
        assert_eq!(failed.state, IndexState::Failed);
        assert_eq!(failed.last_error.as_deref(), Some("start failed"));

        let backed_off = manager.refresh("S", false).await.unwrap();
        assert_eq!(backed_off.state, IndexState::Failed);
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);

        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Ready).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn refresh_capability_failure_cancels_inventory_and_records_error() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![Err("capabilities failed".into())],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));

        assert_eq!(
            manager.refresh("S", true).await.unwrap_err().to_string(),
            "capabilities failed"
        );
        assert!(control.cancelled.load(Ordering::Acquire));
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Failed);
        assert_eq!(status.last_error.as_deref(), Some("capabilities failed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_database_failure_cancels_inventory_and_records_error() {
        let directory = tempdir().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        let control = Arc::new(RecordingInventoryControl::default());
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| {
                drop_table(db, "generations");
                Ok(())
            })
            .unwrap();

        let error = manager.refresh("S", true).await.unwrap_err().to_string();
        assert!(error.contains("no such table"));
        assert!(control.cancelled.load(Ordering::Acquire));
        assert_eq!(
            manager
                .runtime
                .lock()
                .unwrap()
                .get("S")
                .unwrap()
                .last_error
                .clone(),
            Some(error)
        );
    }

    #[tokio::test]
    async fn refresh_rejects_a_duplicate_in_process_build_lock() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(vec![], vec![])),
            settings(path.clone()),
        ));
        let lock = BuildFileLock::acquire(&path, "S").unwrap();
        manager.build_locks.lock().unwrap().insert("S".into(), lock);

        let error = manager.refresh("S", true).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("build lock is already held in this process")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_cancels_inventory_when_shutdown_is_requested_after_start() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let inventory_started = Arc::new(Notify::new());
        let inventory_release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&control),
                ))],
                vec![],
            )
            .with_inventory_gate(
                Arc::clone(&inventory_started),
                Arc::clone(&inventory_release),
            ),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });

        inventory_started.notified().await;
        manager.background_tasks.request_shutdown();
        inventory_release.notify_one();

        let status = refresh.await.unwrap().unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(control.cancelled.load(Ordering::Acquire));
        assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_honors_cancel_during_inventory_startup() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let inventory_started = Arc::new(Notify::new());
        let inventory_release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&control),
                ))],
                vec![Ok(default_capabilities())],
            )
            .with_inventory_gate(
                Arc::clone(&inventory_started),
                Arc::clone(&inventory_release),
            ),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        inventory_started.notified().await;

        let status = manager
            .control("S", IndexControlAction::Cancel)
            .await
            .unwrap();
        assert_eq!(status.state, IndexState::Partial);
        inventory_release.notify_one();

        let status = tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .expect("refresh should finish after startup cancellation")
            .expect("refresh task should not panic")
            .unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(control.cancelled.load(Ordering::Acquire));
        assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_discards_generation_when_shutdown_is_requested_before_spawn() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let capability_started = Arc::new(Notify::new());
        let capability_release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&control),
                ))],
                vec![],
            )
            .with_capability_gate(
                Arc::clone(&capability_started),
                Arc::clone(&capability_release),
            ),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });

        capability_started.notified().await;
        manager.background_tasks.request_shutdown();
        capability_release.notify_one();

        let status = refresh.await.unwrap().unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(control.cancelled.load(Ordering::Acquire));
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_cleans_up_when_background_spawn_is_rejected() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let manager = Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&control),
                ))],
                vec![],
            )),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .reject_next_build_spawn
            .store(true, Ordering::Release);

        let status = manager.refresh("S", true).await.unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(control.cancelled.load(Ordering::Acquire));
        assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
    }

    #[tokio::test]
    async fn refresh_pauses_for_existing_foreground_work_and_control_without_build_is_a_noop() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let stream_started = Arc::new(Notify::new());
        let stream_release = Arc::new(Notify::new());
        let handle = InventoryHandle {
            stream: Box::new(BlockingInventoryStream {
                started: Arc::clone(&stream_started),
                release: Arc::clone(&stream_release),
                event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: false,
                    cancelled: true,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                }))),
            }),
            control: control.clone(),
        };
        let manager = Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(vec![Ok(handle)], vec![])),
            settings(directory.path().join("index.sqlite3")),
        ));

        let guard = manager.foreground_guard("S");
        manager.refresh("S", true).await.unwrap();
        stream_started.notified().await;
        assert!(control.paused.load(Ordering::Acquire));
        drop(guard);
        stream_release.notify_one();
        wait_for_state(&manager, "S", IndexState::NotIndexed).await;

        assert_eq!(
            manager
                .control("S", IndexControlAction::Pause)
                .await
                .unwrap()
                .state,
            IndexState::NotIndexed
        );
        assert!(manager.refresh("Other", true).await.is_err());
        assert!(
            manager
                .control("Other", IndexControlAction::Cancel)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn refresh_discards_generation_when_runtime_build_disappears() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let recovery_handle = immediate_inventory_handle();
        let capability_started = Arc::new(Notify::new());
        let capability_release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(
                vec![
                    Ok(handle_with_control(VecDeque::new(), Arc::clone(&control))),
                    Ok(recovery_handle),
                ],
                vec![],
            )
            .with_capability_gate(
                Arc::clone(&capability_started),
                Arc::clone(&capability_release),
            ),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        capability_started.notified().await;
        manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
        capability_release.notify_one();

        assert_eq!(
            refresh.await.unwrap().unwrap_err().to_string(),
            "index build disappeared before start"
        );
        assert!(control.cancelled.load(Ordering::Acquire));
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
        );
        assert!(manager.active_builds.lock().unwrap().is_empty());
        assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
        assert!(manager.build_locks.lock().unwrap().is_empty());
        assert!(manager.pause_overlays.lock().unwrap().is_empty());
        assert!(manager.pending_cancels.lock().unwrap().is_empty());

        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Ready).await;
    }

    #[tokio::test]
    async fn manager_promotes_success_and_rolls_back_failed_refresh() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
        let ready = manager.status("S").await.unwrap();
        assert_eq!(ready.active_generation, 1);
        let ready_search = manager.search("S", "mock", 3, 10).await.unwrap();
        assert_eq!(ready_search.status.active_generation, 1);
        assert_eq!(
            ready_search.matches,
            vec![IndexedMatch {
                item_id: "Mock.Tag".into(),
                display_name: "Mock tag".into(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec!["Mock".into()],
            }]
        );

        client
            .inventory_events
            .lock()
            .unwrap()
            .push_back(Err("inventory failed".into()));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Failed).await;
        assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
        let failed = manager.status("S").await.unwrap();
        assert_eq!(failed.active_generation, 1);
        assert_eq!(failed.state, IndexState::Failed);
        let failed_search = manager.search("S", "mock", 3, 10).await.unwrap();
        assert_eq!(failed_search.status.active_generation, 1);
        assert_eq!(failed_search.matches, ready_search.matches);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_inventory_warning_keeps_generation_active_and_searchable() {
        let directory = tempdir().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        let warning = "skipped 1 DA2 branch name(s) rejected by the server";
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
                    Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: Some(warning.into()),
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    })),
                ]),
                Arc::new(RecordingInventoryControl::default()),
            ))],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));

        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Ready).await;

        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Ready);
        assert_eq!(status.active_generation, 1);
        assert_eq!(status.entry_count, 1);
        assert_eq!(status.unique_item_count, 1);
        assert_eq!(status.last_error.as_deref(), Some(warning));
        assert_eq!(
            manager
                .search("S", "tag", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );
        let rows = manager.with_database(|db| db.status_rows("S")).unwrap();
        assert_eq!(rows[0].state, "active");
        assert_eq!(rows[0].last_error.as_deref(), Some(warning));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_ownership_is_held_until_inventory_stream_cleanup_finishes() {
        let directory = tempdir().unwrap();
        let (started, started_receiver) = std::sync::mpsc::sync_channel(0);
        let release = Arc::new(AtomicBool::new(false));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(InventoryHandle {
                stream: Box::new(DropGateInventoryStream {
                    started,
                    release: Arc::clone(&release),
                    control: Arc::clone(&control),
                    event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    }))),
                }),
                control,
            })],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("cleanup-order.sqlite3")),
        ));

        manager.refresh("S", true).await.unwrap();
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("inventory stream cleanup did not start");

        assert!(
            manager
                .coordination
                .build_owners
                .lock()
                .unwrap()
                .contains_key("S"),
            "build ownership was released before inventory cleanup finished"
        );
        assert!(
            manager.active_builds.lock().unwrap().contains("S"),
            "active build state was released before inventory cleanup finished"
        );
        assert!(
            manager.build_locks.lock().unwrap().contains_key("S"),
            "build file lock was released before inventory cleanup finished"
        );

        release.store(true, Ordering::Release);
        wait_for_state(&manager, "S", IndexState::Ready).await;

        assert!(
            !manager
                .coordination
                .build_owners
                .lock()
                .unwrap()
                .contains_key("S")
        );
        assert!(!manager.active_builds.lock().unwrap().contains("S"));
        assert!(manager.build_locks.lock().unwrap().get("S").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_terminal_error_and_cancellation_paths_preserve_consistent_status() {
        async fn run_case(
            events: VecDeque<anyhow::Result<InventoryEvent>>,
            maintenance_windows: Vec<String>,
            expected_state: IndexState,
            expected_error: Option<&str>,
        ) {
            let directory = tempdir().unwrap();
            let client = Arc::new(LifecycleClient::new(
                vec![Ok(handle_with_control(
                    events,
                    Arc::new(RecordingInventoryControl::default()),
                ))],
                vec![],
            ));
            let mut config = settings(directory.path().join("index.sqlite3"));
            config.maintenance_windows = maintenance_windows;
            let manager = Arc::new(IndexManager::new(client, config));
            manager.refresh("S", true).await.unwrap();
            wait_for_state(&manager, "S", expected_state).await;
            let status = manager.status("S").await.unwrap();
            assert_eq!(status.last_error.as_deref(), expected_error);
        }

        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        run_case(
            VecDeque::new(),
            vec![],
            IndexState::Failed,
            Some("inventory stream ended before completion"),
        )
        .await;
        run_case(
            VecDeque::from([Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: false,
                truncated: true,
                warning: Some("truncated by server".into()),
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            }))]),
            vec![],
            IndexState::Failed,
            Some("truncated by server"),
        )
        .await;
        run_case(
            VecDeque::from([Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: true,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            }))]),
            vec![],
            IndexState::NotIndexed,
            None,
        )
        .await;
        run_case(
            VecDeque::from([
                Ok(InventoryEvent::Entry(InventoryEntry {
                    display_name: "Invalid".into(),
                    item_id: String::new(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec![],
                })),
                Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                })),
            ]),
            vec![],
            IndexState::Failed,
            Some("inventory entry has an empty ItemID"),
        )
        .await;
        run_case(
            VecDeque::from([Err(anyhow::anyhow!("inventory stream failed"))]),
            vec![],
            IndexState::Failed,
            Some("inventory stream failed"),
        )
        .await;
        run_case(
            VecDeque::new(),
            vec!["invalid".into()],
            IndexState::Failed,
            Some("maintenance window must use HH:MM-HH:MM"),
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_inventory_event_is_cancelled_and_releases_the_scheduler() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(InventoryHandle {
                stream: Box::new(BlockingInventoryStream {
                    started: Arc::new(Notify::new()),
                    release: Arc::new(Notify::new()),
                    event: Some(Ok(InventoryEvent::Entry(inventory_entry(
                        "Stalled",
                        "S.Stalled",
                    )))),
                }),
                control: Arc::clone(&control) as Arc<dyn InventoryControl>,
            })],
            vec![],
        ));
        let mut config = settings(directory.path().join("stalled-inventory.sqlite3"));
        config.operation_timeout_seconds = 1;
        let manager = Arc::new(IndexManager::new(client, config));

        manager.refresh("S", true).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        wait_for_state(&manager, "S", IndexState::Failed).await;
        let status = manager.status("S").await.unwrap();
        assert_eq!(
            status.last_error.as_deref(),
            Some("inventory event timed out after 1 seconds")
        );
        assert!(control.cancelled.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_reports_batch_progress_and_promotion_database_failures() {
        let directory = tempdir().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        let mut successful_config = settings(directory.path().join("successful-batch.sqlite3"));
        successful_config.batch_size = 1;
        let successful_manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            successful_config,
        ));
        successful_manager.refresh("S", true).await.unwrap();
        wait_for_build(&successful_manager, IndexState::Ready).await;
        assert_eq!(successful_manager.status("S").await.unwrap().entry_count, 1);

        let mut batch_config = settings(directory.path().join("batch.sqlite3"));
        batch_config.batch_size = 1;
        batch_config.commit_batch_size = 1;
        let batch_client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([Ok(InventoryEvent::Entry(InventoryEntry {
                    display_name: "Invalid".into(),
                    item_id: String::new(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec![],
                }))]),
                Arc::new(RecordingInventoryControl::default()),
            ))],
            vec![],
        ));
        let batch_manager = Arc::new(IndexManager::new(batch_client, batch_config));
        batch_manager.refresh("S", true).await.unwrap();
        wait_for_state(&batch_manager, "S", IndexState::Failed).await;
        assert_eq!(
            batch_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .as_deref(),
            Some("inventory entry has an empty ItemID")
        );

        let progress_started = Arc::new(Notify::new());
        let progress_release = Arc::new(Notify::new());
        let progress_manager = manager_with_blocking_event(
            directory.path().join("progress.sqlite3"),
            Ok(InventoryEvent::Progress(zero_progress())),
            Arc::clone(&progress_started),
            Arc::clone(&progress_release),
        );
        progress_manager.refresh("S", true).await.unwrap();
        progress_started.notified().await;
        progress_manager
            .with_database(|db| {
                db.connection.execute_batch(
                    "CREATE TRIGGER fail_progress
                     BEFORE UPDATE OF entry_count ON generations
                     BEGIN
                       SELECT RAISE(FAIL, 'progress write failed');
                     END;",
                )?;
                Ok(())
            })
            .unwrap();
        progress_release.notify_one();
        wait_for_state(&progress_manager, "S", IndexState::Failed).await;
        assert!(
            progress_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .unwrap()
                .contains("progress write failed")
        );

        let promotion_started = Arc::new(Notify::new());
        let promotion_release = Arc::new(Notify::new());
        let promotion_manager = manager_with_blocking_event(
            directory.path().join("promotion.sqlite3"),
            Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: true,
                cancelled: false,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            })),
            Arc::clone(&promotion_started),
            Arc::clone(&promotion_release),
        );
        promotion_manager.refresh("S", true).await.unwrap();
        promotion_started.notified().await;
        promotion_manager
            .with_database(|db| {
                db.connection.execute_batch(
                    "CREATE TRIGGER fail_promotion
                     BEFORE UPDATE OF state ON generations
                     WHEN NEW.state = 'active'
                     BEGIN
                       SELECT RAISE(FAIL, 'promotion failed');
                     END;",
                )?;
                Ok(())
            })
            .unwrap();
        promotion_release.notify_one();
        wait_for_state(&promotion_manager, "S", IndexState::Failed).await;
        assert!(
            promotion_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .unwrap()
                .contains("promotion failed")
        );

        let final_insert_client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([
                    Ok(InventoryEvent::Entry(inventory_entry("Final", "S.Final"))),
                    Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    })),
                ]),
                Arc::new(RecordingInventoryControl::default()),
            ))],
            vec![],
        ));
        let final_insert_manager = Arc::new(IndexManager::new(
            final_insert_client,
            settings(directory.path().join("final-insert.sqlite3")),
        ));
        final_insert_manager.refresh("S", true).await.unwrap();
        final_insert_manager
            .with_database(|db| {
                db.connection.execute_batch(
                    "CREATE TRIGGER fail_final_insert
                     BEFORE INSERT ON entries
                     BEGIN
                       SELECT RAISE(FAIL, 'final insert failed');
                     END;",
                )?;
                Ok(())
            })
            .unwrap();
        wait_for_state(&final_insert_manager, "S", IndexState::Failed).await;
        assert!(
            final_insert_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .unwrap()
                .contains("final insert failed")
        );
    }

    #[tokio::test]
    async fn build_stops_when_maintenance_health_or_rate_limit_control_is_cancelled() {
        async fn run_cancelled_case(
            path: PathBuf,
            maintenance_windows: Vec<String>,
        ) -> IndexStatus {
            let control = Arc::new(RecordingInventoryControl::default());
            control.cancel();
            let client = Arc::new(LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&control),
                ))],
                vec![],
            ));
            let mut config = settings(path);
            config.maintenance_windows = maintenance_windows;
            let manager = Arc::new(IndexManager::new(client, config));
            manager.refresh("S", true).await.unwrap();
            wait_for_state(&manager, "S", IndexState::Failed).await;
            manager.status("S").await.unwrap()
        }

        let directory = tempdir().unwrap();
        let health = run_cancelled_case(directory.path().join("health.sqlite3"), vec![]).await;
        assert_eq!(
            health.last_error.as_deref(),
            Some("inventory stream ended before completion")
        );

        let now = Local::now();
        let minute = (now.hour() * 60 + now.minute()) as u16;
        let maintenance = format!(
            "{:02}:{:02}-{:02}:{:02}",
            ((minute + 2) % 1440) / 60,
            ((minute + 2) % 1440) % 60,
            ((minute + 3) % 1440) / 60,
            ((minute + 3) % 1440) % 60
        );
        let maintenance = run_cancelled_case(
            directory.path().join("maintenance.sqlite3"),
            vec![maintenance],
        )
        .await;
        assert_eq!(
            maintenance.last_error.as_deref(),
            Some("inventory stream ended before completion")
        );

        let control = Arc::new(RecordingInventoryControl::default());
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(InventoryHandle {
                stream: Box::new(CancellingEntryStream {
                    control: Arc::clone(&control),
                    yielded: false,
                }),
                control: control.clone(),
            })],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("rate.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Failed).await;
        assert!(control.cancelled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn controls_foreground_quiet_period_and_concurrency_coordinate_builds() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        let stream_started = Arc::new(Notify::new());
        let stream_release = Arc::new(Notify::new());
        let blocking = InventoryHandle {
            stream: Box::new(BlockingInventoryStream {
                started: Arc::clone(&stream_started),
                release: Arc::clone(&stream_release),
                event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: false,
                    cancelled: true,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                }))),
            }),
            control: control.clone(),
        };
        let client = Arc::new(LifecycleClient::new(vec![Ok(blocking)], vec![]));
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.quiet_period_seconds = 0;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.refresh("S", true).await.unwrap();
        stream_started.notified().await;
        let starts = client.inventory_start_count.load(Ordering::Relaxed);
        assert_eq!(starts, 1);
        assert_eq!(
            manager.refresh("S", true).await.unwrap().state,
            IndexState::Partial
        );
        assert_eq!(
            manager.refresh("T", true).await.unwrap_err().to_string(),
            "namespace index build concurrency limit reached"
        );
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), starts);

        let baseline_pauses = control.pause_count.load(Ordering::Relaxed);
        manager
            .control("S", IndexControlAction::Pause)
            .await
            .unwrap();
        assert!(control.pause_count.load(Ordering::Relaxed) > baseline_pauses);
        assert!(control.paused.load(Ordering::Acquire));

        let baseline_resumes = control.resume_count.load(Ordering::Relaxed);
        manager
            .control("S", IndexControlAction::Resume)
            .await
            .unwrap();
        assert!(control.resume_count.load(Ordering::Relaxed) > baseline_resumes);

        let guard = manager.foreground_guard("S");
        assert!(control.paused.load(Ordering::Acquire));
        let resumes_during_foreground = control.resume_count.load(Ordering::Relaxed);
        manager
            .control("S", IndexControlAction::Resume)
            .await
            .unwrap();
        assert_eq!(
            control.resume_count.load(Ordering::Relaxed),
            resumes_during_foreground
        );
        drop(guard);
        wait_for_counter(&control.resume_count, resumes_during_foreground + 1).await;

        let guard = manager.foreground_guard("S");
        manager
            .control("S", IndexControlAction::Pause)
            .await
            .unwrap();
        let resumes_while_operator_paused = control.resume_count.load(Ordering::Relaxed);
        drop(guard);
        tokio::task::yield_now().await;
        assert_eq!(
            control.resume_count.load(Ordering::Relaxed),
            resumes_while_operator_paused
        );
        manager
            .control("S", IndexControlAction::Resume)
            .await
            .unwrap();

        manager
            .control("S", IndexControlAction::Cancel)
            .await
            .unwrap();
        assert!(control.cancelled.load(Ordering::Acquire));
        stream_release.notify_one();
        wait_for_state(&manager, "S", IndexState::NotIndexed).await;
    }

    #[tokio::test]
    async fn shutdown_drains_background_scheduler_and_build_tasks() {
        let directory = tempdir().unwrap();
        let stream_started = Arc::new(Notify::new());
        let stream_release = Arc::new(Notify::new());
        let manager = manager_with_blocking_event(
            directory.path().join("index.sqlite3"),
            Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
            Arc::clone(&stream_started),
            Arc::clone(&stream_release),
        );

        manager.refresh("S", true).await.unwrap();
        stream_started.notified().await;
        manager.start_background_indexing();
        assert!(manager.background_tasks.state.lock().unwrap().active >= 2);

        let shutdown_manager = Arc::clone(&manager);
        let shutdown = tokio::spawn(async move {
            shutdown_manager.shutdown_background_indexing().await;
        });
        for _ in 0..100 {
            if manager.background_tasks.is_shutting_down() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(manager.background_tasks.is_shutting_down());

        stream_release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("background indexing did not drain")
            .unwrap();
        assert_eq!(manager.background_tasks.state.lock().unwrap().active, 0);
        assert_eq!(manager.status("S").await.unwrap().state, IndexState::Failed);
    }

    #[tokio::test]
    async fn background_task_registry_is_idempotent_and_rejects_new_work_after_shutdown() {
        let tasks = Arc::new(BackgroundTasks::new());
        assert!(tasks.spawn(async {}));
        tasks.wait_for_idle().await;
        assert!(!tasks.is_shutting_down());

        tasks.request_shutdown();
        tasks.request_shutdown();
        assert!(tasks.is_shutting_down());
        assert!(!tasks.spawn(async {}));

        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .reject_next_cleanup_spawn
            .store(true, Ordering::Release);
        manager.schedule_cleanup("S");
        assert!(!manager.cleanup_tasks.lock().unwrap().contains_key("S"));
        manager.shutdown_background_indexing().await;
        assert_eq!(
            manager.refresh("S", true).await.unwrap().state,
            IndexState::NotIndexed
        );
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn background_task_registry_rejects_work_when_its_state_lock_is_poisoned() {
        let tasks = Arc::new(BackgroundTasks::new());
        let state = Arc::clone(&tasks);
        let _ = std::panic::catch_unwind(move || {
            let _guard = state.state.lock().unwrap();
            panic!("poison background task state for error-path coverage");
        });
        assert!(!tasks.spawn(async {}));
    }

    #[test]
    fn cleanup_error_paths_tolerate_database_and_registry_failures() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("cleanup-errors.sqlite3")),
        ));
        manager
            .with_database(|db| {
                db.connection
                    .execute_batch("DROP TABLE generations")
                    .unwrap();
                Ok(())
            })
            .unwrap();

        manager.fail_generation_and_schedule_cleanup("S", 1, "failed");
        manager.abandon_generation("S", 1, "abandoned");

        let cleanup_tasks = Arc::clone(&manager.cleanup_tasks);
        let poisoned_cleanup_tasks = Arc::clone(&cleanup_tasks);
        let _ = std::panic::catch_unwind(move || {
            let _guard = cleanup_tasks.lock().unwrap();
            panic!("poison cleanup registry for error-path coverage");
        });
        manager.schedule_cleanup("S");

        let cleanup_worker_active = Arc::new(AtomicBool::new(false));
        spawn_cleanup_worker_if_idle(
            Arc::clone(&cleanup_worker_active),
            manager.settings.database_path.clone(),
            Arc::new(BackgroundTasks::new()),
            poisoned_cleanup_tasks,
            Arc::clone(&manager.coordination),
            false,
        );
        assert!(!cleanup_worker_active.load(Ordering::Acquire));
    }

    #[test]
    fn foreground_guard_resumes_synchronously_without_a_tokio_runtime() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: Some(trait_control),
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );

        let guard = manager.foreground_guard("S");
        assert!(control.paused.load(Ordering::Acquire));
        drop(guard);
        assert!(!control.paused.load(Ordering::Acquire));
        assert!(control.resume_count.load(Ordering::Relaxed) > 0);

        manager.foreground_end("Missing");
        manager.finish_build("Missing", None);

        let poisoned = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("poisoned.sqlite3")),
        ));
        let foreground_users = Arc::clone(&poisoned.foreground_users);
        let _ = std::panic::catch_unwind(move || {
            let _guard = foreground_users.lock().unwrap();
            panic!("poison foreground users for cleanup error-path coverage");
        });
        poisoned.foreground_end("S");
    }

    #[tokio::test]
    async fn pause_overlays_compose_and_are_visible_in_runtime_status() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, control.clone());

        manager.set_pause_overlay("S", None, Some(true));
        let status = manager.status("S").await.unwrap();
        assert_eq!(
            status.pause_reason,
            Some(crate::controller::PauseReason::OpcHealth)
        );
        assert!(control.paused.load(Ordering::Acquire));

        manager.set_pause_overlay("S", Some(true), None);
        let status = manager.status("S").await.unwrap();
        assert_eq!(
            status.pause_reason,
            Some(crate::controller::PauseReason::Maintenance)
        );

        manager.set_pause_overlay("S", Some(false), Some(false));
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.pause_reason, None);
        assert!(!control.paused.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn foreground_cleanup_without_a_runtime_build_is_safe() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.foreground_end("Missing");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    #[test]
    fn finish_build_checks_control_identity_without_an_ownership_token() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("defensive-finalization.sqlite3")),
        ));
        let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&current));
        manager.coordination.build_owners.lock().unwrap().clear();
        let obsolete: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            manager.finish_build_inner("S", Some(&obsolete), None, None);
            manager.finish_build_inner("S", None, None, None);
        });
        assert!(
            manager
                .runtime
                .lock()
                .unwrap()
                .get("S")
                .unwrap()
                .build
                .is_none()
        );
        assert!(manager.active_builds.lock().unwrap().is_empty());
    }

    #[test]
    fn finish_build_handles_a_poisoned_build_owner_registry() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("poisoned-finalization.sqlite3")),
        ));
        insert_runtime_build(&manager, Arc::new(RecordingInventoryControl::default()));
        let owners = Arc::clone(&manager.coordination.build_owners);
        let _ = std::panic::catch_unwind(move || {
            let _guard = owners.lock().unwrap();
            panic!("poison build-owner registry for finalization error-path coverage");
        });
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .finish();
        let ownership = Arc::new(());
        tracing::subscriber::with_default(subscriber, || {
            manager.finish_build_owned("S", &ownership, None);
        });
        assert!(
            manager
                .runtime
                .lock()
                .unwrap()
                .get("S")
                .unwrap()
                .build
                .is_some()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finish_build_for_control_handles_current_obsolete_and_poisoned_runtime() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);

        let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&current));
        manager.finish_build_for_control("S", &current, Some("failed".into()));
        let state = manager.runtime.lock().unwrap();
        assert!(state.get("S").unwrap().build.is_none());
        assert_eq!(
            state.get("S").unwrap().last_error.as_deref(),
            Some("failed")
        );
        drop(state);

        let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let obsolete: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&current));
        manager.finish_build_for_control("S", &obsolete, None);
        manager.finish_build_for_control("Missing", &obsolete, None);

        let runtime = Arc::clone(&manager.runtime);
        let _ = std::panic::catch_unwind(move || {
            let _guard = runtime.lock().unwrap();
            panic!("poison index runtime for finalization error-path coverage");
        });
        manager.finish_build_for_control("S", &obsolete, None);
    }

    #[tokio::test]
    async fn control_reports_a_poisoned_runtime_lock() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| db.enroll("S", &timestamp_now()))
            .unwrap();
        let runtime = Arc::clone(&manager.runtime);
        let _ = std::panic::catch_unwind(move || {
            let _guard = runtime.lock().unwrap();
            panic!("poison index runtime for error-path coverage");
        });
        assert_eq!(
            manager
                .control("S", IndexControlAction::Pause)
                .await
                .unwrap_err()
                .to_string(),
            "index runtime lock poisoned"
        );
    }

    #[tokio::test]
    async fn maintenance_duty_cycle_and_rate_limit_honor_pause_and_cancellation() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.duty_cycle_percent = 50;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: Some(Arc::clone(&trait_control)),
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );

        assert!(
            manager
                .wait_for_maintenance(
                    &trait_control,
                    "S",
                    &[MaintenanceWindow {
                        start_minute: 0,
                        end_minute: 0,
                    }],
                )
                .await
        );
        assert!(control.resume_count.load(Ordering::Relaxed) > 0);

        manager
            .enforce_duty_cycle(&trait_control, "S", Duration::from_millis(1), 50)
            .await;
        assert!(control.pause_count.load(Ordering::Relaxed) > 0);
        assert!(!control.paused.load(Ordering::Acquire));

        let now = Local::now();
        let current_minute = (now.hour() * 60 + now.minute()) as u16;
        let inactive = MaintenanceWindow {
            start_minute: (current_minute + 2) % (24 * 60),
            end_minute: (current_minute + 3) % (24 * 60),
        };
        control.cancel();
        assert!(
            !manager
                .wait_for_maintenance(&trait_control, "S", &[inactive])
                .await
        );

        let delayed_cancel = Arc::new(RecordingInventoryControl::default());
        let delayed_trait: Arc<dyn InventoryControl> = delayed_cancel.clone();
        let canceller = Arc::clone(&delayed_cancel);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            canceller.cancel();
        });
        assert!(
            !manager
                .wait_for_maintenance(&delayed_trait, "S", &[inactive])
                .await
        );
        cancel_task.await.unwrap();

        let rate_control = Arc::new(RecordingInventoryControl::default());
        let rate_trait: Arc<dyn InventoryControl> = rate_control.clone();
        let mut limiter = ItemRateLimiter::new(1, 1);
        assert!(limiter.acquire(&rate_trait).await);
        let canceller = Arc::clone(&rate_control);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            canceller.cancel();
        });
        assert!(!limiter.acquire(&rate_trait).await);
        cancel_task.await.unwrap();

        let progress_control = Arc::new(RecordingInventoryControl::default());
        progress_control.cancel_on_pause();
        let progress_client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 1,
                    unique_items: 1,
                    active_time_ms: 1,
                    paused_time_ms: 0,
                    items_per_second: 1.0,
                    estimated_remaining_ms: None,
                }))]),
                Arc::clone(&progress_control),
            ))],
            vec![],
        ));
        let mut progress_config = settings(directory.path().join("duty-progress.sqlite3"));
        progress_config.duty_cycle_percent = 50;
        let progress_manager = Arc::new(IndexManager::new(progress_client, progress_config));
        progress_manager.refresh("S", true).await.unwrap();
        wait_for_state(&progress_manager, "S", IndexState::Failed).await;
        assert!(progress_control.cancelled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn unhealthy_probe_backs_off_and_stops_when_cancelled() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Err("server unavailable".into())],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: Some(Arc::clone(&trait_control)),
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        let mut next_probe = Instant::now();
        let mut backoff = Duration::from_secs(1);
        let canceller = Arc::clone(&control);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            canceller.cancel();
        });

        assert!(
            !manager
                .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff,)
                .await
        );
        cancel_task.await.unwrap();
        assert_eq!(backoff, Duration::from_secs(2));
        assert!(next_probe > Instant::now());
        assert!(control.pause_count.load(Ordering::Relaxed) > 0);

        let delayed_client = Arc::new(
            LifecycleClient::new(vec![], vec![Ok(default_capabilities())])
                .with_capability_delay(Duration::from_millis(2)),
        );
        let mut delayed_config = settings(directory.path().join("delayed.sqlite3"));
        delayed_config.health_latency_threshold_ms = 0;
        let delayed_manager = Arc::new(IndexManager::new(delayed_client, delayed_config));
        let delayed_control = Arc::new(RecordingInventoryControl::default());
        let delayed_trait: Arc<dyn InventoryControl> = delayed_control.clone();
        insert_runtime_build(&delayed_manager, Arc::clone(&delayed_trait));
        let delayed_canceller = Arc::clone(&delayed_control);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            delayed_canceller.cancel();
        });
        let mut delayed_next_probe = Instant::now();
        let mut delayed_backoff = Duration::from_secs(1);
        assert!(
            !delayed_manager
                .wait_for_health(
                    &delayed_trait,
                    "S",
                    &mut delayed_next_probe,
                    &mut delayed_backoff,
                )
                .await
        );
        cancel_task.await.unwrap();

        let recovery_client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Err("temporary failure".into())],
        ));
        let recovery_manager = Arc::new(IndexManager::new(
            recovery_client,
            settings(directory.path().join("recovery.sqlite3")),
        ));
        let recovery_control = Arc::new(RecordingInventoryControl::default());
        let recovery_trait: Arc<dyn InventoryControl> = recovery_control.clone();
        insert_runtime_build(&recovery_manager, Arc::clone(&recovery_trait));
        let mut next_probe = Instant::now();
        let mut no_delay = Duration::ZERO;
        assert!(
            recovery_manager
                .wait_for_health(&recovery_trait, "S", &mut next_probe, &mut no_delay,)
                .await
        );
        assert!(recovery_control.resume_count.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn adaptive_hard_pause_recovers_without_waiting_for_an_inventory_slice() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("recovery.sqlite3"));
        config.adaptive = true;
        config.adaptive_recovery_delay_seconds = 0;
        config.adaptive_healthy_window_seconds = 1;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));
        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        let paused = controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        assert!(paused.paused);
        manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
        assert!(control.paused.load(Ordering::Acquire));

        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);
        tokio::time::timeout(
            Duration::from_secs(2),
            manager.wait_for_controller_recovery(&trait_control, "S", &mut controller),
        )
        .await
        .expect("controller did not recover")
        .expect("controller recovery pacing update failed")
        .then_some(())
        .expect("controller recovery was cancelled");
        assert!(!control.paused.load(Ordering::Acquire));
        assert!(control.resume_count.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn health_wait_rechecks_after_a_delayed_probe_becomes_due() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("delayed-health.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));
        manager.set_pause_overlay("S", None, Some(true));

        let mut next_probe = Instant::now() + Duration::from_millis(25);
        let mut backoff = Duration::from_secs(1);
        assert!(
            manager
                .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff,)
                .await
        );

        assert!(!manager.health_overlay_active("S"));
        assert_eq!(backoff, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn initial_pacing_failure_is_returned_and_recorded() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        control.fail_pacing_on_call(1);
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        ));
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("index.sqlite3")),
        ));

        let error = manager.refresh("S", true).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unable to apply initial inventory pacing")
        );
        assert!(control.is_cancelled());
        assert_eq!(
            manager.status("S").await.unwrap().last_error.as_deref(),
            Some("unable to apply initial inventory pacing: test pacing update failure")
        );
    }

    #[tokio::test]
    async fn runtime_pacing_failure_fails_the_active_generation() {
        let directory = tempdir().unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        control.fail_pacing_on_call(2);
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::from([
                    Ok(InventoryEvent::Slice(InventorySliceObservation {
                        sequence: 1,
                        backend: InventorySliceBackend::Da2,
                        nodes_returned: 1,
                        has_more: false,
                        native_operations: 1,
                        elapsed_ms: 1,
                        entries_seen: 1,
                        unique_items: 1,
                    })),
                    Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    })),
                ]),
                Arc::clone(&control),
            ))],
            vec![],
        ));
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.adaptive = true;
        let manager = Arc::new(IndexManager::new(client, config));

        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Failed).await;

        let status = manager.status("S").await.unwrap();
        assert_eq!(
            status.last_error.as_deref(),
            Some(
                "unable to update adaptive inventory pacing after slice 1: \
                 test pacing update failure"
            )
        );
        assert!(control.is_cancelled());
    }

    #[tokio::test]
    async fn build_readiness_failure_stops_the_loop_and_cancels_control() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("readiness-failure.sqlite3"));
        config.adaptive = true;
        config.adaptive_recovery_delay_seconds = 0;
        config.adaptive_healthy_window_seconds = 1;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        control.fail_pacing_on_call(1);
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        let _ownership = insert_runtime_build(&manager, Arc::clone(&trait_control));

        let started = Instant::now() - Duration::from_secs(2);
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        let paused = controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
        let mut state = BuildRunState::new(&manager.settings, Some(controller));
        let mut handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&trait_control),
        };

        let outcome = manager
            .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
            .await;

        match outcome {
            BuildLoopOutcome::Failed(error) => {
                assert!(error.contains("unable to update inventory pacing while recovering"));
            }
            BuildLoopOutcome::Finished => panic!("readiness failure unexpectedly finished"),
        }
        assert!(control.is_cancelled());
    }

    #[tokio::test]
    async fn build_loop_records_inventory_shutdown_failure() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("shutdown-failure.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));
        let mut state = BuildRunState::new(&manager.settings, None);
        let mut handle = InventoryHandle {
            stream: Box::new(CompletedThenShutdownErrorInventoryStream { emitted: false }),
            control: Arc::clone(&trait_control),
        };

        let outcome = manager
            .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
            .await;
        assert!(matches!(
            outcome,
            BuildLoopOutcome::Failed(error)
                if error.contains("inventory worker shutdown failed")
        ));
    }

    #[tokio::test]
    async fn build_loop_keeps_build_failure_when_shutdown_also_fails() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("shutdown-after-failure.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));
        let mut state = BuildRunState::new(&manager.settings, None);
        let mut handle = InventoryHandle {
            stream: Box::new(ErrorThenShutdownErrorInventoryStream { emitted: false }),
            control: Arc::clone(&trait_control),
        };

        let outcome = manager
            .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
            .await;
        assert!(matches!(
            outcome,
            BuildLoopOutcome::Failed(error) if error == "injected inventory build failure"
        ));
    }

    #[tokio::test]
    async fn build_readiness_reports_controller_recovery_cancellation() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("readiness-cancelled.sqlite3"));
        config.adaptive = true;
        config.adaptive_recovery_delay_seconds = 1;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));

        let mut controller =
            AdaptiveIndexController::new(manager.controller_config(), Instant::now());
        let paused = controller.observe(
            Instant::now(),
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
        let mut state = BuildRunState::new(&manager.settings, Some(controller));
        let cancel_control = Arc::clone(&control);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_control.cancel();
        });

        assert!(matches!(
            manager
                .wait_for_build_readiness(&trait_control, "S", &[], &mut state)
                .await,
            BuildReadiness::Cancelled
        ));
    }

    #[tokio::test]
    async fn manager_search_uses_cache_and_refresh_clears_it() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;

        let first = manager.search("S", "mock", 3, 1).await.unwrap();
        assert_eq!(first.matches.len(), 1);
        assert!(!first.has_more);
        manager
            .with_database(|db| {
                db.connection
                    .execute("DELETE FROM entries_fts WHERE server = 'S'", [])?;
                db.connection
                    .execute("DELETE FROM entries WHERE server = 'S'", [])?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            manager.search("S", "  MOCK  ", 3, 1).await.unwrap().matches,
            first.matches
        );
        manager.cache.lock().unwrap().clear_server("S");
        assert!(
            manager
                .search("S", "mock", 3, 1)
                .await
                .unwrap()
                .matches
                .is_empty()
        );

        assert!(manager.search("S", "   ", 3, 1).await.is_err());
        assert_eq!(manager.max_results(), 50);
    }

    #[tokio::test]
    async fn search_clamps_limit_and_reports_more_matches() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.max_results = 2;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries(
                    "S",
                    generation,
                    &[
                        inventory_entry("Alpha one", "Alpha.1"),
                        inventory_entry("Alpha two", "Alpha.2"),
                        inventory_entry("Alpha three", "Alpha.3"),
                    ],
                )?;
                db.promote("S", generation, &timestamp_now(), &zero_progress())
            })
            .unwrap();

        let result = manager.search("S", "alpha", 2, 99).await.unwrap();
        assert_eq!(result.matches.len(), 2);
        assert!(result.has_more);
    }

    #[tokio::test]
    async fn search_does_not_hold_database_lock_during_read_only_query() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
                db.promote("S", generation, &timestamp_now(), &zero_progress())
            })
            .unwrap();

        let (search_started, release_search) = manager.install_search_gate();
        let search_manager = Arc::clone(&manager);
        let search_task =
            tokio::spawn(async move { search_manager.search("S", "mock", 3, 10).await });
        tokio::time::timeout(Duration::from_secs(2), search_started)
            .await
            .unwrap()
            .unwrap();

        let status = tokio::time::timeout(Duration::from_secs(2), manager.status("S"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, IndexState::Ready);

        release_search.send(()).unwrap();
        let search = search_task.await.unwrap().unwrap();
        assert_eq!(search.matches.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_only_status_and_search_remain_responsive_while_writer_gate_is_held() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("read-only-gate.sqlite3")),
        ));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        let (locked, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::sync_channel(0);
        let gate_manager = Arc::clone(&manager);
        let gate_thread = std::thread::spawn(move || {
            let _writer_guard = gate_manager.writer_gate.lock().unwrap();
            locked.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();

        let status = tokio::time::timeout(Duration::from_secs(1), manager.status("S"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.active_generation, 1);
        let search = tokio::time::timeout(
            Duration::from_secs(1),
            manager.search("S", "persisted", 3, 10),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(search.matches.len(), 1);
        release.send(()).unwrap();
        gate_thread.join().unwrap();
    }

    #[tokio::test]
    async fn memory_database_search_uses_primary_connection() {
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        ));
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
                db.promote("S", generation, &timestamp_now(), &zero_progress())
            })
            .unwrap();

        let search = manager.search("S", "mock", 3, 10).await.unwrap();
        assert_eq!(search.matches.len(), 1);
        assert_eq!(search.status.state, IndexState::Ready);
    }

    #[tokio::test]
    async fn profile_change_invalidates_persisted_generation_and_cached_search() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );

        *client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            supports_browse_sessions: false,
            supports_search: true,
            max_page_size: 100,
        });
        client.inventory_events.lock().unwrap().extend([
            Ok(InventoryEvent::Entry(inventory_entry(
                "Replacement",
                "New.Tag",
            ))),
            Ok(InventoryEvent::Progress(InventoryProgress {
                branches_visited: 0,
                entries_seen: 1,
                unique_items: 1,
                active_time_ms: 1,
                paused_time_ms: 0,
                items_per_second: 1.0,
                estimated_remaining_ms: None,
            })),
            Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: true,
                cancelled: false,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Flat,
                source: BrowseSource::Flat,
            })),
        ]);

        manager.refresh_if_due("S").await;
        wait_for_build(&manager, IndexState::Ready).await;
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.organization, NamespaceOrganization::Flat);
        assert_eq!(status.source, BrowseSource::Flat);
        assert!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .is_empty()
        );
        assert_eq!(
            manager
                .search("S", "new", 2, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn active_profile_check_handles_missing_invalid_and_unavailable_data() {
        let directory = tempdir().unwrap();
        let missing = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("missing-profile.sqlite3")),
        );
        assert!(!missing.active_profile_changed("S").await.unwrap());

        let invalid = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("invalid-profile.sqlite3")),
        );
        invalid
            .with_database(|db| {
                db.connection
                    .execute_batch("DROP TABLE generations")
                    .unwrap();
                Ok(())
            })
            .unwrap();
        assert!(!invalid.active_profile_changed("S").await.unwrap());

        let unavailable_client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Err("unavailable".into()), Err("unavailable-refresh".into())],
        ));
        let unavailable = Arc::new(IndexManager::new(
            unavailable_client,
            settings(directory.path().join("unavailable-profile.sqlite3")),
        ));
        seed_active_generation(
            &unavailable,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        let error = unavailable
            .active_profile_changed("S")
            .await
            .expect_err("capability errors must be surfaced");
        assert!(error.to_string().contains("unavailable"));
        unavailable.refresh_if_due("S").await;

        let maintenance_client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da3,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 100,
            })],
        ));
        let mut maintenance_config = settings(directory.path().join("maintenance-profile.sqlite3"));
        maintenance_config.maintenance_windows = vec!["invalid".into()];
        let maintenance = Arc::new(IndexManager::new(maintenance_client, maintenance_config));
        seed_active_generation(
            &maintenance,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        maintenance.refresh_if_due("S").await;

        let mut initial_config = settings(directory.path().join("maintenance-initial.sqlite3"));
        initial_config.maintenance_windows = vec!["invalid".into()];
        let initial = IndexManager::new(Arc::new(MockOpcClient::default()), initial_config);
        let initial_status = initial.status("S").await.unwrap();
        assert!(!initial.automatic_refresh_allowed(&initial_status));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn active_profile_check_times_out_a_stalled_capability_probe() {
        let directory = tempdir().unwrap();
        let client = Arc::new(
            LifecycleClient::new(
                vec![],
                vec![Ok(BrowseCapabilities {
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                    supports_browse_sessions: true,
                    supports_search: true,
                    max_page_size: 100,
                })],
            )
            .with_capability_delay(Duration::from_secs(2)),
        );
        let mut config = settings(directory.path().join("profile-timeout.sqlite3"));
        config.operation_timeout_seconds = 1;
        let manager = Arc::new(IndexManager::new(client, config));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );

        let error = manager
            .active_profile_changed("S")
            .await
            .expect_err("stalled capability probes must be bounded");
        assert!(error.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn negotiated_da2_profile_does_not_trigger_profile_invalidation() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da3,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 100,
            })],
        ));
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("negotiated-da2.sqlite3")),
        ));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da3,
            &timestamp_now(),
        );
        manager
            .with_database(|db| {
                db.connection
                    .execute(
                        "UPDATE generations
                     SET source = 'da2', compatibility_fallback = 1
                     WHERE server = 'S' AND state = 'active'",
                        [],
                    )
                    .unwrap();
                Ok(())
            })
            .unwrap();
        let profile = manager
            .with_database(|db| {
                db.active_profile("S")?
                    .ok_or_else(|| anyhow::anyhow!("active profile missing"))
            })
            .unwrap();
        assert_eq!(profile.source, BrowseSource::Da2);
        assert!(profile.compatibility_fallback);

        manager.refresh_if_due("S").await;

        assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn genuine_da2_profile_triggers_da3_invalidation() {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da3,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 100,
            })],
        ));
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("genuine-da2.sqlite3")),
        ));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );

        manager.refresh_if_due("S").await;

        assert_eq!(manager.status("S").await.unwrap().active_generation, 0);
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn automatic_refresh_logs_reachable_invalidation_and_refresh_failures() {
        let directory = tempdir().unwrap();

        let clear_client = Arc::new(MockOpcClient::default());
        *clear_client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            supports_browse_sessions: false,
            supports_search: true,
            max_page_size: 100,
        });
        let clear_manager = Arc::new(IndexManager::new(
            Arc::clone(&clear_client),
            settings(directory.path().join("clear.sqlite3")),
        ));
        seed_active_generation(
            &clear_manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        clear_manager
            .with_database(|db| {
                db.connection.execute_batch(
                    "CREATE TRIGGER fail_clear
                     BEFORE DELETE ON generations
                     BEGIN
                       SELECT RAISE(FAIL, 'clear failed');
                     END;",
                )?;
                Ok(())
            })
            .unwrap();
        clear_manager.refresh_if_due("S").await;
        assert_eq!(
            clear_manager.status("S").await.unwrap().active_generation,
            1
        );
        assert_eq!(
            clear_client.inventory_start_count.load(Ordering::Relaxed),
            0
        );

        let rebuild_client = Arc::new(LifecycleClient::new(
            vec![Err("rebuild start failed".into())],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Flat,
                source: BrowseSource::Flat,
                supports_browse_sessions: false,
                supports_search: true,
                max_page_size: 100,
            })],
        ));
        let rebuild_manager = Arc::new(IndexManager::new(
            Arc::clone(&rebuild_client),
            settings(directory.path().join("rebuild.sqlite3")),
        ));
        seed_active_generation(
            &rebuild_manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        );
        rebuild_manager.refresh_if_due("S").await;
        assert_eq!(
            rebuild_client.inventory_start_count.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            rebuild_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .as_deref(),
            Some("rebuild start failed")
        );

        let stale_client = Arc::new(LifecycleClient::new(
            vec![Err("stale refresh failed".into())],
            vec![Ok(default_capabilities())],
        ));
        let stale_manager = Arc::new(IndexManager::new(
            Arc::clone(&stale_client),
            settings(directory.path().join("stale.sqlite3")),
        ));
        seed_active_generation(
            &stale_manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "0",
        );
        stale_manager.refresh_if_due("S").await;
        assert_eq!(
            stale_client.inventory_start_count.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            stale_manager
                .status("S")
                .await
                .unwrap()
                .last_error
                .as_deref(),
            Some("stale refresh failed")
        );
    }

    #[tokio::test]
    async fn background_refresh_skips_a_fresh_persisted_generation() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| {
                let generation = db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    &timestamp_now(),
                )?;
                db.insert_entries(
                    "S",
                    generation,
                    &[InventoryEntry {
                        display_name: "Persisted".into(),
                        item_id: "persisted".into(),
                        kind: InventoryNodeKind::Item,
                        breadcrumbs: vec![],
                    }],
                )?;
                db.promote(
                    "S",
                    generation,
                    &timestamp_now(),
                    &InventoryProgress {
                        branches_visited: 1,
                        entries_seen: 1,
                        unique_items: 1,
                        active_time_ms: 1,
                        paused_time_ms: 0,
                        items_per_second: 1.0,
                        estimated_remaining_ms: None,
                    },
                )
            })
            .unwrap();
        manager.refresh_if_due("S").await;
        assert_eq!(
            client
                .inventory_start_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn background_refresh_rebuilds_a_stale_persisted_generation() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager
            .with_database(|db| {
                let generation = db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )?;
                db.insert_entries("S", generation, &[inventory_entry("Old", "Old.Tag")])?;
                db.promote("S", generation, "0", &zero_progress())
            })
            .unwrap();

        manager.refresh_if_due("S").await;
        wait_for_build(&manager, IndexState::Ready).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
        assert!(
            manager
                .search("S", "old", 3, 10)
                .await
                .unwrap()
                .matches
                .is_empty()
        );
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn manager_reports_unenrolled_servers_without_scanning() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        let status = manager.status("Other").await.unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(!status.auto_refresh_enabled);
        let response = manager.search("Other", "tag", 3, 10).await.unwrap();
        assert!(response.matches.is_empty());
        assert_eq!(response.status.state, IndexState::NotIndexed);

        let unindexed = manager.search("S", "tag", 3, 0).await.unwrap();
        assert!(unindexed.matches.is_empty());
        assert!(!unindexed.status.auto_refresh_enabled);

        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries(
                    "S",
                    generation,
                    &[inventory_entry("Staging tag", "Staging.Tag")],
                )
            })
            .unwrap();
        let staging = manager.search("S", "staging", 2, 10).await.unwrap();
        assert_eq!(staging.matches.len(), 1);
        assert_eq!(staging.status.state, IndexState::Partial);
        assert!(manager.cache.lock().unwrap().values.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_controls_reject_unenrolled_servers() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("unenrolled-controls.sqlite3")),
        ));

        assert!(matches!(
            manager
                .control("S", IndexControlAction::EnableAutoRefresh)
                .await,
            Err(IndexOperationError::NotEnrolled { server }) if server == "S"
        ));
        assert!(matches!(
            manager
                .control("S", IndexControlAction::Pause)
                .await,
            Err(IndexOperationError::NotEnrolled { server }) if server == "S"
        ));

        let deleted = manager
            .control("S", IndexControlAction::Delete)
            .await
            .unwrap();
        assert_eq!(deleted.state, IndexState::NotIndexed);
    }

    #[tokio::test]
    async fn refresh_rejects_a_server_marked_for_deletion() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("deleting.sqlite3")),
        ));
        manager.deleting.lock().unwrap().insert("S".into());

        assert!(matches!(
            manager.refresh("S", true).await,
            Err(IndexOperationError::Deleting { server }) if server == "S"
        ));
    }

    #[tokio::test]
    async fn all_lifecycle_controls_reject_a_server_marked_for_deletion() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("deleting-controls.sqlite3")),
        ));
        manager.deleting.lock().unwrap().insert("S".into());

        for action in [
            IndexControlAction::Pause,
            IndexControlAction::Resume,
            IndexControlAction::Cancel,
            IndexControlAction::EnableAutoRefresh,
            IndexControlAction::DisableAutoRefresh,
            IndexControlAction::Delete,
        ] {
            assert!(matches!(
                manager.control("S", action).await,
                Err(IndexOperationError::Deleting { server }) if server == "S"
            ));
        }
    }

    #[tokio::test]
    async fn search_does_not_return_stale_matches_while_deletion_is_active() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("deleting-search.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;

        manager.deleting.lock().unwrap().insert("S".into());

        let result = manager.search("S", "mock", 3, 10).await.unwrap();
        assert!(result.matches.is_empty());
        assert_eq!(result.status.state, IndexState::Deleting);
    }

    #[tokio::test]
    async fn search_discards_matches_if_deletion_starts_during_query() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("deleting-during-search.sqlite3")),
        ));
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
                db.promote("S", generation, &timestamp_now(), &zero_progress())
            })
            .unwrap();

        let (search_started, release_search) = manager.install_search_gate();
        let search_manager = Arc::clone(&manager);
        let search_task =
            tokio::spawn(async move { search_manager.search("S", "mock", 3, 10).await });
        tokio::time::timeout(Duration::from_secs(2), search_started)
            .await
            .unwrap()
            .unwrap();

        manager.deleting.lock().unwrap().insert("S".into());
        release_search.send(()).unwrap();

        let result = search_task.await.unwrap().unwrap();
        assert!(result.matches.is_empty());
        assert_eq!(result.status.state, IndexState::Deleting);
    }

    #[tokio::test]
    async fn cancellation_and_deletion_wait_helpers_cover_active_builds() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("active-build-helpers.sqlite3")),
        ));
        let recording = Arc::new(RecordingInventoryControl::default());
        let control: Arc<dyn InventoryControl> = recording.clone();
        insert_runtime_build(&manager, control);

        manager
            .apply_control_action("S", IndexControlAction::EnableAutoRefresh)
            .unwrap();
        manager
            .apply_control_action("S", IndexControlAction::DisableAutoRefresh)
            .unwrap();
        manager
            .apply_control_action("S", IndexControlAction::Delete)
            .unwrap();
        manager.cancel_active_build("S").unwrap();
        assert!(recording.cancelled.load(Ordering::Acquire));

        let wait_manager = Arc::clone(&manager);
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            wait_manager.active_builds.lock().unwrap().remove("S");
            wait_manager.build_changed.notify_waiters();
        });
        manager.wait_for_build_to_finish("S").await;
        release.await.unwrap();
    }

    #[tokio::test]
    async fn deletion_waits_for_an_external_build_lock() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("delete-lock.sqlite3");
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(database.clone()),
        );
        let held_lock = BuildFileLock::acquire(&database, "S").unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop(held_lock);
        });

        let delete_lock = manager.acquire_delete_lock("S").await.unwrap();
        release.await.unwrap();
        drop(delete_lock);
    }

    #[tokio::test]
    async fn manual_refresh_enrolls_only_listed_servers() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        *client.list_servers_result.lock().unwrap() = Ok(vec!["Actual.Server".into()]);
        let manager = Arc::new(IndexManager::new(
            Arc::clone(&client),
            settings(directory.path().join("index.sqlite3")),
        ));

        let error = manager.refresh("Typo.Server", true).await.unwrap_err();
        assert!(matches!(
            error,
            IndexOperationError::UnknownServer { ref server } if server == "Typo.Server"
        ));
        assert!(
            manager
                .with_database(|db| db.enrollment("Typo.Server"))
                .unwrap()
                .is_none()
        );

        manager.refresh("Actual.Server", true).await.unwrap();
        wait_for_state(&manager, "Actual.Server", IndexState::Ready).await;
        assert!(
            manager
                .status("Actual.Server")
                .await
                .unwrap()
                .auto_refresh_enabled
        );
    }

    #[tokio::test]
    async fn disabled_global_scheduler_does_not_block_manual_refresh_or_search() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.enabled = false;
        let manager = Arc::new(IndexManager::new(client, config));

        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn auto_refresh_can_be_disabled_without_deleting_searchable_data() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));

        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        manager
            .control("S", IndexControlAction::DisableAutoRefresh)
            .await
            .unwrap();
        let status = manager.status("S").await.unwrap();
        assert!(!status.auto_refresh_enabled);
        assert!(status.scheduler.next_refresh_at.is_none());
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );

        manager
            .control("S", IndexControlAction::EnableAutoRefresh)
            .await
            .unwrap();
        assert!(manager.status("S").await.unwrap().auto_refresh_enabled);
    }

    #[tokio::test]
    async fn delete_index_removes_enrollment_generations_and_retry_metadata() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        manager
            .with_database(|db| {
                db.set_retry_state(
                    "S",
                    Some(SystemTime::now() + Duration::from_secs(60)),
                    2,
                    true,
                )
            })
            .unwrap();

        let status = manager
            .control("S", IndexControlAction::Delete)
            .await
            .unwrap();
        assert_eq!(status.state, IndexState::Deleting);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.status("S").await.unwrap().state == IndexState::NotIndexed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(!status.auto_refresh_enabled);
        manager
            .with_database(|db| {
                assert!(db.enrollment("S")?.is_none());
                assert!(db.status_rows("S")?.is_empty());
                assert_eq!(db.retry_state("S")?, (None, 0, false));
                Ok(())
            })
            .unwrap();
    }

    #[tokio::test]
    async fn duplicate_delete_is_rejected_while_cleanup_is_in_progress() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("duplicate-delete.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(database.clone()),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        let held_lock = BuildFileLock::acquire(&database, "S").unwrap();

        let status = manager
            .control("S", IndexControlAction::Delete)
            .await
            .unwrap();
        assert_eq!(status.state, IndexState::Deleting);
        assert!(matches!(
            manager.control("S", IndexControlAction::Delete).await,
            Err(IndexOperationError::Deleting { server }) if server == "S"
        ));

        drop(held_lock);
        manager.background_tasks.wait_for_idle().await;
    }

    #[tokio::test]
    async fn failed_background_delete_is_recorded_in_status() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("failed-delete.sqlite3");
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(database.clone()),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        let held_lock = BuildFileLock::acquire(&database, "S").unwrap();

        let status = manager
            .control("S", IndexControlAction::Delete)
            .await
            .unwrap();
        assert_eq!(status.state, IndexState::Deleting);
        manager
            .with_database(|db| {
                db.connection.execute_batch("DROP TABLE entries_fts")?;
                Ok(())
            })
            .unwrap();

        drop(held_lock);
        manager.background_tasks.wait_for_idle().await;
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Failed);
        assert!(!status.auto_refresh_enabled);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("entries_fts"))
        );
    }

    #[tokio::test]
    async fn delete_index_reports_shutdown_when_background_task_cannot_start() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        manager.shutdown_background_indexing().await;

        let error = manager
            .control("S", IndexControlAction::Delete)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("gateway is shutting down"));
        assert_eq!(manager.status("S").await.unwrap().state, IndexState::Ready);
    }

    #[test]
    fn build_file_lock_reports_owner_and_probe_errors() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let lock = BuildFileLock::acquire(&database, "S").unwrap();
        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("process_id="));
        assert!(error.to_string().contains("server=S"));
        drop(lock);

        let lock_path = build_lock_path(&database, "S");
        fs::write(&lock_path, "external test owner\n").unwrap();
        #[cfg(unix)]
        {
            use std::io::BufRead;
            use std::process::{Command, Stdio};

            let mut child = Command::new("flock")
                .arg("-x")
                .arg(&lock_path)
                .arg("sh")
                .arg("-c")
                .arg("echo ready; read line")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut ready = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready, "ready\n");

            let error = BuildFileLock::acquire(&database, "S").unwrap_err();
            assert!(error.to_string().contains("external test owner"));
            assert!(BuildFileLock::is_held(&database, "S").unwrap());
            drop(child.stdin.take());
            child.wait().unwrap();

            fs::write(&lock_path, "").unwrap();
            let mut child = Command::new("flock")
                .arg("-x")
                .arg(&lock_path)
                .arg("sh")
                .arg("-c")
                .arg("echo ready; read line")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut ready = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut ready)
                .unwrap();
            assert_eq!(ready, "ready\n");
            let error = BuildFileLock::acquire(&database, "S").unwrap_err();
            assert!(error.to_string().contains("build lock is already held"));
            drop(child.stdin.take());
            child.wait().unwrap();
        }

        fs::remove_file(&lock_path).unwrap();
        fs::create_dir(&lock_path).unwrap();
        assert!(BuildFileLock::is_held(&database, "S").is_err());
        assert!(BuildFileLock::acquire(&database, "S").is_err());
    }

    #[tokio::test]
    async fn persisted_retry_circuit_state_blocks_restart_until_forced_refresh() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("retry.sqlite3");
        let mut config = settings(path.clone());
        config.circuit_failure_threshold = 1;
        let failing = Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(
                vec![Err("start failed".into())],
                vec![],
            )),
            config.clone(),
        ));
        assert!(failing.refresh("S", true).await.is_err());
        failing.background_tasks.wait_for_idle().await;
        drop(failing);

        let client = Arc::new(MockOpcClient::default());
        let restarted = Arc::new(IndexManager::new(Arc::clone(&client), config));
        let blocked = restarted.refresh("S", false).await.unwrap();
        assert_eq!(blocked.state, IndexState::Failed);
        assert_eq!(blocked.scheduler.consecutive_failures, 1);
        assert!(blocked.scheduler.circuit_open);
        assert!(blocked.scheduler.retry_after.is_some());
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
        assert!(
            restarted
                .with_database(|db| db.scheduled_servers())
                .unwrap()
                .is_empty()
        );

        restarted.refresh("S", true).await.unwrap();
        wait_for_build(&restarted, IndexState::Ready).await;
        let recovered = restarted.status("S").await.unwrap();
        assert_eq!(recovered.scheduler.consecutive_failures, 0);
        assert!(!recovered.scheduler.circuit_open);
        assert!(recovered.scheduler.retry_after.is_none());
    }

    #[tokio::test]
    async fn startup_grace_and_manual_policy_prevent_automatic_first_builds() {
        let directory = tempdir().unwrap();
        let grace_client = Arc::new(MockOpcClient::default());
        let mut grace_config = settings(directory.path().join("grace.sqlite3"));
        grace_config.startup_grace_period_seconds = 60;
        let grace_manager = Arc::new(IndexManager::new(Arc::clone(&grace_client), grace_config));
        grace_manager.start_background_indexing();
        tokio::task::yield_now().await;
        grace_manager.shutdown_background_indexing().await;
        assert_eq!(
            grace_client.inventory_start_count.load(Ordering::Relaxed),
            0
        );

        let manual_client = Arc::new(MockOpcClient::default());
        let manual_manager = Arc::new(IndexManager::new(
            Arc::clone(&manual_client),
            settings(directory.path().join("manual.sqlite3")),
        ));
        manual_manager.refresh_if_due("S").await;
        assert_eq!(
            manual_client.inventory_start_count.load(Ordering::Relaxed),
            0
        );
        manual_manager.start_background_indexing();
        assert_eq!(
            manual_manager.background_refresh_delay("S").await,
            Duration::from_secs(3600)
        );
        manual_manager.shutdown_background_indexing().await;
    }

    #[tokio::test]
    async fn disk_guard_and_sentinel_health_paths_are_reported() {
        let directory = tempdir().unwrap();
        let mut disk_config = settings(directory.path().join("disk.sqlite3"));
        disk_config.minimum_free_space_bytes = u64::MAX;
        let disk_manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            disk_config,
        ));
        let error = disk_manager.refresh("S", true).await.unwrap_err();
        assert!(error.to_string().contains("insufficient free space"));
        assert!(
            disk_manager
                .controller_observation("S", false)
                .insufficient_disk_space
        );

        let client = Arc::new(MockOpcClient::default());
        *client.read_tag_values_result.lock().unwrap() = Ok(vec![TagValue {
            tag_id: "Health.PV".into(),
            value: "1".into(),
            quality: "Bad".into(),
            timestamp: "0".into(),
        }]);
        let mut sentinel_config = settings(directory.path().join("sentinel.sqlite3"));
        sentinel_config.sentinel_tag = Some("Health.PV".into());
        let sentinel_manager = Arc::new(IndexManager::new(client, sentinel_config));
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel_on_pause();
        let trait_control: Arc<dyn InventoryControl> = control;
        insert_runtime_build(&sentinel_manager, Arc::clone(&trait_control));
        let mut next_probe = Instant::now();
        let mut backoff = Duration::from_secs(1);
        assert!(
            !sentinel_manager
                .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff)
                .await
        );
        assert_eq!(
            sentinel_manager.status("S").await.unwrap().health,
            HealthProbeState::Unhealthy
        );
    }

    #[tokio::test]
    async fn sentinel_read_errors_are_reported_as_unhealthy() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        *client.read_tag_values_result.lock().unwrap() = Err("sentinel transport failed".into());
        let mut config = settings(directory.path().join("sentinel-error.sqlite3"));
        config.sentinel_tag = Some("Health.PV".into());
        let manager = Arc::new(IndexManager::new(client, config));
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel_on_pause();
        let trait_control: Arc<dyn InventoryControl> = control.clone();
        insert_runtime_build(&manager, Arc::clone(&trait_control));

        let mut next_probe = Instant::now();
        let mut backoff = Duration::ZERO;
        assert!(
            !manager
                .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff)
                .await
        );
        assert_eq!(
            manager.status("S").await.unwrap().health,
            HealthProbeState::Unhealthy
        );
    }

    #[tokio::test]
    async fn controller_recovery_pacing_failure_is_returned() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("recovery-failure.sqlite3"));
        config.adaptive = true;
        config.adaptive_recovery_delay_seconds = 0;
        config.adaptive_healthy_window_seconds = 1;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        control.fail_pacing_on_call(1);
        let trait_control: Arc<dyn InventoryControl> = control;
        insert_runtime_build(&manager, Arc::clone(&trait_control));

        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        let paused = controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
        let error = manager
            .wait_for_controller_recovery(&trait_control, "S", &mut controller)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unable to update inventory pacing while recovering")
        );
    }

    #[test]
    fn helper_edge_cases_cover_window_time_and_retry_rollback() {
        let now = Instant::now();
        let mut metrics = ForegroundMetricState::default();
        for latency in 0..129 {
            metrics.record_health_at(now, latency, false, false, false);
        }
        assert_eq!(metrics.latencies_ms.len(), 128);
        assert_eq!(metrics.latencies_ms.front(), Some(&1));
        assert_eq!(percentile(&[], 50), None);
        assert!(!instant_timestamp(Instant::now()).is_empty());
        assert!(deterministic_jitter("S", u64::MAX) <= Duration::from_secs(u64::MAX));

        let directory = tempdir().unwrap();
        let db = IndexDb::open(&directory.path().join("retry-rollback.sqlite3")).unwrap();
        db.connection
            .execute_batch(
                "CREATE TRIGGER reject_retry_state
                 BEFORE INSERT ON index_meta
                 WHEN NEW.key = 'failures:S'
                 BEGIN
                   SELECT RAISE(FAIL, 'retry state rejected');
                 END;",
            )
            .unwrap();
        assert!(
            db.set_retry_state("S", Some(SystemTime::now()), 1, false)
                .unwrap_err()
                .to_string()
                .contains("retry state rejected")
        );
        assert_eq!(db.retry_state("S").unwrap(), (None, 0, false));
    }

    #[tokio::test]
    async fn scheduler_delay_covers_retry_terminal_and_maintenance_states() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("scheduler.sqlite3")),
        ));
        manager
            .runtime
            .lock()
            .unwrap()
            .entry("S".into())
            .or_default()
            .retry_after = Some(SystemTime::now() + Duration::from_secs(2));
        assert!(manager.background_refresh_delay("S").await >= Duration::from_secs(1));
        manager.runtime.lock().unwrap().clear();

        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, control);
        manager.mark_promoting("S").unwrap();
        assert_eq!(
            manager.background_refresh_delay("S").await,
            Duration::from_secs(1)
        );
        manager.clear_promoting("S");
        manager.runtime.lock().unwrap().clear();
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
                db.fail_generation("S", generation, "failed")?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            manager.background_refresh_delay("S").await,
            retry_delay("S", 1, false, 300)
        );

        let mut maintenance = settings(directory.path().join("maintenance-delay.sqlite3"));
        maintenance.maintenance_windows = vec!["00:00-00:00".into()];
        let maintenance = IndexManager::new(Arc::new(MockOpcClient::default()), maintenance);
        assert_eq!(
            maintenance.background_refresh_delay("S").await,
            Duration::from_secs(3600)
        );
    }

    #[tokio::test]
    async fn status_promotion_read_failure_and_health_variants_are_safe() {
        let directory = tempdir().unwrap();
        let mut promotion_config = settings(directory.path().to_path_buf());
        promotion_config.database_path = directory.path().to_path_buf();
        let promotion = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            promotion_config,
        ));
        let promotion_control: Arc<dyn InventoryControl> =
            Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&promotion, promotion_control);
        promotion.mark_promoting("S").unwrap();
        let status = promotion.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Promoting);
        assert!(status.last_error.is_some());

        let non_promoting = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().to_path_buf()),
        );
        assert!(non_promoting.status("S").await.is_err());

        let client = Arc::new(MockOpcClient::default());
        *client.read_tag_values_result.lock().unwrap() = Ok(Vec::new());
        let mut config = settings(directory.path().join("health.sqlite3"));
        config.sentinel_tag = Some("Health.PV".into());
        let manager = Arc::new(IndexManager::new(client, config));
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel_on_pause();
        let control: Arc<dyn InventoryControl> = control;
        insert_runtime_build(&manager, Arc::clone(&control));
        let mut next_probe = Instant::now();
        let mut backoff = Duration::from_secs(1);
        assert!(
            !manager
                .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
                .await
        );
        assert_eq!(
            manager.status("S").await.unwrap().health,
            HealthProbeState::Unhealthy
        );

        let good_client = Arc::new(MockOpcClient::default());
        *good_client.read_tag_values_result.lock().unwrap() = Ok(vec![TagValue {
            tag_id: "Health.PV".into(),
            value: "1".into(),
            quality: "Good".into(),
            timestamp: "0".into(),
        }]);
        let mut good_config = settings(directory.path().join("good-health.sqlite3"));
        good_config.sentinel_tag = Some("Health.PV".into());
        let good = Arc::new(IndexManager::new(good_client, good_config));
        let good_control: Arc<dyn InventoryControl> =
            Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&good, Arc::clone(&good_control));
        let mut next_probe = Instant::now();
        let mut backoff = Duration::from_secs(1);
        assert!(
            good.wait_for_health(&good_control, "S", &mut next_probe, &mut backoff)
                .await
        );
        assert_eq!(
            good.status("S").await.unwrap().health,
            HealthProbeState::Healthy
        );
    }

    #[test]
    fn public_metric_and_invalid_maintenance_helpers_are_exercised() {
        struct FixedHostMetrics;

        impl HostMetricsProvider for FixedHostMetrics {
            fn snapshot(&self) -> HostMetrics {
                HostMetrics {
                    cpu_percent: Some(1.0),
                    available_memory_percent: Some(99.0),
                    disk_active_percent: Some(2.0),
                    disk_queue: Some(0.0),
                    ..HostMetrics::default()
                }
            }

            fn latest(&self) -> HostMetrics {
                self.snapshot()
            }
        }

        let fixed = FixedHostMetrics;
        assert_eq!(fixed.latest(), fixed.snapshot());

        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        )
        .with_host_metrics_provider(Arc::new(fixed));
        manager.record_foreground_operation("S", Duration::from_millis(2), true, true);
        let observation = manager.controller_observation("S", false);
        assert!(observation.foreground_error);
        assert!(observation.foreground_bad_quality);
        assert_eq!(observation.host_cpu_percent, Some(1.0));

        let mut invalid = settings(PathBuf::from(":memory:"));
        invalid.maintenance_windows = vec!["not-a-window".into()];
        let invalid = IndexManager::new(Arc::new(MockOpcClient::default()), invalid);
        assert!(!invalid.maintenance_window_is_open());
    }

    #[tokio::test]
    async fn stale_maintenance_delay_and_promotion_search_are_bounded() {
        let directory = tempdir().unwrap();
        let now = Local::now();
        let minute = (now.hour() * 60 + now.minute()) as u16;
        let window = format!(
            "{:02}:{:02}-{:02}:{:02}",
            ((minute + 2) % 1440) / 60,
            ((minute + 2) % 1440) % 60,
            ((minute + 3) % 1440) / 60,
            ((minute + 3) % 1440) % 60
        );
        let mut config = settings(directory.path().join("stale-maintenance.sqlite3"));
        config.maintenance_windows = vec![window];
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "0",
        );
        assert_eq!(
            manager.background_refresh_delay("S").await,
            Duration::from_secs(60)
        );

        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, control);
        manager.mark_promoting("S").unwrap();
        let result = manager.search("S", "persisted", 3, 10).await.unwrap();
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.status.state, IndexState::Promoting);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_cancellation_covers_startup_failure_boundaries() {
        let directory = tempdir().unwrap();

        let pacing_control = Arc::new(RecordingInventoryControl::default());
        pacing_control.fail_pacing_on_call(1);
        let pacing_started = Arc::new(Notify::new());
        let pacing_release = Arc::new(Notify::new());
        let pacing_manager = Arc::new(IndexManager::new(
            Arc::new(
                LifecycleClient::new(
                    vec![Ok(handle_with_control(
                        VecDeque::new(),
                        Arc::clone(&pacing_control),
                    ))],
                    vec![],
                )
                .with_inventory_gate(Arc::clone(&pacing_started), Arc::clone(&pacing_release)),
            ),
            settings(directory.path().join("pacing-cancel.sqlite3")),
        ));
        let refresh_manager = Arc::clone(&pacing_manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        pacing_started.notified().await;
        pacing_manager
            .control("S", IndexControlAction::Cancel)
            .await
            .unwrap();
        pacing_release.notify_one();
        assert_eq!(
            refresh.await.unwrap().unwrap().state,
            IndexState::NotIndexed
        );

        let capability_control = Arc::new(RecordingInventoryControl::default());
        let capability_started = Arc::new(Notify::new());
        let capability_release = Arc::new(Notify::new());
        let capability_manager = Arc::new(IndexManager::new(
            Arc::new(
                LifecycleClient::new(
                    vec![Ok(handle_with_control(
                        VecDeque::new(),
                        Arc::clone(&capability_control),
                    ))],
                    vec![Err("capability failure".into())],
                )
                .with_capability_gate(
                    Arc::clone(&capability_started),
                    Arc::clone(&capability_release),
                ),
            ),
            settings(directory.path().join("capability-cancel.sqlite3")),
        ));
        let refresh_manager = Arc::clone(&capability_manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        capability_started.notified().await;
        capability_control.cancel();
        capability_release.notify_one();
        assert_eq!(
            refresh.await.unwrap().unwrap().state,
            IndexState::NotIndexed
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_treats_cancelled_inventory_start_failure_as_noop() {
        let directory = tempdir().unwrap();
        let inventory_started = Arc::new(Notify::new());
        let inventory_release = Arc::new(Notify::new());
        let client = Arc::new(
            LifecycleClient::new(vec![Err("inventory failure".into())], vec![])
                .with_inventory_gate(
                    Arc::clone(&inventory_started),
                    Arc::clone(&inventory_release),
                ),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(directory.path().join("inventory-cancel.sqlite3")),
        ));

        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        inventory_started.notified().await;
        assert_eq!(
            manager
                .control("S", IndexControlAction::Cancel)
                .await
                .unwrap()
                .state,
            IndexState::Partial
        );
        inventory_release.notify_one();

        assert_eq!(
            refresh.await.unwrap().unwrap().state,
            IndexState::NotIndexed
        );
        assert!(manager.pending_cancels.lock().unwrap().is_empty());
        assert!(build_lock_path(&directory.path().join("inventory-cancel.sqlite3"), "S").exists());
    }

    #[tokio::test]
    async fn attach_control_handles_pending_cancel_after_build_disappears() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("attach-cancel.sqlite3")),
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
        manager.pending_cancels.lock().unwrap().insert("S".into());
        let handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&control),
        };

        assert!(
            manager
                .attach_refresh_control(
                    "S",
                    &ownership,
                    &handle,
                    manager.initial_inventory_limits(),
                )
                .unwrap()
                .is_none()
        );
        assert!(manager.pending_cancels.lock().unwrap().is_empty());
        assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
        assert!(manager.active_builds.lock().unwrap().is_empty());

        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("attach-cancel-control.sqlite3")),
        ));
        let control_impl = Arc::new(RecordingInventoryControl::default());
        control_impl.cancel_on_pacing();
        let control: Arc<dyn InventoryControl> = control_impl;
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
        let handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&control),
        };
        assert!(
            manager
                .attach_refresh_control(
                    "S",
                    &ownership,
                    &handle,
                    manager.initial_inventory_limits(),
                )
                .unwrap()
                .is_none()
        );

        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("attach-error.sqlite3")),
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
        let handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&control),
        };
        let error = manager
            .attach_refresh_control("S", &ownership, &handle, manager.initial_inventory_limits())
            .unwrap_err();
        assert_eq!(error.to_string(), "index build disappeared before start");
        assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
        assert!(manager.active_builds.lock().unwrap().is_empty());
    }

    #[test]
    fn start_generation_stops_when_a_cancelled_control_loses_its_build() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(
                vec![],
                vec![Ok(default_capabilities())],
            )),
            settings(directory.path().join("generation-cancel.sqlite3")),
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel();
        let control: Arc<dyn InventoryControl> = control;
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
        let generation = manager
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&control),
        };

        assert!(
            manager
                .launch_refresh_build("S", generation, handle, ownership, false)
                .is_ok()
        );
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
        );
        assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
        assert!(manager.active_builds.lock().unwrap().is_empty());

        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("generation-mismatch.sqlite3")),
        ));
        let wrong_control: Arc<dyn InventoryControl> =
            Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&manager, Arc::clone(&wrong_control));
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel();
        let control: Arc<dyn InventoryControl> = control;
        manager
            .runtime
            .lock()
            .unwrap()
            .get_mut("S")
            .unwrap()
            .build
            .as_mut()
            .unwrap()
            .control = Some(Arc::clone(&wrong_control));
        let generation = manager
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let handle = InventoryHandle {
            stream: Box::new(VecInventoryStream {
                events: VecDeque::new(),
            }),
            control: Arc::clone(&control),
        };
        assert!(
            manager
                .launch_refresh_build("S", generation, handle, ownership, false)
                .is_ok()
        );
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn control_action_helpers_reconcile_resume_and_queue_cancel() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("control-actions.sqlite3")),
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&control));
        {
            let mut runtime = manager.runtime.lock().unwrap();
            let build = runtime
                .get_mut("S")
                .and_then(|state| state.build.as_mut())
                .unwrap();
            build.operator_paused = true;
            build.foreground_users = 0;
            build.quiet_until = Some(Instant::now() - Duration::from_secs(1));
        }

        manager
            .apply_control_action("S", IndexControlAction::Resume)
            .unwrap();
        {
            let runtime = manager.runtime.lock().unwrap();
            let build = runtime
                .get("S")
                .and_then(|state| state.build.as_ref())
                .unwrap();
            assert!(!build.operator_paused);
            assert!(build.quiet_until.is_none());
        }

        manager
            .runtime
            .lock()
            .unwrap()
            .get_mut("S")
            .unwrap()
            .build
            .as_mut()
            .unwrap()
            .control = None;
        manager
            .apply_control_action("S", IndexControlAction::Cancel)
            .unwrap();
        assert!(manager.pending_cancels.lock().unwrap().contains("S"));
    }

    #[tokio::test]
    async fn health_and_recovery_cancellation_edges_are_bounded() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("health-wait.sqlite3"));
        config.sentinel_tag = Some("Health.PV".into());
        config.sentinel_probe_interval_seconds = 3_600;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&control));
        manager
            .runtime
            .lock()
            .unwrap()
            .get_mut("S")
            .unwrap()
            .sentinel_checked_at = Some(Instant::now());
        let mut next_probe = Instant::now() + Duration::from_secs(60);
        let mut backoff = Duration::from_secs(1);
        assert!(
            manager
                .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
                .await
        );

        struct CancelOnSecondPoll(AtomicUsize);

        impl InventoryControl for CancelOnSecondPoll {
            fn pause(&self) {}

            fn resume(&self) {}

            fn cancel(&self) {}

            fn is_cancelled(&self) -> bool {
                self.0.fetch_add(1, Ordering::AcqRel) > 0
            }
        }

        let mut recovery_config = settings(directory.path().join("recovery-cancel.sqlite3"));
        recovery_config.adaptive = true;
        let recovery = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            recovery_config,
        ));
        let recovery_impl = CancelOnSecondPoll(AtomicUsize::new(0));
        recovery_impl.pause();
        recovery_impl.resume();
        recovery_impl.cancel();
        let recovery_control: Arc<dyn InventoryControl> = Arc::new(recovery_impl);
        insert_runtime_build(&recovery, Arc::clone(&recovery_control));
        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(recovery.controller_config(), started);
        controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        assert!(
            !recovery
                .wait_for_controller_recovery(&recovery_control, "S", &mut controller)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn poisoned_guards_and_empty_promoting_search_fail_safely() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("poisoned-guards.sqlite3")),
        ));
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let overlays = Arc::clone(&manager.pause_overlays);
            let _ = std::panic::catch_unwind(move || {
                let _guard = overlays.lock().unwrap();
                panic!("poison pause overlays");
            });
            manager.set_pause_overlay("S", Some(true), None);
            manager.clear_pause_overlays("S");
            manager.reconcile_pause_state("S");

            let pending = Arc::clone(&manager.pending_cancels);
            let _ = std::panic::catch_unwind(move || {
                let _guard = pending.lock().unwrap();
                panic!("poison pending cancellations");
            });
            assert!(manager.take_pending_cancel("S"));
            manager.clear_pending_cancel("S");

            let promoting = Arc::clone(&manager.promoting);
            let _ = std::panic::catch_unwind(move || {
                let _guard = promoting.lock().unwrap();
                panic!("poison promotion state");
            });
            manager.clear_promoting("S");
        });

        let promotion = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&promotion, control);
        promotion.mark_promoting("S").unwrap();
        let search = promotion.search("S", "tag", 3, 10).await.unwrap();
        assert!(search.matches.is_empty());
        assert_eq!(search.status.state, IndexState::Promoting);
        assert_eq!(
            promotion
                .commit_pending_entries("S", 1, &mut Vec::new())
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn paused_health_wait_and_runtime_poisoning_fail_closed() {
        struct CancelOnSecondPoll(AtomicUsize);

        impl InventoryControl for CancelOnSecondPoll {
            fn pause(&self) {}

            fn resume(&self) {}

            fn cancel(&self) {}

            fn is_cancelled(&self) -> bool {
                self.0.fetch_add(1, Ordering::AcqRel) > 0
            }
        }

        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("health-overlay.sqlite3")),
        ));
        let control_impl = CancelOnSecondPoll(AtomicUsize::new(0));
        control_impl.pause();
        control_impl.resume();
        control_impl.cancel();
        let control: Arc<dyn InventoryControl> = Arc::new(control_impl);
        insert_runtime_build(&manager, Arc::clone(&control));
        manager.pause_overlays.lock().unwrap().insert(
            "S".into(),
            PauseOverlayState {
                maintenance: false,
                health: true,
            },
        );
        let mut next_probe = Instant::now() + Duration::from_secs(60);
        let mut backoff = Duration::from_secs(1);
        assert!(
            !manager
                .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
                .await
        );

        let runtime = Arc::clone(&manager.runtime);
        let _ = std::panic::catch_unwind(move || {
            let _guard = runtime.lock().unwrap();
            panic!("poison runtime");
        });
        manager.reconcile_pause_state("S");
    }

    #[tokio::test]
    async fn run_build_handles_promoting_lock_and_periodic_commit_failures() {
        struct DelayedCompletionStream {
            phase: u8,
        }

        #[async_trait::async_trait]
        impl InventoryStream for DelayedCompletionStream {
            async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
                match self.phase {
                    0 => {
                        self.phase = 1;
                        Some(Ok(InventoryEvent::Entry(inventory_entry(
                            "Periodic",
                            "S.Periodic",
                        ))))
                    }
                    1 => {
                        self.phase = 2;
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        Some(Ok(InventoryEvent::Slice(InventorySliceObservation {
                            sequence: 1,
                            backend: InventorySliceBackend::Da2,
                            nodes_returned: 1,
                            has_more: false,
                            native_operations: 1,
                            elapsed_ms: 1,
                            entries_seen: 1,
                            unique_items: 1,
                        })))
                    }
                    _ => Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    }))),
                }
            }
        }

        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("periodic-commit.sqlite3"));
        config.commit_interval_ms = 1;
        config.commit_batch_size = 100;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let generation = manager
            .with_database(|db| {
                let generation = db
                    .start_generation(
                        "S",
                        NamespaceOrganization::Hierarchical,
                        BrowseSource::Da2,
                        "1",
                    )
                    .unwrap();
                db.connection
                    .execute_batch(
                        "CREATE TRIGGER reject_periodic_insert
                     BEFORE INSERT ON entries
                     BEGIN
                       SELECT RAISE(FAIL, 'periodic insert rejected');
                     END;",
                    )
                    .unwrap();
                Ok(generation)
            })
            .unwrap();
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        Arc::clone(&manager)
            .run_build(
                "S".into(),
                generation,
                InventoryHandle {
                    stream: Box::new(DelayedCompletionStream { phase: 0 }),
                    control,
                },
                ownership,
            )
            .await;
        assert_eq!(manager.status("S").await.unwrap().state, IndexState::Failed);

        let mut successful_config = settings(directory.path().join("periodic-success.sqlite3"));
        successful_config.commit_interval_ms = 1;
        successful_config.commit_batch_size = 100;
        let successful = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            successful_config,
        ));
        let generation = successful
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&successful, Arc::clone(&control));
        Arc::clone(&successful)
            .run_build(
                "S".into(),
                generation,
                InventoryHandle {
                    stream: Box::new(DelayedCompletionStream { phase: 0 }),
                    control,
                },
                ownership,
            )
            .await;
        assert_eq!(
            successful.status("S").await.unwrap().state,
            IndexState::Ready
        );

        let promotion = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("promotion-lock.sqlite3")),
        ));
        let generation = promotion
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&promotion, Arc::clone(&control));
        let promoting = Arc::clone(&promotion.promoting);
        let _ = std::panic::catch_unwind(move || {
            let _guard = promoting.lock().unwrap();
            panic!("poison promotion lock");
        });
        Arc::clone(&promotion)
            .run_build(
                "S".into(),
                generation,
                InventoryHandle {
                    stream: Box::new(VecInventoryStream {
                        events: VecDeque::from([Ok(InventoryEvent::Completed(
                            InventoryCompleted {
                                complete: true,
                                cancelled: false,
                                truncated: false,
                                warning: None,
                                organization: NamespaceOrganization::Hierarchical,
                                source: BrowseSource::Da2,
                            },
                        ))]),
                    }),
                    control,
                },
                ownership,
            )
            .await;
        assert_eq!(
            promotion.status("S").await.unwrap().state,
            IndexState::Failed
        );
    }

    #[tokio::test]
    async fn commit_batching_flushes_thresholds_and_final_pending_entries() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("commit-boundaries.sqlite3"));
        config.commit_batch_size = 3;
        config.commit_interval_ms = 60_000;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config.clone(),
        ));
        let generation = manager
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, Arc::clone(&control));
        let mut state = BuildRunState::new(&config, None);

        for index in 0..7 {
            assert!(matches!(
                manager
                    .handle_entry_event(
                        "S",
                        generation,
                        &control,
                        &mut state,
                        inventory_entry(&format!("Entry {index}"), &format!("S.Entry{index}"),),
                    )
                    .await,
                BuildEventOutcome::Continue
            ));

            match index {
                2 => {
                    assert!(state.pending.is_empty());
                    assert_eq!(state.persisted_item_count, 3);
                }
                5 => {
                    assert!(state.pending.is_empty());
                    assert_eq!(state.persisted_item_count, 6);
                }
                6 => {
                    assert_eq!(state.pending.len(), 1);
                    assert_eq!(state.persisted_item_count, 6);
                }
                _ => {}
            }
        }

        let inserted = manager
            .commit_pending_entries("S", generation, &mut state.pending)
            .unwrap();
        state.persisted_item_count = state.persisted_item_count.saturating_add(inserted);
        assert_eq!(inserted, 1);
        assert!(state.pending.is_empty());
        assert_eq!(state.persisted_item_count, 7);

        let stored_entries = manager
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM entries
                         WHERE server = ?1 AND generation = ?2",
                        rusqlite::params!["S", generation as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(stored_entries, 7);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unexpected_build_unwind_releases_ownership_and_resumes_cleanup() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("unwind.sqlite3")),
        ));
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);
        let generation = manager
            .with_database(|db| {
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            })
            .unwrap();
        let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
        let ownership = insert_runtime_build(&manager, Arc::clone(&control));
        let result = tokio::spawn(Arc::clone(&manager).run_build(
            "S".into(),
            generation,
            InventoryHandle {
                stream: Box::new(PanickingInventoryStream),
                control,
            },
            ownership,
        ))
        .await;
        assert!(result.is_err());
        manager.background_tasks.wait_for_idle().await;
        assert!(manager.active_builds.lock().unwrap().is_empty());
        assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
        assert_eq!(
            manager.with_database(|db| db.status_rows("S")).unwrap()[0].state,
            "failed"
        );
    }

    #[tokio::test]
    async fn stale_and_adaptive_cancellation_scheduler_paths_are_covered() {
        let directory = tempdir().unwrap();
        let stale = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("stale-no-window.sqlite3")),
        ));
        seed_active_generation(
            &stale,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "0",
        );
        assert_eq!(
            stale.background_refresh_delay("S").await,
            Duration::from_secs(1)
        );

        let mut maintenance_config = settings(directory.path().join("invalid-maintenance.sqlite3"));
        maintenance_config.maintenance_windows = vec!["invalid".into()];
        let maintenance = IndexManager::new(Arc::new(MockOpcClient::default()), maintenance_config);
        assert!(!maintenance.automatic_refresh_allowed(&empty_status(
            "S",
            true,
            IndexState::NotIndexed
        )));

        let mut adaptive_config = settings(directory.path().join("adaptive-cancel.sqlite3"));
        adaptive_config.adaptive = true;
        let adaptive = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            adaptive_config,
        ));
        adaptive.record_foreground_operation_with_health(
            "S",
            Duration::from_millis(1),
            false,
            true,
            false,
        );
        let generation = adaptive
            .with_database(|db| {
                db.start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
            })
            .unwrap();
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel_on_pause();
        let control: Arc<dyn InventoryControl> = control;
        let ownership = insert_runtime_build(&adaptive, Arc::clone(&control));
        Arc::clone(&adaptive)
            .run_build(
                "S".into(),
                generation,
                InventoryHandle {
                    stream: Box::new(VecInventoryStream {
                        events: VecDeque::from([Ok(InventoryEvent::Slice(
                            InventorySliceObservation {
                                sequence: 1,
                                backend: InventorySliceBackend::Da2,
                                nodes_returned: 1,
                                has_more: false,
                                native_operations: 1,
                                elapsed_ms: 1,
                                entries_seen: 1,
                                unique_items: 1,
                            },
                        ))]),
                    }),
                    control,
                },
                ownership,
            )
            .await;
        assert_eq!(
            adaptive.status("S").await.unwrap().state,
            IndexState::Failed
        );
    }

    #[test]
    fn restart_recovery_surfaces_staging_update_errors() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("staging-recovery.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
        db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
            .unwrap();
        drop(db);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_restart_recovery
                 BEFORE UPDATE OF state ON generations
                 BEGIN
                   SELECT RAISE(FAIL, 'restart recovery rejected');
                 END;",
            )
            .unwrap();
        drop(connection);
        let error = IndexDb::open(&path)
            .err()
            .expect("restart recovery should fail");
        assert!(error.to_string().contains("restart recovery rejected"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capability_cancellation_cleans_up_generation_start_boundaries() {
        async fn cancel_after_capability(
            path: PathBuf,
            break_generations: bool,
        ) -> anyhow::Result<IndexStatus> {
            let control = Arc::new(RecordingInventoryControl::default());
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let manager = Arc::new(IndexManager::new(
                Arc::new(
                    LifecycleClient::new(
                        vec![Ok(handle_with_control(
                            VecDeque::new(),
                            Arc::clone(&control),
                        ))],
                        vec![Ok(default_capabilities())],
                    )
                    .with_capability_gate(Arc::clone(&started), Arc::clone(&release)),
                ),
                settings(path),
            ));
            if break_generations {
                manager
                    .with_database(|db| {
                        drop_table(db, "generations");
                        Ok(())
                    })
                    .unwrap();
            }
            let refresh_manager = Arc::clone(&manager);
            let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
            started.notified().await;
            control.cancel();
            release.notify_one();
            refresh.await.unwrap().map_err(anyhow::Error::from)
        }

        let directory = tempdir().unwrap();
        assert!(
            cancel_after_capability(directory.path().join("generation.sqlite3"), true)
                .await
                .unwrap_err()
                .to_string()
                .contains("no such table")
        );
        assert_eq!(
            cancel_after_capability(directory.path().join("attached.sqlite3"), false)
                .await
                .unwrap()
                .state,
            IndexState::NotIndexed
        );
    }

    #[tokio::test]
    async fn scheduler_shutdown_quiet_resume_and_batch_commit_complete() {
        let directory = tempdir().unwrap();
        let background_config = settings(directory.path().join("background-shutdown.sqlite3"));
        let background = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            background_config,
        ));
        background.start_background_indexing();
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        background.shutdown_background_indexing().await;

        let quiet = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("quiet-resume.sqlite3")),
        ));
        let quiet_control = Arc::new(RecordingInventoryControl::default());
        let control: Arc<dyn InventoryControl> = quiet_control.clone();
        insert_runtime_build(&quiet, control);
        let guard = quiet.foreground_guard("S");
        let resumes = quiet_control.resume_count.load(Ordering::Relaxed);
        drop(guard);
        wait_for_counter(&quiet_control.resume_count, resumes + 2).await;

        let mut batch_config = settings(directory.path().join("batch-success.sqlite3"));
        batch_config.commit_batch_size = 1;
        let batch = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            batch_config,
        ));
        batch.refresh("S", true).await.unwrap();
        wait_for_build(&batch, IndexState::Ready).await;
        assert_eq!(batch.status("S").await.unwrap().entry_count, 1);
    }

    #[tokio::test]
    async fn controller_recovery_returns_false_for_an_already_cancelled_control() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("recovery-cancelled.sqlite3"));
        config.adaptive = true;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(TestInventoryControl::default());
        control.cancel();
        let control: Arc<dyn InventoryControl> = control;
        insert_runtime_build(&manager, Arc::clone(&control));
        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        assert!(
            !manager
                .wait_for_controller_recovery(&control, "S", &mut controller)
                .await
                .unwrap()
        );
    }

    async fn wait_for_build(manager: &Arc<IndexManager<MockOpcClient>>, expected: IndexState) {
        wait_for_state(manager, "S", expected).await;
    }

    async fn wait_for_state<C: OpcClient>(
        manager: &Arc<IndexManager<C>>,
        server: &str,
        expected: IndexState,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if manager.status(server).await.unwrap().state == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("index build did not reach expected state");
    }

    async fn wait_for_counter(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if counter.load(Ordering::Relaxed) >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("counter did not reach expected value");
    }

    #[test]
    fn completion_event_shape_is_typed() {
        let event = InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            truncated: false,
            warning: None,
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
        });
        assert!(matches!(event, InventoryEvent::Completed(_)));
    }

    #[derive(Default)]
    struct TestInventoryControl {
        cancelled: AtomicBool,
    }

    impl InventoryControl for TestInventoryControl {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }
    }

    #[derive(Default)]
    struct RecordingInventoryControl {
        paused: AtomicBool,
        cancelled: AtomicBool,
        cancel_on_pause: AtomicBool,
        cancel_on_pacing: AtomicBool,
        pause_count: AtomicUsize,
        resume_count: AtomicUsize,
        pacing_calls: AtomicUsize,
        fail_pacing_on_call: AtomicUsize,
    }

    impl InventoryControl for RecordingInventoryControl {
        fn pause(&self) {
            self.pause_count.fetch_add(1, Ordering::Relaxed);
            self.paused.store(true, Ordering::Release);
            if self.cancel_on_pause.load(Ordering::Acquire) {
                self.cancelled.store(true, Ordering::Release);
            }
        }

        fn resume(&self) {
            self.resume_count.fetch_add(1, Ordering::Relaxed);
            self.paused.store(false, Ordering::Release);
        }

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
            let call = self.pacing_calls.fetch_add(1, Ordering::AcqRel) + 1;
            if self.cancel_on_pacing.load(Ordering::Acquire) {
                self.cancelled.store(true, Ordering::Release);
            }
            let failure_call = self.fail_pacing_on_call.load(Ordering::Acquire);
            if call == failure_call {
                anyhow::bail!("test pacing update failure");
            }
            Ok(())
        }
    }

    impl RecordingInventoryControl {
        fn cancel_on_pause(&self) {
            self.cancel_on_pause.store(true, Ordering::Release);
        }

        fn cancel_on_pacing(&self) {
            self.cancel_on_pacing.store(true, Ordering::Release);
        }

        fn fail_pacing_on_call(&self, call: usize) {
            self.fail_pacing_on_call.store(call, Ordering::Release);
        }
    }

    struct RegisterCancellingControl {
        parent: Arc<CoordinatedInventoryControl>,
        cancelled: AtomicBool,
    }

    struct RegisterStoppingControl {
        parent: Arc<CoordinatedInventoryControl>,
    }

    impl InventoryControl for RegisterStoppingControl {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {}

        fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
            self.parent.stop_workers();
            anyhow::bail!("test registration stop");
        }
    }

    impl InventoryControl for RegisterCancellingControl {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
        }

        fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
            self.parent.cancel();
            Ok(())
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }
    }

    struct VecInventoryStream {
        events: VecDeque<anyhow::Result<InventoryEvent>>,
    }

    struct DropGateInventoryStream {
        started: std::sync::mpsc::SyncSender<()>,
        release: Arc<AtomicBool>,
        control: Arc<dyn InventoryControl>,
        event: Option<anyhow::Result<InventoryEvent>>,
    }

    struct PanickingInventoryStream;

    struct PanicAfterReleaseInventoryStream {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct CompletedThenShutdownPanicInventoryStream {
        emitted: bool,
    }

    struct CompletedThenShutdownErrorInventoryStream {
        emitted: bool,
    }

    struct ErrorThenShutdownErrorInventoryStream {
        emitted: bool,
    }

    struct ControlledInventoryStream {
        started: Arc<Notify>,
        started_count: Arc<AtomicUsize>,
        release: Arc<Notify>,
        event: Option<anyhow::Result<InventoryEvent>>,
        shutdowns: Arc<AtomicUsize>,
    }

    #[derive(Default)]
    struct CancellationInventoryControl {
        cancelled: AtomicBool,
        cancellation: Notify,
    }

    struct CancellationAwareInventoryStream {
        started: Arc<Notify>,
        control: Arc<CancellationInventoryControl>,
        shutdowns: Arc<AtomicUsize>,
        emitted: bool,
    }

    #[async_trait::async_trait]
    impl InventoryStream for DropGateInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            self.event.take()
        }
    }

    impl Drop for DropGateInventoryStream {
        fn drop(&mut self) {
            let _ = self.started.send(());
            while !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.control.cancel();
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for PanickingInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            panic!("injected inventory stream panic");
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for PanicAfterReleaseInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            self.started.notify_one();
            self.release.notified().await;
            panic!("injected coordinated inventory stream panic");
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for CompletedThenShutdownPanicInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            if self.emitted {
                None
            } else {
                self.emitted = true;
                Some(Ok(completed_inventory()))
            }
        }

        async fn shutdown(&mut self) -> anyhow::Result<()> {
            panic!("injected coordinated inventory shutdown panic");
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for CompletedThenShutdownErrorInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            if self.emitted {
                None
            } else {
                self.emitted = true;
                Some(Ok(completed_inventory()))
            }
        }

        async fn shutdown(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("injected coordinated inventory shutdown failure")
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for ErrorThenShutdownErrorInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            if self.emitted {
                None
            } else {
                self.emitted = true;
                Some(Err(anyhow::anyhow!("injected inventory build failure")))
            }
        }

        async fn shutdown(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("injected coordinated inventory shutdown failure")
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for ControlledInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            let event = self.event.take()?;
            self.started_count.fetch_add(1, Ordering::AcqRel);
            self.started.notify_one();
            self.release.notified().await;
            Some(event)
        }

        async fn shutdown(&mut self) -> anyhow::Result<()> {
            self.shutdowns.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    impl InventoryControl for CancellationInventoryControl {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {
            self.cancelled.store(true, Ordering::Release);
            self.cancellation.notify_waiters();
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for CancellationAwareInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            if self.emitted {
                return None;
            }
            self.emitted = true;
            self.started.notify_one();
            let cancellation = self.control.cancellation.notified();
            if !self.control.is_cancelled() {
                cancellation.await;
            }
            Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: true,
                truncated: false,
                warning: Some("test cancellation".into()),
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            })))
        }

        async fn shutdown(&mut self) -> anyhow::Result<()> {
            self.shutdowns.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl InventoryStream for VecInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            self.events.pop_front()
        }
    }

    struct BlockingInventoryStream {
        started: Arc<Notify>,
        release: Arc<Notify>,
        event: Option<anyhow::Result<InventoryEvent>>,
    }

    #[async_trait::async_trait]
    impl InventoryStream for BlockingInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            let event = self.event.take()?;
            self.started.notify_one();
            self.release.notified().await;
            Some(event)
        }
    }

    struct CancellingEntryStream {
        control: Arc<RecordingInventoryControl>,
        yielded: bool,
    }

    #[async_trait::async_trait]
    impl InventoryStream for CancellingEntryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            if self.yielded {
                return None;
            }
            self.yielded = true;
            self.control.cancel();
            Some(Ok(InventoryEvent::Entry(inventory_entry(
                "Cancelled",
                "S.Cancelled",
            ))))
        }
    }

    struct LifecycleClient {
        inventories: Mutex<VecDeque<Result<InventoryHandle, String>>>,
        capabilities: Mutex<VecDeque<Result<BrowseCapabilities, String>>>,
        capability_delay: Mutex<Duration>,
        capability_gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        capability_gate_used: AtomicBool,
        inventory_gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        inventory_gate_used: AtomicBool,
        inventory_start_count: AtomicUsize,
        root_browse_page: Mutex<Option<Result<BrowsePage, String>>>,
        root_close_result: Mutex<Option<Result<(), String>>>,
        root_inventories: Mutex<VecDeque<Result<InventoryHandle, String>>>,
        root_inventory_start_count: AtomicUsize,
        root_inventory_start_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    }

    impl LifecycleClient {
        fn new(
            inventories: Vec<Result<InventoryHandle, String>>,
            capabilities: Vec<Result<BrowseCapabilities, String>>,
        ) -> Self {
            Self {
                inventories: Mutex::new(inventories.into()),
                capabilities: Mutex::new(capabilities.into()),
                capability_delay: Mutex::new(Duration::ZERO),
                capability_gate: Mutex::new(None),
                capability_gate_used: AtomicBool::new(false),
                inventory_gate: Mutex::new(None),
                inventory_gate_used: AtomicBool::new(false),
                inventory_start_count: AtomicUsize::new(0),
                root_browse_page: Mutex::new(None),
                root_close_result: Mutex::new(None),
                root_inventories: Mutex::new(VecDeque::new()),
                root_inventory_start_count: AtomicUsize::new(0),
                root_inventory_start_hook: Mutex::new(None),
            }
        }

        fn with_capability_delay(self, delay: Duration) -> Self {
            *self.capability_delay.lock().unwrap() = delay;
            self
        }

        fn with_capability_gate(self, started: Arc<Notify>, release: Arc<Notify>) -> Self {
            *self.capability_gate.lock().unwrap() = Some((started, release));
            self
        }

        fn with_inventory_gate(self, started: Arc<Notify>, release: Arc<Notify>) -> Self {
            *self.inventory_gate.lock().unwrap() = Some((started, release));
            self
        }

        fn with_root_browse_page(self, page: Result<BrowsePage, String>) -> Self {
            *self.root_browse_page.lock().unwrap() = Some(page);
            self
        }

        fn with_root_close_result(self, result: Result<(), String>) -> Self {
            *self.root_close_result.lock().unwrap() = Some(result);
            self
        }

        fn with_root_inventories(self, inventories: Vec<Result<InventoryHandle, String>>) -> Self {
            *self.root_inventories.lock().unwrap() = inventories.into();
            self
        }

        fn with_root_inventory_start_hook<F>(self, hook: F) -> Self
        where
            F: Fn() + Send + Sync + 'static,
        {
            *self.root_inventory_start_hook.lock().unwrap() = Some(Arc::new(hook));
            self
        }
    }

    #[async_trait::async_trait]
    impl OpcClient for LifecycleClient {
        async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
            Ok(vec!["S".into(), "T".into()])
        }

        async fn get_capabilities(&self, _server: &str) -> anyhow::Result<BrowseCapabilities> {
            let delay = *self.capability_delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let gate = self.capability_gate.lock().unwrap().clone();
            if !self.capability_gate_used.swap(true, Ordering::AcqRel)
                && let Some((started, release)) = gate
            {
                started.notify_one();
                release.notified().await;
            }
            self.capabilities
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(default_capabilities()))
                .map_err(anyhow::Error::msg)
        }

        async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
            Ok("session".into())
        }

        async fn browse_page(
            &self,
            _session_id: &str,
            _parent_node_key: Option<&str>,
            _page_token: Option<&str>,
            _page_size: u32,
            _refresh: bool,
        ) -> anyhow::Result<BrowsePage> {
            if let Some(result) = self.root_browse_page.lock().unwrap().take() {
                return result.map_err(anyhow::Error::msg);
            }
            anyhow::bail!("unused in index tests")
        }

        async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
            if let Some(result) = self.root_close_result.lock().unwrap().take() {
                return result.map_err(anyhow::Error::msg);
            }
            Ok(())
        }

        async fn start_inventory(
            &self,
            _server: &str,
            _batch_size: u32,
        ) -> anyhow::Result<InventoryHandle> {
            self.inventory_start_count.fetch_add(1, Ordering::Relaxed);
            let gate = self.inventory_gate.lock().unwrap().clone();
            if !self.inventory_gate_used.swap(true, Ordering::AcqRel)
                && let Some((started, release)) = gate
            {
                started.notify_one();
                release.notified().await;
            }
            self.inventories
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("no inventory configured".into()))
                .map_err(anyhow::Error::msg)
        }

        async fn start_inventory_at_root(
            &self,
            _server: &str,
            _root_item_id: &str,
            _batch_size: u32,
        ) -> anyhow::Result<InventoryHandle> {
            self.root_inventory_start_count
                .fetch_add(1, Ordering::Relaxed);
            if let Some(hook) = self.root_inventory_start_hook.lock().unwrap().clone() {
                hook();
            }
            self.root_inventories
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("no root inventory configured".into()))
                .map_err(anyhow::Error::msg)
        }

        async fn read_tag_values(
            &self,
            _server: &str,
            _tag_ids: Vec<String>,
        ) -> anyhow::Result<Vec<TagValue>> {
            Ok(Vec::new())
        }

        async fn write_tag_value(
            &self,
            _server: &str,
            _tag_id: &str,
            _value: OpcValue,
        ) -> anyhow::Result<WriteResult> {
            anyhow::bail!("unused in index tests")
        }
    }

    fn default_capabilities() -> BrowseCapabilities {
        BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 1000,
        }
    }

    fn immediate_inventory_handle() -> InventoryHandle {
        handle_with_control(
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
                Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 1,
                    unique_items: 1,
                    active_time_ms: 1,
                    paused_time_ms: 0,
                    items_per_second: 1.0,
                    estimated_remaining_ms: None,
                })),
                Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                })),
            ]),
            Arc::new(RecordingInventoryControl::default()),
        )
    }

    fn handle_with_control<T>(
        events: VecDeque<anyhow::Result<InventoryEvent>>,
        control: Arc<T>,
    ) -> InventoryHandle
    where
        T: InventoryControl + 'static,
    {
        InventoryHandle {
            stream: Box::new(VecInventoryStream { events }),
            control,
        }
    }

    fn manager_with_blocking_event(
        path: PathBuf,
        event: anyhow::Result<InventoryEvent>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    ) -> Arc<IndexManager<LifecycleClient>> {
        Arc::new(IndexManager::new(
            Arc::new(LifecycleClient::new(
                vec![Ok(InventoryHandle {
                    stream: Box::new(BlockingInventoryStream {
                        started,
                        release,
                        event: Some(event),
                    }),
                    control: Arc::new(RecordingInventoryControl::default()),
                })],
                vec![],
            )),
            settings(path),
        ))
    }

    fn insert_runtime_build<C: OpcClient>(
        manager: &Arc<IndexManager<C>>,
        control: Arc<dyn InventoryControl>,
    ) -> Arc<()> {
        let _ = manager.with_database(|db| db.enroll("S", &timestamp_now()));
        let ownership = Arc::new(());
        manager
            .coordination
            .build_owners
            .lock()
            .unwrap()
            .insert("S".into(), Arc::clone(&ownership));
        manager
            .coordination
            .active_builds
            .lock()
            .unwrap()
            .insert("S".into());
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: Some(control),
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                retry_after: None,
                last_error: None,
                consecutive_failures: 0,
                circuit_open: false,
                health: HealthProbeState::Unavailable,
                sentinel_checked_at: None,
            },
        );
        ownership
    }

    fn seed_active_generation<C: OpcClient>(
        manager: &Arc<IndexManager<C>>,
        organization: NamespaceOrganization,
        source: BrowseSource,
        completed_at: &str,
    ) {
        manager
            .with_database(|db| {
                let generation =
                    db.start_generation("S", organization, source, &timestamp_now())?;
                db.insert_entries(
                    "S",
                    generation,
                    &[inventory_entry("Persisted", "Persisted.Tag")],
                )?;
                db.promote("S", generation, completed_at, &completed_progress(1))
            })
            .unwrap();
    }

    fn drop_table(db: &mut IndexDb, table: &str) {
        db.connection
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        db.connection
            .execute_batch(&format!("DROP TABLE {table};"))
            .unwrap();
    }

    fn inventory_entry(display_name: &str, item_id: &str) -> InventoryEntry {
        InventoryEntry {
            display_name: display_name.into(),
            item_id: item_id.into(),
            kind: InventoryNodeKind::Item,
            breadcrumbs: vec!["S".into()],
        }
    }

    fn zero_progress() -> InventoryProgress {
        InventoryProgress {
            branches_visited: 0,
            entries_seen: 0,
            unique_items: 0,
            active_time_ms: 0,
            paused_time_ms: 0,
            items_per_second: 0.0,
            estimated_remaining_ms: None,
        }
    }

    fn cached_search(server: &str) -> IndexedSearch {
        IndexedSearch {
            matches: Vec::new(),
            has_more: false,
            status: IndexStatus {
                server: server.into(),
                state: IndexState::Ready,
                auto_refresh_enabled: false,
                active_generation: 1,
                entry_count: 0,
                unique_item_count: 0,
                started_at: None,
                completed_at: None,
                last_error: None,
                database_bytes: 0,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                progress: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                foreground_metrics: ForegroundMetrics::default(),
                host_metrics: HostMetrics::default(),
                health: HealthProbeState::Unavailable,
                sentinel_configured: false,
                storage: StorageDiagnostics::default(),
                scheduler: SchedulerDiagnostics::default(),
            },
        }
    }

    #[test]
    fn split_enrollment_guards_reject_conflicts_and_lock_errors() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        manager.reserve_deletion("S").unwrap();
        assert!(matches!(
            manager.reserve_deletion("S"),
            Err(IndexOperationError::Deleting { server }) if server == "S"
        ));

        let poisoned = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        let deleting = Arc::clone(&poisoned.deleting);
        assert!(
            std::thread::spawn(move || {
                let _guard = deleting.lock().unwrap();
                panic!("poison deletion reservation state");
            })
            .join()
            .is_err()
        );
        assert!(matches!(
            poisoned.reserve_deletion("S"),
            Err(IndexOperationError::Internal(error))
                if error.to_string().contains("index deletion lock poisoned")
        ));
    }

    #[tokio::test]
    async fn split_delete_lock_returns_unheld_acquisition_errors() {
        let error = enrollment::acquire_delete_lock_with(
            Path::new("index.sqlite3"),
            "S",
            |_, _| Err(anyhow::anyhow!("injected lock acquisition failure")),
            |_, _| Ok(false),
        )
        .await
        .err()
        .unwrap();
        assert!(
            error
                .to_string()
                .contains("injected lock acquisition failure")
        );
    }

    #[test]
    fn split_query_rejects_malformed_rows_and_handles_maximum_prefix() {
        let entries = [inventory_entry("Target", "S.Target")];
        let (database, generation) = in_memory_index_with(&entries);
        database
            .connection
            .execute("UPDATE entries SET kind = 99 WHERE server = 'S'", [])
            .unwrap();
        assert!(
            database
                .search("S", generation, "ta", 3, 10)
                .unwrap_err()
                .to_string()
                .contains("unknown indexed node kind 99")
        );

        let (database, generation) = in_memory_index_with(&entries);
        database
            .connection
            .execute(
                "UPDATE entries SET breadcrumbs = 'not-json' WHERE server = 'S'",
                [],
            )
            .unwrap();
        let error = database.search("S", generation, "ta", 3, 10).unwrap_err();
        assert!(
            error
                .downcast_ref::<rusqlite::Error>()
                .is_some_and(|error| {
                    matches!(error, rusqlite::Error::FromSqlConversionFailure(..))
                })
        );

        let (database, generation) = in_memory_index_with(&entries);
        assert!(
            database
                .search("S", generation, "\u{10ffff}", 2, 10)
                .unwrap()
                .is_empty()
        );

        let (database, generation) = in_memory_index_with(&entries);
        database
            .reject_next_prefix_query_map
            .store(true, Ordering::Release);
        assert!(database.search("S", generation, "ta", 2, 10).is_err());

        let (database, generation) = in_memory_index_with(&entries);
        database
            .reject_next_prefix_query_map
            .store(true, Ordering::Release);
        assert!(
            database
                .search("S", generation, "\u{10ffff}", 2, 10)
                .is_err()
        );
    }

    #[test]
    fn split_store_propagates_database_errors() {
        let mut failed_attempt = IndexDb::open(Path::new(":memory:")).unwrap();
        failed_attempt
            .connection
            .execute_batch(
                "CREATE TRIGGER reject_failed_attempt
                 BEFORE INSERT ON generations
                 BEGIN
                   SELECT RAISE(FAIL, 'failed attempt rejected');
                 END;",
            )
            .unwrap();
        assert!(
            failed_attempt
                .record_failed_attempt("S", "failed")
                .unwrap_err()
                .to_string()
                .contains("failed attempt rejected")
        );

        let mut obsolete = IndexDb::open(Path::new(":memory:")).unwrap();
        drop_table(&mut obsolete, "generations");
        assert!(obsolete.obsolete_servers().is_err());

        let mut enrollment = IndexDb::open(Path::new(":memory:")).unwrap();
        drop_table(&mut enrollment, "enrolled_servers");
        assert!(enrollment.enroll("S", "1").is_err());
        assert!(enrollment.set_auto_refresh("S", false).is_err());

        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE generations (server TEXT NOT NULL);
                 INSERT INTO generations(server) VALUES ('S');
                 CREATE TABLE index_meta (
                     key TEXT PRIMARY KEY NOT NULL,
                     value TEXT NOT NULL
                 );",
            )
            .unwrap();
        assert!(migrate_schema_3_to_4(&mut connection).is_err());
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'enrolled_servers'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn quarantine_errors_restore_moved_files_and_report_rollback_failures() {
        use std::io::{Error, ErrorKind};

        let directory = tempdir().unwrap();
        let path = directory.path().join("index.sqlite3");
        let quarantine = directory.path().join("quarantine.sqlite3");
        let wal = IndexDb::sqlite_sidecar_path(&path, "-wal");
        fs::write(&path, b"database").unwrap();
        fs::write(&wal, b"wal").unwrap();

        let metadata_error = store::quarantine_index_files_with(
            &path,
            &quarantine,
            |_| Err(Error::new(ErrorKind::PermissionDenied, "metadata failure")),
            |source, destination| fs::rename(source, destination),
        )
        .unwrap_err();
        assert!(metadata_error.to_string().contains("metadata failure"));

        let first_rename_error = store::quarantine_index_files_with(
            &path,
            &quarantine,
            |source| fs::symlink_metadata(source),
            |_, _| Err(Error::other("first rename failure")),
        )
        .unwrap_err();
        assert!(
            first_rename_error
                .to_string()
                .contains("first rename failure")
        );

        let mut rename_count = 0;
        let rollback_error = store::quarantine_index_files_with(
            &path,
            &quarantine,
            |source| fs::symlink_metadata(source),
            |source, destination| {
                rename_count += 1;
                if rename_count == 2 {
                    Err(Error::other("sidecar rename failure"))
                } else {
                    fs::rename(source, destination)
                }
            },
        )
        .unwrap_err();
        assert!(
            rollback_error
                .to_string()
                .contains("sidecar rename failure")
        );
        assert!(path.exists());
        assert!(wal.exists());
        assert!(!quarantine.exists());

        let mut rename_count = 0;
        let rollback_error = store::quarantine_index_files_with(
            &path,
            &quarantine,
            |source| fs::symlink_metadata(source),
            |source, destination| {
                rename_count += 1;
                match rename_count {
                    2 => Err(Error::other("sidecar rename failure")),
                    3 => Err(Error::other("rollback rename failure")),
                    _ => fs::rename(source, destination),
                }
            },
        )
        .unwrap_err();
        assert!(rollback_error.to_string().contains("rollback also failed"));
        assert!(!path.exists());
        assert!(quarantine.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn first_database_write_schedules_cleanup_for_obsolete_generations() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("obsolete.sqlite3");
        let mut database = IndexDb::open(&path).unwrap();
        let active = database
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        database
            .promote("S", active, "1", &completed_progress(0))
            .unwrap();
        let failed = database
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "2")
            .unwrap();
        database.fail_generation("S", failed, "failed").unwrap();
        drop(database);

        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(path),
        ));
        manager.with_database_write(|_| Ok(())).unwrap();
        manager.background_tasks.wait_for_idle().await;
        assert!(
            manager
                .with_database_read(|db| db.obsolete_servers())
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn split_scheduler_shutdown_and_cleanup_short_circuits_are_safe() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("scheduler-split.sqlite3")),
        ));
        let partial = empty_status("S", false, IndexState::Partial);
        assert_eq!(
            manager.refresh_delay_for_status("S", &partial),
            Duration::from_secs(30)
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(true);
        manager.run_background_indexing(shutdown_rx).await;
        let mut shutdown = shutdown_tx.subscribe();
        wait_for_refresh_or_shutdown(&mut shutdown, Duration::from_secs(60)).await;
        manager
            .with_database_write(|db| {
                db.enroll("S", &timestamp_now())?;
                let generation =
                    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
                db.promote("S", generation, "1", &completed_progress(0))
            })
            .unwrap();
        shutdown = shutdown_tx.subscribe();
        assert_eq!(
            manager.refresh_scheduled_servers(&mut shutdown).await,
            Duration::from_secs(60)
        );
        let control = Arc::new(RecordingInventoryControl::default());
        insert_runtime_build(&manager, control.clone());
        manager.shutdown_background_indexing().await;
        assert!(control.cancelled.load(Ordering::Acquire));

        let cleanup = Arc::clone(&manager.cleanup_tasks);
        assert!(
            std::thread::spawn(move || {
                let _guard = cleanup.lock().unwrap();
                panic!("poison cleanup registry for scheduler coverage");
            })
            .join()
            .is_err()
        );
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::ERROR)
            .finish();
        tracing::subscriber::with_default(subscriber, || manager.schedule_cleanup("S"));

        let active = Arc::new(AtomicBool::new(true));
        spawn_cleanup_worker_if_idle(
            Arc::clone(&active),
            manager.settings.database_path.clone(),
            Arc::clone(&manager.background_tasks),
            Arc::clone(&manager.cleanup_tasks),
            Arc::clone(&manager.coordination),
            false,
        );
        assert!(active.load(Ordering::Acquire));

        let poisoned = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("poisoned-runtime.sqlite3")),
        ));
        let runtime = Arc::clone(&poisoned.runtime);
        assert!(
            std::thread::spawn(move || {
                let _guard = runtime.lock().unwrap();
                panic!("poison runtime state for scheduler shutdown coverage");
            })
            .join()
            .is_err()
        );
        poisoned.shutdown_background_indexing().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deferred_cleanup_returns_when_no_build_is_active() {
        let directory = tempdir().unwrap();
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("cleanup-no-build.sqlite3")),
        );
        let mut shutdown = manager.background_tasks.subscribe();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        assert!(
            wait_for_deferred_cleanup(
                &manager.settings.database_path,
                "S",
                &manager.background_tasks,
                &manager.coordination,
                &manager.cleanup_tasks,
                &mut shutdown,
            )
            .await
        );
    }

    #[tokio::test]
    async fn split_status_reports_unenrolled_deletion_and_promotion_read_errors() {
        let directory = tempdir().unwrap();
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("status-split.sqlite3")),
        );
        manager
            .deletion_errors
            .lock()
            .unwrap()
            .insert("S".into(), "injected delete failure".into());
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::Failed);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("injected delete failure"))
        );

        let promoting = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().to_path_buf()),
        );
        promoting.mark_promoting("S").unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        let (rows, error) = tracing::subscriber::with_default(subscriber, || {
            promoting.load_status_rows("S", true).unwrap()
        });
        assert!(rows.is_empty());
        assert!(error.is_some());
    }

    #[test]
    fn split_traversal_handles_paused_state_and_missing_runtime() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        let build = RuntimeBuild {
            control: None,
            progress: None,
            started_at: "1".into(),
            foreground_users: 0,
            operator_paused: false,
            quiet_until: None,
            effective_limits: None,
            controller_state: Some(crate::controller::ControllerState::Paused(
                crate::controller::PauseReason::OpcHealth,
            )),
            pause_reason: None,
            recovery_deadline: None,
            last_commit_latency_ms: None,
        };
        assert!(!IndexManager::<MockOpcClient>::build_can_resume(
            &build,
            PauseOverlayState::default()
        ));
        manager.update_runtime_after_build(&mut HashMap::new(), "Missing", None);
        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        let unchanged = controller.observe(started, ControllerObservation::default());
        assert!(!unchanged.transitioned);
        IndexManager::<MockOpcClient>::log_controller_transition("S", &unchanged);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn controller_recovery_updates_runtime_after_a_pause() {
        let directory = tempdir().unwrap();
        let mut config = settings(directory.path().join("recovery-transition.sqlite3"));
        config.adaptive = true;
        config.adaptive_recovery_delay_seconds = 1;
        config.adaptive_max_recovery_delay_seconds = 1;
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            config,
        ));
        let control = Arc::new(RecordingInventoryControl::default());
        let trait_control: Arc<dyn InventoryControl> = control;
        insert_runtime_build(&manager, Arc::clone(&trait_control));
        let started = Instant::now();
        let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
        let paused = controller.observe(
            started,
            ControllerObservation {
                foreground_bad_quality: true,
                ..ControllerObservation::default()
            },
        );
        manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        assert!(
            manager
                .wait_for_controller_recovery(&trait_control, "S", &mut controller)
                .await
                .unwrap()
        );
        assert_eq!(
            controller.state(),
            crate::controller::ControllerState::Ramping
        );
    }

    #[test]
    fn reconcile_pause_state_handles_a_build_without_a_control() {
        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        );
        manager.runtime.lock().unwrap().insert(
            "S".into(),
            RuntimeState {
                build: Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: "1".into(),
                    foreground_users: 0,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                }),
                ..RuntimeState::default()
            },
        );
        manager.reconcile_pause_state("S");
    }

    #[test]
    fn store_quarantine_logger_handles_no_files_to_move() {
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            store::log_quarantine_result(
                Path::new("index.sqlite3"),
                Path::new("index.quarantine"),
                &anyhow::anyhow!("invalid schema"),
                false,
            );
        });
    }

    #[test]
    fn store_open_reports_fts_corruption_and_preserves_a_live_staging_generation() {
        let directory = tempdir().unwrap();
        let corrupt_path = directory.path().join("corrupt-fts.sqlite3");
        let database = IndexDb::open(&corrupt_path).unwrap();
        database
            .connection
            .execute_batch("DROP TABLE entries_fts_data")
            .unwrap();
        drop(database);
        let error = IndexDb::open_once(&corrupt_path)
            .err()
            .expect("a missing FTS backing table should fail validation");
        assert!(
            format!("{error:#}").contains("corrupt"),
            "unexpected FTS validation error: {error:#}"
        );

        let staging_path = directory.path().join("live-staging.sqlite3");
        let mut database = IndexDb::open(&staging_path).unwrap();
        let generation = database
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
            .unwrap();
        drop(database);
        let lock = BuildFileLock::acquire(&staging_path, "S").unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        let reopened =
            tracing::subscriber::with_default(subscriber, || IndexDb::open(&staging_path).unwrap());
        assert_eq!(reopened.status_rows("S").unwrap()[0].generation, generation);
        assert_eq!(reopened.status_rows("S").unwrap()[0].state, "staging");
        drop(reopened);
        drop(lock);
    }
}
