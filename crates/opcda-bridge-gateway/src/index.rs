//! Persistent, gateway-owned namespace index and refresh coordinator.

use crate::config::{InitialBuildPolicy, ResolvedIndexConfig};
use crate::controller::{
    AdaptiveIndexController, ControllerConfig, ControllerObservation, HostMetrics,
    HostMetricsProvider, InventoryLimits, default_host_metrics_provider,
};
use crate::opc::{
    BrowseSource, InventoryControl, InventoryEntry, InventoryEvent, InventoryHandle,
    InventoryNodeKind, InventoryPacing, InventoryProgress, InventorySliceObservation,
    MAX_NATIVE_INVENTORY_BATCH_SIZE, NamespaceOrganization, OpcClient,
};
use chrono::{DateTime, Local, Timelike};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SCHEMA_VERSION: i64 = 3;
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexStatus {
    pub server: String,
    pub state: IndexState,
    pub configured: bool,
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

#[derive(Debug, Clone, Copy, Default)]
struct PauseOverlayState {
    maintenance: bool,
    health: bool,
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

fn database_coordination_key<F>(path: &Path, current_dir: F) -> PathBuf
where
    F: FnOnce() -> std::io::Result<PathBuf>,
{
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else if path == Path::new(":memory:") {
        return path.to_path_buf();
    } else {
        current_dir()
            .map(|directory| directory.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    canonical_database_path(&absolute)
}

fn canonical_database_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    let Some(file_name) = path.file_name() else {
        return path.to_path_buf();
    };
    path.parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .map(|canonical_parent| canonical_parent.join(file_name))
        .unwrap_or_else(|| path.to_path_buf())
}

fn new_database_coordination() -> Arc<DatabaseCoordination> {
    Arc::new(DatabaseCoordination {
        writer_gate: Arc::new(Mutex::new(())),
        active_builds: Arc::new(Mutex::new(HashSet::new())),
        build_owners: Arc::new(Mutex::new(HashMap::new())),
        build_changed: Arc::new(tokio::sync::Notify::new()),
    })
}

fn database_coordination(path: &Path) -> Arc<DatabaseCoordination> {
    if path == Path::new(":memory:") {
        return new_database_coordination();
    }
    let key = database_coordination_key(path, std::env::current_dir);
    let registry = DATABASE_COORDINATIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let coordination = new_database_coordination();
    registry.insert(key, Arc::downgrade(&coordination));
    coordination
}

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
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return false,
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
}

#[derive(Debug)]
struct BuildFileLock {
    file: Option<fs::File>,
}

impl BuildFileLock {
    fn acquire(database_path: &Path, server: &str) -> anyhow::Result<Self> {
        if database_path == Path::new(":memory:") {
            return Ok(Self { file: None });
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
        Ok(Self { file: Some(file) })
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
            let _ = FileExt::unlock(&file);
            drop(file);
        }
    }
}

impl IndexDb {
    fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        tracing::info!(
            process_id = std::process::id(),
            database = %path.display(),
            "opening namespace index database"
        );
        match Self::open_once(path) {
            Ok(db) => Ok(db),
            Err(error) => {
                if !is_quarantinable_index_error(&error) {
                    return Err(error);
                }
                let quarantine = path.with_extension(format!("quarantine-{}", Uuid::new_v4()));
                if path.exists() {
                    fs::rename(path, &quarantine)?;
                    tracing::warn!(
                        database = %path.display(),
                        quarantine = %quarantine.display(),
                        error = %error,
                        "quarantined invalid namespace index"
                    );
                }
                Self::open_once(path)
            }
        }
    }

    fn open_once(path: &Path) -> anyhow::Result<Self> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );",
        )?;
        let schema_version = connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|version| {
                version.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("invalid namespace index schema version {version:?}")
                })
            })
            .transpose()?;
        if let Some(version) = schema_version
            && !matches!(version, 2 | SCHEMA_VERSION)
        {
            anyhow::bail!("unsupported namespace index schema version {version}");
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS generations (
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
                 compatibility_fallback INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (server, generation)
             );
             CREATE TABLE IF NOT EXISTS entries (
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
             CREATE INDEX IF NOT EXISTS entries_display_prefix
               ON entries(server, generation, display_name_norm);
             CREATE INDEX IF NOT EXISTS entries_item_prefix
               ON entries(server, generation, item_id_norm);
             CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
                 server UNINDEXED,
                 generation UNINDEXED,
                 item_id,
                 display_name,
                 breadcrumbs,
                 tokenize = 'trigram'
             );",
        )?;
        if schema_version == Some(2) {
            connection.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE generations
                   ADD COLUMN compatibility_fallback INTEGER NOT NULL DEFAULT 0;
                 INSERT OR REPLACE INTO index_meta(key, value)
                   VALUES ('schema_version', '3');
                 COMMIT;",
            )?;
        } else {
            connection.execute(
                "INSERT OR REPLACE INTO index_meta(key, value) VALUES ('schema_version', ?1)",
                [SCHEMA_VERSION.to_string()],
            )?;
        }
        let relational_entries_exist =
            connection.query_row("SELECT EXISTS(SELECT 1 FROM entries LIMIT 1)", [], |row| {
                row.get::<_, bool>(0)
            })?;
        let full_text_entries_exist = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM entries_fts LIMIT 1)",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if relational_entries_exist != full_text_entries_exist {
            anyhow::bail!("namespace index relational and full-text data are inconsistent");
        }
        let staging_servers = {
            let mut statement = connection
                .prepare("SELECT DISTINCT server FROM generations WHERE state = 'staging'")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for server in staging_servers {
            if BuildFileLock::is_held(path, &server)? {
                tracing::debug!(
                    database = %path.display(),
                    server = %server,
                    "preserving namespace index staging generation owned by a live process"
                );
                continue;
            }
            connection.execute(
                "UPDATE generations AS interrupted
                 SET state = CASE
                         WHEN EXISTS (
                             SELECT 1 FROM generations AS active
                             WHERE active.server = interrupted.server
                               AND active.state = 'active'
                         )
                         THEN 'superseded'
                         ELSE 'failed'
                     END,
                     last_error = COALESCE(
                         last_error,
                         'namespace index build interrupted by gateway restart'
                     )
                 WHERE interrupted.server = ?1
                   AND interrupted.state = 'staging'",
                [server],
            )?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            connection,
        })
    }

    fn open_read_only(path: &Path) -> anyhow::Result<Self> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "query_only", true)?;
        Ok(Self {
            path: path.to_path_buf(),
            connection,
        })
    }

    fn storage_diagnostics(&self) -> StorageDiagnostics {
        storage_diagnostics_for_path(&self.path)
    }

    fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        PathBuf::from(sidecar)
    }

    fn retry_state(&self, server: &str) -> anyhow::Result<(Option<SystemTime>, u32, bool)> {
        let get = |key: String| -> anyhow::Result<Option<String>> {
            Ok(self
                .connection
                .query_row(
                    "SELECT value FROM index_meta WHERE key = ?1",
                    [key],
                    |row| row.get(0),
                )
                .optional()?)
        };
        let retry_after = get(format!("retry_after:{server}"))?
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)));
        let failures = get(format!("failures:{server}"))?
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let circuit_open = get(format!("circuit:{server}"))?.is_some_and(|value| value == "1");
        Ok((retry_after, failures, circuit_open))
    }

    fn set_retry_state(
        &self,
        server: &str,
        retry_after: Option<SystemTime>,
        failures: u32,
        circuit_open: bool,
    ) -> anyhow::Result<()> {
        let retry = retry_after
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_millis().to_string())
            .unwrap_or_default();
        self.connection.execute_batch("BEGIN IMMEDIATE;")?;
        let result = (|| {
            for (key, value) in [
                (format!("retry_after:{server}"), retry),
                (format!("failures:{server}"), failures.to_string()),
                (
                    format!("circuit:{server}"),
                    if circuit_open { "1" } else { "0" }.to_string(),
                ),
            ] {
                self.connection.execute(
                    "INSERT OR REPLACE INTO index_meta(key, value) VALUES (?1, ?2)",
                    params![key, value],
                )?;
            }
            Ok::<(), rusqlite::Error>(())
        })();
        match result {
            Ok(()) => self.connection.execute_batch("COMMIT;")?,
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK;");
                return Err(error.into());
            }
        }
        Ok(())
    }

    fn start_generation(
        &mut self,
        server: &str,
        organization: NamespaceOrganization,
        source: BrowseSource,
        started_at: &str,
    ) -> anyhow::Result<u64> {
        let generation = self.connection.query_row(
            "SELECT COALESCE(MAX(generation), 0) + 1
                 FROM generations WHERE server = ?1",
            [server],
            |row| row.get::<_, i64>(0),
        )?;
        let public_generation = u64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation is negative"))?;
        self.connection.execute(
            "INSERT INTO generations
             (server, generation, state, organization, source, started_at)
             VALUES (?1, ?2, 'staging', ?3, ?4, ?5)",
            params![
                server,
                generation,
                namespace_string(organization),
                source_string(source),
                started_at
            ],
        )?;
        tracing::debug!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation = public_generation,
            "started namespace index generation"
        );
        Ok(public_generation)
    }

    fn insert_entries(
        &mut self,
        server: &str,
        generation: u64,
        entries: &[InventoryEntry],
    ) -> anyhow::Result<u64> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let transaction = self.connection.transaction()?;
        let mut inserted_count = 0_u64;
        for entry in entries {
            if entry.item_id.is_empty() {
                anyhow::bail!("inventory entry has an empty ItemID");
            }
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO entries
                 (server, generation, item_id, item_id_norm, display_name,
                  display_name_norm, kind, breadcrumbs)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    server,
                    generation,
                    entry.item_id,
                    normalize_query(&entry.item_id),
                    entry.display_name,
                    normalize_query(&entry.display_name),
                    node_kind_number(entry.kind),
                    serde_json::to_string(&entry.breadcrumbs)?
                ],
            )?;
            if inserted > 0 {
                inserted_count = inserted_count.saturating_add(u64::try_from(inserted)?);
                transaction.execute(
                    "INSERT INTO entries_fts
                     (server, generation, item_id, display_name, breadcrumbs)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        server,
                        generation,
                        entry.item_id,
                        entry.display_name,
                        entry.breadcrumbs.join(" ")
                    ],
                )?;
            }
        }
        transaction.commit()?;
        tracing::debug!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            batch_size = entries.len(),
            inserted_count,
            "committed namespace index entries"
        );
        Ok(inserted_count)
    }

    fn update_progress(
        &self,
        server: &str,
        generation: u64,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        let entry_count = i64::try_from(progress.entries_seen)
            .map_err(|_| anyhow::anyhow!("namespace index entry count exceeds SQLite range"))?;
        let unique_item_count = i64::try_from(progress.unique_items).map_err(|_| {
            anyhow::anyhow!("namespace index unique item count exceeds SQLite range")
        })?;
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        self.connection.execute(
            "UPDATE generations SET entry_count = ?1, unique_item_count = ?2
             WHERE server = ?3 AND generation = ?4 AND state = 'staging'",
            params![entry_count, unique_item_count, server, generation],
        )?;
        tracing::debug!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            entries_seen = progress.entries_seen,
            unique_items = progress.unique_items,
            "updated namespace index progress"
        );
        Ok(())
    }

    #[cfg(test)]
    fn promote(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        self.promote_with_profile(
            server,
            generation,
            completed_at,
            progress.unique_items,
            None,
            None,
        )
    }

    fn promote_with_profile(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        searchable_item_count: u64,
        profile: Option<(NamespaceOrganization, BrowseSource)>,
        warning: Option<&str>,
    ) -> anyhow::Result<()> {
        let activation_started = Instant::now();
        let searchable_item_count = i64::try_from(searchable_item_count)
            .map_err(|_| anyhow::anyhow!("namespace index entry count exceeds SQLite range"))?;
        let (organization, source) = profile
            .map(|(organization, source)| {
                (
                    Some(namespace_string(organization)),
                    Some(source_string(source)),
                )
            })
            .unwrap_or((None, None));
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE generations SET state = 'superseded'
             WHERE server = ?1 AND state = 'active'",
            [server],
        )?;
        let promoted = transaction.execute(
            "UPDATE generations
             SET state = 'active', completed_at = ?1,
                 entry_count = ?2, unique_item_count = ?2,
                 organization = COALESCE(?3, organization),
                 compatibility_fallback =
                   CASE WHEN source = 'da3' AND ?4 = 'da2' THEN 1 ELSE 0 END,
                 source = COALESCE(?4, source), last_error = ?5
             WHERE server = ?6 AND generation = ?7 AND state = 'staging'",
            params![
                completed_at,
                searchable_item_count,
                organization,
                source,
                warning,
                server,
                generation
            ],
        )?;
        if promoted != 1 {
            anyhow::bail!("namespace index generation is not staging");
        }
        transaction.commit()?;
        tracing::info!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            entry_count = searchable_item_count,
            unique_item_count = searchable_item_count,
            effective_organization = organization.unwrap_or(""),
            effective_source = source.unwrap_or(""),
            warning = warning.unwrap_or(""),
            activation_duration_ms = activation_started.elapsed().as_millis() as u64,
            "activated namespace index generation"
        );
        Ok(())
    }

    fn fail_generation(&self, server: &str, generation: u64, error: &str) -> anyhow::Result<()> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        self.connection.execute(
            "UPDATE generations SET state = 'failed', last_error = ?1
             WHERE server = ?2 AND generation = ?3 AND state = 'staging'",
            params![error, server, generation],
        )?;
        tracing::warn!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            error,
            "marked namespace index generation failed"
        );
        Ok(())
    }

    fn discard_empty_generation(&self, server: &str, generation: u64) -> anyhow::Result<bool> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        Ok(self.connection.execute(
            "DELETE FROM generations
             WHERE server = ?1 AND generation = ?2 AND state = 'staging'
               AND NOT EXISTS (
                   SELECT 1 FROM entries
                   WHERE entries.server = generations.server
                     AND entries.generation = generations.generation
               )
               AND NOT EXISTS (
                   SELECT 1 FROM entries_fts
                   WHERE entries_fts.server = generations.server
                     AND entries_fts.generation = generations.generation
               )",
            params![server, generation],
        )? == 1)
    }

    fn obsolete_servers(&self) -> anyhow::Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT server FROM generations
             WHERE state IN ('superseded', 'failed')",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn has_obsolete_generations(&self, server: &str) -> anyhow::Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM generations
                     WHERE server = ?1 AND state IN ('superseded', 'failed')
                 )",
                [server],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn clear_server(&mut self, server: &str) -> anyhow::Result<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute("DELETE FROM entries_fts WHERE server = ?1", [server])?;
        transaction.execute("DELETE FROM generations WHERE server = ?1", [server])?;
        transaction.commit()?;
        Ok(())
    }

    fn status_rows(&self, server: &str) -> anyhow::Result<Vec<DbStatus>> {
        let mut statement = self.connection.prepare(
            "SELECT generation, state, organization, source, started_at,
                        completed_at, entry_count, unique_item_count, last_error
                 FROM generations
                 WHERE server = ?1 AND state IN ('active', 'staging', 'failed')
                 ORDER BY CASE state WHEN 'active' THEN 0 WHEN 'staging' THEN 1 ELSE 2 END,
                          generation DESC
                 ",
        )?;
        let rows = statement.query_map([server], |row| {
            Ok(DbStatus {
                generation: row.get::<_, i64>(0)? as u64,
                state: row.get(1)?,
                organization: parse_namespace(&row.get::<_, String>(2)?),
                source: parse_source(&row.get::<_, String>(3)?),
                started_at: row.get(4)?,
                completed_at: row.get(5)?,
                entry_count: row.get::<_, i64>(6)? as u64,
                unique_item_count: row.get::<_, i64>(7)? as u64,
                last_error: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn active_profile(&self, server: &str) -> anyhow::Result<Option<StoredIndexProfile>> {
        self.connection
            .query_row(
                "SELECT organization, source, compatibility_fallback
                 FROM generations
                 WHERE server = ?1 AND state = 'active'
                 ORDER BY generation DESC
                 LIMIT 1",
                [server],
                |row| {
                    Ok(StoredIndexProfile {
                        organization: parse_namespace(&row.get::<_, String>(0)?),
                        source: parse_source(&row.get::<_, String>(1)?),
                        compatibility_fallback: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn search_generation(&self, server: &str) -> anyhow::Result<Option<u64>> {
        self.connection
            .query_row(
                "SELECT generation FROM generations
                 WHERE server = ?1 AND state IN ('active', 'staging')
                 ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END,
                          generation DESC
                 LIMIT 1",
                [server],
                |row| row.get::<_, i64>(0).map(|value| value as u64),
            )
            .optional()
            .map_err(Into::into)
    }

    fn search(
        &self,
        server: &str,
        generation: u64,
        query: &str,
        mode: i32,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let normalized_query = normalize_query(query);
        if mode == 1 {
            return self.search_exact(server, generation, &normalized_query, limit);
        }
        let fts_compatible = normalized_query
            .split_whitespace()
            .all(|term| term.chars().count() >= 3);
        if normalized_query.chars().count() >= 3 && fts_compatible && mode != 2 {
            return self.search_full_text(server, generation, &normalized_query, limit);
        }
        let mut sql = format!(
            "SELECT e.item_id, e.display_name, e.kind, e.breadcrumbs FROM entries e
             WHERE e.server = ? AND e.generation = {generation}"
        );
        let mut values = vec![server.to_string()];
        match mode {
            2 => {
                let pattern = format!("{}%", escape_like(&normalized_query));
                sql.push_str(
                    " AND (e.display_name_norm LIKE ? ESCAPE '\\'
                        OR e.item_id_norm LIKE ? ESCAPE '\\')",
                );
                values.extend([pattern.clone(), pattern]);
            }
            _ => {
                let pattern = format!("%{}%", escape_like(&normalized_query));
                sql.push_str(
                    " AND (e.display_name_norm LIKE ? ESCAPE '\\'
                        OR e.item_id_norm LIKE ? ESCAPE '\\'
                        OR e.breadcrumbs LIKE ? ESCAPE '\\')",
                );
                values.extend([pattern.clone(), pattern.clone(), pattern]);
            }
        }
        sql.push_str(
            " ORDER BY CASE
                 WHEN e.display_name_norm = ? THEN 0
                 WHEN e.item_id_norm = ? THEN 1
                 WHEN e.display_name_norm LIKE ? ESCAPE '\\' THEN 2
                 WHEN e.item_id_norm LIKE ? ESCAPE '\\' THEN 3
                 WHEN e.display_name_norm LIKE ? ESCAPE '\\' THEN 4
                 WHEN e.item_id_norm LIKE ? ESCAPE '\\' THEN 5
                 ELSE 6 END,
                 length(e.display_name_norm), e.display_name_norm, e.item_id_norm
             LIMIT ",
        );
        values.extend([
            normalized_query.clone(),
            normalized_query.clone(),
            format!("{}%", escape_like(&normalized_query)),
            format!("{}%", escape_like(&normalized_query)),
            format!("%{}%", escape_like(&normalized_query)),
            format!("%{}%", escape_like(&normalized_query)),
        ]);
        sql.push_str(&(limit.saturating_add(1)).to_string());
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values.iter()), |row| {
            let kind = match row.get::<_, i64>(2)? {
                1 => InventoryNodeKind::Item,
                2 => InventoryNodeKind::BranchAndItem,
                value => {
                    return Err(rusqlite::Error::InvalidParameterName(format!(
                        "unknown indexed node kind {value}"
                    )));
                }
            };
            let breadcrumbs = serde_json::from_str(&row.get::<_, String>(3)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(IndexedMatch {
                item_id: row.get(0)?,
                display_name: row.get(1)?,
                kind,
                breadcrumbs,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn search_exact(
        &self,
        server: &str,
        generation: u64,
        normalized_query: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let candidate_limit = i64::from(limit.saturating_add(1));
        let mut candidates: HashMap<String, (SearchCandidate, IndexedMatch)> = HashMap::new();

        for column in ["display_name_norm", "item_id_norm"] {
            let sql = format!(
                "SELECT e.item_id, e.display_name, e.display_name_norm,
                        e.item_id_norm, e.kind, e.breadcrumbs
                 FROM entries e
                 WHERE e.server = ?1 AND e.generation = ?2 AND e.{column} = ?3
                 ORDER BY length(e.display_name_norm), e.display_name_norm,
                          e.item_id_norm, e.item_id
                 LIMIT ?4"
            );
            let mut statement = self.connection.prepare(&sql)?;
            let rows = statement.query_map(
                params![server, generation, normalized_query, candidate_limit],
                |row| {
                    let item_id = row.get::<_, String>(0)?;
                    let display_name = row.get::<_, String>(1)?;
                    let display_name_norm = row.get::<_, String>(2)?;
                    let item_id_norm = row.get::<_, String>(3)?;
                    let kind = parse_indexed_kind(row.get::<_, i64>(4)?)?;
                    let breadcrumbs = parse_indexed_breadcrumbs(row.get::<_, String>(5)?)?;
                    let candidate = SearchCandidate {
                        rank: SearchRank {
                            tier: search_rank(normalized_query, &display_name_norm, &item_id_norm),
                            display_name_len: display_name_norm.chars().count(),
                            display_name_norm,
                            item_id_norm,
                        },
                        item_id: item_id.clone(),
                    };
                    Ok((
                        candidate,
                        IndexedMatch {
                            item_id,
                            display_name,
                            kind,
                            breadcrumbs,
                        },
                    ))
                },
            )?;
            for row in rows {
                let (candidate, value) = row?;
                candidates
                    .entry(value.item_id.clone())
                    .or_insert((candidate, value));
            }
        }

        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        candidates.truncate(usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX));
        Ok(candidates.into_iter().map(|(_, value)| value).collect())
    }

    fn search_full_text(
        &self,
        server: &str,
        generation: u64,
        normalized_query: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let fts_query = build_fts_query(normalized_query);
        let mut statement = self.connection.prepare(
            "SELECT item_id, display_name
             FROM entries_fts
             WHERE entries_fts MATCH ?1 AND server = ?2 AND generation = ?3",
        )?;
        let rows = statement.query_map(params![fts_query, server, generation], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let capacity = usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX);
        let mut candidates = BinaryHeap::with_capacity(capacity);
        for row in rows {
            let (item_id, display_name) = row?;
            let display_name_norm = normalize_query(&display_name);
            let item_id_norm = normalize_query(&item_id);
            let candidate = SearchCandidate {
                rank: SearchRank {
                    tier: search_rank(normalized_query, &display_name_norm, &item_id_norm),
                    display_name_len: display_name_norm.chars().count(),
                    display_name_norm,
                    item_id_norm,
                },
                item_id,
            };
            if candidates.len() < capacity {
                candidates.push(candidate);
            } else if candidates.peek().is_some_and(|worst| candidate < *worst) {
                candidates.pop();
                candidates.push(candidate);
            }
        }
        drop(statement);

        let mut statement = self.connection.prepare(
            "SELECT display_name, kind, breadcrumbs
             FROM entries
             WHERE server = ?1 AND generation = ?2 AND item_id = ?3",
        )?;
        let mut matches = Vec::with_capacity(candidates.len());
        for candidate in candidates.into_sorted_vec() {
            let result =
                statement.query_row(params![server, generation, candidate.item_id], |row| {
                    let kind = parse_indexed_kind(row.get::<_, i64>(1)?)?;
                    let breadcrumbs = parse_indexed_breadcrumbs(row.get::<_, String>(2)?)?;
                    Ok(IndexedMatch {
                        item_id: candidate.item_id.clone(),
                        display_name: row.get(0)?,
                        kind,
                        breadcrumbs,
                    })
                });
            match result {
                Ok(value) => matches.push(value),
                Err(rusqlite::Error::QueryReturnedNoRows) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(matches)
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

fn search_rank(query: &str, display_name_norm: &str, item_id_norm: &str) -> u8 {
    if display_name_norm == query {
        0
    } else if item_id_norm == query {
        1
    } else if display_name_norm.starts_with(query) {
        2
    } else if item_id_norm.starts_with(query) {
        3
    } else if display_name_norm.contains(query) {
        4
    } else if item_id_norm.contains(query) {
        5
    } else {
        6
    }
}

fn build_fts_query(query: &str) -> String {
    query
        .split_whitespace()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn parse_indexed_kind(value: i64) -> rusqlite::Result<InventoryNodeKind> {
    match value {
        1 => Ok(InventoryNodeKind::Item),
        2 => Ok(InventoryNodeKind::BranchAndItem),
        value => Err(rusqlite::Error::InvalidParameterName(format!(
            "unknown indexed node kind {value}"
        ))),
    }
}

fn parse_indexed_breadcrumbs(value: String) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(error))
    })
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

#[cfg(test)]
fn cleanup_obsolete_generations(
    path: &Path,
    server: &str,
    background_tasks: &BackgroundTasks,
) -> anyhow::Result<CleanupStats> {
    cleanup_obsolete_generations_coordinated(
        path,
        server,
        background_tasks,
        Arc::new(Mutex::new(())),
        Arc::new(Mutex::new(HashSet::new())),
    )
}

fn cleanup_checkpoint(
    connection: &Connection,
    writer_gate: &Mutex<()>,
    active_builds: &Mutex<HashSet<String>>,
    path: &Path,
    server: &str,
) -> rusqlite::Result<(i64, i64)> {
    let writer_guard = writer_gate
        .lock()
        .map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
    let active = active_builds
        .lock()
        .map(|builds| !builds.is_empty())
        .unwrap_or(true);
    let result = if active {
        tracing::debug!(
            process_id = std::process::id(),
            database = %path.display(),
            server,
            "skipping namespace index cleanup checkpoint while a build is active"
        );
        Err(rusqlite::Error::ExecuteReturnedResults)
    } else {
        connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
    };
    drop(writer_guard);
    result
}

fn cleanup_obsolete_generations_coordinated(
    path: &Path,
    server: &str,
    background_tasks: &BackgroundTasks,
    writer_gate: Arc<Mutex<()>>,
    active_builds: Arc<Mutex<HashSet<String>>>,
) -> anyhow::Result<CleanupStats> {
    let cleanup_started = Instant::now();
    let read_only = IndexDb::open_read_only(path)?;
    if !read_only.has_obsolete_generations(server)? {
        tracing::debug!(
            process_id = std::process::id(),
            database = %path.display(),
            server,
            "skipped namespace index cleanup because no obsolete generations exist"
        );
        return Ok(CleanupStats::default());
    }
    let mut connection = None;
    let mut stats = CleanupStats::default();
    loop {
        if background_tasks.is_shutting_down() {
            stats.stopped_for_shutdown = true;
            break;
        }
        let build_active = || {
            active_builds
                .lock()
                .map(|builds| !builds.is_empty())
                .unwrap_or(true)
        };
        if build_active() {
            stats.deferred_for_build = true;
            tracing::info!(
                process_id = std::process::id(),
                database = %path.display(),
                server,
                "deferring namespace index cleanup while an index build is active"
            );
            break;
        }
        if !read_only.has_obsolete_generations(server)? {
            break;
        }
        #[cfg(test)]
        background_tasks.wait_for_cleanup_writer_gate_hook();
        let writer_guard = writer_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
        if build_active() {
            stats.deferred_for_build = true;
            tracing::info!(
                process_id = std::process::id(),
                database = %path.display(),
                server,
                "deferring namespace index cleanup after waiting for the writer gate"
            );
            drop(writer_guard);
            break;
        }
        if !read_only.has_obsolete_generations(server)? {
            drop(writer_guard);
            break;
        }
        #[cfg(test)]
        background_tasks.wait_for_cleanup_batch_hook();
        if connection.is_none() {
            let opened = Connection::open(path)?;
            opened.pragma_update(None, "foreign_keys", true)?;
            opened.pragma_update(None, "journal_mode", "WAL")?;
            opened.busy_timeout(Duration::from_secs(5))?;
            connection = Some(opened);
        }
        let transaction = connection
            .as_mut()
            .expect("cleanup connection initialized")
            .transaction()?;
        let fts_entries = transaction.execute(
            "DELETE FROM entries_fts
             WHERE rowid IN (
                 SELECT f.rowid
                 FROM entries_fts f
                 INNER JOIN generations g
                   ON g.server = f.server AND g.generation = f.generation
                 WHERE g.server = ?1 AND g.state IN ('superseded', 'failed')
                 LIMIT ?2
             )",
            params![server, CLEANUP_BATCH_SIZE as i64],
        )?;
        let entries = transaction.execute(
            "DELETE FROM entries
             WHERE rowid IN (
                 SELECT e.rowid
                 FROM entries e
                 INNER JOIN generations g
                   ON g.server = e.server AND g.generation = e.generation
                 WHERE g.server = ?1 AND g.state IN ('superseded', 'failed')
                 LIMIT ?2
             )",
            params![server, CLEANUP_BATCH_SIZE as i64],
        )?;
        let generations = transaction.execute(
            "DELETE FROM generations
             WHERE rowid IN (
                 SELECT g.rowid
                 FROM generations g
                 WHERE g.server = ?1 AND g.state IN ('superseded', 'failed')
                   AND NOT EXISTS (
                       SELECT 1 FROM entries e
                       WHERE e.server = g.server AND e.generation = g.generation
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM entries_fts f
                       WHERE f.server = g.server AND f.generation = g.generation
                   )
                 LIMIT ?2
             )",
            params![server, CLEANUP_BATCH_SIZE as i64],
        )?;
        transaction.commit()?;

        stats.batches = stats.batches.saturating_add(1);
        stats.fts_entries = stats.fts_entries.saturating_add(fts_entries as u64);
        stats.entries = stats.entries.saturating_add(entries as u64);
        stats.generations = stats.generations.saturating_add(generations as u64);
        drop(writer_guard);
        if fts_entries == 0 && entries == 0 && generations == 0 {
            break;
        }
        std::thread::sleep(CLEANUP_BATCH_PAUSE);
    }
    let checkpoint = connection.as_ref().map(|connection| {
        cleanup_checkpoint(
            connection,
            writer_gate.as_ref(),
            active_builds.as_ref(),
            path,
            server,
        )
    });
    tracing::info!(
        process_id = std::process::id(),
        database = %path.display(),
        server,
        batches = stats.batches,
        entries_deleted = stats.entries,
        fts_entries_deleted = stats.fts_entries,
        generations_deleted = stats.generations,
        stopped_for_shutdown = stats.stopped_for_shutdown,
        deferred_for_build = stats.deferred_for_build,
        checkpoint = ?checkpoint,
        duration_ms = cleanup_started.elapsed().as_millis() as u64,
        "completed namespace index obsolete-generation cleanup"
    );
    Ok(stats)
}

async fn run_scheduled_cleanup(
    path: PathBuf,
    server: String,
    background_tasks: Arc<BackgroundTasks>,
    cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    coordination: Arc<DatabaseCoordination>,
) {
    let mut shutdown = background_tasks.subscribe();
    let mut consecutive_failures = 0_u32;
    loop {
        let should_run = cleanup_tasks
            .lock()
            .map(|mut tasks| {
                let task = tasks.entry(server.clone()).or_default();
                task.requested = false;
                !background_tasks.is_shutting_down() && !*shutdown.borrow()
            })
            .unwrap_or(false);
        if !should_run {
            if let Ok(mut tasks) = cleanup_tasks.lock() {
                tasks.remove(&server);
            }
            return;
        }

        let cleanup_path = path.clone();
        let cleanup_server = server.clone();
        let background_tasks_for_blocking = Arc::clone(&background_tasks);
        let coordination_for_blocking = Arc::clone(&coordination);
        let result = match tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if background_tasks_for_blocking
                .panic_next_cleanup_worker
                .swap(false, Ordering::AcqRel)
            {
                panic!("injected namespace index cleanup worker panic");
            }
            cleanup_obsolete_generations_coordinated(
                &cleanup_path,
                &cleanup_server,
                background_tasks_for_blocking.as_ref(),
                Arc::clone(&coordination_for_blocking.writer_gate),
                Arc::clone(&coordination_for_blocking.active_builds),
            )
        })
        .await
        {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!(
                "namespace index cleanup worker failed: {error}"
            )),
        };
        match result {
            Ok(stats) if stats.deferred_for_build => {
                if let Ok(mut tasks) = cleanup_tasks.lock()
                    && let Some(task) = tasks.get_mut(&server)
                {
                    task.requested = true;
                }
                tracing::debug!(
                    process_id = std::process::id(),
                    database = %path.display(),
                    server,
                    "namespace index cleanup remains pending until builds terminate"
                );
                let notified = coordination.build_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let build_active = coordination
                    .active_builds
                    .lock()
                    .map(|builds| !builds.is_empty())
                    .unwrap_or(true);
                if !build_active {
                    continue;
                }
                #[cfg(test)]
                background_tasks.wait_for_cleanup_notification_hook().await;
                if *shutdown.borrow() {
                    return;
                }
                tokio::select! {
                    _ = &mut notified => {}
                    _ = shutdown.changed() => return,
                }
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                #[cfg(test)]
                if let Ok(mut tasks) = cleanup_tasks.lock()
                    && let Some(task) = tasks.get_mut(&server)
                {
                    task.failures = task.failures.saturating_add(1);
                }
                let retry = consecutive_failures <= CLEANUP_RETRY_LIMIT
                    && !background_tasks.is_shutting_down();
                tracing::warn!(
                    process_id = std::process::id(),
                    database = %path.display(),
                    server,
                    error = %error,
                    attempt = consecutive_failures,
                    retry,
                    "namespace index obsolete-generation cleanup failed"
                );
                if retry {
                    let multiplier = 2_u32.pow(consecutive_failures.saturating_sub(1));
                    tokio::time::sleep(CLEANUP_RETRY_INITIAL_BACKOFF.saturating_mul(multiplier))
                        .await;
                    continue;
                }
            }
        }

        let rerun = cleanup_tasks
            .lock()
            .map(|mut tasks| {
                let rerun = tasks
                    .get(&server)
                    .is_some_and(|task| task.requested && !background_tasks.is_shutting_down());
                if !rerun {
                    tasks.remove(&server);
                }
                rerun
            })
            .unwrap_or(false);
        if !rerun {
            return;
        }
        consecutive_failures = 0;
    }
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
        spawn_cleanup_worker_if_idle(
            Arc::clone(&self.active),
            self.path.clone(),
            Arc::clone(&self.background_tasks),
            Arc::clone(&self.cleanup_tasks),
            Arc::clone(&self.coordination),
            false,
        );
    }
}

fn spawn_cleanup_worker_if_idle(
    active: Arc<AtomicBool>,
    path: PathBuf,
    background_tasks: Arc<BackgroundTasks>,
    cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    coordination: Arc<DatabaseCoordination>,
    reject_spawn: bool,
) {
    if active.swap(true, Ordering::AcqRel) {
        return;
    }
    let server = match cleanup_tasks.lock() {
        Ok(mut tasks) => {
            let server = tasks
                .iter()
                .find_map(|(server, task)| task.requested.then(|| server.clone()));
            if let Some(server) = &server
                && let Some(task) = tasks.get_mut(server)
            {
                task.running = true;
            }
            server
        }
        Err(_) => None,
    };
    let Some(server) = server else {
        active.store(false, Ordering::Release);
        return;
    };
    let worker_active = Arc::clone(&active);
    let worker_path = path.clone();
    let worker_tasks = Arc::clone(&background_tasks);
    let worker_cleanup_tasks = Arc::clone(&cleanup_tasks);
    let worker_coordination = Arc::clone(&coordination);
    let worker_background_tasks = Arc::clone(&background_tasks);
    let spawned = !reject_spawn
        && background_tasks.spawn(async move {
            let _worker_guard = CleanupWorkerGuard {
                active: worker_active,
                path: worker_path,
                background_tasks: worker_tasks,
                cleanup_tasks: Arc::clone(&worker_cleanup_tasks),
                coordination: Arc::clone(&worker_coordination),
            };
            run_scheduled_cleanup(
                path,
                server,
                worker_background_tasks,
                worker_cleanup_tasks,
                worker_coordination,
            )
            .await;
        });
    if !spawned && let Ok(mut tasks) = cleanup_tasks.lock() {
        tasks.retain(|_, task| !task.running);
        active.store(false, Ordering::Release);
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
    foreground_users: Arc<Mutex<HashMap<String, usize>>>,
    pause_overlays: Arc<Mutex<HashMap<String, PauseOverlayState>>>,
    foreground_metrics: Arc<Mutex<HashMap<String, ForegroundMetricState>>>,
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

fn storage_diagnostics_for_path(path: &Path) -> StorageDiagnostics {
    let main_bytes = fs::metadata(path).map_or(0, |metadata| metadata.len());
    let wal_bytes = fs::metadata(IndexDb::sqlite_sidecar_path(path, "-wal"))
        .map_or(0, |metadata| metadata.len());
    let shm_bytes = fs::metadata(IndexDb::sqlite_sidecar_path(path, "-shm"))
        .map_or(0, |metadata| metadata.len());
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let free_bytes = fs2::available_space(parent).ok();
    StorageDiagnostics {
        main_bytes,
        wal_bytes,
        shm_bytes,
        free_bytes,
        last_commit_latency_ms: None,
    }
}

impl<C: OpcClient> IndexManager<C> {
    pub fn new(client: Arc<C>, settings: ResolvedIndexConfig) -> Self {
        let cache_capacity = settings.query_cache_capacity.max(1);
        let host_metrics = default_host_metrics_provider(&settings.database_path);
        let coordination = database_coordination(&settings.database_path);
        tracing::debug!(
            process_id = std::process::id(),
            database = %settings.database_path.display(),
            configured_servers = ?settings.servers,
            enabled = settings.enabled,
            concurrency = settings.concurrency,
            "created namespace index manager"
        );
        Self {
            client,
            settings,
            database: Arc::new(Mutex::new(None)),
            coordination: Arc::clone(&coordination),
            writer_gate: Arc::clone(&coordination.writer_gate),
            build_changed: Arc::clone(&coordination.build_changed),
            build_locks: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(Mutex::new(HashMap::new())),
            active_builds: Arc::clone(&coordination.active_builds),
            pending_cancels: Arc::new(Mutex::new(HashSet::new())),
            promoting: Arc::new(Mutex::new(HashSet::new())),
            foreground_users: Arc::new(Mutex::new(HashMap::new())),
            pause_overlays: Arc::new(Mutex::new(HashMap::new())),
            foreground_metrics: Arc::new(Mutex::new(HashMap::new())),
            cache: Arc::new(Mutex::new(QueryCache {
                values: HashMap::new(),
                order: VecDeque::new(),
                capacity: cache_capacity,
            })),
            host_metrics,
            background_tasks: Arc::new(BackgroundTasks::new()),
            cleanup_tasks: Arc::new(Mutex::new(HashMap::new())),
            cleanup_worker_active: Arc::new(AtomicBool::new(false)),
            background_started: AtomicBool::new(false),
            #[cfg(test)]
            reject_next_build_spawn: AtomicBool::new(false),
            #[cfg(test)]
            reject_next_cleanup_spawn: AtomicBool::new(false),
            #[cfg(test)]
            build_reservation_hook: Mutex::new(None),
            #[cfg(test)]
            search_gate: Arc::new(Mutex::new(None)),
        }
    }

    pub fn max_results(&self) -> u32 {
        self.settings.max_results
    }

    pub fn with_host_metrics_provider(mut self, provider: Arc<dyn HostMetricsProvider>) -> Self {
        self.host_metrics = provider;
        self
    }

    pub fn record_foreground_operation(
        &self,
        server: &str,
        elapsed: Duration,
        error: bool,
        bad_quality: bool,
    ) {
        self.record_foreground_operation_with_health(server, elapsed, error, bad_quality, error);
    }

    pub fn record_foreground_operation_with_health(
        &self,
        server: &str,
        elapsed: Duration,
        error: bool,
        bad_quality: bool,
        health_failure: bool,
    ) {
        if let Ok(mut metrics) = self.foreground_metrics.lock() {
            metrics
                .entry(server.to_string())
                .or_default()
                .record_health_at(
                    Instant::now(),
                    elapsed.as_millis().try_into().unwrap_or(u64::MAX),
                    error,
                    bad_quality,
                    health_failure,
                );
        }
    }

    #[cfg(test)]
    fn install_search_gate(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.search_gate.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }

    #[cfg(test)]
    fn install_build_reservation_hook(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (started, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::sync_channel(0);
        *self.build_reservation_hook.lock().unwrap() = Some(Arc::new(BuildReservationHook {
            started,
            release: Mutex::new(release_rx),
            fired: AtomicBool::new(false),
        }));
        (started_rx, release)
    }

    #[cfg(test)]
    fn wait_for_build_reservation_hook(&self) {
        let hook = self
            .build_reservation_hook
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

    pub fn start_background_indexing(self: &Arc<Self>) {
        if !self.settings.enabled || self.settings.paused {
            return;
        }
        if self.background_started.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.settings.servers.is_empty() {
            return;
        }
        let manager = Arc::clone(self);
        let mut shutdown = self.background_tasks.subscribe();
        self.background_tasks.spawn(async move {
            let startup_grace = Duration::from_secs(manager.settings.startup_grace_period_seconds);
            if !startup_grace.is_zero() {
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(startup_grace) => {}
                }
            }

            loop {
                if *shutdown.borrow() {
                    break;
                }
                let mut delay = Duration::from_secs(60);
                for server in manager.settings.servers.clone() {
                    if *shutdown.borrow() {
                        break;
                    }
                    manager.refresh_if_due(&server).await;
                    delay = delay.min(manager.background_refresh_delay(&server).await);
                }
                if *shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    _ = shutdown.changed() => {}
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        });
    }

    pub async fn shutdown_background_indexing(&self) {
        self.background_tasks.request_shutdown();
        if let Ok(runtime) = self.runtime.lock() {
            for state in runtime.values() {
                if let Some(control) = state
                    .build
                    .as_ref()
                    .and_then(|build| build.control.as_ref())
                {
                    control.cancel();
                }
            }
        }
        self.background_tasks.wait_for_idle().await;
    }

    async fn background_refresh_delay(&self, server: &str) -> Duration {
        if let Ok(runtime) = self.runtime.lock()
            && let Some(retry_after) = runtime.get(server).and_then(|state| state.retry_after)
            && let Ok(remaining) = retry_after.duration_since(SystemTime::now())
        {
            return remaining.max(Duration::from_secs(1));
        }

        match self.status(server).await {
            Ok(status) => match status.state {
                IndexState::Ready | IndexState::Stale => {
                    if status.state == IndexState::Stale && !self.maintenance_window_is_open() {
                        return if self.settings.maintenance_windows.is_empty() {
                            Duration::from_secs(1)
                        } else {
                            Duration::from_secs(60)
                        };
                    }
                    let scheduled =
                        Duration::from_secs(self.settings.refresh_interval_seconds.max(1))
                            .saturating_add(deterministic_jitter(
                                server,
                                self.settings.schedule_jitter_seconds,
                            ));
                    status
                        .completed_at
                        .as_deref()
                        .and_then(parse_timestamp)
                        .and_then(|completed| {
                            SystemTime::now()
                                .duration_since(completed)
                                .ok()
                                .map(|elapsed| scheduled.saturating_sub(elapsed))
                        })
                        .unwrap_or(Duration::from_secs(1))
                }
                IndexState::Refreshing | IndexState::Partial => Duration::from_secs(30),
                IndexState::Promoting => Duration::from_secs(1),
                IndexState::Failed => {
                    retry_delay(server, 1, false, self.settings.circuit_open_seconds)
                }
                IndexState::NotIndexed => match self.settings.initial_build_policy {
                    InitialBuildPolicy::Immediate => {
                        retry_delay(server, 1, false, self.settings.circuit_open_seconds)
                    }
                    InitialBuildPolicy::MaintenanceWindow
                        if !self.settings.maintenance_windows.is_empty() =>
                    {
                        Duration::from_secs(60)
                    }
                    InitialBuildPolicy::MaintenanceWindow | InitialBuildPolicy::Manual => {
                        Duration::from_secs(3600)
                    }
                },
            },
            Err(_) => retry_delay(server, 1, false, self.settings.circuit_open_seconds),
        }
    }

    async fn active_profile_changed(&self, server: &str) -> anyhow::Result<bool> {
        let stored_profile = match self.with_database_read(|db| db.active_profile(server)) {
            Ok(Some(stored_profile)) => stored_profile,
            Ok(None) => {
                tracing::warn!(
                    server = %server,
                    "active namespace index profile is unavailable"
                );
                return Ok(false);
            }
            Err(error) => {
                tracing::warn!(
                    server = %server,
                    error = %error,
                    "unable to inspect active namespace index profile"
                );
                return Ok(false);
            }
        };
        let capabilities = self
            .with_opc_timeout(
                "active profile capability probe",
                self.client.get_capabilities(server),
            )
            .await?;
        Ok(!index_profile_is_compatible(
            stored_profile.organization,
            stored_profile.source,
            stored_profile.compatibility_fallback,
            capabilities.organization,
            capabilities.source,
        ))
    }

    async fn refresh_if_due(self: &Arc<Self>, server: &str) {
        match self.status(server).await {
            Ok(status)
                if status.active_generation > 0 && status.state != IndexState::Refreshing =>
            {
                let profile_changed = match self.active_profile_changed(server).await {
                    Ok(profile_changed) => profile_changed,
                    Err(error) => {
                        tracing::warn!(
                            server = %server,
                            error = %error,
                            "unable to inspect namespace index profile before refresh"
                        );
                        return;
                    }
                };
                if profile_changed {
                    if !self.automatic_refresh_allowed(&status) {
                        tracing::debug!(
                            server = %server,
                            "automatic namespace index rebuild is waiting for a maintenance window"
                        );
                    } else {
                        if let Err(error) = self.with_database_write(|db| db.clear_server(server)) {
                            tracing::warn!(
                                server = %server,
                                error = %error,
                                "unable to invalidate namespace index after profile change"
                            );
                            return;
                        }
                        if let Ok(mut cache) = self.cache.lock() {
                            cache.clear_server(server);
                        }
                        if let Err(error) = self.refresh(server, true).await {
                            tracing::warn!(
                                server = %server,
                                error = %error,
                                "automatic namespace index rebuild after profile change failed"
                            );
                        }
                    }
                } else if matches!(
                    status.state,
                    IndexState::Stale | IndexState::Failed | IndexState::NotIndexed
                ) && self.automatic_refresh_allowed(&status)
                    && let Err(error) = self.refresh(server, false).await
                {
                    tracing::warn!(
                        server = %server,
                        error = %error,
                        "automatic namespace index refresh failed"
                    );
                }
            }
            Ok(status)
                if matches!(
                    status.state,
                    IndexState::NotIndexed
                        | IndexState::Partial
                        | IndexState::Stale
                        | IndexState::Failed
                ) =>
            {
                if self.automatic_refresh_allowed(&status)
                    && let Err(error) = self.refresh(server, false).await
                {
                    tracing::warn!(server = %server, error = %error, "automatic namespace index refresh failed");
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(server = %server, error = %error, "unable to inspect namespace index before refresh");
            }
        }
    }

    fn automatic_refresh_allowed(&self, status: &IndexStatus) -> bool {
        if status.active_generation == 0 {
            match self.settings.initial_build_policy {
                InitialBuildPolicy::Immediate => true,
                InitialBuildPolicy::Manual => {
                    tracing::info!(
                        server = %status.server,
                        "automatic namespace index build is disabled until a manual refresh"
                    );
                    false
                }
                InitialBuildPolicy::MaintenanceWindow => {
                    let allowed = self.maintenance_window_is_open();
                    if !allowed {
                        tracing::debug!(
                            server = %status.server,
                            "automatic namespace index build is waiting for a maintenance window"
                        );
                    }
                    allowed
                }
            }
        } else {
            self.settings.maintenance_windows.is_empty() || self.maintenance_window_is_open()
        }
    }

    fn maintenance_window_is_open(&self) -> bool {
        match parse_maintenance_windows(&self.settings.maintenance_windows) {
            Ok(windows) => maintenance_window_active(&windows, Local::now()),
            Err(error) => {
                tracing::warn!(error = %error, "invalid namespace index maintenance window");
                false
            }
        }
    }

    fn set_pause_overlay(&self, server: &str, maintenance: Option<bool>, health: Option<bool>) {
        let update_result = self.pause_overlays.lock().map(|mut overlays| {
            let state = overlays.entry(server.to_string()).or_default();
            if let Some(value) = maintenance {
                state.maintenance = value;
            }
            if let Some(value) = health {
                state.health = value;
            }
            if !state.maintenance && !state.health {
                overlays.remove(server);
            }
        });
        if update_result.is_err() {
            tracing::error!(
                server,
                "unable to update namespace index pause overlays because the overlay lock is poisoned"
            );
            return;
        }
        self.reconcile_pause_state(server);
    }

    fn clear_pause_overlays(&self, server: &str) {
        if self
            .pause_overlays
            .lock()
            .map(|mut overlays| {
                overlays.remove(server);
            })
            .is_err()
        {
            tracing::error!(
                server,
                "unable to clear namespace index pause overlays because the overlay lock is poisoned"
            );
        }
    }

    fn reconcile_pause_state(&self, server: &str) {
        let overlay = match self.pause_overlays.lock() {
            Ok(overlays) => overlays.get(server).copied().unwrap_or_default(),
            Err(_) => {
                tracing::error!(
                    server,
                    "unable to reconcile namespace index pause state because the overlay lock is poisoned"
                );
                return;
            }
        };
        let (control, reason) = match self.runtime.lock() {
            Ok(mut runtime) => {
                let Some(build) = runtime
                    .get_mut(server)
                    .and_then(|state| state.build.as_mut())
                else {
                    return;
                };
                let foreground = build.foreground_users > 0
                    || build
                        .quiet_until
                        .is_some_and(|deadline| deadline > Instant::now());
                let reason = if build.operator_paused {
                    Some(crate::controller::PauseReason::Operator)
                } else if foreground {
                    Some(crate::controller::PauseReason::Foreground)
                } else if overlay.maintenance {
                    Some(crate::controller::PauseReason::Maintenance)
                } else if overlay.health {
                    Some(crate::controller::PauseReason::OpcHealth)
                } else if let Some(crate::controller::ControllerState::Paused(reason)) =
                    build.controller_state
                {
                    Some(reason)
                } else {
                    None
                };
                build.pause_reason = reason;
                (build.control.clone(), reason)
            }
            Err(_) => {
                tracing::error!(
                    server,
                    "unable to reconcile namespace index pause state because the runtime lock is poisoned"
                );
                return;
            }
        };
        if let Some(control) = control {
            if reason.is_some() {
                control.pause();
            } else {
                control.resume();
            }
        }
    }

    pub fn foreground_guard(self: &Arc<Self>, server: &str) -> ForegroundGuard<C> {
        let foreground_users = self
            .foreground_users
            .lock()
            .map(|mut users| {
                let count = users.entry(server.to_string()).or_default();
                *count = count.saturating_add(1);
                *count
            })
            .unwrap_or(1);
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.foreground_users = foreground_users;
            build.quiet_until = None;
        }
        self.reconcile_pause_state(server);
        ForegroundGuard {
            manager: Arc::clone(self),
            server: server.to_string(),
        }
    }

    fn foreground_end(self: &Arc<Self>, server: &str) {
        let quiet_period = Duration::from_secs(self.settings.quiet_period_seconds);
        let runtime = Arc::clone(&self.runtime);
        let foreground_users = Arc::clone(&self.foreground_users);
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        let resume_server_name = server_name.clone();
        let decrement_runtime = Arc::clone(&runtime);
        let decrement = move || {
            if let Ok(mut users) = foreground_users.lock() {
                let remaining = users
                    .get_mut(&server_name)
                    .map(|count| {
                        *count = count.saturating_sub(1);
                        *count
                    })
                    .unwrap_or(0);
                if remaining == 0 {
                    users.remove(&server_name);
                }
            }
            if let Ok(mut states) = decrement_runtime.lock()
                && let Some(build) = states
                    .get_mut(&server_name)
                    .and_then(|state| state.build.as_mut())
            {
                build.foreground_users = foreground_users
                    .lock()
                    .ok()
                    .and_then(|users| users.get(&server_name).copied())
                    .unwrap_or(0);
                if build.foreground_users == 0 {
                    build.quiet_until = Some(Instant::now() + quiet_period);
                }
            }
        };
        decrement();
        self.reconcile_pause_state(server);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                tokio::time::sleep(quiet_period).await;
                let should_reconcile = if let Ok(mut states) = runtime.lock()
                    && let Some(build) = states
                        .get_mut(&resume_server_name)
                        .and_then(|state| state.build.as_mut())
                    && build.foreground_users == 0
                    && build
                        .quiet_until
                        .is_some_and(|deadline| deadline <= Instant::now())
                {
                    build.quiet_until = None;
                    true
                } else {
                    false
                };
                if should_reconcile {
                    manager.reconcile_pause_state(&resume_server_name);
                }
            });
        } else {
            if let Ok(mut states) = runtime.lock()
                && let Some(build) = states
                    .get_mut(&resume_server_name)
                    .and_then(|state| state.build.as_mut())
                && build.foreground_users == 0
                && build
                    .quiet_until
                    .is_some_and(|deadline| deadline <= Instant::now())
            {
                build.quiet_until = None;
            }
            self.reconcile_pause_state(server);
        }
    }

    pub async fn status(&self, server: &str) -> anyhow::Result<IndexStatus> {
        let configured = self.settings.servers.iter().any(|s| s == server);
        if !configured {
            return Ok(empty_status(server, false, IndexState::NotIndexed));
        }
        let sentinel_configured = self.settings.sentinel_tag.is_some();
        let is_promoting = self
            .promoting
            .lock()
            .ok()
            .is_some_and(|servers| servers.contains(server));
        let mut promotion_read_error = None;
        let rows = if is_promoting && self.settings.database_path != Path::new(":memory:") {
            match IndexDb::open_read_only(&self.settings.database_path)
                .and_then(|db| db.status_rows(server))
            {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server,
                        error = %error,
                        "unable to read namespace index status during promotion"
                    );
                    promotion_read_error = Some(error.to_string());
                    Vec::new()
                }
            }
        } else {
            self.with_database_read(|db| db.status_rows(server))?
        };
        let active_row = rows.iter().find(|row| row.state == "active").cloned();
        let staging_row = rows.iter().find(|row| row.state == "staging").cloned();
        let failed_row = rows.iter().find(|row| row.state == "failed").cloned();
        let failed_after_active = active_row.as_ref().and_then(|active| {
            failed_row
                .as_ref()
                .filter(|failed| failed.generation > active.generation)
                .cloned()
        });
        let (build, runtime_error) = {
            let runtime = self
                .runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
            let state = runtime.get(server);
            (
                state.and_then(|state| state.build.clone()),
                state.and_then(|state| state.last_error.clone()),
            )
        };
        let build_active = build.is_some();
        let build_started_at = build.as_ref().map(|value| value.started_at.clone());
        let last_success_at = active_row.as_ref().and_then(|row| row.completed_at.clone());
        let last_attempt_at = build_started_at
            .clone()
            .or_else(|| failed_row.as_ref().map(|row| row.started_at.clone()))
            .or_else(|| active_row.as_ref().map(|row| row.started_at.clone()));
        let storage = if is_promoting {
            storage_diagnostics_for_path(&self.settings.database_path)
        } else {
            self.storage_diagnostics()?
        };
        let database_bytes = storage
            .main_bytes
            .saturating_add(storage.wal_bytes)
            .saturating_add(storage.shm_bytes);
        let mut status = if let Some(build) = build {
            let row = active_row
                .clone()
                .or(staging_row.clone())
                .or(failed_row.clone());
            if let Some(row) = row {
                let state = if is_promoting {
                    IndexState::Promoting
                } else if active_row.is_some() {
                    IndexState::Refreshing
                } else if staging_row.is_some() {
                    IndexState::Partial
                } else {
                    IndexState::Failed
                };
                let mut status =
                    status_from_row(server, row, state, build.progress.clone(), database_bytes);
                status.started_at = Some(build.started_at);
                status
            } else {
                IndexStatus {
                    server: server.to_string(),
                    state: if is_promoting {
                        IndexState::Promoting
                    } else {
                        IndexState::Partial
                    },
                    configured: true,
                    active_generation: 0,
                    entry_count: build.progress.as_ref().map_or(0, |p| p.entries_seen),
                    unique_item_count: build.progress.as_ref().map_or(0, |p| p.unique_items),
                    started_at: Some(build.started_at),
                    completed_at: None,
                    last_error: None,
                    database_bytes,
                    organization: NamespaceOrganization::Unspecified,
                    source: BrowseSource::Unspecified,
                    progress: build.progress,
                    effective_limits: build.effective_limits,
                    controller_state: build.controller_state,
                    pause_reason: build.pause_reason,
                    recovery_deadline: build.recovery_deadline.map(instant_timestamp),
                    foreground_metrics: ForegroundMetrics::default(),
                    host_metrics: HostMetrics::default(),
                    health: HealthProbeState::Unavailable,
                    sentinel_configured,
                    storage: StorageDiagnostics::default(),
                    scheduler: SchedulerDiagnostics::default(),
                }
            }
        } else if let Some(row) = active_row.clone() {
            let stale = row
                .completed_at
                .as_deref()
                .and_then(parse_timestamp)
                .is_some_and(|completed| {
                    SystemTime::now()
                        .duration_since(completed)
                        .unwrap_or_default()
                        > Duration::from_secs(self.settings.refresh_interval_seconds)
                });
            let state = if failed_after_active.is_some() {
                IndexState::Failed
            } else if stale {
                IndexState::Stale
            } else {
                IndexState::Ready
            };
            let mut status = status_from_row(server, row, state, None, database_bytes);
            if let Some(failed) = failed_after_active {
                status.last_error = failed.last_error;
            }
            status
        } else if let Some(row) = staging_row {
            status_from_row(server, row, IndexState::Partial, None, database_bytes)
        } else if let Some(row) = failed_row {
            status_from_row(server, row, IndexState::Failed, None, database_bytes)
        } else {
            let mut status = empty_status(server, true, IndexState::NotIndexed);
            status.database_bytes = database_bytes;
            status
        };
        status.sentinel_configured = sentinel_configured;
        if !build_active && let Some(error) = runtime_error {
            status.state = IndexState::Failed;
            status.last_error = Some(error);
        }
        if let Some(error) = promotion_read_error {
            status.last_error = Some(error);
        }
        let active_count = self
            .foreground_users
            .lock()
            .ok()
            .and_then(|users| users.get(server).copied())
            .unwrap_or(0) as u64;
        status.foreground_metrics = self
            .foreground_metrics
            .lock()
            .ok()
            .and_then(|metrics| {
                metrics
                    .get(server)
                    .map(|value| value.snapshot(active_count))
            })
            .unwrap_or(ForegroundMetrics {
                active_count,
                ..ForegroundMetrics::default()
            });
        status.host_metrics = self.host_metrics.snapshot();
        status.storage = storage;
        let mut scheduler = SchedulerDiagnostics {
            next_refresh_at: last_success_at.as_deref().and_then(|completed| {
                parse_timestamp(completed).and_then(|completed| {
                    completed
                        .checked_add(
                            Duration::from_secs(self.settings.refresh_interval_seconds.max(1))
                                .saturating_add(deterministic_jitter(
                                    server,
                                    self.settings.schedule_jitter_seconds,
                                )),
                        )
                        .map(system_time_timestamp)
                })
            }),
            last_attempt_at,
            last_success_at,
            last_success_duration_ms: active_row.as_ref().and_then(|row| {
                row.completed_at
                    .as_deref()
                    .and_then(parse_timestamp)
                    .and_then(|completed| {
                        parse_timestamp(&row.started_at)
                            .and_then(|started| completed.duration_since(started).ok())
                    })
                    .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
            }),
            ..SchedulerDiagnostics::default()
        };
        if let Ok(runtime) = self.runtime.lock()
            && let Some(state) = runtime.get(server)
        {
            scheduler.retry_after = state.retry_after.map(system_time_timestamp);
            scheduler.consecutive_failures = state.consecutive_failures;
            scheduler.circuit_open = state.circuit_open;
            status.health = state.health;
            if let Some(build) = &state.build {
                status.effective_limits = build.effective_limits;
                status.controller_state = build.controller_state;
                status.pause_reason = build.pause_reason;
                status.recovery_deadline = build.recovery_deadline.map(instant_timestamp);
                status.storage.last_commit_latency_ms = build.last_commit_latency_ms;
            }
        }
        status.scheduler = scheduler;
        Ok(status)
    }

    pub async fn refresh(
        self: &Arc<Self>,
        server: &str,
        force: bool,
    ) -> anyhow::Result<IndexStatus> {
        self.require_configured(server)?;
        if self.background_tasks.is_shutting_down() {
            return self.status(server).await;
        }
        let storage = self.with_database_read(|db| Ok(db.storage_diagnostics()))?;
        if storage.free_bytes.is_some_and(|free| {
            free < self
                .settings
                .minimum_free_space_bytes
                .saturating_add(self.settings.storage_headroom_bytes)
        }) {
            anyhow::bail!(
                "insufficient free space for namespace index ({} bytes available, {} required)",
                storage.free_bytes.unwrap_or_default(),
                self.settings
                    .minimum_free_space_bytes
                    .saturating_add(self.settings.storage_headroom_bytes)
            );
        }
        self.load_persisted_retry_state(server)?;
        let build_ownership = {
            let foreground_users = self
                .foreground_users
                .lock()
                .map_err(|_| anyhow::anyhow!("index foreground lock poisoned"))?
                .get(server)
                .copied()
                .unwrap_or(0);
            let mut runtime = self
                .runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
            let state = runtime.entry(server.to_string()).or_default();
            let active_builds = self
                .active_builds
                .lock()
                .map_err(|_| anyhow::anyhow!("index active-build lock poisoned"))?
                .len();
            let backing_off = !force
                && state
                    .retry_after
                    .is_some_and(|retry| SystemTime::now() < retry);
            let circuit_open = !force && state.circuit_open;
            if state.build.is_some() || backing_off || circuit_open {
                None
            } else if active_builds >= self.settings.concurrency.max(1) as usize {
                anyhow::bail!("namespace index build concurrency limit reached");
            } else {
                let mut build_locks = self
                    .build_locks
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index build-lock registry poisoned"))?;
                if build_locks.contains_key(server) {
                    anyhow::bail!(
                        "namespace index build lock is already held in this process for server {server}"
                    );
                }
                let lock = BuildFileLock::acquire(&self.settings.database_path, server)?;
                #[cfg(test)]
                self.wait_for_build_reservation_hook();
                let ownership = Arc::new(());
                let mut build_owners = self
                    .coordination
                    .build_owners
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index build-owner registry poisoned"))?;
                if build_owners.contains_key(server) {
                    anyhow::bail!(
                        "index build owner is already registered in this process for server {server}"
                    );
                }
                let mut active_builds = self
                    .active_builds
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index active-build lock poisoned"))?;
                if active_builds.len() >= self.settings.concurrency.max(1) as usize {
                    anyhow::bail!("namespace index build concurrency limit reached");
                }
                build_owners.insert(server.to_string(), Arc::clone(&ownership));
                active_builds.insert(server.to_string());
                build_locks.insert(server.to_string(), lock);
                state.build = Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: timestamp_now(),
                    foreground_users,
                    operator_paused: false,
                    quiet_until: None,
                    effective_limits: None,
                    controller_state: None,
                    pause_reason: None,
                    recovery_deadline: None,
                    last_commit_latency_ms: None,
                });
                state.last_error = None;
                Some(ownership)
            }
        };
        let Some(build_ownership) = build_ownership else {
            return self.status(server).await;
        };

        let initial_limits = self.initial_inventory_limits();
        let handle = match self
            .with_opc_timeout(
                "start inventory",
                self.client
                    .start_inventory(server, initial_limits.batch_size),
            )
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                if self.take_pending_cancel(server) {
                    self.finish_build_owned(server, &build_ownership, None);
                    return self.status(server).await;
                }
                self.record_start_failure(server, &build_ownership, &error.to_string())?;
                return Err(error);
            }
        };
        let control_was_cancelled_before_attach = handle.control.is_cancelled();
        if let Err(error) = handle.control.set_pacing(pacing_for_limits(initial_limits)) {
            let message = format!("unable to apply initial inventory pacing: {error}");
            let cancelled = self.take_pending_cancel(server)
                || (!control_was_cancelled_before_attach && handle.control.is_cancelled());
            handle.control.cancel();
            if cancelled {
                self.finish_build_owned(server, &build_ownership, None);
                return self.status(server).await;
            }
            self.record_start_failure(server, &build_ownership, &message)?;
            return Err(anyhow::anyhow!(message));
        }
        let control_result = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))
            .and_then(|mut runtime| {
                let build = runtime
                    .get_mut(server)
                    .and_then(|state| state.build.as_mut())
                    .ok_or_else(|| anyhow::anyhow!("index build disappeared before start"))?;
                build.control = Some(Arc::clone(&handle.control));
                Ok(())
            });
        if let Err(error) = control_result {
            let cancelled = self.take_pending_cancel(server)
                || (!control_was_cancelled_before_attach && handle.control.is_cancelled());
            handle.control.cancel();
            if cancelled {
                self.finish_build_owned(server, &build_ownership, None);
                return self.status(server).await;
            }
            self.finish_build_owned(server, &build_ownership, Some(error.to_string()));
            return Err(error);
        }
        if self.take_pending_cancel(server) {
            handle.control.cancel();
            self.finish_build_for_control_owned(server, &handle.control, &build_ownership, None);
            return self.status(server).await;
        }
        tracing::info!(
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            batch_size = initial_limits.batch_size,
            item_rate_per_second = initial_limits.item_rate_per_second,
            duty_cycle_percent = initial_limits.duty_cycle_percent,
            "started namespace index inventory"
        );
        if self.background_tasks.is_shutting_down() {
            handle.control.cancel();
            self.finish_build_owned(server, &build_ownership, None);
            return self.status(server).await;
        }
        let (organization, source) = match self
            .with_opc_timeout(
                "inventory capability probe",
                self.client.get_capabilities(server),
            )
            .await
        {
            Ok(capabilities) => (capabilities.organization, capabilities.source),
            Err(error) => {
                let cancelled =
                    !control_was_cancelled_before_attach && handle.control.is_cancelled();
                handle.control.cancel();
                if cancelled {
                    self.finish_build_for_control_owned(
                        server,
                        &handle.control,
                        &build_ownership,
                        None,
                    );
                    return self.status(server).await;
                }
                self.record_start_failure(server, &build_ownership, &error.to_string())?;
                return Err(error);
            }
        };
        let generation = self.with_database_write(|db| {
            db.start_generation(server, organization, source, &timestamp_now())
        });
        let generation = match generation {
            Ok(generation) => generation,
            Err(error) => {
                tracing::error!(
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    operation = "start_generation",
                    error = %error,
                    "namespace index database operation failed"
                );
                let cancelled =
                    !control_was_cancelled_before_attach && handle.control.is_cancelled();
                handle.control.cancel();
                if cancelled {
                    self.finish_build_for_control_owned(
                        server,
                        &handle.control,
                        &build_ownership,
                        None,
                    );
                    return self.status(server).await;
                }
                self.record_start_failure(server, &build_ownership, &error.to_string())?;
                return Err(error);
            }
        };
        self.schedule_cleanup(server);
        let control_result = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))
            .and_then(|runtime| {
                let build = runtime
                    .get(server)
                    .and_then(|state| state.build.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("index build disappeared before start"))?;
                if build
                    .control
                    .as_ref()
                    .is_some_and(|control| Arc::ptr_eq(control, &handle.control))
                {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("index build disappeared before start"))
                }
            });
        if let Err(error) = control_result {
            let cancelled = handle.control.is_cancelled();
            handle.control.cancel();
            self.abandon_generation(server, generation, &error.to_string());
            if cancelled {
                self.finish_build_owned(server, &build_ownership, None);
                return self.status(server).await;
            }
            self.finish_build_owned(server, &build_ownership, Some(error.to_string()));
            return Err(error);
        }
        if !control_was_cancelled_before_attach && handle.control.is_cancelled() {
            handle.control.cancel();
            self.abandon_generation(server, generation, "index build cancelled during startup");
            self.finish_build_for_control_owned(server, &handle.control, &build_ownership, None);
            return self.status(server).await;
        }
        self.reconcile_pause_state(server);
        if self.background_tasks.is_shutting_down() {
            handle.control.cancel();
            self.abandon_generation(server, generation, "gateway shutdown before index build");
            self.finish_build_owned(server, &build_ownership, None);
            return self.status(server).await;
        }
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        let control = Arc::clone(&handle.control);
        #[cfg(test)]
        let reject_spawn = self.reject_next_build_spawn.swap(false, Ordering::AcqRel);
        #[cfg(not(test))]
        let reject_spawn = false;
        let build_ownership_for_task = Arc::clone(&build_ownership);
        if reject_spawn
            || !self.background_tasks.spawn(async move {
                manager
                    .run_build(server_name, generation, handle, build_ownership_for_task)
                    .await;
            })
        {
            control.cancel();
            self.abandon_generation(server, generation, "index build task was not started");
            self.finish_build_for_control_owned(server, &control, &build_ownership, None);
        }
        self.status(server).await
    }

    fn controller_config(&self) -> ControllerConfig {
        ControllerConfig {
            floor: InventoryLimits {
                item_rate_per_second: self.settings.minimum_item_rate,
                batch_size: self.settings.minimum_batch_size,
                duty_cycle_percent: self.settings.minimum_duty_cycle_percent,
            },
            canary: InventoryLimits {
                item_rate_per_second: self.settings.canary_item_rate,
                batch_size: self.settings.canary_batch_size,
                duty_cycle_percent: self.settings.canary_duty_cycle_percent,
            },
            ceiling: InventoryLimits {
                item_rate_per_second: self.settings.item_rate_limit,
                batch_size: self.settings.inventory_batch_size,
                duty_cycle_percent: self.settings.duty_cycle_percent,
            },
            unlimited_item_rate: self.settings.item_rate_limit == 0,
            healthy_window: Duration::from_secs(
                self.settings.adaptive_healthy_window_seconds.max(1),
            ),
            recovery_delay: Duration::from_secs(
                self.settings.adaptive_recovery_delay_seconds.max(1),
            ),
            maximum_recovery_delay: Duration::from_secs(
                self.settings.adaptive_max_recovery_delay_seconds.max(1),
            ),
            foreground_latency_absolute_ms: self.settings.health_latency_threshold_ms.max(1),
        }
    }

    async fn with_opc_timeout<T>(
        &self,
        operation: &'static str,
        future: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let timeout = Duration::from_secs(self.settings.operation_timeout_seconds.max(1));
        tokio::time::timeout(timeout, future).await.map_err(|_| {
            anyhow::anyhow!(
                "OPC namespace index {operation} timed out after {} seconds",
                timeout.as_secs()
            )
        })?
    }

    fn mark_promoting(&self, server: &str) -> anyhow::Result<()> {
        self.promoting
            .lock()
            .map_err(|_| anyhow::anyhow!("index promotion lock poisoned"))?
            .insert(server.to_string());
        Ok(())
    }

    fn clear_promoting(&self, server: &str) {
        if let Err(error) = self.promoting.lock().map(|mut servers| {
            servers.remove(server);
        }) {
            tracing::error!(
                server = %server,
                error = %error,
                "unable to clear namespace index promotion state"
            );
        }
    }

    fn load_persisted_retry_state(&self, server: &str) -> anyhow::Result<()> {
        let persisted = self.with_database_read(|db| db.retry_state(server))?;
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
        let state = runtime.entry(server.to_string()).or_default();
        if state.build.is_none() {
            state.retry_after = persisted.0;
            state.consecutive_failures = persisted.1;
            state.circuit_open = persisted.2
                && state
                    .retry_after
                    .is_some_and(|retry| SystemTime::now() < retry);
        }
        Ok(())
    }

    fn storage_diagnostics(&self) -> anyhow::Result<StorageDiagnostics> {
        let database = self
            .database
            .lock()
            .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
        Ok(database
            .as_ref()
            .map_or_else(StorageDiagnostics::default, IndexDb::storage_diagnostics))
    }

    fn initial_inventory_limits(&self) -> InventoryLimits {
        if !self.settings.adaptive {
            return InventoryLimits {
                item_rate_per_second: self.settings.item_rate_limit,
                batch_size: self.settings.inventory_batch_size,
                duty_cycle_percent: self.settings.duty_cycle_percent,
            };
        }
        AdaptiveIndexController::new(self.controller_config(), Instant::now()).limits()
    }

    fn take_pending_cancel(&self, server: &str) -> bool {
        match self.pending_cancels.lock() {
            Ok(mut pending) => pending.remove(server),
            Err(error) => {
                tracing::error!(
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    error = %error,
                    "unable to read pending namespace index cancellation; cancelling build defensively"
                );
                true
            }
        }
    }

    fn clear_pending_cancel(&self, server: &str) {
        if let Err(error) = self.pending_cancels.lock().map(|mut pending| {
            pending.remove(server);
        }) {
            tracing::error!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                error = %error,
                "unable to clear pending namespace index cancellation"
            );
        }
    }

    pub async fn control(
        &self,
        server: &str,
        action: IndexControlAction,
    ) -> anyhow::Result<IndexStatus> {
        self.require_configured(server)?;
        if let Ok(mut runtime) = self.runtime.lock() {
            if let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
            {
                match action {
                    IndexControlAction::Pause => build.operator_paused = true,
                    IndexControlAction::Resume => {
                        build.operator_paused = false;
                        if build.foreground_users == 0
                            && build
                                .quiet_until
                                .is_some_and(|deadline| deadline <= Instant::now())
                        {
                            build.quiet_until = None;
                        }
                    }
                    IndexControlAction::Cancel => {
                        if let Some(control) = &build.control {
                            control.cancel();
                        } else {
                            self.pending_cancels
                                .lock()
                                .map_err(|_| anyhow::anyhow!("index cancel lock poisoned"))?
                                .insert(server.to_string());
                        }
                    }
                }
            }
        } else {
            return Err(anyhow::anyhow!("index runtime lock poisoned"));
        }
        if !matches!(action, IndexControlAction::Cancel) {
            self.reconcile_pause_state(server);
        }
        self.status(server).await
    }

    pub async fn search(
        &self,
        server: &str,
        query: &str,
        mode: i32,
        limit: u32,
    ) -> anyhow::Result<IndexedSearch> {
        if normalize_query(query).is_empty() {
            anyhow::bail!("search query must not be empty");
        }
        if !self.settings.servers.iter().any(|value| value == server) {
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status: empty_status(server, false, IndexState::NotIndexed),
            });
        }
        let limit = limit.max(1).min(self.settings.max_results);
        let status = self.status(server).await?;
        let normalized_query = normalize_query(query);
        let generation = if status.active_generation > 0 {
            Some(status.active_generation)
        } else if status.state == IndexState::Promoting {
            None
        } else {
            self.with_database_read(|db| db.search_generation(server))?
        };
        let Some(generation) = generation else {
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status,
            });
        };
        let key = CacheKey {
            server: server.to_string(),
            generation,
            query: normalized_query.clone(),
            mode,
            limit,
        };
        if status.active_generation == generation
            && let Some(mut value) = self
                .cache
                .lock()
                .map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?
                .get(&key)
        {
            value.status = status;
            return Ok(value);
        }

        let search_started = Instant::now();
        let database_path = self.settings.database_path.clone();
        let server_name = server.to_string();
        let query_name = query.to_string();
        #[cfg(test)]
        let search_gate = self.search_gate.lock().unwrap().take();
        let mut matches = if database_path == Path::new(":memory:") {
            self.with_database_read(|db| db.search(server, generation, query, mode, limit))?
        } else {
            tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some((started, release)) = search_gate {
                    let _ = started.send(());
                    let _ = release.blocking_recv();
                }
                let db = IndexDb::open_read_only(&database_path)?;
                db.search(&server_name, generation, &query_name, mode, limit)
            })
            .await??
        };
        let has_more = matches.len() > limit as usize;
        matches.truncate(limit as usize);
        tracing::debug!(
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            generation,
            mode,
            limit,
            matches = matches.len(),
            has_more,
            duration_ms = search_started.elapsed().as_millis() as u64,
            "completed namespace index search"
        );
        let value = IndexedSearch {
            matches,
            has_more,
            status,
        };
        if value.status.active_generation == generation {
            self.cache
                .lock()
                .map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?
                .insert(key, value.clone());
        }
        Ok(value)
    }

    async fn run_build(
        self: Arc<Self>,
        server: String,
        generation: u64,
        mut handle: InventoryHandle,
        ownership: Arc<()>,
    ) {
        let mut finalization = BuildFinalizationGuard::new(
            Arc::clone(&self),
            server.clone(),
            generation,
            Arc::clone(&handle.control),
            Arc::clone(&ownership),
        );
        let build_started = Instant::now();
        let maintenance_windows =
            match parse_maintenance_windows(&self.settings.maintenance_windows) {
                Ok(windows) => windows,
                Err(error) => {
                    handle.control.cancel();
                    let message = error.to_string();
                    self.fail_generation_and_schedule_cleanup(&server, generation, &message);
                    self.finish_build_for_control_owned(
                        &server,
                        &handle.control,
                        &ownership,
                        Some(message),
                    );
                    finalization.disarm();
                    return;
                }
            };
        let mut pending = Vec::new();
        let mut last_progress = InventoryProgress {
            branches_visited: 0,
            entries_seen: 0,
            unique_items: 0,
            active_time_ms: 0,
            paused_time_ms: 0,
            items_per_second: 0.0,
            estimated_remaining_ms: None,
        };
        let mut completed = false;
        let mut cancelled = false;
        let mut failed = None;
        let mut completion_warning = None;
        let mut completion_profile = None;
        let mut terminal = false;
        let mut accounted_active_time_ms = 0_u64;
        let mut persisted_item_count = 0_u64;
        let mut rate_limiter =
            ItemRateLimiter::new(self.settings.item_rate_limit, self.settings.burst_size);
        let mut controller = self
            .settings
            .adaptive
            .then(|| AdaptiveIndexController::new(self.controller_config(), build_started));
        let mut effective_duty_cycle_percent = self.settings.duty_cycle_percent;
        let mut last_commit_at = Instant::now();
        if let Some(controller) = controller.as_ref() {
            self.update_runtime_controller(&server, controller.limits(), controller.state(), None);
        }
        let mut next_health_probe = Instant::now();
        let mut health_backoff = Duration::from_secs(1);
        loop {
            if !pending.is_empty()
                && last_commit_at.elapsed()
                    >= Duration::from_millis(self.settings.commit_interval_ms.max(1))
            {
                match self.commit_pending_entries(&server, generation, &mut pending) {
                    Ok(inserted) => {
                        persisted_item_count = persisted_item_count.saturating_add(inserted);
                        last_commit_at = Instant::now();
                    }
                    Err(error) => {
                        failed = Some(error.to_string());
                        break;
                    }
                }
            }
            if !self
                .wait_for_maintenance(&handle.control, &server, &maintenance_windows)
                .await
            {
                cancelled = true;
                break;
            }
            if !self
                .wait_for_health(
                    &handle.control,
                    &server,
                    &mut next_health_probe,
                    &mut health_backoff,
                )
                .await
            {
                cancelled = true;
                break;
            }
            if let Some(controller) = controller.as_mut() {
                match self
                    .wait_for_controller_recovery(&handle.control, &server, controller)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        cancelled = true;
                        break;
                    }
                    Err(error) => {
                        handle.control.cancel();
                        failed = Some(error.to_string());
                        break;
                    }
                }
            }
            let event = match tokio::time::timeout(
                Duration::from_secs(self.settings.operation_timeout_seconds.max(1)),
                handle.stream.next(),
            )
            .await
            {
                Ok(event) => event,
                Err(_) => {
                    handle.control.cancel();
                    failed = Some(format!(
                        "inventory event timed out after {} seconds",
                        self.settings.operation_timeout_seconds.max(1)
                    ));
                    break;
                }
            };
            let Some(event) = event else {
                break;
            };
            match event {
                Ok(InventoryEvent::Entry(entry)) => {
                    if !rate_limiter.acquire(&handle.control).await {
                        cancelled = true;
                        break;
                    }
                    pending.push(entry);
                    if pending.len() >= self.settings.commit_batch_size as usize {
                        match self.commit_pending_entries(&server, generation, &mut pending) {
                            Ok(inserted) => {
                                persisted_item_count =
                                    persisted_item_count.saturating_add(inserted);
                                last_commit_at = Instant::now();
                            }
                            Err(error) => {
                                failed = Some(error.to_string());
                                break;
                            }
                        }
                    }
                }
                Ok(InventoryEvent::Progress(progress)) => {
                    let active_time_delta_ms = progress
                        .active_time_ms
                        .saturating_sub(accounted_active_time_ms);
                    accounted_active_time_ms = progress.active_time_ms;
                    last_progress = progress.clone();
                    if let Err(error) = self.with_database_write(|db| {
                        db.update_progress(&server, generation, &progress)
                    }) {
                        tracing::error!(
                            process_id = std::process::id(),
                            database = %self.settings.database_path.display(),
                            server = %server,
                            generation,
                            operation = "update_progress",
                            entries_seen = progress.entries_seen,
                            unique_items = progress.unique_items,
                            error = %error,
                            "namespace index database operation failed"
                        );
                        failed = Some(error.to_string());
                        break;
                    }
                    self.update_runtime_progress(&server, progress);
                    if active_time_delta_ms > 0
                        && !self
                            .enforce_duty_cycle(
                                &handle.control,
                                &server,
                                Duration::from_millis(active_time_delta_ms),
                                effective_duty_cycle_percent,
                            )
                            .await
                    {
                        cancelled = true;
                        break;
                    }
                }
                Ok(InventoryEvent::Slice(slice)) => {
                    if let Some(controller) = controller.as_mut() {
                        let decision = controller.observe(
                            Instant::now(),
                            self.controller_observation_for_slice(&server, &slice),
                        );
                        effective_duty_cycle_percent = decision.limits.duty_cycle_percent;
                        self.update_runtime_controller(
                            &server,
                            decision.limits,
                            decision.state,
                            decision.recovery_at,
                        );
                        if let Err(error) = handle
                            .control
                            .set_pacing(pacing_for_limits(decision.limits))
                        {
                            let message = format!(
                                "unable to update adaptive inventory pacing after slice {}: {error}",
                                slice.sequence
                            );
                            tracing::error!(
                                server = %server,
                                generation,
                                sequence = slice.sequence,
                                error = %error,
                                "namespace index pacing update failed"
                            );
                            handle.control.cancel();
                            failed = Some(message);
                            break;
                        }
                        tracing::debug!(
                            server = %server,
                            sequence = slice.sequence,
                            backend = ?slice.backend,
                            nodes_returned = slice.nodes_returned,
                            native_operations = slice.native_operations,
                            elapsed_ms = slice.elapsed_ms,
                            state = ?decision.state,
                            item_rate_per_second = decision.limits.item_rate_per_second,
                            batch_size = decision.limits.batch_size,
                            duty_cycle_percent = decision.limits.duty_cycle_percent,
                            "updated adaptive namespace inventory pacing"
                        );
                    }
                }
                Ok(InventoryEvent::Completed(result)) => {
                    terminal = true;
                    completed = result.complete;
                    cancelled = result.cancelled;
                    completion_profile = Some((result.organization, result.source));
                    if result.truncated {
                        failed = Some(
                            result
                                .warning
                                .unwrap_or_else(|| "inventory was truncated".to_string()),
                        );
                    } else {
                        completion_warning = result.warning;
                    }
                    break;
                }
                Err(error) => {
                    tracing::error!(
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server = %server,
                        generation,
                        operation = "insert_entries",
                        batch_size = pending.len(),
                        error = %error,
                        "namespace index database operation failed"
                    );
                    failed = Some(error.to_string());
                    break;
                }
            }
        }
        if !terminal && failed.is_none() {
            failed = Some("inventory stream ended before completion".to_string());
        }
        if !pending.is_empty() && failed.is_none() {
            match self.commit_pending_entries(&server, generation, &mut pending) {
                Ok(inserted) => {
                    persisted_item_count = persisted_item_count.saturating_add(inserted);
                }
                Err(error) => {
                    tracing::error!(
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server = %server,
                        generation,
                        operation = "insert_entries",
                        batch_size = pending.len(),
                        error = %error,
                        "namespace index database operation failed"
                    );
                    failed = Some(error.to_string());
                }
            }
        }

        if let Some(error) = failed {
            self.fail_generation_and_schedule_cleanup(&server, generation, &error);
            tracing::error!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server = %server,
                generation,
                duration_ms = build_started.elapsed().as_millis() as u64,
                error = %error,
                "namespace index build failed"
            );
            self.finish_build_for_control_owned(&server, &handle.control, &ownership, Some(error));
        } else if completed && !cancelled && !handle.control.is_cancelled() {
            let result = match self.mark_promoting(&server) {
                Ok(()) => {
                    let completed_at = timestamp_now();
                    let result = self.with_database_write(|db| {
                        db.promote_with_profile(
                            &server,
                            generation,
                            &completed_at,
                            persisted_item_count,
                            completion_profile,
                            completion_warning.as_deref(),
                        )
                    });
                    self.clear_promoting(&server);
                    result
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => {
                    self.schedule_cleanup(&server);
                    if let Ok(mut cache) = self.cache.lock() {
                        cache.clear_server(&server);
                    }
                    tracing::info!(
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server = %server,
                        generation,
                        duration_ms = build_started.elapsed().as_millis() as u64,
                        entries_seen = last_progress.entries_seen,
                        unique_items = last_progress.unique_items,
                        persisted_items = persisted_item_count,
                        "namespace index build completed"
                    );
                    if let Some(warning) = completion_warning {
                        tracing::warn!(
                            process_id = std::process::id(),
                            database = %self.settings.database_path.display(),
                            server = %server,
                            generation,
                            warning = %warning,
                            "namespace index completed with warning"
                        );
                    }
                    self.finish_build_for_control_owned(&server, &handle.control, &ownership, None);
                }
                Err(error) => {
                    tracing::error!(
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server = %server,
                        generation,
                        operation = "promote",
                        error = %error,
                        "namespace index database operation failed"
                    );
                    self.fail_generation_and_schedule_cleanup(
                        &server,
                        generation,
                        &error.to_string(),
                    );
                    self.finish_build_for_control_owned(
                        &server,
                        &handle.control,
                        &ownership,
                        Some(error.to_string()),
                    );
                }
            }
        } else {
            self.abandon_generation(&server, generation, "namespace index build cancelled");
            tracing::warn!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server = %server,
                generation,
                duration_ms = build_started.elapsed().as_millis() as u64,
                cancelled,
                "namespace index build cancelled"
            );
            self.finish_build_for_control_owned(&server, &handle.control, &ownership, None);
        }
        finalization.disarm();
    }

    fn commit_pending_entries(
        &self,
        server: &str,
        generation: u64,
        pending: &mut Vec<InventoryEntry>,
    ) -> anyhow::Result<u64> {
        if pending.is_empty() {
            return Ok(0);
        }
        let started = Instant::now();
        let result = self.with_database_write(|db| db.insert_entries(server, generation, pending));
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.last_commit_latency_ms =
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX));
        }
        if result.is_ok() {
            pending.clear();
        }
        result
    }

    async fn enforce_duty_cycle(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        work_duration: Duration,
        duty_cycle_percent: u8,
    ) -> bool {
        let duty = u32::from(duty_cycle_percent.clamp(1, 100));
        if duty >= 100 {
            return !control.is_cancelled();
        }
        let pause_duration = work_duration.mul_f64(f64::from(100 - duty) / f64::from(duty));
        let overlays = self
            .pause_overlays
            .lock()
            .ok()
            .and_then(|values| values.get(server).copied())
            .unwrap_or_default();
        let can_pause = self.runtime.lock().ok().is_some_and(|runtime| {
            runtime
                .get(server)
                .and_then(|state| state.build.as_ref())
                .is_some_and(|build| Self::build_can_resume(build, overlays))
        });
        if can_pause {
            control.pause();
        }
        let still_running = wait_with_cancellation(control, pause_duration).await;
        if can_pause
            && still_running
            && self.runtime.lock().ok().is_some_and(|runtime| {
                runtime
                    .get(server)
                    .and_then(|state| state.build.as_ref())
                    .is_some_and(|build| Self::build_can_resume(build, overlays))
            })
        {
            control.resume();
        }
        still_running
    }

    async fn wait_for_maintenance(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        windows: &[MaintenanceWindow],
    ) -> bool {
        let mut outside_window =
            !windows.is_empty() && !maintenance_window_active(windows, Local::now());
        self.set_pause_overlay(server, Some(outside_window), None);
        while outside_window {
            if !wait_with_cancellation(control, Duration::from_secs(1)).await {
                return false;
            }
            outside_window = !maintenance_window_active(windows, Local::now());
            self.set_pause_overlay(server, Some(outside_window), None);
        }
        true
    }

    async fn wait_for_health(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        next_probe: &mut Instant,
        backoff: &mut Duration,
    ) -> bool {
        loop {
            if control.is_cancelled() {
                self.set_pause_overlay(server, None, Some(false));
                return false;
            }
            let now = Instant::now();
            let sentinel_due = self.settings.sentinel_tag.is_some()
                && self
                    .runtime
                    .lock()
                    .ok()
                    .and_then(|runtime| {
                        runtime
                            .get(server)
                            .and_then(|state| state.sentinel_checked_at)
                    })
                    .is_none_or(|checked| {
                        now.duration_since(checked)
                            >= Duration::from_secs(self.settings.sentinel_probe_interval_seconds)
                    });
            if now < *next_probe && !sentinel_due {
                if self.health_overlay_active(server) {
                    if !wait_with_cancellation(control, next_probe.saturating_duration_since(now))
                        .await
                    {
                        self.set_pause_overlay(server, None, Some(false));
                        return false;
                    }
                    continue;
                }
                return true;
            }
            let started = Instant::now();
            self.set_pause_overlay(server, None, Some(true));
            let result = self
                .with_opc_timeout(
                    "health capability probe",
                    self.client.get_capabilities(server),
                )
                .await;
            let elapsed = started.elapsed();
            let sentinel = if let Some(tag) = self.settings.sentinel_tag.as_deref() {
                match self
                    .with_opc_timeout(
                        "health sentinel read",
                        self.client.read_tag_values(server, vec![tag.to_string()]),
                    )
                    .await
                {
                    Ok(values) if values.len() == 1 => {
                        let healthy = values[0].quality.eq_ignore_ascii_case("good");
                        Some((
                            healthy,
                            if healthy {
                                None
                            } else {
                                Some("sentinel quality is not Good".to_string())
                            },
                        ))
                    }
                    Ok(_) => Some((false, Some("sentinel read returned no value".to_string()))),
                    Err(error) => Some((false, Some(error.to_string()))),
                }
            } else {
                None
            };
            let healthy = result.is_ok()
                && elapsed <= Duration::from_millis(self.settings.health_latency_threshold_ms);
            let healthy = healthy && sentinel.as_ref().is_none_or(|(healthy, _)| *healthy);
            self.update_health_state(
                server,
                if self.settings.sentinel_tag.is_none() {
                    HealthProbeState::Unavailable
                } else if healthy {
                    HealthProbeState::Healthy
                } else {
                    HealthProbeState::Unhealthy
                },
            );
            if healthy {
                self.set_pause_overlay(server, None, Some(false));
                *backoff = Duration::from_secs(1);
                *next_probe = Instant::now()
                    + Duration::from_secs(self.settings.health_probe_interval_seconds.max(1));
                return true;
            }

            let reason = match result {
                Ok(_) => sentinel.and_then(|(_, reason)| reason).unwrap_or_else(|| {
                    format!(
                        "health probe exceeded {} ms ({} ms)",
                        self.settings.health_latency_threshold_ms,
                        elapsed.as_millis()
                    )
                }),
                Err(error) => format!("health probe failed: {error}"),
            };
            tracing::warn!(server = %server, reason = %reason, "deferring namespace inventory");
            let delay = (*backoff).min(Duration::from_secs(300));
            *backoff = backoff
                .checked_mul(2)
                .unwrap_or(Duration::from_secs(300))
                .min(Duration::from_secs(300));
            *next_probe = Instant::now() + delay;
            if !wait_with_cancellation(control, delay).await {
                self.set_pause_overlay(server, None, Some(false));
                return false;
            }
        }
    }

    async fn wait_for_controller_recovery(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        controller: &mut AdaptiveIndexController,
    ) -> anyhow::Result<bool> {
        while matches!(
            controller.state(),
            crate::controller::ControllerState::Paused(_)
        ) {
            if control.is_cancelled() {
                return Ok(false);
            }
            let wait = controller
                .recovery_at()
                .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|| Duration::from_millis(100));
            if !wait_with_cancellation(control, wait).await {
                return Ok(false);
            }
            let decision =
                controller.observe(Instant::now(), self.controller_observation(server, false));
            self.update_runtime_controller(
                server,
                decision.limits,
                decision.state,
                decision.recovery_at,
            );
            control
                .set_pacing(pacing_for_limits(decision.limits))
                .map_err(|error| {
                    anyhow::anyhow!("unable to update inventory pacing while recovering: {error}")
                })?;
            if decision.transitioned {
                tracing::info!(
                    server,
                    state = ?decision.state,
                    reason = ?decision.reason,
                    recovery_deadline = ?decision.recovery_at.map(instant_timestamp),
                    "updated adaptive namespace inventory state while paused"
                );
            }
        }
        Ok(true)
    }

    fn health_overlay_active(&self, server: &str) -> bool {
        self.pause_overlays
            .lock()
            .ok()
            .and_then(|overlays| overlays.get(server).copied())
            .is_some_and(|overlay| overlay.health)
    }

    fn update_health_state(&self, server: &str, health: HealthProbeState) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(state) = runtime.get_mut(server)
        {
            state.health = health;
            state.sentinel_checked_at = Some(Instant::now());
        }
    }

    fn update_runtime_controller(
        &self,
        server: &str,
        limits: InventoryLimits,
        state: crate::controller::ControllerState,
        recovery_deadline: Option<Instant>,
    ) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.effective_limits = Some(limits);
            build.controller_state = Some(state);
            build.recovery_deadline = recovery_deadline;
            drop(runtime);
            self.reconcile_pause_state(server);
        }
    }

    fn foreground_active(&self, server: &str) -> bool {
        self.foreground_users
            .lock()
            .ok()
            .and_then(|users| users.get(server).copied())
            .is_some_and(|count| count > 0)
    }

    fn controller_observation(&self, server: &str, inventory_error: bool) -> ControllerObservation {
        let now = Instant::now();
        let foreground_failure_window =
            Duration::from_secs(self.settings.health_probe_interval_seconds.max(1));
        let (foreground, recent_foreground_failure, recent_foreground_bad_quality) = self
            .foreground_metrics
            .lock()
            .ok()
            .and_then(|metrics| {
                metrics.get(server).map(|value| {
                    let active_count = self
                        .foreground_users
                        .lock()
                        .ok()
                        .and_then(|users| users.get(server).copied())
                        .unwrap_or(0) as u64;
                    (
                        value.snapshot(active_count),
                        value.recent_health_failure(now, foreground_failure_window),
                        value.recent_bad_quality(now, foreground_failure_window),
                    )
                })
            })
            .unwrap_or((ForegroundMetrics::default(), false, false));
        let host = self.host_metrics.snapshot();
        let health = self
            .runtime
            .lock()
            .ok()
            .and_then(|runtime| runtime.get(server).map(|state| state.health))
            .unwrap_or(HealthProbeState::Unavailable);
        let mut storage = self.storage_diagnostics().unwrap_or_default();
        storage.last_commit_latency_ms = self.runtime.lock().ok().and_then(|runtime| {
            runtime
                .get(server)
                .and_then(|state| state.build.as_ref())
                .and_then(|build| build.last_commit_latency_ms)
        });
        ControllerObservation {
            foreground_active: self.foreground_active(server),
            foreground_error: recent_foreground_failure,
            foreground_bad_quality: recent_foreground_bad_quality,
            foreground_latency_ms: foreground.latency_p95_ms,
            baseline_latency_ms: Some(self.settings.health_latency_threshold_ms),
            inventory_error: inventory_error || health == HealthProbeState::Unhealthy,
            host_cpu_percent: host.cpu_percent,
            available_memory_percent: host.available_memory_percent,
            disk_active_percent: host.disk_active_percent,
            disk_queue: host.disk_queue,
            database_commit_p95_ms: storage.last_commit_latency_ms,
            insufficient_disk_space: storage.free_bytes.is_some_and(|free| {
                free < self
                    .settings
                    .minimum_free_space_bytes
                    .saturating_add(self.settings.storage_headroom_bytes)
            }),
        }
    }

    fn controller_observation_for_slice(
        &self,
        server: &str,
        slice: &InventorySliceObservation,
    ) -> ControllerObservation {
        self.controller_observation(server, slice.native_operations == 0)
    }

    fn build_can_resume(build: &RuntimeBuild, overlays: PauseOverlayState) -> bool {
        build.foreground_users == 0
            && !build.operator_paused
            && build
                .quiet_until
                .is_none_or(|deadline| deadline <= Instant::now())
            && !overlays.maintenance
            && !overlays.health
            && !matches!(
                build.controller_state,
                Some(crate::controller::ControllerState::Paused(_))
            )
    }

    fn update_runtime_progress(&self, server: &str, progress: InventoryProgress) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.progress = Some(progress);
        }
    }

    #[cfg(test)]
    fn finish_build(&self, server: &str, error: Option<String>) {
        let ownership = self
            .coordination
            .build_owners
            .lock()
            .ok()
            .and_then(|owners| owners.get(server).cloned());
        self.finish_build_inner(server, None, ownership.as_ref(), error);
    }

    #[cfg(test)]
    fn finish_build_for_control(
        &self,
        server: &str,
        control: &Arc<dyn InventoryControl>,
        error: Option<String>,
    ) {
        let ownership = self
            .coordination
            .build_owners
            .lock()
            .ok()
            .and_then(|owners| owners.get(server).cloned());
        self.finish_build_inner(server, Some(control), ownership.as_ref(), error);
    }

    fn finish_build_owned(&self, server: &str, ownership: &Arc<()>, error: Option<String>) {
        self.finish_build_inner(server, None, Some(ownership), error);
    }

    fn finish_build_for_control_owned(
        &self,
        server: &str,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        error: Option<String>,
    ) {
        self.finish_build_inner(server, Some(control), Some(ownership), error);
    }

    fn finish_build_inner(
        &self,
        server: &str,
        control: Option<&Arc<dyn InventoryControl>>,
        ownership: Option<&Arc<()>>,
        error: Option<String>,
    ) {
        let owns_build = match self.runtime.lock() {
            Ok(mut runtime) => {
                let is_owner = if let Some(ownership) = ownership {
                    let token_matches = match self.coordination.build_owners.lock() {
                        Ok(owners) => owners
                            .get(server)
                            .is_some_and(|current| Arc::ptr_eq(current, ownership)),
                        Err(_) => {
                            tracing::error!(
                                process_id = std::process::id(),
                                database = %self.settings.database_path.display(),
                                server,
                                "unable to finalize namespace index build because the ownership registry is poisoned"
                            );
                            return;
                        }
                    };
                    let control_matches = match control {
                        Some(control) => runtime.get(server).is_none_or(|state| {
                            state.build.as_ref().is_none_or(|build| {
                                build
                                    .control
                                    .as_ref()
                                    .is_some_and(|current| Arc::ptr_eq(current, control))
                            })
                        }),
                        None => true,
                    };
                    token_matches && control_matches
                } else if let Some(state) = runtime.get(server) {
                    match control {
                        Some(control) => state
                            .build
                            .as_ref()
                            .and_then(|build| build.control.as_ref())
                            .is_some_and(|current| Arc::ptr_eq(current, control)),
                        None => state.build.is_some(),
                    }
                } else {
                    false
                };
                if is_owner && let Some(state) = runtime.get_mut(server) {
                    state.last_error = error.clone();
                    if error.is_some() {
                        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                        state.circuit_open =
                            state.consecutive_failures >= self.settings.circuit_failure_threshold;
                        state.retry_after = Some(
                            SystemTime::now()
                                + retry_delay(
                                    server,
                                    state.consecutive_failures,
                                    state.circuit_open,
                                    self.settings.circuit_open_seconds,
                                ),
                        );
                    } else {
                        state.retry_after = None;
                        state.consecutive_failures = 0;
                        state.circuit_open = false;
                    }
                }
                is_owner
            }
            Err(_) => {
                tracing::error!(
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    "unable to finalize namespace index build because the runtime lock is poisoned"
                );
                return;
            }
        };
        if owns_build {
            let _ = self.persist_retry_state(server);
            self.clear_pause_overlays(server);
            if let Ok(mut owners) = self.coordination.build_owners.lock()
                && ownership.is_none_or(|ownership| {
                    owners
                        .get(server)
                        .is_some_and(|current| Arc::ptr_eq(current, ownership))
                })
            {
                owners.remove(server);
            }
            self.clear_active_build(server);
            self.clear_build_lock(server);
            if let Ok(mut runtime) = self.runtime.lock()
                && let Some(state) = runtime.get_mut(server)
            {
                let _ = state.build.take();
            }
            self.schedule_cleanup(server);
            self.clear_pending_cancel(server);
        } else if control.is_some() {
            tracing::warn!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                "ignored completion from obsolete namespace index build"
            );
        }
    }

    fn record_start_failure(
        &self,
        server: &str,
        ownership: &Arc<()>,
        error: &str,
    ) -> anyhow::Result<()> {
        self.finish_build_owned(server, ownership, Some(error.to_string()));
        Ok(())
    }

    fn persist_retry_state(&self, server: &str) -> anyhow::Result<()> {
        let (retry_after, failures, circuit_open) = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?
            .get(server)
            .map(|state| {
                (
                    state.retry_after,
                    state.consecutive_failures,
                    state.circuit_open,
                )
            })
            .unwrap_or((None, 0, false));
        self.with_database_write(|db| {
            db.set_retry_state(server, retry_after, failures, circuit_open)
        })
    }

    fn clear_build_lock(&self, server: &str) {
        if let Ok(mut build_locks) = self.build_locks.lock() {
            build_locks.remove(server);
        }
    }

    fn clear_active_build(&self, server: &str) {
        if let Ok(mut active) = self.active_builds.lock() {
            active.remove(server);
            self.build_changed.notify_waiters();
        }
    }

    fn fail_generation_and_schedule_cleanup(&self, server: &str, generation: u64, error: &str) {
        match self.with_database_write(|db| db.fail_generation(server, generation, error)) {
            Ok(()) => self.schedule_cleanup(server),
            Err(database_error) => tracing::error!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                generation,
                error = %database_error,
                "unable to mark failed namespace index generation for cleanup"
            ),
        }
    }

    fn abandon_generation(&self, server: &str, generation: u64, reason: &str) {
        match self.with_database_write(|db| db.discard_empty_generation(server, generation)) {
            Ok(true) => {}
            Ok(false) => self.fail_generation_and_schedule_cleanup(server, generation, reason),
            Err(error) => tracing::error!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                generation,
                error = %error,
                "unable to abandon namespace index generation"
            ),
        }
    }

    fn schedule_cleanup(&self, server: &str) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let should_spawn = match self.cleanup_tasks.lock() {
            Ok(mut tasks) => {
                let task = tasks.entry(server.to_string()).or_default();
                task.requested = true;
                !task.running
            }
            Err(_) => {
                tracing::error!(
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    "namespace index cleanup registry lock is poisoned"
                );
                return;
            }
        };
        if !should_spawn || self.background_tasks.is_shutting_down() {
            return;
        }
        spawn_cleanup_worker_if_idle(
            Arc::clone(&self.cleanup_worker_active),
            self.settings.database_path.clone(),
            Arc::clone(&self.background_tasks),
            Arc::clone(&self.cleanup_tasks),
            Arc::clone(&self.coordination),
            {
                #[cfg(test)]
                {
                    self.reject_next_cleanup_spawn.swap(false, Ordering::AcqRel)
                }
                #[cfg(not(test))]
                {
                    false
                }
            },
        );
    }

    fn require_configured(&self, server: &str) -> anyhow::Result<()> {
        if !self.settings.servers.iter().any(|value| value == server) {
            anyhow::bail!("server is not configured for namespace indexing");
        }
        Ok(())
    }

    #[cfg(test)]
    fn with_database<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        self.with_database_read(operation)
    }

    fn with_database_read<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        let needs_open = self
            .database
            .lock()
            .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?
            .is_none();
        let cleanup_servers = if needs_open {
            let _writer_guard = self
                .writer_gate
                .lock()
                .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            self.initialize_database(&mut database)?
        } else {
            Vec::new()
        };
        let result = {
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            operation(database.as_mut().expect("database initialized"))
        };
        for server in cleanup_servers {
            self.schedule_cleanup(&server);
        }
        result
    }

    fn with_database_write<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        let _writer_guard = self
            .writer_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
        let (result, cleanup_servers) = {
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            let cleanup_servers = self.initialize_database(&mut database)?;
            (
                operation(database.as_mut().expect("database initialized")),
                cleanup_servers,
            )
        };
        for server in cleanup_servers {
            self.schedule_cleanup(&server);
        }
        result
    }

    fn initialize_database(&self, database: &mut Option<IndexDb>) -> anyhow::Result<Vec<String>> {
        if database.is_none() {
            tracing::debug!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                "initializing namespace index database handle"
            );
            *database = Some(IndexDb::open(&self.settings.database_path)?);
            Ok(database
                .as_ref()
                .expect("database initialized")
                .obsolete_servers()?)
        } else {
            Ok(Vec::new())
        }
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

fn status_from_row(
    server: &str,
    row: DbStatus,
    state: IndexState,
    progress: Option<InventoryProgress>,
    database_bytes: u64,
) -> IndexStatus {
    IndexStatus {
        server: server.to_string(),
        state,
        configured: true,
        active_generation: if row.state == "active" {
            row.generation
        } else {
            0
        },
        entry_count: row.entry_count,
        unique_item_count: row.unique_item_count,
        started_at: Some(row.started_at),
        completed_at: row.completed_at,
        last_error: row.last_error,
        database_bytes,
        organization: row.organization,
        source: row.source,
        progress,
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
    }
}

fn empty_status(server: &str, configured: bool, state: IndexState) -> IndexStatus {
    IndexStatus {
        server: server.to_string(),
        state,
        configured,
        active_generation: 0,
        entry_count: 0,
        unique_item_count: 0,
        started_at: None,
        completed_at: None,
        last_error: None,
        database_bytes: 0,
        organization: NamespaceOrganization::Unspecified,
        source: BrowseSource::Unspecified,
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
    }
}

fn is_quarantinable_index_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("unsupported namespace index schema version")
        || message.contains("invalid namespace index schema version")
        || message.contains("namespace index relational and full-text data are inconsistent")
        || message.contains("file is not a database")
        || message.contains("database disk image is malformed")
}

pub(crate) fn normalize_query(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
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

fn pacing_for_limits(limits: InventoryLimits) -> InventoryPacing {
    let min_interval = if limits.item_rate_per_second == 0 {
        Duration::ZERO
    } else {
        let numerator = u128::from(limits.batch_size.max(1)) * 1_000_000_000;
        let denominator = u128::from(limits.item_rate_per_second);
        Duration::from_nanos(numerator.div_ceil(denominator) as u64)
    };
    InventoryPacing {
        min_interval,
        item_rate_per_second: (limits.item_rate_per_second > 0)
            .then_some(limits.item_rate_per_second),
        batch_size: Some(limits.batch_size.clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE)),
    }
}

fn stable_server_hash(server: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in server.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    format!("{hash:016x}")
}

fn build_lock_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), |name| name.to_os_string());
    database_path.with_file_name(format!(
        "{}.{}.build.lock",
        file_name.to_string_lossy(),
        stable_server_hash(server)
    ))
}

#[cfg(windows)]
fn build_owner_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), |name| name.to_os_string());
    database_path.with_file_name(format!(
        "{}.{}.build.owner",
        file_name.to_string_lossy(),
        stable_server_hash(server)
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

fn deterministic_jitter(server: &str, maximum_seconds: u64) -> Duration {
    if maximum_seconds == 0 {
        return Duration::ZERO;
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in server.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    let range = maximum_seconds.saturating_add(1);
    Duration::from_secs(if range == 0 {
        maximum_seconds
    } else {
        hash % range
    })
}

fn retry_delay(
    server: &str,
    consecutive_failures: u32,
    circuit_open: bool,
    circuit_open_seconds: u64,
) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(8);
    let multiplier = 1_u64 << exponent;
    let base = RETRY_INITIAL_BACKOFF
        .checked_mul(multiplier as u32)
        .unwrap_or(RETRY_MAX_BACKOFF)
        .min(RETRY_MAX_BACKOFF);
    let jitter_limit = (base.as_secs() / 5).max(1);
    let jitter = deterministic_jitter(
        &format!("{server}:retry:{consecutive_failures}"),
        jitter_limit,
    );
    let exponential = base.saturating_add(jitter).min(RETRY_MAX_BACKOFF);
    if circuit_open {
        exponential.max(Duration::from_secs(circuit_open_seconds).min(RETRY_MAX_BACKOFF))
    } else {
        exponential
    }
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
    use crate::opc::{
        BrowseCapabilities, BrowsePage, InventoryCompleted, InventoryEntry, InventoryEvent,
        InventorySliceBackend, InventorySliceObservation, InventoryStream, OpcValue, TagValue,
        WriteResult,
    };
    use crate::test_support::MockOpcClient;
    use chrono::TimeZone;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::sync::Notify;

    fn settings(path: PathBuf) -> ResolvedIndexConfig {
        ResolvedIndexConfig {
            database_path: path,
            servers: vec!["S".into()],
            enabled: true,
            refresh_interval_seconds: 604_800,
            initial_build_policy: InitialBuildPolicy::Immediate,
            startup_grace_period_seconds: 0,
            schedule_jitter_seconds: 0,
            inventory_batch_size: 100,
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
            operation_timeout_seconds: 30,
            maintenance_windows: Vec::new(),
            concurrency: 1,
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

    fn synthetic_entries(prefix: &str, count: usize) -> Vec<InventoryEntry> {
        (0..count)
            .map(|index| {
                inventory_entry(&format!("{prefix}-{index}"), &format!("{prefix}.{index}"))
            })
            .collect()
    }

    #[test]
    fn normalization_and_timestamp_helpers_are_safe() {
        assert_eq!(normalize_query("  FCS0201   PV "), "fcs0201 pv");
        assert_eq!(escape_like(r"a%b_c\d"), r"a\%b\_c\\d");
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
        assert_eq!(
            namespace_string(NamespaceOrganization::Unspecified),
            "unspecified"
        );
        assert_eq!(namespace_string(NamespaceOrganization::Flat), "flat");
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
        assert_eq!(pacing.min_interval, Duration::from_millis(100));
        assert_eq!(pacing.item_rate_per_second, Some(100));
        assert_eq!(pacing.batch_size, Some(10));
        assert_eq!(
            pacing_for_limits(InventoryLimits {
                item_rate_per_second: 3,
                batch_size: 1,
                duty_cycle_percent: 1,
            })
            .min_interval,
            Duration::from_nanos(333_333_334)
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
    fn build_file_lock_is_exclusive_and_reusable() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let lock = BuildFileLock::acquire_with(&database, "S", |_file, _metadata| Ok(())).unwrap();
        assert!(build_lock_path(&database, "S").exists());
        assert!(BuildFileLock::is_held(&database, "S").unwrap());
        assert!(!BuildFileLock::is_held(&database, "T").unwrap());
        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("build lock is already held"));
        drop(lock);
        assert!(!BuildFileLock::is_held(&database, "S").unwrap());
        assert!(build_lock_path(&database, "S").exists());
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

        let duplicate_migration_path = directory.path().join("duplicate-migration.sqlite3");
        drop(IndexDb::open(&duplicate_migration_path).unwrap());
        let duplicate_migration = Connection::open(&duplicate_migration_path).unwrap();
        duplicate_migration
            .execute(
                "UPDATE index_meta SET value = '2' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
        drop(duplicate_migration);
        assert!(
            IndexDb::open_once(&duplicate_migration_path)
                .err()
                .expect("duplicate migration column should fail")
                .to_string()
                .contains("duplicate column")
        );

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
    fn sqlite_migrates_v2_and_records_confirmed_compatibility_fallbacks() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("v2.sqlite3");
        drop(IndexDb::open(&path).unwrap());
        let legacy = Connection::open(&path).unwrap();
        legacy
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 DROP TABLE entries;
                 DROP TABLE entries_fts;
                 DROP TABLE generations;
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
                 INSERT INTO generations (
                     server, generation, state, organization, source, started_at,
                     completed_at, entry_count, unique_item_count
                 ) VALUES ('S', 1, 'active', 'hierarchical', 'da2', '1', '2', 0, 0);
                 UPDATE index_meta SET value = '2' WHERE key = 'schema_version';",
            )
            .unwrap();
        drop(legacy);

        let mut migrated = IndexDb::open_once(&path).unwrap();
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
        assert!(
            !migrated
                .active_profile("S")
                .unwrap()
                .unwrap()
                .compatibility_fallback
        );

        let generation = migrated
            .start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da3,
                "3",
            )
            .unwrap();
        migrated
            .insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
            .unwrap();
        migrated
            .promote_with_profile(
                "S",
                generation,
                "4",
                1,
                Some((NamespaceOrganization::Hierarchical, BrowseSource::Da2)),
                Some("DA3 compatibility fallback"),
            )
            .unwrap();
        let profile = migrated.active_profile("S").unwrap().unwrap();
        assert_eq!(profile.source, BrowseSource::Da2);
        assert!(profile.compatibility_fallback);
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
            1
        );
        background_tasks.wait_for_cleanup_writer_gate_hook();
    }

    #[test]
    fn cleanup_rechecks_obsolete_data_after_waiting_for_the_writer_gate() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cleanup-obsolete-recheck.sqlite3");
        let mut db = IndexDb::open(&path).unwrap();
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
        assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_and_build_share_the_writer_gate_across_servers() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cross-server-gate.sqlite3");
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(path.clone());
        config.servers.push("T".into());
        let manager = Arc::new(IndexManager::new(client, config));
        manager
            .with_database(|db| {
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
        let manager = Arc::new(IndexManager::new(Arc::new(MockOpcClient::default()), {
            let mut config = settings(path);
            config.servers.push("T".into());
            config
        }));
        let (obsolete, active) = manager
            .with_database(|db| {
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
    async fn abandoning_a_populated_generation_defers_large_deletion_to_cleanup() {
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
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
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
            retry_delay("S", 1, false, 300)
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
    }

    #[test]
    fn background_indexing_respects_disabled_paused_and_idempotent_start() {
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

        let mut enabled = settings(directory.path().join("enabled.sqlite3"));
        enabled.servers.clear();
        let enabled = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            enabled,
        ));
        enabled.start_background_indexing();
        enabled.start_background_indexing();
        assert!(enabled.background_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn background_indexing_runs_initial_refresh_and_enters_delay_loop() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.refresh_interval_seconds = 1;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.start_background_indexing();
        wait_for_build(&manager, IndexState::Ready).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn background_indexing_does_not_start_first_build_without_window() {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.initial_build_policy = InitialBuildPolicy::MaintenanceWindow;
        config.maintenance_windows.clear();
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.start_background_indexing();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
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
            Duration::from_secs(30)
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
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
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
        config.servers.push("T".into());
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

        manager.start_background_indexing();
        stream_started.notified().await;
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
        initial_config.initial_build_policy = InitialBuildPolicy::MaintenanceWindow;
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
    async fn manager_reports_unconfigured_servers_without_scanning() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        let status = manager.status("Other").await.unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert!(!status.configured);
        let response = manager.search("Other", "tag", 3, 10).await.unwrap();
        assert!(response.matches.is_empty());
        assert_eq!(response.status.state, IndexState::NotIndexed);

        let configured = manager.search("S", "tag", 3, 0).await.unwrap();
        assert!(configured.matches.is_empty());
        assert!(configured.status.configured);

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
        drop(failing);

        let client = Arc::new(MockOpcClient::default());
        let restarted = Arc::new(IndexManager::new(Arc::clone(&client), config));
        let blocked = restarted.refresh("S", false).await.unwrap();
        assert_eq!(blocked.state, IndexState::NotIndexed);
        assert_eq!(blocked.scheduler.consecutive_failures, 1);
        assert!(blocked.scheduler.circuit_open);
        assert!(blocked.scheduler.retry_after.is_some());
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);

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
        let mut manual_config = settings(directory.path().join("manual.sqlite3"));
        manual_config.initial_build_policy = InitialBuildPolicy::Manual;
        let manual_manager = Arc::new(IndexManager::new(Arc::clone(&manual_client), manual_config));
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
        maintenance.initial_build_policy = InitialBuildPolicy::MaintenanceWindow;
        maintenance.maintenance_windows = vec!["00:00-00:00".into()];
        let maintenance = IndexManager::new(Arc::new(MockOpcClient::default()), maintenance);
        assert_eq!(
            maintenance.background_refresh_delay("S").await,
            Duration::from_secs(60)
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
        }

        let manager = IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        )
        .with_host_metrics_provider(Arc::new(FixedHostMetrics));
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
        assert!(
            manager
                .with_database(|db| db.status_rows("S"))
                .unwrap()
                .is_empty()
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
        maintenance_config.initial_build_policy = InitialBuildPolicy::MaintenanceWindow;
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
            refresh.await.unwrap()
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
        let mut background_config = settings(directory.path().join("background-shutdown.sqlite3"));
        background_config.initial_build_policy = InitialBuildPolicy::Manual;
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

        fn fail_pacing_on_call(&self, call: usize) {
            self.fail_pacing_on_call.store(call, Ordering::Release);
        }
    }

    struct VecInventoryStream {
        events: VecDeque<anyhow::Result<InventoryEvent>>,
    }

    struct PanickingInventoryStream;

    #[async_trait::async_trait]
    impl InventoryStream for PanickingInventoryStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            panic!("injected inventory stream panic");
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
    }

    #[async_trait::async_trait]
    impl OpcClient for LifecycleClient {
        async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
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
            anyhow::bail!("unused in index tests")
        }

        async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
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

    fn handle_with_control(
        events: VecDeque<anyhow::Result<InventoryEvent>>,
        control: Arc<RecordingInventoryControl>,
    ) -> InventoryHandle {
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
                configured: true,
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
}
