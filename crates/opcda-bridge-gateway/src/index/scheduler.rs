use super::{
    HealthProbeState, IndexDb, IndexManager, IndexState, IndexStatus, QueryCache,
    index_profile_is_compatible, parse_timestamp,
};
use crate::config::ResolvedIndexConfig;
use crate::controller::{InventoryLimits, default_host_metrics_provider};
use crate::opc::{InventoryControl, InventoryProgress, OpcClient};
use chrono::{DateTime, Local, Timelike};
use fs2::FileExt;
use rusqlite::{Connection, params};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime};

pub(super) const RETRY_INITIAL_BACKOFF: Duration = Duration::from_secs(300);

pub(super) const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(86_400);

pub(super) const CLEANUP_BATCH_SIZE: usize = 10_000;

pub(super) const CLEANUP_BATCH_PAUSE: Duration = Duration::from_millis(1);

pub(super) const CLEANUP_RETRY_LIMIT: u32 = 3;

pub(super) const CLEANUP_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(super) struct RuntimeBuild {
    pub(super) control: Option<Arc<dyn InventoryControl>>,
    pub(super) progress: Option<InventoryProgress>,
    pub(super) started_at: String,
    pub(super) foreground_users: usize,
    pub(super) operator_paused: bool,
    pub(super) quiet_until: Option<Instant>,
    pub(super) effective_limits: Option<InventoryLimits>,
    pub(super) controller_state: Option<crate::controller::ControllerState>,
    pub(super) pause_reason: Option<crate::controller::PauseReason>,
    pub(super) recovery_deadline: Option<Instant>,
    pub(super) last_commit_latency_ms: Option<u64>,
}

#[derive(Default)]
pub(super) struct RuntimeState {
    pub(super) build: Option<RuntimeBuild>,
    pub(super) retry_after: Option<SystemTime>,
    pub(super) last_error: Option<String>,
    pub(super) consecutive_failures: u32,
    pub(super) circuit_open: bool,
    pub(super) health: HealthProbeState,
    pub(super) sentinel_checked_at: Option<Instant>,
}

pub(super) struct BackgroundTasks {
    pub(super) state: Mutex<BackgroundTaskState>,
    pub(super) shutdown: tokio::sync::watch::Sender<bool>,
    pub(super) idle: tokio::sync::Notify,
    #[cfg(test)]
    pub(super) panic_next_cleanup_worker: AtomicBool,
    #[cfg(test)]
    pub(super) cleanup_batch_hook: Mutex<Option<Arc<CleanupBatchHook>>>,
    #[cfg(test)]
    pub(super) cleanup_writer_gate_hook: Mutex<Option<Arc<CleanupBatchHook>>>,
    #[cfg(test)]
    pub(super) cleanup_notification_hook: Mutex<Option<Arc<CleanupNotificationHook>>>,
}

#[derive(Default)]
pub(super) struct BackgroundTaskState {
    pub(super) active: usize,
    pub(super) shutting_down: bool,
}

pub(super) struct BackgroundTaskGuard {
    pub(super) tasks: Arc<BackgroundTasks>,
}

#[derive(Default)]
pub(super) struct CleanupTaskState {
    pub(super) running: bool,
    pub(super) requested: bool,
    #[cfg(test)]
    pub(super) failures: usize,
}

pub(super) struct DatabaseCoordination {
    pub(super) writer_gate: Arc<Mutex<()>>,
    pub(super) active_builds: Arc<Mutex<HashSet<String>>>,
    pub(super) build_owners: Arc<Mutex<HashMap<String, Arc<()>>>>,
    pub(super) build_changed: Arc<tokio::sync::Notify>,
}

pub(super) static DATABASE_COORDINATIONS: OnceLock<
    Mutex<HashMap<PathBuf, Weak<DatabaseCoordination>>>,
> = OnceLock::new();

#[cfg(test)]
pub(super) struct CleanupBatchHook {
    pub(super) started: std::sync::mpsc::SyncSender<()>,
    pub(super) release: Mutex<std::sync::mpsc::Receiver<()>>,
    pub(super) fired: AtomicBool,
}

#[cfg(test)]
pub(super) struct BuildReservationHook {
    pub(super) started: std::sync::mpsc::SyncSender<()>,
    pub(super) release: Mutex<std::sync::mpsc::Receiver<()>>,
    pub(super) fired: AtomicBool,
}

#[cfg(test)]
pub(super) struct CleanupNotificationHook {
    pub(super) started: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
    pub(super) fired: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MaintenanceWindow {
    pub(super) start_minute: u16,
    pub(super) end_minute: u16,
}

#[derive(Debug)]
pub(super) struct BuildFileLock {
    pub(super) file: Option<fs::File>,
    #[cfg(windows)]
    pub(super) owner_path: Option<PathBuf>,
}

pub(super) fn is_lock_conflict(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock || matches!(error.raw_os_error(), Some(32 | 33))
}

pub(super) fn build_lock_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), std::ffi::OsStr::to_os_string);
    database_path.with_file_name(format!(
        "{}.{}.build.lock",
        file_name.to_string_lossy(),
        stable_server_hash(server)
    ))
}

#[derive(Default)]
pub(super) struct CleanupStats {
    pub(super) batches: u64,
    pub(super) entries: u64,
    pub(super) fts_entries: u64,
    pub(super) generations: u64,
    pub(super) stopped_for_shutdown: bool,
    pub(super) deferred_for_build: bool,
}

pub(super) struct CleanupBatch {
    pub(super) entries: u64,
    pub(super) fts_entries: u64,
    pub(super) generations: u64,
}

pub(super) enum CleanupBatchResult {
    Shutdown,
    Deferred,
    NoObsoleteGenerations,
    Deleted(CleanupBatch),
}

pub(super) enum CleanupAttempt {
    Retry,
    Return,
    Finished,
}

pub(super) struct CleanupWorkerGuard {
    pub(super) active: Arc<AtomicBool>,
    pub(super) path: PathBuf,
    pub(super) background_tasks: Arc<BackgroundTasks>,
    pub(super) cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    pub(super) coordination: Arc<DatabaseCoordination>,
}

impl BackgroundTasks {
    pub(super) fn new() -> Self {
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
    pub(super) fn install_cleanup_notification_hook(
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
    pub(super) async fn wait_for_cleanup_notification_hook(&self) {
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

    pub(super) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub(super) fn is_shutting_down(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.shutting_down)
            .unwrap_or(true)
    }

    pub(super) fn request_shutdown(&self) {
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
    pub(super) fn panic_next_cleanup_worker(&self) {
        self.panic_next_cleanup_worker
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn install_cleanup_batch_hook(
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
    pub(super) fn wait_for_cleanup_batch_hook(&self) {
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
    pub(super) fn install_cleanup_writer_gate_hook(
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
    pub(super) fn wait_for_cleanup_writer_gate_hook(&self) {
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

    pub(super) fn spawn<F>(self: &Arc<Self>, future: F) -> bool
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

    pub(super) async fn wait_for_idle(&self) {
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

impl MaintenanceWindow {
    pub(super) fn parse(value: &str) -> anyhow::Result<Self> {
        let (start, end) = value
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("maintenance window must use HH:MM-HH:MM"))?;
        Ok(Self {
            start_minute: parse_clock(start)?,
            end_minute: parse_clock(end)?,
        })
    }

    pub(super) fn contains(self, minute: u16) -> bool {
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

pub(super) fn parse_clock(value: &str) -> anyhow::Result<u16> {
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

pub(super) fn parse_maintenance_windows(
    values: &[String],
) -> anyhow::Result<Vec<MaintenanceWindow>> {
    values
        .iter()
        .map(|value| MaintenanceWindow::parse(value))
        .collect()
}

pub(super) fn maintenance_window_active(
    windows: &[MaintenanceWindow],
    now: DateTime<Local>,
) -> bool {
    if windows.is_empty() {
        return false;
    }
    let minute = (now.hour() * 60 + now.minute()) as u16;
    windows
        .iter()
        .copied()
        .any(|window| window.contains(minute))
}

pub(super) async fn wait_with_cancellation(
    control: &Arc<dyn InventoryControl>,
    duration: Duration,
) -> bool {
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

impl BuildFileLock {
    pub(super) fn acquire(database_path: &Path, server: &str) -> anyhow::Result<Self> {
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

    pub(super) fn acquire_with<F>(
        database_path: &Path,
        server: &str,
        initialize: F,
    ) -> anyhow::Result<Self>
    where
        F: FnOnce(&mut fs::File, &[u8]) -> std::io::Result<()>,
    {
        Self::acquire_with_open(
            database_path,
            server,
            |lock_path| {
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(lock_path)
            },
            initialize,
        )
    }

    pub(super) fn acquire_with_open<F, O>(
        database_path: &Path,
        server: &str,
        open: O,
        initialize: F,
    ) -> anyhow::Result<Self>
    where
        F: FnOnce(&mut fs::File, &[u8]) -> std::io::Result<()>,
        O: FnOnce(&Path) -> std::io::Result<fs::File>,
    {
        let lock_path = build_lock_path(database_path, server);
        lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(fs::create_dir_all)
            .transpose()?;
        let mut file = match open(&lock_path) {
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

    pub(super) fn is_held(database_path: &Path, server: &str) -> anyhow::Result<bool> {
        Self::is_held_with(database_path, server, FileExt::try_lock_exclusive)
    }

    pub(super) fn is_held_with<F>(
        database_path: &Path,
        server: &str,
        try_lock: F,
    ) -> anyhow::Result<bool>
    where
        F: FnOnce(&fs::File) -> std::io::Result<()>,
    {
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
        match try_lock(&file) {
            Ok(()) => {
                FileExt::unlock(&file)?;
                Ok(false)
            }
            Err(error) if is_lock_conflict(&error) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for BuildFileLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            #[cfg(windows)]
            if let Some(owner_path) = self.owner_path.take() {
                match fs::remove_file(&owner_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => tracing::warn!(target: "opcda_bridge_gateway::index",
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

#[cfg(windows)]
pub(super) fn build_owner_path(database_path: &Path, server: &str) -> PathBuf {
    let database_path = canonical_database_path(database_path);
    let file_name = database_path
        .file_name()
        .map_or_else(|| "index.sqlite3".into(), std::ffi::OsStr::to_os_string);
    database_path.with_file_name(format!(
        "{}.{}.build.owner",
        file_name.to_string_lossy(),
        stable_server_hash(server)
    ))
}

#[cfg(windows)]
pub(super) fn read_lock_owner(lock_path: &Path, database_path: &Path, server: &str) -> String {
    fs::read_to_string(build_owner_path(database_path, server))
        .or_else(|_| fs::read_to_string(lock_path))
        .unwrap_or_else(|_| "owner details unavailable".to_string())
}

#[cfg(not(windows))]
pub(super) fn read_lock_owner(lock_path: &Path, _database_path: &Path, _server: &str) -> String {
    fs::read_to_string(lock_path).unwrap_or_else(|_| "owner details unavailable".to_string())
}

impl<C: OpcClient> IndexManager<C> {
    pub fn new(client: Arc<C>, settings: ResolvedIndexConfig) -> Self {
        let cache_capacity = settings.query_cache_capacity.max(1);
        let host_metrics = default_host_metrics_provider(&settings.database_path);
        let coordination = database_coordination(&settings.database_path);
        tracing::debug!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %settings.database_path.display(),
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
            deleting: Arc::new(Mutex::new(HashSet::new())),
            deletion_errors: Arc::new(Mutex::new(HashMap::new())),
            foreground_users: Arc::new(Mutex::new(HashMap::new())),
            pause_overlays: Arc::new(Mutex::new(HashMap::new())),
            foreground_metrics: Arc::new(Mutex::new(HashMap::new())),
            commit_latency_recorded_at: Arc::new(Mutex::new(HashMap::new())),
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

    #[cfg(test)]
    pub(super) fn install_build_reservation_hook(
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
    pub(super) fn wait_for_build_reservation_hook(&self) {
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
        let manager = Arc::clone(self);
        let shutdown = self.background_tasks.subscribe();
        self.background_tasks.spawn(async move {
            manager.run_background_indexing(shutdown).await;
        });
    }

    pub(super) async fn run_background_indexing(
        self: &Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        if !self.wait_for_startup_grace(&mut shutdown).await {
            return;
        }
        loop {
            if *shutdown.borrow() {
                break;
            }
            let delay = self.refresh_scheduled_servers(&mut shutdown).await;
            wait_for_refresh_or_shutdown(&mut shutdown, delay).await;
        }
    }

    pub(super) async fn wait_for_startup_grace(
        &self,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        let startup_grace = Duration::from_secs(self.settings.startup_grace_period_seconds);
        if startup_grace.is_zero() {
            return true;
        }
        tokio::select! {
            _ = shutdown.changed() => false,
            _ = tokio::time::sleep(startup_grace) => true,
        }
    }

    pub(super) async fn refresh_scheduled_servers(
        self: &Arc<Self>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Duration {
        let mut delay = Duration::from_secs(60);
        let servers = match self.with_database_read(|db| db.scheduled_servers()) {
            Ok(servers) => servers,
            Err(error) => {
                tracing::warn!(target: "opcda_bridge_gateway::index", error = %error, "unable to list scheduled namespace indexes");
                Vec::new()
            }
        };
        for server in servers {
            if *shutdown.borrow() {
                break;
            }
            self.refresh_if_due(&server).await;
            delay = delay.min(self.background_refresh_delay(&server).await);
        }
        delay
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

    pub(super) fn persisted_refresh_retry_delay(&self, server: &str) -> Option<Duration> {
        self.runtime
            .lock()
            .ok()
            .and_then(|runtime| runtime.get(server).and_then(|state| state.retry_after))
            .and_then(|retry_after| retry_after.duration_since(SystemTime::now()).ok())
            .map(|remaining| remaining.max(Duration::from_secs(1)))
    }

    pub(super) fn ready_refresh_delay(&self, server: &str, status: &IndexStatus) -> Duration {
        if status.state == IndexState::Stale && !self.maintenance_window_is_open() {
            return if self.settings.maintenance_windows.is_empty() {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(60)
            };
        }
        let scheduled =
            Duration::from_secs(self.settings.refresh_interval_seconds.max(1)).saturating_add(
                deterministic_jitter(server, self.settings.schedule_jitter_seconds),
            );
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

    pub(super) fn refresh_delay_for_status(&self, server: &str, status: &IndexStatus) -> Duration {
        match status.state {
            IndexState::Ready | IndexState::Stale => self.ready_refresh_delay(server, status),
            IndexState::Refreshing | IndexState::Partial => Duration::from_secs(30),
            IndexState::Promoting => Duration::from_secs(1),
            IndexState::Failed => retry_delay(server, 1, false, self.settings.circuit_open_seconds),
            IndexState::NotIndexed => Duration::from_secs(3600),
            IndexState::Deleting => Duration::from_secs(30),
        }
    }

    pub(super) async fn background_refresh_delay(&self, server: &str) -> Duration {
        if let Some(delay) = self.persisted_refresh_retry_delay(server) {
            return delay;
        }
        match self.status(server).await {
            Ok(status) => self.refresh_delay_for_status(server, &status),
            Err(_) => retry_delay(server, 1, false, self.settings.circuit_open_seconds),
        }
    }

    pub(super) async fn active_profile_changed(&self, server: &str) -> anyhow::Result<bool> {
        let stored_profile = match self.with_database_read(|db| db.active_profile(server)) {
            Ok(Some(stored_profile)) => stored_profile,
            Ok(None) => {
                tracing::warn!(target: "opcda_bridge_gateway::index",
                    server = %server,
                    "active namespace index profile is unavailable"
                );
                return Ok(false);
            }
            Err(error) => {
                tracing::warn!(target: "opcda_bridge_gateway::index",
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

    pub(super) async fn refresh_active_generation_if_due(
        self: &Arc<Self>,
        server: &str,
        status: &IndexStatus,
    ) {
        if matches!(
            status.state,
            IndexState::Stale | IndexState::Failed | IndexState::NotIndexed
        ) && self.automatic_refresh_allowed(status)
            && let Err(error) = self.refresh(server, false).await
        {
            tracing::warn!(target: "opcda_bridge_gateway::index",
                server = %server,
                error = %error,
                "automatic namespace index refresh failed"
            );
        }
    }

    pub(super) async fn refresh_after_profile_change(
        self: &Arc<Self>,
        server: &str,
        status: &IndexStatus,
    ) {
        if !self.automatic_refresh_allowed(status) {
            tracing::debug!(target: "opcda_bridge_gateway::index",
                server = %server,
                "automatic namespace index rebuild is waiting for a maintenance window"
            );
            return;
        }
        if let Err(error) = self.with_database_write(|db| db.clear_server(server)) {
            tracing::warn!(target: "opcda_bridge_gateway::index",
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
            tracing::warn!(target: "opcda_bridge_gateway::index",
                server = %server,
                error = %error,
                "automatic namespace index rebuild after profile change failed"
            );
        }
    }

    pub(super) async fn refresh_existing_index_if_due(
        self: &Arc<Self>,
        server: &str,
        status: &IndexStatus,
    ) {
        let profile_changed = match self.active_profile_changed(server).await {
            Ok(profile_changed) => profile_changed,
            Err(error) => {
                tracing::warn!(target: "opcda_bridge_gateway::index",
                    server = %server,
                    error = %error,
                    "unable to inspect namespace index profile before refresh"
                );
                return;
            }
        };
        if profile_changed {
            self.refresh_after_profile_change(server, status).await;
        } else {
            self.refresh_active_generation_if_due(server, status).await;
        }
    }

    pub(super) async fn refresh_if_due(self: &Arc<Self>, server: &str) {
        let status = match self.status(server).await {
            Ok(status) => status,
            Err(error) => {
                tracing::warn!(target: "opcda_bridge_gateway::index",
                    server = %server,
                    error = %error,
                    "unable to inspect namespace index before refresh"
                );
                return;
            }
        };
        if status.active_generation > 0 && status.state != IndexState::Refreshing {
            self.refresh_existing_index_if_due(server, &status).await;
        }
    }

    pub(super) fn automatic_refresh_allowed(&self, status: &IndexStatus) -> bool {
        status.auto_refresh_enabled
            && (self.settings.maintenance_windows.is_empty() || self.maintenance_window_is_open())
    }

    pub(super) fn maintenance_window_is_open(&self) -> bool {
        match parse_maintenance_windows(&self.settings.maintenance_windows) {
            Ok(windows) => maintenance_window_active(&windows, Local::now()),
            Err(error) => {
                tracing::warn!(target: "opcda_bridge_gateway::index", error = %error, "invalid namespace index maintenance window");
                false
            }
        }
    }

    pub(super) fn load_persisted_retry_state(&self, server: &str) -> anyhow::Result<()> {
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

    pub(super) fn record_start_failure(
        &self,
        server: &str,
        ownership: &Arc<()>,
        error: &str,
    ) -> anyhow::Result<()> {
        let persisted = self.with_database_write(|db| db.record_failed_attempt(server, error));
        self.finish_build_owned(server, ownership, Some(error.to_string()));
        persisted
    }

    pub(super) fn persist_retry_state(&self, server: &str) -> anyhow::Result<()> {
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

    pub(super) fn schedule_cleanup(&self, server: &str) {
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
                tracing::error!(target: "opcda_bridge_gateway::index",
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
            self.take_cleanup_spawn_rejection(),
        );
    }
}

pub(super) fn database_coordination_key<F>(path: &Path, current_dir: F) -> PathBuf
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

pub(super) fn canonical_database_path(path: &Path) -> PathBuf {
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

pub(super) fn new_database_coordination() -> Arc<DatabaseCoordination> {
    Arc::new(DatabaseCoordination {
        writer_gate: Arc::new(Mutex::new(())),
        active_builds: Arc::new(Mutex::new(HashSet::new())),
        build_owners: Arc::new(Mutex::new(HashMap::new())),
        build_changed: Arc::new(tokio::sync::Notify::new()),
    })
}

pub(super) fn database_coordination(path: &Path) -> Arc<DatabaseCoordination> {
    if path == Path::new(":memory:") {
        return new_database_coordination();
    }
    let key = database_coordination_key(path, std::env::current_dir);
    let registry = DATABASE_COORDINATIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let coordination = new_database_coordination();
    registry.insert(key, Arc::downgrade(&coordination));
    coordination
}

#[cfg(test)]
pub(super) fn cleanup_obsolete_generations(
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

pub(super) fn cleanup_checkpoint(
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
        tracing::debug!(target: "opcda_bridge_gateway::index",
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

pub(super) fn cleanup_build_is_active(active_builds: &Mutex<HashSet<String>>) -> bool {
    active_builds
        .lock()
        .map(|builds| !builds.is_empty())
        .unwrap_or(true)
}

pub(super) fn delete_cleanup_batch(
    transaction: &rusqlite::Transaction<'_>,
    server: &str,
) -> rusqlite::Result<CleanupBatch> {
    let fts_entries = transaction.execute(
        "DELETE FROM entries_fts
         WHERE rowid IN (
             SELECT f.rowid
             FROM entries_fts f
             INNER JOIN generations g
               ON g.server = f.server AND g.generation = f.generation
             WHERE g.server = ?1
               AND (g.state = 'superseded'
                    OR (g.state = 'failed' AND EXISTS (
                        SELECT 1 FROM generations AS active
                        WHERE active.server = g.server AND active.state = 'active'
                    )))
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
             WHERE g.server = ?1
               AND (g.state = 'superseded'
                    OR (g.state = 'failed' AND EXISTS (
                        SELECT 1 FROM generations AS active
                        WHERE active.server = g.server AND active.state = 'active'
                    )))
             LIMIT ?2
         )",
        params![server, CLEANUP_BATCH_SIZE as i64],
    )?;
    let generations = transaction.execute(
        "DELETE FROM generations
         WHERE rowid IN (
             SELECT g.rowid
             FROM generations g
             WHERE g.server = ?1
               AND (g.state = 'superseded'
                    OR (g.state = 'failed' AND EXISTS (
                        SELECT 1 FROM generations AS active
                        WHERE active.server = g.server AND active.state = 'active'
                    )))
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
    Ok(CleanupBatch {
        entries: entries as u64,
        fts_entries: fts_entries as u64,
        generations: generations as u64,
    })
}

pub(super) fn cleanup_one_batch(
    read_only: &IndexDb,
    connection: &mut Option<Connection>,
    path: &Path,
    server: &str,
    background_tasks: &BackgroundTasks,
    writer_gate: &Mutex<()>,
    active_builds: &Mutex<HashSet<String>>,
) -> anyhow::Result<CleanupBatchResult> {
    if background_tasks.is_shutting_down() {
        return Ok(CleanupBatchResult::Shutdown);
    }
    if cleanup_build_is_active(active_builds) {
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %path.display(),
            server,
            "deferring namespace index cleanup while an index build is active"
        );
        return Ok(CleanupBatchResult::Deferred);
    }
    if !read_only.has_obsolete_generations(server)? {
        return Ok(CleanupBatchResult::NoObsoleteGenerations);
    }
    #[cfg(test)]
    background_tasks.wait_for_cleanup_writer_gate_hook();
    let writer_guard = writer_gate
        .lock()
        .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
    if cleanup_build_is_active(active_builds) {
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %path.display(),
            server,
            "deferring namespace index cleanup after waiting for the writer gate"
        );
        drop(writer_guard);
        return Ok(CleanupBatchResult::Deferred);
    }
    if !read_only.has_obsolete_generations(server)? {
        drop(writer_guard);
        return Ok(CleanupBatchResult::NoObsoleteGenerations);
    }
    #[cfg(test)]
    background_tasks.wait_for_cleanup_batch_hook();
    if connection.is_none() {
        let opened = Connection::open(path)?;
        opened.pragma_update(None, "foreign_keys", true)?;
        opened.pragma_update(None, "journal_mode", "WAL")?;
        opened.busy_timeout(Duration::from_secs(5))?;
        *connection = Some(opened);
    }
    let transaction = connection
        .as_mut()
        .expect("cleanup connection initialized")
        .transaction()?;
    let batch = delete_cleanup_batch(&transaction, server)?;
    transaction.commit()?;
    drop(writer_guard);
    Ok(CleanupBatchResult::Deleted(batch))
}

pub(super) fn cleanup_obsolete_generations_coordinated(
    path: &Path,
    server: &str,
    background_tasks: &BackgroundTasks,
    writer_gate: Arc<Mutex<()>>,
    active_builds: Arc<Mutex<HashSet<String>>>,
) -> anyhow::Result<CleanupStats> {
    let cleanup_started = Instant::now();
    let read_only = IndexDb::open_read_only(path)?;
    if !read_only.has_obsolete_generations(server)? {
        tracing::debug!(target: "opcda_bridge_gateway::index",
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
        match cleanup_one_batch(
            &read_only,
            &mut connection,
            path,
            server,
            background_tasks,
            writer_gate.as_ref(),
            active_builds.as_ref(),
        )? {
            CleanupBatchResult::Shutdown => {
                stats.stopped_for_shutdown = true;
                break;
            }
            CleanupBatchResult::Deferred => {
                stats.deferred_for_build = true;
                break;
            }
            CleanupBatchResult::NoObsoleteGenerations => break,
            CleanupBatchResult::Deleted(batch) => {
                stats.batches = stats.batches.saturating_add(1);
                stats.fts_entries = stats.fts_entries.saturating_add(batch.fts_entries);
                stats.entries = stats.entries.saturating_add(batch.entries);
                stats.generations = stats.generations.saturating_add(batch.generations);
                if batch.fts_entries == 0 && batch.entries == 0 && batch.generations == 0 {
                    break;
                }
                std::thread::sleep(CLEANUP_BATCH_PAUSE);
            }
        }
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
    tracing::info!(target: "opcda_bridge_gateway::index",
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

pub(super) fn cleanup_task_should_run(
    server: &str,
    background_tasks: &BackgroundTasks,
    cleanup_tasks: &Mutex<HashMap<String, CleanupTaskState>>,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) -> bool {
    cleanup_tasks
        .lock()
        .map(|mut tasks| {
            let task = tasks.entry(server.to_string()).or_default();
            task.requested = false;
            !background_tasks.is_shutting_down() && !*shutdown.borrow()
        })
        .unwrap_or(false)
}

pub(super) fn clear_finished_cleanup_task(
    server: &str,
    background_tasks: &BackgroundTasks,
    cleanup_tasks: &Mutex<HashMap<String, CleanupTaskState>>,
) -> bool {
    cleanup_tasks
        .lock()
        .map(|mut tasks| {
            let rerun = tasks
                .get(server)
                .is_some_and(|task| task.requested && !background_tasks.is_shutting_down());
            if !rerun {
                tasks.remove(server);
            }
            rerun
        })
        .unwrap_or(false)
}

pub(super) async fn run_cleanup_worker(
    path: &Path,
    server: &str,
    background_tasks: &Arc<BackgroundTasks>,
    coordination: &Arc<DatabaseCoordination>,
) -> anyhow::Result<CleanupStats> {
    let cleanup_path = path.to_path_buf();
    let cleanup_server = server.to_owned();
    let background_tasks_for_blocking = Arc::clone(background_tasks);
    let coordination_for_blocking = Arc::clone(coordination);
    match tokio::task::spawn_blocking(move || {
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
    }
}

pub(super) async fn wait_for_deferred_cleanup(
    path: &Path,
    server: &str,
    #[cfg(test)] background_tasks: &Arc<BackgroundTasks>,
    #[cfg(not(test))] _background_tasks: &Arc<BackgroundTasks>,
    coordination: &Arc<DatabaseCoordination>,
    cleanup_tasks: &Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    if let Ok(mut tasks) = cleanup_tasks.lock()
        && let Some(task) = tasks.get_mut(server)
    {
        task.requested = true;
    }
    tracing::debug!(target: "opcda_bridge_gateway::index",
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
        return true;
    }
    #[cfg(test)]
    background_tasks.wait_for_cleanup_notification_hook().await;
    if *shutdown.borrow() {
        return false;
    }
    tokio::select! {
        _ = &mut notified => true,
        _ = shutdown.changed() => false,
    }
}

pub(super) async fn retry_cleanup_after_failure(
    path: &Path,
    server: &str,
    background_tasks: &Arc<BackgroundTasks>,
    #[cfg(test)] cleanup_tasks: &Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    #[cfg(not(test))] _cleanup_tasks: &Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    consecutive_failures: &mut u32,
    error: &anyhow::Error,
) -> bool {
    *consecutive_failures = consecutive_failures.saturating_add(1);
    #[cfg(test)]
    if let Ok(mut tasks) = cleanup_tasks.lock()
        && let Some(task) = tasks.get_mut(server)
    {
        task.failures = task.failures.saturating_add(1);
    }
    let retry =
        *consecutive_failures <= CLEANUP_RETRY_LIMIT && !background_tasks.is_shutting_down();
    tracing::warn!(target: "opcda_bridge_gateway::index",
        process_id = std::process::id(),
        database = %path.display(),
        server,
        error = %error,
        attempt = *consecutive_failures,
        retry,
        "namespace index obsolete-generation cleanup failed"
    );
    if retry {
        let multiplier = 2_u32.pow(consecutive_failures.saturating_sub(1));
        tokio::time::sleep(CLEANUP_RETRY_INITIAL_BACKOFF.saturating_mul(multiplier)).await;
    }
    retry
}

pub(super) async fn run_cleanup_attempt(
    path: &Path,
    server: &str,
    background_tasks: &Arc<BackgroundTasks>,
    cleanup_tasks: &Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    coordination: &Arc<DatabaseCoordination>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    consecutive_failures: &mut u32,
) -> CleanupAttempt {
    match run_cleanup_worker(path, server, background_tasks, coordination).await {
        Ok(stats) if stats.deferred_for_build => {
            if wait_for_deferred_cleanup(
                path,
                server,
                background_tasks,
                coordination,
                cleanup_tasks,
                shutdown,
            )
            .await
            {
                CleanupAttempt::Retry
            } else {
                CleanupAttempt::Return
            }
        }
        Ok(_) => CleanupAttempt::Finished,
        Err(error) => {
            if retry_cleanup_after_failure(
                path,
                server,
                background_tasks,
                cleanup_tasks,
                consecutive_failures,
                &error,
            )
            .await
            {
                CleanupAttempt::Retry
            } else {
                CleanupAttempt::Finished
            }
        }
    }
}

pub(super) async fn run_scheduled_cleanup(
    path: PathBuf,
    server: String,
    background_tasks: Arc<BackgroundTasks>,
    cleanup_tasks: Arc<Mutex<HashMap<String, CleanupTaskState>>>,
    coordination: Arc<DatabaseCoordination>,
) {
    let mut shutdown = background_tasks.subscribe();
    let mut consecutive_failures = 0_u32;
    loop {
        if !cleanup_task_should_run(
            &server,
            background_tasks.as_ref(),
            cleanup_tasks.as_ref(),
            &shutdown,
        ) {
            if let Ok(mut tasks) = cleanup_tasks.lock() {
                tasks.remove(&server);
            }
            return;
        }

        match run_cleanup_attempt(
            &path,
            &server,
            &background_tasks,
            &cleanup_tasks,
            &coordination,
            &mut shutdown,
            &mut consecutive_failures,
        )
        .await
        {
            CleanupAttempt::Retry => continue,
            CleanupAttempt::Return => return,
            CleanupAttempt::Finished => {}
        }

        if !clear_finished_cleanup_task(&server, background_tasks.as_ref(), cleanup_tasks.as_ref())
        {
            return;
        }
        consecutive_failures = 0;
    }
}

pub(super) fn spawn_cleanup_worker_if_idle(
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

pub(super) fn stable_server_hash(server: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in server.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    format!("{hash:016x}")
}

pub(super) fn deterministic_jitter(server: &str, maximum_seconds: u64) -> Duration {
    if maximum_seconds == 0 {
        return Duration::ZERO;
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in server.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    let range = maximum_seconds.saturating_add(1);
    Duration::from_secs(hash % range)
}

pub(super) async fn wait_for_refresh_or_shutdown(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    delay: Duration,
) {
    if *shutdown.borrow() {
        return;
    }
    tokio::select! {
        _ = shutdown.changed() => {}
        _ = tokio::time::sleep(delay) => {}
    }
}

pub(super) fn retry_delay(
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
