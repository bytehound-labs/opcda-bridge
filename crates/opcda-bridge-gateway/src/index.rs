//! Persistent, gateway-owned namespace index and refresh coordinator.

use crate::config::ResolvedIndexConfig;
use crate::opc::{
    BrowseSource, InventoryControl, InventoryEntry, InventoryEvent, InventoryHandle,
    InventoryNodeKind, InventoryProgress, NamespaceOrganization, OpcClient,
};
use chrono::{DateTime, Local, Timelike};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SCHEMA_VERSION: i64 = 2;
const RETRY_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    NotIndexed,
    Partial,
    Ready,
    Stale,
    Refreshing,
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
}

#[derive(Default)]
struct RuntimeState {
    build: Option<RuntimeBuild>,
    retry_after: Option<SystemTime>,
    last_error: Option<String>,
}

struct BackgroundTasks {
    state: Mutex<BackgroundTaskState>,
    shutdown: tokio::sync::watch::Sender<bool>,
    idle: tokio::sync::Notify,
}

#[derive(Default)]
struct BackgroundTaskState {
    active: usize,
    shutting_down: bool,
}

struct BackgroundTaskGuard {
    tasks: Arc<BackgroundTasks>,
}

impl BackgroundTasks {
    fn new() -> Self {
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Self {
            state: Mutex::new(BackgroundTaskState::default()),
            shutdown,
            idle: tokio::sync::Notify::new(),
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
    path: PathBuf,
}

impl BuildFileLock {
    fn acquire(database_path: &Path, server: &str) -> anyhow::Result<Self> {
        let file_name = database_path
            .file_name()
            .map_or_else(|| "index.sqlite3".into(), |name| name.to_os_string());
        let lock_path =
            database_path.with_file_name(format!("{}.build.lock", file_name.to_string_lossy()));
        if let Some(parent) = lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let mut file = match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let owner = fs::read_to_string(&lock_path)
                    .unwrap_or_else(|_| "owner details unavailable".to_string());
                anyhow::bail!(
                    "namespace index build lock already exists at {} ({})",
                    lock_path.display(),
                    owner.trim()
                );
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = format!("process_id={}\nserver={server}\n", std::process::id());
        if let Err(error) = file
            .write_all(metadata.as_bytes())
            .and_then(|_| file.sync_all())
        {
            let _ = fs::remove_file(&lock_path);
            return Err(error.into());
        }
        Ok(Self { path: lock_path })
    }
}

impl Drop for BuildFileLock {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                process_id = std::process::id(),
                lock = %self.path.display(),
                error = %error,
                "unable to remove namespace index build lock"
            );
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
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );",
        )?;
        if let Some(version) = connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            let version = version.parse::<i64>().map_err(|_| {
                anyhow::anyhow!("invalid namespace index schema version {version:?}")
            })?;
            if version != SCHEMA_VERSION {
                anyhow::bail!("unsupported namespace index schema version {version}");
            }
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
             );
             INSERT OR REPLACE INTO index_meta(key, value)
               VALUES ('schema_version', '2');",
        )?;
        connection.execute(
            "DELETE FROM entries
             WHERE EXISTS (
                 SELECT 1 FROM generations
                 WHERE state = 'staging'
                   AND generations.server = entries.server
                   AND generations.generation = entries.generation
             )",
            [],
        )?;
        connection.execute("DELETE FROM generations WHERE state = 'staging'", [])?;
        connection.execute("DELETE FROM entries_fts", [])?;
        connection.execute(
            "INSERT INTO entries_fts(server, generation, item_id, display_name, breadcrumbs)
             SELECT server, generation, item_id, display_name, breadcrumbs FROM entries",
            [],
        )?;
        Ok(Self {
            path: path.to_path_buf(),
            connection,
        })
    }

    fn database_bytes(&self) -> u64 {
        fs::metadata(&self.path).map_or(0, |metadata| metadata.len())
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
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "DELETE FROM entries_fts
             WHERE server = ?1
               AND generation IN (
                   SELECT generation FROM generations
                   WHERE server = ?1 AND state IN ('staging', 'failed')
               )",
            [server],
        )?;
        transaction.execute(
            "DELETE FROM entries
             WHERE server = ?1
               AND generation IN (
                   SELECT generation FROM generations
                   WHERE server = ?1 AND state IN ('staging', 'failed')
               )",
            [server],
        )?;
        transaction.execute(
            "DELETE FROM generations
             WHERE server = ?1 AND state IN ('staging', 'failed')",
            [server],
        )?;
        transaction.execute(
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
        transaction.commit()?;
        tracing::debug!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            "started namespace index generation"
        );
        Ok(generation as u64)
    }

    fn insert_entries(
        &mut self,
        server: &str,
        generation: u64,
        entries: &[InventoryEntry],
    ) -> anyhow::Result<()> {
        let transaction = self.connection.transaction()?;
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
                    generation as i64,
                    entry.item_id,
                    normalize_query(&entry.item_id),
                    entry.display_name,
                    normalize_query(&entry.display_name),
                    node_kind_number(entry.kind),
                    serde_json::to_string(&entry.breadcrumbs)?
                ],
            )?;
            if inserted > 0 {
                transaction.execute(
                    "INSERT INTO entries_fts
                     (server, generation, item_id, display_name, breadcrumbs)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        server,
                        generation as i64,
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
            "committed namespace index entries"
        );
        Ok(())
    }

    fn update_progress(
        &self,
        server: &str,
        generation: u64,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE generations SET entry_count = ?1, unique_item_count = ?2
             WHERE server = ?3 AND generation = ?4 AND state = 'staging'",
            params![
                progress.entries_seen as i64,
                progress.unique_items as i64,
                server,
                generation as i64
            ],
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

    fn promote(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        self.promote_with_warning(server, generation, completed_at, progress, None)
    }

    fn promote_with_warning(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        _progress: &InventoryProgress,
        warning: Option<&str>,
    ) -> anyhow::Result<()> {
        let (entry_count, unique_item_count) = self.connection.query_row(
            "SELECT COUNT(*), COUNT(DISTINCT item_id)
             FROM entries WHERE server = ?1 AND generation = ?2",
            params![server, generation as i64],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if entry_count < 0 || unique_item_count < 0 || entry_count != unique_item_count {
            anyhow::bail!("namespace index generation count validation failed");
        }
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE generations SET state = 'superseded'
             WHERE server = ?1 AND state = 'active'",
            [server],
        )?;
        let promoted = transaction.execute(
            "UPDATE generations
             SET state = 'active', completed_at = ?1,
                 entry_count = ?2, unique_item_count = ?3, last_error = ?4
             WHERE server = ?5 AND generation = ?6 AND state = 'staging'",
            params![
                completed_at,
                entry_count,
                unique_item_count,
                warning,
                server,
                generation as i64
            ],
        )?;
        if promoted != 1 {
            anyhow::bail!("namespace index generation is not staging");
        }
        transaction.execute(
            "DELETE FROM entries_fts
             WHERE server = ?1
               AND generation IN (
                   SELECT generation FROM generations
                   WHERE server = ?1 AND state = 'superseded'
               )",
            [server],
        )?;
        transaction.execute(
            "DELETE FROM entries
             WHERE server = ?1
               AND generation IN (
                   SELECT generation FROM generations
                   WHERE server = ?1 AND state = 'superseded'
               )",
            [server],
        )?;
        transaction.execute(
            "DELETE FROM generations WHERE server = ?1 AND state = 'superseded'",
            [server],
        )?;
        transaction.commit()?;
        tracing::info!(
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            entry_count,
            unique_item_count,
            warning = warning.unwrap_or(""),
            "promoted namespace index generation"
        );
        Ok(())
    }

    fn fail_generation(&self, server: &str, generation: u64, error: &str) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE generations SET state = 'failed', last_error = ?1
             WHERE server = ?2 AND generation = ?3 AND state = 'staging'",
            params![error, server, generation as i64],
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

    fn discard_generation(&mut self, server: &str, generation: u64) -> anyhow::Result<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "DELETE FROM entries_fts WHERE server = ?1 AND generation = ?2",
            params![server, generation as i64],
        )?;
        transaction.execute(
            "DELETE FROM entries WHERE server = ?1 AND generation = ?2",
            params![server, generation as i64],
        )?;
        transaction.execute(
            "DELETE FROM generations WHERE server = ?1 AND generation = ?2 AND state = 'staging'",
            params![server, generation as i64],
        )?;
        transaction.commit()?;
        Ok(())
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
        let mut sql = format!(
            "SELECT e.item_id, e.display_name, e.kind, e.breadcrumbs FROM entries e
             WHERE e.server = ? AND e.generation = {}",
            generation
        );
        let mut values = vec![server.to_string()];
        match mode {
            1 => {
                sql.push_str(" AND (e.display_name_norm = ? OR e.item_id_norm = ?)");
                values.extend([normalized_query.clone(), normalized_query.clone()]);
            }
            2 => {
                let pattern = format!("{}%", escape_like(&normalized_query));
                sql.push_str(
                    " AND (e.display_name_norm LIKE ? ESCAPE '\\'
                        OR e.item_id_norm LIKE ? ESCAPE '\\')",
                );
                values.extend([pattern.clone(), pattern]);
            }
            _ => {
                let fts_compatible = normalized_query
                    .split_whitespace()
                    .all(|term| term.chars().count() >= 3);
                if normalized_query.chars().count() >= 3 && fts_compatible {
                    let fts_query = format!("\"{}\"", normalized_query.replace('"', "\"\""));
                    sql.push_str(
                        " AND EXISTS (
                             SELECT 1 FROM entries_fts f
                             WHERE f.server = e.server
                               AND f.generation = e.generation
                               AND f.item_id = e.item_id
                               AND entries_fts MATCH ?
                         )",
                    );
                    values.push(fts_query);
                } else {
                    let pattern = format!("%{}%", escape_like(&normalized_query));
                    sql.push_str(
                        " AND (e.display_name_norm LIKE ? ESCAPE '\\'
                            OR e.item_id_norm LIKE ? ESCAPE '\\'
                            OR e.breadcrumbs LIKE ? ESCAPE '\\')",
                    );
                    values.extend([pattern.clone(), pattern.clone(), pattern]);
                }
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

pub struct IndexManager<C: OpcClient> {
    client: Arc<C>,
    settings: ResolvedIndexConfig,
    database: Arc<Mutex<Option<IndexDb>>>,
    build_locks: Arc<Mutex<HashMap<PathBuf, BuildFileLock>>>,
    runtime: Arc<Mutex<HashMap<String, RuntimeState>>>,
    active_builds: Arc<Mutex<HashSet<String>>>,
    foreground_users: Arc<Mutex<HashMap<String, usize>>>,
    cache: Arc<Mutex<QueryCache>>,
    background_tasks: Arc<BackgroundTasks>,
    background_started: AtomicBool,
}

impl<C: OpcClient> IndexManager<C> {
    pub fn new(client: Arc<C>, settings: ResolvedIndexConfig) -> Self {
        let cache_capacity = settings.query_cache_capacity.max(1);
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
            build_locks: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(Mutex::new(HashMap::new())),
            active_builds: Arc::new(Mutex::new(HashSet::new())),
            foreground_users: Arc::new(Mutex::new(HashMap::new())),
            cache: Arc::new(Mutex::new(QueryCache {
                values: HashMap::new(),
                order: VecDeque::new(),
                capacity: cache_capacity,
            })),
            background_tasks: Arc::new(BackgroundTasks::new()),
            background_started: AtomicBool::new(false),
        }
    }

    pub fn max_results(&self) -> u32 {
        self.settings.max_results
    }

    pub fn start_background_indexing(self: &Arc<Self>) {
        if !self.settings.enabled || self.settings.paused {
            return;
        }
        if self.background_started.swap(true, Ordering::AcqRel) {
            return;
        }
        for server in self.settings.servers.clone() {
            let manager = Arc::clone(self);
            let mut shutdown = self.background_tasks.subscribe();
            self.background_tasks.spawn(async move {
                manager.refresh_if_due(&server).await;
                loop {
                    if *shutdown.borrow() {
                        break;
                    }
                    let delay = manager.background_refresh_delay(&server).await;
                    if *shutdown.borrow() {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.changed() => {}
                        _ = tokio::time::sleep(delay) => {
                            manager.refresh_if_due(&server).await;
                        }
                    }
                }
            });
        }
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
        match self.status(server).await {
            Ok(status) => match status.state {
                IndexState::Ready | IndexState::Stale => status
                    .completed_at
                    .as_deref()
                    .and_then(parse_timestamp)
                    .and_then(|completed| {
                        SystemTime::now()
                            .duration_since(completed)
                            .ok()
                            .map(|elapsed| {
                                Duration::from_secs(self.settings.refresh_interval_seconds.max(1))
                                    .saturating_sub(elapsed)
                            })
                    })
                    .unwrap_or(Duration::from_secs(1)),
                IndexState::Refreshing | IndexState::Partial => Duration::from_secs(30),
                IndexState::Failed | IndexState::NotIndexed => RETRY_BACKOFF,
            },
            Err(_) => RETRY_BACKOFF,
        }
    }

    async fn refresh_if_due(self: &Arc<Self>, server: &str) {
        match self.status(server).await {
            Ok(status)
                if status.active_generation > 0 && status.state != IndexState::Refreshing =>
            {
                let profile_changed = self
                    .client
                    .get_capabilities(server)
                    .await
                    .map(|capabilities| {
                        capabilities.organization != status.organization
                            || capabilities.source != status.source
                    })
                    .unwrap_or(false);
                if profile_changed {
                    if let Err(error) = self.with_database(|db| db.clear_server(server)) {
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
                } else if matches!(
                    status.state,
                    IndexState::Stale | IndexState::Failed | IndexState::NotIndexed
                ) && let Err(error) = self.refresh(server, false).await
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
                if let Err(error) = self.refresh(server, false).await {
                    tracing::warn!(server = %server, error = %error, "automatic namespace index refresh failed");
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(server = %server, error = %error, "unable to inspect namespace index before refresh");
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
            if let Some(control) = &build.control {
                control.pause();
            }
        }
        ForegroundGuard {
            manager: Arc::clone(self),
            server: server.to_string(),
        }
    }

    fn foreground_end(&self, server: &str) {
        let quiet_period = Duration::from_secs(self.settings.quiet_period_seconds);
        let runtime = Arc::clone(&self.runtime);
        let foreground_users = Arc::clone(&self.foreground_users);
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
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                tokio::time::sleep(quiet_period).await;
                if let Ok(mut states) = runtime.lock()
                    && let Some(build) = states
                        .get_mut(&resume_server_name)
                        .and_then(|state| state.build.as_mut())
                    && build.foreground_users == 0
                    && !build.operator_paused
                    && build
                        .quiet_until
                        .is_some_and(|deadline| deadline <= Instant::now())
                {
                    build.quiet_until = None;
                    if let Some(control) = &build.control {
                        control.resume();
                    }
                }
            });
        } else {
            if let Ok(mut states) = runtime.lock()
                && let Some(build) = states
                    .get_mut(&resume_server_name)
                    .and_then(|state| state.build.as_mut())
                && build.foreground_users == 0
                && !build.operator_paused
                && build
                    .quiet_until
                    .is_some_and(|deadline| deadline <= Instant::now())
            {
                build.quiet_until = None;
                if let Some(control) = &build.control {
                    control.resume();
                }
            }
        }
    }

    pub async fn status(&self, server: &str) -> anyhow::Result<IndexStatus> {
        let configured = self.settings.servers.iter().any(|s| s == server);
        if !configured {
            return Ok(empty_status(server, false, IndexState::NotIndexed));
        }
        let rows = self.with_database(|db| db.status_rows(server))?;
        let active_row = rows.iter().find(|row| row.state == "active").cloned();
        let staging_row = rows.iter().find(|row| row.state == "staging").cloned();
        let failed_row = rows.iter().find(|row| row.state == "failed").cloned();
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
        let database_bytes = self.database_bytes()?;
        let mut status = if let Some(build) = build {
            let row = active_row.clone().or(staging_row.clone()).or(failed_row);
            if let Some(row) = row {
                let state = if active_row.is_some() {
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
                    state: IndexState::Partial,
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
                }
            }
        } else if let Some(row) = active_row {
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
            let state = if failed_row.is_some() {
                IndexState::Failed
            } else if stale {
                IndexState::Stale
            } else {
                IndexState::Ready
            };
            let mut status = status_from_row(server, row, state, None, database_bytes);
            if let Some(failed) = failed_row {
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
        if !build_active && let Some(error) = runtime_error {
            status.state = IndexState::Failed;
            status.last_error = Some(error);
        }
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
        let should_start = {
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
            if state.build.is_some() || backing_off {
                false
            } else if active_builds >= self.settings.concurrency.max(1) as usize {
                anyhow::bail!("namespace index build concurrency limit reached");
            } else {
                let mut build_locks = self
                    .build_locks
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index build-lock registry poisoned"))?;
                if build_locks.contains_key(&self.settings.database_path) {
                    anyhow::bail!(
                        "namespace index build lock is already held in this process: {}",
                        self.settings.database_path.display()
                    );
                }
                let lock = BuildFileLock::acquire(&self.settings.database_path, server)?;
                build_locks.insert(self.settings.database_path.clone(), lock);
                state.build = Some(RuntimeBuild {
                    control: None,
                    progress: None,
                    started_at: timestamp_now(),
                    foreground_users,
                    operator_paused: false,
                    quiet_until: None,
                });
                state.last_error = None;
                self.active_builds
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index active-build lock poisoned"))?
                    .insert(server.to_string());
                true
            }
        };
        if !should_start {
            return self.status(server).await;
        }

        let handle = match self
            .client
            .start_inventory(server, self.settings.batch_size)
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                self.record_start_failure(server, &error.to_string())?;
                return Err(error);
            }
        };
        tracing::info!(
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            batch_size = self.settings.batch_size,
            "started namespace index inventory"
        );
        if self.background_tasks.is_shutting_down() {
            handle.control.cancel();
            self.finish_build(server, None);
            return self.status(server).await;
        }
        let (organization, source) = match self.client.get_capabilities(server).await {
            Ok(capabilities) => (capabilities.organization, capabilities.source),
            Err(error) => {
                handle.control.cancel();
                self.record_start_failure(server, &error.to_string())?;
                return Err(error);
            }
        };
        let generation = self.with_database(|db| {
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
                handle.control.cancel();
                self.record_start_failure(server, &error.to_string())?;
                return Err(error);
            }
        };
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
                if build.foreground_users > 0 || build.operator_paused {
                    handle.control.pause();
                }
                Ok(())
            });
        if let Err(error) = control_result {
            handle.control.cancel();
            let _ = self.with_database(|db| db.discard_generation(server, generation));
            self.finish_build(server, Some(error.to_string()));
            return Err(error);
        }
        if self.background_tasks.is_shutting_down() {
            handle.control.cancel();
            let _ = self.with_database(|db| db.discard_generation(server, generation));
            self.finish_build(server, None);
            return self.status(server).await;
        }
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        let control = Arc::clone(&handle.control);
        if !self.background_tasks.spawn(async move {
            manager.run_build(server_name, generation, handle).await;
        }) {
            control.cancel();
            let _ = self.with_database(|db| db.discard_generation(server, generation));
            self.finish_build_for_control(server, &control, None);
        }
        self.status(server).await
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
                && let Some(control) = &build.control
            {
                match action {
                    IndexControlAction::Pause => {
                        build.operator_paused = true;
                        control.pause();
                    }
                    IndexControlAction::Resume => {
                        build.operator_paused = false;
                        if build.foreground_users == 0
                            && build
                                .quiet_until
                                .is_none_or(|deadline| deadline <= Instant::now())
                        {
                            build.quiet_until = None;
                            control.resume();
                        }
                    }
                    IndexControlAction::Cancel => control.cancel(),
                }
            }
        } else {
            return Err(anyhow::anyhow!("index runtime lock poisoned"));
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
        let result = self.with_database(|db| {
            let Some(generation) = db.search_generation(server)? else {
                return Ok(None);
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
                value.status = status.clone();
                return Ok(Some(value));
            }
            let mut matches = db.search(server, generation, query, mode, limit)?;
            let has_more = matches.len() > limit as usize;
            matches.truncate(limit as usize);
            let value = IndexedSearch {
                matches,
                has_more,
                status: status.clone(),
            };
            if status.active_generation == generation {
                self.cache
                    .lock()
                    .map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?
                    .insert(key, value.clone());
            }
            Ok(Some(value))
        })?;
        let Some(value) = result else {
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status,
            });
        };
        Ok(value)
    }

    async fn run_build(
        self: Arc<Self>,
        server: String,
        generation: u64,
        mut handle: InventoryHandle,
    ) {
        let build_started = Instant::now();
        let maintenance_windows =
            match parse_maintenance_windows(&self.settings.maintenance_windows) {
                Ok(windows) => windows,
                Err(error) => {
                    handle.control.cancel();
                    let message = error.to_string();
                    let _ =
                        self.with_database(|db| db.fail_generation(&server, generation, &message));
                    self.finish_build_for_control(&server, &handle.control, Some(message));
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
        let mut terminal = false;
        let mut accounted_active_time_ms = 0_u64;
        let mut rate_limiter =
            ItemRateLimiter::new(self.settings.item_rate_limit, self.settings.burst_size);
        let mut next_health_probe = Instant::now();
        let mut health_backoff = Duration::from_secs(1);
        loop {
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
            let Some(event) = handle.stream.next().await else {
                break;
            };
            match event {
                Ok(InventoryEvent::Entry(entry)) => {
                    if !rate_limiter.acquire(&handle.control).await {
                        cancelled = true;
                        break;
                    }
                    pending.push(entry);
                    if pending.len() >= self.settings.batch_size as usize {
                        if let Err(error) = self
                            .with_database(|db| db.insert_entries(&server, generation, &pending))
                        {
                            failed = Some(error.to_string());
                            break;
                        }
                        pending.clear();
                    }
                }
                Ok(InventoryEvent::Progress(progress)) => {
                    let active_time_delta_ms = progress
                        .active_time_ms
                        .saturating_sub(accounted_active_time_ms);
                    accounted_active_time_ms = progress.active_time_ms;
                    last_progress = progress.clone();
                    if let Err(error) =
                        self.with_database(|db| db.update_progress(&server, generation, &progress))
                    {
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
                            )
                            .await
                    {
                        cancelled = true;
                        break;
                    }
                }
                Ok(InventoryEvent::Completed(result)) => {
                    terminal = true;
                    completed = result.complete;
                    cancelled = result.cancelled;
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
        if !pending.is_empty()
            && failed.is_none()
            && let Err(error) =
                self.with_database(|db| db.insert_entries(&server, generation, &pending))
        {
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

        if let Some(error) = failed {
            let _ = self.with_database(|db| db.fail_generation(&server, generation, &error));
            tracing::error!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server = %server,
                generation,
                duration_ms = build_started.elapsed().as_millis() as u64,
                error = %error,
                "namespace index build failed"
            );
            self.finish_build_for_control(&server, &handle.control, Some(error));
        } else if completed && !cancelled && !handle.control.is_cancelled() {
            let result = self.with_database(|db| {
                let completed_at = timestamp_now();
                match completion_warning.as_deref() {
                    Some(warning) => db.promote_with_warning(
                        &server,
                        generation,
                        &completed_at,
                        &last_progress,
                        Some(warning),
                    ),
                    None => db.promote(&server, generation, &completed_at, &last_progress),
                }
            });
            match result {
                Ok(()) => {
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
                    self.finish_build_for_control(&server, &handle.control, None);
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
                    let _ = self.with_database(|db| {
                        db.fail_generation(&server, generation, &error.to_string())
                    });
                    self.finish_build_for_control(
                        &server,
                        &handle.control,
                        Some(error.to_string()),
                    );
                }
            }
        } else {
            let _ = self.with_database(|db| db.discard_generation(&server, generation));
            tracing::warn!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server = %server,
                generation,
                duration_ms = build_started.elapsed().as_millis() as u64,
                cancelled,
                "namespace index build cancelled"
            );
            self.finish_build_for_control(&server, &handle.control, None);
        }
    }

    async fn enforce_duty_cycle(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        work_duration: Duration,
    ) -> bool {
        let duty = u32::from(self.settings.duty_cycle_percent.clamp(1, 100));
        if duty >= 100 {
            return !control.is_cancelled();
        }
        let pause_duration = work_duration.mul_f64(f64::from(100 - duty) / f64::from(duty));
        let can_pause = self.runtime.lock().ok().is_some_and(|runtime| {
            runtime
                .get(server)
                .and_then(|state| state.build.as_ref())
                .is_some_and(|build| self.build_can_resume(build))
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
                    .is_some_and(|build| self.build_can_resume(build))
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
        while !windows.is_empty() && !maintenance_window_active(windows, Local::now()) {
            control.pause();
            if !wait_with_cancellation(control, Duration::from_secs(1)).await {
                return false;
            }
        }
        if self.can_resume_build(control, server) {
            control.resume();
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
        if control.is_cancelled() {
            return false;
        }
        if Instant::now() < *next_probe {
            return true;
        }
        let started = Instant::now();
        control.pause();
        let result = self.client.get_capabilities(server).await;
        let elapsed = started.elapsed();
        let healthy = result.is_ok()
            && elapsed <= Duration::from_millis(self.settings.health_latency_threshold_ms);
        if healthy {
            *backoff = Duration::from_secs(1);
            *next_probe = Instant::now()
                + Duration::from_secs(self.settings.health_probe_interval_seconds.max(1));
            if self.can_resume_build(control, server) {
                control.resume();
            }
            return true;
        }

        let reason = match result {
            Ok(_) => format!(
                "health probe exceeded {} ms ({} ms)",
                self.settings.health_latency_threshold_ms,
                elapsed.as_millis()
            ),
            Err(error) => format!("health probe failed: {error}"),
        };
        tracing::warn!(server = %server, reason = %reason, "deferring namespace inventory");
        let delay = (*backoff).min(Duration::from_secs(300));
        *backoff = backoff
            .checked_mul(2)
            .unwrap_or(Duration::from_secs(300))
            .min(Duration::from_secs(300));
        *next_probe = Instant::now() + delay;
        let still_running = wait_with_cancellation(control, delay).await;
        if still_running && self.can_resume_build(control, server) {
            control.resume();
        }
        still_running
    }

    fn can_resume_build(&self, control: &Arc<dyn InventoryControl>, server: &str) -> bool {
        !control.is_cancelled()
            && self
                .runtime
                .lock()
                .ok()
                .and_then(|runtime| {
                    runtime
                        .get(server)
                        .and_then(|state| state.build.as_ref())
                        .map(|build| self.build_can_resume(build))
                })
                .unwrap_or(false)
    }

    fn build_can_resume(&self, build: &RuntimeBuild) -> bool {
        build.foreground_users == 0
            && !build.operator_paused
            && build
                .quiet_until
                .is_none_or(|deadline| deadline <= Instant::now())
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

    fn finish_build(&self, server: &str, error: Option<String>) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(state) = runtime.get_mut(server)
        {
            state.last_error = error.clone();
            state.retry_after = error.map(|_| SystemTime::now() + RETRY_BACKOFF);
            state.build = None;
        }
        self.clear_build_lock();
        self.clear_active_build(server);
    }

    fn finish_build_for_control(
        &self,
        server: &str,
        control: &Arc<dyn InventoryControl>,
        error: Option<String>,
    ) {
        let owns_build = match self.runtime.lock() {
            Ok(mut runtime) => {
                if let Some(state) = runtime.get_mut(server) {
                    let is_current = state
                        .build
                        .as_ref()
                        .and_then(|build| build.control.as_ref())
                        .is_some_and(|current| Arc::ptr_eq(current, control));
                    if is_current {
                        state.last_error = error.clone();
                        state.retry_after = error.map(|_| SystemTime::now() + RETRY_BACKOFF);
                        state.build = None;
                    }
                    is_current
                } else {
                    false
                }
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
            self.clear_build_lock();
            self.clear_active_build(server);
        } else {
            tracing::warn!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                "ignored completion from obsolete namespace index build"
            );
        }
    }

    fn record_start_failure(&self, server: &str, error: &str) -> anyhow::Result<()> {
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
        let state = runtime.entry(server.to_string()).or_default();
        state.last_error = Some(error.to_string());
        state.retry_after = Some(SystemTime::now() + RETRY_BACKOFF);
        state.build = None;
        drop(runtime);
        self.clear_build_lock();
        self.clear_active_build(server);
        Ok(())
    }

    fn clear_build_lock(&self) {
        if let Ok(mut build_locks) = self.build_locks.lock() {
            build_locks.remove(&self.settings.database_path);
        }
    }

    fn clear_active_build(&self, server: &str) {
        if let Ok(mut active) = self.active_builds.lock() {
            active.remove(server);
        }
    }

    fn require_configured(&self, server: &str) -> anyhow::Result<()> {
        if !self.settings.servers.iter().any(|value| value == server) {
            anyhow::bail!("server is not configured for namespace indexing");
        }
        Ok(())
    }

    fn with_database<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        let mut database = self
            .database
            .lock()
            .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
        if database.is_none() {
            tracing::debug!(
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                "initializing namespace index database handle"
            );
            *database = Some(IndexDb::open(&self.settings.database_path)?);
        }
        operation(database.as_mut().expect("database initialized"))
    }

    fn database_bytes(&self) -> anyhow::Result<u64> {
        self.with_database(|db| Ok(db.database_bytes()))
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
    }
}

fn is_quarantinable_index_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("unsupported namespace index schema version")
        || message.contains("invalid namespace index schema version")
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

fn parse_timestamp(value: &str) -> Option<SystemTime> {
    value.parse::<u128>().ok().and_then(|millis| {
        u64::try_from(millis)
            .ok()
            .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
    })
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
        InventoryStream, OpcValue, TagValue, WriteResult,
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
            refresh_interval_seconds: 86_400,
            batch_size: 100,
            item_rate_limit: 0,
            burst_size: 100,
            duty_cycle_percent: 100,
            quiet_period_seconds: 0,
            health_probe_interval_seconds: 30,
            health_latency_threshold_ms: 500,
            maintenance_windows: Vec::new(),
            concurrency: 1,
            query_cache_capacity: 256,
            paused: false,
            max_results: 50,
        }
    }

    #[test]
    fn normalization_and_timestamp_helpers_are_safe() {
        assert_eq!(normalize_query("  FCS0201   PV "), "fcs0201 pv");
        assert_eq!(escape_like(r"a%b_c\d"), r"a\%b\_c\\d");
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
    fn build_file_lock_is_exclusive_and_released() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("index.sqlite3");
        let lock = BuildFileLock::acquire(&database, "S").unwrap();
        assert!(lock.path.exists());
        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("build lock already exists"));
        drop(lock);
        assert!(!database.with_file_name("index.sqlite3.build.lock").exists());
        let replacement = BuildFileLock::acquire(&database, "S").unwrap();
        drop(replacement);
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
    fn sqlite_open_quarantines_invalid_schema_and_cleans_interrupted_builds() {
        let directory = tempdir().unwrap();
        let memory = IndexDb::open(Path::new(":memory:")).unwrap();
        assert_eq!(memory.database_bytes(), 0);
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
        assert!(reopened.status_rows("S").unwrap().is_empty());
        assert_eq!(reopened.search_generation("S").unwrap(), None);
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

        db.connection
            .execute("UPDATE entries SET kind = 99 WHERE server = 'S'", [])
            .unwrap();
        assert!(db.search("S", generation, "valid", 1, 10).is_err());
        db.connection
            .execute(
                "UPDATE entries SET kind = 1, breadcrumbs = 'not-json'
                 WHERE server = 'S'",
                [],
            )
            .unwrap();
        assert!(db.search("S", generation, "valid", 1, 10).is_err());
        assert!(
            db.promote("S", generation + 1, "2", &zero_progress())
                .is_err()
        );

        db.fail_generation("S", generation, "failed").unwrap();
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
        assert_eq!(db.status_rows("S").unwrap().len(), 1);
        db.discard_generation("S", replacement).unwrap();
        assert!(db.status_rows("S").unwrap().is_empty());

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
        drop(cleanup);
        assert!(IndexDb::open_once(&cleanup_path).is_err());

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
        drop_table(&mut promote_db, "entries");
        assert!(promote_db.promote("S", 1, "2", &zero_progress()).is_err());

        let mut fail_db = IndexDb::open(&directory.path().join("fail.sqlite3")).unwrap();
        drop_table(&mut fail_db, "generations");
        assert!(fail_db.fail_generation("S", 1, "failed").is_err());

        let mut discard_db = IndexDb::open(&directory.path().join("discard.sqlite3")).unwrap();
        drop_table(&mut discard_db, "entries_fts");
        assert!(discard_db.discard_generation("S", 1).is_err());

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
    fn promote_rejects_duplicate_item_ids_in_a_corrupted_schema() {
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
        assert_eq!(
            db.promote("S", generation, "2", &zero_progress())
                .unwrap_err()
                .to_string(),
            "namespace index generation count validation failed"
        );
    }

    #[tokio::test]
    async fn background_refresh_delay_uses_persisted_state() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        assert_eq!(manager.background_refresh_delay("S").await, RETRY_BACKOFF);

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
        assert!(ready_delay <= Duration::from_secs(86_400));
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
                }),
                retry_after: None,
                last_error: None,
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
                }),
                retry_after: None,
                last_error: None,
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
        assert_eq!(manager.background_refresh_delay("S").await, RETRY_BACKOFF);
        manager.refresh_if_due("S").await;

        manager.refresh_if_due("Other").await;
    }

    #[tokio::test]
    async fn manager_status_covers_partial_stale_refreshing_and_runtime_errors() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        assert_eq!(
            manager.status("S").await.unwrap().state,
            IndexState::NotIndexed
        );

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
                }),
                retry_after: None,
                last_error: Some("obsolete build failure".into()),
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
                }),
                retry_after: None,
                last_error: None,
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

    #[tokio::test]
    async fn refresh_database_failure_cancels_inventory_and_records_error() {
        let directory = tempdir().unwrap();
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
        assert!(!directory.path().join("index.sqlite3.build.lock").exists());
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
        assert!(!directory.path().join("index.sqlite3.build.lock").exists());
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

    #[tokio::test]
    async fn completed_inventory_warning_keeps_generation_active_and_searchable() {
        let directory = tempdir().unwrap();
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

    #[tokio::test]
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
            VecDeque::new(),
            vec!["invalid".into()],
            IndexState::Failed,
            Some("maintenance window must use HH:MM-HH:MM"),
        )
        .await;
    }

    #[tokio::test]
    async fn build_reports_batch_progress_and_promotion_database_failures() {
        let directory = tempdir().unwrap();
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
        manager.shutdown_background_indexing().await;
        assert_eq!(
            manager.refresh("S", true).await.unwrap().state,
            IndexState::NotIndexed
        );
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
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
                }),
                retry_after: None,
                last_error: None,
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
    async fn foreground_cleanup_without_a_runtime_build_is_safe() {
        let directory = tempdir().unwrap();
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(directory.path().join("index.sqlite3")),
        ));
        manager.foreground_end("Missing");
        tokio::time::sleep(Duration::from_millis(1)).await;
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
                }),
                retry_after: None,
                last_error: None,
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
            .enforce_duty_cycle(&trait_control, "S", Duration::from_millis(1))
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
                }),
                retry_after: None,
                last_error: None,
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

    async fn wait_for_build(manager: &Arc<IndexManager<MockOpcClient>>, expected: IndexState) {
        wait_for_state(manager, "S", expected).await;
    }

    async fn wait_for_state<C: OpcClient>(
        manager: &Arc<IndexManager<C>>,
        server: &str,
        expected: IndexState,
    ) {
        for _ in 0..100 {
            if manager.status(server).await.unwrap().state == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("index build did not reach {expected:?}");
    }

    async fn wait_for_counter(counter: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if counter.load(Ordering::Relaxed) >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("counter did not reach {expected}");
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
        pause_count: AtomicUsize,
        resume_count: AtomicUsize,
    }

    impl InventoryControl for RecordingInventoryControl {
        fn pause(&self) {
            self.pause_count.fetch_add(1, Ordering::Relaxed);
            self.paused.store(true, Ordering::Release);
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
    }

    struct VecInventoryStream {
        events: VecDeque<anyhow::Result<InventoryEvent>>,
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
    ) {
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
                }),
                retry_after: None,
                last_error: None,
            },
        );
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
                db.promote("S", generation, completed_at, &zero_progress())
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
            },
        }
    }
}
