use super::{
    DbStatus, IndexDb, IndexManager, RuntimeBuild, RuntimeState, StatusRows, instant_timestamp,
    parse_timestamp, scheduler, system_time_timestamp,
};
use crate::controller::{HostMetrics, HostMetricsProvider, InventoryLimits};
use crate::opc::{BrowseSource, InventoryProgress, NamespaceOrganization, OpcClient};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

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
    pub auto_refresh_policy: AutoRefreshPolicy,
    pub next_refresh_at: Option<String>,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_success_duration_ms: Option<u64>,
    pub retry_after: Option<String>,
    pub consecutive_failures: u32,
    pub circuit_open: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AutoRefreshPolicy {
    #[default]
    Allowed,
    Disabled,
    Paused,
}

pub struct ForegroundGuard<C: OpcClient> {
    pub(super) manager: Arc<IndexManager<C>>,
    pub(super) server: String,
}

#[derive(Default)]
pub(super) struct ForegroundMetricState {
    pub(super) latencies_ms: VecDeque<u64>,
    pub(super) operations: u64,
    pub(super) errors: u64,
    pub(super) bad_quality: u64,
    pub(super) last_error: bool,
    pub(super) last_bad_quality: bool,
    pub(super) last_health_failure_at: Option<Instant>,
    pub(super) last_bad_quality_at: Option<Instant>,
}

impl ForegroundMetricState {
    pub(super) fn record_health_at(
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

    pub(super) fn recent_health_failure(&self, now: Instant, max_age: Duration) -> bool {
        self.last_health_failure_at
            .is_some_and(|recorded| now.saturating_duration_since(recorded) <= max_age)
    }

    pub(super) fn recent_bad_quality(&self, now: Instant, max_age: Duration) -> bool {
        self.last_bad_quality_at
            .is_some_and(|recorded| now.saturating_duration_since(recorded) <= max_age)
    }

    pub(super) fn snapshot(&self, active_count: u64) -> ForegroundMetrics {
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

pub(super) fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let rank = (values.len() * percentile).div_ceil(100).max(1);
    let index = (rank - 1).min(values.len() - 1);
    values.get(index).copied()
}

#[derive(Default)]
pub(super) struct RuntimeStatus {
    pub(super) build: Option<RuntimeBuild>,
    pub(super) last_error: Option<String>,
    pub(super) retry_after: Option<SystemTime>,
    pub(super) consecutive_failures: u32,
    pub(super) circuit_open: bool,
    pub(super) health: HealthProbeState,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PauseOverlayState {
    pub(super) maintenance: bool,
    pub(super) health: bool,
}

impl<C: OpcClient> Drop for ForegroundGuard<C> {
    fn drop(&mut self) {
        self.manager.foreground_end(&self.server);
    }
}

impl<C: OpcClient> IndexManager<C> {
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

    pub(super) fn set_pause_overlay(
        &self,
        server: &str,
        maintenance: Option<bool>,
        health: Option<bool>,
    ) {
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
            tracing::error!(target: "opcda_bridge_gateway::index",
                server,
                "unable to update namespace index pause overlays because the overlay lock is poisoned"
            );
            return;
        }
        self.reconcile_pause_state(server);
    }

    pub(super) fn clear_pause_overlays(&self, server: &str) {
        if self
            .pause_overlays
            .lock()
            .map(|mut overlays| {
                overlays.remove(server);
            })
            .is_err()
        {
            tracing::error!(target: "opcda_bridge_gateway::index",
                server,
                "unable to clear namespace index pause overlays because the overlay lock is poisoned"
            );
        }
    }

    pub(super) fn reconcile_pause_state(&self, server: &str) {
        let overlay = match self.pause_overlays.lock() {
            Ok(overlays) => overlays.get(server).copied().unwrap_or_default(),
            Err(_) => {
                tracing::error!(target: "opcda_bridge_gateway::index",
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
                tracing::error!(target: "opcda_bridge_gateway::index",
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

    pub(super) fn decrement_foreground_users(&self, server: &str) {
        if let Ok(mut users) = self.foreground_users.lock() {
            let remaining = users
                .get_mut(server)
                .map(|count| {
                    *count = count.saturating_sub(1);
                    *count
                })
                .unwrap_or(0);
            if remaining == 0 {
                users.remove(server);
            }
        }
    }

    pub(super) fn update_runtime_after_foreground_end(&self, server: &str, quiet_period: Duration) {
        if let Ok(mut states) = self.runtime.lock()
            && let Some(build) = states
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.foreground_users = self
                .foreground_users
                .lock()
                .ok()
                .and_then(|users| users.get(server).copied())
                .unwrap_or(0);
            if build.foreground_users == 0 {
                build.quiet_until = Some(Instant::now() + quiet_period);
            }
        }
    }

    pub(super) fn clear_expired_foreground_quiet_period(
        runtime: &Mutex<HashMap<String, RuntimeState>>,
        server: &str,
    ) -> bool {
        if let Ok(mut states) = runtime.lock()
            && let Some(build) = states
                .get_mut(server)
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
        }
    }

    pub(super) fn foreground_end(self: &Arc<Self>, server: &str) {
        let quiet_period = Duration::from_secs(self.settings.quiet_period_seconds);
        self.decrement_foreground_users(server);
        self.update_runtime_after_foreground_end(server, quiet_period);
        self.reconcile_pause_state(server);

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            Self::clear_expired_foreground_quiet_period(&self.runtime, server);
            self.reconcile_pause_state(server);
            return;
        };

        let runtime = Arc::clone(&self.runtime);
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        handle.spawn(async move {
            tokio::time::sleep(quiet_period).await;
            if Self::clear_expired_foreground_quiet_period(&runtime, &server_name) {
                manager.reconcile_pause_state(&server_name);
            }
        });
    }

    pub async fn status(&self, server: &str) -> anyhow::Result<IndexStatus> {
        let is_deleting = self.is_deleting(server)?;
        let deletion_error = self.deletion_error(server)?;
        let deletion_failed = deletion_error.is_some();
        if is_deleting {
            let mut status = empty_status(server, IndexState::Deleting);
            status.scheduler.auto_refresh_policy = self.auto_refresh_policy();
            status.sentinel_configured = self.settings.sentinel_tag.is_some();
            let storage = storage_diagnostics_for_path(&self.settings.database_path);
            status.database_bytes = storage
                .main_bytes
                .saturating_add(storage.wal_bytes)
                .saturating_add(storage.shm_bytes);
            return Ok(status);
        }
        let is_promoting = self
            .promoting
            .lock()
            .ok()
            .is_some_and(|servers| servers.contains(server));
        let (enrolled, enrollment_error) = if is_promoting
            && self.settings.database_path != Path::new(":memory:")
        {
            Ok(
                match IndexDb::open_read_only(&self.settings.database_path)
                    .and_then(|db| db.is_enrolled(server))
                {
                    Ok(enrolled) => (enrolled, None),
                    Err(error) => {
                        tracing::warn!(target: "opcda_bridge_gateway::index", server, error = %error, "unable to read namespace index enrollment during promotion");
                        (true, Some(error.to_string()))
                    }
                },
            )
        } else {
            self.with_database_read(|db| db.is_enrolled(server))
                .map(|enrolled| (enrolled, None))
        }?;
        let sentinel_configured = self.settings.sentinel_tag.is_some();
        if !enrolled {
            let mut status = empty_status(server, IndexState::NotIndexed);
            status.scheduler.auto_refresh_policy = self.auto_refresh_policy();
            status.sentinel_configured = sentinel_configured;
            if let Some(error) = deletion_error {
                status.state = IndexState::Failed;
                status.last_error = Some(format!("namespace index deletion failed: {error}"));
            }
            return Ok(status);
        };
        let (rows, promotion_read_error) = self.load_status_rows(server, is_promoting)?;
        let promotion_read_error = promotion_read_error.or(enrollment_error);
        let rows = StatusRows::from_rows(&rows);
        if !is_promoting {
            self.load_persisted_retry_state(server)?;
        }
        let runtime = self.runtime_status(server)?;
        let storage = self.status_storage(is_promoting)?;
        let database_bytes = storage
            .main_bytes
            .saturating_add(storage.wal_bytes)
            .saturating_add(storage.shm_bytes);
        let mut status = self.base_status(
            server,
            &rows,
            runtime.build.as_ref(),
            is_promoting,
            database_bytes,
            sentinel_configured,
        );
        status.sentinel_configured = sentinel_configured;
        self.apply_status_errors(
            &mut status,
            runtime.build.is_some(),
            runtime.last_error.as_deref(),
            promotion_read_error.as_deref(),
        );
        if let Some(error) = deletion_error {
            status.state = IndexState::Failed;
            status.last_error = Some(format!("namespace index deletion failed: {error}"));
        }
        status.foreground_metrics = self.foreground_metrics_snapshot(server);
        status.host_metrics = self.host_metrics.latest();
        status.storage = storage;
        self.apply_runtime_status(&mut status, &runtime);
        status.scheduler = self.scheduler_diagnostics(
            server,
            &rows,
            &runtime,
            status.active_generation > 0 && promotion_read_error.is_none() && !deletion_failed,
        );
        Ok(status)
    }

    pub(super) fn load_status_rows(
        &self,
        server: &str,
        is_promoting: bool,
    ) -> anyhow::Result<(Vec<DbStatus>, Option<String>)> {
        if is_promoting && self.settings.database_path != Path::new(":memory:") {
            return match IndexDb::open_read_only(&self.settings.database_path)
                .and_then(|db| db.status_rows(server))
            {
                Ok(rows) => Ok((rows, None)),
                Err(error) => {
                    tracing::warn!(target: "opcda_bridge_gateway::index",
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server,
                        error = %error,
                        "unable to read namespace index status during promotion"
                    );
                    Ok((Vec::new(), Some(error.to_string())))
                }
            };
        }
        Ok((self.with_database_read(|db| db.status_rows(server))?, None))
    }

    pub(super) fn runtime_status(&self, server: &str) -> anyhow::Result<RuntimeStatus> {
        let runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
        Ok(runtime
            .get(server)
            .map(|state| RuntimeStatus {
                build: state.build.clone(),
                last_error: state.last_error.clone(),
                retry_after: state.retry_after,
                consecutive_failures: state.consecutive_failures,
                circuit_open: state.circuit_open,
                health: state.health,
            })
            .unwrap_or_default())
    }

    pub(super) fn status_storage(&self, is_promoting: bool) -> anyhow::Result<StorageDiagnostics> {
        if is_promoting {
            Ok(storage_diagnostics_for_path(&self.settings.database_path))
        } else {
            self.storage_diagnostics()
        }
    }

    pub(super) fn base_status(
        &self,
        server: &str,
        rows: &StatusRows,
        build: Option<&RuntimeBuild>,
        is_promoting: bool,
        database_bytes: u64,
        sentinel_configured: bool,
    ) -> IndexStatus {
        match build {
            Some(build) => {
                self.status_during_build(server, rows, build, is_promoting, database_bytes)
            }
            None => self.status_without_build(server, rows, database_bytes, sentinel_configured),
        }
    }

    pub(super) fn status_during_build(
        &self,
        server: &str,
        rows: &StatusRows,
        build: &RuntimeBuild,
        is_promoting: bool,
        database_bytes: u64,
    ) -> IndexStatus {
        let row = rows
            .active
            .clone()
            .or_else(|| rows.staging.clone())
            .or_else(|| rows.failed.clone());
        match row {
            Some(row) => {
                let state = if is_promoting {
                    IndexState::Promoting
                } else if rows.active.is_some() {
                    IndexState::Refreshing
                } else if rows.staging.is_some() {
                    IndexState::Partial
                } else {
                    IndexState::Failed
                };
                let mut status =
                    status_from_row(server, row, state, build.progress.clone(), database_bytes);
                status.started_at = Some(build.started_at.clone());
                status
            }
            None => self.status_from_runtime_build(server, build, is_promoting, database_bytes),
        }
    }

    pub(super) fn status_from_runtime_build(
        &self,
        server: &str,
        build: &RuntimeBuild,
        is_promoting: bool,
        database_bytes: u64,
    ) -> IndexStatus {
        let mut status = empty_status(
            server,
            if is_promoting {
                IndexState::Promoting
            } else {
                IndexState::Partial
            },
        );
        status.entry_count = build
            .progress
            .as_ref()
            .map_or(0, |progress| progress.entries_seen);
        status.unique_item_count = build
            .progress
            .as_ref()
            .map_or(0, |progress| progress.unique_items);
        status.started_at = Some(build.started_at.clone());
        status.database_bytes = database_bytes;
        status.progress = build.progress.clone();
        status.effective_limits = build.effective_limits;
        status.controller_state = build.controller_state;
        status.pause_reason = build.pause_reason;
        status.recovery_deadline = build.recovery_deadline.map(instant_timestamp);
        status
    }

    pub(super) fn status_without_build(
        &self,
        server: &str,
        rows: &StatusRows,
        database_bytes: u64,
        sentinel_configured: bool,
    ) -> IndexStatus {
        match (
            rows.active.clone(),
            rows.staging.clone(),
            rows.failed.clone(),
        ) {
            (Some(row), _, _) => self.status_from_active_row(server, rows, row, database_bytes),
            (None, Some(row), _) => {
                status_from_row(server, row, IndexState::Partial, None, database_bytes)
            }
            (None, None, Some(row)) => {
                status_from_row(server, row, IndexState::Failed, None, database_bytes)
            }
            (None, None, None) => {
                let mut status = empty_status(server, IndexState::NotIndexed);
                status.database_bytes = database_bytes;
                status.sentinel_configured = sentinel_configured;
                status
            }
        }
    }

    pub(super) fn status_from_active_row(
        &self,
        server: &str,
        rows: &StatusRows,
        row: DbStatus,
        database_bytes: u64,
    ) -> IndexStatus {
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
        let state = if rows.failed_after_active.is_some() {
            IndexState::Failed
        } else if stale {
            IndexState::Stale
        } else {
            IndexState::Ready
        };
        let mut status = status_from_row(server, row, state, None, database_bytes);
        if let Some(failed) = &rows.failed_after_active {
            status.last_error = failed.last_error.clone();
        }
        status
    }

    pub(super) fn apply_status_errors(
        &self,
        status: &mut IndexStatus,
        build_active: bool,
        runtime_error: Option<&str>,
        promotion_read_error: Option<&str>,
    ) {
        if !build_active && let Some(error) = runtime_error {
            status.state = IndexState::Failed;
            status.last_error = Some(error.to_owned());
        }
        if let Some(error) = promotion_read_error {
            status.last_error = Some(error.to_owned());
        }
    }

    pub(super) fn foreground_metrics_snapshot(&self, server: &str) -> ForegroundMetrics {
        let active_count = self
            .foreground_users
            .lock()
            .ok()
            .and_then(|users| users.get(server).copied())
            .unwrap_or(0) as u64;
        self.foreground_metrics
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
            })
    }

    pub(super) fn apply_runtime_status(&self, status: &mut IndexStatus, runtime: &RuntimeStatus) {
        status.health = runtime.health;
        if let Some(build) = &runtime.build {
            status.effective_limits = build.effective_limits;
            status.controller_state = build.controller_state;
            status.pause_reason = build.pause_reason;
            status.recovery_deadline = build.recovery_deadline.map(instant_timestamp);
            status.storage.last_commit_latency_ms = build.last_commit_latency_ms;
        }
    }

    pub(super) fn auto_refresh_policy(&self) -> AutoRefreshPolicy {
        if !self.settings.enabled {
            AutoRefreshPolicy::Disabled
        } else if self.settings.paused {
            AutoRefreshPolicy::Paused
        } else {
            AutoRefreshPolicy::Allowed
        }
    }

    pub(super) fn scheduler_diagnostics(
        &self,
        server: &str,
        rows: &StatusRows,
        runtime: &RuntimeStatus,
        usable_index: bool,
    ) -> SchedulerDiagnostics {
        let last_success_at = rows
            .active
            .as_ref()
            .and_then(|row| row.completed_at.clone());
        let auto_refresh_policy = self.auto_refresh_policy();
        let mut scheduler = SchedulerDiagnostics {
            auto_refresh_policy,
            next_refresh_at: (usable_index && auto_refresh_policy == AutoRefreshPolicy::Allowed)
                .then(|| {
                    self.next_refresh_at(server, last_success_at.as_deref())
                        .and_then(|next| parse_timestamp(&next))
                        .map(|next| runtime.retry_after.map_or(next, |retry| retry.max(next)))
                        .map(system_time_timestamp)
                })
                .flatten(),
            last_attempt_at: runtime
                .build
                .as_ref()
                .map(|build| build.started_at.clone())
                .or_else(|| rows.failed.as_ref().map(|row| row.started_at.clone()))
                .or_else(|| rows.active.as_ref().map(|row| row.started_at.clone())),
            last_success_at,
            last_success_duration_ms: rows.active.as_ref().and_then(status_duration_ms),
            ..SchedulerDiagnostics::default()
        };
        scheduler.retry_after = runtime.retry_after.map(system_time_timestamp);
        scheduler.consecutive_failures = runtime.consecutive_failures;
        scheduler.circuit_open = runtime.circuit_open;
        scheduler
    }

    pub(super) fn next_refresh_at(&self, server: &str, completed: Option<&str>) -> Option<String> {
        let completed = completed.and_then(parse_timestamp)?;
        completed
            .checked_add(
                Duration::from_secs(self.settings.refresh_interval_seconds.max(1)).saturating_add(
                    scheduler::deterministic_jitter(server, self.settings.schedule_jitter_seconds),
                ),
            )
            .map(system_time_timestamp)
    }

    pub(super) fn storage_diagnostics(&self) -> anyhow::Result<StorageDiagnostics> {
        let database = self
            .database
            .lock()
            .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
        Ok(database
            .as_ref()
            .map_or_else(StorageDiagnostics::default, IndexDb::storage_diagnostics))
    }
}

pub(super) fn storage_diagnostics_for_path(path: &Path) -> StorageDiagnostics {
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

pub(super) fn status_from_row(
    server: &str,
    row: DbStatus,
    state: IndexState,
    progress: Option<InventoryProgress>,
    database_bytes: u64,
) -> IndexStatus {
    IndexStatus {
        server: server.to_string(),
        state,
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

pub(super) fn status_duration_ms(row: &DbStatus) -> Option<u64> {
    row.completed_at
        .as_deref()
        .and_then(parse_timestamp)
        .and_then(|completed| {
            parse_timestamp(&row.started_at)
                .and_then(|started| completed.duration_since(started).ok())
        })
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
}

pub(super) fn empty_status(server: &str, state: IndexState) -> IndexStatus {
    IndexStatus {
        server: server.to_string(),
        state,
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
