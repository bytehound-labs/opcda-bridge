use super::{
    BuildFileLock, IndexControlAction, IndexManager, IndexStatus, RuntimeBuild, timestamp_now,
};
use crate::opc::{InventoryControl, InventoryHandle, OpcClient};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

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

impl<C: OpcClient> IndexManager<C> {
    pub async fn refresh(
        self: &Arc<Self>,
        server: &str,
        force: bool,
    ) -> Result<IndexStatus, IndexOperationError> {
        if self
            .is_deleting(server)
            .map_err(IndexOperationError::Internal)?
        {
            return Err(IndexOperationError::Deleting {
                server: server.to_string(),
            });
        }
        let enrolled = self
            .with_database_read(|db| db.enrollment(server))
            .map_err(IndexOperationError::Internal)?;
        if enrolled.is_none() {
            self.validate_server_for_enrollment(server).await?;
            self.with_database_write(|db| db.enroll(server, &timestamp_now()))
                .map_err(IndexOperationError::Internal)?;
        }
        self.refresh_enrolled(server, force)
            .await
            .map_err(IndexOperationError::Internal)
    }

    pub(super) async fn validate_server_for_enrollment(
        &self,
        server: &str,
    ) -> Result<(), IndexOperationError> {
        let servers = self
            .with_opc_timeout(
                "list servers for index enrollment",
                self.client.list_servers("localhost"),
            )
            .await
            .map_err(IndexOperationError::Internal)?;
        if servers.iter().any(|listed| listed == server) {
            Ok(())
        } else {
            Err(IndexOperationError::UnknownServer {
                server: server.to_string(),
            })
        }
    }

    pub(super) async fn refresh_enrolled(
        self: &Arc<Self>,
        server: &str,
        force: bool,
    ) -> anyhow::Result<IndexStatus> {
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
        let build_ownership = self.reserve_refresh_build(server, force)?;
        let Some(build_ownership) = build_ownership else {
            return self.status(server).await;
        };

        let initial_limits = self.initial_inventory_limits();
        let Some(handle) = self
            .start_refresh_inventory(server, &build_ownership, initial_limits)
            .await?
        else {
            return self.status(server).await;
        };
        let Some(control_was_cancelled_before_attach) =
            self.attach_refresh_control(server, &build_ownership, &handle, initial_limits)?
        else {
            return self.status(server).await;
        };
        tracing::info!(target: "opcda_bridge_gateway::index",
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
        let Some(generation) = self
            .start_refresh_generation(
                server,
                &handle.control,
                &build_ownership,
                control_was_cancelled_before_attach,
            )
            .await?
        else {
            return self.status(server).await;
        };
        self.launch_refresh_build(
            server,
            generation,
            handle,
            build_ownership,
            control_was_cancelled_before_attach,
        )?;
        self.status(server).await
    }

    pub(super) fn reserve_refresh_build(
        &self,
        server: &str,
        force: bool,
    ) -> anyhow::Result<Option<Arc<()>>> {
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
            return Ok(None);
        }
        if active_builds >= self.settings.concurrency.max(1) as usize {
            anyhow::bail!("namespace index build concurrency limit reached");
        }
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
        if let Ok(mut recorded_at) = self.commit_latency_recorded_at.lock() {
            recorded_at.remove(server);
        }
        state.last_error = None;
        Ok(Some(ownership))
    }

    pub(super) async fn start_refresh_generation(
        &self,
        server: &str,
        control: &Arc<dyn InventoryControl>,
        build_ownership: &Arc<()>,
        control_was_cancelled_before_attach: bool,
    ) -> anyhow::Result<Option<u64>> {
        let (organization, source) = match self
            .with_opc_timeout(
                "inventory capability probe",
                self.client.get_capabilities(server),
            )
            .await
        {
            Ok(capabilities) => (capabilities.organization, capabilities.source),
            Err(error) => {
                let cancelled = !control_was_cancelled_before_attach && control.is_cancelled();
                control.cancel();
                if cancelled {
                    self.finish_build_for_control_owned(server, control, build_ownership, None);
                    return Ok(None);
                }
                self.record_start_failure(server, build_ownership, &error.to_string())?;
                return Err(error);
            }
        };
        let generation = self.with_database_write(|db| {
            db.start_generation(server, organization, source, &timestamp_now())
        });
        let generation = match generation {
            Ok(generation) => generation,
            Err(error) => {
                tracing::error!(target: "opcda_bridge_gateway::index",
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    operation = "start_generation",
                    error = %error,
                    "namespace index database operation failed"
                );
                let cancelled = !control_was_cancelled_before_attach && control.is_cancelled();
                control.cancel();
                if cancelled {
                    self.finish_build_for_control_owned(server, control, build_ownership, None);
                    return Ok(None);
                }
                self.record_start_failure(server, build_ownership, &error.to_string())?;
                return Err(error);
            }
        };
        self.schedule_cleanup(server);
        Ok(Some(generation))
    }

    pub(super) fn launch_refresh_build(
        self: &Arc<Self>,
        server: &str,
        generation: u64,
        handle: InventoryHandle,
        build_ownership: Arc<()>,
        control_was_cancelled_before_attach: bool,
    ) -> anyhow::Result<()> {
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
                return Ok(());
            }
            self.finish_build_owned(server, &build_ownership, Some(error.to_string()));
            return Err(error);
        }
        if !control_was_cancelled_before_attach && handle.control.is_cancelled() {
            handle.control.cancel();
            self.abandon_generation(server, generation, "index build cancelled during startup");
            self.finish_build_for_control_owned(server, &handle.control, &build_ownership, None);
            return Ok(());
        }
        self.reconcile_pause_state(server);
        if self.background_tasks.is_shutting_down() {
            handle.control.cancel();
            self.abandon_generation(server, generation, "gateway shutdown before index build");
            self.finish_build_owned(server, &build_ownership, None);
            return Ok(());
        }
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        let control = Arc::clone(&handle.control);
        let reject_spawn = self.take_build_spawn_rejection();
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
        Ok(())
    }

    pub(super) fn take_pending_cancel(&self, server: &str) -> bool {
        match self.pending_cancels.lock() {
            Ok(mut pending) => pending.remove(server),
            Err(error) => {
                tracing::error!(target: "opcda_bridge_gateway::index",
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

    pub(super) fn clear_pending_cancel(&self, server: &str) {
        if let Err(error) = self.pending_cancels.lock().map(|mut pending| {
            pending.remove(server);
        }) {
            tracing::error!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                error = %error,
                "unable to clear pending namespace index cancellation"
            );
        }
    }

    pub async fn control(
        self: &Arc<Self>,
        server: &str,
        action: IndexControlAction,
    ) -> Result<IndexStatus, IndexOperationError> {
        match action {
            IndexControlAction::EnableAutoRefresh => {
                self.reject_if_deleting(server)?;
                self.change_auto_refresh(server, true)?;
            }
            IndexControlAction::DisableAutoRefresh => {
                self.reject_if_deleting(server)?;
                self.change_auto_refresh(server, false)?;
            }
            IndexControlAction::Delete => {
                self.delete_index(server).await?;
            }
            IndexControlAction::Pause | IndexControlAction::Resume | IndexControlAction::Cancel => {
                self.reject_if_deleting(server)?;
                self.require_enrollment(server)?;
                self.apply_control_action(server, action)
                    .map_err(IndexOperationError::Internal)?;
                if !matches!(action, IndexControlAction::Cancel) {
                    self.reconcile_pause_state(server);
                }
            }
        }
        self.status(server)
            .await
            .map_err(IndexOperationError::Internal)
    }

    pub(super) fn require_enrollment(&self, server: &str) -> Result<(), IndexOperationError> {
        self.reject_if_deleting(server)?;
        if self
            .with_database_read(|db| db.enrollment(server))
            .map_err(IndexOperationError::Internal)?
            .is_some()
        {
            Ok(())
        } else {
            Err(IndexOperationError::NotEnrolled {
                server: server.to_string(),
            })
        }
    }

    pub(super) fn change_auto_refresh(
        &self,
        server: &str,
        enabled: bool,
    ) -> Result<(), IndexOperationError> {
        self.reject_if_deleting(server)?;
        let changed = self
            .with_database_write(|db| db.set_auto_refresh(server, enabled))
            .map_err(IndexOperationError::Internal)?;
        if changed {
            Ok(())
        } else {
            Err(IndexOperationError::NotEnrolled {
                server: server.to_string(),
            })
        }
    }

    pub(super) fn is_deleting(&self, server: &str) -> anyhow::Result<bool> {
        self.deleting
            .lock()
            .map(|servers| servers.contains(server))
            .map_err(|_| anyhow::anyhow!("index deletion lock poisoned"))
    }

    pub(super) fn deletion_error(&self, server: &str) -> anyhow::Result<Option<String>> {
        self.deletion_errors
            .lock()
            .map(|errors| errors.get(server).cloned())
            .map_err(|_| anyhow::anyhow!("index deletion error lock poisoned"))
    }

    pub(super) fn reject_if_deleting(&self, server: &str) -> Result<(), IndexOperationError> {
        if self
            .is_deleting(server)
            .map_err(IndexOperationError::Internal)?
        {
            Err(IndexOperationError::Deleting {
                server: server.to_string(),
            })
        } else {
            Ok(())
        }
    }

    pub(super) fn reserve_deletion(&self, server: &str) -> Result<(), IndexOperationError> {
        let mut deleting = self.deleting.lock().map_err(|_| {
            IndexOperationError::Internal(anyhow::anyhow!("index deletion lock poisoned"))
        })?;
        if !deleting.insert(server.to_string()) {
            return Err(IndexOperationError::Deleting {
                server: server.to_string(),
            });
        }
        Ok(())
    }

    pub(super) async fn delete_index(
        self: &Arc<Self>,
        server: &str,
    ) -> Result<(), IndexOperationError> {
        self.reject_if_deleting(server)?;
        let enrolled = self
            .with_database_write(|db| db.set_auto_refresh(server, false))
            .map_err(IndexOperationError::Internal)?;
        if !enrolled {
            return Ok(());
        }
        self.reserve_deletion(server)?;
        if let Ok(mut errors) = self.deletion_errors.lock() {
            errors.remove(server);
        }
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        if !self.background_tasks.spawn(async move {
            let result = manager.delete_index_background(&server_name).await;
            if let Err(error) = result {
                tracing::error!(target: "opcda_bridge_gateway::index",
                    process_id = std::process::id(),
                    database = %manager.settings.database_path.display(),
                    server = %server_name,
                    error = %error,
                    "namespace index deletion failed"
                );
                if let Ok(mut errors) = manager.deletion_errors.lock() {
                    errors.insert(server_name.clone(), error.to_string());
                }
            }
            if let Ok(mut deleting) = manager.deleting.lock() {
                deleting.remove(&server_name);
            }
        }) {
            if let Ok(mut deleting) = self.deleting.lock() {
                deleting.remove(server);
            }
            return Err(IndexOperationError::Internal(anyhow::anyhow!(
                "gateway is shutting down"
            )));
        }
        Ok(())
    }

    pub(super) async fn delete_index_background(
        self: &Arc<Self>,
        server: &str,
    ) -> anyhow::Result<()> {
        self.cancel_active_build(server)?;
        self.wait_for_build_to_finish(server).await;
        let _lock = self.acquire_delete_lock(server).await?;
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        tokio::task::spawn_blocking(move || {
            manager.with_database_write(|db| db.delete_index(&server_name))
        })
        .await??;
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear_server(server);
        }
        if let Ok(mut runtime) = self.runtime.lock() {
            runtime.remove(server);
        }
        self.clear_pending_cancel(server);
        Ok(())
    }

    pub(super) fn cancel_active_build(&self, server: &str) -> anyhow::Result<()> {
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
        if let Some(build) = runtime
            .get_mut(server)
            .and_then(|state| state.build.as_mut())
        {
            self.cancel_build(server, build)?;
        }
        Ok(())
    }

    pub(super) async fn wait_for_build_to_finish(&self, server: &str) {
        loop {
            let notified = self.build_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let running = self
                .active_builds
                .lock()
                .map(|builds| builds.contains(server))
                .unwrap_or(true);
            if !running {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn acquire_delete_lock(&self, server: &str) -> anyhow::Result<BuildFileLock> {
        acquire_delete_lock_with(
            &self.settings.database_path,
            server,
            BuildFileLock::acquire,
            BuildFileLock::is_held,
        )
        .await
    }

    pub(super) fn apply_control_action(
        &self,
        server: &str,
        action: IndexControlAction,
    ) -> anyhow::Result<()> {
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("index runtime lock poisoned"))?;
        let Some(build) = runtime
            .get_mut(server)
            .and_then(|state| state.build.as_mut())
        else {
            return Ok(());
        };
        match action {
            IndexControlAction::Pause => build.operator_paused = true,
            IndexControlAction::Resume => Self::resume_build(build),
            IndexControlAction::Cancel => self.cancel_build(server, build)?,
            IndexControlAction::EnableAutoRefresh
            | IndexControlAction::DisableAutoRefresh
            | IndexControlAction::Delete => {}
        }
        Ok(())
    }

    pub(super) fn resume_build(build: &mut RuntimeBuild) {
        build.operator_paused = false;
        if build.foreground_users == 0
            && build
                .quiet_until
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            build.quiet_until = None;
        }
    }

    pub(super) fn cancel_build(&self, server: &str, build: &RuntimeBuild) -> anyhow::Result<()> {
        if let Some(control) = &build.control {
            control.cancel();
        } else {
            self.pending_cancels
                .lock()
                .map_err(|_| anyhow::anyhow!("index cancel lock poisoned"))?
                .insert(server.to_string());
        }
        Ok(())
    }
}

pub(super) async fn acquire_delete_lock_with<A, H>(
    database_path: &Path,
    server: &str,
    mut acquire: A,
    mut is_held: H,
) -> anyhow::Result<BuildFileLock>
where
    A: FnMut(&Path, &str) -> anyhow::Result<BuildFileLock>,
    H: FnMut(&Path, &str) -> anyhow::Result<bool>,
{
    loop {
        match acquire(database_path, server) {
            Ok(lock) => return Ok(lock),
            Err(_error) if is_held(database_path, server)? => {
                tracing::info!(target: "opcda_bridge_gateway::index",
                    server,
                    "waiting for external namespace index build before deletion"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
}
