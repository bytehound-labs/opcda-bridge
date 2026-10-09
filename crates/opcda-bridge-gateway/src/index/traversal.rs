use super::{
    ForegroundMetrics, HealthProbeState, IndexManager, MaintenanceWindow, PauseOverlayState,
    RuntimeBuild, RuntimeState, instant_timestamp, maintenance_window_active,
    parse_maintenance_windows, percentile, scheduler, timestamp_now, wait_with_cancellation,
};
use crate::config::ResolvedIndexConfig;
use crate::controller::{
    AdaptiveIndexController, ControllerConfig, ControllerObservation, InventoryLimits,
};
use crate::opc::{
    BrowseSource, InventoryCompleted, InventoryControl, InventoryEntry, InventoryEvent,
    InventoryHandle, InventoryNodeKind, InventoryPacing, InventoryProgress, InventorySliceBackend,
    InventorySliceObservation, InventoryStream, MAX_NATIVE_INVENTORY_BATCH_SIZE,
    NamespaceOrganization, OpcClient,
};
use chrono::Local;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub(super) struct HealthProbeObservation {
    pub(super) healthy: bool,
    pub(super) failure_reason: String,
    pub(super) sentinel_configured: bool,
}

pub(super) struct HealthSentinelObservation {
    pub(super) healthy: bool,
    pub(super) failure_reason: Option<String>,
}

pub(super) struct BuildRunState {
    pub(super) pending: Vec<InventoryEntry>,
    pub(super) last_progress: InventoryProgress,
    pub(super) telemetry: BuildTelemetry,
    pub(super) completed: bool,
    pub(super) cancelled: bool,
    pub(super) failed: Option<String>,
    pub(super) completion_warning: Option<String>,
    pub(super) completion_profile: Option<(NamespaceOrganization, BrowseSource)>,
    pub(super) terminal: bool,
    pub(super) accounted_active_time_ms: u64,
    pub(super) persisted_item_count: u64,
    pub(super) drained_event_count: u64,
    pub(super) received_entry_count: u64,
    pub(super) rate_limiter: ItemRateLimiter,
    pub(super) controller: Option<AdaptiveIndexController>,
    pub(super) effective_duty_cycle_percent: u8,
    pub(super) last_commit_at: Instant,
    pub(super) next_health_probe: Instant,
    pub(super) health_backoff: Duration,
}

#[derive(Debug, Default)]
pub(super) struct BuildTelemetry {
    pub(super) slice_count: u64,
    pub(super) slice_nodes_returned: u64,
    pub(super) slice_native_operations: u64,
    pub(super) slice_elapsed_ms: u64,
    pub(super) slice_elapsed_max_ms: u64,
    pub(super) slice_entries_delta: u64,
    pub(super) slice_entries_delta_max: u64,
    pub(super) slice_unique_items_delta: u64,
    pub(super) da2_slices: u64,
    pub(super) da3_slices: u64,
    pub(super) last_slice_entries_seen: u64,
    pub(super) last_slice_unique_items: u64,
    pub(super) progress_events: u64,
    pub(super) item_entries: u64,
    pub(super) branch_and_item_entries: u64,
    pub(super) commit_attempts: u64,
    pub(super) commit_failures: u64,
    pub(super) committed_entries: u64,
    pub(super) commit_elapsed_ms: u64,
    pub(super) commit_elapsed_max_ms: u64,
    pub(super) commit_latency_samples_ms: VecDeque<u64>,
    pub(super) terminal_event_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TerminalBuildCounts {
    pub(super) last_progress_entries_seen: u64,
    pub(super) last_progress_unique_items: u64,
    pub(super) persisted_items: u64,
    pub(super) drained_events: u64,
    pub(super) received_entry_events: u64,
    pub(super) pending_entries: u64,
    pub(super) pending_unique_items: u64,
}

pub(super) struct BuildFinalizationContext<'a> {
    pub(super) server: &'a str,
    pub(super) generation: u64,
    pub(super) control: &'a Arc<dyn InventoryControl>,
    pub(super) control_was_cancelled_before_cleanup: bool,
    pub(super) ownership: &'a Arc<()>,
    pub(super) build_started: Instant,
}

pub(super) enum BuildReadiness {
    Ready,
    Cancelled,
    Failed(String),
}

pub(super) enum HealthProbeAction {
    Ready,
    Wait(Duration),
    Probe,
}

pub(super) enum BuildEventOutcome {
    Continue,
    Stop,
    Cancelled,
    Failed(String),
}

pub(super) enum BuildLoopOutcome {
    Finished,
    Failed(String),
}

pub(super) struct CoordinatedInventoryControl {
    pub(super) state: Arc<CoordinatedInventoryControlState>,
}

pub(super) struct CoordinatedInventoryControlState {
    pub(super) controls: Mutex<HashMap<usize, Arc<dyn InventoryControl>>>,
    pub(super) cancelled: AtomicBool,
    pub(super) worker_stop_requested: AtomicBool,
    pub(super) paused: AtomicBool,
    pub(super) pacing: Mutex<InventoryPacing>,
}

pub(super) struct CoordinatedInventoryStream {
    pub(super) receiver: UnboundedReceiver<anyhow::Result<InventoryEvent>>,
    pub(super) control: Arc<CoordinatedInventoryControl>,
    pub(super) coordinator: Option<tokio::task::JoinHandle<()>>,
    pub(super) terminal_event_seen: bool,
}

pub(super) struct InventoryRootPlan {
    pub(super) root_entries: Vec<InventoryEntry>,
    pub(super) worker_roots: Vec<String>,
    pub(super) organization: NamespaceOrganization,
    pub(super) source: BrowseSource,
}

pub(super) enum WorkerInventoryMessage {
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

pub(super) enum WorkerEventAction {
    Continue,
    Completed,
    Stop,
}

pub(super) struct WorkerFinishedGuard {
    pub(super) sender: UnboundedSender<WorkerInventoryMessage>,
    pub(super) worker_id: usize,
}

pub(super) struct ItemRateLimiter {
    pub(super) rate: f64,
    pub(super) capacity: f64,
    pub(super) tokens: f64,
    pub(super) last_refill: Instant,
}

pub(super) struct BuildFinalizationGuard<C: OpcClient> {
    pub(super) manager: Arc<IndexManager<C>>,
    pub(super) server: String,
    pub(super) generation: u64,
    pub(super) control: Arc<dyn InventoryControl>,
    pub(super) ownership: Arc<()>,
    pub(super) armed: bool,
}

impl BuildTelemetry {
    pub(super) fn record_entry(&mut self, kind: InventoryNodeKind) {
        match kind {
            InventoryNodeKind::Item => self.item_entries += 1,
            InventoryNodeKind::BranchAndItem => self.branch_and_item_entries += 1,
        }
    }

    pub(super) fn record_progress(&mut self) {
        self.progress_events += 1;
    }

    pub(super) fn record_slice(&mut self, slice: &InventorySliceObservation) {
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

    pub(super) fn record_commit(&mut self, inserted: u64, elapsed: Duration, failed: bool) {
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

    pub(super) fn commit_latency_percentile(&self, percentile_value: usize) -> Option<u64> {
        let mut values = self
            .commit_latency_samples_ms
            .iter()
            .copied()
            .collect::<Vec<_>>();
        values.sort_unstable();
        percentile(&values, percentile_value)
    }

    pub(super) fn record_terminal_event(&mut self, elapsed: Duration) {
        self.terminal_event_ms = Some(elapsed.as_millis().try_into().unwrap_or(u64::MAX));
    }
}

impl BuildRunState {
    pub(super) fn new(
        settings: &ResolvedIndexConfig,
        controller: Option<AdaptiveIndexController>,
    ) -> Self {
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

    pub(super) fn record_completion(&mut self, result: InventoryCompleted, elapsed: Duration) {
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

    pub(super) fn terminal_counts(&self) -> TerminalBuildCounts {
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

impl Drop for WorkerFinishedGuard {
    fn drop(&mut self) {
        let _ = self.sender.send(WorkerInventoryMessage::Finished {
            _worker_id: self.worker_id,
        });
    }
}

impl CoordinatedInventoryControl {
    pub(super) fn new(initial_pacing: InventoryPacing) -> Arc<Self> {
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

    pub(super) fn register(
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

    pub(super) fn unregister(&self, worker_id: usize) {
        if let Ok(mut controls) = self.state.controls.lock() {
            controls.remove(&worker_id);
        }
    }

    pub(super) fn snapshot_controls(&self) -> Vec<Arc<dyn InventoryControl>> {
        self.state
            .controls
            .lock()
            .map(|controls| controls.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn set_pacing(&self, pacing: InventoryPacing) -> anyhow::Result<()> {
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

    pub(super) fn pause_all(&self) {
        self.state.paused.store(true, Ordering::Release);
        for control in self.snapshot_controls() {
            control.pause();
        }
    }

    pub(super) fn resume_all(&self) {
        self.state.paused.store(false, Ordering::Release);
        for control in self.snapshot_controls() {
            control.resume();
        }
    }

    pub(super) fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state
            .worker_stop_requested
            .store(true, Ordering::Release);
        self.stop_registered_workers();
    }

    pub(super) fn stop_workers(&self) {
        self.state
            .worker_stop_requested
            .store(true, Ordering::Release);
        self.stop_registered_workers();
    }

    pub(super) fn stop_registered_workers(&self) {
        for control in self.snapshot_controls() {
            control.cancel();
        }
    }

    pub(super) fn should_stop_workers(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
            || self.state.worker_stop_requested.load(Ordering::Acquire)
    }

    pub(super) fn is_cancelled(&self) -> bool {
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

impl ItemRateLimiter {
    pub(super) fn new(rate: u32, burst_size: u32) -> Self {
        let capacity = f64::from(burst_size.max(1));
        Self {
            rate: f64::from(rate),
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    pub(super) async fn acquire(&mut self, control: &Arc<dyn InventoryControl>) -> bool {
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

impl<C: OpcClient> BuildFinalizationGuard<C> {
    pub(super) fn new(
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

    pub(super) fn disarm(&mut self) {
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
        tracing::error!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.manager.settings.database_path.display(),
            server = %self.server,
            generation = self.generation,
            "namespace index build unwound unexpectedly; ownership was released"
        );
    }
}

impl<C: OpcClient> IndexManager<C> {
    pub(super) async fn start_refresh_inventory(
        self: &Arc<Self>,
        server: &str,
        build_ownership: &Arc<()>,
        initial_limits: InventoryLimits,
    ) -> anyhow::Result<Option<InventoryHandle>> {
        if let Some(root_item_id) = self.settings.inventory_root.as_deref() {
            return match self
                .with_opc_timeout(
                    "start root-scoped inventory",
                    self.client.start_inventory_at_root(
                        server,
                        root_item_id,
                        initial_limits.batch_size,
                    ),
                )
                .await
            {
                Ok(handle) => Ok(Some(handle)),
                Err(error) => {
                    if self.take_pending_cancel(server) {
                        self.finish_build_owned(server, build_ownership, None);
                        return Ok(None);
                    }
                    self.record_start_failure(server, build_ownership, &error.to_string())?;
                    Err(error)
                }
            };
        }
        if self.settings.worker_count > 1 {
            match self
                .start_coordinated_inventory(server, initial_limits)
                .await
            {
                Ok(Some(handle)) => return Ok(Some(handle)),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(target: "opcda_bridge_gateway::index",
                        server,
                        error = %error,
                        worker_count = self.settings.worker_count,
                        "root-partitioned inventory unavailable; falling back to one full-root worker"
                    );
                }
            }
        }
        match self
            .with_opc_timeout(
                "start inventory",
                self.client
                    .start_inventory(server, initial_limits.batch_size),
            )
            .await
        {
            Ok(handle) => Ok(Some(handle)),
            Err(error) => {
                if self.take_pending_cancel(server) {
                    self.finish_build_owned(server, build_ownership, None);
                    return Ok(None);
                }
                self.record_start_failure(server, build_ownership, &error.to_string())?;
                Err(error)
            }
        }
    }

    pub(super) async fn start_coordinated_inventory(
        self: &Arc<Self>,
        server: &str,
        initial_limits: InventoryLimits,
    ) -> anyhow::Result<Option<InventoryHandle>> {
        let Some(plan) = self.discover_inventory_roots(server).await? else {
            return Ok(None);
        };

        let worker_count =
            (self.settings.worker_count.max(1) as usize).min(plan.worker_roots.len());
        let control = CoordinatedInventoryControl::new(pacing_for_limits(initial_limits));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let coordinator_control = Arc::clone(&control);
        let manager = Arc::clone(self);
        let server_name = server.to_string();
        let coordinator = tokio::spawn(async move {
            manager
                .run_coordinated_inventory(
                    server_name,
                    plan,
                    worker_count,
                    initial_limits,
                    coordinator_control,
                    sender,
                )
                .await;
        });

        Ok(Some(InventoryHandle {
            stream: Box::new(CoordinatedInventoryStream {
                receiver,
                control: Arc::clone(&control),
                coordinator: Some(coordinator),
                terminal_event_seen: false,
            }),
            control,
        }))
    }

    pub(super) async fn discover_inventory_roots(
        &self,
        server: &str,
    ) -> anyhow::Result<Option<InventoryRootPlan>> {
        let capabilities = self
            .with_opc_timeout(
                "get inventory browse capabilities",
                self.client.get_capabilities(server),
            )
            .await?;
        if capabilities.organization != NamespaceOrganization::Hierarchical
            || !capabilities.supports_browse_sessions
        {
            return Ok(None);
        }

        let session_id = self
            .with_opc_timeout(
                "open inventory root browse session",
                self.client.open_browse_session(server),
            )
            .await?;
        let page_size = capabilities
            .max_page_size
            .clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE);
        let page_result = self
            .with_opc_timeout(
                "browse inventory root",
                self.client
                    .browse_page(&session_id, None, None, page_size, true),
            )
            .await;
        let close_result = self
            .with_opc_timeout(
                "close inventory root browse session",
                self.client.close_browse_session(&session_id),
            )
            .await;
        let page = page_result?;
        close_result?;
        if !page.complete || page.next_page_token.is_some() {
            anyhow::bail!("inventory root browse returned a continuation page");
        }

        let mut root_entries = Vec::new();
        let mut worker_roots = Vec::new();
        for node in page.nodes {
            match node.kind {
                crate::opc::BrowseNodeKind::Item => {
                    if let Some(item_id) = node.item_id {
                        root_entries.push(InventoryEntry {
                            display_name: node.display_name,
                            item_id,
                            kind: InventoryNodeKind::Item,
                            breadcrumbs: Vec::new(),
                        });
                    }
                }
                crate::opc::BrowseNodeKind::BranchAndItem => {
                    if let Some(item_id) = node.item_id {
                        root_entries.push(InventoryEntry {
                            display_name: node.display_name.clone(),
                            item_id: item_id.clone(),
                            kind: InventoryNodeKind::BranchAndItem,
                            breadcrumbs: Vec::new(),
                        });
                        worker_roots.push(item_id);
                    }
                }
                crate::opc::BrowseNodeKind::Branch => {
                    if let Some(item_id) = node.item_id {
                        worker_roots.push(item_id);
                    }
                }
            }
        }
        worker_roots.sort();
        worker_roots.dedup();
        if worker_roots.len() < 2 {
            return Ok(None);
        }
        Ok(Some(InventoryRootPlan {
            root_entries,
            worker_roots,
            organization: page.organization,
            source: page.source,
        }))
    }

    pub(super) async fn run_coordinated_inventory(
        self: Arc<Self>,
        server: String,
        plan: InventoryRootPlan,
        worker_count: usize,
        initial_limits: InventoryLimits,
        control: Arc<CoordinatedInventoryControl>,
        sender: UnboundedSender<anyhow::Result<InventoryEvent>>,
    ) {
        let queue = Arc::new(Mutex::new(VecDeque::from(plan.worker_roots.clone())));
        let (worker_sender, mut worker_receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let mut workers = Vec::new();
        for worker_id in 0..worker_count {
            let manager = Arc::clone(&self);
            let queue = Arc::clone(&queue);
            let control = Arc::clone(&control);
            let worker_sender = worker_sender.clone();
            let server = server.clone();
            workers.push(tokio::spawn(async move {
                manager
                    .run_inventory_worker(
                        server,
                        worker_id,
                        queue,
                        initial_limits,
                        control,
                        worker_sender,
                    )
                    .await;
            }));
        }
        drop(worker_sender);

        let mut seen = HashSet::new();
        let mut worker_progress = HashMap::new();
        let mut worker_last_progress = HashMap::new();
        let mut next_slice_sequence = 0u64;

        for entry in &plan.root_entries {
            if !self.emit_coordinated_entry(entry.clone(), &mut seen, &control, &sender) {
                control.cancel();
                break;
            }
        }

        while let Some(message) = worker_receiver.recv().await {
            match message {
                WorkerInventoryMessage::Started { worker_id } => {
                    worker_progress
                        .entry(worker_id)
                        .or_insert_with(zero_inventory_progress);
                    worker_last_progress.remove(&worker_id);
                }
                WorkerInventoryMessage::Entry(entry) => {
                    let _ = self.emit_coordinated_entry(entry, &mut seen, &control, &sender);
                }
                WorkerInventoryMessage::Progress {
                    worker_id,
                    progress,
                } => {
                    let cumulative = worker_progress
                        .entry(worker_id)
                        .or_insert_with(zero_inventory_progress);
                    accumulate_inventory_progress(
                        cumulative,
                        worker_last_progress.get(&worker_id),
                        &progress,
                    );
                    worker_last_progress.insert(worker_id, progress);
                    let aggregate =
                        aggregate_inventory_progress(&worker_progress, seen.len() as u64);
                    let _ = sender.send(Ok(InventoryEvent::Progress(aggregate)));
                }
                WorkerInventoryMessage::Slice(slice) => {
                    let sequence = next_slice_sequence;
                    next_slice_sequence = next_slice_sequence.saturating_add(1);
                    let aggregate = InventorySliceObservation {
                        sequence,
                        entries_seen: slice.entries_seen,
                        unique_items: seen.len() as u64,
                        ..slice
                    };
                    let _ = sender.send(Ok(InventoryEvent::Slice(aggregate)));
                }
                WorkerInventoryMessage::Completed { worker_id, result } => {
                    if !result.complete && !result.cancelled && !result.truncated {
                        control.cancel();
                        let _ = sender.send(Err(anyhow::anyhow!(
                            "inventory worker {worker_id} ended before completion"
                        )));
                        for worker in workers {
                            let _ = worker.await;
                        }
                        return;
                    }
                }
                WorkerInventoryMessage::Failed { worker_id, error } => {
                    control.cancel();
                    let _ = sender.send(Err(anyhow::anyhow!(
                        "inventory worker {worker_id} failed: {error}"
                    )));
                    for worker in workers {
                        let _ = worker.await;
                    }
                    return;
                }
                WorkerInventoryMessage::Finished { .. } => {}
            }
        }
        Self::finish_coordinated_inventory_after_channel_close(&plan, &control, workers, &sender)
            .await;
    }

    pub(super) async fn finish_coordinated_inventory_after_channel_close(
        plan: &InventoryRootPlan,
        control: &CoordinatedInventoryControl,
        workers: Vec<tokio::task::JoinHandle<()>>,
        sender: &UnboundedSender<anyhow::Result<InventoryEvent>>,
    ) {
        let mut worker_task_error = None;
        control.stop_workers();
        for worker in workers {
            if let Err(error) = worker.await {
                worker_task_error.get_or_insert(error);
            }
        }
        if let Some(error) = worker_task_error {
            let _ = sender.send(Err(anyhow::anyhow!(
                "coordinated inventory worker task failed: {error}"
            )));
        } else {
            let cancelled = control.is_cancelled();
            let warning = cancelled.then(|| "inventory cancelled".to_string());
            let _ = sender.send(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: !cancelled,
                cancelled,
                truncated: false,
                warning,
                organization: plan.organization,
                source: plan.source,
            })));
        }
    }

    pub(super) fn emit_coordinated_entry(
        &self,
        entry: InventoryEntry,
        seen: &mut HashSet<String>,
        control: &CoordinatedInventoryControl,
        sender: &UnboundedSender<anyhow::Result<InventoryEvent>>,
    ) -> bool {
        if !seen.insert(entry.item_id.clone()) {
            return true;
        }
        if sender.send(Ok(InventoryEvent::Entry(entry))).is_err() {
            control.cancel();
            return false;
        }
        true
    }

    pub(super) async fn run_inventory_worker(
        self: Arc<Self>,
        server: String,
        worker_id: usize,
        queue: Arc<Mutex<VecDeque<String>>>,
        initial_limits: InventoryLimits,
        control: Arc<CoordinatedInventoryControl>,
        sender: UnboundedSender<WorkerInventoryMessage>,
    ) {
        let _finished = WorkerFinishedGuard {
            sender: sender.clone(),
            worker_id,
        };
        loop {
            if control.should_stop_workers() {
                break;
            }
            let root = match Self::pop_inventory_root(&queue) {
                Ok(root) => root,
                Err(error) => {
                    let _ = sender.send(WorkerInventoryMessage::Failed {
                        worker_id,
                        error: error.to_string(),
                    });
                    return;
                }
            };
            let Some(root) = root else {
                break;
            };
            let Some(mut stream) = (match self
                .start_inventory_worker_stream(
                    &server,
                    &root,
                    worker_id,
                    initial_limits,
                    control.as_ref(),
                    &sender,
                )
                .await
            {
                Ok(stream) => stream,
                Err(()) => return,
            }) else {
                break;
            };
            let completed = Self::forward_inventory_worker_stream(
                &mut *stream,
                worker_id,
                control.as_ref(),
                &sender,
            )
            .await;
            let _ = stream.shutdown().await;
            self.unregister_coordinated_worker(&control, worker_id);
            if !completed && !control.should_stop_workers() {
                let _ = sender.send(WorkerInventoryMessage::Failed {
                    worker_id,
                    error: "inventory worker stream ended before completion".to_string(),
                });
                control.cancel();
                break;
            }
        }
    }

    pub(super) async fn start_inventory_worker_stream(
        &self,
        server: &str,
        root: &str,
        worker_id: usize,
        initial_limits: InventoryLimits,
        control: &CoordinatedInventoryControl,
        sender: &UnboundedSender<WorkerInventoryMessage>,
    ) -> Result<Option<Box<dyn InventoryStream>>, ()> {
        let handle = match self
            .with_opc_timeout(
                "start root inventory",
                self.client
                    .start_inventory_at_root(server, root, initial_limits.batch_size),
            )
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                if control.should_stop_workers() {
                    return Ok(None);
                }
                let _ = sender.send(WorkerInventoryMessage::Failed {
                    worker_id,
                    error: error.to_string(),
                });
                control.cancel();
                return Err(());
            }
        };
        let InventoryHandle {
            stream,
            control: worker_control,
        } = handle;
        match control.register(worker_id, worker_control) {
            Ok(true) => {}
            Ok(false) => {
                let mut stream = stream;
                let _ = stream.shutdown().await;
                return Ok(None);
            }
            Err(error) => {
                let mut stream = stream;
                let _ = stream.shutdown().await;
                if control.should_stop_workers() {
                    return Ok(None);
                }
                let _ = sender.send(WorkerInventoryMessage::Failed {
                    worker_id,
                    error: error.to_string(),
                });
                control.cancel();
                return Err(());
            }
        }
        let mut stream = stream;
        if sender
            .send(WorkerInventoryMessage::Started { worker_id })
            .is_err()
        {
            control.cancel();
            let _ = stream.shutdown().await;
            return Ok(None);
        }
        Ok(Some(stream))
    }

    pub(super) async fn forward_inventory_worker_stream(
        stream: &mut dyn InventoryStream,
        worker_id: usize,
        control: &CoordinatedInventoryControl,
        sender: &UnboundedSender<WorkerInventoryMessage>,
    ) -> bool {
        while let Some(event) = stream.next().await {
            match Self::forward_inventory_event(event, worker_id, control, sender) {
                WorkerEventAction::Continue => {}
                WorkerEventAction::Completed => return true,
                WorkerEventAction::Stop => return false,
            }
        }
        false
    }

    pub(super) fn forward_inventory_event(
        event: anyhow::Result<InventoryEvent>,
        worker_id: usize,
        control: &CoordinatedInventoryControl,
        sender: &UnboundedSender<WorkerInventoryMessage>,
    ) -> WorkerEventAction {
        match event {
            Ok(InventoryEvent::Entry(entry)) => {
                if sender.send(WorkerInventoryMessage::Entry(entry)).is_err() {
                    control.cancel();
                    WorkerEventAction::Stop
                } else {
                    WorkerEventAction::Continue
                }
            }
            Ok(InventoryEvent::Progress(progress)) => {
                if sender
                    .send(WorkerInventoryMessage::Progress {
                        worker_id,
                        progress,
                    })
                    .is_err()
                {
                    control.cancel();
                    WorkerEventAction::Stop
                } else {
                    WorkerEventAction::Continue
                }
            }
            Ok(InventoryEvent::Slice(slice)) => {
                if sender.send(WorkerInventoryMessage::Slice(slice)).is_err() {
                    control.cancel();
                    WorkerEventAction::Stop
                } else {
                    WorkerEventAction::Continue
                }
            }
            Ok(InventoryEvent::Completed(result)) => {
                if sender
                    .send(WorkerInventoryMessage::Completed { worker_id, result })
                    .is_err()
                {
                    control.cancel();
                    WorkerEventAction::Stop
                } else {
                    WorkerEventAction::Completed
                }
            }
            Err(error) => {
                if !control.should_stop_workers() {
                    let _ = sender.send(WorkerInventoryMessage::Failed {
                        worker_id,
                        error: error.to_string(),
                    });
                    control.cancel();
                }
                WorkerEventAction::Stop
            }
        }
    }

    pub(super) fn unregister_coordinated_worker(
        &self,
        control: &CoordinatedInventoryControl,
        worker_id: usize,
    ) {
        control.unregister(worker_id);
    }

    pub(super) fn pop_inventory_root(
        queue: &Mutex<VecDeque<String>>,
    ) -> anyhow::Result<Option<String>> {
        queue
            .lock()
            .map(|mut roots| roots.pop_front())
            .map_err(|error| anyhow::anyhow!("inventory root queue lock poisoned: {error}"))
    }

    pub(super) fn attach_refresh_control(
        &self,
        server: &str,
        build_ownership: &Arc<()>,
        handle: &InventoryHandle,
        initial_limits: InventoryLimits,
    ) -> anyhow::Result<Option<bool>> {
        let control_was_cancelled_before_attach = handle.control.is_cancelled();
        if let Err(error) = handle.control.set_pacing(pacing_for_limits(initial_limits)) {
            let message = format!("unable to apply initial inventory pacing: {error}");
            let cancelled = self.take_pending_cancel(server)
                || (!control_was_cancelled_before_attach && handle.control.is_cancelled());
            handle.control.cancel();
            if cancelled {
                self.finish_build_owned(server, build_ownership, None);
                return Ok(None);
            }
            self.record_start_failure(server, build_ownership, &message)?;
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
                self.finish_build_owned(server, build_ownership, None);
                return Ok(None);
            }
            self.finish_build_owned(server, build_ownership, Some(error.to_string()));
            return Err(error);
        }
        if self.take_pending_cancel(server) {
            handle.control.cancel();
            self.finish_build_for_control_owned(server, &handle.control, build_ownership, None);
            return Ok(None);
        }
        Ok(Some(control_was_cancelled_before_attach))
    }

    pub(super) fn controller_config(&self) -> ControllerConfig {
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
            foreground_latency_soft_ms: self.settings.adaptive_foreground_soft_latency_ms.max(1),
            foreground_latency_hard_ms: self
                .settings
                .adaptive_foreground_hard_latency_ms
                .max(self.settings.adaptive_foreground_soft_latency_ms.max(1)),
        }
    }

    pub(super) async fn with_opc_timeout<T>(
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

    pub(super) fn mark_promoting(&self, server: &str) -> anyhow::Result<()> {
        self.promoting
            .lock()
            .map_err(|_| anyhow::anyhow!("index promotion lock poisoned"))?
            .insert(server.to_string());
        Ok(())
    }

    pub(super) fn clear_promoting(&self, server: &str) {
        if let Err(error) = self.promoting.lock().map(|mut servers| {
            servers.remove(server);
        }) {
            tracing::error!(target: "opcda_bridge_gateway::index",
                server = %server,
                error = %error,
                "unable to clear namespace index promotion state"
            );
        }
    }

    pub(super) fn initial_inventory_limits(&self) -> InventoryLimits {
        if !self.settings.adaptive {
            return InventoryLimits {
                item_rate_per_second: self.settings.item_rate_limit,
                batch_size: self.settings.inventory_batch_size,
                duty_cycle_percent: self.settings.duty_cycle_percent,
            };
        }
        AdaptiveIndexController::new(self.controller_config(), Instant::now()).limits()
    }

    pub(super) async fn run_build(
        self: Arc<Self>,
        server: String,
        generation: u64,
        inventory_handle: InventoryHandle,
        ownership: Arc<()>,
    ) {
        let mut finalization = BuildFinalizationGuard::new(
            Arc::clone(&self),
            server.clone(),
            generation,
            Arc::clone(&inventory_handle.control),
            Arc::clone(&ownership),
        );
        // Keep the stream local to a scope declared after the finalization guard.
        // If the build task unwinds, Rust drops this handle before the guard can
        // release ownership and the file lock.
        let mut handle = inventory_handle;
        let build_started = Instant::now();
        let maintenance_windows =
            match parse_maintenance_windows(&self.settings.maintenance_windows) {
                Ok(windows) => windows,
                Err(error) => {
                    handle.control.cancel();
                    let control = Arc::clone(&handle.control);
                    drop(handle);
                    let message = error.to_string();
                    self.fail_generation_and_schedule_cleanup(&server, generation, &message);
                    self.finish_build_for_control_owned(
                        &server,
                        &control,
                        &ownership,
                        Some(message),
                    );
                    finalization.disarm();
                    return;
                }
            };
        let controller = self
            .settings
            .adaptive
            .then(|| AdaptiveIndexController::new(self.controller_config(), build_started));
        let mut state = BuildRunState::new(&self.settings, controller);
        if let Some(controller) = state.controller.as_ref() {
            self.update_runtime_controller(&server, controller.limits(), controller.state(), None);
        }
        state.next_health_probe = Instant::now();
        let outcome = self
            .run_build_loop(
                &server,
                generation,
                &mut handle,
                &maintenance_windows,
                &mut state,
                build_started,
            )
            .await;
        let control = Arc::clone(&handle.control);
        let control_was_cancelled_before_cleanup = control.is_cancelled();
        drop(handle);
        self.finalize_build(
            BuildFinalizationContext {
                server: &server,
                generation,
                control: &control,
                control_was_cancelled_before_cleanup,
                ownership: &ownership,
                build_started,
            },
            state,
            outcome,
        );
        finalization.disarm();
    }

    pub(super) async fn run_build_loop(
        &self,
        server: &str,
        generation: u64,
        handle: &mut InventoryHandle,
        maintenance_windows: &[MaintenanceWindow],
        state: &mut BuildRunState,
        build_started: Instant,
    ) -> BuildLoopOutcome {
        loop {
            if let Some(error) = self.commit_pending_if_due(server, generation, state) {
                state.failed = Some(error);
                break;
            }
            match self
                .wait_for_build_readiness(&handle.control, server, maintenance_windows, state)
                .await
            {
                BuildReadiness::Ready => {}
                BuildReadiness::Cancelled => {
                    state.cancelled = true;
                    break;
                }
                BuildReadiness::Failed(error) => {
                    state.failed = Some(error);
                    break;
                }
            }
            let Ok(event) = tokio::time::timeout(
                Duration::from_secs(self.settings.operation_timeout_seconds.max(1)),
                handle.stream.next(),
            )
            .await
            else {
                handle.control.cancel();
                state.failed = Some(format!(
                    "inventory event timed out after {} seconds",
                    self.settings.operation_timeout_seconds.max(1)
                ));
                break;
            };
            let Some(event) = event else {
                break;
            };
            state.drained_event_count = state.drained_event_count.saturating_add(1);
            match self
                .handle_inventory_event(
                    server,
                    generation,
                    &handle.control,
                    state,
                    build_started,
                    event,
                )
                .await
            {
                BuildEventOutcome::Continue => {}
                BuildEventOutcome::Stop => break,
                BuildEventOutcome::Cancelled => {
                    state.cancelled = true;
                    break;
                }
                BuildEventOutcome::Failed(error) => {
                    state.failed = Some(error);
                    break;
                }
            }
        }
        if let Err(error) = handle.stream.shutdown().await {
            if state.failed.is_none() {
                state.failed = Some(format!("inventory worker shutdown failed: {error}"));
            } else {
                tracing::warn!(target: "opcda_bridge_gateway::index",
                    server = %server,
                    generation,
                    error = %error,
                    "inventory worker shutdown also failed after build failure"
                );
            }
        }
        if !state.terminal && state.failed.is_none() {
            state.failed = Some("inventory stream ended before completion".to_string());
        }
        if !state.pending.is_empty() && state.failed.is_none() {
            match self.commit_pending_entries_with_telemetry(server, generation, state) {
                Ok(inserted) => {
                    state.persisted_item_count =
                        state.persisted_item_count.saturating_add(inserted);
                }
                Err(error) => {
                    self.log_entry_commit_failure(server, generation, state.pending.len(), &error);
                    state.failed = Some(error.to_string());
                }
            }
        }
        state
            .failed
            .take()
            .map_or(BuildLoopOutcome::Finished, BuildLoopOutcome::Failed)
    }

    pub(super) async fn wait_for_build_readiness(
        &self,
        control: &Arc<dyn InventoryControl>,
        server: &str,
        maintenance_windows: &[MaintenanceWindow],
        state: &mut BuildRunState,
    ) -> BuildReadiness {
        if !self
            .wait_for_maintenance(control, server, maintenance_windows)
            .await
        {
            return BuildReadiness::Cancelled;
        }
        if !self
            .wait_for_health(
                control,
                server,
                &mut state.next_health_probe,
                &mut state.health_backoff,
            )
            .await
        {
            return BuildReadiness::Cancelled;
        }
        let Some(controller) = state.controller.as_mut() else {
            return BuildReadiness::Ready;
        };
        match self
            .wait_for_controller_recovery(control, server, controller)
            .await
        {
            Ok(true) => BuildReadiness::Ready,
            Ok(false) => BuildReadiness::Cancelled,
            Err(error) => {
                control.cancel();
                BuildReadiness::Failed(error.to_string())
            }
        }
    }

    pub(super) async fn handle_inventory_event(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        state: &mut BuildRunState,
        build_started: Instant,
        event: anyhow::Result<InventoryEvent>,
    ) -> BuildEventOutcome {
        match event {
            Ok(InventoryEvent::Entry(entry)) => {
                state.received_entry_count = state.received_entry_count.saturating_add(1);
                state.telemetry.record_entry(entry.kind);
                self.handle_entry_event(server, generation, control, state, entry)
                    .await
            }
            Ok(InventoryEvent::Progress(progress)) => {
                state.telemetry.record_progress();
                self.handle_progress_event(server, generation, control, state, progress)
                    .await
            }
            Ok(InventoryEvent::Slice(slice)) => {
                self.handle_slice_event(server, generation, control, state, slice)
            }
            Ok(InventoryEvent::Completed(result)) => {
                state.record_completion(result, build_started.elapsed());
                BuildEventOutcome::Stop
            }
            Err(error) => {
                tracing::error!(target: "opcda_bridge_gateway::index",
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server = %server,
                    generation,
                    operation = "insert_entries",
                    batch_size = state.pending.len(),
                    error = %error,
                    "namespace index database operation failed"
                );
                BuildEventOutcome::Failed(error.to_string())
            }
        }
    }

    pub(super) async fn handle_entry_event(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        state: &mut BuildRunState,
        entry: InventoryEntry,
    ) -> BuildEventOutcome {
        if !state.rate_limiter.acquire(control).await {
            return BuildEventOutcome::Cancelled;
        }
        state.pending.push(entry);
        if state.pending.len() < self.settings.commit_batch_size as usize {
            return BuildEventOutcome::Continue;
        }
        match self.commit_pending_entries_with_telemetry(server, generation, state) {
            Ok(inserted) => {
                state.persisted_item_count = state.persisted_item_count.saturating_add(inserted);
                state.last_commit_at = Instant::now();
                BuildEventOutcome::Continue
            }
            Err(error) => {
                self.log_entry_commit_failure(server, generation, state.pending.len(), &error);
                BuildEventOutcome::Failed(error.to_string())
            }
        }
    }

    pub(super) async fn handle_progress_event(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        state: &mut BuildRunState,
        progress: InventoryProgress,
    ) -> BuildEventOutcome {
        let active_time_delta_ms = progress
            .active_time_ms
            .saturating_sub(state.accounted_active_time_ms);
        state.accounted_active_time_ms = progress.active_time_ms;
        state.last_progress = progress.clone();
        if let Err(error) =
            self.with_database_write(|db| db.update_progress(server, generation, &progress))
        {
            tracing::error!(target: "opcda_bridge_gateway::index",
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
            return BuildEventOutcome::Failed(error.to_string());
        }
        self.update_runtime_progress(server, progress);
        if active_time_delta_ms > 0
            && !self
                .enforce_duty_cycle(
                    control,
                    server,
                    Duration::from_millis(active_time_delta_ms),
                    state.effective_duty_cycle_percent,
                )
                .await
        {
            return BuildEventOutcome::Cancelled;
        }
        BuildEventOutcome::Continue
    }

    pub(super) fn handle_slice_event(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        state: &mut BuildRunState,
        slice: InventorySliceObservation,
    ) -> BuildEventOutcome {
        state.telemetry.record_slice(&slice);
        let Some(controller) = state.controller.as_mut() else {
            return BuildEventOutcome::Continue;
        };
        let decision = controller.observe(
            Instant::now(),
            self.controller_observation_for_slice(server, &slice),
        );
        state.effective_duty_cycle_percent = decision.limits.duty_cycle_percent;
        self.update_runtime_controller(
            server,
            decision.limits,
            decision.state,
            decision.recovery_at,
        );
        if let Err(error) = control.set_pacing(pacing_for_limits(decision.limits)) {
            let message = format!(
                "unable to update adaptive inventory pacing after slice {}: {error}",
                slice.sequence
            );
            tracing::error!(target: "opcda_bridge_gateway::index",
                server = %server,
                generation,
                sequence = slice.sequence,
                error = %error,
                "namespace index pacing update failed"
            );
            control.cancel();
            return BuildEventOutcome::Failed(message);
        }
        tracing::debug!(target: "opcda_bridge_gateway::index",
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
        BuildEventOutcome::Continue
    }

    pub(super) fn commit_pending_if_due(
        &self,
        server: &str,
        generation: u64,
        state: &mut BuildRunState,
    ) -> Option<String> {
        if state.pending.is_empty()
            || state.last_commit_at.elapsed()
                < Duration::from_millis(self.settings.commit_interval_ms.max(1))
        {
            return None;
        }
        match self.commit_pending_entries_with_telemetry(server, generation, state) {
            Ok(inserted) => {
                state.persisted_item_count = state.persisted_item_count.saturating_add(inserted);
                state.last_commit_at = Instant::now();
                None
            }
            Err(error) => {
                self.log_entry_commit_failure(server, generation, state.pending.len(), &error);
                Some(error.to_string())
            }
        }
    }

    pub(super) fn log_entry_commit_failure(
        &self,
        server: &str,
        generation: u64,
        batch_size: usize,
        error: &anyhow::Error,
    ) {
        tracing::error!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server = %server,
            generation,
            operation = "insert_entries",
            batch_size,
            error = %error,
            "namespace index database operation failed"
        );
    }

    pub(super) fn finalize_build(
        &self,
        context: BuildFinalizationContext<'_>,
        state: BuildRunState,
        outcome: BuildLoopOutcome,
    ) {
        let completed =
            state.completed && !state.cancelled && !context.control_was_cancelled_before_cleanup;
        let outcome_label = match &outcome {
            BuildLoopOutcome::Failed(_) => "failed",
            BuildLoopOutcome::Finished if completed => "completed",
            BuildLoopOutcome::Finished => "cancelled",
        };
        let error = match &outcome {
            BuildLoopOutcome::Failed(error) => Some(error.as_str()),
            BuildLoopOutcome::Finished => None,
        };
        self.log_build_telemetry(
            context.server,
            context.generation,
            context.build_started,
            &state,
            outcome_label,
            error,
        );
        match outcome {
            BuildLoopOutcome::Failed(error) => {
                self.finish_failed_build(
                    context.server,
                    context.generation,
                    context.control,
                    context.ownership,
                    context.build_started,
                    error,
                );
            }
            BuildLoopOutcome::Finished if completed => {
                self.finish_completed_build(
                    context.server,
                    context.generation,
                    context.control,
                    context.ownership,
                    context.build_started,
                    state,
                );
            }
            BuildLoopOutcome::Finished => {
                self.finish_cancelled_build(
                    context.server,
                    context.generation,
                    context.control,
                    context.ownership,
                    context.build_started,
                    state.cancelled,
                );
            }
        }
    }

    pub(super) fn log_build_telemetry(
        &self,
        server: &str,
        generation: u64,
        build_started: Instant,
        state: &BuildRunState,
        outcome: &str,
        error: Option<&str>,
    ) {
        let telemetry = &state.telemetry;
        let counts = state.terminal_counts();
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            generation,
            outcome,
            error = ?error,
            duration_ms = build_started.elapsed().as_millis() as u64,
            terminal_event_ms = ?telemetry.terminal_event_ms,
            last_progress_entries_seen = counts.last_progress_entries_seen,
            last_progress_unique_items = counts.last_progress_unique_items,
            persisted_items = counts.persisted_items,
            drained_events = counts.drained_events,
            received_entry_events = counts.received_entry_events,
            pending_entries = counts.pending_entries,
            pending_unique_items = counts.pending_unique_items,
            active_time_ms = state.last_progress.active_time_ms,
            paused_time_ms = state.last_progress.paused_time_ms,
            progress_events = telemetry.progress_events,
            slice_count = telemetry.slice_count,
            slice_nodes_returned = telemetry.slice_nodes_returned,
            slice_native_operations = telemetry.slice_native_operations,
            slice_elapsed_ms = telemetry.slice_elapsed_ms,
            slice_elapsed_max_ms = telemetry.slice_elapsed_max_ms,
            slice_entries_delta = telemetry.slice_entries_delta,
            slice_entries_delta_max = telemetry.slice_entries_delta_max,
            slice_unique_items_delta = telemetry.slice_unique_items_delta,
            da2_slices = telemetry.da2_slices,
            da3_slices = telemetry.da3_slices,
            item_entries = telemetry.item_entries,
            branch_and_item_entries = telemetry.branch_and_item_entries,
            commit_attempts = telemetry.commit_attempts,
            commit_failures = telemetry.commit_failures,
            committed_entries = telemetry.committed_entries,
            commit_elapsed_ms = telemetry.commit_elapsed_ms,
            commit_elapsed_max_ms = telemetry.commit_elapsed_max_ms,
            commit_latency_p50_ms = ?telemetry.commit_latency_percentile(50),
            commit_latency_p95_ms = ?telemetry.commit_latency_percentile(95),
            "namespace index build telemetry"
        );
    }

    pub(super) fn finish_failed_build(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        build_started: Instant,
        error: String,
    ) {
        self.fail_generation_and_schedule_cleanup(server, generation, &error);
        tracing::error!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server = %server,
            generation,
            duration_ms = build_started.elapsed().as_millis() as u64,
            error = %error,
            "namespace index build failed"
        );
        self.finish_build_for_control_owned(server, control, ownership, Some(error));
    }

    pub(super) fn finish_completed_build(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        build_started: Instant,
        state: BuildRunState,
    ) {
        let result = self.promote_completed_build(
            server,
            generation,
            state.persisted_item_count,
            state.completion_profile,
            state.completion_warning.as_deref(),
        );
        match result {
            Ok(()) => self.finish_promoted_build(
                server,
                generation,
                control,
                ownership,
                build_started,
                state,
            ),
            Err(error) => {
                self.finish_promotion_failure(server, generation, control, ownership, error);
            }
        }
    }

    pub(super) fn promote_completed_build(
        &self,
        server: &str,
        generation: u64,
        persisted_item_count: u64,
        completion_profile: Option<(NamespaceOrganization, BrowseSource)>,
        completion_warning: Option<&str>,
    ) -> anyhow::Result<()> {
        let promotion_started = Instant::now();
        self.mark_promoting(server)?;
        let completed_at = timestamp_now();
        let result = self.with_database_write(|db| {
            db.promote_with_profile(
                server,
                generation,
                &completed_at,
                persisted_item_count,
                completion_profile,
                completion_warning,
            )
        });
        self.clear_promoting(server);
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            generation,
            promotion_duration_ms = promotion_started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "namespace index generation promotion finished"
        );
        result
    }

    pub(super) fn finish_promoted_build(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        build_started: Instant,
        state: BuildRunState,
    ) {
        self.schedule_cleanup(server);
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear_server(server);
        }
        let counts = state.terminal_counts();
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server = %server,
            generation,
            duration_ms = build_started.elapsed().as_millis() as u64,
            last_progress_entries_seen = counts.last_progress_entries_seen,
            last_progress_unique_items = counts.last_progress_unique_items,
            persisted_items = counts.persisted_items,
            drained_events = counts.drained_events,
            received_entry_events = counts.received_entry_events,
            pending_entries = counts.pending_entries,
            pending_unique_items = counts.pending_unique_items,
            committed_entries = state.telemetry.committed_entries,
            "namespace index build completed"
        );
        if let Some(warning) = state.completion_warning {
            tracing::warn!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server = %server,
                generation,
                warning = %warning,
                "namespace index completed with warning"
            );
        }
        self.finish_build_for_control_owned(server, control, ownership, None);
    }

    pub(super) fn finish_promotion_failure(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        error: anyhow::Error,
    ) {
        tracing::error!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server = %server,
            generation,
            operation = "promote",
            error = %error,
            "namespace index database operation failed"
        );
        self.fail_generation_and_schedule_cleanup(server, generation, &error.to_string());
        self.finish_build_for_control_owned(server, control, ownership, Some(error.to_string()));
    }

    pub(super) fn finish_cancelled_build(
        &self,
        server: &str,
        generation: u64,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        build_started: Instant,
        cancelled: bool,
    ) {
        self.abandon_generation(server, generation, "namespace index build cancelled");
        tracing::warn!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server = %server,
            generation,
            duration_ms = build_started.elapsed().as_millis() as u64,
            cancelled,
            "namespace index build cancelled"
        );
        self.finish_build_for_control_owned(server, control, ownership, None);
    }

    pub(super) fn commit_pending_entries(
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
        if let Ok(mut recorded_at) = self.commit_latency_recorded_at.lock() {
            recorded_at.insert(server.to_string(), Instant::now());
        }
        if result.is_ok() {
            pending.clear();
        }
        result
    }

    pub(super) fn commit_pending_entries_with_telemetry(
        &self,
        server: &str,
        generation: u64,
        state: &mut BuildRunState,
    ) -> anyhow::Result<u64> {
        let started = Instant::now();
        let result = self.commit_pending_entries(server, generation, &mut state.pending);
        let inserted = result.as_ref().ok().copied().unwrap_or(0);
        state
            .telemetry
            .record_commit(inserted, started.elapsed(), result.is_err());
        result
    }

    pub(super) async fn enforce_duty_cycle(
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

    pub(super) async fn wait_for_maintenance(
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

    pub(super) async fn wait_for_health(
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
            match self.health_probe_action(server, *next_probe, Instant::now()) {
                HealthProbeAction::Ready => return true,
                HealthProbeAction::Wait(delay) => {
                    if !wait_with_cancellation(control, delay).await {
                        self.set_pause_overlay(server, None, Some(false));
                        return false;
                    }
                    continue;
                }
                HealthProbeAction::Probe => {}
            }
            self.set_pause_overlay(server, None, Some(true));
            let observation = self.probe_health(server).await;
            self.update_health_state(server, Self::health_state(&observation));
            if observation.healthy {
                self.set_pause_overlay(server, None, Some(false));
                *backoff = Duration::from_secs(1);
                *next_probe = Instant::now()
                    + Duration::from_secs(self.settings.health_probe_interval_seconds.max(1));
                return true;
            }

            tracing::warn!(target: "opcda_bridge_gateway::index",
                server = %server,
                reason = %observation.failure_reason,
                "deferring namespace inventory"
            );
            let delay = (*backoff).min(Duration::from_secs(300));
            *backoff = next_health_backoff(*backoff);
            *next_probe = Instant::now() + delay;
            if !wait_with_cancellation(control, delay).await {
                self.set_pause_overlay(server, None, Some(false));
                return false;
            }
        }
    }

    pub(super) fn health_state(observation: &HealthProbeObservation) -> HealthProbeState {
        match (observation.sentinel_configured, observation.healthy) {
            (false, _) => HealthProbeState::Unavailable,
            (true, true) => HealthProbeState::Healthy,
            (true, false) => HealthProbeState::Unhealthy,
        }
    }

    pub(super) fn health_probe_action(
        &self,
        server: &str,
        next_probe: Instant,
        now: Instant,
    ) -> HealthProbeAction {
        let sentinel_due = self.sentinel_probe_due(server, now);
        if now >= next_probe || sentinel_due {
            return HealthProbeAction::Probe;
        }
        if self.health_overlay_active(server) {
            HealthProbeAction::Wait(next_probe.saturating_duration_since(now))
        } else {
            HealthProbeAction::Ready
        }
    }

    pub(super) fn sentinel_probe_due(&self, server: &str, now: Instant) -> bool {
        self.settings.sentinel_tag.is_some()
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
                })
    }

    pub(super) async fn probe_health(&self, server: &str) -> HealthProbeObservation {
        let started = Instant::now();
        let capability = self
            .with_opc_timeout(
                "health capability probe",
                self.client.get_capabilities(server),
            )
            .await;
        let elapsed = started.elapsed();
        let sentinel = self.read_health_sentinel(server).await;
        let sentinel_healthy = sentinel.as_ref().is_none_or(|value| value.healthy);
        let healthy = capability.is_ok()
            && elapsed <= Duration::from_millis(self.settings.health_latency_threshold_ms)
            && sentinel_healthy;
        let failure_reason = if let Err(error) = capability.as_ref() {
            format!("health probe failed: {error}")
        } else if let Some(sentinel) = sentinel.as_ref().filter(|value| !value.healthy) {
            sentinel
                .failure_reason
                .clone()
                .unwrap_or_else(|| "sentinel read was unhealthy".to_string())
        } else {
            format!(
                "health probe exceeded {} ms ({} ms)",
                self.settings.health_latency_threshold_ms,
                elapsed.as_millis()
            )
        };
        HealthProbeObservation {
            healthy,
            failure_reason,
            sentinel_configured: self.settings.sentinel_tag.is_some(),
        }
    }

    pub(super) async fn read_health_sentinel(
        &self,
        server: &str,
    ) -> Option<HealthSentinelObservation> {
        let tag = self.settings.sentinel_tag.as_deref()?;
        Some(
            match self
                .with_opc_timeout(
                    "health sentinel read",
                    self.client.read_tag_values(server, vec![tag.to_string()]),
                )
                .await
            {
                Ok(values) if values.len() == 1 => {
                    let healthy = values[0].quality.eq_ignore_ascii_case("good");
                    HealthSentinelObservation {
                        healthy,
                        failure_reason: (!healthy)
                            .then(|| "sentinel quality is not Good".to_string()),
                    }
                }
                Ok(_) => HealthSentinelObservation {
                    healthy: false,
                    failure_reason: Some("sentinel read returned no value".to_string()),
                },
                Err(error) => HealthSentinelObservation {
                    healthy: false,
                    failure_reason: Some(error.to_string()),
                },
            },
        )
    }

    pub(super) async fn wait_for_controller_recovery(
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
            Self::log_controller_transition(server, &decision);
        }
        Ok(true)
    }

    pub(super) fn log_controller_transition(
        server: &str,
        decision: &crate::controller::ControllerDecision,
    ) {
        if decision.transitioned {
            tracing::info!(target: "opcda_bridge_gateway::index",
                server,
                state = ?decision.state,
                reason = ?decision.reason,
                recovery_deadline = ?decision.recovery_at.map(instant_timestamp),
                "updated adaptive namespace inventory state while paused"
            );
        }
    }

    pub(super) fn health_overlay_active(&self, server: &str) -> bool {
        self.pause_overlays
            .lock()
            .ok()
            .and_then(|overlays| overlays.get(server).copied())
            .is_some_and(|overlay| overlay.health)
    }

    pub(super) fn update_health_state(&self, server: &str, health: HealthProbeState) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(state) = runtime.get_mut(server)
        {
            state.health = health;
            state.sentinel_checked_at = Some(Instant::now());
        }
    }

    pub(super) fn update_runtime_controller(
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

    pub(super) fn foreground_active(&self, server: &str) -> bool {
        self.foreground_users
            .lock()
            .ok()
            .and_then(|users| users.get(server).copied())
            .is_some_and(|count| count > 0)
    }

    pub(super) fn controller_observation(
        &self,
        server: &str,
        inventory_error: bool,
    ) -> ControllerObservation {
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
        let commit_latency_is_fresh = self
            .commit_latency_recorded_at
            .lock()
            .ok()
            .and_then(|recorded_at| recorded_at.get(server).copied())
            .is_some_and(|recorded_at| {
                now.saturating_duration_since(recorded_at)
                    <= Duration::from_secs(self.settings.adaptive_recovery_delay_seconds.max(1))
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
            database_commit_p95_ms: commit_latency_is_fresh
                .then_some(storage.last_commit_latency_ms)
                .flatten(),
            insufficient_disk_space: storage.free_bytes.is_some_and(|free| {
                free < self
                    .settings
                    .minimum_free_space_bytes
                    .saturating_add(self.settings.storage_headroom_bytes)
            }),
        }
    }

    pub(super) fn controller_observation_for_slice(
        &self,
        server: &str,
        slice: &InventorySliceObservation,
    ) -> ControllerObservation {
        self.controller_observation(server, slice.native_operations == 0)
    }

    pub(super) fn build_can_resume(build: &RuntimeBuild, overlays: PauseOverlayState) -> bool {
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

    pub(super) fn update_runtime_progress(&self, server: &str, progress: InventoryProgress) {
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(build) = runtime
                .get_mut(server)
                .and_then(|state| state.build.as_mut())
        {
            build.progress = Some(progress);
        }
    }

    #[cfg(test)]
    pub(super) fn finish_build(&self, server: &str, error: Option<String>) {
        let ownership = self
            .coordination
            .build_owners
            .lock()
            .ok()
            .and_then(|owners| owners.get(server).cloned());
        self.finish_build_inner(server, None, ownership.as_ref(), error);
    }

    #[cfg(test)]
    pub(super) fn finish_build_for_control(
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

    pub(super) fn finish_build_owned(
        &self,
        server: &str,
        ownership: &Arc<()>,
        error: Option<String>,
    ) {
        self.finish_build_inner(server, None, Some(ownership), error);
    }

    pub(super) fn finish_build_for_control_owned(
        &self,
        server: &str,
        control: &Arc<dyn InventoryControl>,
        ownership: &Arc<()>,
        error: Option<String>,
    ) {
        self.finish_build_inner(server, Some(control), Some(ownership), error);
    }

    pub(super) fn finish_build_inner(
        &self,
        server: &str,
        control: Option<&Arc<dyn InventoryControl>>,
        ownership: Option<&Arc<()>>,
        error: Option<String>,
    ) {
        let owns_build = match self.runtime.lock() {
            Ok(mut runtime) => {
                let Some(is_owner) =
                    self.build_completion_is_current(&runtime, server, control, ownership)
                else {
                    return;
                };
                if is_owner {
                    self.update_runtime_after_build(&mut runtime, server, error.as_deref());
                }
                is_owner
            }
            Err(_) => {
                tracing::error!(target: "opcda_bridge_gateway::index",
                    process_id = std::process::id(),
                    database = %self.settings.database_path.display(),
                    server,
                    "unable to finalize namespace index build because the runtime lock is poisoned"
                );
                return;
            }
        };
        if owns_build {
            self.finalize_owned_build(server, ownership);
        } else if control.is_some() {
            tracing::warn!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                "ignored completion from obsolete namespace index build"
            );
        }
    }

    pub(super) fn build_completion_is_current(
        &self,
        runtime: &HashMap<String, RuntimeState>,
        server: &str,
        control: Option<&Arc<dyn InventoryControl>>,
        ownership: Option<&Arc<()>>,
    ) -> Option<bool> {
        if let Some(ownership) = ownership {
            let token_matches = match self.coordination.build_owners.lock() {
                Ok(owners) => owners
                    .get(server)
                    .is_some_and(|current| Arc::ptr_eq(current, ownership)),
                Err(_) => {
                    tracing::error!(target: "opcda_bridge_gateway::index",
                        process_id = std::process::id(),
                        database = %self.settings.database_path.display(),
                        server,
                        "unable to finalize namespace index build because the ownership registry is poisoned"
                    );
                    return None;
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
            Some(token_matches && control_matches)
        } else {
            Some(runtime.get(server).is_some_and(|state| {
                match control {
                    Some(control) => state
                        .build
                        .as_ref()
                        .and_then(|build| build.control.as_ref())
                        .is_some_and(|current| Arc::ptr_eq(current, control)),
                    None => state.build.is_some(),
                }
            }))
        }
    }

    pub(super) fn update_runtime_after_build(
        &self,
        runtime: &mut HashMap<String, RuntimeState>,
        server: &str,
        error: Option<&str>,
    ) {
        let Some(state) = runtime.get_mut(server) else {
            return;
        };
        state.last_error = error.map(str::to_owned);
        let cancellation_deadline = state
            .build
            .as_ref()
            .and_then(|build| build.operator_cancelled_until);
        if error.is_some() {
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.circuit_open =
                state.consecutive_failures >= self.settings.circuit_failure_threshold;
            state.retry_after = Some(
                SystemTime::now()
                    + scheduler::retry_delay(
                        server,
                        state.consecutive_failures,
                        state.circuit_open,
                        self.settings.circuit_open_seconds,
                    ),
            );
            if let Some(deadline) = cancellation_deadline {
                state.retry_after = state.retry_after.map(|retry| retry.max(deadline));
            }
        } else if let Some(deadline) = cancellation_deadline {
            state.retry_after = Some(deadline);
        } else {
            state.retry_after = None;
            state.consecutive_failures = 0;
            state.circuit_open = false;
        }
    }

    pub(super) fn finalize_owned_build(&self, server: &str, ownership: Option<&Arc<()>>) {
        if let Err(error) = self.persist_retry_state(server) {
            tracing::error!(target: "opcda_bridge_gateway::index", server, error = %error,
                "unable to persist namespace index scheduling deadline");
            if let Ok(mut runtime) = self.runtime.lock()
                && let Some(state) = runtime.get_mut(server)
            {
                state.last_error = Some(format!(
                    "unable to persist index scheduling deadline: {error}"
                ));
            }
        }
        self.clear_pause_overlays(server);
        self.remove_build_owner(server, ownership);
        self.clear_active_build(server);
        self.clear_build_lock(server);
        if let Ok(mut runtime) = self.runtime.lock()
            && let Some(state) = runtime.get_mut(server)
        {
            let _ = state.build.take();
        }
        self.schedule_cleanup(server);
        self.clear_pending_cancel(server);
    }

    pub(super) fn remove_build_owner(&self, server: &str, ownership: Option<&Arc<()>>) {
        if let Ok(mut owners) = self.coordination.build_owners.lock()
            && ownership.is_none_or(|ownership| {
                owners
                    .get(server)
                    .is_some_and(|current| Arc::ptr_eq(current, ownership))
            })
        {
            owners.remove(server);
        }
    }

    pub(super) fn clear_build_lock(&self, server: &str) {
        if let Ok(mut build_locks) = self.build_locks.lock() {
            build_locks.remove(server);
        }
    }

    pub(super) fn clear_active_build(&self, server: &str) {
        if let Ok(mut active) = self.active_builds.lock() {
            active.remove(server);
            self.build_changed.notify_waiters();
        }
    }

    pub(super) fn fail_generation_and_schedule_cleanup(
        &self,
        server: &str,
        generation: u64,
        error: &str,
    ) {
        match self.with_database_write(|db| db.fail_generation(server, generation, error)) {
            Ok(()) => self.schedule_cleanup(server),
            Err(database_error) => tracing::error!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                generation,
                error = %database_error,
                "unable to mark failed namespace index generation for cleanup"
            ),
        }
    }

    pub(super) fn abandon_generation(&self, server: &str, generation: u64, reason: &str) {
        match self.with_database_write(|db| db.discard_empty_generation(server, generation)) {
            Ok(true) => {}
            Ok(false) => self.fail_generation_and_schedule_cleanup(server, generation, reason),
            Err(error) => tracing::error!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                server,
                generation,
                error = %error,
                "unable to abandon namespace index generation"
            ),
        }
    }
}

pub(super) fn pacing_for_limits(limits: InventoryLimits) -> InventoryPacing {
    // The native item-rate limiter charges each operation by its item cost.
    // Keep it independent from the batch size instead of adding a second,
    // batch-derived minimum interval.
    InventoryPacing {
        min_interval: Duration::ZERO,
        item_rate_per_second: (limits.item_rate_per_second > 0)
            .then_some(limits.item_rate_per_second),
        batch_size: Some(limits.batch_size.clamp(1, MAX_NATIVE_INVENTORY_BATCH_SIZE)),
    }
}

pub(super) fn zero_inventory_progress() -> InventoryProgress {
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

pub(super) fn accumulate_inventory_progress(
    cumulative: &mut InventoryProgress,
    previous: Option<&InventoryProgress>,
    progress: &InventoryProgress,
) {
    let zero = zero_inventory_progress();
    let previous = previous.unwrap_or(&zero);
    cumulative.branches_visited = cumulative.branches_visited.saturating_add(
        progress
            .branches_visited
            .saturating_sub(previous.branches_visited),
    );
    cumulative.entries_seen = cumulative
        .entries_seen
        .saturating_add(progress.entries_seen.saturating_sub(previous.entries_seen));
    cumulative.unique_items = cumulative
        .unique_items
        .saturating_add(progress.unique_items.saturating_sub(previous.unique_items));
    cumulative.active_time_ms = cumulative.active_time_ms.saturating_add(
        progress
            .active_time_ms
            .saturating_sub(previous.active_time_ms),
    );
    cumulative.paused_time_ms = cumulative.paused_time_ms.saturating_add(
        progress
            .paused_time_ms
            .saturating_sub(previous.paused_time_ms),
    );
    cumulative.items_per_second = if cumulative.active_time_ms == 0 {
        0.0
    } else {
        cumulative.unique_items as f64
            / Duration::from_millis(cumulative.active_time_ms).as_secs_f64()
    };
    cumulative.estimated_remaining_ms = progress.estimated_remaining_ms;
}

pub(super) fn aggregate_inventory_progress(
    progress_by_worker: &HashMap<usize, InventoryProgress>,
    unique_items: u64,
) -> InventoryProgress {
    let branches_visited = progress_by_worker
        .values()
        .map(|progress| progress.branches_visited)
        .sum();
    let entries_seen = progress_by_worker
        .values()
        .map(|progress| progress.entries_seen)
        .sum();
    let active_time_ms = progress_by_worker
        .values()
        .map(|progress| progress.active_time_ms)
        .sum();
    let paused_time_ms = progress_by_worker
        .values()
        .map(|progress| progress.paused_time_ms)
        .sum();
    let estimated_remaining_ms = progress_by_worker
        .values()
        .filter_map(|progress| progress.estimated_remaining_ms)
        .max();
    let items_per_second = if active_time_ms == 0 {
        0.0
    } else {
        unique_items as f64 / Duration::from_millis(active_time_ms).as_secs_f64()
    };
    InventoryProgress {
        branches_visited,
        entries_seen,
        unique_items,
        active_time_ms,
        paused_time_ms,
        items_per_second,
        estimated_remaining_ms,
    }
}

pub(super) fn next_health_backoff(backoff: Duration) -> Duration {
    backoff
        .checked_mul(2)
        .unwrap_or(Duration::from_secs(300))
        .min(Duration::from_secs(300))
}
