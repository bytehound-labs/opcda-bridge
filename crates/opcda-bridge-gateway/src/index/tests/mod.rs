use super::*;

use super::{query::*, scheduler::*, status::*, store::*, traversal::*};

use crate::controller::{
    AdaptiveIndexController, ControllerObservation, HostMetrics, InventoryLimits,
};

use crate::opc::{
    BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, BrowseSource, InventoryCompleted,
    InventoryControl, InventoryEntry, InventoryEvent, InventoryHandle, InventoryNodeKind,
    InventoryPacing, InventoryProgress, InventorySliceBackend, InventorySliceObservation,
    InventoryStream, MAX_NATIVE_INVENTORY_BATCH_SIZE, NamespaceOrganization, OpcClient, OpcValue,
    TagValue, WriteResult,
};

use crate::test_support::MockOpcClient;

use chrono::{Local, TimeZone, Timelike};

use proptest::prelude::*;

use rusqlite::{Connection, params};

use std::collections::{HashMap, VecDeque};

use std::error::Error;

use std::fs;

use std::path::{Path, PathBuf};

use std::sync::Arc;

use std::sync::Mutex;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tempfile::tempdir;

use tokio::sync::Notify;

fn settings(path: PathBuf) -> ResolvedIndexConfig {
    ResolvedIndexConfig {
        database_path: path,
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
        .map(|index| inventory_entry(&format!("{prefix}-{index}"), &format!("{prefix}.{index}")))
        .collect()
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
                operator_cancelled_until: None,
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
    auto_refresh_enabled: bool,
) {
    manager
        .with_database(|db| {
            let generation = db.start_generation("S", organization, source, &timestamp_now())?;
            db.set_auto_refresh("S", auto_refresh_enabled)?;
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

#[derive(Clone)]
struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogWriter {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

mod lifecycle;
mod query;
mod scheduler;
mod status;
mod store;
mod traversal;
