//! Shared `OpcClient` test double, used by both the `server` and `run`
//! unit test modules so the gRPC-facing logic can be exercised without the
//! Windows-only COM adapter.

use crate::opc::{
    BrowseCapabilities, BrowsePage, BrowseSource, InventoryCompleted, InventoryControl,
    InventoryEntry, InventoryEvent, InventoryHandle, InventoryNodeKind, InventoryProgress,
    InventoryStream, NamespaceOrganization, OpcClient, OpcValue, TagValue, WriteResult,
};
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// A configurable mock `OpcClient`.
///
/// Each field controls the result returned by its corresponding trait
/// method; `Default` produces a client where every call succeeds with an
/// empty/neutral result. `list_servers_delay` optionally sleeps before
/// returning, and `list_servers_started` is notified the moment the call
/// begins — together they let tests put a `list_servers` call reliably "in
/// flight" without relying on wall-clock races (e.g. to exercise
/// graceful-shutdown draining).
pub(crate) struct MockOpcClient {
    pub(crate) list_servers_result: Mutex<Result<Vec<String>, String>>,
    pub(crate) list_servers_delay: Mutex<Option<Duration>>,
    pub(crate) list_servers_started: Arc<Notify>,
    pub(crate) capabilities_result: Mutex<Result<BrowseCapabilities, String>>,
    pub(crate) open_browse_session_result: Mutex<Result<String, String>>,
    pub(crate) browse_page_result: Mutex<Result<BrowsePage, String>>,
    pub(crate) browse_page_results: Mutex<VecDeque<Result<BrowsePage, String>>>,
    pub(crate) close_browse_session_result: Mutex<Result<(), String>>,
    pub(crate) read_tag_values_result: Mutex<Result<Vec<TagValue>, String>>,
    pub(crate) write_tag_value_result: Mutex<Result<WriteResult, String>>,
    pub(crate) inventory_events: Mutex<VecDeque<Result<InventoryEvent, String>>>,
    pub(crate) inventory_start_count: Arc<AtomicUsize>,
    pub(crate) inventory_paused: Arc<AtomicBool>,
    pub(crate) inventory_cancelled: Arc<AtomicBool>,
}

impl Default for MockOpcClient {
    fn default() -> Self {
        Self {
            list_servers_result: Mutex::new(Ok(vec![])),
            list_servers_delay: Mutex::new(None),
            list_servers_started: Arc::new(Notify::new()),
            capabilities_result: Mutex::new(Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 1000,
            })),
            open_browse_session_result: Mutex::new(Ok("native-session".into())),
            browse_page_result: Mutex::new(Ok(BrowsePage {
                nodes: vec![],
                next_page_token: None,
                complete: true,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            })),
            browse_page_results: Mutex::new(VecDeque::new()),
            close_browse_session_result: Mutex::new(Ok(())),
            read_tag_values_result: Mutex::new(Ok(vec![])),
            write_tag_value_result: Mutex::new(Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            })),
            inventory_events: Mutex::new(VecDeque::from([
                Ok(InventoryEvent::Entry(InventoryEntry {
                    display_name: "Mock tag".into(),
                    item_id: "Mock.Tag".into(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec!["Mock".into()],
                })),
                Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 1,
                    unique_items: 1,
                    active_time_ms: 1,
                    paused_time_ms: 0,
                    items_per_second: 1000.0,
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
            ])),
            inventory_start_count: Arc::new(AtomicUsize::new(0)),
            inventory_paused: Arc::new(AtomicBool::new(false)),
            inventory_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait::async_trait]
impl OpcClient for MockOpcClient {
    async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
        self.list_servers_started.notify_one();
        let delay = *self.list_servers_delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.list_servers_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn get_capabilities(&self, _server: &str) -> anyhow::Result<BrowseCapabilities> {
        self.capabilities_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
        self.open_browse_session_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn browse_page(
        &self,
        _session_id: &str,
        _parent_node_key: Option<&str>,
        _page_token: Option<&str>,
        _page_size: u32,
        _refresh: bool,
    ) -> anyhow::Result<BrowsePage> {
        if let Some(result) = self.browse_page_results.lock().unwrap().pop_front() {
            return result.map_err(|e| anyhow::anyhow!("{e}"));
        }
        self.browse_page_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
        self.close_browse_session_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn start_inventory(
        &self,
        _server: &str,
        _batch_size: u32,
    ) -> anyhow::Result<InventoryHandle> {
        self.inventory_start_count.fetch_add(1, Ordering::Relaxed);
        let events = self
            .inventory_events
            .lock()
            .unwrap()
            .drain(..)
            .map(|event| event.map_err(|error| anyhow::anyhow!("{error}")))
            .collect();
        Ok(InventoryHandle {
            stream: Box::new(MockInventoryStream { events }),
            control: Arc::new(MockInventoryControl {
                paused: Arc::clone(&self.inventory_paused),
                cancelled: Arc::clone(&self.inventory_cancelled),
            }),
        })
    }

    async fn read_tag_values(
        &self,
        _server: &str,
        _tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<TagValue>> {
        self.read_tag_values_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn write_tag_value(
        &self,
        _server: &str,
        _tag_id: &str,
        _value: OpcValue,
    ) -> anyhow::Result<WriteResult> {
        self.write_tag_value_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

struct MockInventoryControl {
    paused: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl InventoryControl for MockInventoryControl {
    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct MockInventoryStream {
    events: VecDeque<anyhow::Result<InventoryEvent>>,
}

#[async_trait::async_trait]
impl InventoryStream for MockInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        self.events.pop_front()
    }
}

#[test]
fn test_mock_opc_client_default() {
    let mock = MockOpcClient::default();
    let result = mock.list_servers_result.lock().unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_mock_inventory_stream_and_control() {
    let mock = MockOpcClient::default();
    let mut handle = mock.start_inventory("S", 10).await.unwrap();

    handle.control.pause();
    assert!(mock.inventory_paused.load(Ordering::Acquire));
    handle.control.resume();
    assert!(!mock.inventory_paused.load(Ordering::Acquire));
    handle.control.cancel();
    assert!(handle.control.is_cancelled());
    assert!(handle.stream.next().await.is_some());
}
