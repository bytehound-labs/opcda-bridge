//! Shared `OpcClient` test double, used by both the `server` and `run`
//! unit test modules so the gRPC-facing logic can be exercised without the
//! Windows-only COM adapter.

use crate::opc::{OpcClient, OpcValue, TagValue, WriteResult};
use std::sync::atomic::AtomicUsize;
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
    pub(crate) browse_tags_result: Mutex<Result<Vec<String>, String>>,
    pub(crate) read_tag_values_result: Mutex<Result<Vec<TagValue>, String>>,
    pub(crate) write_tag_value_result: Mutex<Result<WriteResult, String>>,
}

impl Default for MockOpcClient {
    fn default() -> Self {
        Self {
            list_servers_result: Mutex::new(Ok(vec![])),
            list_servers_delay: Mutex::new(None),
            list_servers_started: Arc::new(Notify::new()),
            browse_tags_result: Mutex::new(Ok(vec![])),
            read_tag_values_result: Mutex::new(Ok(vec![])),
            write_tag_value_result: Mutex::new(Ok(WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            })),
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

    async fn browse_tags(
        &self,
        _server: &str,
        _max_tags: usize,
        _progress: Arc<AtomicUsize>,
        _tags_sink: Arc<Mutex<Vec<String>>>,
    ) -> anyhow::Result<Vec<String>> {
        self.browse_tags_result
            .lock()
            .unwrap()
            .clone()
            .map_err(|e| anyhow::anyhow!("{e}"))
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

#[test]
fn test_mock_opc_client_default() {
    let mock = MockOpcClient::default();
    let result = mock.list_servers_result.lock().unwrap();
    assert!(result.is_ok());
}
