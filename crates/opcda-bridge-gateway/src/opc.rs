use std::sync::Arc;
use std::time::Duration;

/// Maximum number of entries requested from one native inventory operation.
///
/// On Windows this is sourced from the upstream OPC DA client contract. The
/// non-Windows value keeps configuration and controller tests aligned with
/// that contract without requiring the Windows-only dependency.
#[cfg(target_os = "windows")]
pub const MAX_NATIVE_INVENTORY_BATCH_SIZE: u32 = opc_da_client::MAX_INVENTORY_BATCH_SIZE;

#[cfg(not(target_os = "windows"))]
pub const MAX_NATIVE_INVENTORY_BATCH_SIZE: u32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceOrganization {
    Unspecified,
    Flat,
    Hierarchical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseSource {
    Unspecified,
    Da3,
    Da2,
    Flat,
    Derived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseNodeKind {
    Branch,
    Item,
    BranchAndItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseCapabilities {
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub supports_browse_sessions: bool,
    pub supports_search: bool,
    pub max_page_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseNode {
    pub node_key: String,
    pub display_name: String,
    pub kind: BrowseNodeKind,
    pub item_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowsePage {
    pub nodes: Vec<BrowseNode>,
    pub next_page_token: Option<String>,
    pub complete: bool,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryNodeKind {
    Item,
    BranchAndItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryEntry {
    pub display_name: String,
    pub item_id: String,
    pub kind: InventoryNodeKind,
    pub breadcrumbs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InventoryProgress {
    pub branches_visited: u64,
    pub entries_seen: u64,
    pub unique_items: u64,
    pub active_time_ms: u64,
    pub paused_time_ms: u64,
    pub items_per_second: f64,
    pub estimated_remaining_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventorySliceBackend {
    Da3,
    Da2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventorySliceObservation {
    pub sequence: u64,
    pub backend: InventorySliceBackend,
    pub nodes_returned: u64,
    pub has_more: bool,
    pub native_operations: u64,
    pub elapsed_ms: u64,
    pub entries_seen: u64,
    pub unique_items: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryCompleted {
    pub complete: bool,
    pub cancelled: bool,
    pub truncated: bool,
    pub warning: Option<String>,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InventoryEvent {
    Entry(InventoryEntry),
    Progress(InventoryProgress),
    Slice(InventorySliceObservation),
    Completed(InventoryCompleted),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryPacing {
    pub min_interval: Duration,
    /// Maximum number of inventory items requested per second at the native
    /// COM boundary.
    pub item_rate_per_second: Option<u32>,
    /// Native entries requested for the next inventory slice.
    pub batch_size: Option<u32>,
}

impl Default for InventoryPacing {
    fn default() -> Self {
        Self {
            min_interval: Duration::ZERO,
            item_rate_per_second: None,
            batch_size: None,
        }
    }
}

#[async_trait::async_trait]
pub trait InventoryStream: Send {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>>;

    /// Wait for internal worker tasks to stop after cancellation.
    ///
    /// Ordinary streams do not own additional tasks and therefore use the
    /// default no-op implementation. Coordinated streams override this so
    /// build ownership is not released before every worker has joined.
    async fn shutdown(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait InventoryControl: Send + Sync {
    fn pause(&self);
    fn resume(&self);
    fn cancel(&self);
    /// Applies limits to subsequent native inventory calls.
    ///
    /// A failure is terminal for the active build because continuing with stale
    /// pacing would violate the coordinator's load-control decision.
    fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
        Ok(())
    }
    fn is_cancelled(&self) -> bool {
        false
    }
}

pub struct InventoryHandle {
    pub stream: Box<dyn InventoryStream>,
    pub control: Arc<dyn InventoryControl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
    /// The exact semantic value returned by OPC DA.
    ///
    /// For a `VT_BSTR`, quote characters are preserved exactly as returned by
    /// the server; the gateway does not add display framing.
    pub value: String,
    pub quality: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    pub tag_id: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpcValue {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

#[async_trait::async_trait]
pub trait OpcClient: Send + Sync + 'static {
    async fn list_servers(&self, host: &str) -> anyhow::Result<Vec<String>>;
    async fn get_capabilities(&self, server: &str) -> anyhow::Result<BrowseCapabilities>;
    async fn open_browse_session(&self, server: &str) -> anyhow::Result<String>;
    async fn browse_page(
        &self,
        session_id: &str,
        parent_node_key: Option<&str>,
        page_token: Option<&str>,
        page_size: u32,
        refresh: bool,
    ) -> anyhow::Result<BrowsePage>;
    async fn close_browse_session(&self, session_id: &str) -> anyhow::Result<()>;
    async fn start_inventory(
        &self,
        server: &str,
        batch_size: u32,
    ) -> anyhow::Result<InventoryHandle>;
    /// Start an inventory rooted at one exact canonical ItemID.
    ///
    /// Providers that cannot isolate a subtree use the default error. The
    /// coordinator uses that error only while attempting automatic root
    /// partitioning; an explicitly configured `inventory_root` treats a
    /// provider failure as fatal so the requested scope is never widened.
    async fn start_inventory_at_root(
        &self,
        server: &str,
        root_item_id: &str,
        batch_size: u32,
    ) -> anyhow::Result<InventoryHandle> {
        let _ = (server, root_item_id, batch_size);
        anyhow::bail!("root-scoped namespace inventory is not supported")
    }
    async fn read_tag_values(
        &self,
        server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<TagValue>>;
    async fn write_tag_value(
        &self,
        server: &str,
        tag_id: &str,
        value: OpcValue,
    ) -> anyhow::Result<WriteResult>;
}

pub type SharedOpcClient<C> = Arc<C>;

#[cfg(test)]
mod tests {
    use super::*;

    struct DefaultRootClient;

    #[async_trait::async_trait]
    impl OpcClient for DefaultRootClient {
        async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
            anyhow::bail!("unused")
        }

        async fn get_capabilities(&self, _server: &str) -> anyhow::Result<BrowseCapabilities> {
            anyhow::bail!("unused")
        }

        async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
            anyhow::bail!("unused")
        }

        async fn browse_page(
            &self,
            _session_id: &str,
            _parent_node_key: Option<&str>,
            _page_token: Option<&str>,
            _page_size: u32,
            _refresh: bool,
        ) -> anyhow::Result<BrowsePage> {
            anyhow::bail!("unused")
        }

        async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }

        async fn start_inventory(
            &self,
            _server: &str,
            _batch_size: u32,
        ) -> anyhow::Result<InventoryHandle> {
            anyhow::bail!("unused")
        }

        async fn read_tag_values(
            &self,
            _server: &str,
            _tag_ids: Vec<String>,
        ) -> anyhow::Result<Vec<TagValue>> {
            anyhow::bail!("unused")
        }

        async fn write_tag_value(
            &self,
            _server: &str,
            _tag_id: &str,
            _value: OpcValue,
        ) -> anyhow::Result<WriteResult> {
            anyhow::bail!("unused")
        }
    }

    struct DefaultInventoryControl;

    impl InventoryControl for DefaultInventoryControl {
        fn pause(&self) {
            std::hint::black_box(());
        }

        fn resume(&self) {
            std::hint::black_box(());
        }

        fn cancel(&self) {
            std::hint::black_box(());
        }
    }

    #[test]
    fn inventory_control_is_not_cancelled_by_default() {
        let control = DefaultInventoryControl;
        control.pause();
        control.resume();
        control.cancel();
        assert!(!control.is_cancelled());
        control.set_pacing(InventoryPacing::default()).unwrap();
    }

    #[tokio::test]
    async fn opc_client_default_root_inventory_is_unsupported() {
        let client = DefaultRootClient;
        assert!(client.list_servers("host").await.is_err());
        assert!(client.get_capabilities("server").await.is_err());
        assert!(client.open_browse_session("server").await.is_err());
        assert!(
            client
                .browse_page("session", None, None, 10, false)
                .await
                .is_err()
        );
        assert!(client.close_browse_session("session").await.is_err());
        assert!(client.start_inventory("server", 10).await.is_err());
        assert!(
            client
                .start_inventory_at_root("server", "Area.Loop", 10)
                .await
                .is_err()
        );
        assert!(
            client
                .read_tag_values("server", vec!["tag".into()])
                .await
                .is_err()
        );
        assert!(
            client
                .write_tag_value("server", "tag", OpcValue::Float(1.0))
                .await
                .is_err()
        );
        let result = client
            .start_inventory_at_root("server", "Area.Loop", 10)
            .await;
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("not supported"))
        );
    }
}
