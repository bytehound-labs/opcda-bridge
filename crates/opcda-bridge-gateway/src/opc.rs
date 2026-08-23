use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceOrganization {
    Unspecified,
    Flat,
    Hierarchical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseSource {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
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
