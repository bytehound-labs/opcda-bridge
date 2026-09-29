use super::index::SearchIndexState;
use std::fmt;

/// Default number of children requested for one browse page.
pub const DEFAULT_PAGE_SIZE: u32 = 200;
/// How the OPC server organizes its namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceOrganization {
    Unspecified,
    Flat,
    Hierarchical,
}

impl fmt::Display for NamespaceOrganization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Flat => "flat",
            Self::Hierarchical => "hierarchical",
        })
    }
}

/// Native or configured strategy that produced browse results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseSource {
    Unspecified,
    Da3,
    Da2,
    Flat,
    Derived,
}

impl fmt::Display for BrowseSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Da3 => "da3",
            Self::Da2 => "da2",
            Self::Flat => "flat",
            Self::Derived => "derived",
        })
    }
}

/// Whether a browse node is expandable, selectable as an OPC item, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseNodeKind {
    Unspecified,
    Branch,
    Item,
    BranchAndItem,
}

impl BrowseNodeKind {
    /// Whether this node can be expanded with another browse request.
    pub fn is_branch(self) -> bool {
        matches!(self, Self::Branch | Self::BranchAndItem)
    }

    /// Whether this node identifies an OPC item that can be read or written.
    pub fn is_item(self) -> bool {
        matches!(self, Self::Item | Self::BranchAndItem)
    }
}

impl fmt::Display for BrowseNodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::Branch => "branch",
            Self::Item => "item",
            Self::BranchAndItem => "branch-and-item",
        })
    }
}
/// Gateway and namespace features reported for one OPC server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capabilities {
    pub application_version: String,
    pub protocol_version: String,
    pub max_page_size: u32,
    pub supports_browse_sessions: bool,
    pub supports_search: bool,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub supports_indexed_search: bool,
    pub indexed_search_protocol_version: String,
    pub max_indexed_search_results: u32,
    pub search_index_state: SearchIndexState,
    pub search_index_promoting: bool,
}

/// One child returned by a browse page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseNode {
    /// Opaque navigation identity. Round-trip it unchanged when expanding.
    pub node_key: String,
    /// One local label suitable for display.
    pub display_name: String,
    pub kind: BrowseNodeKind,
    /// Exact OPC DA ItemID, present only for selectable nodes.
    pub item_id: Option<String>,
}

/// One bounded page of immediate children and its continuation metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowsePage {
    pub session_id: String,
    pub nodes: Vec<BrowseNode>,
    pub next_page_token: Option<String>,
    pub complete: bool,
    pub organization: NamespaceOrganization,
    pub source: BrowseSource,
    pub warning: Option<String>,
}

/// Parameters for one browse-page request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowsePageRequest {
    pub server: String,
    pub session_id: Option<String>,
    pub parent_node_key: Option<String>,
    pub page_token: Option<String>,
    pub page_size: u32,
    pub refresh: bool,
}

impl BrowsePageRequest {
    /// Open a new browse session and request its root page.
    pub fn root(server: impl Into<String>, page_size: u32) -> Self {
        Self {
            server: server.into(),
            session_id: None,
            parent_node_key: None,
            page_token: None,
            page_size,
            refresh: false,
        }
    }

    /// Request the first page beneath an already-discovered branch.
    pub fn children(
        server: impl Into<String>,
        session_id: impl Into<String>,
        parent_node_key: impl Into<String>,
        page_size: u32,
    ) -> Self {
        Self {
            server: server.into(),
            session_id: Some(session_id.into()),
            parent_node_key: Some(parent_node_key.into()),
            page_token: None,
            page_size,
            refresh: false,
        }
    }

    /// Request the next page for a root or child browse.
    pub fn next(
        server: impl Into<String>,
        session_id: impl Into<String>,
        parent_node_key: Option<String>,
        page_token: impl Into<String>,
        page_size: u32,
    ) -> Self {
        Self {
            server: server.into(),
            session_id: Some(session_id.into()),
            parent_node_key,
            page_token: Some(page_token.into()),
            page_size,
            refresh: false,
        }
    }

    /// Ask the gateway to bypass cached namespace metadata.
    pub fn with_refresh(mut self, refresh: bool) -> Self {
        self.refresh = refresh;
        self
    }
}
/// One navigation step associated with a search match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseBreadcrumb {
    pub node_key: String,
    pub display_name: String,
}
