//! Plain data types returned by [`crate::Client`]'s methods.

use crate::{Error, Result};
use opcda_bridge_proto::bridge as proto;
use std::fmt;

/// Default number of children requested for one browse page.
pub const DEFAULT_PAGE_SIZE: u32 = 200;
/// Default maximum number of matches requested by a search.
pub const DEFAULT_SEARCH_MAX_RESULTS: u32 = 200;
/// Default maximum number of matches requested from the persistent index.
pub const DEFAULT_INDEX_SEARCH_MAX_RESULTS: u32 = 50;

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

/// Match behavior for namespace search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMatchMode {
    Exact,
    Prefix,
    Contains,
}

impl fmt::Display for SearchMatchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Exact => "exact",
            Self::Prefix => "prefix",
            Self::Contains => "contains",
        })
    }
}

/// Readiness of a gateway-owned persistent namespace index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexState {
    Unspecified,
    NotIndexed,
    Partial,
    Ready,
    Stale,
    Refreshing,
    Failed,
}

impl fmt::Display for SearchIndexState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unspecified => "unspecified",
            Self::NotIndexed => "not-indexed",
            Self::Partial => "partial",
            Self::Ready => "ready",
            Self::Stale => "stale",
            Self::Refreshing => "refreshing",
            Self::Failed => "failed",
        })
    }
}

/// Operator action applied to an active namespace-index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexControlAction {
    Pause,
    Resume,
    Cancel,
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

/// Parameters for a bounded namespace search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub server: String,
    pub query: String,
    pub match_mode: SearchMatchMode,
    pub session_id: Option<String>,
    pub scope_node_key: Option<String>,
    pub max_results: u32,
    pub include_branches: bool,
    pub refresh: bool,
}

impl SearchRequest {
    pub fn new(
        server: impl Into<String>,
        query: impl Into<String>,
        match_mode: SearchMatchMode,
    ) -> Self {
        Self {
            server: server.into(),
            query: query.into(),
            match_mode,
            session_id: None,
            scope_node_key: None,
            max_results: DEFAULT_SEARCH_MAX_RESULTS,
            include_branches: false,
            refresh: false,
        }
    }
}

/// Parameters for one persistent-index query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexRequest {
    pub server: String,
    pub query: String,
    pub match_mode: SearchMatchMode,
    pub max_results: u32,
}

impl SearchIndexRequest {
    pub fn new(
        server: impl Into<String>,
        query: impl Into<String>,
        match_mode: SearchMatchMode,
    ) -> Self {
        Self {
            server: server.into(),
            query: query.into(),
            match_mode,
            max_results: DEFAULT_INDEX_SEARCH_MAX_RESULTS,
        }
    }
}

/// Progress reported for a running persistent namespace inventory.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedSearchProgress {
    pub branches_visited: u64,
    pub entries_seen: u64,
    pub unique_items: u64,
    pub active_time_ms: u64,
    pub paused_time_ms: u64,
    pub items_per_second: f64,
    pub estimated_remaining_ms: Option<u64>,
}

/// Persistent namespace-index state and build metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexStatus {
    pub server: String,
    pub state: SearchIndexState,
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
    pub progress: Option<IndexedSearchProgress>,
}

/// One selectable result from the persistent namespace index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSearchMatch {
    pub item_id: String,
    pub display_name: String,
    pub kind: BrowseNodeKind,
    pub breadcrumbs: Vec<String>,
}

/// Ranked persistent-index matches plus snapshot readiness metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexResponse {
    pub matches: Vec<IndexedSearchMatch>,
    pub has_more: bool,
    pub status: SearchIndexStatus,
}

/// One navigation step associated with a search match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseBreadcrumb {
    pub node_key: String,
    pub display_name: String,
}

/// A progressively emitted namespace-search result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMatch {
    pub node: BrowseNode,
    pub breadcrumbs: Vec<BrowseBreadcrumb>,
}

/// Progress emitted while a namespace search is still running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProgress {
    pub visited_nodes: u32,
    pub matches: u32,
    pub partial: bool,
}

/// Terminal search metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCompleted {
    pub complete: bool,
    pub cancelled: bool,
    pub truncated: bool,
    pub warning: Option<String>,
}

/// One event from the gateway's search stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchEvent {
    Match(SearchMatch),
    Progress(SearchProgress),
    Completed(SearchCompleted),
}

/// A single tag's semantic value returned by [`crate::Client::read`].
///
/// For an OPC DA `VT_BSTR`, `value` contains the exact BSTR contents. The
/// bridge does not add or remove quote characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
    pub value: String,
    pub quality: String,
    pub timestamp: String,
}

/// The result of a single [`crate::Client::write`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    pub tag_id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// A tag value to write, parsed from a raw string via [`parse_value`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

/// Parse a raw string into bool, integer, float, or string form.
pub fn parse_value(raw: &str) -> Value {
    if let Ok(b) = raw.parse::<bool>() {
        return Value::Bool(b);
    }
    if let Ok(i) = raw.parse::<i32>() {
        return Value::Int(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return Value::Float(f);
    }
    Value::String(raw.to_string())
}

fn invalid_enum(field: &str, value: i32) -> Error {
    Error::Protocol(format!("gateway returned unknown {field} value {value}"))
}

fn organization(value: i32) -> Result<NamespaceOrganization> {
    match proto::NamespaceOrganization::try_from(value)
        .map_err(|_| invalid_enum("namespace organization", value))?
    {
        proto::NamespaceOrganization::Unspecified => Ok(NamespaceOrganization::Unspecified),
        proto::NamespaceOrganization::Flat => Ok(NamespaceOrganization::Flat),
        proto::NamespaceOrganization::Hierarchical => Ok(NamespaceOrganization::Hierarchical),
    }
}

fn source(value: i32) -> Result<BrowseSource> {
    match proto::BrowseSource::try_from(value).map_err(|_| invalid_enum("browse source", value))? {
        proto::BrowseSource::Unspecified => Ok(BrowseSource::Unspecified),
        proto::BrowseSource::Da3 => Ok(BrowseSource::Da3),
        proto::BrowseSource::Da2 => Ok(BrowseSource::Da2),
        proto::BrowseSource::Flat => Ok(BrowseSource::Flat),
        proto::BrowseSource::Derived => Ok(BrowseSource::Derived),
    }
}

fn node_kind(value: i32) -> Result<BrowseNodeKind> {
    match proto::BrowseNodeKind::try_from(value)
        .map_err(|_| invalid_enum("browse node kind", value))?
    {
        proto::BrowseNodeKind::Unspecified => Ok(BrowseNodeKind::Unspecified),
        proto::BrowseNodeKind::Branch => Ok(BrowseNodeKind::Branch),
        proto::BrowseNodeKind::Item => Ok(BrowseNodeKind::Item),
        proto::BrowseNodeKind::BranchAndItem => Ok(BrowseNodeKind::BranchAndItem),
    }
}

fn search_index_state(value: i32) -> Result<SearchIndexState> {
    match proto::SearchIndexState::try_from(value)
        .map_err(|_| invalid_enum("search index state", value))?
    {
        proto::SearchIndexState::Unspecified => Ok(SearchIndexState::Unspecified),
        proto::SearchIndexState::NotIndexed => Ok(SearchIndexState::NotIndexed),
        proto::SearchIndexState::Partial => Ok(SearchIndexState::Partial),
        proto::SearchIndexState::Ready => Ok(SearchIndexState::Ready),
        proto::SearchIndexState::Stale => Ok(SearchIndexState::Stale),
        proto::SearchIndexState::Refreshing => Ok(SearchIndexState::Refreshing),
        proto::SearchIndexState::Failed => Ok(SearchIndexState::Failed),
    }
}

impl TryFrom<proto::GetCapabilitiesResponse> for Capabilities {
    type Error = Error;

    fn try_from(value: proto::GetCapabilitiesResponse) -> Result<Self> {
        Ok(Self {
            application_version: value.application_version,
            protocol_version: value.protocol_version,
            max_page_size: value.max_page_size,
            supports_browse_sessions: value.supports_browse_sessions,
            supports_search: value.supports_search,
            organization: organization(value.organization)?,
            source: source(value.source)?,
            supports_indexed_search: value.supports_indexed_search,
            indexed_search_protocol_version: value.indexed_search_protocol_version,
            max_indexed_search_results: value.max_indexed_search_results,
            search_index_state: search_index_state(value.search_index_state)?,
        })
    }
}

impl TryFrom<proto::BrowseNode> for BrowseNode {
    type Error = Error;

    fn try_from(value: proto::BrowseNode) -> Result<Self> {
        let kind = node_kind(value.kind)?;
        if kind.is_item() && value.item_id.is_none() {
            return Err(Error::Protocol(
                "gateway returned a selectable browse node without an ItemID".into(),
            ));
        }
        if !kind.is_item() && value.item_id.is_some() {
            return Err(Error::Protocol(
                "gateway returned an ItemID for a non-selectable browse node".into(),
            ));
        }
        Ok(Self {
            node_key: value.node_key,
            display_name: value.display_name,
            kind,
            item_id: value.item_id,
        })
    }
}

impl TryFrom<proto::BrowsePage> for BrowsePage {
    type Error = Error;

    fn try_from(value: proto::BrowsePage) -> Result<Self> {
        if value.complete && value.next_page_token.is_some() {
            return Err(Error::Protocol(
                "gateway returned a complete browse page with a continuation token".into(),
            ));
        }
        if !value.complete && value.next_page_token.is_none() {
            return Err(Error::Protocol(
                "gateway returned an incomplete browse page without a continuation token".into(),
            ));
        }
        Ok(Self {
            session_id: value.session_id,
            nodes: value
                .nodes
                .into_iter()
                .map(BrowseNode::try_from)
                .collect::<Result<_>>()?,
            next_page_token: value.next_page_token,
            complete: value.complete,
            organization: organization(value.organization)?,
            source: source(value.source)?,
            warning: value.warning,
        })
    }
}

impl From<BrowsePageRequest> for proto::BrowseRequest {
    fn from(value: BrowsePageRequest) -> Self {
        Self {
            server: value.server,
            session_id: value.session_id,
            parent_node_key: value.parent_node_key,
            page_token: value.page_token,
            page_size: value.page_size,
            refresh: value.refresh,
        }
    }
}

impl From<SearchRequest> for proto::SearchRequest {
    fn from(value: SearchRequest) -> Self {
        let match_mode = match value.match_mode {
            SearchMatchMode::Exact => proto::SearchMatchMode::Exact,
            SearchMatchMode::Prefix => proto::SearchMatchMode::Prefix,
            SearchMatchMode::Contains => proto::SearchMatchMode::Contains,
        };
        Self {
            server: value.server,
            query: value.query,
            match_mode: match_mode as i32,
            session_id: value.session_id,
            scope_node_key: value.scope_node_key,
            max_results: value.max_results,
            include_branches: value.include_branches,
            refresh: value.refresh,
        }
    }
}

impl From<SearchIndexRequest> for proto::SearchIndexRequest {
    fn from(value: SearchIndexRequest) -> Self {
        let match_mode = match value.match_mode {
            SearchMatchMode::Exact => proto::SearchMatchMode::Exact,
            SearchMatchMode::Prefix => proto::SearchMatchMode::Prefix,
            SearchMatchMode::Contains => proto::SearchMatchMode::Contains,
        };
        Self {
            server: value.server,
            query: value.query,
            match_mode: match_mode as i32,
            max_results: value.max_results,
        }
    }
}

impl From<SearchIndexControlAction> for proto::SearchIndexControlAction {
    fn from(value: SearchIndexControlAction) -> Self {
        match value {
            SearchIndexControlAction::Pause => Self::Pause,
            SearchIndexControlAction::Resume => Self::Resume,
            SearchIndexControlAction::Cancel => Self::Cancel,
        }
    }
}

impl From<proto::IndexedSearchProgress> for IndexedSearchProgress {
    fn from(value: proto::IndexedSearchProgress) -> Self {
        Self {
            branches_visited: value.branches_visited,
            entries_seen: value.entries_seen,
            unique_items: value.unique_items,
            active_time_ms: value.active_time_ms,
            paused_time_ms: value.paused_time_ms,
            items_per_second: value.items_per_second,
            estimated_remaining_ms: value.estimated_remaining_ms,
        }
    }
}

impl TryFrom<proto::SearchIndexStatus> for SearchIndexStatus {
    type Error = Error;

    fn try_from(value: proto::SearchIndexStatus) -> Result<Self> {
        Ok(Self {
            server: value.server,
            state: search_index_state(value.state)?,
            configured: value.configured,
            active_generation: value.active_generation,
            entry_count: value.entry_count,
            unique_item_count: value.unique_item_count,
            started_at: value.started_at,
            completed_at: value.completed_at,
            last_error: value.last_error,
            database_bytes: value.database_bytes,
            organization: organization(value.organization)?,
            source: source(value.source)?,
            progress: value.progress.map(Into::into),
        })
    }
}

impl TryFrom<proto::IndexedSearchMatch> for IndexedSearchMatch {
    type Error = Error;

    fn try_from(value: proto::IndexedSearchMatch) -> Result<Self> {
        let kind = node_kind(value.kind)?;
        if !kind.is_item() {
            return Err(Error::Protocol(
                "gateway returned a non-selectable indexed search match".into(),
            ));
        }
        if value.item_id.is_empty() {
            return Err(Error::Protocol(
                "gateway returned an indexed search match without an ItemID".into(),
            ));
        }
        Ok(Self {
            item_id: value.item_id,
            display_name: value.display_name,
            kind,
            breadcrumbs: value.breadcrumbs,
        })
    }
}

impl TryFrom<proto::SearchIndexResponse> for SearchIndexResponse {
    type Error = Error;

    fn try_from(value: proto::SearchIndexResponse) -> Result<Self> {
        Ok(Self {
            matches: value
                .matches
                .into_iter()
                .map(IndexedSearchMatch::try_from)
                .collect::<Result<_>>()?,
            has_more: value.has_more,
            status: value
                .status
                .ok_or_else(|| {
                    Error::Protocol("gateway returned indexed search results without status".into())
                })?
                .try_into()?,
        })
    }
}

impl TryFrom<proto::SearchEvent> for SearchEvent {
    type Error = Error;

    fn try_from(value: proto::SearchEvent) -> Result<Self> {
        match value.event {
            Some(proto::search_event::Event::Match(found)) => {
                let node = found.node.ok_or_else(|| {
                    Error::Protocol("gateway returned a search match without a node".into())
                })?;
                Ok(Self::Match(SearchMatch {
                    node: node.try_into()?,
                    breadcrumbs: found
                        .breadcrumbs
                        .into_iter()
                        .map(|part| BrowseBreadcrumb {
                            node_key: part.node_key,
                            display_name: part.display_name,
                        })
                        .collect(),
                }))
            }
            Some(proto::search_event::Event::Progress(progress)) => {
                Ok(Self::Progress(SearchProgress {
                    visited_nodes: progress.visited_nodes,
                    matches: progress.matches,
                    partial: progress.partial,
                }))
            }
            Some(proto::search_event::Event::Completed(completed)) => {
                Ok(Self::Completed(SearchCompleted {
                    complete: completed.complete,
                    cancelled: completed.cancelled,
                    truncated: completed.truncated,
                    warning: completed.warning,
                }))
            }
            None => Err(Error::Protocol(
                "gateway returned an empty search event".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_parsing_covers_all_variants() {
        assert!(matches!(parse_value("true"), Value::Bool(true)));
        assert!(matches!(parse_value("false"), Value::Bool(false)));
        assert!(matches!(parse_value("42"), Value::Int(42)));
        assert!(matches!(parse_value("-1"), Value::Int(-1)));
        assert!(matches!(parse_value("9.5"), Value::Float(v) if v == 9.5));
        assert!(matches!(parse_value("hello"), Value::String(v) if v == "hello"));
    }

    #[test]
    fn enum_display_and_node_predicates_are_stable() {
        assert_eq!(
            NamespaceOrganization::Unspecified.to_string(),
            "unspecified"
        );
        assert_eq!(NamespaceOrganization::Flat.to_string(), "flat");
        assert_eq!(
            NamespaceOrganization::Hierarchical.to_string(),
            "hierarchical"
        );
        assert_eq!(BrowseSource::Unspecified.to_string(), "unspecified");
        assert_eq!(BrowseSource::Da3.to_string(), "da3");
        assert_eq!(BrowseSource::Da2.to_string(), "da2");
        assert_eq!(BrowseSource::Flat.to_string(), "flat");
        assert_eq!(BrowseSource::Derived.to_string(), "derived");
        assert_eq!(BrowseNodeKind::Unspecified.to_string(), "unspecified");
        assert_eq!(BrowseNodeKind::Branch.to_string(), "branch");
        assert_eq!(BrowseNodeKind::Item.to_string(), "item");
        assert_eq!(BrowseNodeKind::BranchAndItem.to_string(), "branch-and-item");
        assert_eq!(SearchMatchMode::Exact.to_string(), "exact");
        assert_eq!(SearchMatchMode::Prefix.to_string(), "prefix");
        assert_eq!(SearchMatchMode::Contains.to_string(), "contains");
        assert_eq!(SearchIndexState::Unspecified.to_string(), "unspecified");
        assert_eq!(SearchIndexState::NotIndexed.to_string(), "not-indexed");
        assert_eq!(SearchIndexState::Partial.to_string(), "partial");
        assert_eq!(SearchIndexState::Ready.to_string(), "ready");
        assert_eq!(SearchIndexState::Stale.to_string(), "stale");
        assert_eq!(SearchIndexState::Refreshing.to_string(), "refreshing");
        assert_eq!(SearchIndexState::Failed.to_string(), "failed");
        assert!(BrowseNodeKind::Branch.is_branch());
        assert!(!BrowseNodeKind::Branch.is_item());
        assert!(BrowseNodeKind::Item.is_item());
        assert!(!BrowseNodeKind::Item.is_branch());
        assert!(BrowseNodeKind::BranchAndItem.is_branch());
        assert!(BrowseNodeKind::BranchAndItem.is_item());
        assert!(!BrowseNodeKind::Unspecified.is_branch());
        assert!(!BrowseNodeKind::Unspecified.is_item());
    }

    #[test]
    fn browse_request_builders_map_all_fields() {
        let root = BrowsePageRequest::root("S", 20).with_refresh(true);
        assert_eq!(root.server, "S");
        assert_eq!(root.page_size, 20);
        assert!(root.refresh);

        let children = BrowsePageRequest::children("S", "session", "node", 30);
        assert_eq!(children.session_id.as_deref(), Some("session"));
        assert_eq!(children.parent_node_key.as_deref(), Some("node"));

        let next = BrowsePageRequest::next("S", "session", Some("node".into()), "token", 40);
        let proto: proto::BrowseRequest = next.into();
        assert_eq!(proto.page_token.as_deref(), Some("token"));
        assert_eq!(proto.page_size, 40);
    }

    #[test]
    fn search_request_defaults_and_mapping_are_typed() {
        for (mode, expected) in [
            (SearchMatchMode::Exact, proto::SearchMatchMode::Exact),
            (SearchMatchMode::Prefix, proto::SearchMatchMode::Prefix),
            (SearchMatchMode::Contains, proto::SearchMatchMode::Contains),
        ] {
            let request = SearchRequest::new("S", "query", mode);
            assert_eq!(request.max_results, DEFAULT_SEARCH_MAX_RESULTS);
            let mapped: proto::SearchRequest = request.into();
            assert_eq!(mapped.match_mode, expected as i32);
        }
    }

    #[test]
    fn indexed_search_request_and_controls_map_all_variants() {
        for (mode, expected) in [
            (SearchMatchMode::Exact, proto::SearchMatchMode::Exact),
            (SearchMatchMode::Prefix, proto::SearchMatchMode::Prefix),
            (SearchMatchMode::Contains, proto::SearchMatchMode::Contains),
        ] {
            let request = SearchIndexRequest::new("S", "query", mode);
            assert_eq!(request.max_results, DEFAULT_INDEX_SEARCH_MAX_RESULTS);
            let mapped: proto::SearchIndexRequest = request.into();
            assert_eq!(mapped.match_mode, expected as i32);
        }
        for (action, expected) in [
            (
                SearchIndexControlAction::Pause,
                proto::SearchIndexControlAction::Pause,
            ),
            (
                SearchIndexControlAction::Resume,
                proto::SearchIndexControlAction::Resume,
            ),
            (
                SearchIndexControlAction::Cancel,
                proto::SearchIndexControlAction::Cancel,
            ),
        ] {
            assert_eq!(proto::SearchIndexControlAction::from(action), expected);
        }
    }

    #[test]
    fn invalid_and_inconsistent_proto_values_are_rejected() {
        assert_eq!(
            organization(proto::NamespaceOrganization::Unspecified as i32).unwrap(),
            NamespaceOrganization::Unspecified
        );
        assert_eq!(
            organization(proto::NamespaceOrganization::Flat as i32).unwrap(),
            NamespaceOrganization::Flat
        );
        assert_eq!(
            organization(proto::NamespaceOrganization::Hierarchical as i32).unwrap(),
            NamespaceOrganization::Hierarchical
        );
        assert_eq!(
            source(proto::BrowseSource::Unspecified as i32).unwrap(),
            BrowseSource::Unspecified
        );
        assert_eq!(
            source(proto::BrowseSource::Da3 as i32).unwrap(),
            BrowseSource::Da3
        );
        assert_eq!(
            source(proto::BrowseSource::Da2 as i32).unwrap(),
            BrowseSource::Da2
        );
        assert_eq!(
            source(proto::BrowseSource::Flat as i32).unwrap(),
            BrowseSource::Flat
        );
        assert_eq!(
            source(proto::BrowseSource::Derived as i32).unwrap(),
            BrowseSource::Derived
        );
        assert_eq!(
            node_kind(proto::BrowseNodeKind::Unspecified as i32).unwrap(),
            BrowseNodeKind::Unspecified
        );
        assert_eq!(
            node_kind(proto::BrowseNodeKind::Branch as i32).unwrap(),
            BrowseNodeKind::Branch
        );
        assert_eq!(
            node_kind(proto::BrowseNodeKind::Item as i32).unwrap(),
            BrowseNodeKind::Item
        );
        assert_eq!(
            node_kind(proto::BrowseNodeKind::BranchAndItem as i32).unwrap(),
            BrowseNodeKind::BranchAndItem
        );
        assert!(matches!(organization(99), Err(Error::Protocol(_))));
        assert!(matches!(source(99), Err(Error::Protocol(_))));
        assert!(matches!(node_kind(99), Err(Error::Protocol(_))));
        for (proto_state, state) in [
            (
                proto::SearchIndexState::Unspecified,
                SearchIndexState::Unspecified,
            ),
            (
                proto::SearchIndexState::NotIndexed,
                SearchIndexState::NotIndexed,
            ),
            (proto::SearchIndexState::Partial, SearchIndexState::Partial),
            (proto::SearchIndexState::Ready, SearchIndexState::Ready),
            (proto::SearchIndexState::Stale, SearchIndexState::Stale),
            (
                proto::SearchIndexState::Refreshing,
                SearchIndexState::Refreshing,
            ),
            (proto::SearchIndexState::Failed, SearchIndexState::Failed),
        ] {
            assert_eq!(search_index_state(proto_state as i32).unwrap(), state);
        }
        assert!(matches!(search_index_state(99), Err(Error::Protocol(_))));

        let missing_item_id = proto::BrowseNode {
            kind: proto::BrowseNodeKind::Item as i32,
            ..Default::default()
        };
        assert!(matches!(
            BrowseNode::try_from(missing_item_id),
            Err(Error::Protocol(_))
        ));
        let unexpected_item_id = proto::BrowseNode {
            kind: proto::BrowseNodeKind::Branch as i32,
            item_id: Some("not-valid".into()),
            ..Default::default()
        };
        assert!(matches!(
            BrowseNode::try_from(unexpected_item_id),
            Err(Error::Protocol(_))
        ));

        let complete_with_token = proto::BrowsePage {
            complete: true,
            next_page_token: Some("token".into()),
            ..Default::default()
        };
        assert!(matches!(
            BrowsePage::try_from(complete_with_token),
            Err(Error::Protocol(_))
        ));

        let incomplete_without_token = proto::BrowsePage::default();
        assert!(matches!(
            BrowsePage::try_from(incomplete_without_token),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn search_event_conversion_covers_every_event() {
        let found = proto::SearchEvent {
            event: Some(proto::search_event::Event::Match(proto::SearchMatch {
                node: Some(proto::BrowseNode {
                    node_key: "n".into(),
                    display_name: "PV".into(),
                    kind: proto::BrowseNodeKind::Item as i32,
                    item_id: Some("FCS!TAG.PV".into()),
                }),
                breadcrumbs: vec![proto::BrowseBreadcrumb {
                    node_key: "root".into(),
                    display_name: "FCS".into(),
                }],
            })),
        };
        assert!(matches!(
            SearchEvent::try_from(found).unwrap(),
            SearchEvent::Match(_)
        ));

        let progress = proto::SearchEvent {
            event: Some(proto::search_event::Event::Progress(
                proto::SearchProgress {
                    visited_nodes: 10,
                    matches: 2,
                    partial: true,
                },
            )),
        };
        assert!(matches!(
            SearchEvent::try_from(progress).unwrap(),
            SearchEvent::Progress(_)
        ));

        let completed = proto::SearchEvent {
            event: Some(proto::search_event::Event::Completed(
                proto::SearchCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                },
            )),
        };
        assert!(matches!(
            SearchEvent::try_from(completed).unwrap(),
            SearchEvent::Completed(_)
        ));

        assert!(matches!(
            SearchEvent::try_from(proto::SearchEvent::default()),
            Err(Error::Protocol(_))
        ));
        let missing_node = proto::SearchEvent {
            event: Some(proto::search_event::Event::Match(
                proto::SearchMatch::default(),
            )),
        };
        assert!(matches!(
            SearchEvent::try_from(missing_node),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn indexed_search_response_preserves_identity_and_status() {
        let response = proto::SearchIndexResponse {
            matches: vec![proto::IndexedSearchMatch {
                item_id: "FCS0201!204FI00510.PV".into(),
                display_name: "PV".into(),
                kind: proto::BrowseNodeKind::BranchAndItem as i32,
                breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
            }],
            has_more: true,
            status: Some(proto::SearchIndexStatus {
                server: "Yokogawa.CSHIS_OPC.1".into(),
                state: proto::SearchIndexState::Refreshing as i32,
                configured: true,
                active_generation: 7,
                entry_count: 100_001,
                unique_item_count: 100_000,
                started_at: Some("start".into()),
                completed_at: Some("complete".into()),
                last_error: Some("prior error".into()),
                database_bytes: 4096,
                organization: proto::NamespaceOrganization::Hierarchical as i32,
                source: proto::BrowseSource::Da2 as i32,
                progress: Some(proto::IndexedSearchProgress {
                    branches_visited: 10,
                    entries_seen: 20,
                    unique_items: 19,
                    active_time_ms: 30,
                    paused_time_ms: 40,
                    items_per_second: 12.5,
                    estimated_remaining_ms: Some(50),
                }),
            }),
        };
        let typed = SearchIndexResponse::try_from(response).unwrap();
        assert_eq!(typed.matches[0].item_id, "FCS0201!204FI00510.PV");
        assert_eq!(typed.matches[0].kind, BrowseNodeKind::BranchAndItem);
        assert_eq!(typed.status.state, SearchIndexState::Refreshing);
        assert_eq!(
            typed
                .status
                .progress
                .as_ref()
                .unwrap()
                .estimated_remaining_ms,
            Some(50)
        );
        assert!(typed.has_more);

        let item = IndexedSearchMatch::try_from(proto::IndexedSearchMatch {
            kind: proto::BrowseNodeKind::Item as i32,
            item_id: "id".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(item.kind, BrowseNodeKind::Item);
        assert!(matches!(
            IndexedSearchMatch::try_from(proto::IndexedSearchMatch {
                kind: proto::BrowseNodeKind::Item as i32,
                ..Default::default()
            }),
            Err(Error::Protocol(_))
        ));

        assert!(matches!(
            IndexedSearchMatch::try_from(proto::IndexedSearchMatch {
                kind: proto::BrowseNodeKind::Branch as i32,
                ..Default::default()
            }),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            SearchIndexResponse::try_from(proto::SearchIndexResponse::default()),
            Err(Error::Protocol(_))
        ));
    }
}
