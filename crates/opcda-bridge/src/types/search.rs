use super::browse::{BrowseBreadcrumb, BrowseNode};
use std::fmt;

/// Default maximum number of matches requested by a search.
pub const DEFAULT_SEARCH_MAX_RESULTS: u32 = 200;
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
