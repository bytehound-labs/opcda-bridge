//! Plain data types returned by [`crate::Client`]'s methods.
//!
//! Browse, search, index, value, and conversion items live in private modules
//! and are re-exported from this module.

mod browse;
mod conversions;
mod index;
mod search;
mod values;

pub use browse::{
    BrowseBreadcrumb, BrowseNode, BrowseNodeKind, BrowsePage, BrowsePageRequest, BrowseSource,
    Capabilities, DEFAULT_PAGE_SIZE, NamespaceOrganization,
};
pub use index::{
    DEFAULT_INDEX_SEARCH_MAX_RESULTS, IndexControllerState, IndexForegroundDiagnostics,
    IndexHealthDiagnostics, IndexHealthState, IndexHostDiagnostics, IndexInventoryLimits,
    IndexPauseReason, IndexSchedulerDiagnostics, IndexStorageDiagnostics, IndexedSearchMatch,
    IndexedSearchProgress, SearchIndexControlAction, SearchIndexRequest, SearchIndexResponse,
    SearchIndexState, SearchIndexStatus,
};
pub use search::{
    DEFAULT_SEARCH_MAX_RESULTS, SearchCompleted, SearchEvent, SearchMatch, SearchMatchMode,
    SearchProgress, SearchRequest,
};
pub use values::{TagValue, Value, WriteResult, parse_value};
