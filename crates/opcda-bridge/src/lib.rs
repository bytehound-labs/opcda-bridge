//! Reusable, presentation-free async client for the opcda-bridge gateway's
//! gRPC API.
//!
//! This crate is the typed connect/read/write/browse/search/list-servers surface
//! extracted from `opcda-bridge-client`'s `commands.rs`: no `clap`, no
//! `tabled`, no `serde_json`/`toml` — just [`Client`], the parameters its
//! methods take, and the plain result types they return ([`BrowsePage`],
//! [`SearchEvent`], [`SearchIndexResponse`], [`TagValue`], [`WriteResult`], [`Value`]). `opcda-bridge-client` depends
//! on this crate and adds only CLI parsing and table/JSON rendering on top
//! of it; any other async Rust program that needs typed OPC DA
//! reads/writes/browses without shelling out to the CLI binary and parsing
//! its output can depend on this crate directly instead.
//!
//! Read results contain semantic values: an OPC DA `VT_BSTR` is returned with
//! its exact contents, without display quote framing.
//!
//! ```no_run
//! use opcda_bridge::{
//!     BrowsePageRequest, SearchIndexRequest, SearchMatchMode, SearchRequest,
//! };
//!
//! # async fn example() -> opcda_bridge::Result<()> {
//! let mut client = opcda_bridge::Client::connect("localhost:7600").await?;
//! let servers = client.list_servers().await?;
//! let root = client.browse(servers[0].clone(), 200).await?;
//! if let Some(token) = root.next_page_token.clone() {
//!     let request =
//!         BrowsePageRequest::next(servers[0].clone(), root.session_id.clone(), None, token, 200);
//!     let _next_page = client.browse_page(request).await?;
//! }
//! let mut search = client
//!     .search_stream(SearchRequest::new(
//!         servers[0].clone(),
//!         "Some",
//!         SearchMatchMode::Prefix,
//!     ))
//!     .await?;
//! while let Some(event) = search.message().await? {
//!     println!("{event:?}");
//! }
//! let indexed = client
//!     .search_index(SearchIndexRequest::new(
//!         servers[0].clone(),
//!         "Some PV",
//!         SearchMatchMode::Contains,
//!     ))
//!     .await?;
//! for found in indexed.matches {
//!     println!("{}: {}", found.display_name, found.item_id);
//! }
//! let values = client
//!     .read(servers[0].clone(), vec!["Some.Tag".into()])
//!     .await?;
//! client.close_browse_session(root.session_id).await?;
//! # let _ = values;
//! # Ok(())
//! # }
//! ```

mod client;
mod compatibility;
mod error;
mod types;

#[cfg(test)]
mod test_support;

pub use client::Client;
pub use client::SearchStream;
pub use compatibility::{
    CompatibilityEvidence, CompatibilityFeature, CompatibilityReport, CompatibilitySource,
    CompatibilityStatus, FeatureCompatibility, FeatureCompatibilityStatus, GatewayInfo,
    ProtocolFeatureSupport, ProtocolProfile, ProtocolVersionRange, current_client_profile,
    evaluate_compatibility, legacy_gateway_profile, unknown_compatibility_report,
};
pub use error::{Error, Result};
pub use opcda_bridge_proto::DEFAULT_BRIDGE_PORT;
pub use types::{
    BrowseBreadcrumb, BrowseNode, BrowseNodeKind, BrowsePage, BrowsePageRequest, BrowseSource,
    Capabilities, DEFAULT_INDEX_SEARCH_MAX_RESULTS, DEFAULT_PAGE_SIZE, DEFAULT_SEARCH_MAX_RESULTS,
    IndexControllerState, IndexForegroundDiagnostics, IndexHealthDiagnostics, IndexHealthState,
    IndexHostDiagnostics, IndexInventoryLimits, IndexPauseReason, IndexSchedulerDiagnostics,
    IndexStorageDiagnostics, IndexedSearchMatch, IndexedSearchProgress, NamespaceOrganization,
    SearchCompleted, SearchEvent, SearchIndexControlAction, SearchIndexRequest,
    SearchIndexResponse, SearchIndexState, SearchIndexStatus, SearchMatch, SearchMatchMode,
    SearchProgress, SearchRequest, TagValue, Value, WriteResult, parse_value,
};
