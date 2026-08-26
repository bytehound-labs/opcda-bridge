/// Default TCP port used by the gateway and clients.
pub const DEFAULT_BRIDGE_PORT: u16 = 7600;

/// Generated compatibility contract metadata shared by the gateway and clients.
pub mod compatibility {
    /// A package release line and the protocol contracts it implements.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReleaseLine {
        pub name: &'static str,
        pub min_version: &'static str,
        pub max_version: &'static str,
        pub status: &'static str,
        pub core_protocol: u32,
        pub namespace_protocol: u32,
        pub indexed_search_protocol: u32,
    }

    include!(concat!(env!("OUT_DIR"), "/compatibility_generated.rs"));

    /// The canonical compatibility catalog bundled with this crate.
    pub const CATALOG_TOML: &str = include_str!("../compatibility.toml");

    fn version_tuple(value: &str) -> Option<(u64, u64, u64)> {
        let parts = value.split('.').collect::<Vec<_>>();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        Some((
            parts[0].parse().ok()?,
            parts[1].parse().ok()?,
            parts[2].parse().ok()?,
        ))
    }

    /// Find the catalog release line containing an exact `X.Y.Z` version.
    pub fn release_line_for(version: &str) -> Option<&'static ReleaseLine> {
        let version = version_tuple(version)?;
        RELEASE_LINES.iter().find(|line| {
            let Some(minimum) = version_tuple(line.min_version) else {
                return false;
            };
            let Some(maximum) = version_tuple(line.max_version) else {
                return false;
            };
            minimum <= version && version <= maximum
        })
    }
}

pub mod bridge {
    tonic::include_proto!("bridge");
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_BRIDGE_PORT;
    use crate::bridge::write_request::TypedValue;
    use crate::bridge::{
        BrowseNode, BrowseNodeKind, BrowsePage, BrowseRequest, BrowseSource,
        CloseBrowseSessionRequest, ControlSearchIndexRequest, GetCapabilitiesRequest,
        GetCapabilitiesResponse, GetSearchIndexStatusRequest, IndexedSearchMatch,
        IndexedSearchProgress, ListServersRequest, ListServersResponse, NamespaceOrganization,
        ReadRequest, ReadResponse, RefreshSearchIndexRequest, SearchCompleted, SearchEvent,
        SearchIndexControlAction, SearchIndexRequest, SearchIndexResponse, SearchIndexState,
        SearchIndexStatus, SearchMatch, SearchMatchMode, SearchProgress, TagValue, WriteRequest,
        WriteResponse,
    };

    #[test]
    fn test_default_bridge_port() {
        assert_eq!(DEFAULT_BRIDGE_PORT, 7600);
    }

    #[test]
    fn test_compatibility_catalog_release_lines() {
        let legacy = crate::compatibility::release_line_for("0.3.1").unwrap();
        assert_eq!(legacy.name, "legacy");
        assert_eq!(legacy.namespace_protocol, 1);
        assert_eq!(legacy.status, "legacy");

        let paged = crate::compatibility::release_line_for("0.3.2").unwrap();
        assert_eq!(paged.name, "paged");
        assert_eq!(paged.namespace_protocol, 2);

        let indexed = crate::compatibility::release_line_for("0.4.3").unwrap();
        assert_eq!(indexed.name, "indexed");
        assert_eq!(indexed.indexed_search_protocol, 1);
        assert!(crate::compatibility::release_line_for("1.0").is_none());
        assert!(crate::compatibility::release_line_for("0.x.3").is_none());
    }

    #[test]
    fn test_list_servers_request() {
        let req = ListServersRequest {
            host: "localhost".into(),
        };
        assert_eq!(req.host, "localhost");
    }

    #[test]
    fn test_list_servers_request_empty() {
        let req = ListServersRequest {
            host: String::new(),
        };
        assert!(req.host.is_empty());
    }

    #[test]
    fn test_list_servers_response_empty() {
        let resp = ListServersResponse { servers: vec![] };
        assert!(resp.servers.is_empty());
    }

    #[test]
    fn test_list_servers_response_with_data() {
        let resp = ListServersResponse {
            servers: vec!["S1".into(), "S2".into()],
        };
        assert_eq!(resp.servers.len(), 2);
        assert_eq!(resp.servers[0], "S1");
    }

    #[test]
    fn test_browse_request() {
        let req = BrowseRequest {
            server: "MyServer".into(),
            session_id: Some("session".into()),
            parent_node_key: Some("parent".into()),
            page_token: Some("page".into()),
            page_size: 500,
            refresh: true,
        };
        assert_eq!(req.server, "MyServer");
        assert_eq!(req.session_id.as_deref(), Some("session"));
        assert_eq!(req.parent_node_key.as_deref(), Some("parent"));
        assert_eq!(req.page_token.as_deref(), Some("page"));
        assert_eq!(req.page_size, 500);
        assert!(req.refresh);
    }

    #[test]
    fn test_browse_request_defaults() {
        let req = BrowseRequest {
            server: String::new(),
            session_id: None,
            parent_node_key: None,
            page_token: None,
            page_size: 0,
            refresh: false,
        };
        assert!(req.session_id.is_none());
        assert_eq!(req.page_size, 0);
    }

    #[test]
    fn test_browse_page_and_node() {
        let node = BrowseNode {
            node_key: "opaque".into(),
            display_name: "PV".into(),
            kind: BrowseNodeKind::BranchAndItem as i32,
            item_id: Some("FCS0201!204FI00510.PV".into()),
        };
        let page = BrowsePage {
            session_id: "session".into(),
            nodes: vec![node],
            next_page_token: Some("next".into()),
            complete: false,
            organization: NamespaceOrganization::Hierarchical as i32,
            source: BrowseSource::Da2 as i32,
            warning: Some("partial".into()),
        };
        assert_eq!(page.nodes[0].display_name, "PV");
        assert_eq!(page.next_page_token.as_deref(), Some("next"));
        assert!(!page.complete);
    }

    #[test]
    fn test_capabilities_and_close_request() {
        let request = GetCapabilitiesRequest { server: "S".into() };
        let response = GetCapabilitiesResponse {
            application_version: "0.3.0".into(),
            protocol_version: "2".into(),
            max_page_size: 1000,
            supports_browse_sessions: true,
            supports_search: true,
            organization: NamespaceOrganization::Flat as i32,
            source: BrowseSource::Flat as i32,
            supports_indexed_search: true,
            indexed_search_protocol_version: "1".into(),
            max_indexed_search_results: 50,
            search_index_state: SearchIndexState::Ready as i32,
            search_index_promoting: false,
        };
        let close = CloseBrowseSessionRequest {
            session_id: "s".into(),
        };
        assert_eq!(request.server, "S");
        assert_eq!(response.max_page_size, 1000);
        assert_eq!(close.session_id, "s");
        assert!(response.supports_indexed_search);
        assert_eq!(response.max_indexed_search_results, 50);
    }

    #[test]
    fn test_search_messages() {
        let match_message = SearchMatch {
            node: None,
            breadcrumbs: vec![],
        };
        let progress = SearchProgress {
            visited_nodes: 4,
            matches: 2,
            partial: true,
        };
        let completed = SearchCompleted {
            complete: false,
            cancelled: false,
            truncated: true,
            warning: Some("capped".into()),
        };
        let event = SearchEvent {
            event: Some(crate::bridge::search_event::Event::Progress(progress)),
        };
        assert!(event.event.is_some());
        assert!(match_message.breadcrumbs.is_empty());
        assert!(completed.truncated);
        assert_eq!(
            SearchMatchMode::Contains as i32,
            SearchMatchMode::try_from(SearchMatchMode::Contains as i32).unwrap() as i32
        );
    }

    #[test]
    fn test_indexed_search_messages() {
        let status_request = GetSearchIndexStatusRequest { server: "S".into() };
        let refresh_request = RefreshSearchIndexRequest {
            server: "S".into(),
            force: true,
        };
        let control_request = ControlSearchIndexRequest {
            server: "S".into(),
            action: SearchIndexControlAction::Pause as i32,
        };
        let search_request = SearchIndexRequest {
            server: "S".into(),
            query: "PV".into(),
            match_mode: SearchMatchMode::Prefix as i32,
            max_results: 50,
        };
        let status = SearchIndexStatus {
            server: "S".into(),
            state: SearchIndexState::Refreshing as i32,
            configured: true,
            active_generation: 2,
            entry_count: 100,
            unique_item_count: 99,
            started_at: Some("start".into()),
            completed_at: Some("complete".into()),
            last_error: None,
            database_bytes: 1024,
            organization: NamespaceOrganization::Hierarchical as i32,
            source: BrowseSource::Da3 as i32,
            progress: Some(IndexedSearchProgress {
                branches_visited: 1,
                entries_seen: 2,
                unique_items: 2,
                active_time_ms: 3,
                paused_time_ms: 4,
                items_per_second: 5.0,
                estimated_remaining_ms: Some(6),
            }),
            ..Default::default()
        };
        let response = SearchIndexResponse {
            matches: vec![IndexedSearchMatch {
                item_id: "Exact.ItemID".into(),
                display_name: "PV".into(),
                kind: BrowseNodeKind::Item as i32,
                breadcrumbs: vec!["Area".into()],
            }],
            has_more: true,
            status: Some(status),
        };

        assert_eq!(status_request.server, "S");
        assert!(refresh_request.force);
        assert_eq!(
            control_request.action,
            SearchIndexControlAction::Pause as i32
        );
        assert_eq!(search_request.max_results, 50);
        assert!(response.has_more);
        assert_eq!(response.matches[0].item_id, "Exact.ItemID");
    }

    #[test]
    fn test_read_request() {
        let req = ReadRequest {
            server: "MyServer".into(),
            tag_ids: vec!["t1".into(), "t2".into()],
        };
        assert_eq!(req.server, "MyServer");
        assert_eq!(req.tag_ids.len(), 2);
    }

    #[test]
    fn test_read_request_empty_tags() {
        let req = ReadRequest {
            server: "S".into(),
            tag_ids: vec![],
        };
        assert!(req.tag_ids.is_empty());
    }

    #[test]
    fn test_tag_value() {
        let tv = TagValue {
            tag_id: "t1".into(),
            value: "42.5".into(),
            quality: "Good".into(),
            timestamp: "2026-01-01 00:00:00".into(),
        };
        assert_eq!(tv.tag_id, "t1");
        assert_eq!(tv.value, "42.5");
        assert_eq!(tv.quality, "Good");
        assert_eq!(tv.timestamp, "2026-01-01 00:00:00");
    }

    #[test]
    fn test_read_response() {
        let resp = ReadResponse {
            values: vec![TagValue {
                tag_id: "t1".into(),
                value: "42".into(),
                quality: "Good".into(),
                timestamp: "now".into(),
            }],
        };
        assert_eq!(resp.values.len(), 1);
    }

    #[test]
    fn test_read_response_empty() {
        let resp = ReadResponse { values: vec![] };
        assert!(resp.values.is_empty());
    }

    #[test]
    fn test_write_request_string() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::StringValue("hello".into())),
        };
        assert_eq!(req.server, "S");
        assert_eq!(req.tag_id, "t1");
        assert!(matches!(req.typed_value, Some(TypedValue::StringValue(ref s)) if s == "hello"));
    }

    #[test]
    fn test_write_request_int() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::IntValue(42)),
        };
        assert!(matches!(req.typed_value, Some(TypedValue::IntValue(42))));
    }

    #[test]
    fn test_write_request_float() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::FloatValue(9.5)),
        };
        assert!(
            matches!(req.typed_value, Some(TypedValue::FloatValue(v)) if (v - 9.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_write_request_bool() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: Some(TypedValue::BoolValue(true)),
        };
        assert!(matches!(req.typed_value, Some(TypedValue::BoolValue(true))));
    }

    #[test]
    fn test_write_request_no_value() {
        let req = WriteRequest {
            server: "S".into(),
            tag_id: "t1".into(),
            typed_value: None,
        };
        assert!(req.typed_value.is_none());
    }

    #[test]
    fn test_write_response_success() {
        let resp = WriteResponse {
            tag_id: "t1".into(),
            success: true,
            error: None,
        };
        assert_eq!(resp.tag_id, "t1");
        assert!(resp.success);
        assert_eq!(resp.error, None);
    }

    #[test]
    fn test_write_response_failure() {
        let resp = WriteResponse {
            tag_id: "t1".into(),
            success: false,
            error: Some("access denied".into()),
        };
        assert!(!resp.success);
        assert_eq!(resp.error, Some("access denied".into()));
    }

    #[test]
    fn test_typed_value_string() {
        let tv = TypedValue::StringValue("test".into());
        assert!(matches!(tv, TypedValue::StringValue(ref s) if s == "test"));
    }

    #[test]
    fn test_typed_value_int() {
        let tv = TypedValue::IntValue(-1);
        assert!(matches!(tv, TypedValue::IntValue(-1)));
    }

    #[test]
    fn test_typed_value_float() {
        let tv = TypedValue::FloatValue(0.0);
        assert!(matches!(tv, TypedValue::FloatValue(v) if v == 0.0));
    }

    #[test]
    fn test_typed_value_bool() {
        let tv = TypedValue::BoolValue(false);
        assert!(matches!(tv, TypedValue::BoolValue(false)));
    }
}
