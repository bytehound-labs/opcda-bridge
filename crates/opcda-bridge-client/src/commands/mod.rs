//! CLI command implementations.
//!
//! Each subcommand lives in a private module. The `cmd_*` functions are
//! re-exported so `opcda_bridge_client::commands::cmd_*` stays the public path.

mod browse;
mod capabilities;
mod compatibility;
mod index;
mod read;
mod search;
mod servers;
mod write;

pub use browse::{cmd_browse, cmd_close_browse_session};
pub use capabilities::cmd_capabilities;
pub use compatibility::cmd_compatibility;
pub use index::{cmd_index_control, cmd_index_refresh, cmd_index_search, cmd_index_status};
pub use read::cmd_read;
pub use search::cmd_search;
pub use servers::cmd_servers;
pub use write::cmd_write;

#[cfg(test)]
use browse::{
    ensure_page_bound, merge_warnings, next_browse_page_count, render_browse,
    should_fetch_browse_page, stopped_at_browse_safety_cap,
};
#[cfg(test)]
use compatibility::{render_compatibility, version_range};
#[cfg(test)]
use index::{
    WatchStatusOutput, render_index_status, render_indexed_search, render_watch_status,
    watch_index_status,
};
#[cfg(test)]
use search::{search_completion_messages, search_output_event};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputFormat;
    use crate::test_support::{MockBridgeService, start_mock_server};
    use opcda_bridge::{
        BrowseNode, BrowsePage, Client, CompatibilityFeature, CompatibilityReport,
        FeatureCompatibilityStatus, SearchEvent, SearchMatchMode,
    };
    use opcda_bridge::{
        BrowseNodeKind, BrowseSource, CompatibilityEvidence, CompatibilitySource,
        CompatibilityStatus, FeatureCompatibility, NamespaceOrganization, ProtocolVersionRange,
        SearchIndexControlAction, SearchIndexState,
    };
    use opcda_bridge_proto::bridge::search_event;
    use opcda_bridge_proto::bridge::write_request::TypedValue;
    use opcda_bridge_proto::bridge::{
        BrowseBreadcrumb, BrowseNode as ProtoBrowseNode, BrowseNodeKind as ProtoBrowseNodeKind,
        BrowsePage as ProtoBrowsePage, BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse,
        GetGatewayInfoResponse, IndexedSearchMatch, IndexedSearchProgress, ListServersResponse,
        NamespaceOrganization as ProtoOrganization, ProtocolFeature, ProtocolFeatureKind,
        ReadResponse, SearchCompleted, SearchEvent as ProtoSearchEvent, SearchIndexResponse,
        SearchIndexState as ProtoSearchIndexState, SearchIndexStatus, SearchMatch, SearchProgress,
        TagValue as ProtoTagValue, WriteResponse,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    fn page(complete: bool, token: Option<&str>, name: &str) -> ProtoBrowsePage {
        ProtoBrowsePage {
            session_id: "session".into(),
            nodes: vec![ProtoBrowseNode {
                node_key: format!("key-{name}"),
                display_name: name.into(),
                kind: ProtoBrowseNodeKind::Item as i32,
                item_id: Some(format!("Item.{name}")),
            }],
            next_page_token: token.map(str::to_string),
            complete,
            organization: ProtoOrganization::Hierarchical as i32,
            source: ProtoBrowseSource::Da2 as i32,
            warning: None,
        }
    }

    #[tokio::test]
    async fn basic_commands_render_table_and_json() {
        let service = MockBridgeService {
            capabilities_response: GetCapabilitiesResponse {
                application_version: "0.3".into(),
                protocol_version: "0.3".into(),
                max_page_size: 1000,
                supports_browse_sessions: true,
                supports_search: true,
                organization: ProtoOrganization::Flat as i32,
                source: ProtoBrowseSource::Flat as i32,
                supports_indexed_search: true,
                indexed_search_protocol_version: "1".into(),
                max_indexed_search_results: 50,
                search_index_state: ProtoSearchIndexState::Ready as i32,
                search_index_promoting: false,
            },
            list_servers_response: ListServersResponse {
                servers: vec!["S".into()],
            },
            read_response: ReadResponse {
                values: vec![ProtoTagValue {
                    tag_id: "t".into(),
                    value: "1".into(),
                    quality: "Good".into(),
                    timestamp: "now".into(),
                }],
            },
            write_response: WriteResponse {
                tag_id: "t".into(),
                success: true,
                error: None,
            },
            ..Default::default()
        };
        let capabilities_requests = Arc::clone(&service.capabilities_requests);
        let read_requests = Arc::clone(&service.read_requests);
        let write_requests = Arc::clone(&service.write_requests);
        let host = start_mock_server(service).await;
        cmd_servers(host.clone(), OutputFormat::Table)
            .await
            .unwrap();
        cmd_capabilities(host.clone(), "S".into(), OutputFormat::Json)
            .await
            .unwrap();
        cmd_read(
            host.clone(),
            "S".into(),
            vec!["t".into()],
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let err = cmd_search(
            "unused".into(),
            "S".into(),
            "PV".into(),
            SearchMatchMode::Exact,
            None,
            None,
            0,
            false,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("greater than zero"));
        cmd_write(
            host.clone(),
            "S".into(),
            "t".into(),
            "1".into(),
            OutputFormat::Table,
        )
        .await
        .unwrap();
        cmd_write(
            host,
            "S".into(),
            "t".into(),
            "text".into(),
            OutputFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(capabilities_requests.lock().unwrap()[0].server, "S");
        {
            let requests = read_requests.lock().unwrap();
            assert_eq!(requests[0].server, "S");
            assert_eq!(requests[0].tag_ids, ["t"]);
        }
        {
            let requests = write_requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].server, "S");
            assert_eq!(requests[0].tag_id, "t");
            assert!(matches!(
                requests[0].typed_value,
                Some(TypedValue::IntValue(1))
            ));
            assert!(matches!(
                requests[1].typed_value,
                Some(TypedValue::StringValue(ref value)) if value == "text"
            ));
        }
    }

    #[tokio::test]
    async fn compatibility_command_reports_full_and_rejects_missing_requirements() {
        let host = start_mock_server(MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.5.0".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::IndexedSearch as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                ],
            },
            ..Default::default()
        })
        .await;
        cmd_compatibility(
            host,
            None,
            vec![
                CompatibilityFeature::Core,
                CompatibilityFeature::IndexedSearch,
            ],
            OutputFormat::Json,
        )
        .await
        .unwrap();

        let host = start_mock_server(MockBridgeService {
            gateway_info_response: GetGatewayInfoResponse {
                application_version: "0.3.2".into(),
                compatibility_schema_version: 1,
                features: vec![
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Core as i32,
                        min_version: 1,
                        max_version: 1,
                    },
                    ProtocolFeature {
                        kind: ProtocolFeatureKind::Namespace as i32,
                        min_version: 2,
                        max_version: 2,
                    },
                ],
            },
            ..Default::default()
        })
        .await;
        let error = cmd_compatibility(
            host,
            None,
            vec![CompatibilityFeature::IndexedSearch],
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("indexed-search"));
    }

    #[test]
    fn compatibility_rendering_handles_empty_features_and_version_ranges() {
        let empty = CompatibilityReport {
            client_version: "client".into(),
            library_version: "library".into(),
            gateway_version: None,
            source: CompatibilitySource::Unknown,
            status: CompatibilityStatus::Unknown,
            evidence: CompatibilityEvidence::Unverified,
            features: Vec::new(),
        };
        let rendered = render_compatibility(&empty, OutputFormat::Table).unwrap();
        assert!(rendered.contains("none"));
        assert!(rendered.contains("gateway did not provide"));

        let ranged = CompatibilityReport {
            gateway_version: Some("gateway".into()),
            source: CompatibilitySource::GatewayInfo,
            status: CompatibilityStatus::Partial,
            evidence: CompatibilityEvidence::ContractBoundaryTested,
            features: vec![FeatureCompatibility {
                feature: CompatibilityFeature::Namespace,
                status: FeatureCompatibilityStatus::Incompatible,
                client_versions: ProtocolVersionRange { min: 1, max: 2 },
                gateway_versions: Some(ProtocolVersionRange { min: 3, max: 4 }),
                negotiated_version: None,
                reason: "ranges do not overlap".into(),
            }],
            ..empty
        };
        let rendered = render_compatibility(&ranged, OutputFormat::Table).unwrap();
        assert!(rendered.contains("1-2"));
        assert!(rendered.contains("3-4"));
        assert!(rendered.contains("none"));

        let exact = CompatibilityReport {
            features: vec![FeatureCompatibility {
                feature: CompatibilityFeature::Core,
                status: FeatureCompatibilityStatus::Compatible,
                client_versions: ProtocolVersionRange::exact(1),
                gateway_versions: None,
                negotiated_version: Some(1),
                reason: "exact".into(),
            }],
            ..ranged
        };
        let rendered = render_compatibility(&exact, OutputFormat::Table).unwrap();
        assert!(rendered.contains("unknown"));
        assert!(rendered.contains('1'));
        assert_eq!(version_range(Some(ProtocolVersionRange::exact(1))), "1");
        assert_eq!(
            version_range(Some(ProtocolVersionRange { min: 1, max: 2 })),
            "1-2"
        );
        assert_eq!(version_range(None), "unknown");
    }

    #[tokio::test]
    async fn browse_one_page_preserves_metadata_and_request() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), page(true, None, "B")],
            ..Default::default()
        };
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        cmd_browse(
            host,
            "S".into(),
            Some("session".into()),
            Some("parent".into()),
            None,
            20,
            false,
            100,
            true,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].page_size, 20);
        assert!(requests[0].refresh);
    }

    #[tokio::test]
    async fn browse_all_follows_pages_and_honors_remaining_limit() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next-1"), "A"), page(true, None, "B")],
            ..Default::default()
        };
        let requests = Arc::clone(&service.browse_requests);
        let host = start_mock_server(service).await;
        cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            3,
            true,
            3,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].page_token.as_deref(), Some("next-1"));
        assert_eq!(requests[1].page_size, 2);
    }

    #[tokio::test]
    async fn browse_all_reports_exhausted_mock_response_sequence() {
        let service = MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A")],
            ..Default::default()
        };
        let host = start_mock_server(service).await;
        let error = cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            2,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("mock browse response sequence exhausted")
        );
    }

    #[test]
    fn browse_paging_helpers_preserve_page_counts_and_cap_conditions() {
        assert_eq!(next_browse_page_count(1), 2);
        assert!(should_fetch_browse_page(true, false, 1, 2));
        assert!(!should_fetch_browse_page(false, false, 1, 2));
        assert!(!should_fetch_browse_page(true, true, 1, 2));
        assert!(!should_fetch_browse_page(true, false, 2, 2));
        assert!(stopped_at_browse_safety_cap(true, false, 2, 2));
        assert!(!stopped_at_browse_safety_cap(false, false, 2, 2));
        assert!(!stopped_at_browse_safety_cap(true, true, 2, 2));
        assert!(!stopped_at_browse_safety_cap(true, false, 1, 2));
    }

    #[tokio::test]
    async fn browse_all_rejects_invalid_limits_and_session_changes() {
        let err = cmd_browse(
            "unused".into(),
            "S".into(),
            None,
            None,
            None,
            0,
            false,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("page-size"));

        let err = cmd_browse(
            "unused".into(),
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            0,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("greater than zero"));

        let mut second = page(true, None, "B");
        second.session_id = "other".into();
        let host = start_mock_server(MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), second],
            ..Default::default()
        })
        .await;
        let err = cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("changed browse session"));

        let mut second = page(true, None, "B");
        second.source = ProtoBrowseSource::Da3 as i32;
        let host = start_mock_server(MockBridgeService {
            browse_responses: vec![page(false, Some("next"), "A"), second],
            ..Default::default()
        })
        .await;
        let err = cmd_browse(
            host,
            "S".into(),
            None,
            None,
            None,
            1,
            true,
            10,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("namespace metadata"));
    }

    #[tokio::test]
    async fn close_and_search_stream_events() {
        let service = MockBridgeService {
            search_events: vec![
                ProtoSearchEvent {
                    event: Some(search_event::Event::Match(SearchMatch {
                        node: Some(ProtoBrowseNode {
                            node_key: "n".into(),
                            display_name: "PV".into(),
                            kind: ProtoBrowseNodeKind::Item as i32,
                            item_id: Some("FCS!TAG.PV".into()),
                        }),
                        breadcrumbs: vec![BrowseBreadcrumb {
                            node_key: "b".into(),
                            display_name: "FCS".into(),
                        }],
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Progress(SearchProgress {
                        visited_nodes: 5,
                        matches: 1,
                        partial: true,
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Completed(SearchCompleted {
                        complete: false,
                        cancelled: false,
                        truncated: true,
                        warning: Some("cap reached".into()),
                    })),
                },
            ],
            ..Default::default()
        };
        let close_requests = Arc::clone(&service.close_requests);
        let search_requests = Arc::clone(&service.search_requests);
        let host = start_mock_server(service).await;
        cmd_close_browse_session(host.clone(), "session".into(), OutputFormat::Json)
            .await
            .unwrap();
        cmd_search(
            host.clone(),
            "S".into(),
            "PV".into(),
            SearchMatchMode::Contains,
            Some("session".into()),
            Some("scope".into()),
            20,
            true,
            true,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        assert_eq!(close_requests.lock().unwrap()[0].session_id, "session");
        {
            let requests = search_requests.lock().unwrap();
            assert_eq!(requests[0].max_results, 20);
            assert!(requests[0].include_branches);
        }

        let host = start_mock_server(MockBridgeService {
            search_events: vec![
                ProtoSearchEvent {
                    event: Some(search_event::Event::Match(SearchMatch {
                        node: Some(ProtoBrowseNode {
                            node_key: "n".into(),
                            display_name: "PV".into(),
                            kind: ProtoBrowseNodeKind::Branch as i32,
                            item_id: None,
                        }),
                        breadcrumbs: vec![BrowseBreadcrumb {
                            node_key: "b".into(),
                            display_name: "FCS".into(),
                        }],
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Progress(SearchProgress {
                        visited_nodes: 5,
                        matches: 1,
                        partial: true,
                    })),
                },
                ProtoSearchEvent {
                    event: Some(search_event::Event::Completed(SearchCompleted {
                        complete: false,
                        cancelled: false,
                        truncated: true,
                        warning: Some("cap reached".into()),
                    })),
                },
            ],
            ..Default::default()
        })
        .await;
        cmd_search(
            host,
            "S".into(),
            "PV".into(),
            SearchMatchMode::Contains,
            None,
            None,
            20,
            false,
            false,
            OutputFormat::Table,
        )
        .await
        .unwrap();
    }

    fn proto_index_status(state: ProtoSearchIndexState) -> SearchIndexStatus {
        SearchIndexStatus {
            server: "S".into(),
            state: state as i32,
            configured: true,
            active_generation: 3,
            entry_count: 101,
            unique_item_count: 100,
            started_at: Some("start".into()),
            completed_at: Some("complete".into()),
            last_error: Some("warning".into()),
            database_bytes: 2048,
            organization: ProtoOrganization::Hierarchical as i32,
            source: ProtoBrowseSource::Da2 as i32,
            effective_limits: Some(opcda_bridge_proto::bridge::IndexInventoryLimits {
                item_rate_per_second: 100,
                batch_size: 25,
                duty_cycle_percent: 5,
            }),
            controller_state: opcda_bridge_proto::bridge::IndexControllerState::Throttled as i32,
            pause_reason: Some(opcda_bridge_proto::bridge::IndexPauseReason::Database as i32),
            recovery_deadline: Some("recover".into()),
            pause_reason_detail: Some("commit latency".into()),
            foreground: Some(opcda_bridge_proto::bridge::IndexForegroundDiagnostics {
                active_count: 1,
                operations: 2,
                errors: 3,
                bad_quality: 4,
                latency_p50_ms: Some(5),
                latency_p95_ms: Some(6),
                latency_max_ms: Some(7),
                last_error: true,
                last_bad_quality: true,
            }),
            host: Some(opcda_bridge_proto::bridge::IndexHostDiagnostics {
                cpu_percent: Some(8.0),
                available_memory_percent: Some(9.0),
                disk_active_percent: Some(10.0),
                disk_queue: Some(11.0),
                process_working_set_bytes: Some(12),
                process_private_bytes: Some(13),
                process_read_bytes_per_second: Some(14),
                process_write_bytes_per_second: Some(15),
                disk_free_bytes: Some(16),
            }),
            storage: Some(opcda_bridge_proto::bridge::IndexStorageDiagnostics {
                main_bytes: 17,
                wal_bytes: 18,
                shm_bytes: 19,
                free_bytes: Some(20),
                last_commit_latency_ms: Some(21),
            }),
            scheduler: Some(opcda_bridge_proto::bridge::IndexSchedulerDiagnostics {
                next_refresh_at: Some("next".into()),
                last_attempt_at: Some("attempt".into()),
                last_success_at: Some("success".into()),
                last_success_duration_ms: Some(22),
                retry_after: Some("retry".into()),
                consecutive_failures: 23,
                circuit_open: true,
            }),
            health: Some(opcda_bridge_proto::bridge::IndexHealthDiagnostics {
                state: opcda_bridge_proto::bridge::IndexHealthState::Healthy as i32,
                sentinel_configured: true,
            }),
            promoting: true,
            progress: Some(IndexedSearchProgress {
                branches_visited: 4,
                entries_seen: 5,
                unique_items: 5,
                active_time_ms: 6,
                paused_time_ms: 7,
                items_per_second: 8.5,
                estimated_remaining_ms: Some(9),
            }),
        }
    }

    #[tokio::test]
    async fn indexed_search_commands_render_and_forward_requests() {
        let service = MockBridgeService {
            search_index_status_response: proto_index_status(ProtoSearchIndexState::Ready),
            refresh_search_index_response: proto_index_status(ProtoSearchIndexState::Refreshing),
            control_search_index_response: proto_index_status(ProtoSearchIndexState::Partial),
            search_index_response: SearchIndexResponse {
                matches: vec![IndexedSearchMatch {
                    item_id: "FCS0201!204FI00510.PV".into(),
                    display_name: "PV".into(),
                    kind: ProtoBrowseNodeKind::Item as i32,
                    breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
                }],
                has_more: true,
                status: Some(proto_index_status(ProtoSearchIndexState::Stale)),
            },
            ..Default::default()
        };
        let status_requests = Arc::clone(&service.search_index_status_requests);
        let refresh_requests = Arc::clone(&service.refresh_search_index_requests);
        let control_requests = Arc::clone(&service.control_search_index_requests);
        let search_requests = Arc::clone(&service.search_index_requests);
        let host = start_mock_server(service).await;

        cmd_index_status(host.clone(), "S".into(), OutputFormat::Table, None)
            .await
            .unwrap();
        cmd_index_refresh(host.clone(), "S".into(), true, OutputFormat::Json)
            .await
            .unwrap();
        cmd_index_control(
            host.clone(),
            "S".into(),
            SearchIndexControlAction::Pause,
            OutputFormat::Table,
        )
        .await
        .unwrap();
        cmd_index_search(
            host.clone(),
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            25,
            OutputFormat::Json,
        )
        .await
        .unwrap();
        cmd_index_search(
            host,
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            25,
            OutputFormat::Table,
        )
        .await
        .unwrap();

        assert_eq!(status_requests.lock().unwrap()[0].server, "S");
        assert!(refresh_requests.lock().unwrap()[0].force);
        assert_eq!(
            control_requests.lock().unwrap()[0].action,
            opcda_bridge_proto::bridge::SearchIndexControlAction::Pause as i32
        );
        let search_requests = search_requests.lock().unwrap();
        assert_eq!(search_requests[0].query, "PV1");
        assert_eq!(search_requests[0].max_results, 25);
    }

    #[test]
    fn indexed_status_rendering_includes_diagnostics_and_failure_labels() {
        let status = opcda_bridge::SearchIndexStatus::try_from(proto_index_status(
            ProtoSearchIndexState::Ready,
        ))
        .unwrap();
        let table = render_index_status(status.clone(), OutputFormat::Table).unwrap();
        for value in [
            "Effective item rate/s",
            "Foreground p95 ms",
            "Host CPU %",
            "SQLite WAL bytes",
            "Next refresh",
            "Health",
            "warning",
        ] {
            assert!(table.contains(value), "missing {value} in {table}");
        }
        let json = render_index_status(status, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["effective_limits"]["batch_size"], 25);
        assert_eq!(value["foreground"]["latency_p95_ms"], 6);
        assert_eq!(value["host"]["disk_free_bytes"], 16);
        assert_eq!(value["storage"]["last_commit_latency_ms"], 21);
        assert_eq!(value["scheduler"]["consecutive_failures"], 23);
        assert_eq!(value["health"]["state"], "healthy");
        assert_eq!(value["promoting"], true);

        let mut failed = proto_index_status(ProtoSearchIndexState::Failed);
        failed.last_error = Some("failure".into());
        let table = render_index_status(failed.try_into().unwrap(), OutputFormat::Table).unwrap();
        assert!(table.contains("Last error"));
        assert!(table.contains("failure"));
    }

    #[test]
    fn watched_status_rendering_preserves_table_and_json_shapes() {
        let status: opcda_bridge::SearchIndexStatus =
            proto_index_status(ProtoSearchIndexState::Ready)
                .try_into()
                .unwrap();
        let table = render_watch_status(status.clone(), OutputFormat::Table).unwrap();
        assert!(matches!(&table, WatchStatusOutput::Table(_)));
        let rendered = match table {
            WatchStatusOutput::Table(rendered) | WatchStatusOutput::Json(rendered) => rendered,
        };
        assert!(rendered.contains("Health"));

        let json = render_watch_status(status, OutputFormat::Json).unwrap();
        assert!(matches!(&json, WatchStatusOutput::Json(_)));
        let rendered = match json {
            WatchStatusOutput::Table(rendered) | WatchStatusOutput::Json(rendered) => rendered,
        };
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["state"], "ready");
    }

    #[tokio::test]
    async fn index_status_watch_renders_and_stops_on_ctrl_c() {
        let service = MockBridgeService {
            search_index_status_response: proto_index_status(ProtoSearchIndexState::Ready),
            ..Default::default()
        };
        let requests = Arc::clone(&service.search_index_status_requests);
        let host = start_mock_server(service).await;
        let mut client = Client::connect(&host).await.unwrap();
        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(())
        });
        watch_index_status(&mut client, "S".into(), OutputFormat::Table, 60, ctrl_c)
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);

        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(())
        });
        watch_index_status(&mut client, "S".into(), OutputFormat::Json, 60, ctrl_c)
            .await
            .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);

        let host = start_mock_server(MockBridgeService::default()).await;
        let mut client = Client::connect(&host).await.unwrap();
        let ctrl_c: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> =
            Box::pin(async { Ok(()) });
        watch_index_status(&mut client, "S".into(), OutputFormat::Json, 60, ctrl_c)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn indexed_search_validates_query_and_limit_before_connecting() {
        for (query, mode, expected) in [
            ("PV", SearchMatchMode::Contains, "at least 3"),
            ("P", SearchMatchMode::Exact, "at least 2"),
            ("P", SearchMatchMode::Prefix, "at least 2"),
            (" ", SearchMatchMode::Contains, "must not be empty"),
        ] {
            let error = cmd_index_search(
                "unused".into(),
                "S".into(),
                query.into(),
                mode,
                50,
                OutputFormat::Table,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
        let error = cmd_index_search(
            "unused".into(),
            "S".into(),
            "PV1".into(),
            SearchMatchMode::Contains,
            0,
            OutputFormat::Table,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn rendering_helpers_include_metadata_and_all_warning_combinations() {
        let typed = BrowsePage {
            session_id: "session".into(),
            nodes: vec![BrowseNode {
                node_key: "key".into(),
                display_name: "Branch".into(),
                kind: BrowseNodeKind::BranchAndItem,
                item_id: Some("Exact.ItemID".into()),
            }],
            next_page_token: Some("next".into()),
            complete: false,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Derived,
            warning: Some("partial".into()),
        };
        let json = render_browse(typed.clone(), 1, OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["session_id"], "session");
        assert_eq!(value["nodes"][0]["item_id"], "Exact.ItemID");
        assert_eq!(value["complete"], false);
        let table = render_browse(typed, 1, OutputFormat::Table).unwrap();
        assert!(table.contains("Next page token: next"));
        assert!(table.contains("Warning: partial"));

        assert_eq!(merge_warnings(None, None), None);
        assert_eq!(merge_warnings(Some("a".into()), None).as_deref(), Some("a"));
        assert_eq!(merge_warnings(None, Some("b".into())).as_deref(), Some("b"));
        assert_eq!(
            merge_warnings(Some("a".into()), Some("b".into())).as_deref(),
            Some("a; b")
        );

        let oversized = BrowsePage {
            nodes: vec![
                BrowseNode {
                    node_key: "1".into(),
                    display_name: "1".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: Some("1".into()),
                },
                BrowseNode {
                    node_key: "2".into(),
                    display_name: "2".into(),
                    kind: BrowseNodeKind::Item,
                    item_id: Some("2".into()),
                },
            ],
            ..typed_page()
        };
        assert!(ensure_page_bound(&oversized, 1).is_err());

        let empty_incomplete = BrowsePage {
            complete: false,
            next_page_token: Some("next".into()),
            ..typed_page()
        };
        assert!(ensure_page_bound(&empty_incomplete, 1).is_err());
    }

    #[test]
    fn search_event_json_is_tagged() {
        let event = search_output_event(SearchEvent::Progress(opcda_bridge::SearchProgress {
            visited_nodes: 3,
            matches: 1,
            partial: true,
        }));
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["event"], "progress");
        assert_eq!(value["visited_nodes"], 3);
    }

    #[test]
    fn search_completion_messages_only_render_table_diagnostics() {
        let table =
            search_completion_messages(OutputFormat::Table, true, false, true, Some("cap reached"));
        assert_eq!(
            table,
            [
                "Search complete: complete=true, cancelled=false, truncated=true",
                "Warning: cap reached",
            ]
        );
        assert!(
            search_completion_messages(OutputFormat::Json, true, false, true, Some("cap reached"),)
                .is_empty()
        );
    }

    #[test]
    fn indexed_search_rendering_exposes_status_without_node_keys() {
        let status = opcda_bridge::SearchIndexStatus {
            server: "S".into(),
            state: SearchIndexState::Ready,
            auto_refresh_enabled: true,
            active_generation: 2,
            entry_count: 1,
            unique_item_count: 1,
            started_at: None,
            completed_at: Some("done".into()),
            last_error: None,
            database_bytes: 512,
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            progress: None,
            effective_limits: None,
            controller_state: opcda_bridge::IndexControllerState::Unspecified,
            pause_reason: None,
            pause_reason_detail: None,
            recovery_deadline: None,
            foreground: Default::default(),
            host: Default::default(),
            storage: Default::default(),
            scheduler: Default::default(),
            health: Default::default(),
            promoting: false,
        };
        let response = opcda_bridge::SearchIndexResponse {
            matches: vec![opcda_bridge::IndexedSearchMatch {
                item_id: "Exact.ItemID".into(),
                display_name: "Tag".into(),
                kind: BrowseNodeKind::Item,
                breadcrumbs: vec!["Area".into()],
            }],
            has_more: false,
            status: status.clone(),
        };
        let json = render_indexed_search(response.clone(), OutputFormat::Json).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["matches"][0]["item_id"], "Exact.ItemID");
        assert_eq!(
            value["matches"][0]["breadcrumbs"],
            serde_json::json!(["Area"])
        );
        assert!(value["matches"][0].get("node_key").is_none());
        assert_eq!(value["status"]["state"], "ready");
        let table = render_indexed_search(response, OutputFormat::Table).unwrap();
        assert!(table.contains("Exact.ItemID"));
        assert!(table.contains("Has more: false"));
        assert!(
            render_index_status(status, OutputFormat::Json)
                .unwrap()
                .contains("\"progress\": null")
        );
    }

    fn typed_page() -> BrowsePage {
        BrowsePage {
            session_id: "session".into(),
            nodes: Vec::new(),
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        }
    }
}
