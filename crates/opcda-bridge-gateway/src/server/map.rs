use crate::browse::MAX_PAGE_SIZE;
use crate::index::{IndexOperationError, IndexState, IndexStatus};
use crate::opc::{
    BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, BrowseSource, InventoryProgress,
    NamespaceOrganization, OpcValue, TagValue, WriteResult,
};
use opcda_bridge_proto::bridge::{
    BrowseNode as ProtoBrowseNode, BrowsePage as ProtoBrowsePage,
    BrowseSource as ProtoBrowseSource, GetCapabilitiesResponse, GetGatewayInfoResponse,
    IndexControllerState, IndexForegroundDiagnostics, IndexHealthDiagnostics, IndexHealthState,
    IndexHostDiagnostics, IndexInventoryLimits, IndexPauseReason, IndexSchedulerDiagnostics,
    IndexStorageDiagnostics, IndexedSearchMatch, IndexedSearchProgress,
    NamespaceOrganization as ProtoNamespaceOrganization, ProtocolFeature, ProtocolFeatureKind,
    SearchIndexState, SearchIndexStatus, TagValue as ProtoTagValue, WriteResponse,
    write_request::TypedValue as ProtoTypedValue,
};
use tonic::Status;

pub(super) fn internal(error: impl std::fmt::Display) -> Status {
    let message = error.to_string();
    tracing::error!(error = %message, "OPC operation failed");
    Status::internal(message)
}

pub(super) fn index_error(error: IndexOperationError) -> Status {
    match error {
        IndexOperationError::UnknownServer { server } => {
            Status::invalid_argument(format!("OPC DA server {server:?} is not registered"))
        }
        IndexOperationError::NotEnrolled { server } => Status::not_found(format!(
            "namespace index for OPC DA server {server:?} is not enrolled"
        )),
        IndexOperationError::Deleting { server } => Status::failed_precondition(format!(
            "namespace index for OPC DA server {server:?} is being deleted"
        )),
        IndexOperationError::Internal(error) => internal(error),
    }
}

pub(super) fn resolve_host(host: &str) -> &str {
    if host.is_empty() { "localhost" } else { host }
}

pub(super) fn map_namespace_organization(
    value: NamespaceOrganization,
) -> ProtoNamespaceOrganization {
    match value {
        NamespaceOrganization::Unspecified => ProtoNamespaceOrganization::Unspecified,
        NamespaceOrganization::Flat => ProtoNamespaceOrganization::Flat,
        NamespaceOrganization::Hierarchical => ProtoNamespaceOrganization::Hierarchical,
    }
}

pub(super) fn map_browse_source(value: BrowseSource) -> ProtoBrowseSource {
    match value {
        BrowseSource::Unspecified => ProtoBrowseSource::Unspecified,
        BrowseSource::Da3 => ProtoBrowseSource::Da3,
        BrowseSource::Da2 => ProtoBrowseSource::Da2,
        BrowseSource::Flat => ProtoBrowseSource::Flat,
        BrowseSource::Derived => ProtoBrowseSource::Derived,
    }
}

pub(super) fn map_browse_node(node: BrowseNode) -> ProtoBrowseNode {
    let item_id = match node.kind {
        BrowseNodeKind::Branch => None,
        BrowseNodeKind::Item | BrowseNodeKind::BranchAndItem => node.item_id,
    };
    ProtoBrowseNode {
        node_key: node.node_key,
        display_name: node.display_name,
        kind: match node.kind {
            BrowseNodeKind::Branch => opcda_bridge_proto::bridge::BrowseNodeKind::Branch,
            BrowseNodeKind::Item => opcda_bridge_proto::bridge::BrowseNodeKind::Item,
            BrowseNodeKind::BranchAndItem => {
                opcda_bridge_proto::bridge::BrowseNodeKind::BranchAndItem
            }
        } as i32,
        item_id,
    }
}

pub(super) fn map_browse_page(session_id: String, page: BrowsePage) -> ProtoBrowsePage {
    ProtoBrowsePage {
        session_id,
        nodes: page.nodes.into_iter().map(map_browse_node).collect(),
        next_page_token: page.next_page_token,
        complete: page.complete,
        organization: map_namespace_organization(page.organization) as i32,
        source: map_browse_source(page.source) as i32,
        warning: page.warning,
    }
}

pub(super) fn gateway_release_line() -> &'static opcda_bridge_proto::compatibility::ReleaseLine {
    opcda_bridge_proto::compatibility::release_line_for(env!("CARGO_PKG_VERSION"))
        .expect("gateway package version must be in the compatibility catalog")
}

pub(super) fn map_capabilities(
    capabilities: BrowseCapabilities,
    index_status: &IndexStatus,
    max_indexed_search_results: u32,
) -> GetCapabilitiesResponse {
    let release_line = gateway_release_line();
    map_capabilities_for_release_line(
        capabilities,
        index_status,
        max_indexed_search_results,
        release_line,
    )
}

pub(super) fn map_capabilities_for_release_line(
    capabilities: BrowseCapabilities,
    index_status: &IndexStatus,
    max_indexed_search_results: u32,
    release_line: &opcda_bridge_proto::compatibility::ReleaseLine,
) -> GetCapabilitiesResponse {
    GetCapabilitiesResponse {
        application_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: release_line.namespace_protocol.to_string(),
        max_page_size: capabilities.max_page_size.min(MAX_PAGE_SIZE),
        supports_browse_sessions: capabilities.supports_browse_sessions,
        supports_search: capabilities.supports_search,
        organization: map_namespace_organization(capabilities.organization) as i32,
        source: map_browse_source(capabilities.source) as i32,
        supports_indexed_search: release_line.indexed_search_protocol > 0,
        indexed_search_protocol_version: if release_line.indexed_search_protocol == 0 {
            String::new()
        } else {
            release_line.indexed_search_protocol.to_string()
        },
        max_indexed_search_results,
        search_index_state: map_index_state(index_status.state) as i32,
        search_index_promoting: is_promoting_state(index_status.state),
    }
}

pub(super) fn gateway_info() -> GetGatewayInfoResponse {
    let release_line = gateway_release_line();
    GetGatewayInfoResponse {
        application_version: env!("CARGO_PKG_VERSION").to_string(),
        compatibility_schema_version: opcda_bridge_proto::compatibility::SCHEMA_VERSION,
        features: vec![
            ProtocolFeature {
                kind: ProtocolFeatureKind::Core as i32,
                min_version: release_line.core_protocol,
                max_version: release_line.core_protocol,
            },
            ProtocolFeature {
                kind: ProtocolFeatureKind::Namespace as i32,
                min_version: release_line.namespace_protocol,
                max_version: release_line.namespace_protocol,
            },
            ProtocolFeature {
                kind: ProtocolFeatureKind::IndexedSearch as i32,
                min_version: release_line.indexed_search_protocol,
                max_version: release_line.indexed_search_protocol,
            },
        ],
    }
}

pub(super) fn map_index_state(state: IndexState) -> SearchIndexState {
    match state {
        IndexState::NotIndexed => SearchIndexState::NotIndexed,
        IndexState::Partial => SearchIndexState::Partial,
        IndexState::Ready => SearchIndexState::Ready,
        IndexState::Stale => SearchIndexState::Stale,
        IndexState::Refreshing => SearchIndexState::Refreshing,
        IndexState::Promoting => SearchIndexState::Refreshing,
        IndexState::Failed => SearchIndexState::Failed,
        IndexState::Deleting => SearchIndexState::Deleting,
    }
}

pub(super) fn is_promoting_state(state: IndexState) -> bool {
    matches!(state, IndexState::Promoting)
}

pub(super) fn map_inventory_progress(progress: InventoryProgress) -> IndexedSearchProgress {
    IndexedSearchProgress {
        branches_visited: progress.branches_visited,
        entries_seen: progress.entries_seen,
        unique_items: progress.unique_items,
        active_time_ms: progress.active_time_ms,
        paused_time_ms: progress.paused_time_ms,
        items_per_second: progress.items_per_second,
        estimated_remaining_ms: progress.estimated_remaining_ms,
    }
}

pub(super) fn map_index_status(status: IndexStatus) -> SearchIndexStatus {
    let controller_state = match status.controller_state {
        None => IndexControllerState::Unspecified,
        Some(crate::controller::ControllerState::Ramping) => IndexControllerState::Ramping,
        Some(crate::controller::ControllerState::Steady) => IndexControllerState::Steady,
        Some(crate::controller::ControllerState::Throttled) => IndexControllerState::Throttled,
        Some(crate::controller::ControllerState::Paused(_)) => IndexControllerState::Paused,
    };
    let pause_reason = status.pause_reason.and_then(|reason| match reason {
        crate::controller::PauseReason::Foreground => Some(IndexPauseReason::Foreground),
        crate::controller::PauseReason::OpcHealth => Some(IndexPauseReason::OpcHealth),
        crate::controller::PauseReason::HostCpu => Some(IndexPauseReason::HostCpu),
        crate::controller::PauseReason::Memory => Some(IndexPauseReason::Memory),
        crate::controller::PauseReason::Disk => Some(IndexPauseReason::Disk),
        crate::controller::PauseReason::Database => Some(IndexPauseReason::Database),
        crate::controller::PauseReason::Operator => Some(IndexPauseReason::Operator),
        crate::controller::PauseReason::Circuit => Some(IndexPauseReason::Circuit),
        crate::controller::PauseReason::Maintenance => None,
    });
    let pause_reason_detail = status
        .pause_reason
        .map(|reason| reason.as_str().to_string());
    let health_state = match status.health {
        crate::index::HealthProbeState::Unavailable => IndexHealthState::Unavailable,
        crate::index::HealthProbeState::Healthy => IndexHealthState::Healthy,
        crate::index::HealthProbeState::Unhealthy => IndexHealthState::Unhealthy,
    };
    SearchIndexStatus {
        server: status.server,
        state: map_index_state(status.state) as i32,
        configured: status.auto_refresh_enabled,
        active_generation: status.active_generation,
        entry_count: status.entry_count,
        unique_item_count: status.unique_item_count,
        started_at: status.started_at,
        completed_at: status.completed_at,
        last_error: status.last_error,
        database_bytes: status.database_bytes,
        organization: map_namespace_organization(status.organization) as i32,
        source: map_browse_source(status.source) as i32,
        progress: status.progress.map(map_inventory_progress),
        effective_limits: status.effective_limits.map(|limits| IndexInventoryLimits {
            item_rate_per_second: limits.item_rate_per_second,
            batch_size: limits.batch_size,
            duty_cycle_percent: u32::from(limits.duty_cycle_percent),
        }),
        controller_state: controller_state as i32,
        pause_reason: pause_reason.map(|reason| reason as i32),
        recovery_deadline: status.recovery_deadline,
        pause_reason_detail,
        foreground: Some(IndexForegroundDiagnostics {
            active_count: status.foreground_metrics.active_count,
            operations: status.foreground_metrics.operations,
            errors: status.foreground_metrics.errors,
            bad_quality: status.foreground_metrics.bad_quality,
            latency_p50_ms: status.foreground_metrics.latency_p50_ms,
            latency_p95_ms: status.foreground_metrics.latency_p95_ms,
            latency_max_ms: status.foreground_metrics.latency_max_ms,
            last_error: status.foreground_metrics.last_error,
            last_bad_quality: status.foreground_metrics.last_bad_quality,
        }),
        host: Some(IndexHostDiagnostics {
            cpu_percent: status.host_metrics.cpu_percent,
            available_memory_percent: status.host_metrics.available_memory_percent,
            disk_active_percent: status.host_metrics.disk_active_percent,
            disk_queue: status.host_metrics.disk_queue,
            process_working_set_bytes: status.host_metrics.process_working_set_bytes,
            process_private_bytes: status.host_metrics.process_private_bytes,
            process_read_bytes_per_second: status.host_metrics.process_read_bytes_per_second,
            process_write_bytes_per_second: status.host_metrics.process_write_bytes_per_second,
            disk_free_bytes: status.host_metrics.disk_free_bytes,
        }),
        storage: Some(IndexStorageDiagnostics {
            main_bytes: status.storage.main_bytes,
            wal_bytes: status.storage.wal_bytes,
            shm_bytes: status.storage.shm_bytes,
            free_bytes: status.storage.free_bytes,
            last_commit_latency_ms: status.storage.last_commit_latency_ms,
        }),
        scheduler: Some(IndexSchedulerDiagnostics {
            next_refresh_at: status.scheduler.next_refresh_at,
            last_attempt_at: status.scheduler.last_attempt_at,
            last_success_at: status.scheduler.last_success_at,
            last_success_duration_ms: status.scheduler.last_success_duration_ms,
            retry_after: status.scheduler.retry_after,
            consecutive_failures: status.scheduler.consecutive_failures,
            circuit_open: status.scheduler.circuit_open,
        }),
        health: Some(IndexHealthDiagnostics {
            state: health_state as i32,
            sentinel_configured: status.sentinel_configured,
        }),
        promoting: is_promoting_state(status.state),
    }
}

pub(super) fn map_index_match(value: crate::index::IndexedMatch) -> IndexedSearchMatch {
    IndexedSearchMatch {
        item_id: value.item_id,
        display_name: value.display_name,
        kind: match value.kind {
            crate::opc::InventoryNodeKind::Item => opcda_bridge_proto::bridge::BrowseNodeKind::Item,
            crate::opc::InventoryNodeKind::BranchAndItem => {
                opcda_bridge_proto::bridge::BrowseNodeKind::BranchAndItem
            }
        } as i32,
        breadcrumbs: value.breadcrumbs,
    }
}

pub(super) fn map_to_proto_tag_values(values: Vec<TagValue>) -> Vec<ProtoTagValue> {
    values
        .into_iter()
        .map(|value| ProtoTagValue {
            tag_id: value.tag_id,
            value: value.value,
            quality: value.quality,
            timestamp: value.timestamp,
        })
        .collect()
}

pub(super) fn typed_value_to_opc_value(
    typed_value: Option<ProtoTypedValue>,
) -> Result<OpcValue, Status> {
    let typed_value =
        typed_value.ok_or_else(|| Status::invalid_argument("no typed_value provided"))?;
    Ok(match typed_value {
        ProtoTypedValue::StringValue(value) => OpcValue::String(value),
        ProtoTypedValue::IntValue(value) => OpcValue::Int(value),
        ProtoTypedValue::FloatValue(value) => OpcValue::Float(value),
        ProtoTypedValue::BoolValue(value) => OpcValue::Bool(value),
    })
}

pub(super) fn map_to_write_response(result: WriteResult) -> WriteResponse {
    WriteResponse {
        tag_id: result.tag_id,
        success: result.success,
        error: result.error,
    }
}
