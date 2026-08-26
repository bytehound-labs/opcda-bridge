use opc_da_client::{
    BrowseNamespace, BrowseNodeFilter, BrowseNodeKind as ExtBrowseNodeKind, BrowseNodeToken,
    BrowsePageRequest, BrowsePageToken, BrowseSessionToken,
    InventoryControl as ExtInventoryControl, InventoryEvent as ExtInventoryEvent, InventoryOptions,
    InventoryPacing as ExtInventoryPacing, InventorySliceBackend as ExtInventorySliceBackend,
    InventoryStream as ExtInventoryStream, OpcDaClient, OpcProvider, OpcValue as ExtOpcValue,
    TagValue as ExtTagValue, WriteResult as ExtWriteResult,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::opc::{
    BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, BrowseSource, InventoryCompleted,
    InventoryControl, InventoryEntry, InventoryEvent, InventoryHandle, InventoryNodeKind,
    InventoryPacing, InventoryProgress, InventorySliceBackend, InventorySliceObservation,
    InventoryStream, MAX_NATIVE_INVENTORY_BATCH_SIZE, NamespaceOrganization, OpcClient, OpcValue,
    TagValue, WriteResult,
};

#[derive(Default)]
pub struct OpcDaAdapter {
    client: OpcDaClient,
    sessions: Mutex<HashMap<String, (NamespaceOrganization, BrowseSource)>>,
}

#[async_trait::async_trait]
impl OpcClient for OpcDaAdapter {
    async fn list_servers(&self, host: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.client.list_servers(host).await?)
    }

    async fn get_capabilities(&self, server: &str) -> anyhow::Result<BrowseCapabilities> {
        let capabilities = self.client.browse_capabilities(server).await?;
        let (organization, source) = map_capabilities(&capabilities);
        Ok(BrowseCapabilities {
            organization,
            source,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: capabilities.max_page_size,
        })
    }

    async fn open_browse_session(&self, server: &str) -> anyhow::Result<String> {
        let capabilities = self.client.browse_capabilities(server).await?;
        let (organization, source) = map_capabilities(&capabilities);
        let session = self.client.open_browse_session(server).await?.to_string();
        self.sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("browse session lock poisoned"))?
            .insert(session.clone(), (organization, source));
        Ok(session)
    }

    async fn browse_page(
        &self,
        session_id: &str,
        parent_node_key: Option<&str>,
        page_token: Option<&str>,
        page_size: u32,
        _refresh: bool,
    ) -> anyhow::Result<BrowsePage> {
        let session = parse_token::<BrowseSessionToken>(session_id, "browse session")?;
        let parent = parent_node_key
            .map(|value| parse_token::<BrowseNodeToken>(value, "browse node"))
            .transpose()?;
        let continuation = page_token
            .map(|value| parse_token::<BrowsePageToken>(value, "browse page"))
            .transpose()?;
        let page = self
            .client
            .browse_page(
                &session,
                BrowsePageRequest {
                    parent,
                    filter: BrowseNodeFilter::All,
                    max_elements: page_size,
                    continuation,
                },
            )
            .await?;
        let (organization, source) = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("browse session lock poisoned"))?
            .get(session_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown browse session"))?;

        Ok(BrowsePage {
            nodes: page
                .nodes
                .into_iter()
                .map(|node| BrowseNode {
                    node_key: node.token.to_string(),
                    display_name: node.name,
                    kind: map_node_kind(node.kind),
                    item_id: node.item_id,
                })
                .collect(),
            next_page_token: page.continuation.map(|token| token.to_string()),
            complete: page.continuation.is_none(),
            organization,
            source,
            warning: None,
        })
    }

    async fn close_browse_session(&self, session_id: &str) -> anyhow::Result<()> {
        let session = parse_token::<BrowseSessionToken>(session_id, "browse session")?;
        let result = self.client.close_browse_session(&session).await;
        self.sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("browse session lock poisoned"))?
            .remove(session_id);
        Ok(result?)
    }

    async fn start_inventory(
        &self,
        server: &str,
        batch_size: u32,
    ) -> anyhow::Result<InventoryHandle> {
        if !(1..=MAX_NATIVE_INVENTORY_BATCH_SIZE).contains(&batch_size) {
            anyhow::bail!(
                "native inventory batch size must be between 1 and {}",
                MAX_NATIVE_INVENTORY_BATCH_SIZE
            );
        }
        let stream = self
            .client
            .start_inventory(
                server,
                InventoryOptions {
                    batch_size,
                    max_entries: None,
                },
            )
            .await?;
        let control = stream.control();
        Ok(InventoryHandle {
            stream: Box::new(AdapterInventoryStream { inner: stream }),
            control: Arc::new(AdapterInventoryControl { inner: control }),
        })
    }

    async fn read_tag_values(
        &self,
        server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<TagValue>> {
        let values = self.client.read_tag_values(server, tag_ids).await?;
        Ok(values.into_iter().map(map_tag_value).collect())
    }

    async fn write_tag_value(
        &self,
        server: &str,
        tag_id: &str,
        value: OpcValue,
    ) -> anyhow::Result<WriteResult> {
        let result = self
            .client
            .write_tag_value(server, tag_id, map_value(value))
            .await?;
        Ok(map_write_result(result))
    }
}

struct AdapterInventoryControl {
    inner: ExtInventoryControl,
}

impl InventoryControl for AdapterInventoryControl {
    fn pause(&self) {
        self.inner.pause();
    }

    fn resume(&self) {
        self.inner.resume();
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn set_pacing(&self, pacing: InventoryPacing) -> anyhow::Result<()> {
        self.apply_pacing(pacing)
    }

    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

impl AdapterInventoryControl {
    fn apply_pacing(&self, pacing: InventoryPacing) -> anyhow::Result<()> {
        self.inner.set_pacing(ExtInventoryPacing {
            min_interval: pacing.min_interval,
        });
        if let Some(batch_size) = pacing.batch_size {
            self.inner.set_batch_size(batch_size).map_err(|error| {
                anyhow::anyhow!("native inventory batch update failed: {error}")
            })?;
        }
        Ok(())
    }
}

struct AdapterInventoryStream {
    inner: ExtInventoryStream,
}

#[async_trait::async_trait]
impl InventoryStream for AdapterInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        self.inner
            .message()
            .await
            .map(|result| result.map(map_inventory_event).map_err(anyhow::Error::from))
    }
}

fn map_inventory_event(event: ExtInventoryEvent) -> InventoryEvent {
    match event {
        ExtInventoryEvent::Entry(entry) => InventoryEvent::Entry(InventoryEntry {
            display_name: entry.display_name,
            item_id: entry.item_id,
            kind: match entry.kind {
                ExtBrowseNodeKind::Item => InventoryNodeKind::Item,
                ExtBrowseNodeKind::BranchAndItem => InventoryNodeKind::BranchAndItem,
                ExtBrowseNodeKind::Branch => InventoryNodeKind::Item,
            },
            breadcrumbs: entry.breadcrumbs,
        }),
        ExtInventoryEvent::Progress(progress) => InventoryEvent::Progress(InventoryProgress {
            branches_visited: progress.branches_visited,
            entries_seen: progress.entries_seen,
            unique_items: progress.unique_items,
            active_time_ms: progress.active_time_ms,
            paused_time_ms: progress.paused_time_ms,
            items_per_second: progress.items_per_second,
            estimated_remaining_ms: progress.estimated_remaining_ms,
        }),
        ExtInventoryEvent::Slice(slice) => InventoryEvent::Slice(InventorySliceObservation {
            sequence: slice.sequence,
            backend: match slice.backend {
                ExtInventorySliceBackend::Da3 => InventorySliceBackend::Da3,
                ExtInventorySliceBackend::Da2 => InventorySliceBackend::Da2,
            },
            nodes_returned: slice.nodes_returned,
            has_more: slice.has_more,
            native_operations: slice.native_operations,
            elapsed_ms: slice.elapsed_ms,
            entries_seen: slice.entries_seen,
            unique_items: slice.unique_items,
        }),
        ExtInventoryEvent::Completed(completed) => {
            let (organization, source) = map_capabilities(&completed.capabilities);
            InventoryEvent::Completed(InventoryCompleted {
                complete: completed.complete,
                cancelled: completed.cancelled,
                truncated: completed.truncated,
                warning: completed.warning,
                organization,
                source,
            })
        }
    }
}

fn parse_token<T>(value: &str, label: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {label} token: {error}"))
}

fn map_capabilities(
    capabilities: &opc_da_client::BrowseCapabilities,
) -> (NamespaceOrganization, BrowseSource) {
    let organization = match capabilities.namespace {
        BrowseNamespace::Flat => NamespaceOrganization::Flat,
        BrowseNamespace::Hierarchical => NamespaceOrganization::Hierarchical,
        BrowseNamespace::Unknown => NamespaceOrganization::Unspecified,
    };
    let source = match capabilities.namespace {
        BrowseNamespace::Flat => BrowseSource::Flat,
        BrowseNamespace::Hierarchical | BrowseNamespace::Unknown => {
            if capabilities.supports_da3 {
                BrowseSource::Da3
            } else if capabilities.supports_da2 {
                BrowseSource::Da2
            } else {
                BrowseSource::Derived
            }
        }
    };
    (organization, source)
}

fn map_node_kind(kind: ExtBrowseNodeKind) -> BrowseNodeKind {
    match kind {
        ExtBrowseNodeKind::Branch => BrowseNodeKind::Branch,
        ExtBrowseNodeKind::Item => BrowseNodeKind::Item,
        ExtBrowseNodeKind::BranchAndItem => BrowseNodeKind::BranchAndItem,
    }
}

fn map_tag_value(value: ExtTagValue) -> TagValue {
    TagValue {
        tag_id: value.tag_id,
        value: value.value,
        quality: value.quality,
        timestamp: value.timestamp,
    }
}

fn map_write_result(result: ExtWriteResult) -> WriteResult {
    WriteResult {
        tag_id: result.tag_id,
        success: result.success,
        error: result.error,
    }
}

fn map_value(value: OpcValue) -> ExtOpcValue {
    match value {
        OpcValue::String(value) => ExtOpcValue::String(value),
        OpcValue::Int(value) => ExtOpcValue::Int(value),
        OpcValue::Float(value) => ExtOpcValue::Float(value),
        OpcValue::Bool(value) => ExtOpcValue::Bool(value),
    }
}
