use opc_da_client::{
    BrowseNamespace, BrowseNodeFilter, BrowseNodeKind as ExtBrowseNodeKind, BrowseNodeToken,
    BrowsePageRequest, BrowsePageToken, BrowseSessionToken, OpcDaClient, OpcProvider,
    OpcValue as ExtOpcValue, TagValue as ExtTagValue, WriteResult as ExtWriteResult,
};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::opc::{
    BrowseCapabilities, BrowseNode, BrowseNodeKind, BrowsePage, BrowseSource,
    NamespaceOrganization, OpcClient, OpcValue, TagValue, WriteResult,
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
