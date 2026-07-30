use crate::opc::{OpcClient, OpcValue, TagValue, WriteResult};
use opc_da_client::{OpcDaWrapper, OpcProvider, OpcValue as ExtOpcValue};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

pub struct OpcDaAdapter(pub OpcDaWrapper);

impl Default for OpcDaAdapter {
    fn default() -> Self {
        Self(OpcDaWrapper::default())
    }
}

#[async_trait::async_trait]
impl OpcClient for OpcDaAdapter {
    async fn list_servers(&self, host: &str) -> anyhow::Result<Vec<String>> {
        self.0.list_servers(host).await
    }

    async fn browse_tags(
        &self,
        server: &str,
        max_tags: usize,
        progress: Arc<AtomicUsize>,
        tags_sink: Arc<Mutex<Vec<String>>>,
    ) -> anyhow::Result<Vec<String>> {
        self.0
            .browse_tags(server, max_tags, progress, tags_sink)
            .await
    }

    async fn read_tag_values(
        &self,
        server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<TagValue>> {
        let values = self.0.read_tag_values(server, tag_ids).await?;
        Ok(values
            .into_iter()
            .map(|v| TagValue {
                tag_id: v.tag_id,
                value: v.value,
                quality: v.quality,
                timestamp: v.timestamp,
            })
            .collect())
    }

    async fn write_tag_value(
        &self,
        server: &str,
        tag_id: &str,
        value: OpcValue,
    ) -> anyhow::Result<WriteResult> {
        let ext_value = match value {
            OpcValue::String(s) => ExtOpcValue::String(s),
            OpcValue::Int(i) => ExtOpcValue::Int(i),
            OpcValue::Float(f) => ExtOpcValue::Float(f),
            OpcValue::Bool(b) => ExtOpcValue::Bool(b),
        };
        let result = self.0.write_tag_value(server, tag_id, ext_value).await?;
        Ok(WriteResult {
            tag_id: result.tag_id,
            success: result.success,
            error: result.error,
        })
    }
}
