use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagValue {
    pub tag_id: String,
    pub value: String,
    pub quality: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResult {
    pub tag_id: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpcValue {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

#[async_trait::async_trait]
pub trait OpcClient: Send + Sync + 'static {
    async fn list_servers(&self, host: &str) -> anyhow::Result<Vec<String>>;
    async fn browse_tags(
        &self,
        server: &str,
        max_tags: usize,
        progress: Arc<AtomicUsize>,
        tags_sink: Arc<Mutex<Vec<String>>>,
    ) -> anyhow::Result<Vec<String>>;
    async fn read_tag_values(
        &self,
        server: &str,
        tag_ids: Vec<String>,
    ) -> anyhow::Result<Vec<TagValue>>;
    async fn write_tag_value(
        &self,
        server: &str,
        tag_id: &str,
        value: OpcValue,
    ) -> anyhow::Result<WriteResult>;
}
