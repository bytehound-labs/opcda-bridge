//! Gateway-owned browse sessions, cursors, and bounded page caching.

use crate::opc::{BrowseNode, BrowsePage, OpcClient};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::Status;
use uuid::Uuid;

pub const DEFAULT_PAGE_SIZE: u32 = 200;
pub const MAX_PAGE_SIZE: u32 = 1_000;
pub const MAX_ACTIVE_SESSIONS: usize = 128;
pub const SESSION_TTL: Duration = Duration::from_secs(60);
const MAX_CACHED_PAGES_PER_SESSION: usize = 256;
const MAX_NODE_BINDINGS_PER_SESSION: usize = 100_000;
const MAX_PAGE_BINDINGS_PER_SESSION: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PageKey {
    parent_node_key: Option<String>,
    page_token: Option<String>,
    page_size: u32,
}

#[derive(Debug, Clone)]
struct PageBinding {
    native_page_token: String,
    parent_node_key: Option<String>,
    page_size: u32,
}

struct Session {
    server: String,
    native_session_id: String,
    last_used: Instant,
    operation_lock: Arc<tokio::sync::Mutex<()>>,
    node_bindings: HashMap<String, String>,
    page_bindings: HashMap<String, PageBinding>,
    page_binding_order: VecDeque<String>,
    pages: HashMap<PageKey, BrowsePage>,
    page_order: VecDeque<PageKey>,
}

impl Session {
    fn new(server: String, native_session_id: String) -> Self {
        Self {
            server,
            native_session_id,
            last_used: Instant::now(),
            operation_lock: Arc::new(tokio::sync::Mutex::new(())),
            node_bindings: HashMap::new(),
            page_bindings: HashMap::new(),
            page_binding_order: VecDeque::new(),
            pages: HashMap::new(),
            page_order: VecDeque::new(),
        }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    fn cache_page(&mut self, key: PageKey, page: BrowsePage) {
        if self.pages.contains_key(&key) {
            self.page_order.retain(|existing| existing != &key);
        }
        self.pages.insert(key.clone(), page);
        self.page_order.push_back(key);
        while self.page_order.len() > MAX_CACHED_PAGES_PER_SESSION {
            if let Some(oldest) = self.page_order.pop_front() {
                self.pages.remove(&oldest);
            }
        }
    }

    fn invalidate_parent(&mut self, parent_node_key: &Option<String>) {
        self.pages
            .retain(|key, _| &key.parent_node_key != parent_node_key);
        self.page_order
            .retain(|key| &key.parent_node_key != parent_node_key);
        self.page_bindings
            .retain(|_, binding| &binding.parent_node_key != parent_node_key);
        self.page_binding_order
            .retain(|token| self.page_bindings.contains_key(token));
    }

    fn bind_page(&mut self, token: String, binding: PageBinding) {
        self.page_bindings.insert(token.clone(), binding);
        self.page_binding_order.push_back(token);
        while self.page_binding_order.len() > MAX_PAGE_BINDINGS_PER_SESSION {
            if let Some(oldest) = self.page_binding_order.pop_front() {
                self.page_bindings.remove(&oldest);
            }
        }
    }
}

/// Owns gateway-facing sessions and translates their opaque identifiers into
/// the native session/cursor identifiers used by the OPC client.
pub struct BrowseManager<C: OpcClient> {
    client: Arc<C>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl<C: OpcClient> BrowseManager<C> {
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn open_session(&self, server: &str) -> Result<String, Status> {
        let expired = self.remove_expired();
        self.close_expired(expired).await;

        {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            if sessions.len() >= MAX_ACTIVE_SESSIONS {
                return Err(Status::resource_exhausted(
                    "maximum active browse sessions reached",
                ));
            }
        }

        let native_session_id = self
            .client
            .open_browse_session(server)
            .await
            .map_err(operation_error)?;
        let session_id = new_token();
        let session = Session::new(server.to_string(), native_session_id);
        let capacity_race = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            if sessions.len() >= MAX_ACTIVE_SESSIONS {
                Some(session.native_session_id.clone())
            } else {
                sessions.insert(session_id.clone(), session);
                None
            }
        };
        if let Some(native_session_id) = capacity_race {
            if let Err(error) = self.client.close_browse_session(&native_session_id).await {
                tracing::warn!(
                    server = %server,
                    error = %error,
                    "failed to close browse session after capacity race"
                );
            }
            return Err(Status::resource_exhausted(
                "maximum active browse sessions reached",
            ));
        }
        Ok(session_id)
    }

    pub async fn browse(
        &self,
        server: &str,
        session_id: Option<&str>,
        parent_node_key: Option<&str>,
        page_token: Option<&str>,
        page_size: u32,
        refresh: bool,
    ) -> Result<(String, BrowsePage), Status> {
        let expired = self.remove_expired();
        self.close_expired(expired).await;
        let page_size = normalize_page_size(page_size)?;
        if refresh && page_token.is_some() {
            return Err(Status::invalid_argument(
                "refresh cannot be combined with a browse page token",
            ));
        }
        let temporary_session = session_id.is_none();
        let session_id = match session_id {
            Some(session_id) => session_id.to_string(),
            None => self.open_session(server).await?,
        };
        let result = self
            .browse_existing(
                server,
                &session_id,
                parent_node_key,
                page_token,
                page_size,
                refresh,
            )
            .await;
        if temporary_session
            && result.is_err()
            && let Err(error) = self.close_session(&session_id).await
        {
            tracing::debug!(
                session = %session_id,
                error = %error,
                "failed to close temporary browse session after error"
            );
        }
        result.map(|page| (session_id, page))
    }

    async fn browse_existing(
        &self,
        server: &str,
        session_id: &str,
        parent_node_key: Option<&str>,
        page_token: Option<&str>,
        page_size: u32,
        refresh: bool,
    ) -> Result<BrowsePage, Status> {
        let page_key = PageKey {
            parent_node_key: parent_node_key.map(str::to_string),
            page_token: page_token.map(str::to_string),
            page_size,
        };

        let (native_session_id, native_parent_key, native_page_token, operation_lock, cached_page) = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| Status::not_found("browse session not found or expired"))?;
            if session.server != server {
                return Err(Status::failed_precondition(
                    "browse session belongs to a different OPC server",
                ));
            }
            session.touch();

            let native_parent_key = parent_node_key
                .map(|key| {
                    session
                        .node_bindings
                        .get(key)
                        .cloned()
                        .ok_or_else(|| Status::not_found("browse parent node not found"))
                })
                .transpose()?;

            let native_page_token = page_token
                .map(|token| {
                    let binding = session
                        .page_bindings
                        .get(token)
                        .ok_or_else(|| Status::failed_precondition("invalid browse page token"))?;
                    if binding.page_size != page_size {
                        return Err(Status::failed_precondition(
                            "browse page token was created for a different page size",
                        ));
                    }
                    if binding.parent_node_key != page_key.parent_node_key {
                        return Err(Status::failed_precondition(
                            "browse page token is bound to a different parent",
                        ));
                    }
                    Ok(binding.native_page_token.clone())
                })
                .transpose()?;

            let cached_page = if refresh {
                session.invalidate_parent(&page_key.parent_node_key);
                None
            } else {
                session.pages.get(&page_key).cloned()
            };

            (
                session.native_session_id.clone(),
                native_parent_key,
                native_page_token,
                Arc::clone(&session.operation_lock),
                cached_page,
            )
        };

        if let Some(page) = cached_page {
            return Ok(page);
        }

        let _operation_guard = operation_lock.lock().await;
        {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| Status::not_found("browse session closed during browse"))?;
            session.touch();
            if !refresh && let Some(page) = session.pages.get(&page_key).cloned() {
                return Ok(page);
            }
        }
        let native_page = self
            .client
            .browse_page(
                &native_session_id,
                native_parent_key.as_deref(),
                native_page_token.as_deref(),
                page_size,
                refresh,
            )
            .await
            .map_err(operation_error)?;

        if native_page.complete && native_page.next_page_token.is_some() {
            return Err(Status::internal(
                "OPC client returned a complete browse page with a continuation token",
            ));
        }
        if !native_page.complete && native_page.next_page_token.is_none() {
            return Err(Status::internal(
                "OPC client returned an incomplete browse page without a continuation token",
            ));
        }

        let page = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| Status::not_found("browse session closed during browse"))?;
            session.touch();

            if session.node_bindings.len() + native_page.nodes.len() > MAX_NODE_BINDINGS_PER_SESSION
            {
                return Err(Status::resource_exhausted(
                    "maximum browse nodes per session reached",
                ));
            }

            let nodes = native_page
                .nodes
                .into_iter()
                .map(|node| {
                    let node_key = new_token();
                    session
                        .node_bindings
                        .insert(node_key.clone(), node.node_key);
                    BrowseNode {
                        node_key,
                        display_name: node.display_name,
                        kind: node.kind,
                        item_id: node.item_id,
                    }
                })
                .collect();

            let next_page_token = native_page.next_page_token.map(|native_token| {
                let token = new_token();
                session.bind_page(
                    token.clone(),
                    PageBinding {
                        native_page_token: native_token,
                        parent_node_key: page_key.parent_node_key.clone(),
                        page_size,
                    },
                );
                token
            });

            let page = BrowsePage {
                nodes,
                next_page_token,
                complete: native_page.complete,
                organization: native_page.organization,
                source: native_page.source,
                warning: native_page.warning,
            };
            session.cache_page(page_key, page.clone());
            page
        };

        Ok(page)
    }

    pub async fn close_session(&self, session_id: &str) -> Result<(), Status> {
        let (native_session_id, operation_lock) = {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| Status::internal("browse session lock poisoned"))?;
            let session = sessions
                .get(session_id)
                .ok_or_else(|| Status::not_found("browse session not found or expired"))?;
            (
                session.native_session_id.clone(),
                Arc::clone(&session.operation_lock),
            )
        };
        let _operation_guard = operation_lock.lock().await;
        let removed = self
            .sessions
            .lock()
            .map_err(|_| Status::internal("browse session lock poisoned"))?
            .remove(session_id);
        if removed.is_none() {
            return Err(Status::not_found("browse session not found or expired"));
        }
        self.client
            .close_browse_session(&native_session_id)
            .await
            .map_err(operation_error)
    }

    fn remove_expired(&self) -> Vec<Session> {
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("browse session lock poisoned while expiring sessions");
            return Vec::new();
        };
        let cutoff = Instant::now() - SESSION_TTL;
        let expired_ids: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| session.last_used < cutoff)
            .map(|(id, _)| id.clone())
            .collect();
        expired_ids
            .into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect()
    }

    async fn close_expired(&self, expired: Vec<Session>) {
        for session in expired {
            let _operation_guard = session.operation_lock.lock().await;
            if let Err(error) = self
                .client
                .close_browse_session(&session.native_session_id)
                .await
            {
                tracing::warn!(
                    server = %session.server,
                    error = %error,
                    "failed to close expired native browse session"
                );
            }
        }
    }
}

fn new_token() -> String {
    Uuid::new_v4().to_string()
}

pub fn normalize_page_size(page_size: u32) -> Result<u32, Status> {
    let page_size = if page_size == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        page_size
    };
    if page_size > MAX_PAGE_SIZE {
        return Err(Status::invalid_argument(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(page_size)
}

fn operation_error(error: anyhow::Error) -> Status {
    tracing::error!(error = %error, "OPC browse operation failed");
    Status::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opc::{
        BrowseCapabilities, BrowseNodeKind, BrowsePage, BrowseSource, NamespaceOrganization,
        OpcValue,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockClient {
        opens: AtomicUsize,
        closes: AtomicUsize,
        browses: AtomicUsize,
        browse_delay: Mutex<Option<Duration>>,
        open_delay: Mutex<Option<Duration>>,
        browse_page_result: Mutex<Option<Result<BrowsePage, String>>>,
        fail_open: std::sync::atomic::AtomicBool,
        fail_browse: std::sync::atomic::AtomicBool,
        fail_close: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl OpcClient for MockClient {
        async fn list_servers(&self, _host: &str) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn get_capabilities(&self, _server: &str) -> anyhow::Result<BrowseCapabilities> {
            Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: MAX_PAGE_SIZE,
            })
        }

        async fn open_browse_session(&self, _server: &str) -> anyhow::Result<String> {
            self.opens.fetch_add(1, Ordering::Relaxed);
            let delay = *self.open_delay.lock().unwrap();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if self.fail_open.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("open failed"));
            }
            Ok("native-session".into())
        }

        async fn browse_page(
            &self,
            _session_id: &str,
            _parent_node_key: Option<&str>,
            page_token: Option<&str>,
            _page_size: u32,
            _refresh: bool,
        ) -> anyhow::Result<BrowsePage> {
            self.browses.fetch_add(1, Ordering::Relaxed);
            let delay = *self.browse_delay.lock().unwrap();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if self.fail_browse.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("browse failed"));
            }
            if let Some(result) = self.browse_page_result.lock().unwrap().clone() {
                return result.map_err(|error| anyhow::anyhow!("{error}"));
            }
            Ok(BrowsePage {
                nodes: vec![BrowseNode {
                    node_key: "native-node".into(),
                    display_name: page_token.unwrap_or("root").into(),
                    kind: BrowseNodeKind::BranchAndItem,
                    item_id: Some("exact.item".into()),
                }],
                next_page_token: page_token.is_none().then(|| "native-next".into()),
                complete: page_token.is_some(),
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            })
        }

        async fn close_browse_session(&self, _session_id: &str) -> anyhow::Result<()> {
            self.closes.fetch_add(1, Ordering::Relaxed);
            if self.fail_close.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("close failed"));
            }
            Ok(())
        }

        async fn read_tag_values(
            &self,
            _server: &str,
            _tag_ids: Vec<String>,
        ) -> anyhow::Result<Vec<crate::opc::TagValue>> {
            Ok(Vec::new())
        }

        async fn write_tag_value(
            &self,
            _server: &str,
            _tag_id: &str,
            _value: crate::opc::OpcValue,
        ) -> anyhow::Result<crate::opc::WriteResult> {
            Ok(crate::opc::WriteResult {
                tag_id: String::new(),
                success: true,
                error: None,
            })
        }
    }

    fn manager() -> (BrowseManager<MockClient>, Arc<MockClient>) {
        let client = Arc::new(MockClient {
            opens: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            browses: AtomicUsize::new(0),
            browse_delay: Mutex::new(None),
            open_delay: Mutex::new(None),
            browse_page_result: Mutex::new(None),
            fail_open: std::sync::atomic::AtomicBool::new(false),
            fail_browse: std::sync::atomic::AtomicBool::new(false),
            fail_close: std::sync::atomic::AtomicBool::new(false),
        });
        (BrowseManager::new(Arc::clone(&client)), client)
    }

    #[test]
    fn normalize_page_size_defaults_and_rejects_large_values() {
        assert_eq!(normalize_page_size(0).unwrap(), DEFAULT_PAGE_SIZE);
        assert_eq!(normalize_page_size(MAX_PAGE_SIZE).unwrap(), MAX_PAGE_SIZE);
        assert_eq!(
            normalize_page_size(MAX_PAGE_SIZE + 1).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn session_cache_and_bindings_evict_old_entries() {
        let mut session = Session::new("S".into(), "native".into());
        session.touch();
        let page = BrowsePage {
            nodes: Vec::new(),
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        };
        let key = PageKey {
            parent_node_key: None,
            page_token: None,
            page_size: 10,
        };
        session.cache_page(key.clone(), page.clone());
        session.cache_page(key, page.clone());
        for index in 0..=MAX_CACHED_PAGES_PER_SESSION {
            session.cache_page(
                PageKey {
                    parent_node_key: None,
                    page_token: Some(index.to_string()),
                    page_size: 10,
                },
                page.clone(),
            );
        }
        assert_eq!(session.pages.len(), MAX_CACHED_PAGES_PER_SESSION);

        for index in 0..=MAX_PAGE_BINDINGS_PER_SESSION {
            session.bind_page(
                format!("token-{index}"),
                PageBinding {
                    native_page_token: format!("native-{index}"),
                    parent_node_key: Some("parent".into()),
                    page_size: 10,
                },
            );
        }
        assert_eq!(session.page_bindings.len(), MAX_PAGE_BINDINGS_PER_SESSION);
        session.cache_page(
            PageKey {
                parent_node_key: Some("parent".into()),
                page_token: Some("child".into()),
                page_size: 10,
            },
            page,
        );
        session.invalidate_parent(&Some("parent".into()));
        assert!(
            session
                .pages
                .keys()
                .all(|key| key.parent_node_key.as_deref() != Some("parent"))
        );
        assert!(
            session
                .page_bindings
                .values()
                .all(|binding| binding.parent_node_key.as_deref() != Some("parent"))
        );
    }

    #[tokio::test]
    async fn rejects_native_open_and_active_session_limit() {
        let (manager, client) = manager();
        client.fail_open.store(true, Ordering::Relaxed);
        assert_eq!(
            manager.open_session("S").await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        client.fail_open.store(false, Ordering::Relaxed);
        for index in 0..MAX_ACTIVE_SESSIONS {
            manager.sessions.lock().unwrap().insert(
                format!("session-{index}"),
                Session::new("S".into(), format!("native-{index}")),
            );
        }
        assert_eq!(
            manager.open_session("S").await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn capacity_race_closes_the_extra_native_session() {
        let (failing_manager, failing_client) = manager();
        for index in 0..(MAX_ACTIVE_SESSIONS - 1) {
            failing_manager.sessions.lock().unwrap().insert(
                format!("session-{index}"),
                Session::new("S".into(), format!("native-{index}")),
            );
        }
        *failing_client.open_delay.lock().unwrap() = Some(Duration::from_millis(10));
        failing_client.fail_close.store(true, Ordering::Relaxed);
        let (first, second) = tokio::join!(
            failing_manager.open_session("S"),
            failing_manager.open_session("S")
        );
        assert_eq!(
            [first.as_ref().err(), second.as_ref().err()]
                .into_iter()
                .filter(|error| error.is_some())
                .count(),
            1
        );
        assert_eq!(
            [first.as_ref().err(), second.as_ref().err()]
                .into_iter()
                .flatten()
                .next()
                .unwrap()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(failing_client.closes.load(Ordering::Relaxed), 1);

        let (success_manager, success_client) = manager();
        for index in 0..(MAX_ACTIVE_SESSIONS - 1) {
            success_manager.sessions.lock().unwrap().insert(
                format!("session-{index}"),
                Session::new("S".into(), format!("native-{index}")),
            );
        }
        *success_client.open_delay.lock().unwrap() = Some(Duration::from_millis(10));
        let (first, second) = tokio::join!(
            success_manager.open_session("S"),
            success_manager.open_session("S")
        );
        assert_eq!(
            [first.as_ref().err(), second.as_ref().err()]
                .into_iter()
                .filter(|error| error.is_some())
                .count(),
            1
        );
        assert_eq!(success_client.closes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn opens_browse_session_and_translates_opaque_values() {
        let (manager, client) = manager();
        let (session, first) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        assert_eq!(client.opens.load(Ordering::Relaxed), 1);
        assert_ne!(session, "native-session");
        assert_eq!(first.nodes[0].display_name, "root");
        assert!(first.next_page_token.is_some());
        assert_ne!(
            first.nodes[0].node_key, "native-node",
            "gateway must not expose native node keys"
        );
    }

    #[tokio::test]
    async fn page_token_is_bound_to_parent_and_page_size() {
        let (manager, _) = manager();
        let (session, first) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        let token = first.next_page_token.unwrap();
        let parent = first.nodes[0].node_key.clone();

        let wrong_parent = manager
            .browse("S", Some(&session), Some("wrong"), Some(&token), 10, false)
            .await
            .unwrap_err();
        assert_eq!(wrong_parent.code(), tonic::Code::NotFound);

        let wrong_size = manager
            .browse("S", Some(&session), Some(&parent), Some(&token), 11, false)
            .await
            .unwrap_err();
        assert_eq!(wrong_size.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn cache_hit_does_not_open_another_native_session() {
        let (manager, client) = manager();
        let (session, first) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        let (_, second) = manager
            .browse("S", Some(&session), None, None, 10, false)
            .await
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(client.opens.load(Ordering::Relaxed), 1);
        assert_eq!(client.browses.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn concurrent_identical_pages_share_the_in_flight_result() {
        let (manager, client) = manager();
        let (session, first_page) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        let page_token = first_page.next_page_token.unwrap();
        *client.browse_delay.lock().unwrap() = Some(Duration::from_millis(20));
        let session_for_first = session.clone();
        let session_for_second = session.clone();
        let (first, second) = tokio::join!(
            manager.browse(
                "S",
                Some(&session_for_first),
                None,
                Some(&page_token),
                10,
                false
            ),
            manager.browse(
                "S",
                Some(&session_for_second),
                None,
                Some(&page_token),
                10,
                false
            )
        );
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(client.browses.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn refresh_forces_a_new_native_page() {
        let (manager, client) = manager();
        let (session, _) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        manager
            .browse("S", Some(&session), None, None, 10, true)
            .await
            .unwrap();
        assert_eq!(client.browses.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rejects_refresh_with_page_token_and_wrong_server_or_token() {
        let (manager, _) = manager();
        let (session, first) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        let token = first.next_page_token.unwrap();
        let parent = first.nodes[0].node_key.clone();
        assert_eq!(
            manager
                .browse("S", Some(&session), None, Some(&token), 10, true)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            manager
                .browse("Other", Some(&session), None, None, 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            manager
                .browse("S", Some(&session), None, Some("unknown"), 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            manager
                .browse("S", Some(&session), Some(&parent), Some(&token), 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn rejects_inconsistent_native_pages_and_node_limit() {
        let (first_manager, client) = manager();
        let session = first_manager.open_session("S").await.unwrap();
        let invalid_page_with_token = BrowsePage {
            nodes: Vec::new(),
            next_page_token: Some("next".into()),
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        };
        *client.browse_page_result.lock().unwrap() = Some(Ok(invalid_page_with_token));
        assert_eq!(
            first_manager
                .browse("S", Some(&session), None, None, 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        *client.browse_page_result.lock().unwrap() = Some(Ok(BrowsePage {
            nodes: Vec::new(),
            next_page_token: None,
            complete: false,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        }));
        assert_eq!(
            first_manager
                .browse("S", Some(&session), None, None, 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );

        let (limit_manager, client) = manager();
        let session = limit_manager.open_session("S").await.unwrap();
        limit_manager
            .sessions
            .lock()
            .unwrap()
            .get_mut(&session)
            .unwrap()
            .node_bindings
            .extend(
                (0..MAX_NODE_BINDINGS_PER_SESSION)
                    .map(|index| (format!("gateway-{index}"), format!("native-{index}"))),
            );
        *client.browse_page_result.lock().unwrap() = Some(Ok(BrowsePage {
            nodes: vec![BrowseNode {
                node_key: "native-node".into(),
                display_name: "node".into(),
                kind: BrowseNodeKind::Item,
                item_id: Some("item".into()),
            }],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        }));
        assert_eq!(
            limit_manager
                .browse("S", Some(&session), None, None, 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn temporary_browse_error_closes_its_session() {
        let (manager, client) = manager();
        client
            .fail_browse
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let error = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(client.closes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn temporary_browse_error_logs_close_failure() {
        let (manager, client) = manager();
        client.fail_browse.store(true, Ordering::Relaxed);
        client.fail_close.store(true, Ordering::Relaxed);
        let error = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(client.closes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn expired_sessions_are_closed_before_the_next_request() {
        let (expired_manager, client) = manager();
        let (session, _) = expired_manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        expired_manager
            .sessions
            .lock()
            .unwrap()
            .get_mut(&session)
            .unwrap()
            .last_used = Instant::now() - SESSION_TTL - Duration::from_secs(1);
        let error = expired_manager
            .browse("S", Some(&session), None, None, 10, false)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
        assert_eq!(client.closes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn close_error_still_removes_gateway_session() {
        let (manager, client) = manager();
        let (session, _) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        client
            .fail_close
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            manager.close_session(&session).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            manager.close_session(&session).await.unwrap_err().code(),
            tonic::Code::NotFound
        );
    }

    #[tokio::test]
    async fn concurrent_close_reports_already_removed_session() {
        let (manager, client) = manager();
        let manager = Arc::new(manager);
        let (session, _) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        let operation_lock = Arc::clone(
            &manager
                .sessions
                .lock()
                .unwrap()
                .get(&session)
                .unwrap()
                .operation_lock,
        );
        let guard = operation_lock.lock().await;
        let first_manager = Arc::clone(&manager);
        let first_session = session.clone();
        let first = tokio::spawn(async move { first_manager.close_session(&first_session).await });
        let second_manager = Arc::clone(&manager);
        let second_session = session.clone();
        let second =
            tokio::spawn(async move { second_manager.close_session(&second_session).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(guard);
        let (first, second) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(
            [first.as_ref().err(), second.as_ref().err()]
                .into_iter()
                .filter(|error| error.is_some())
                .count(),
            1
        );
        assert_eq!(client.closes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn expired_close_errors_are_logged_and_poisoned_sessions_are_safe() {
        let (expired_manager, client) = manager();
        let (session, _) = expired_manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        expired_manager
            .sessions
            .lock()
            .unwrap()
            .get_mut(&session)
            .unwrap()
            .last_used = Instant::now() - SESSION_TTL - Duration::from_secs(1);
        client.fail_close.store(true, Ordering::Relaxed);
        assert_eq!(
            expired_manager
                .browse("S", Some(&session), None, None, 10, false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );

        let (poisoned_manager, _) = manager();
        let sessions = Arc::clone(&poisoned_manager.sessions);
        let _ = std::thread::spawn(move || {
            let _guard = sessions.lock().unwrap();
            panic!("poison browse session lock");
        })
        .join();
        assert!(poisoned_manager.remove_expired().is_empty());
    }

    #[tokio::test]
    async fn local_mock_covers_all_opc_operations() {
        let (_, client) = manager();
        assert!(client.list_servers("host").await.unwrap().is_empty());
        assert!(client.get_capabilities("S").await.unwrap().supports_search);
        assert_eq!(
            client.open_browse_session("S").await.unwrap(),
            "native-session"
        );
        assert!(
            client
                .browse_page("native", None, None, 10, false)
                .await
                .is_ok()
        );
        assert!(client.close_browse_session("native").await.is_ok());
        assert!(
            client
                .read_tag_values("S", vec![])
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .write_tag_value("S", "tag", OpcValue::Bool(true))
                .await
                .unwrap()
                .success
        );
        client
            .browse_page_result
            .lock()
            .unwrap()
            .replace(Err("page failed".into()));
        assert!(
            client
                .browse_page("native", None, None, 10, false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn explicit_close_releases_native_session() {
        let (manager, client) = manager();
        let (session, _) = manager
            .browse("S", None, None, None, 10, false)
            .await
            .unwrap();
        manager.close_session(&session).await.unwrap();
        assert_eq!(client.closes.load(Ordering::Relaxed), 1);
        assert_eq!(
            manager.close_session(&session).await.unwrap_err().code(),
            tonic::Code::NotFound
        );
    }
}
