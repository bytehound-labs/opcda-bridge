use super::map::map_browse_node;
use crate::browse::{BrowseManager, DEFAULT_PAGE_SIZE};
use crate::index::normalize_query;
use crate::opc::{BrowseNode, BrowseNodeKind, OpcClient};
use opcda_bridge_proto::bridge::{
    BrowseBreadcrumb, SearchCompleted, SearchEvent, SearchMatch, SearchMatchMode, SearchProgress,
    SearchRequest, search_event::Event,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::Status;

pub(super) const DEFAULT_SEARCH_RESULTS: u32 = 200;
pub(super) const MAX_SEARCH_RESULTS: u32 = 1_000;
pub(super) const MAX_SEARCH_VISITED: u32 = 50_000;

pub(super) fn search_mode(mode: i32) -> Result<SearchMatchMode, Status> {
    let mode = SearchMatchMode::try_from(mode)
        .map_err(|_| Status::invalid_argument("unknown search match mode"))?;
    if mode == SearchMatchMode::Unspecified {
        Ok(SearchMatchMode::Contains)
    } else {
        Ok(mode)
    }
}

pub(super) fn validate_search(request: &SearchRequest) -> Result<(SearchMatchMode, u32), Status> {
    let normalized_query = normalize_query(&request.query);
    if normalized_query.is_empty() {
        return Err(Status::invalid_argument("search query must not be empty"));
    }
    let mode = search_mode(request.match_mode)?;
    if mode == SearchMatchMode::Contains && request.query.chars().count() < 2 {
        return Err(Status::invalid_argument(
            "contains searches require at least two characters",
        ));
    }
    let max_results = if request.max_results == 0 {
        DEFAULT_SEARCH_RESULTS
    } else {
        request.max_results
    };
    if max_results > MAX_SEARCH_RESULTS {
        return Err(Status::invalid_argument(format!(
            "max_results must not exceed {MAX_SEARCH_RESULTS}"
        )));
    }
    Ok((mode, max_results))
}

pub(super) fn search_matches(node: &BrowseNode, query: &str, mode: SearchMatchMode) -> bool {
    let matches = |value: &str| match mode {
        SearchMatchMode::Exact => value == query,
        SearchMatchMode::Prefix => value.starts_with(query),
        SearchMatchMode::Contains | SearchMatchMode::Unspecified => value.contains(query),
    };
    matches(&node.display_name) || node.item_id.as_deref().is_some_and(matches)
}

pub(super) fn is_expandable(kind: BrowseNodeKind) -> bool {
    matches!(kind, BrowseNodeKind::Branch | BrowseNodeKind::BranchAndItem)
}

fn search_event(event: Event) -> SearchEvent {
    SearchEvent { event: Some(event) }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_search<C: OpcClient>(
    manager: Arc<BrowseManager<C>>,
    _foreground: crate::index::ForegroundGuard<C>,
    server: String,
    session_id: String,
    request: SearchRequest,
    mode: SearchMatchMode,
    max_results: u32,
    temporary_session: bool,
    tx: mpsc::Sender<Result<SearchEvent, Status>>,
) {
    let result = run_search_inner(
        Arc::clone(&manager),
        &server,
        &session_id,
        &request,
        mode,
        max_results,
        &tx,
    )
    .await;

    if let Err(error) = result {
        let _ = tx.send(Err(error)).await;
    }
    if temporary_session && let Err(error) = manager.close_session(&session_id).await {
        tracing::debug!(error = %error, "temporary search session was already closed");
    }
}

struct SearchScope {
    parent_node_key: Option<String>,
    breadcrumbs: Vec<BrowseBreadcrumb>,
    refresh: bool,
}

#[derive(Default)]
struct SearchState {
    matched_item_ids: HashSet<String>,
    visited_nodes: u32,
    matches: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SearchStep {
    Continue,
    Stop,
}

impl SearchState {
    fn match_node(
        &mut self,
        node: &BrowseNode,
        request: &SearchRequest,
        mode: SearchMatchMode,
        breadcrumbs: &[BrowseBreadcrumb],
    ) -> Option<SearchMatch> {
        let item_match = search_matches(node, &request.query, mode);
        let branch_match = request.include_branches && item_match;
        let has_new_item = node
            .item_id
            .as_ref()
            .is_none_or(|item_id| !self.matched_item_ids.contains(item_id));
        if !(item_match && has_new_item && (node.item_id.is_some() || branch_match)) {
            return None;
        }

        if let Some(item_id) = node.item_id.as_ref() {
            self.matched_item_ids.insert(item_id.clone());
        }
        self.matches = self.matches.saturating_add(1);
        let mut result_breadcrumbs = breadcrumbs.to_vec();
        result_breadcrumbs.push(BrowseBreadcrumb {
            node_key: node.node_key.clone(),
            display_name: node.display_name.clone(),
        });
        Some(SearchMatch {
            node: Some(map_browse_node(node.clone())),
            breadcrumbs: result_breadcrumbs,
        })
    }
}

fn initial_search_scopes(request: &SearchRequest) -> VecDeque<SearchScope> {
    let breadcrumbs = request
        .scope_node_key
        .as_ref()
        .map(|node_key| {
            vec![BrowseBreadcrumb {
                node_key: node_key.clone(),
                display_name: String::new(),
            }]
        })
        .unwrap_or_default();
    VecDeque::from([SearchScope {
        parent_node_key: request.scope_node_key.clone(),
        breadcrumbs,
        refresh: request.refresh,
    }])
}

fn child_search_scope(node: &BrowseNode, breadcrumbs: &[BrowseBreadcrumb]) -> Option<SearchScope> {
    if !is_expandable(node.kind) {
        return None;
    }
    let mut child_breadcrumbs = breadcrumbs.to_vec();
    child_breadcrumbs.push(BrowseBreadcrumb {
        node_key: node.node_key.clone(),
        display_name: node.display_name.clone(),
    });
    Some(SearchScope {
        parent_node_key: Some(node.node_key.clone()),
        breadcrumbs: child_breadcrumbs,
        refresh: false,
    })
}

async fn send_search_event(tx: &mpsc::Sender<Result<SearchEvent, Status>>, event: Event) -> bool {
    tx.send(Ok(search_event(event))).await.is_ok()
}

async fn send_search_completion(
    tx: &mpsc::Sender<Result<SearchEvent, Status>>,
    complete: bool,
    truncated: bool,
    warning: Option<&str>,
) -> Result<(), Status> {
    tx.send(Ok(search_event(Event::Completed(SearchCompleted {
        complete,
        cancelled: false,
        truncated,
        warning: warning.map(str::to_string),
    }))))
    .await
    .map_err(|_| Status::cancelled("search stream closed"))
}

struct SearchContext<'a, C: OpcClient> {
    manager: Arc<BrowseManager<C>>,
    server: &'a str,
    session_id: &'a str,
    request: &'a SearchRequest,
    mode: SearchMatchMode,
    max_results: u32,
    state: SearchState,
    scopes: VecDeque<SearchScope>,
    tx: &'a mpsc::Sender<Result<SearchEvent, Status>>,
}

impl<'a, C: OpcClient> SearchContext<'a, C> {
    async fn process_node(
        &mut self,
        node: BrowseNode,
        breadcrumbs: &[BrowseBreadcrumb],
    ) -> Result<SearchStep, Status> {
        self.state.visited_nodes = self.state.visited_nodes.saturating_add(1);
        if let Some(search_match) =
            self.state
                .match_node(&node, self.request, self.mode, breadcrumbs)
        {
            if !send_search_event(self.tx, Event::Match(search_match)).await {
                return Ok(SearchStep::Stop);
            }
            if self.state.matches >= self.max_results {
                send_search_completion(self.tx, false, true, Some("search result limit reached"))
                    .await?;
                return Ok(SearchStep::Stop);
            }
        }

        if let Some(scope) = child_search_scope(&node, breadcrumbs) {
            self.scopes.push_back(scope);
        }
        if self.state.visited_nodes >= MAX_SEARCH_VISITED {
            send_search_completion(self.tx, false, true, Some("search visit limit reached"))
                .await?;
            return Ok(SearchStep::Stop);
        }
        Ok(SearchStep::Continue)
    }

    async fn process_page(
        &mut self,
        nodes: Vec<BrowseNode>,
        partial: bool,
        breadcrumbs: &[BrowseBreadcrumb],
    ) -> Result<SearchStep, Status> {
        for node in nodes {
            if self.process_node(node, breadcrumbs).await? == SearchStep::Stop {
                return Ok(SearchStep::Stop);
            }
        }

        if !send_search_event(
            self.tx,
            Event::Progress(SearchProgress {
                visited_nodes: self.state.visited_nodes,
                matches: self.state.matches,
                partial,
            }),
        )
        .await
        {
            return Ok(SearchStep::Stop);
        }
        Ok(SearchStep::Continue)
    }

    async fn process_scope(&mut self, scope: SearchScope) -> Result<SearchStep, Status> {
        let SearchScope {
            parent_node_key,
            breadcrumbs,
            refresh,
        } = scope;
        let mut page_token = None;
        let mut first_page = true;
        loop {
            let (_, page) = self
                .manager
                .browse(
                    self.server,
                    Some(self.session_id),
                    parent_node_key.as_deref(),
                    page_token.as_deref(),
                    DEFAULT_PAGE_SIZE,
                    refresh && first_page,
                )
                .await?;
            first_page = false;
            let next_page_token = page.next_page_token;
            if self
                .process_page(page.nodes, next_page_token.is_some(), &breadcrumbs)
                .await?
                == SearchStep::Stop
            {
                return Ok(SearchStep::Stop);
            }

            let Some(next_page_token) = next_page_token else {
                return Ok(SearchStep::Continue);
            };
            page_token = Some(next_page_token);
        }
    }
}

pub(super) async fn run_search_inner<C: OpcClient>(
    manager: Arc<BrowseManager<C>>,
    server: &str,
    session_id: &str,
    request: &SearchRequest,
    mode: SearchMatchMode,
    max_results: u32,
    tx: &mpsc::Sender<Result<SearchEvent, Status>>,
) -> Result<(), Status> {
    if !send_search_event(
        tx,
        Event::Progress(SearchProgress {
            visited_nodes: 0,
            matches: 0,
            partial: false,
        }),
    )
    .await
    {
        return Ok(());
    }

    let mut context = SearchContext {
        manager,
        server,
        session_id,
        request,
        mode,
        max_results,
        state: SearchState::default(),
        scopes: initial_search_scopes(request),
        tx,
    };
    while let Some(scope) = context.scopes.pop_front() {
        if context.process_scope(scope).await? == SearchStep::Stop {
            return Ok(());
        }
    }

    send_search_completion(tx, true, false, None).await?;
    Ok(())
}
