use super::*;

#[test]
fn normalization_and_timestamp_helpers_are_safe() {
    assert_eq!(normalize_query("  FCS0201   PV "), "fcs0201 pv");
    assert_eq!(escape_like(r"a%b_c\d"), r"a\%b\_c\\d");
    assert_eq!(prefix_upper_bound("abc"), Some("abd".into()));
    assert_eq!(prefix_upper_bound("a\u{10ffff}"), Some("b".into()));
    assert_eq!(
        prefix_upper_bound("\u{d7ff}\u{10ffff}"),
        Some("\u{e000}".into())
    );
    assert_eq!(prefix_upper_bound("\u{10ffff}"), None);
    assert_eq!(build_fts_query("fcs0201 pv"), "\"fcs0201\" AND \"pv\"");
    assert_eq!(search_rank("219", "219", "display-exact"), 0);
    assert_eq!(search_rank("219", "ordinary", "219"), 1);
    assert_eq!(search_rank("219", "219 block", "ordinary"), 2);
    assert_eq!(search_rank("219", "ordinary", "219.item"), 3);
    assert_eq!(search_rank("219", "block 219", "display-contains"), 4);
    assert_eq!(search_rank("219", "ordinary", "area.219.item"), 5);
    assert_eq!(search_rank("219", "ordinary", "ordinary"), 6);
    assert_eq!(parse_indexed_kind(2), Ok(InventoryNodeKind::BranchAndItem));
    assert!(parse_indexed_kind(99).is_err());
    assert_eq!(
        parse_indexed_breadcrumbs(r#"["Area","Unit"]"#.into()).unwrap(),
        vec!["Area", "Unit"]
    );
    assert!(parse_indexed_breadcrumbs("not-json".into()).is_err());
    assert!(parse_timestamp("not-a-timestamp").is_none());
    assert_eq!(
        parse_timestamp(u128::from(u64::MAX).to_string().as_str()),
        UNIX_EPOCH.checked_add(Duration::from_millis(u64::MAX))
    );
    assert!(parse_timestamp(u128::MAX.to_string().as_str()).is_none());
    assert_eq!(SearchMode::try_from(0), Ok(SearchMode::Unspecified));
    assert_eq!(SearchMode::try_from(1), Ok(SearchMode::Exact));
    assert_eq!(SearchMode::try_from(2), Ok(SearchMode::Prefix));
    assert_eq!(SearchMode::try_from(3), Ok(SearchMode::Contains));
    assert_eq!(SearchMode::try_from(4), Err(()));
}

fn in_memory_index_with(entries: &[InventoryEntry]) -> (IndexDb, u64) {
    let mut database = IndexDb::open(Path::new(":memory:")).unwrap();
    let generation = database
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    database.insert_entries("S", generation, entries).unwrap();
    (database, generation)
}

#[cfg(feature = "fuzzing")]
#[test]
fn fuzzing_search_classifies_fts_syntax_rejections() {
    let query = "fuzz!\0\u{3}";
    let entries = [inventory_entry(query, "0")];

    assert!(matches!(
        super::fuzzing::search_all_modes(query, &entries, 10),
        Err(super::fuzzing::SearchAllModesError::QueryRejected(_))
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn indexed_query_normalization_is_idempotent(value in any::<String>()) {
        let normalized = normalize_query(&value);
        prop_assert_eq!(normalize_query(&normalized), normalized);
    }

    #[test]
    fn indexed_search_preserves_match_tier_order(
        // SQLite FTS5 trigram matching does not support supplementary-plane query scalars.
        value in prop::collection::vec(
            proptest::char::range(' ', '~'),
            0..32,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>()),
    ) {
        let query = format!("fuzz{value}");
        let item_prefix = format!("{query}\0prefix");
        let item_contains = format!("contains:{query}:suffix");
        let entries = [
            inventory_entry(&query, "0"),
            inventory_entry("zzzz", &query),
            inventory_entry(&format!("{query} suffix"), "2"),
            inventory_entry("zzzz", &item_prefix),
            inventory_entry(&format!("prefix {query} suffix"), "4"),
            inventory_entry("zzzz", &item_contains),
            inventory_entry("zzzz", "6"),
        ];
        let (database, generation) = in_memory_index_with(&entries);
        let item_ids = |mode| {
            database
                .search("S", generation, &query, mode, 10)
                .unwrap()
                .into_iter()
                .map(|entry| entry.item_id)
                .collect::<Vec<_>>()
        };

        prop_assert_eq!(item_ids(0), item_ids(3));
        prop_assert_eq!(
            item_ids(1),
            vec!["0".to_string(), query.clone()]
        );
        prop_assert_eq!(
            item_ids(2),
            vec![
                "0".to_string(),
                query.clone(),
                "2".to_string(),
                item_prefix.clone()
            ]
        );
        prop_assert_eq!(
            item_ids(3),
            vec![
                "0".to_string(),
                query,
                "2".to_string(),
                item_prefix,
                "4".to_string(),
                item_contains,
            ]
        );
    }

    #[test]
    fn indexed_record_storage_round_trips_fields_and_breadcrumbs(
        item_id in any::<String>(),
        display_name in any::<String>(),
        breadcrumbs in prop::collection::vec(any::<String>(), 0..8),
        branch_and_item in any::<bool>(),
    ) {
        let item_id = if item_id.is_empty() {
            "fuzz-item".to_string()
        } else {
            item_id
        };
        let display_name = format!("fuzz{display_name}");
        let kind = if branch_and_item {
            InventoryNodeKind::BranchAndItem
        } else {
            InventoryNodeKind::Item
        };
        let entry = InventoryEntry {
            item_id: item_id.clone(),
            display_name: display_name.clone(),
            kind,
            breadcrumbs: breadcrumbs.clone(),
        };
        let expected = IndexedMatch {
            item_id,
            display_name,
            kind,
            breadcrumbs,
        };
        let (database, generation) = in_memory_index_with(std::slice::from_ref(&entry));
        let matches = database
            .search("S", generation, &entry.display_name, 1, 10)
            .unwrap();

        prop_assert_eq!(matches, vec![expected]);
    }
}

#[test]
fn query_cache_evicts_oldest_and_clears_by_server() {
    let mut cache = QueryCache {
        values: HashMap::new(),
        order: VecDeque::new(),
        capacity: 1,
    };
    let first = CacheKey {
        server: "first".into(),
        generation: 1,
        query: "query".into(),
        mode: 3,
        limit: 10,
    };
    let second = CacheKey {
        server: "second".into(),
        generation: 1,
        query: "query".into(),
        mode: 3,
        limit: 10,
    };
    cache.insert(first.clone(), cached_search("first"));
    assert!(cache.get(&first).is_some());
    cache.insert(second.clone(), cached_search("second"));
    assert!(cache.get(&first).is_none());
    assert!(cache.get(&second).is_some());
    cache.clear_server("second");
    assert!(cache.get(&second).is_none());
}

#[test]
fn full_text_search_ranks_bounded_candidates_without_join_sort() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("ranked-search.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.insert_entries(
        "S",
        generation,
        &[
            inventory_entry("ordinary two", "area.219.item"),
            inventory_entry("block 219", "display-contains"),
            inventory_entry("ordinary", "219.item"),
            inventory_entry("219 block", "display-prefix"),
            inventory_entry("219", "display-exact"),
        ],
    )
    .unwrap();
    db.promote("S", generation, "2", &zero_progress()).unwrap();

    let matches = db.search("S", generation, "219", 3, 10).unwrap();
    assert_eq!(
        matches
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "display-exact",
            "display-prefix",
            "219.item",
            "display-contains",
            "area.219.item",
        ]
    );
    assert_eq!(
        db.search("S", generation, "219", 3, 2)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["display-exact", "display-prefix", "219.item"]
    );
    assert_eq!(
        db.search("S", generation, "ordinary two", 3, 10)
            .unwrap()
            .len(),
        1
    );
    db.connection
        .execute(
            "DELETE FROM entries
             WHERE server = 'S' AND generation = ?1 AND item_id = 'display-exact'",
            [generation as i64],
        )
        .unwrap();
    assert_eq!(db.search("S", generation, "219", 3, 10).unwrap().len(), 4);
}

#[test]
fn exact_search_uses_ranked_equality_matches_without_duplicates() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exact-search.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.insert_entries(
        "S",
        generation,
        &[
            inventory_entry("PUMP", "z-display"),
            inventory_entry("pump", "a-display"),
            inventory_entry("Pump output", "PUMP"),
            inventory_entry("PUMP", "pump"),
            inventory_entry("unrelated", "other"),
        ],
    )
    .unwrap();
    db.promote("S", generation, "2", &zero_progress()).unwrap();

    let matches = db.search("S", generation, "PuMp", 1, 10).unwrap();
    assert_eq!(
        matches
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a-display", "pump", "z-display", "PUMP"]
    );
    assert_eq!(
        db.search("S", generation, "pump", 1, 2)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a-display", "pump", "z-display"]
    );
}

#[test]
fn exact_search_bounds_common_display_name_matches_with_equality_indexes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("broad-exact-search.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    let entries = (0..10_000)
        .map(|index| inventory_entry("PV", &format!("Area.{index:05}.PV")))
        .collect::<Vec<_>>();
    db.insert_entries("S", generation, &entries).unwrap();
    db.promote("S", generation, "2", &zero_progress()).unwrap();

    let plan_for = |sql: &str| {
        db.connection
            .prepare(sql)
            .unwrap()
            .query_map(params!["S", generation as i64, "pv", 4_i64], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    let display_plan = plan_for(
        "EXPLAIN QUERY PLAN
         SELECT e.item_id, e.display_name, e.display_name_norm,
                e.item_id_norm, e.kind, e.breadcrumbs
         FROM entries e
         WHERE e.server = ?1 AND e.generation = ?2
           AND e.display_name_norm = ?3
         ORDER BY e.item_id_norm, e.item_id
         LIMIT ?4",
    );
    assert!(
        display_plan
            .iter()
            .any(|detail| detail.contains("entries_display_exact"))
    );
    assert!(
        display_plan
            .iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE"))
    );

    let item_plan = plan_for(
        "EXPLAIN QUERY PLAN
         SELECT e.item_id, e.display_name, e.display_name_norm,
                e.item_id_norm, e.kind, e.breadcrumbs
         FROM entries e
         WHERE e.server = ?1 AND e.generation = ?2
           AND e.item_id_norm = ?3
           AND e.display_name_norm <> ?3
         ORDER BY length(e.display_name_norm), e.display_name_norm,
                  e.item_id_norm, e.item_id
         LIMIT ?4",
    );
    assert!(
        item_plan
            .iter()
            .any(|detail| detail.contains("entries_item_exact"))
    );
    assert!(
        item_plan
            .iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE"))
    );

    let matches = db.search("S", generation, "PV", 1, 3).unwrap();
    assert_eq!(matches.len(), 4);
    assert_eq!(
        matches
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Area.00000.PV",
            "Area.00001.PV",
            "Area.00002.PV",
            "Area.00003.PV"
        ]
    );
}

#[test]
fn prefix_search_uses_indexed_ranges_and_preserves_ranking() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("prefix-search.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.insert_entries(
        "S",
        generation,
        &[
            inventory_entry("219 block", "display-prefix"),
            inventory_entry("219", "display-exact"),
            inventory_entry("ordinary", "219.item"),
            inventory_entry("219 both", "219.both"),
            inventory_entry("ordinary", "x219.item"),
            inventory_entry("ordinary", "x%219.item"),
            inventory_entry("éclair", "unicode-display"),
            inventory_entry("ordinary", "\u{10ffff}item"),
            inventory_entry("block 219", "display-contains"),
        ],
    )
    .unwrap();
    db.promote("S", generation, "2", &zero_progress()).unwrap();

    assert_eq!(
        db.search("S", generation, "219", 2, 10)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["display-exact", "219.both", "display-prefix", "219.item"]
    );
    assert_eq!(
        db.search("S", generation, "219", 2, 2)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["display-exact", "219.both", "display-prefix"]
    );
    assert_eq!(
        db.search("S", generation, "x%", 2, 10)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["x%219.item"]
    );
    assert_eq!(
        db.search("S", generation, "É", 2, 10)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["unicode-display"]
    );
    assert_eq!(
        db.search("S", generation, "\u{10ffff}", 2, 10)
            .unwrap()
            .iter()
            .map(|value| value.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["\u{10ffff}item"]
    );
}

#[test]
fn full_text_search_reports_missing_tables() {
    let directory = tempdir().unwrap();
    let fts_path = directory.path().join("missing-fts.sqlite3");
    let mut fts_db = IndexDb::open(&fts_path).unwrap();
    let fts_generation = fts_db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    fts_db
        .insert_entries("S", fts_generation, &[inventory_entry("Tag", "S.Tag")])
        .unwrap();
    fts_db
        .promote("S", fts_generation, "2", &zero_progress())
        .unwrap();
    fts_db
        .connection
        .execute("DROP TABLE entries_fts", [])
        .unwrap();
    assert!(fts_db.search("S", fts_generation, "tag", 3, 10).is_err());

    let entries_path = directory.path().join("missing-entries.sqlite3");
    let mut entries_db = IndexDb::open(&entries_path).unwrap();
    let entries_generation = entries_db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    entries_db
        .insert_entries("S", entries_generation, &[inventory_entry("Tag", "S.Tag")])
        .unwrap();
    entries_db
        .promote("S", entries_generation, "2", &zero_progress())
        .unwrap();
    entries_db
        .connection
        .execute("DROP TABLE entries", [])
        .unwrap();
    assert!(
        entries_db
            .search("S", entries_generation, "tag", 3, 10)
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_during_promotion_does_not_wait_for_database_mutex() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "2",
        false,
    );
    insert_runtime_build(&manager, Arc::new(RecordingInventoryControl::default()));
    manager.mark_promoting("S").unwrap();

    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let lock_manager = Arc::clone(&manager);
    let lock_thread = std::thread::spawn(move || {
        let database_guard = lock_manager.database.lock().unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(database_guard);
    });
    locked_rx.recv().unwrap();

    let search_manager = Arc::clone(&manager);
    let search_task =
        tokio::spawn(async move { search_manager.search("S", "Persisted", 3, 10).await });
    let search = tokio::time::timeout(Duration::from_secs(1), search_task)
        .await
        .expect("indexed search should not wait for the writer mutex")
        .expect("search task should not panic")
        .unwrap();
    assert_eq!(search.status.state, IndexState::Promoting);
    assert_eq!(search.matches.len(), 1);
    assert_eq!(search.matches[0].item_id, "Persisted.Tag");

    release_tx.send(()).unwrap();
    lock_thread.join().unwrap();
}

#[tokio::test]
async fn manager_search_uses_cache_and_refresh_clears_it() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;

    let first = manager.search("S", "mock", 3, 1).await.unwrap();
    assert_eq!(first.matches.len(), 1);
    assert!(!first.has_more);
    manager
        .with_database(|db| {
            db.connection
                .execute("DELETE FROM entries_fts WHERE server = 'S'", [])?;
            db.connection
                .execute("DELETE FROM entries WHERE server = 'S'", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        manager.search("S", "  MOCK  ", 3, 1).await.unwrap().matches,
        first.matches
    );
    manager.cache.lock().unwrap().clear_server("S");
    assert!(
        manager
            .search("S", "mock", 3, 1)
            .await
            .unwrap()
            .matches
            .is_empty()
    );

    assert!(manager.search("S", "   ", 3, 1).await.is_err());
    assert_eq!(manager.max_results(), 50);
}

#[tokio::test]
async fn search_clamps_limit_and_reports_more_matches() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.max_results = 2;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries(
                "S",
                generation,
                &[
                    inventory_entry("Alpha one", "Alpha.1"),
                    inventory_entry("Alpha two", "Alpha.2"),
                    inventory_entry("Alpha three", "Alpha.3"),
                ],
            )?;
            db.promote("S", generation, &timestamp_now(), &zero_progress())
        })
        .unwrap();

    let result = manager.search("S", "alpha", 2, 99).await.unwrap();
    assert_eq!(result.matches.len(), 2);
    assert!(result.has_more);
}

#[tokio::test]
async fn search_does_not_hold_database_lock_during_read_only_query() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
            db.promote("S", generation, &timestamp_now(), &zero_progress())
        })
        .unwrap();

    let (search_started, release_search) = manager.install_search_gate();
    let search_manager = Arc::clone(&manager);
    let search_task = tokio::spawn(async move { search_manager.search("S", "mock", 3, 10).await });
    tokio::time::timeout(Duration::from_secs(2), search_started)
        .await
        .unwrap()
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), manager.status("S"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, IndexState::Ready);

    release_search.send(()).unwrap();
    let search = search_task.await.unwrap().unwrap();
    assert_eq!(search.matches.len(), 1);
}

#[tokio::test]
async fn memory_database_search_uses_primary_connection() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
            db.promote("S", generation, &timestamp_now(), &zero_progress())
        })
        .unwrap();

    let search = manager.search("S", "mock", 3, 10).await.unwrap();
    assert_eq!(search.matches.len(), 1);
    assert_eq!(search.status.state, IndexState::Ready);
}

#[tokio::test]
async fn profile_change_invalidates_persisted_generation_and_cached_search() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    manager.change_auto_refresh("S", true).unwrap();
    assert_eq!(
        manager
            .search("S", "mock", 3, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );

    *client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
        organization: NamespaceOrganization::Flat,
        source: BrowseSource::Flat,
        supports_browse_sessions: false,
        supports_search: true,
        max_page_size: 100,
    });
    client.inventory_events.lock().unwrap().extend([
        Ok(InventoryEvent::Entry(inventory_entry(
            "Replacement",
            "New.Tag",
        ))),
        Ok(InventoryEvent::Progress(InventoryProgress {
            branches_visited: 0,
            entries_seen: 1,
            unique_items: 1,
            active_time_ms: 1,
            paused_time_ms: 0,
            items_per_second: 1.0,
            estimated_remaining_ms: None,
        })),
        Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            truncated: false,
            warning: None,
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
        })),
    ]);

    manager.refresh_if_due("S").await;
    wait_for_build(&manager, IndexState::Ready).await;
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.organization, NamespaceOrganization::Flat);
    assert_eq!(status.source, BrowseSource::Flat);
    assert!(
        manager
            .search("S", "mock", 3, 10)
            .await
            .unwrap()
            .matches
            .is_empty()
    );
    assert_eq!(
        manager
            .search("S", "new", 2, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn search_does_not_return_stale_matches_while_deletion_is_active() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("deleting-search.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;

    manager.deleting.lock().unwrap().insert("S".into());

    let result = manager.search("S", "mock", 3, 10).await.unwrap();
    assert!(result.matches.is_empty());
    assert_eq!(result.status.state, IndexState::Deleting);
}

#[tokio::test]
async fn search_discards_matches_if_deletion_starts_during_query() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("deleting-during-search.sqlite3")),
    ));
    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", generation, &[inventory_entry("Mock tag", "mock.tag")])?;
            db.promote("S", generation, &timestamp_now(), &zero_progress())
        })
        .unwrap();

    let (search_started, release_search) = manager.install_search_gate();
    let search_manager = Arc::clone(&manager);
    let search_task = tokio::spawn(async move { search_manager.search("S", "mock", 3, 10).await });
    tokio::time::timeout(Duration::from_secs(2), search_started)
        .await
        .unwrap()
        .unwrap();

    manager.deleting.lock().unwrap().insert("S".into());
    release_search.send(()).unwrap();

    let result = search_task.await.unwrap().unwrap();
    assert!(result.matches.is_empty());
    assert_eq!(result.status.state, IndexState::Deleting);
}

#[tokio::test]
async fn stale_maintenance_delay_and_promotion_search_are_bounded() {
    let directory = tempdir().unwrap();
    let now = Local::now();
    let minute = (now.hour() * 60 + now.minute()) as u16;
    let window = format!(
        "{:02}:{:02}-{:02}:{:02}",
        ((minute + 2) % 1440) / 60,
        ((minute + 2) % 1440) % 60,
        ((minute + 3) % 1440) / 60,
        ((minute + 3) % 1440) % 60
    );
    let mut config = settings(directory.path().join("stale-maintenance.sqlite3"));
    config.maintenance_windows = vec![window];
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
        false,
    );
    assert_eq!(
        manager.background_refresh_delay("S").await,
        Duration::from_secs(60)
    );

    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, control);
    manager.mark_promoting("S").unwrap();
    let result = manager.search("S", "persisted", 3, 10).await.unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.status.state, IndexState::Promoting);
}

#[tokio::test]
async fn poisoned_guards_and_empty_promoting_search_fail_safely() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("poisoned-guards.sqlite3")),
    ));
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let overlays = Arc::clone(&manager.pause_overlays);
        let _ = std::panic::catch_unwind(move || {
            let _guard = overlays.lock().unwrap();
            panic!("poison pause overlays");
        });
        manager.set_pause_overlay("S", Some(true), None);
        manager.clear_pause_overlays("S");
        manager.reconcile_pause_state("S");

        let pending = Arc::clone(&manager.pending_cancels);
        let _ = std::panic::catch_unwind(move || {
            let _guard = pending.lock().unwrap();
            panic!("poison pending cancellations");
        });
        assert!(manager.take_pending_cancel("S"));
        manager.clear_pending_cancel("S");

        let promoting = Arc::clone(&manager.promoting);
        let _ = std::panic::catch_unwind(move || {
            let _guard = promoting.lock().unwrap();
            panic!("poison promotion state");
        });
        manager.clear_promoting("S");
    });

    let promotion = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&promotion, control);
    promotion.mark_promoting("S").unwrap();
    let search = promotion.search("S", "tag", 3, 10).await.unwrap();
    assert!(search.matches.is_empty());
    assert_eq!(search.status.state, IndexState::Promoting);
    assert_eq!(
        promotion
            .commit_pending_entries("S", 1, &mut Vec::new())
            .unwrap(),
        0
    );
}

fn cached_search(server: &str) -> IndexedSearch {
    IndexedSearch {
        matches: Vec::new(),
        has_more: false,
        status: IndexStatus {
            server: server.into(),
            state: IndexState::Ready,
            auto_refresh_enabled: false,
            active_generation: 1,
            entry_count: 0,
            unique_item_count: 0,
            started_at: None,
            completed_at: None,
            last_error: None,
            database_bytes: 0,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            progress: None,
            effective_limits: None,
            controller_state: None,
            pause_reason: None,
            recovery_deadline: None,
            foreground_metrics: ForegroundMetrics::default(),
            host_metrics: HostMetrics::default(),
            health: HealthProbeState::Unavailable,
            sentinel_configured: false,
            storage: StorageDiagnostics::default(),
            scheduler: SchedulerDiagnostics::default(),
        },
    }
}

#[test]
fn split_query_rejects_malformed_rows_and_handles_maximum_prefix() {
    let entries = [inventory_entry("Target", "S.Target")];
    let (database, generation) = in_memory_index_with(&entries);
    database
        .connection
        .execute("UPDATE entries SET kind = 99 WHERE server = 'S'", [])
        .unwrap();
    assert!(
        database
            .search("S", generation, "ta", 3, 10)
            .unwrap_err()
            .to_string()
            .contains("unknown indexed node kind 99")
    );

    let (database, generation) = in_memory_index_with(&entries);
    database
        .connection
        .execute(
            "UPDATE entries SET breadcrumbs = 'not-json' WHERE server = 'S'",
            [],
        )
        .unwrap();
    let error = database.search("S", generation, "ta", 3, 10).unwrap_err();
    assert!(
        error
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(|error| {
                matches!(error, rusqlite::Error::FromSqlConversionFailure(..))
            })
    );

    let (database, generation) = in_memory_index_with(&entries);
    assert!(
        database
            .search("S", generation, "\u{10ffff}", 2, 10)
            .unwrap()
            .is_empty()
    );

    let (database, generation) = in_memory_index_with(&entries);
    database
        .reject_next_prefix_query_map
        .store(true, Ordering::Release);
    assert!(database.search("S", generation, "ta", 2, 10).is_err());

    let (database, generation) = in_memory_index_with(&entries);
    database
        .reject_next_prefix_query_map
        .store(true, Ordering::Release);
    assert!(
        database
            .search("S", generation, "\u{10ffff}", 2, 10)
            .is_err()
    );
}
