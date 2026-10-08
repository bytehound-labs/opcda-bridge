use super::*;

#[test]
fn index_operation_errors_have_stable_messages_and_sources() {
    let unknown = IndexOperationError::UnknownServer {
        server: "Typo.Server".into(),
    };
    assert_eq!(
        unknown.to_string(),
        "OPC DA server \"Typo.Server\" is not registered"
    );
    assert!(unknown.source().is_none());

    let not_enrolled = IndexOperationError::NotEnrolled {
        server: "Unenrolled.Server".into(),
    };
    assert_eq!(
        not_enrolled.to_string(),
        "namespace index for OPC DA server \"Unenrolled.Server\" is not enrolled"
    );
    assert!(not_enrolled.source().is_none());

    let deleting = IndexOperationError::Deleting {
        server: "Deleting.Server".into(),
    };
    assert_eq!(
        deleting.to_string(),
        "namespace index for OPC DA server \"Deleting.Server\" is being deleted"
    );
    assert!(deleting.source().is_none());

    let internal = IndexOperationError::Internal(anyhow::anyhow!("database failed"));
    assert_eq!(internal.to_string(), "database failed");
    assert!(internal.source().is_some());
}

#[tokio::test]
async fn background_indexing_never_starts_an_unenrolled_server() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.refresh_interval_seconds = 1;
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

    manager.start_background_indexing();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn manager_reports_unenrolled_servers_without_scanning() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    let status = manager.status("Other").await.unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    assert!(!status.auto_refresh_enabled);
    let response = manager.search("Other", "tag", 3, 10).await.unwrap();
    assert!(response.matches.is_empty());
    assert_eq!(response.status.state, IndexState::NotIndexed);

    let unindexed = manager.search("S", "tag", 3, 0).await.unwrap();
    assert!(unindexed.matches.is_empty());
    assert!(!unindexed.status.auto_refresh_enabled);

    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries(
                "S",
                generation,
                &[inventory_entry("Staging tag", "Staging.Tag")],
            )
        })
        .unwrap();
    let staging = manager.search("S", "staging", 2, 10).await.unwrap();
    assert_eq!(staging.matches.len(), 1);
    assert_eq!(staging.status.state, IndexState::Partial);
    assert!(manager.cache.lock().unwrap().values.is_empty());
}

#[tokio::test]
async fn lifecycle_controls_reject_unenrolled_servers() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("unenrolled-controls.sqlite3")),
    ));

    assert!(matches!(
        manager
            .control("S", IndexControlAction::EnableAutoRefresh)
            .await,
        Err(IndexOperationError::NotEnrolled { server }) if server == "S"
    ));
    assert!(matches!(
        manager
            .control("S", IndexControlAction::Pause)
            .await,
        Err(IndexOperationError::NotEnrolled { server }) if server == "S"
    ));

    let deleted = manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    assert_eq!(deleted.state, IndexState::NotIndexed);
}

#[tokio::test]
async fn refresh_rejects_a_server_marked_for_deletion() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("deleting.sqlite3")),
    ));
    manager.deleting.lock().unwrap().insert("S".into());

    assert!(matches!(
        manager.refresh("S", true).await,
        Err(IndexOperationError::Deleting { server }) if server == "S"
    ));
}

#[tokio::test]
async fn all_lifecycle_controls_reject_a_server_marked_for_deletion() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("deleting-controls.sqlite3")),
    ));
    manager.deleting.lock().unwrap().insert("S".into());

    for action in [
        IndexControlAction::Pause,
        IndexControlAction::Resume,
        IndexControlAction::Cancel,
        IndexControlAction::EnableAutoRefresh,
        IndexControlAction::DisableAutoRefresh,
        IndexControlAction::Delete,
    ] {
        assert!(matches!(
            manager.control("S", action).await,
            Err(IndexOperationError::Deleting { server }) if server == "S"
        ));
    }
}

#[tokio::test]
async fn cancellation_and_deletion_wait_helpers_cover_active_builds() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("active-build-helpers.sqlite3")),
    ));
    let recording = Arc::new(RecordingInventoryControl::default());
    let control: Arc<dyn InventoryControl> = recording.clone();
    insert_runtime_build(&manager, control);

    manager
        .apply_control_action("S", IndexControlAction::EnableAutoRefresh)
        .unwrap();
    manager
        .apply_control_action("S", IndexControlAction::DisableAutoRefresh)
        .unwrap();
    manager
        .apply_control_action("S", IndexControlAction::Delete)
        .unwrap();
    manager.cancel_active_build("S").unwrap();
    assert!(recording.cancelled.load(Ordering::Acquire));

    let wait_manager = Arc::clone(&manager);
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        wait_manager.active_builds.lock().unwrap().remove("S");
        wait_manager.build_changed.notify_waiters();
    });
    manager.wait_for_build_to_finish("S").await;
    release.await.unwrap();
}

#[tokio::test]
async fn deletion_waits_for_an_external_build_lock() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("delete-lock.sqlite3");
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(database.clone()),
    );
    let held_lock = BuildFileLock::acquire(&database, "S").unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(held_lock);
    });

    let delete_lock = manager.acquire_delete_lock("S").await.unwrap();
    release.await.unwrap();
    drop(delete_lock);
}

#[tokio::test]
async fn manual_refresh_enrolls_only_listed_servers() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    *client.list_servers_result.lock().unwrap() = Ok(vec!["Actual.Server".into()]);
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));

    let error = manager.refresh("Typo.Server", true).await.unwrap_err();
    assert!(matches!(
        error,
        IndexOperationError::UnknownServer { ref server } if server == "Typo.Server"
    ));
    assert!(
        manager
            .with_database(|db| db.enrollment("Typo.Server"))
            .unwrap()
            .is_none()
    );

    manager.refresh("Actual.Server", true).await.unwrap();
    wait_for_state(&manager, "Actual.Server", IndexState::Ready).await;
    assert!(
        manager
            .status("Actual.Server")
            .await
            .unwrap()
            .auto_refresh_enabled
    );
}

#[tokio::test]
async fn disabled_global_scheduler_does_not_block_manual_refresh_or_search() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.enabled = false;
    let manager = Arc::new(IndexManager::new(client, config));

    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    assert_eq!(
        manager
            .search("S", "mock", 3, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
}

#[tokio::test]
async fn auto_refresh_preference_is_distinct_from_gateway_policy() {
    for (enabled, paused) in [(false, false), (false, true), (true, true), (true, false)] {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.enabled = enabled;
        config.paused = paused;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

        manager.start_background_indexing();
        assert_eq!(
            manager.background_started.load(Ordering::Acquire),
            enabled && !paused
        );
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;

        let status = manager.status("S").await.unwrap();
        assert!(status.auto_refresh_enabled);
        assert_eq!(
            status.scheduler.auto_refresh_policy,
            if !enabled {
                AutoRefreshPolicy::Disabled
            } else if paused {
                AutoRefreshPolicy::Paused
            } else {
                AutoRefreshPolicy::Allowed
            }
        );
        assert_eq!(
            status.scheduler.next_refresh_at.is_some(),
            enabled && !paused,
            "enabled={enabled}, paused={paused}"
        );
        let generation = status.active_generation;
        let disabled = manager
            .control("S", IndexControlAction::DisableAutoRefresh)
            .await
            .unwrap();
        assert!(!disabled.auto_refresh_enabled);
        assert!(disabled.scheduler.next_refresh_at.is_none());
        assert_eq!(disabled.active_generation, generation);
        assert_eq!(
            manager
                .search("S", "mock", 3, 10)
                .await
                .unwrap()
                .matches
                .len(),
            1
        );

        *client.inventory_events.lock().unwrap() = MockOpcClient::default()
            .inventory_events
            .into_inner()
            .unwrap();
        manager.refresh("S", true).await.unwrap();
        wait_for_build(&manager, IndexState::Ready).await;
        assert!(!manager.status("S").await.unwrap().auto_refresh_enabled);
        let reenabled = manager
            .control("S", IndexControlAction::EnableAutoRefresh)
            .await
            .unwrap();
        assert!(reenabled.auto_refresh_enabled);
        assert_eq!(
            reenabled.scheduler.next_refresh_at.is_some(),
            enabled && !paused
        );
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 2);
        manager.shutdown_background_indexing().await;
    }
}

#[tokio::test]
async fn gateway_auto_refresh_policy_is_reported_without_an_active_index() {
    for (enabled, paused, policy) in [
        (true, false, AutoRefreshPolicy::Allowed),
        (false, false, AutoRefreshPolicy::Disabled),
        (true, true, AutoRefreshPolicy::Paused),
    ] {
        let mut config = settings(PathBuf::from(":memory:"));
        config.enabled = enabled;
        config.paused = paused;
        let manager = IndexManager::new(Arc::new(MockOpcClient::default()), config);
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.state, IndexState::NotIndexed);
        assert_eq!(status.scheduler.auto_refresh_policy, policy);
        assert!(status.scheduler.next_refresh_at.is_none());
        manager.deleting.lock().unwrap().insert("S".into());
        let deleting = manager.status("S").await.unwrap();
        assert_eq!(deleting.state, IndexState::Deleting);
        assert_eq!(deleting.scheduler.auto_refresh_policy, policy);
    }
}

#[tokio::test]
async fn automatic_refresh_runs_only_when_gateway_policy_allows_it() {
    for (enabled, paused) in [(false, false), (false, true), (true, true), (true, false)] {
        let directory = tempdir().unwrap();
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(directory.path().join("scheduled.sqlite3"));
        config.enabled = enabled;
        config.paused = paused;
        config.refresh_interval_seconds = 1;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "0",
        );

        manager.start_background_indexing();
        if enabled && !paused {
            wait_for_counter(&client.inventory_start_count, 1).await;
            wait_for_build(&manager, IndexState::Ready).await;
            assert!(manager.status("S").await.unwrap().active_generation > 1);
        } else {
            assert!(!manager.background_started.load(Ordering::Acquire));
            assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
            assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
        }
        manager.shutdown_background_indexing().await;
    }
}

#[tokio::test]
async fn disabling_auto_refresh_preserves_an_active_build_and_cached_generation() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    let control = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, control.clone());

    let status = manager
        .control("S", IndexControlAction::DisableAutoRefresh)
        .await
        .unwrap();
    assert!(!status.auto_refresh_enabled);
    assert_eq!(status.state, IndexState::Refreshing);
    assert_eq!(status.active_generation, 1);
    assert!(status.scheduler.next_refresh_at.is_none());
    assert!(!control.cancelled.load(Ordering::Acquire));
    assert_eq!(
        manager
            .search("S", "persisted", 3, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
    manager.finish_build_owned("S", &ownership, None);
}

#[tokio::test]
async fn auto_refresh_can_be_disabled_without_deleting_searchable_data() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));

    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    manager
        .control("S", IndexControlAction::DisableAutoRefresh)
        .await
        .unwrap();
    let status = manager.status("S").await.unwrap();
    assert!(!status.auto_refresh_enabled);
    assert!(status.scheduler.next_refresh_at.is_none());
    assert_eq!(
        manager
            .search("S", "mock", 3, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );

    manager
        .control("S", IndexControlAction::EnableAutoRefresh)
        .await
        .unwrap();
    assert!(manager.status("S").await.unwrap().auto_refresh_enabled);
}

#[tokio::test]
async fn duplicate_delete_is_rejected_while_cleanup_is_in_progress() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("duplicate-delete.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(database.clone()),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    let held_lock = BuildFileLock::acquire(&database, "S").unwrap();

    let status = manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    assert_eq!(status.state, IndexState::Deleting);
    assert!(matches!(
        manager.control("S", IndexControlAction::Delete).await,
        Err(IndexOperationError::Deleting { server }) if server == "S"
    ));

    drop(held_lock);
    manager.background_tasks.wait_for_idle().await;
}

#[tokio::test]
async fn delete_index_reports_shutdown_when_background_task_cannot_start() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    manager.shutdown_background_indexing().await;

    let error = manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("gateway is shutting down"));
    assert_eq!(manager.status("S").await.unwrap().state, IndexState::Ready);
}

#[test]
fn split_enrollment_guards_reject_conflicts_and_lock_errors() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    manager.reserve_deletion("S").unwrap();
    assert!(matches!(
        manager.reserve_deletion("S"),
        Err(IndexOperationError::Deleting { server }) if server == "S"
    ));

    let poisoned = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    let deleting = Arc::clone(&poisoned.deleting);
    assert!(
        std::thread::spawn(move || {
            let _guard = deleting.lock().unwrap();
            panic!("poison deletion reservation state");
        })
        .join()
        .is_err()
    );
    assert!(matches!(
        poisoned.reserve_deletion("S"),
        Err(IndexOperationError::Internal(error))
            if error.to_string().contains("index deletion lock poisoned")
    ));
}

#[tokio::test]
async fn split_delete_lock_returns_unheld_acquisition_errors() {
    let error = enrollment::acquire_delete_lock_with(
        Path::new("index.sqlite3"),
        "S",
        |_, _| Err(anyhow::anyhow!("injected lock acquisition failure")),
        |_, _| Ok(false),
    )
    .await
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("injected lock acquisition failure")
    );
}
