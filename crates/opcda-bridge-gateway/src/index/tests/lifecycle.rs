use super::*;

#[tokio::test]
async fn operator_cancel_defers_automatic_work_and_manual_refresh_overrides_it() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("operator-cancel.sqlite3");
    let mut config = settings(path);
    config.refresh_interval_seconds = 3_600;
    config.schedule_jitter_seconds = 120;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config.clone(),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    let before = parse_timestamp(&timestamp_now()).unwrap();
    let interval = Duration::from_secs(config.refresh_interval_seconds)
        .saturating_add(deterministic_jitter("S", config.schedule_jitter_seconds));

    manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert!(control.is_cancelled());
    manager.finish_build_for_control_owned("S", &control, &ownership, None);
    let status = manager.status("S").await.unwrap();
    let deadline = parse_timestamp(status.scheduler.next_refresh_at.as_deref().unwrap()).unwrap();
    assert!(deadline >= before + interval);
    assert!(deadline <= std::time::SystemTime::now() + interval);
    assert_eq!(status.active_generation, 1);
    assert_eq!(status.scheduler.consecutive_failures, 0);
    assert_eq!(
        manager.with_database(|db| db.retry_state("S")).unwrap().0,
        Some(deadline)
    );
    manager.shutdown_background_indexing().await;
    drop(manager);

    let client = Arc::new(MockOpcClient::default());
    let restarted = Arc::new(IndexManager::new(Arc::clone(&client), config));
    let status = restarted.status("S").await.unwrap();
    assert_eq!(
        parse_timestamp(status.scheduler.next_refresh_at.as_deref().unwrap()),
        Some(deadline)
    );
    restarted.refresh_if_due("S").await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    restarted.refresh("S", true).await.unwrap();
    wait_for_build(&restarted, IndexState::Ready).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
    assert!(
        restarted
            .with_database(|db| db.retry_state("S"))
            .unwrap()
            .0
            .is_none()
    );
    restarted.shutdown_background_indexing().await;
}

#[tokio::test]
async fn idle_cancel_does_not_defer_a_completed_index() {
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
    let before = manager.status("S").await.unwrap().scheduler.next_refresh_at;
    manager.runtime.lock().unwrap().clear();
    let after = manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert_eq!(after.scheduler.next_refresh_at, before);
    assert!(
        manager
            .with_database(|db| db.retry_state("S"))
            .unwrap()
            .0
            .is_none()
    );
}

#[tokio::test]
async fn cancellation_before_attachment_survives_obsolete_owner_completion() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let ownership = manager.reserve_refresh_build("S", true).unwrap().unwrap();
    manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    let deadline = manager.status("S").await.unwrap().scheduler.next_refresh_at;
    manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert_eq!(
        manager.status("S").await.unwrap().scheduler.next_refresh_at,
        deadline
    );
    manager.finish_build_owned("S", &Arc::new(()), None);
    assert_eq!(
        manager.status("S").await.unwrap().scheduler.next_refresh_at,
        deadline
    );
    let limits = manager.initial_inventory_limits();
    let handle = manager
        .start_refresh_inventory("S", &ownership, limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        manager
            .attach_refresh_control("S", &ownership, &handle, limits)
            .unwrap(),
        None
    );
    assert!(handle.control.is_cancelled());
    manager.finish_build_for_control_owned("S", &handle.control, &ownership, None);
    assert_eq!(
        manager.status("S").await.unwrap().scheduler.next_refresh_at,
        deadline
    );
    assert!(!manager.active_builds.lock().unwrap().contains("S"));
}

#[tokio::test]
async fn cancellation_persistence_failure_is_explicit_and_does_not_resume_automatic_work() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager
        .with_database(|db| {
            db.connection.execute_batch(
                "CREATE TRIGGER reject_retry_deadline BEFORE INSERT ON index_meta
             WHEN NEW.key = 'retry_after:S'
             BEGIN SELECT RAISE(ABORT, 'retry deadline rejected'); END;",
            )?;
            Ok(())
        })
        .unwrap();
    assert!(
        manager
            .control("S", IndexControlAction::Cancel)
            .await
            .unwrap_err()
            .to_string()
            .contains("retry deadline rejected")
    );
    assert!(control.is_cancelled());
    manager.finish_build_for_control_owned("S", &control, &ownership, None);
    let status = manager.status("S").await.unwrap();
    assert!(
        status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("persist"))
    );
    assert!(
        parse_timestamp(status.scheduler.next_refresh_at.as_deref().unwrap()).unwrap()
            > std::time::SystemTime::now()
    );
    assert!(!manager.automatic_refresh_allowed(&status));
}

#[tokio::test]
async fn operator_cancel_rejects_an_overflowing_interval_before_cancelling() {
    let mut config = settings(PathBuf::from(":memory:"));
    config.refresh_interval_seconds = u64::MAX;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    let error = manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cancellation deadline overflow"));
    assert!(!control.is_cancelled());
    assert!(
        manager
            .with_database(|db| db.retry_state("S"))
            .unwrap()
            .0
            .is_none()
    );
    manager.finish_build_owned("S", &ownership, None);
}

#[tokio::test]
async fn genuine_failure_after_operator_cancel_preserves_the_later_deadline() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    let cancelled_until = manager
        .with_database(|db| db.retry_state("S"))
        .unwrap()
        .0
        .unwrap();
    manager.finish_build_for_control_owned(
        "S",
        &control,
        &ownership,
        Some("native inventory failed".into()),
    );
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.scheduler.consecutive_failures, 1);
    assert_eq!(
        status.last_error.as_deref(),
        Some("native inventory failed")
    );
    assert!(
        parse_timestamp(status.scheduler.retry_after.as_deref().unwrap()).unwrap()
            >= cancelled_until
    );
    assert!(!manager.automatic_refresh_allowed(&status));
}

#[test]
fn retry_state_snapshot_remains_locked_until_persisted() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let database = manager.database.lock().unwrap();
    let persister = Arc::clone(&manager);
    let persisted = std::thread::spawn(move || persister.persist_retry_state("S"));
    let deadline = Instant::now() + Duration::from_secs(2);
    while manager.writer_gate.try_lock().is_ok() {
        assert!(Instant::now() < deadline, "retry persistence did not start");
        std::thread::yield_now();
    }
    let snapshot_locked = manager.runtime.try_lock().is_err();
    drop(database);
    persisted.join().unwrap().unwrap();
    assert!(
        snapshot_locked,
        "cancellation could replace the pending snapshot"
    );
}

#[tokio::test]
async fn stale_scheduler_work_cannot_reenroll_a_deleted_index() {
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let stale_status = manager.status("S").await.unwrap();
    manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    manager.background_tasks.wait_for_idle().await;
    manager
        .refresh_existing_index_if_due("S", &stale_status)
        .await;
    manager.refresh_enrolled("S", false).await.unwrap();
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert_eq!(
        manager.status("S").await.unwrap().state,
        IndexState::NotIndexed
    );
    assert!(!manager.with_database(|db| db.is_enrolled("S")).unwrap());
}

#[tokio::test]
async fn deletion_during_a_scheduler_probe_prevents_reenrollment() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![Ok(default_capabilities())])
            .with_capability_gate(Arc::clone(&started), Arc::clone(&release)),
    );
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let status = manager.status("S").await.unwrap();
    let scheduled = Arc::clone(&manager);
    let task = tokio::spawn(async move {
        scheduled.refresh_existing_index_if_due("S", &status).await;
    });
    started.notified().await;
    manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    manager.background_tasks.wait_for_idle().await;
    release.notify_one();
    task.await.unwrap();
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert!(!manager.with_database(|db| db.is_enrolled("S")).unwrap());
}

#[tokio::test]
async fn stale_profile_probe_does_not_rebuild_a_recreated_generation() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![Ok(default_capabilities())])
            .with_capability_gate(Arc::clone(&started), Arc::clone(&release)),
    );
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Flat,
        BrowseSource::Flat,
        "0",
    );
    let status = manager.status("S").await.unwrap();
    let scheduled = Arc::clone(&manager);
    let task = tokio::spawn(async move {
        scheduled.refresh_existing_index_if_due("S", &status).await;
    });
    started.notified().await;
    manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    manager.background_tasks.wait_for_idle().await;
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    release.notify_one();
    task.await.unwrap();
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert_eq!(manager.status("S").await.unwrap().state, IndexState::Ready);
}

#[tokio::test]
async fn profile_rebuild_cannot_clear_another_owned_builds_active_cache() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Flat,
        BrowseSource::Flat,
        "0",
    );
    let stale_status = manager.status("S").await.unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, control);
    manager
        .refresh_after_profile_change("S", &stale_status)
        .await;
    assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
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
async fn profile_invalidation_preserves_a_newer_completed_generation() {
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Flat,
        BrowseSource::Flat,
        "0",
    );
    let stale_status = manager.status("S").await.unwrap();
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    let current = manager.status("S").await.unwrap();
    manager
        .refresh_after_profile_change("S", &stale_status)
        .await;
    let after = manager.status("S").await.unwrap();
    assert_eq!(after.active_generation, current.active_generation);
    assert_eq!(
        after.scheduler.last_success_at,
        current.scheduler.last_success_at
    );
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert!(!manager.active_builds.lock().unwrap().contains("S"));
    assert!(
        !manager
            .coordination
            .build_owners
            .lock()
            .unwrap()
            .contains_key("S")
    );
}

#[tokio::test]
async fn profile_rebuild_respects_gateway_policy_capacity_and_shutdown() {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    for (enabled, occupied) in [(false, false), (true, true), (true, false)] {
        let client = Arc::new(MockOpcClient::default());
        let mut config = settings(PathBuf::from(":memory:"));
        config.enabled = enabled;
        let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));
        seed_active_generation(
            &manager,
            NamespaceOrganization::Flat,
            BrowseSource::Flat,
            "0",
        );
        let status = manager.status("S").await.unwrap();
        if occupied {
            manager.active_builds.lock().unwrap().insert("Other".into());
        } else if enabled {
            manager.shutdown_background_indexing().await;
            assert!(manager.reserve_refresh_build("S", false).unwrap().is_none());
        }
        manager.refresh_after_profile_change("S", &status).await;
        assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
        assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
        assert!(!manager.active_builds.lock().unwrap().contains("S"));
    }
}

#[tokio::test]
async fn scheduler_recheck_fails_closed_when_a_persisted_deadline_becomes_invalid() {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![Ok(default_capabilities())])
            .with_capability_gate(Arc::clone(&started), Arc::clone(&release)),
    );
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    let status = manager.status("S").await.unwrap();
    let probe = manager.refresh_existing_index_if_due("S", &status);
    let invalidate = async {
        started.notified().await;
        manager
            .with_database(|db| {
                db.connection.execute(
                    "INSERT INTO index_meta(key, value) VALUES ('retry_after:S', 'invalid')",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        release.notify_one();
    };
    tokio::join!(probe, invalidate);
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert!(!manager.active_builds.lock().unwrap().contains("S"));
}

#[tokio::test]
async fn malformed_cancellation_deadline_fails_closed() {
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(PathBuf::from(":memory:")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
    );
    manager
        .with_database(|db| {
            db.connection.execute(
                "INSERT INTO index_meta(key, value) VALUES ('retry_after:S', 'invalid')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(
        manager
            .status("S")
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid persisted")
    );
    manager.refresh_if_due("S").await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
}

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
    assert!(status.scheduler.next_refresh_at.is_none());
    let response = manager.search("Other", "tag", 3, 10).await.unwrap();
    assert!(response.matches.is_empty());
    assert_eq!(response.status.state, IndexState::NotIndexed);

    let unindexed = manager.search("S", "tag", 3, 0).await.unwrap();
    assert!(unindexed.matches.is_empty());
    assert!(unindexed.status.scheduler.next_refresh_at.is_none());

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
        !manager
            .with_database(|db| db.is_enrolled("Typo.Server"))
            .unwrap()
    );

    manager.refresh("Actual.Server", true).await.unwrap();
    wait_for_state(&manager, "Actual.Server", IndexState::Ready).await;
    assert!(
        manager
            .status("Actual.Server")
            .await
            .unwrap()
            .scheduler
            .next_refresh_at
            .is_some()
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
async fn every_usable_index_participates_under_gateway_policy() {
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
        let refreshed = manager.status("S").await.unwrap();
        assert_eq!(
            refreshed.scheduler.next_refresh_at.is_some(),
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
async fn operator_cancellation_preserves_the_searchable_generation() {
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
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert_eq!(status.state, IndexState::Refreshing);
    assert_eq!(status.active_generation, 1);
    assert!(status.scheduler.next_refresh_at.is_some());
    assert!(control.cancelled.load(Ordering::Acquire));
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
async fn an_existing_cache_is_scheduled_without_a_preference_setting() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));

    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    let status = manager.status("S").await.unwrap();
    assert!(status.scheduler.next_refresh_at.is_some());
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
