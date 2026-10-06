use super::*;

#[test]
fn controller_observation_expires_stale_commit_latency() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        ResolvedIndexConfig {
            adaptive_recovery_delay_seconds: 1,
            ..settings(PathBuf::from(":memory:"))
        },
    );
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: None,
                started_at: "test".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: Some(2_000),
            }),
            ..RuntimeState::default()
        },
    );
    manager
        .commit_latency_recorded_at
        .lock()
        .unwrap()
        .insert("S".into(), Instant::now() - Duration::from_secs(2));

    let stale = manager.controller_observation("S", false);
    assert_eq!(stale.database_commit_p95_ms, None);

    manager
        .commit_latency_recorded_at
        .lock()
        .unwrap()
        .insert("S".into(), Instant::now());
    let fresh = manager.controller_observation("S", false);
    assert_eq!(fresh.database_commit_p95_ms, Some(2_000));
}

#[test]
fn reserving_a_new_build_clears_previous_commit_latency_timestamp() {
    let directory = tempdir().unwrap();
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    );
    manager
        .commit_latency_recorded_at
        .lock()
        .unwrap()
        .insert("S".into(), Instant::now());

    let ownership = manager.reserve_refresh_build("S", true).unwrap().unwrap();
    assert!(
        !manager
            .commit_latency_recorded_at
            .lock()
            .unwrap()
            .contains_key("S")
    );
    manager.finish_build_owned("S", &ownership, None);
}

#[test]
fn foreground_metrics_keep_rolling_latency_percentiles() {
    let mut metrics = ForegroundMetricState::default();
    metrics.record_health_at(Instant::now(), 30, false, false, false);
    metrics.record_health_at(Instant::now(), 10, true, true, true);
    metrics.record_health_at(Instant::now(), 20, false, false, false);
    let snapshot = metrics.snapshot(2);
    assert_eq!(snapshot.active_count, 2);
    assert_eq!(snapshot.operations, 3);
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.bad_quality, 1);
    assert_eq!(snapshot.latency_p50_ms, Some(20));
    assert_eq!(snapshot.latency_p95_ms, Some(30));
    assert_eq!(snapshot.latency_max_ms, Some(30));
    assert!(!snapshot.last_error);
}

#[test]
fn foreground_health_failures_expire_without_a_follow_up_operation() {
    let recorded_at = Instant::now();
    let mut metrics = ForegroundMetricState::default();
    metrics.record_health_at(recorded_at, 10, true, true, true);
    assert!(
        metrics.recent_health_failure(recorded_at + Duration::from_secs(1), Duration::from_secs(2))
    );
    assert!(
        !metrics
            .recent_health_failure(recorded_at + Duration::from_secs(3), Duration::from_secs(2))
    );
}

#[test]
fn foreground_bad_quality_expires_without_a_follow_up_operation() {
    let recorded_at = Instant::now();
    let mut metrics = ForegroundMetricState::default();
    metrics.record_health_at(recorded_at, 10, false, true, false);
    assert!(
        metrics.recent_bad_quality(recorded_at + Duration::from_secs(1), Duration::from_secs(2))
    );
    assert!(
        !metrics.recent_bad_quality(recorded_at + Duration::from_secs(3), Duration::from_secs(2))
    );
}

#[test]
fn controller_observation_includes_recent_bad_quality() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    manager.record_foreground_operation_with_health(
        "S",
        Duration::from_millis(10),
        false,
        true,
        false,
    );

    let observation = manager.controller_observation("S", false);
    assert!(observation.foreground_bad_quality);
    assert!(!observation.foreground_error);
}

#[test]
fn storage_diagnostics_include_sqlite_sidecars() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let db = IndexDb::open(&path).unwrap();
    drop(db);
    std::fs::write(IndexDb::sqlite_sidecar_path(&path, "-wal"), vec![0_u8; 7]).unwrap();
    std::fs::write(IndexDb::sqlite_sidecar_path(&path, "-shm"), vec![0_u8; 11]).unwrap();
    let storage = storage_diagnostics_for_path(&path);
    assert_eq!(storage.wal_bytes, 7);
    assert_eq!(storage.shm_bytes, 11);
    assert!(storage.free_bytes.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn restart_during_refresh_keeps_active_status_ready() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("restart-status.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        )
        .unwrap();
    db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
        .unwrap();
    db.promote("S", active, &timestamp_now(), &completed_progress(1))
        .unwrap();
    let interrupted = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            &timestamp_now(),
        )
        .unwrap();
    db.insert_entries(
        "S",
        interrupted,
        &[inventory_entry("Interrupted", "S.Interrupted")],
    )
    .unwrap();
    drop(db);

    let manager = IndexManager::new(Arc::new(MockOpcClient::default()), settings(path));
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Ready);
    assert_eq!(status.active_generation, active);
    assert!(status.last_error.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn older_failed_generations_do_not_poison_active_status() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("older-failed.sqlite3")),
    ));
    let active = manager
        .with_database(|db| {
            let failed =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", failed, &[inventory_entry("Failed", "S.Failed")])?;
            db.fail_generation("S", failed, "old refresh failed")?;
            let active =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "2")?;
            db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])?;
            db.promote("S", active, &timestamp_now(), &completed_progress(1))?;
            Ok(active)
        })
        .unwrap();

    let status = manager.status("S").await.unwrap();
    assert_eq!(status.active_generation, active);
    assert!(matches!(
        status.state,
        IndexState::Ready | IndexState::Stale
    ));
    assert_eq!(status.last_error, None);
}

#[tokio::test]
async fn background_delay_and_refresh_handle_partial_status_errors_and_unconfigured_servers() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: None,
                started_at: "1".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: None,
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );
    assert_eq!(
        manager.background_refresh_delay("S").await,
        Duration::from_secs(3600)
    );
    manager.refresh_if_due("S").await;

    manager.runtime.lock().unwrap().clear();
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: None,
                started_at: "2".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: None,
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );
    assert_eq!(
        manager.status("S").await.unwrap().state,
        IndexState::Refreshing
    );
    manager.refresh_if_due("S").await;

    manager.runtime.lock().unwrap().clear();
    manager
        .with_database(|db| {
            drop_table(db, "generations");
            Ok(())
        })
        .unwrap();
    assert_eq!(
        manager.background_refresh_delay("S").await,
        retry_delay("S", 1, false, 300)
    );
    manager.refresh_if_due("S").await;

    manager.refresh_if_due("Other").await;
}

#[tokio::test]
async fn manager_status_covers_partial_stale_refreshing_and_runtime_errors() {
    let directory = tempdir().unwrap();
    let mut index_settings = settings(directory.path().join("index.sqlite3"));
    index_settings.sentinel_tag = Some("Health.PV".into());
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        index_settings,
    ));
    let not_indexed = manager.status("S").await.unwrap();
    assert_eq!(not_indexed.state, IndexState::NotIndexed);
    assert!(not_indexed.sentinel_configured);
    assert_eq!(not_indexed.health, HealthProbeState::Unavailable);

    let generation = manager
        .with_database(|db| {
            let generation = db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )?;
            db.update_progress(
                "S",
                generation,
                &InventoryProgress {
                    branches_visited: 2,
                    entries_seen: 3,
                    unique_items: 2,
                    active_time_ms: 4,
                    paused_time_ms: 5,
                    items_per_second: 6.0,
                    estimated_remaining_ms: Some(7),
                },
            )?;
            Ok(generation)
        })
        .unwrap();
    let partial = manager.status("S").await.unwrap();
    assert_eq!(partial.state, IndexState::Partial);
    assert_eq!(partial.entry_count, 3);

    manager
        .with_database(|db| {
            db.insert_entries("S", generation, &[inventory_entry("Persisted", "S.Tag")])?;
            db.promote("S", generation, "0", &zero_progress())
        })
        .unwrap();
    let stale = manager.status("S").await.unwrap();
    assert_eq!(stale.state, IndexState::Stale);
    assert_eq!(stale.active_generation, generation);

    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: Some(zero_progress()),
                started_at: "runtime-start".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: Some("obsolete build failure".into()),
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );
    let refreshing = manager.status("S").await.unwrap();
    assert_eq!(refreshing.state, IndexState::Refreshing);
    assert_eq!(refreshing.started_at.as_deref(), Some("runtime-start"));
    assert!(refreshing.progress.is_some());
    assert_ne!(
        refreshing.last_error.as_deref(),
        Some("obsolete build failure")
    );
    manager.mark_promoting("S").unwrap();
    assert_eq!(
        manager.status("S").await.unwrap().state,
        IndexState::Promoting
    );
    manager.clear_promoting("S");

    {
        let mut runtime = manager.runtime.lock().unwrap();
        let state = runtime.get_mut("S").unwrap();
        state.build = None;
        state.last_error = Some("runtime failure".into());
    }
    let failed = manager.status("S").await.unwrap();
    assert_eq!(failed.state, IndexState::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("runtime failure"));

    manager.with_database(|db| db.clear_server("S")).unwrap();
    {
        let mut runtime = manager.runtime.lock().unwrap();
        let state = runtime.get_mut("S").unwrap();
        state.last_error = None;
        state.build = Some(RuntimeBuild {
            control: None,
            progress: Some(InventoryProgress {
                branches_visited: 1,
                entries_seen: 8,
                unique_items: 7,
                active_time_ms: 2,
                paused_time_ms: 3,
                items_per_second: 4.0,
                estimated_remaining_ms: None,
            }),
            started_at: "runtime-only".into(),
            foreground_users: 0,
            operator_paused: false,
            quiet_until: None,
            effective_limits: None,
            controller_state: None,
            pause_reason: None,
            recovery_deadline: None,
            last_commit_latency_ms: None,
        });
    }
    let runtime_only = manager.status("S").await.unwrap();
    assert_eq!(runtime_only.state, IndexState::Partial);
    assert_eq!(runtime_only.entry_count, 8);
    assert_eq!(runtime_only.unique_item_count, 7);

    manager.runtime.lock().unwrap().clear();
    manager
        .with_database(|db| {
            let generation = db.start_generation(
                "S",
                NamespaceOrganization::Flat,
                BrowseSource::Flat,
                "failed-start",
            )?;
            db.fail_generation("S", generation, "persisted failure")
        })
        .unwrap();
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: None,
                started_at: "failed-runtime".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: None,
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );
    let failed_build = manager.status("S").await.unwrap();
    assert_eq!(failed_build.state, IndexState::Failed);
    assert_eq!(
        failed_build.last_error.as_deref(),
        Some("persisted failure")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_during_promotion_does_not_wait_for_database_mutex() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| {
            let generation = db
                .start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
                .unwrap();
            db.update_progress("S", generation, &zero_progress())
                .unwrap();
            Ok(())
        })
        .unwrap();
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: None,
                progress: Some(zero_progress()),
                started_at: "runtime-start".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: None,
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );
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
    let status_manager = Arc::clone(&manager);
    let status_task = tokio::spawn(async move { status_manager.status("S").await });
    let status = tokio::time::timeout(Duration::from_secs(1), status_task)
        .await
        .expect("promotion status should not wait for the writer mutex")
        .expect("status task should not panic")
        .unwrap();
    assert_eq!(status.state, IndexState::Promoting);
    release_tx.send(()).unwrap();
    lock_thread.join().unwrap();
}

#[tokio::test]
async fn refresh_pauses_for_existing_foreground_work_and_control_without_build_is_a_noop() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let stream_started = Arc::new(Notify::new());
    let stream_release = Arc::new(Notify::new());
    let handle = InventoryHandle {
        stream: Box::new(BlockingInventoryStream {
            started: Arc::clone(&stream_started),
            release: Arc::clone(&stream_release),
            event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: true,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            }))),
        }),
        control: control.clone(),
    };
    let manager = Arc::new(IndexManager::new(
        Arc::new(LifecycleClient::new(vec![Ok(handle)], vec![])),
        settings(directory.path().join("index.sqlite3")),
    ));

    let guard = manager.foreground_guard("S");
    manager.refresh("S", true).await.unwrap();
    stream_started.notified().await;
    assert!(control.paused.load(Ordering::Acquire));
    drop(guard);
    stream_release.notify_one();
    wait_for_state(&manager, "S", IndexState::NotIndexed).await;

    assert_eq!(
        manager
            .control("S", IndexControlAction::Pause)
            .await
            .unwrap()
            .state,
        IndexState::NotIndexed
    );
    assert!(manager.refresh("Other", true).await.is_err());
    assert!(
        manager
            .control("Other", IndexControlAction::Cancel)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn build_terminal_error_and_cancellation_paths_preserve_consistent_status() {
    async fn run_case(
        events: VecDeque<anyhow::Result<InventoryEvent>>,
        maintenance_windows: Vec<String>,
        expected_state: IndexState,
        expected_error: Option<&str>,
    ) {
        let directory = tempdir().unwrap();
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                events,
                Arc::new(RecordingInventoryControl::default()),
            ))],
            vec![],
        ));
        let mut config = settings(directory.path().join("index.sqlite3"));
        config.maintenance_windows = maintenance_windows;
        let manager = Arc::new(IndexManager::new(client, config));
        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", expected_state).await;
        let status = manager.status("S").await.unwrap();
        assert_eq!(status.last_error.as_deref(), expected_error);
    }

    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    run_case(
        VecDeque::new(),
        vec![],
        IndexState::Failed,
        Some("inventory stream ended before completion"),
    )
    .await;
    run_case(
        VecDeque::from([Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: false,
            cancelled: false,
            truncated: true,
            warning: Some("truncated by server".into()),
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        }))]),
        vec![],
        IndexState::Failed,
        Some("truncated by server"),
    )
    .await;
    run_case(
        VecDeque::from([Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: false,
            cancelled: true,
            truncated: false,
            warning: None,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        }))]),
        vec![],
        IndexState::NotIndexed,
        None,
    )
    .await;
    run_case(
        VecDeque::from([
            Ok(InventoryEvent::Entry(InventoryEntry {
                display_name: "Invalid".into(),
                item_id: String::new(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec![],
            })),
            Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: true,
                cancelled: false,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            })),
        ]),
        vec![],
        IndexState::Failed,
        Some("inventory entry has an empty ItemID"),
    )
    .await;
    run_case(
        VecDeque::from([Err(anyhow::anyhow!("inventory stream failed"))]),
        vec![],
        IndexState::Failed,
        Some("inventory stream failed"),
    )
    .await;
    run_case(
        VecDeque::new(),
        vec!["invalid".into()],
        IndexState::Failed,
        Some("maintenance window must use HH:MM-HH:MM"),
    )
    .await;
}

#[tokio::test]
async fn controls_foreground_quiet_period_and_concurrency_coordinate_builds() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let stream_started = Arc::new(Notify::new());
    let stream_release = Arc::new(Notify::new());
    let blocking = InventoryHandle {
        stream: Box::new(BlockingInventoryStream {
            started: Arc::clone(&stream_started),
            release: Arc::clone(&stream_release),
            event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                complete: false,
                cancelled: true,
                truncated: false,
                warning: None,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            }))),
        }),
        control: control.clone(),
    };
    let client = Arc::new(LifecycleClient::new(vec![Ok(blocking)], vec![]));
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.quiet_period_seconds = 0;
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

    manager.refresh("S", true).await.unwrap();
    stream_started.notified().await;
    let starts = client.inventory_start_count.load(Ordering::Relaxed);
    assert_eq!(starts, 1);
    assert_eq!(
        manager.refresh("S", true).await.unwrap().state,
        IndexState::Partial
    );
    assert_eq!(
        manager.refresh("T", true).await.unwrap_err().to_string(),
        "namespace index build concurrency limit reached"
    );
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), starts);

    let baseline_pauses = control.pause_count.load(Ordering::Relaxed);
    manager
        .control("S", IndexControlAction::Pause)
        .await
        .unwrap();
    assert!(control.pause_count.load(Ordering::Relaxed) > baseline_pauses);
    assert!(control.paused.load(Ordering::Acquire));

    let baseline_resumes = control.resume_count.load(Ordering::Relaxed);
    manager
        .control("S", IndexControlAction::Resume)
        .await
        .unwrap();
    assert!(control.resume_count.load(Ordering::Relaxed) > baseline_resumes);

    let guard = manager.foreground_guard("S");
    assert!(control.paused.load(Ordering::Acquire));
    let resumes_during_foreground = control.resume_count.load(Ordering::Relaxed);
    manager
        .control("S", IndexControlAction::Resume)
        .await
        .unwrap();
    assert_eq!(
        control.resume_count.load(Ordering::Relaxed),
        resumes_during_foreground
    );
    drop(guard);
    wait_for_counter(&control.resume_count, resumes_during_foreground + 1).await;

    let guard = manager.foreground_guard("S");
    manager
        .control("S", IndexControlAction::Pause)
        .await
        .unwrap();
    let resumes_while_operator_paused = control.resume_count.load(Ordering::Relaxed);
    drop(guard);
    tokio::task::yield_now().await;
    assert_eq!(
        control.resume_count.load(Ordering::Relaxed),
        resumes_while_operator_paused
    );
    manager
        .control("S", IndexControlAction::Resume)
        .await
        .unwrap();

    manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert!(control.cancelled.load(Ordering::Acquire));
    stream_release.notify_one();
    wait_for_state(&manager, "S", IndexState::NotIndexed).await;
}

#[test]
fn foreground_guard_resumes_synchronously_without_a_tokio_runtime() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: Some(trait_control),
                progress: None,
                started_at: "1".into(),
                foreground_users: 0,
                operator_paused: false,
                quiet_until: None,
                effective_limits: None,
                controller_state: None,
                pause_reason: None,
                recovery_deadline: None,
                last_commit_latency_ms: None,
            }),
            retry_after: None,
            last_error: None,
            consecutive_failures: 0,
            circuit_open: false,
            health: HealthProbeState::Unavailable,
            sentinel_checked_at: None,
        },
    );

    let guard = manager.foreground_guard("S");
    assert!(control.paused.load(Ordering::Acquire));
    drop(guard);
    assert!(!control.paused.load(Ordering::Acquire));
    assert!(control.resume_count.load(Ordering::Relaxed) > 0);

    manager.foreground_end("Missing");
    manager.finish_build("Missing", None);

    let poisoned = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("poisoned.sqlite3")),
    ));
    let foreground_users = Arc::clone(&poisoned.foreground_users);
    let _ = std::panic::catch_unwind(move || {
        let _guard = foreground_users.lock().unwrap();
        panic!("poison foreground users for cleanup error-path coverage");
    });
    poisoned.foreground_end("S");
}

#[tokio::test]
async fn pause_overlays_compose_and_are_visible_in_runtime_status() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, control.clone());

    manager.set_pause_overlay("S", None, Some(true));
    let status = manager.status("S").await.unwrap();
    assert_eq!(
        status.pause_reason,
        Some(crate::controller::PauseReason::OpcHealth)
    );
    assert!(control.paused.load(Ordering::Acquire));

    manager.set_pause_overlay("S", Some(true), None);
    let status = manager.status("S").await.unwrap();
    assert_eq!(
        status.pause_reason,
        Some(crate::controller::PauseReason::Maintenance)
    );

    manager.set_pause_overlay("S", Some(false), Some(false));
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.pause_reason, None);
    assert!(!control.paused.load(Ordering::Acquire));
}

#[tokio::test]
async fn foreground_cleanup_without_a_runtime_build_is_safe() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.foreground_end("Missing");
    tokio::time::sleep(Duration::from_millis(1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_status_and_search_remain_responsive_while_writer_gate_is_held() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("read-only-gate.sqlite3")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    let (locked, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release, release_rx) = std::sync::mpsc::sync_channel(0);
    let gate_manager = Arc::clone(&manager);
    let gate_thread = std::thread::spawn(move || {
        let _writer_guard = gate_manager.writer_gate.lock().unwrap();
        locked.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.recv().unwrap();

    let status = tokio::time::timeout(Duration::from_secs(1), manager.status("S"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.active_generation, 1);
    let search = tokio::time::timeout(
        Duration::from_secs(1),
        manager.search("S", "persisted", 3, 10),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(search.matches.len(), 1);
    release.send(()).unwrap();
    gate_thread.join().unwrap();
}

#[tokio::test]
async fn failed_background_delete_is_recorded_in_status() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("failed-delete.sqlite3");
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
    manager
        .with_database(|db| {
            db.connection.execute_batch("DROP TABLE entries_fts")?;
            Ok(())
        })
        .unwrap();

    drop(held_lock);
    manager.background_tasks.wait_for_idle().await;
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Failed);
    assert!(!status.auto_refresh_enabled);
    assert!(
        status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("entries_fts"))
    );
}

#[tokio::test]
async fn status_promotion_read_failure_and_health_variants_are_safe() {
    let directory = tempdir().unwrap();
    let mut promotion_config = settings(directory.path().to_path_buf());
    promotion_config.database_path = directory.path().to_path_buf();
    let promotion = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        promotion_config,
    ));
    let promotion_control: Arc<dyn InventoryControl> =
        Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&promotion, promotion_control);
    promotion.mark_promoting("S").unwrap();
    let status = promotion.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Promoting);
    assert!(status.last_error.is_some());

    let non_promoting = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().to_path_buf()),
    );
    assert!(non_promoting.status("S").await.is_err());

    let client = Arc::new(MockOpcClient::default());
    *client.read_tag_values_result.lock().unwrap() = Ok(Vec::new());
    let mut config = settings(directory.path().join("health.sqlite3"));
    config.sentinel_tag = Some("Health.PV".into());
    let manager = Arc::new(IndexManager::new(client, config));
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel_on_pause();
    let control: Arc<dyn InventoryControl> = control;
    insert_runtime_build(&manager, Arc::clone(&control));
    let mut next_probe = Instant::now();
    let mut backoff = Duration::from_secs(1);
    assert!(
        !manager
            .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
            .await
    );
    assert_eq!(
        manager.status("S").await.unwrap().health,
        HealthProbeState::Unhealthy
    );

    let good_client = Arc::new(MockOpcClient::default());
    *good_client.read_tag_values_result.lock().unwrap() = Ok(vec![TagValue {
        tag_id: "Health.PV".into(),
        value: "1".into(),
        quality: "Good".into(),
        timestamp: "0".into(),
    }]);
    let mut good_config = settings(directory.path().join("good-health.sqlite3"));
    good_config.sentinel_tag = Some("Health.PV".into());
    let good = Arc::new(IndexManager::new(good_client, good_config));
    let good_control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&good, Arc::clone(&good_control));
    let mut next_probe = Instant::now();
    let mut backoff = Duration::from_secs(1);
    assert!(
        good.wait_for_health(&good_control, "S", &mut next_probe, &mut backoff)
            .await
    );
    assert_eq!(
        good.status("S").await.unwrap().health,
        HealthProbeState::Healthy
    );
}

#[test]
fn public_metric_and_invalid_maintenance_helpers_are_exercised() {
    struct FixedHostMetrics;

    impl HostMetricsProvider for FixedHostMetrics {
        fn snapshot(&self) -> HostMetrics {
            HostMetrics {
                cpu_percent: Some(1.0),
                available_memory_percent: Some(99.0),
                disk_active_percent: Some(2.0),
                disk_queue: Some(0.0),
                ..HostMetrics::default()
            }
        }

        fn latest(&self) -> HostMetrics {
            self.snapshot()
        }
    }

    let fixed = FixedHostMetrics;
    assert_eq!(fixed.latest(), fixed.snapshot());

    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    )
    .with_host_metrics_provider(Arc::new(fixed));
    manager.record_foreground_operation("S", Duration::from_millis(2), true, true);
    let observation = manager.controller_observation("S", false);
    assert!(observation.foreground_error);
    assert!(observation.foreground_bad_quality);
    assert_eq!(observation.host_cpu_percent, Some(1.0));

    let mut invalid = settings(PathBuf::from(":memory:"));
    invalid.maintenance_windows = vec!["not-a-window".into()];
    let invalid = IndexManager::new(Arc::new(MockOpcClient::default()), invalid);
    assert!(!invalid.maintenance_window_is_open());
}

#[tokio::test]
async fn split_status_reports_unenrolled_deletion_and_promotion_read_errors() {
    let directory = tempdir().unwrap();
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("status-split.sqlite3")),
    );
    manager
        .deletion_errors
        .lock()
        .unwrap()
        .insert("S".into(), "injected delete failure".into());
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Failed);
    assert!(
        status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("injected delete failure"))
    );

    let promoting = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().to_path_buf()),
    );
    promoting.mark_promoting("S").unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .finish();
    let (rows, error) = tracing::subscriber::with_default(subscriber, || {
        promoting.load_status_rows("S", true).unwrap()
    });
    assert!(rows.is_empty());
    assert!(error.is_some());
}
