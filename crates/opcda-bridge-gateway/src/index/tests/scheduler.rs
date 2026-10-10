use super::*;

#[test]
fn database_coordination_key_handles_relative_and_unresolvable_paths() {
    let directory = tempdir().unwrap();
    let absolute = directory.path().join("index.sqlite3");
    assert_eq!(
        database_coordination_key(Path::new(":memory:"), std::env::current_dir),
        PathBuf::from(":memory:")
    );
    assert_eq!(canonical_database_path(Path::new("")), PathBuf::from(""));
    assert_eq!(
        database_coordination_key(&absolute, std::env::current_dir),
        canonical_database_path(&absolute)
    );
    assert_eq!(
        database_coordination_key(Path::new("index.sqlite3"), || {
            Ok(PathBuf::from("/database"))
        }),
        PathBuf::from("/database/index.sqlite3")
    );
    assert_eq!(
        database_coordination_key(Path::new("index.sqlite3"), || {
            Err(std::io::Error::other("current directory unavailable"))
        }),
        PathBuf::from("index.sqlite3")
    );
}

#[test]
fn database_coordination_reuses_the_same_identity_for_path_aliases() {
    let directory = tempdir().unwrap();
    let nested = directory.path().join("nested");
    fs::create_dir(&nested).unwrap();
    let database = directory.path().join("index.sqlite3");
    fs::write(&database, []).unwrap();
    let alias = nested.join("..").join("index.sqlite3");

    let canonical_coordination = database_coordination(&database);
    let aliased_coordination = database_coordination(&alias);

    assert!(Arc::ptr_eq(&canonical_coordination, &aliased_coordination));
    assert_eq!(
        build_lock_path(&database, "S"),
        build_lock_path(&alias, "S")
    );
}

#[test]
fn in_memory_databases_do_not_share_coordination() {
    let first = database_coordination(Path::new(":memory:"));
    let second = database_coordination(Path::new(":memory:"));

    assert!(!Arc::ptr_eq(&first, &second));
}

#[test]
fn retry_state_round_trips_through_index_meta() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let db = IndexDb::open(&path).unwrap();
    let retry_after = Some(SystemTime::now() + Duration::from_secs(30));
    db.set_retry_state("S", retry_after, 3, true).unwrap();
    let (_, failures, circuit_open) = db.retry_state("S").unwrap();
    assert_eq!(failures, 3);
    assert!(circuit_open);
}

#[test]
fn build_file_lock_is_exclusive_and_reusable() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let lock = BuildFileLock::acquire_with(&database, "S", |_file, _metadata| Ok(())).unwrap();
    assert!(build_lock_path(&database, "S").exists());
    #[cfg(windows)]
    assert!(build_owner_path(&database, "S").exists());
    assert!(BuildFileLock::is_held(&database, "S").unwrap());
    assert!(!BuildFileLock::is_held(&database, "T").unwrap());
    let error = BuildFileLock::acquire(&database, "S").unwrap_err();
    assert!(error.to_string().contains("build lock is already held"));
    drop(lock);
    assert!(!BuildFileLock::is_held(&database, "S").unwrap());
    assert!(build_lock_path(&database, "S").exists());
    #[cfg(windows)]
    assert!(!build_owner_path(&database, "S").exists());
    let other_server_lock = BuildFileLock::acquire(&database, "T").unwrap();
    assert_ne!(
        build_lock_path(&database, "T"),
        build_lock_path(&database, "S"),
        "different servers must not share a build lock"
    );
    drop(other_server_lock);
    fs::write(build_lock_path(&database, "S"), "stale process metadata\n").unwrap();
    let replacement = BuildFileLock::acquire(&database, "S").unwrap();
    drop(replacement);
}

#[test]
fn build_file_lock_reports_initialization_and_cleanup_errors() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let error = BuildFileLock::acquire_with(&database, "S", |_file, _metadata| {
        Err(std::io::Error::other("lock metadata write failed"))
    })
    .unwrap_err();
    assert!(error.to_string().contains("lock metadata write failed"));
    assert!(build_lock_path(&database, "S").exists());

    #[cfg(unix)]
    {
        let error =
            BuildFileLock::acquire(Path::new("/proc/opcda-bridge-index.sqlite3"), "S").unwrap_err();
        assert!(!error.to_string().is_empty());
    }

    #[cfg(unix)]
    {
        let lock = BuildFileLock::acquire(&database, "S").unwrap();
        let lock_path = build_lock_path(&database, "S");
        fs::remove_file(&lock_path).unwrap();
        fs::create_dir(&lock_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, || drop(lock));
        assert!(lock_path.is_dir());
        fs::remove_dir(lock_path).unwrap();
    }
}

#[test]
fn build_file_lock_treats_open_conflicts_as_held() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let error = BuildFileLock::acquire_with_open(
        &database,
        "S",
        |lock_path| {
            fs::write(lock_path, [])?;
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        },
        |_file, _metadata| Ok(()),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("namespace index build lock is already held")
    );
    assert!(error.to_string().contains("operation would block"));
}

#[test]
fn build_file_lock_preserves_owner_details_for_open_conflicts() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let error = BuildFileLock::acquire_with_open(
        &database,
        "S",
        |lock_path| {
            fs::write(lock_path, "external owner metadata")?;
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        },
        |_file, _metadata| Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("external owner metadata"));
}

#[test]
fn build_file_lock_propagates_non_conflict_probe_errors() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let lock = BuildFileLock::acquire(&database, "S").unwrap();
    let error = BuildFileLock::is_held_with(&database, "S", |_| {
        Err(std::io::Error::other("lock probe failed"))
    })
    .unwrap_err();
    assert_eq!(error.to_string(), "lock probe failed");
    drop(lock);
}

#[test]
fn scheduled_refresh_jitter_is_deterministic_and_bounded() {
    assert_eq!(deterministic_jitter("S", 0), Duration::ZERO);
    assert_eq!(
        deterministic_jitter("S", 3600),
        deterministic_jitter("S", 3600)
    );
    assert!(deterministic_jitter("S", 3600) <= Duration::from_secs(3600));
}

#[test]
fn maintenance_windows_parse_and_match_day_boundaries() {
    let daytime = MaintenanceWindow::parse("08:30-17:00").unwrap();
    assert!(daytime.contains(8 * 60 + 30));
    assert!(!daytime.contains(17 * 60));

    let overnight = MaintenanceWindow::parse("22:00-06:00").unwrap();
    assert!(overnight.contains(23 * 60));
    assert!(overnight.contains(5 * 60 + 59));
    assert!(!overnight.contains(12 * 60));

    let all_day = MaintenanceWindow::parse("00:00-00:00").unwrap();
    assert!(all_day.contains(12 * 60));

    assert!(MaintenanceWindow::parse("bad").is_err());
    assert!(MaintenanceWindow::parse("aa:00-01:00").is_err());
    assert!(MaintenanceWindow::parse("01:aa-02:00").is_err());
    assert!(MaintenanceWindow::parse("25:00-01:00").is_err());
    assert!(MaintenanceWindow::parse("01:60-02:00").is_err());
    assert!(parse_maintenance_windows(&["08:00-09:00".into()]).is_ok());

    let now = chrono::Local
        .with_ymd_and_hms(2026, 1, 1, 9, 0, 0)
        .single()
        .unwrap();
    assert!(maintenance_window_active(&[daytime], now));
    assert!(!maintenance_window_active(&[], now));
}

#[test]
fn cleanup_stops_before_writing_when_shutdown_is_requested() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-shutdown.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    db.promote("S", active, "0", &completed_progress(0))
        .unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.fail_generation("S", generation, "obsolete").unwrap();
    drop(db);

    let background_tasks = BackgroundTasks::new();
    background_tasks.request_shutdown();
    let stats = cleanup_obsolete_generations(&path, "S", &background_tasks).unwrap();

    assert!(stats.stopped_for_shutdown);
    assert_eq!(stats.batches, 0);
    assert_eq!(
        IndexDb::open(&path)
            .unwrap()
            .status_rows("S")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn cleanup_stops_when_obsolete_data_disappears_before_batch_delete() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-no-progress.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    db.promote("S", active, "0", &completed_progress(0))
        .unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.fail_generation("S", generation, "obsolete").unwrap();
    drop(db);

    let background_tasks = Arc::new(BackgroundTasks::new());
    let (started, release) = background_tasks.install_cleanup_batch_hook();
    let cleanup_path = path.clone();
    let cleanup_tasks = Arc::clone(&background_tasks);
    let cleanup = std::thread::spawn(move || {
        cleanup_obsolete_generations(&cleanup_path, "S", cleanup_tasks.as_ref())
    });

    started.recv().unwrap();
    let remover = Connection::open(&path).unwrap();
    remover
        .execute("DELETE FROM generations WHERE server = 'S'", [])
        .unwrap();
    drop(remover);
    release.send(()).unwrap();

    let stats = cleanup.join().unwrap().unwrap();
    assert_eq!(stats.batches, 1);
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.fts_entries, 0);
    assert_eq!(stats.generations, 0);
    assert!(
        IndexDb::open(&path)
            .unwrap()
            .status_rows("S")
            .unwrap()
            .is_empty()
    );
    background_tasks.wait_for_cleanup_batch_hook();
}

#[test]
fn cleanup_checkpoint_defers_for_builds_and_tolerates_poisoned_locks() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-checkpoint.sqlite3");
    let connection = Connection::open(&path).unwrap();
    let writer_gate = Mutex::new(());
    let active_builds = Mutex::new(HashSet::new());

    assert!(cleanup_checkpoint(&connection, &writer_gate, &active_builds, &path, "S",).is_ok());

    active_builds.lock().unwrap().insert("S".into());
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        assert!(
            cleanup_checkpoint(&connection, &writer_gate, &active_builds, &path, "S",).is_err()
        );
    });
    active_builds.lock().unwrap().clear();

    let poisoned_builds = Arc::new(Mutex::new(HashSet::new()));
    let poison_builds = Arc::clone(&poisoned_builds);
    std::thread::spawn(move || {
        let _guard = poison_builds.lock().unwrap();
        panic!("poison cleanup checkpoint active-build lock");
    })
    .join()
    .unwrap_err();
    assert!(
        cleanup_checkpoint(
            &connection,
            &writer_gate,
            poisoned_builds.as_ref(),
            &path,
            "S",
        )
        .is_err()
    );

    let poisoned_gate = Arc::new(Mutex::new(()));
    let poison_gate = Arc::clone(&poisoned_gate);
    std::thread::spawn(move || {
        let _guard = poison_gate.lock().unwrap();
        panic!("poison cleanup checkpoint writer lock");
    })
    .join()
    .unwrap_err();
    assert!(
        cleanup_checkpoint(
            &connection,
            poisoned_gate.as_ref(),
            &active_builds,
            &path,
            "S",
        )
        .is_err()
    );
}

#[test]
fn cleanup_rechecks_build_state_after_waiting_for_the_writer_gate() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-gate-recheck.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    db.promote("S", active, "0", &completed_progress(0))
        .unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.fail_generation("S", generation, "obsolete").unwrap();
    drop(db);

    let background_tasks = Arc::new(BackgroundTasks::new());
    let (started, release) = background_tasks.install_cleanup_writer_gate_hook();
    let writer_gate = Arc::new(Mutex::new(()));
    let active_builds = Arc::new(Mutex::new(HashSet::new()));
    let cleanup_path = path.clone();
    let cleanup_tasks = Arc::clone(&background_tasks);
    let cleanup_writer_gate = Arc::clone(&writer_gate);
    let cleanup_active_builds = Arc::clone(&active_builds);
    let cleanup = std::thread::spawn(move || {
        let subscriber = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            cleanup_obsolete_generations_coordinated(
                &cleanup_path,
                "S",
                cleanup_tasks.as_ref(),
                cleanup_writer_gate,
                cleanup_active_builds,
            )
        })
    });

    started.recv().unwrap();
    active_builds.lock().unwrap().insert("T".into());
    release.send(()).unwrap();
    let stats = cleanup.join().unwrap().unwrap();
    assert_eq!(stats.batches, 0);
    assert!(stats.deferred_for_build);
    assert_eq!(
        IndexDb::open(&path)
            .unwrap()
            .status_rows("S")
            .unwrap()
            .len(),
        2
    );
    background_tasks.wait_for_cleanup_writer_gate_hook();
}

#[test]
fn cleanup_rechecks_obsolete_data_after_waiting_for_the_writer_gate() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-obsolete-recheck.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    db.promote("S", active, "0", &completed_progress(0))
        .unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.fail_generation("S", generation, "obsolete").unwrap();
    drop(db);

    let background_tasks = Arc::new(BackgroundTasks::new());
    let (started, release) = background_tasks.install_cleanup_writer_gate_hook();
    let writer_gate = Arc::new(Mutex::new(()));
    let active_builds = Arc::new(Mutex::new(HashSet::new()));
    let cleanup_path = path.clone();
    let cleanup_tasks = Arc::clone(&background_tasks);
    let cleanup_writer_gate = Arc::clone(&writer_gate);
    let cleanup_active_builds = Arc::clone(&active_builds);
    let cleanup = std::thread::spawn(move || {
        cleanup_obsolete_generations_coordinated(
            &cleanup_path,
            "S",
            cleanup_tasks.as_ref(),
            cleanup_writer_gate,
            cleanup_active_builds,
        )
    });

    started.recv().unwrap();
    let remover = Connection::open(&path).unwrap();
    remover
        .execute("DELETE FROM generations WHERE server = 'S'", [])
        .unwrap();
    drop(remover);
    release.send(()).unwrap();
    let stats = cleanup.join().unwrap().unwrap();
    assert_eq!(stats.batches, 0);
    assert!(!stats.deferred_for_build);
    assert!(
        IndexDb::open(&path)
            .unwrap()
            .status_rows("S")
            .unwrap()
            .is_empty()
    );
    background_tasks.wait_for_cleanup_writer_gate_hook();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_first_build_waits_for_the_shared_writer_gate() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-build-gate.sqlite3");
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(client, settings(path.clone())));
    manager
        .with_database(|db| {
            let active =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", active, "0", &completed_progress(0))?;
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.fail_generation("S", generation, "obsolete")?;
            Ok(())
        })
        .unwrap();
    let (cleanup_started, cleanup_release) = manager.background_tasks.install_cleanup_batch_hook();
    manager.schedule_cleanup("S");
    tokio::task::spawn_blocking(move || cleanup_started.recv().unwrap())
        .await
        .unwrap();

    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!refresh.is_finished());

    cleanup_release.send(()).unwrap();
    refresh.await.unwrap().unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    manager.background_tasks.wait_for_cleanup_batch_hook();
    assert_eq!(manager.status("S").await.unwrap().active_generation, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_and_build_share_the_writer_gate_across_servers() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cross-server-gate.sqlite3");
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(client, settings(path.clone())));
    manager
        .with_database(|db| {
            let active =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", active, "0", &completed_progress(0))?;
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.fail_generation("S", generation, "obsolete")?;
            Ok(())
        })
        .unwrap();
    let (cleanup_started, cleanup_release) = manager.background_tasks.install_cleanup_batch_hook();
    manager.schedule_cleanup("S");
    tokio::task::spawn_blocking(move || cleanup_started.recv().unwrap())
        .await
        .unwrap();

    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("T", true).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!refresh.is_finished());
    cleanup_release.send(()).unwrap();
    refresh.await.unwrap().unwrap();
    wait_for_state(&manager, "T", IndexState::Ready).await;
}

#[test]
fn build_reservation_hook_blocks_once_and_then_becomes_inert() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("build-reservation-hook.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let (started, release) = manager.install_build_reservation_hook();
    let waiter = Arc::clone(&manager);
    let wait = std::thread::spawn(move || waiter.wait_for_build_reservation_hook());
    started.recv().unwrap();
    release.send(()).unwrap();
    wait.join().unwrap();
    manager.wait_for_build_reservation_hook();
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_rejects_a_build_owner_registered_during_reservation() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("build-owner-race.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let (started, release) = manager.install_build_reservation_hook();
    let hook_manager = Arc::clone(&manager);
    let hook = std::thread::spawn(move || {
        started.recv().unwrap();
        hook_manager
            .coordination
            .build_owners
            .lock()
            .unwrap()
            .insert("S".into(), Arc::new(()));
        release.send(()).unwrap();
    });

    let error = manager.refresh("S", true).await.unwrap_err();
    hook.join().unwrap();
    assert!(
        error
            .to_string()
            .contains("build owner is already registered in this process")
    );
    assert!(manager.active_builds.lock().unwrap().is_empty());
    assert!(manager.build_locks.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_rechecks_the_concurrency_limit_after_reservation() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("build-concurrency-race.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let (started, release) = manager.install_build_reservation_hook();
    let hook_manager = Arc::clone(&manager);
    let hook = std::thread::spawn(move || {
        started.recv().unwrap();
        hook_manager
            .active_builds
            .lock()
            .unwrap()
            .insert("T".into());
        release.send(()).unwrap();
    });

    let error = manager.refresh("S", true).await.unwrap_err();
    hook.join().unwrap();
    assert!(
        error
            .to_string()
            .contains("namespace index build concurrency limit reached")
    );
    assert!(manager.build_locks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cleanup_stays_pending_while_any_build_is_active_and_resumes_after_termination() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-deferred.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let (obsolete, active) = manager
        .with_database(|db| {
            let current =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", current, "0", &completed_progress(0))?;
            let obsolete =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
            db.fail_generation("S", obsolete, "failed")?;
            let active =
                db.start_generation("T", NamespaceOrganization::Flat, BrowseSource::Flat, "2")?;
            db.insert_entries("T", active, &[inventory_entry("Active", "T.Active")])?;
            db.promote("T", active, "3", &completed_progress(1))?;
            Ok((obsolete, active))
        })
        .unwrap();
    let (hook_started, hook_release) = manager.background_tasks.install_cleanup_notification_hook();
    manager.active_builds.lock().unwrap().insert("T".into());
    manager.schedule_cleanup("S");
    hook_started.notified().await;
    assert!(
        manager
            .cleanup_tasks
            .lock()
            .unwrap()
            .get("S")
            .is_some_and(|task| task.requested && task.running)
    );

    manager.active_builds.lock().unwrap().remove("T");
    manager.coordination.build_changed.notify_waiters();
    hook_release.notify_one();
    manager.background_tasks.wait_for_idle().await;
    assert_eq!(
        manager
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM generations
                         WHERE server = 'S' AND generation = ?1",
                        [obsolete as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap(),
        0
    );
    assert_eq!(manager.status("T").await.unwrap().active_generation, active);
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_on_one_manager_resumes_after_a_build_on_another_manager_finishes() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cross-manager-cleanup.sqlite3");
    let manager_a = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path.clone()),
    ));
    let manager_b = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let obsolete = manager_a
        .with_database(|db| {
            let current =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", current, "0", &completed_progress(0))?;
            let obsolete =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
            db.fail_generation("S", obsolete, "failed")?;
            Ok(obsolete)
        })
        .unwrap();
    manager_a.active_builds.lock().unwrap().insert("T".into());
    let (hook_started, hook_release) = manager_b
        .background_tasks
        .install_cleanup_notification_hook();

    manager_b.schedule_cleanup("S");
    hook_started.notified().await;
    assert!(
        manager_b
            .cleanup_tasks
            .lock()
            .unwrap()
            .get("S")
            .is_some_and(|task| task.requested && task.running)
    );

    manager_a.active_builds.lock().unwrap().remove("T");
    manager_a.coordination.build_changed.notify_waiters();
    hook_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        manager_b.background_tasks.wait_for_idle(),
    )
    .await
    .expect("cross-manager cleanup did not resume after build completion");

    assert_eq!(
        manager_b
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM generations
                         WHERE server = 'S' AND generation = ?1",
                        [obsolete as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_cleanup_exits_when_shutdown_precedes_notification_subscription() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-shutdown-race.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    manager
        .with_database(|db| {
            let current =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", current, "0", &completed_progress(0))?;
            let obsolete =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
            db.fail_generation("S", obsolete, "failed")?;
            Ok(())
        })
        .unwrap();
    manager.active_builds.lock().unwrap().insert("T".into());
    let (hook_started, hook_release) = manager.background_tasks.install_cleanup_notification_hook();

    manager.schedule_cleanup("S");
    hook_started.notified().await;
    manager.background_tasks.request_shutdown();
    hook_release.notify_one();

    tokio::time::timeout(
        Duration::from_secs(2),
        manager.background_tasks.wait_for_idle(),
    )
    .await
    .expect("deferred cleanup did not stop after shutdown");
    assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_notification_registration_closes_the_lost_wakeup_window() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-notification-race.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    let obsolete = manager
        .with_database(|db| {
            let current =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", current, "0", &completed_progress(0))?;
            let obsolete =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
            db.fail_generation("S", obsolete, "failed")?;
            Ok(obsolete)
        })
        .unwrap();
    let (hook_started, hook_release) = manager.background_tasks.install_cleanup_notification_hook();
    let writer_guard = manager.coordination.writer_gate.lock().unwrap();
    manager.schedule_cleanup("S");
    manager.active_builds.lock().unwrap().insert("T".into());
    drop(writer_guard);
    hook_started.notified().await;

    manager.active_builds.lock().unwrap().remove("T");
    manager.coordination.build_changed.notify_waiters();
    hook_release.notify_one();

    tokio::time::timeout(
        Duration::from_secs(2),
        manager.background_tasks.wait_for_idle(),
    )
    .await
    .expect("cleanup worker missed the build-completion notification");
    assert_eq!(
        manager
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM generations
                         WHERE server = 'S' AND generation = ?1",
                        [obsolete as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap(),
        0
    );
}

#[test]
fn cleanup_uses_a_separate_connection_without_blocking_primary_reads() {
    use std::thread;

    let directory = tempdir().unwrap();
    let path = directory.path().join("concurrent-cleanup.sqlite3");
    let mut primary = IndexDb::open(&path).unwrap();
    let obsolete = primary
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    let obsolete_entries = synthetic_entries("Obsolete", CLEANUP_BATCH_SIZE + 1);
    primary
        .insert_entries("S", obsolete, &obsolete_entries)
        .unwrap();
    primary
        .promote(
            "S",
            obsolete,
            "2",
            &completed_progress(obsolete_entries.len() as u64),
        )
        .unwrap();
    let active = primary
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
        .unwrap();
    primary
        .insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
        .unwrap();
    primary
        .promote("S", active, "4", &completed_progress(1))
        .unwrap();

    primary.connection.execute_batch("BEGIN").unwrap();
    assert_eq!(
        primary
            .connection
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        (CLEANUP_BATCH_SIZE + 2) as i64
    );
    let cleanup_path = path.clone();
    let background_tasks = Arc::new(BackgroundTasks::new());
    let cleanup_tasks = Arc::clone(&background_tasks);
    let cleanup = thread::spawn(move || {
        cleanup_obsolete_generations(&cleanup_path, "S", cleanup_tasks.as_ref())
    });

    for _ in 0..100 {
        assert_eq!(primary.search_generation("S").unwrap(), Some(active));
        assert_eq!(
            primary.search("S", active, "active", 1, 10).unwrap().len(),
            1
        );
        thread::yield_now();
    }
    let stats = cleanup.join().unwrap().unwrap();
    primary.connection.execute_batch("COMMIT").unwrap();
    assert_eq!(stats.entries, (CLEANUP_BATCH_SIZE + 1) as u64);
    assert_eq!(primary.search_generation("S").unwrap(), Some(active));
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_retries_transient_failures() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("cleanup-retry.sqlite3")),
    ));
    let (obsolete, active) = manager
        .with_database(|db| {
            let obsolete =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.insert_entries("S", obsolete, &[inventory_entry("Obsolete", "S.Obsolete")])?;
            db.promote("S", obsolete, "2", &completed_progress(1))?;
            let active =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")?;
            db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])?;
            db.promote("S", active, "4", &completed_progress(1))?;
            db.connection
                .execute_batch(
                    "CREATE TRIGGER fail_obsolete_cleanup_once
                 BEFORE DELETE ON entries
                 WHEN OLD.generation = 1
                 BEGIN
                   SELECT RAISE(FAIL, 'transient obsolete cleanup rejection');
                 END;",
                )
                .unwrap();
            Ok((obsolete, active))
        })
        .unwrap();
    manager.schedule_cleanup("S");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let failed_once = manager
                .cleanup_tasks
                .lock()
                .ok()
                .and_then(|tasks| tasks.get("S").map(|task| task.failures > 0))
                .unwrap_or(false);
            if failed_once {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager
        .with_database(|db| {
            db.connection
                .execute_batch("DROP TRIGGER fail_obsolete_cleanup_once;")?;
            Ok(())
        })
        .unwrap();
    manager.background_tasks.wait_for_idle().await;

    assert_eq!(
        manager
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM generations
                     WHERE server = 'S' AND generation = ?1",
                        [i64::try_from(obsolete)?],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap(),
        0
    );
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.active_generation, active);
    assert!(matches!(
        status.state,
        IndexState::Ready | IndexState::Stale
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn scheduled_cleanup_stops_cleanly_during_shutdown() {
    let directory = tempdir().unwrap();
    let background_tasks = Arc::new(BackgroundTasks::new());
    let cleanup_tasks = Arc::new(Mutex::new(HashMap::new()));
    background_tasks.request_shutdown();

    run_scheduled_cleanup(
        directory.path().join("shutdown-cleanup.sqlite3"),
        "S".into(),
        Arc::clone(&background_tasks),
        Arc::clone(&cleanup_tasks),
        Arc::new(DatabaseCoordination {
            writer_gate: Arc::new(Mutex::new(())),
            active_builds: Arc::new(Mutex::new(HashSet::new())),
            build_owners: Arc::new(Mutex::new(HashMap::new())),
            build_changed: Arc::new(tokio::sync::Notify::new()),
        }),
    )
    .await;

    assert!(cleanup_tasks.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn scheduled_cleanup_retries_after_worker_panic() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("cleanup-panic.sqlite3")),
    ));
    manager.background_tasks.panic_next_cleanup_worker();
    manager.schedule_cleanup("S");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let failed_once = manager
                .cleanup_tasks
                .lock()
                .ok()
                .and_then(|tasks| tasks.get("S").map(|task| task.failures > 0))
                .unwrap_or(false);
            if failed_once {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.background_tasks.wait_for_idle().await;

    assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn background_refresh_delay_uses_persisted_state() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    assert_eq!(
        manager.background_refresh_delay("S").await,
        Duration::from_secs(3600)
    );

    manager
        .with_database(|db| {
            let generation = db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                &timestamp_now(),
            )?;
            db.promote(
                "S",
                generation,
                &timestamp_now(),
                &InventoryProgress {
                    branches_visited: 0,
                    entries_seen: 0,
                    unique_items: 0,
                    active_time_ms: 0,
                    paused_time_ms: 0,
                    items_per_second: 0.0,
                    estimated_remaining_ms: None,
                },
            )
        })
        .unwrap();
    let ready_delay = manager.background_refresh_delay("S").await;
    assert!(ready_delay <= Duration::from_secs(604_800));

    manager.deleting.lock().unwrap().insert("S".into());
    assert_eq!(
        manager.background_refresh_delay("S").await,
        Duration::from_secs(30)
    );
}

#[tokio::test]
async fn background_indexing_starts_idempotently() {
    let directory = tempdir().unwrap();
    let enabled = settings(directory.path().join("enabled.sqlite3"));
    let enabled = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        enabled,
    ));
    enabled.start_background_indexing();
    enabled.start_background_indexing();
    assert!(enabled.background_started.load(Ordering::Acquire));
    enabled.shutdown_background_indexing().await;
}

#[tokio::test]
async fn background_indexing_does_not_start_first_build() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let config = settings(directory.path().join("index.sqlite3"));
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

    manager.start_background_indexing();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    manager.shutdown_background_indexing().await;
}

#[tokio::test]
async fn background_indexing_continues_after_a_scheduled_server_query_failure() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("scheduled-query-failure.sqlite3")),
    ));
    manager
        .with_database(|db| {
            drop_table(db, "enrolled_servers");
            Ok(())
        })
        .unwrap();

    manager.start_background_indexing();
    tokio::task::yield_now().await;
    manager.shutdown_background_indexing().await;
}

#[tokio::test]
async fn background_indexing_wakes_for_the_next_refresh_check() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.refresh_interval_seconds = 1;
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));
    let future = SystemTime::now()
        .checked_add(Duration::from_secs(60))
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &future,
        true,
    );

    manager.start_background_indexing();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn refresh_start_failure_backs_off_until_forced_retry() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![Err("start failed".into()), Ok(immediate_inventory_handle())],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));

    assert_eq!(
        manager.refresh("S", true).await.unwrap_err().to_string(),
        "start failed"
    );
    let failed = manager.status("S").await.unwrap();
    assert_eq!(failed.state, IndexState::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("start failed"));
    assert!(!failed.auto_refresh_enabled);
    assert!(failed.scheduler.next_refresh_at.is_none());

    let backed_off = manager.refresh("S", false).await.unwrap();
    assert_eq!(backed_off.state, IndexState::Failed);
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);

    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Ready).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 2);
    assert!(!manager.status("S").await.unwrap().auto_refresh_enabled);
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_cancels_inventory_when_shutdown_is_requested_after_start() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let inventory_started = Arc::new(Notify::new());
    let inventory_release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        )
        .with_inventory_gate(
            Arc::clone(&inventory_started),
            Arc::clone(&inventory_release),
        ),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));
    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });

    inventory_started.notified().await;
    manager.background_tasks.request_shutdown();
    inventory_release.notify_one();

    let status = refresh.await.unwrap().unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    assert!(control.cancelled.load(Ordering::Acquire));
    assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_cleans_up_when_background_spawn_is_rejected() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let manager = Arc::new(IndexManager::new(
        Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        )),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .reject_next_build_spawn
        .store(true, Ordering::Release);

    let status = manager.refresh("S", true).await.unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    assert!(control.cancelled.load(Ordering::Acquire));
    assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_ownership_is_held_until_inventory_stream_cleanup_finishes() {
    let directory = tempdir().unwrap();
    let (started, started_receiver) = std::sync::mpsc::sync_channel(0);
    let release = Arc::new(AtomicBool::new(false));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(InventoryHandle {
            stream: Box::new(DropGateInventoryStream {
                started,
                release: Arc::clone(&release),
                control: Arc::clone(&control),
                event: Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                }))),
            }),
            control,
        })],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("cleanup-order.sqlite3")),
    ));

    manager.refresh("S", true).await.unwrap();
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("inventory stream cleanup did not start");

    assert!(
        manager
            .coordination
            .build_owners
            .lock()
            .unwrap()
            .contains_key("S"),
        "build ownership was released before inventory cleanup finished"
    );
    assert!(
        manager.active_builds.lock().unwrap().contains("S"),
        "active build state was released before inventory cleanup finished"
    );
    assert!(
        manager.build_locks.lock().unwrap().contains_key("S"),
        "build file lock was released before inventory cleanup finished"
    );

    release.store(true, Ordering::Release);
    wait_for_state(&manager, "S", IndexState::Ready).await;

    assert!(
        !manager
            .coordination
            .build_owners
            .lock()
            .unwrap()
            .contains_key("S")
    );
    assert!(!manager.active_builds.lock().unwrap().contains("S"));
    assert!(manager.build_locks.lock().unwrap().get("S").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_inventory_event_is_cancelled_and_releases_the_scheduler() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(InventoryHandle {
            stream: Box::new(BlockingInventoryStream {
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                event: Some(Ok(InventoryEvent::Entry(inventory_entry(
                    "Stalled",
                    "S.Stalled",
                )))),
            }),
            control: Arc::clone(&control) as Arc<dyn InventoryControl>,
        })],
        vec![],
    ));
    let mut config = settings(directory.path().join("stalled-inventory.sqlite3"));
    config.operation_timeout_seconds = 1;
    let manager = Arc::new(IndexManager::new(client, config));

    manager.refresh("S", true).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    wait_for_state(&manager, "S", IndexState::Failed).await;
    let status = manager.status("S").await.unwrap();
    assert_eq!(
        status.last_error.as_deref(),
        Some("inventory event timed out after 1 seconds")
    );
    assert!(control.cancelled.load(Ordering::Acquire));
}

#[tokio::test]
async fn shutdown_drains_background_scheduler_and_build_tasks() {
    let directory = tempdir().unwrap();
    let stream_started = Arc::new(Notify::new());
    let stream_release = Arc::new(Notify::new());
    let manager = manager_with_blocking_event(
        directory.path().join("index.sqlite3"),
        Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
        Arc::clone(&stream_started),
        Arc::clone(&stream_release),
    );

    manager.refresh("S", true).await.unwrap();
    stream_started.notified().await;
    manager.start_background_indexing();
    assert!(manager.background_tasks.state.lock().unwrap().active >= 2);

    let shutdown_manager = Arc::clone(&manager);
    let shutdown = tokio::spawn(async move {
        shutdown_manager.shutdown_background_indexing().await;
    });
    for _ in 0..100 {
        if manager.background_tasks.is_shutting_down() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(manager.background_tasks.is_shutting_down());

    stream_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("background indexing did not drain")
        .unwrap();
    assert_eq!(manager.background_tasks.state.lock().unwrap().active, 0);
    assert_eq!(manager.status("S").await.unwrap().state, IndexState::Failed);
}

#[tokio::test]
async fn background_task_registry_is_idempotent_and_rejects_new_work_after_shutdown() {
    let tasks = Arc::new(BackgroundTasks::new());
    assert!(tasks.spawn(async {}));
    tasks.wait_for_idle().await;
    assert!(!tasks.is_shutting_down());

    tasks.request_shutdown();
    tasks.request_shutdown();
    assert!(tasks.is_shutting_down());
    assert!(!tasks.spawn(async {}));

    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .reject_next_cleanup_spawn
        .store(true, Ordering::Release);
    manager.schedule_cleanup("S");
    assert!(!manager.cleanup_tasks.lock().unwrap().contains_key("S"));
    manager.shutdown_background_indexing().await;
    assert_eq!(
        manager.refresh("S", true).await.unwrap().state,
        IndexState::NotIndexed
    );
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn background_task_registry_rejects_work_when_its_state_lock_is_poisoned() {
    let tasks = Arc::new(BackgroundTasks::new());
    let state = Arc::clone(&tasks);
    let _ = std::panic::catch_unwind(move || {
        let _guard = state.state.lock().unwrap();
        panic!("poison background task state for error-path coverage");
    });
    assert!(!tasks.spawn(async {}));
}

#[test]
fn cleanup_error_paths_tolerate_database_and_registry_failures() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("cleanup-errors.sqlite3")),
    ));
    manager
        .with_database(|db| {
            db.connection
                .execute_batch("DROP TABLE generations")
                .unwrap();
            Ok(())
        })
        .unwrap();

    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        manager.fail_generation_and_schedule_cleanup("S", 1, "failed");
        manager.abandon_generation("S", 1, "abandoned");
    });

    let cleanup_tasks = Arc::clone(&manager.cleanup_tasks);
    let poisoned_cleanup_tasks = Arc::clone(&cleanup_tasks);
    let _ = std::panic::catch_unwind(move || {
        let _guard = cleanup_tasks.lock().unwrap();
        panic!("poison cleanup registry for error-path coverage");
    });
    manager.schedule_cleanup("S");

    let cleanup_worker_active = Arc::new(AtomicBool::new(false));
    spawn_cleanup_worker_if_idle(
        Arc::clone(&cleanup_worker_active),
        manager.settings.database_path.clone(),
        Arc::new(BackgroundTasks::new()),
        poisoned_cleanup_tasks,
        Arc::clone(&manager.coordination),
        false,
    );
    assert!(!cleanup_worker_active.load(Ordering::Acquire));
}

#[test]
fn cleanup_deferral_log_fields_are_evaluated_when_enabled() {
    let directory = tempdir().unwrap();
    let database_path = directory.path().join("cleanup-deferral.sqlite3");
    let database = IndexDb::open(&database_path).unwrap();
    let mut connection = None;
    let background_tasks = BackgroundTasks::new();
    let writer_gate = Mutex::new(());
    let active_builds = Mutex::new(HashSet::from(["T".to_string()]));
    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(true)
        .with_level(true)
        .with_max_level(tracing::Level::INFO)
        .with_writer(CapturedLogWriter(Arc::clone(&output)))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);

    let result = tracing::dispatcher::with_default(&dispatch, || {
        cleanup_one_batch(
            &database,
            &mut connection,
            &database_path,
            "S",
            &background_tasks,
            &writer_gate,
            &active_builds,
        )
    })
    .unwrap();
    assert!(matches!(result, CleanupBatchResult::Deferred));
    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("INFO opcda_bridge_gateway::index:"),
        "{output}"
    );
    assert!(
        output.contains("deferring namespace index cleanup while an index build is active"),
        "{output}"
    );
    for field in ["process_id=", "database=", "server=\"S\""] {
        assert!(output.contains(field), "{output}");
    }
}

#[test]
fn finish_build_handles_a_poisoned_build_owner_registry() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("poisoned-finalization.sqlite3")),
    ));
    insert_runtime_build(&manager, Arc::new(RecordingInventoryControl::default()));
    let owners = Arc::clone(&manager.coordination.build_owners);
    let _ = std::panic::catch_unwind(move || {
        let _guard = owners.lock().unwrap();
        panic!("poison build-owner registry for finalization error-path coverage");
    });
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    let ownership = Arc::new(());
    tracing::subscriber::with_default(subscriber, || {
        manager.finish_build_owned("S", &ownership, None);
    });
    assert!(
        manager
            .runtime
            .lock()
            .unwrap()
            .get("S")
            .unwrap()
            .build
            .is_some()
    );
}

#[tokio::test]
async fn build_loop_records_inventory_shutdown_failure() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("shutdown-failure.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));
    let mut state = BuildRunState::new(&manager.settings, None);
    let mut handle = InventoryHandle {
        stream: Box::new(CompletedThenShutdownErrorInventoryStream { emitted: false }),
        control: Arc::clone(&trait_control),
    };

    let outcome = manager
        .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
        .await;
    assert!(matches!(
        outcome,
        BuildLoopOutcome::Failed(error)
            if error.contains("inventory worker shutdown failed")
    ));
}

#[tokio::test]
async fn build_loop_keeps_build_failure_when_shutdown_also_fails() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("shutdown-after-failure.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));
    let mut state = BuildRunState::new(&manager.settings, None);
    let mut handle = InventoryHandle {
        stream: Box::new(ErrorThenShutdownErrorInventoryStream { emitted: false }),
        control: Arc::clone(&trait_control),
    };

    let outcome = manager
        .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
        .await;
    assert!(matches!(
        outcome,
        BuildLoopOutcome::Failed(error) if error == "injected inventory build failure"
    ));
}

#[tokio::test]
async fn automatic_refresh_logs_reachable_invalidation_and_refresh_failures() {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let directory = tempdir().unwrap();

    let clear_client = Arc::new(MockOpcClient::default());
    *clear_client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
        organization: NamespaceOrganization::Flat,
        source: BrowseSource::Flat,
        supports_browse_sessions: false,
        supports_search: true,
        max_page_size: 100,
    });
    let clear_manager = Arc::new(IndexManager::new(
        Arc::clone(&clear_client),
        settings(directory.path().join("clear.sqlite3")),
    ));
    seed_active_generation(
        &clear_manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
        true,
    );
    clear_manager
        .with_database(|db| {
            db.connection.execute_batch(
                "CREATE TRIGGER fail_clear
                 BEFORE DELETE ON generations
                 BEGIN
                   SELECT RAISE(FAIL, 'clear failed');
                 END;",
            )?;
            Ok(())
        })
        .unwrap();
    clear_manager.refresh_if_due("S").await;
    assert_eq!(
        clear_manager.status("S").await.unwrap().active_generation,
        1
    );
    assert_eq!(
        clear_client.inventory_start_count.load(Ordering::Relaxed),
        0
    );
    assert!(!clear_manager.active_builds.lock().unwrap().contains("S"));
    clear_manager
        .with_database(|db| {
            db.set_retry_state("S", None, 0, false)?;
            db.connection.execute_batch(
                "CREATE TRIGGER fail_attempt BEFORE INSERT ON generations
                 BEGIN SELECT RAISE(ABORT, 'failure audit rejected'); END;",
            )?;
            Ok(())
        })
        .unwrap();
    clear_manager.runtime.lock().unwrap().clear();
    clear_manager.refresh_if_due("S").await;
    assert!(!clear_manager.active_builds.lock().unwrap().contains("S"));
    assert_eq!(
        clear_manager.status("S").await.unwrap().active_generation,
        1
    );

    let rebuild_client = Arc::new(LifecycleClient::new(
        vec![Err("rebuild start failed".into())],
        vec![Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Flat,
            source: BrowseSource::Flat,
            supports_browse_sessions: false,
            supports_search: true,
            max_page_size: 100,
        })],
    ));
    let rebuild_manager = Arc::new(IndexManager::new(
        Arc::clone(&rebuild_client),
        settings(directory.path().join("rebuild.sqlite3")),
    ));
    seed_active_generation(
        &rebuild_manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
        true,
    );
    rebuild_manager.refresh_if_due("S").await;
    assert_eq!(
        rebuild_client.inventory_start_count.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        rebuild_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .as_deref(),
        Some("rebuild start failed")
    );

    let stale_client = Arc::new(LifecycleClient::new(
        vec![Err("stale refresh failed".into())],
        vec![Ok(default_capabilities())],
    ));
    let stale_manager = Arc::new(IndexManager::new(
        Arc::clone(&stale_client),
        settings(directory.path().join("stale.sqlite3")),
    ));
    seed_active_generation(
        &stale_manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
        true,
    );
    stale_manager.refresh_if_due("S").await;
    assert_eq!(
        stale_client.inventory_start_count.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        stale_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .as_deref(),
        Some("stale refresh failed")
    );
}

#[tokio::test]
async fn background_refresh_skips_a_fresh_persisted_generation() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| {
            let generation = db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                &timestamp_now(),
            )?;
            db.insert_entries(
                "S",
                generation,
                &[InventoryEntry {
                    display_name: "Persisted".into(),
                    item_id: "persisted".into(),
                    kind: InventoryNodeKind::Item,
                    breadcrumbs: vec![],
                }],
            )?;
            db.promote(
                "S",
                generation,
                &timestamp_now(),
                &InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 1,
                    unique_items: 1,
                    active_time_ms: 1,
                    paused_time_ms: 0,
                    items_per_second: 1.0,
                    estimated_remaining_ms: None,
                },
            )
        })
        .unwrap();
    manager.refresh_if_due("S").await;
    assert_eq!(
        client
            .inventory_start_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn background_refresh_rebuilds_a_stale_persisted_generation() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| {
            let generation = db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )?;
            db.insert_entries("S", generation, &[inventory_entry("Old", "Old.Tag")])?;
            db.promote("S", generation, "0", &zero_progress())
        })
        .unwrap();
    manager.change_auto_refresh("S", true).unwrap();

    manager.refresh_if_due("S").await;
    wait_for_build(&manager, IndexState::Ready).await;
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
    assert!(
        manager
            .search("S", "old", 3, 10)
            .await
            .unwrap()
            .matches
            .is_empty()
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
}

#[test]
fn build_file_lock_reports_owner_and_probe_errors() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("index.sqlite3");
    let lock = BuildFileLock::acquire(&database, "S").unwrap();
    let error = BuildFileLock::acquire(&database, "S").unwrap_err();
    assert!(error.to_string().contains("process_id="));
    assert!(error.to_string().contains("server=S"));
    drop(lock);

    let lock_path = build_lock_path(&database, "S");
    fs::write(&lock_path, "external test owner\n").unwrap();
    #[cfg(unix)]
    {
        use std::io::BufRead;
        use std::process::{Command, Stdio};

        let mut child = Command::new("flock")
            .arg("-x")
            .arg(&lock_path)
            .arg("sh")
            .arg("-c")
            .arg("echo ready; read line")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready, "ready\n");

        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("external test owner"));
        assert!(BuildFileLock::is_held(&database, "S").unwrap());
        drop(child.stdin.take());
        child.wait().unwrap();

        fs::write(&lock_path, "").unwrap();
        let mut child = Command::new("flock")
            .arg("-x")
            .arg(&lock_path)
            .arg("sh")
            .arg("-c")
            .arg("echo ready; read line")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready, "ready\n");
        let error = BuildFileLock::acquire(&database, "S").unwrap_err();
        assert!(error.to_string().contains("build lock is already held"));
        drop(child.stdin.take());
        child.wait().unwrap();
    }

    fs::remove_file(&lock_path).unwrap();
    fs::create_dir(&lock_path).unwrap();
    assert!(BuildFileLock::is_held(&database, "S").is_err());
    assert!(BuildFileLock::acquire(&database, "S").is_err());
}

#[test]
fn helper_edge_cases_cover_window_time_and_retry_rollback() {
    let now = Instant::now();
    let mut metrics = ForegroundMetricState::default();
    for latency in 0..129 {
        metrics.record_health_at(now, latency, false, false, false);
    }
    assert_eq!(metrics.latencies_ms.len(), 128);
    assert_eq!(metrics.latencies_ms.front(), Some(&1));
    assert_eq!(percentile(&[], 50), None);
    assert!(!instant_timestamp(Instant::now()).is_empty());
    assert!(deterministic_jitter("S", u64::MAX) <= Duration::from_secs(u64::MAX));

    let directory = tempdir().unwrap();
    let db = IndexDb::open(&directory.path().join("retry-rollback.sqlite3")).unwrap();
    db.connection
        .execute_batch(
            "CREATE TRIGGER reject_retry_state
             BEFORE INSERT ON index_meta
             WHEN NEW.key = 'failures:S'
             BEGIN
               SELECT RAISE(FAIL, 'retry state rejected');
             END;",
        )
        .unwrap();
    assert!(
        db.set_retry_state("S", Some(SystemTime::now()), 1, false)
            .unwrap_err()
            .to_string()
            .contains("retry state rejected")
    );
    assert_eq!(db.retry_state("S").unwrap(), (None, 0, false));
}

#[tokio::test]
async fn scheduler_delay_covers_retry_terminal_and_maintenance_states() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("scheduler.sqlite3")),
    ));
    manager
        .runtime
        .lock()
        .unwrap()
        .entry("S".into())
        .or_default()
        .retry_after = Some(SystemTime::now() + Duration::from_secs(2));
    assert!(manager.background_refresh_delay("S").await >= Duration::from_secs(1));
    manager.runtime.lock().unwrap().clear();

    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, control);
    manager.mark_promoting("S").unwrap();
    assert_eq!(
        manager.background_refresh_delay("S").await,
        Duration::from_secs(1)
    );
    manager.clear_promoting("S");
    manager.runtime.lock().unwrap().clear();
    manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            db.fail_generation("S", generation, "failed")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        manager.background_refresh_delay("S").await,
        retry_delay("S", 1, false, 300)
    );

    let mut maintenance = settings(directory.path().join("maintenance-delay.sqlite3"));
    maintenance.maintenance_windows = vec!["00:00-00:00".into()];
    let maintenance = IndexManager::new(Arc::new(MockOpcClient::default()), maintenance);
    assert_eq!(
        maintenance.background_refresh_delay("S").await,
        Duration::from_secs(3600)
    );
}

#[tokio::test]
async fn stale_and_adaptive_cancellation_scheduler_paths_are_covered() {
    let directory = tempdir().unwrap();
    let stale = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("stale-no-window.sqlite3")),
    ));
    seed_active_generation(
        &stale,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        "0",
        true,
    );
    assert_eq!(
        stale.background_refresh_delay("S").await,
        Duration::from_secs(1)
    );

    let mut maintenance_config = settings(directory.path().join("invalid-maintenance.sqlite3"));
    maintenance_config.maintenance_windows = vec!["invalid".into()];
    let maintenance = IndexManager::new(Arc::new(MockOpcClient::default()), maintenance_config);
    assert!(!maintenance.automatic_refresh_allowed(&empty_status("S", IndexState::NotIndexed)));

    let mut adaptive_config = settings(directory.path().join("adaptive-cancel.sqlite3"));
    adaptive_config.adaptive = true;
    let adaptive = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        adaptive_config,
    ));
    adaptive.record_foreground_operation_with_health(
        "S",
        Duration::from_millis(1),
        false,
        true,
        false,
    );
    let generation = adaptive
        .with_database(|db| {
            db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
        })
        .unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel_on_pause();
    let control: Arc<dyn InventoryControl> = control;
    let ownership = insert_runtime_build(&adaptive, Arc::clone(&control));
    Arc::clone(&adaptive)
        .run_build(
            "S".into(),
            generation,
            InventoryHandle {
                stream: Box::new(VecInventoryStream {
                    events: VecDeque::from([Ok(InventoryEvent::Slice(
                        InventorySliceObservation {
                            sequence: 1,
                            backend: InventorySliceBackend::Da2,
                            nodes_returned: 1,
                            has_more: false,
                            native_operations: 1,
                            elapsed_ms: 1,
                            entries_seen: 1,
                            unique_items: 1,
                        },
                    ))]),
                }),
                control,
            },
            ownership,
        )
        .await;
    assert_eq!(
        adaptive.status("S").await.unwrap().state,
        IndexState::Failed
    );
}

#[tokio::test]
async fn scheduler_shutdown_quiet_resume_and_batch_commit_complete() {
    let directory = tempdir().unwrap();
    let background_config = settings(directory.path().join("background-shutdown.sqlite3"));
    let background = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        background_config,
    ));
    background.start_background_indexing();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    background.shutdown_background_indexing().await;

    let quiet = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("quiet-resume.sqlite3")),
    ));
    let quiet_control = Arc::new(RecordingInventoryControl::default());
    let control: Arc<dyn InventoryControl> = quiet_control.clone();
    insert_runtime_build(&quiet, control);
    let guard = quiet.foreground_guard("S");
    let resumes = quiet_control.resume_count.load(Ordering::Relaxed);
    drop(guard);
    wait_for_counter(&quiet_control.resume_count, resumes + 2).await;

    let mut batch_config = settings(directory.path().join("batch-success.sqlite3"));
    batch_config.commit_batch_size = 1;
    let batch = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        batch_config,
    ));
    batch.refresh("S", true).await.unwrap();
    wait_for_build(&batch, IndexState::Ready).await;
    assert_eq!(batch.status("S").await.unwrap().entry_count, 1);
}

struct DropGateInventoryStream {
    started: std::sync::mpsc::SyncSender<()>,
    release: Arc<AtomicBool>,
    control: Arc<dyn InventoryControl>,
    event: Option<anyhow::Result<InventoryEvent>>,
}

struct CompletedThenShutdownErrorInventoryStream {
    emitted: bool,
}

struct ErrorThenShutdownErrorInventoryStream {
    emitted: bool,
}

#[async_trait::async_trait]
impl InventoryStream for DropGateInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        self.event.take()
    }
}

impl Drop for DropGateInventoryStream {
    fn drop(&mut self) {
        let _ = self.started.send(());
        while !self.release.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.control.cancel();
    }
}

#[async_trait::async_trait]
impl InventoryStream for CompletedThenShutdownErrorInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        if self.emitted {
            None
        } else {
            self.emitted = true;
            Some(Ok(completed_inventory()))
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("injected coordinated inventory shutdown failure")
    }
}

#[async_trait::async_trait]
impl InventoryStream for ErrorThenShutdownErrorInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        if self.emitted {
            None
        } else {
            self.emitted = true;
            Some(Err(anyhow::anyhow!("injected inventory build failure")))
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("injected coordinated inventory shutdown failure")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn split_scheduler_shutdown_and_cleanup_short_circuits_are_safe() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("scheduler-split.sqlite3")),
    ));
    let mut partial = empty_status("S", IndexState::Partial);
    assert_eq!(
        manager.refresh_delay_for_status("S", &partial),
        Duration::from_secs(30)
    );
    partial.active_generation = 1;
    partial.auto_refresh_enabled = true;
    partial.scheduler.next_refresh_at = Some("0".into());
    assert!(!manager.automatic_refresh_allowed(&partial));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(true);
    manager.run_background_indexing(shutdown_rx).await;
    let mut shutdown = shutdown_tx.subscribe();
    wait_for_refresh_or_shutdown(&mut shutdown, Duration::from_secs(60)).await;
    manager
        .with_database_write(|db| {
            db.enroll("S", &timestamp_now())?;
            db.set_auto_refresh("S", true)?;
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")?;
            db.promote("S", generation, "1", &completed_progress(0))
        })
        .unwrap();
    shutdown = shutdown_tx.subscribe();
    assert_eq!(
        manager.refresh_scheduled_servers(&mut shutdown).await,
        Duration::from_secs(60)
    );
    let control = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, control.clone());
    manager.shutdown_background_indexing().await;
    assert!(control.cancelled.load(Ordering::Acquire));

    let cleanup = Arc::clone(&manager.cleanup_tasks);
    assert!(
        std::thread::spawn(move || {
            let _guard = cleanup.lock().unwrap();
            panic!("poison cleanup registry for scheduler coverage");
        })
        .join()
        .is_err()
    );
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    tracing::subscriber::with_default(subscriber, || manager.schedule_cleanup("S"));

    let active = Arc::new(AtomicBool::new(true));
    spawn_cleanup_worker_if_idle(
        Arc::clone(&active),
        manager.settings.database_path.clone(),
        Arc::clone(&manager.background_tasks),
        Arc::clone(&manager.cleanup_tasks),
        Arc::clone(&manager.coordination),
        false,
    );
    assert!(active.load(Ordering::Acquire));

    let poisoned = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("poisoned-runtime.sqlite3")),
    ));
    let runtime = Arc::clone(&poisoned.runtime);
    assert!(
        std::thread::spawn(move || {
            let _guard = runtime.lock().unwrap();
            panic!("poison runtime state for scheduler shutdown coverage");
        })
        .join()
        .is_err()
    );
    poisoned.shutdown_background_indexing().await;
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_cleanup_returns_when_no_build_is_active() {
    let directory = tempdir().unwrap();
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("cleanup-no-build.sqlite3")),
    );
    let mut shutdown = manager.background_tasks.subscribe();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    assert!(
        wait_for_deferred_cleanup(
            &manager.settings.database_path,
            "S",
            &manager.background_tasks,
            &manager.coordination,
            &manager.cleanup_tasks,
            &mut shutdown,
        )
        .await
    );
}

#[cfg(windows)]
#[test]
fn build_lock_cleanup_warning_keeps_the_index_module_target_and_fields() {
    let directory = tempdir().unwrap();
    let database_path = directory.path().join("index.sqlite3");
    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(true)
        .with_level(true)
        .with_writer(CapturedLogWriter(Arc::clone(&output)))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let lock = BuildFileLock::acquire(&database_path, "characterization").unwrap();
        let owner_path = lock.owner_path.as_ref().unwrap().clone();
        std::fs::remove_file(&owner_path).unwrap();
        std::fs::create_dir(&owner_path).unwrap();
        drop(lock);
        std::fs::remove_dir(owner_path).unwrap();
    });
    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("WARN opcda_bridge_gateway::index:"),
        "{output}"
    );
    assert!(
        output.contains("unable to remove namespace index build owner metadata"),
        "{output}"
    );
    for field in ["process_id=", "owner=", "error="] {
        assert!(output.contains(field), "{output}");
    }
}
