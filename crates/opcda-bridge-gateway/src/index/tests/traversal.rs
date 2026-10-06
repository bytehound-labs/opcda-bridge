use super::*;

fn root_node(display_name: &str, kind: BrowseNodeKind, item_id: Option<&str>) -> BrowseNode {
    BrowseNode {
        node_key: display_name.into(),
        display_name: display_name.into(),
        kind,
        item_id: item_id.map(str::to_owned),
    }
}

#[tokio::test]
async fn configured_inventory_root_uses_root_scoped_start() {
    let client = Arc::new(MockOpcClient::default());
    client.inventory_root_events.lock().unwrap().insert(
        "FCS0201".into(),
        VecDeque::from([Ok(completed_inventory())]),
    );
    let mut config = settings(PathBuf::from(":memory:"));
    config.inventory_root = Some("FCS0201".into());
    config.worker_count = 4;
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

    let handle = manager
        .start_refresh_inventory(
            "S",
            &Arc::new(()),
            InventoryLimits {
                item_rate_per_second: 0,
                batch_size: 17,
                duty_cycle_percent: 100,
            },
        )
        .await
        .unwrap()
        .expect("configured root should start an inventory");
    let InventoryHandle {
        mut stream,
        control,
    } = handle;
    assert!(matches!(
        stream.next().await,
        Some(Ok(InventoryEvent::Completed(_)))
    ));
    stream.shutdown().await.unwrap();
    assert!(!control.is_cancelled());
    assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 0);
    assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 1);
    assert_eq!(
        client.inventory_started_roots.lock().unwrap().as_slice(),
        ["FCS0201"]
    );
    assert_eq!(client.inventory_batch_size.load(Ordering::Acquire), 17);
}

#[test]
fn coordinated_control_handles_registration_and_pacing_edges() {
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    control.cancel();
    let cancelled_worker = Arc::new(RecordingInventoryControl::default());
    assert!(
        !control
            .register(
                0,
                Arc::clone(&cancelled_worker) as Arc<dyn InventoryControl>
            )
            .unwrap()
    );
    assert!(cancelled_worker.is_cancelled());

    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    control.pause();
    let paused_worker = Arc::new(RecordingInventoryControl::default());
    assert!(
        control
            .register(1, Arc::clone(&paused_worker) as Arc<dyn InventoryControl>)
            .unwrap()
    );
    assert_eq!(paused_worker.pause_count.load(Ordering::Acquire), 1);

    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let racing_worker = Arc::new(RegisterCancellingControl {
        parent: Arc::clone(&control),
        cancelled: AtomicBool::new(false),
    });
    racing_worker.pause();
    racing_worker.resume();
    assert!(
        !control
            .register(2, Arc::clone(&racing_worker) as Arc<dyn InventoryControl>)
            .unwrap()
    );
    assert!(racing_worker.is_cancelled());

    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let pacing_worker = Arc::new(RecordingInventoryControl::default());
    control
        .register(3, Arc::clone(&pacing_worker) as Arc<dyn InventoryControl>)
        .unwrap();
    let trait_control: &dyn InventoryControl = &*control;
    trait_control
        .set_pacing(InventoryPacing {
            min_interval: Duration::from_millis(5),
            item_rate_per_second: Some(10),
            batch_size: Some(20),
        })
        .unwrap();
    assert_eq!(pacing_worker.pacing_calls.load(Ordering::Acquire), 2);
    control.stop_workers();
    assert!(control.should_stop_workers());

    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let failing_worker = Arc::new(RecordingInventoryControl::default());
    failing_worker.fail_pacing_on_call(2);
    control
        .register(4, Arc::clone(&failing_worker) as Arc<dyn InventoryControl>)
        .unwrap();
    let error = control
        .set_pacing(InventoryPacing {
            min_interval: Duration::from_millis(1),
            item_rate_per_second: None,
            batch_size: None,
        })
        .unwrap_err();
    assert!(error.to_string().contains("test pacing update failure"));
    assert!(control.is_cancelled());
}

#[tokio::test]
async fn coordinated_channel_close_reports_worker_failure_or_completion() {
    let plan = InventoryRootPlan {
        root_entries: Vec::new(),
        worker_roots: Vec::new(),
        organization: NamespaceOrganization::Hierarchical,
        source: BrowseSource::Da2,
    };

    let (sender, mut receiver) =
        tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let worker = tokio::spawn(async {
        panic!("injected coordinated worker task failure");
    });
    IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
        &plan,
        &control,
        vec![worker],
        &sender,
    )
    .await;
    let error = receiver
        .recv()
        .await
        .expect("worker failure should produce an error")
        .expect_err("worker task failure must be reported");
    assert!(error.to_string().contains("worker task failed"));

    let (sender, mut receiver) =
        tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
        &plan,
        &control,
        Vec::new(),
        &sender,
    )
    .await;
    assert!(matches!(
        receiver.recv().await,
        Some(Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            warning: None,
            ..
        })))
    ));

    let (sender, mut receiver) =
        tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    control.cancel();
    IndexManager::<MockOpcClient>::finish_coordinated_inventory_after_channel_close(
        &plan,
        &control,
        Vec::new(),
        &sender,
    )
    .await;
    assert!(matches!(
        receiver.recv().await,
        Some(Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: false,
            cancelled: true,
            warning: Some(_),
            ..
        })))
    ));
}

#[tokio::test]
async fn coordinated_stream_shutdown_reports_coordinator_join_failure() {
    let (_sender, receiver) =
        tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let coordinator = tokio::spawn(async {
        panic!("injected coordinator panic");
    });
    let mut stream = CoordinatedInventoryStream {
        receiver,
        control: Arc::clone(&control),
        coordinator: Some(coordinator),
        terminal_event_seen: false,
    };
    let error = stream.shutdown().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("coordinated inventory task failed")
    );
    assert!(control.is_cancelled());
}

#[test]
fn aggregate_inventory_progress_handles_zero_active_time() {
    let mut progress = HashMap::new();
    progress.insert(0, zero_inventory_progress());
    let aggregate = aggregate_inventory_progress(&progress, 0);
    assert_eq!(aggregate.items_per_second, 0.0);
    assert_eq!(
        aggregate_inventory_progress(&HashMap::new(), 0).items_per_second,
        0.0
    );
}

#[test]
fn accumulate_inventory_progress_handles_zero_active_time() {
    let mut cumulative = zero_inventory_progress();
    accumulate_inventory_progress(
        &mut cumulative,
        None,
        &InventoryProgress {
            unique_items: 3,
            ..zero_inventory_progress()
        },
    );
    assert_eq!(cumulative.unique_items, 3);
    assert_eq!(cumulative.items_per_second, 0.0);
}

#[tokio::test]
async fn coordinated_inventory_deduplicates_roots_and_aggregates_progress() {
    let client = Arc::new(MockOpcClient::default());
    *client.browse_page_result.lock().unwrap() = Ok(BrowsePage {
        nodes: vec![
            root_node("Branch A", BrowseNodeKind::Branch, Some("Root.A")),
            root_node("Branch B", BrowseNodeKind::BranchAndItem, Some("Root.B")),
            root_node("Root leaf", BrowseNodeKind::Item, Some("Root.Leaf")),
        ],
        next_page_token: None,
        complete: true,
        organization: NamespaceOrganization::Hierarchical,
        source: BrowseSource::Da2,
        warning: None,
    });
    *client.inventory_root_events.lock().unwrap() = HashMap::from([
        (
            "Root.A".into(),
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("A", "A.Item"))),
                Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 1,
                    entries_seen: 1,
                    unique_items: 1,
                    active_time_ms: 10,
                    paused_time_ms: 2,
                    items_per_second: 100.0,
                    estimated_remaining_ms: Some(50),
                })),
                Ok(InventoryEvent::Slice(InventorySliceObservation {
                    sequence: 11,
                    backend: InventorySliceBackend::Da2,
                    nodes_returned: 1,
                    has_more: true,
                    native_operations: 3,
                    elapsed_ms: 20,
                    entries_seen: 1,
                    unique_items: 1,
                })),
                Ok(completed_inventory()),
            ]),
        ),
        (
            "Root.B".into(),
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry(
                    "A duplicate",
                    "A.Item",
                ))),
                Ok(InventoryEvent::Entry(inventory_entry("B", "B.Item"))),
                Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 2,
                    entries_seen: 2,
                    unique_items: 2,
                    active_time_ms: 20,
                    paused_time_ms: 3,
                    items_per_second: 100.0,
                    estimated_remaining_ms: None,
                })),
                Ok(InventoryEvent::Slice(InventorySliceObservation {
                    sequence: 22,
                    backend: InventorySliceBackend::Da2,
                    nodes_returned: 2,
                    has_more: false,
                    native_operations: 4,
                    elapsed_ms: 15,
                    entries_seen: 2,
                    unique_items: 2,
                })),
                Ok(completed_inventory()),
            ]),
        ),
    ]);
    let mut config = settings(PathBuf::from(":memory:"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(Arc::clone(&client), config));

    let InventoryHandle {
        mut stream,
        control,
    } = manager
        .start_refresh_inventory(
            "S",
            &Arc::new(()),
            InventoryLimits {
                item_rate_per_second: 100,
                batch_size: 25,
                duty_cycle_percent: 100,
            },
        )
        .await
        .unwrap()
        .expect("two expandable roots should use coordinated inventory");
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    stream.shutdown().await.unwrap();
    assert!(!control.is_cancelled());

    let mut entries: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            InventoryEvent::Entry(entry) => Some(entry),
            _ => None,
        })
        .collect();
    entries.sort_by(|left, right| left.item_id.cmp(&right.item_id));
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["A.Item", "B.Item", "Root.B", "Root.Leaf"]
    );
    assert_eq!(entries[2].breadcrumbs, Vec::<String>::new());
    assert_eq!(entries[2].kind, InventoryNodeKind::BranchAndItem);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            InventoryEvent::Progress(progress)
                if progress.branches_visited == 3
                    && progress.entries_seen == 3
                    && progress.active_time_ms == 30
                    && progress.paused_time_ms == 5
                    && (3..=4).contains(&progress.unique_items)
        )
    }));
    let slices: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            InventoryEvent::Slice(slice) => Some(slice),
            _ => None,
        })
        .collect();
    assert_eq!(
        slices.iter().map(|slice| slice.nodes_returned).sum::<u64>(),
        3
    );
    assert_eq!(
        slices
            .iter()
            .map(|slice| slice.native_operations)
            .sum::<u64>(),
        7
    );
    assert_eq!(slices.iter().map(|slice| slice.elapsed_ms).max(), Some(20));
    assert!(slices.iter().any(|slice| slice.entries_seen == 2));
    assert!(
        slices
            .iter()
            .all(|slice| (3..=4).contains(&slice.unique_items))
    );
    assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 2);
    let mut started_roots = client.inventory_started_roots.lock().unwrap().clone();
    started_roots.sort();
    assert_eq!(started_roots, vec!["Root.A", "Root.B"]);
    assert!(matches!(
        events.last(),
        Some(InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            ..
        }))
    ));
}

#[tokio::test]
async fn configured_inventory_root_failure_is_fatal() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_inventories(vec![Err("configured root failed".into())]),
    );
    let mut config = settings(directory.path().join("configured-root-failure.sqlite3"));
    config.inventory_root = Some("FCS0201".into());
    let manager = Arc::new(IndexManager::new(client, config));
    manager.with_database(|_| Ok(())).unwrap();
    let ownership = Arc::new(());
    let error = manager
        .start_refresh_inventory("S", &ownership, coordinator_limits())
        .await
        .err()
        .expect("configured root failure should be returned");
    assert!(error.to_string().contains("configured root failed"));
    assert!(manager.status("S").await.is_ok());
}

#[tokio::test]
async fn configured_inventory_root_failure_is_cleanly_cancelled_when_pending() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_inventories(vec![Err("configured root failed".into())]),
    );
    let mut config = settings(directory.path().join("configured-root-cancelled.sqlite3"));
    config.inventory_root = Some("FCS0201".into());
    let manager = Arc::new(IndexManager::new(client, config));
    manager.with_database(|_| Ok(())).unwrap();
    manager.pending_cancels.lock().unwrap().insert("S".into());

    let ownership = Arc::new(());
    let result = manager
        .start_refresh_inventory("S", &ownership, coordinator_limits())
        .await
        .unwrap();
    assert!(result.is_none());
    assert!(manager.pending_cancels.lock().unwrap().is_empty());
    assert!(manager.status("S").await.is_ok());
}

#[tokio::test]
async fn lifecycle_client_reports_missing_root_inventory_fixture() {
    let client = LifecycleClient::new(vec![], vec![]);
    let result = client.start_inventory_at_root("S", "Root", 1).await;
    assert!(result.is_err());
    assert!(
        result
            .err()
            .is_some_and(|error| error.to_string().contains("no root inventory configured"))
    );
}

#[tokio::test]
async fn coordinated_start_falls_back_when_partitioning_is_not_safe() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![Ok(immediate_inventory_handle())], vec![]).with_root_browse_page(
            Ok(BrowsePage {
                nodes: vec![root_node(
                    "Only branch",
                    BrowseNodeKind::Branch,
                    Some("Root.Only"),
                )],
                next_page_token: None,
                complete: true,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            }),
        ),
    );
    let mut config = settings(directory.path().join("partition-unsafe.sqlite3"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client.clone(), config));

    let handle = manager
        .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
        .await
        .unwrap()
        .expect("unsafe partitioning should fall back to full-root inventory");

    assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
    assert_eq!(client.root_inventory_start_count.load(Ordering::Acquire), 0);
    drop(handle);
}

#[tokio::test]
async fn coordinated_start_failure_falls_back_to_full_root_inventory() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![Ok(immediate_inventory_handle())], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_close_result(Err("partition discovery failed".into())),
    );
    let mut config = settings(directory.path().join("partition-fallback.sqlite3"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client.clone(), config));
    let handle = manager
        .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
        .await
        .unwrap()
        .expect("fallback should start a full-root inventory");
    assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
    assert_eq!(client.root_inventory_start_count.load(Ordering::Acquire), 0);
    drop(handle);
}

#[tokio::test]
async fn coordinated_inventory_reports_worker_failure() {
    let client = Arc::new(MockOpcClient::default());
    *client.browse_page_result.lock().unwrap() = Ok(BrowsePage {
        nodes: vec![
            root_node("A", BrowseNodeKind::Branch, Some("Root.A")),
            root_node("B", BrowseNodeKind::Branch, Some("Root.B")),
        ],
        next_page_token: None,
        complete: true,
        organization: NamespaceOrganization::Hierarchical,
        source: BrowseSource::Da2,
        warning: None,
    });
    *client.inventory_root_events.lock().unwrap() = HashMap::from([
        (
            "Root.A".into(),
            VecDeque::from([Err("worker exploded".into())]),
        ),
        ("Root.B".into(), VecDeque::from([Ok(completed_inventory())])),
    ]);
    let mut config = settings(PathBuf::from(":memory:"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client, config));
    let InventoryHandle { mut stream, .. } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .expect("two roots should use coordinated inventory");

    let error = stream
        .next()
        .await
        .expect("worker failure should produce an event")
        .expect_err("worker failure must be terminal");
    assert!(error.to_string().contains("worker exploded"));
    stream.shutdown().await.unwrap();
}

#[tokio::test]
async fn coordinated_inventory_reports_worker_registration_failure() {
    let control = Arc::new(RecordingInventoryControl::default());
    control.fail_pacing_on_call(1);
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(BrowsePage {
                nodes: vec![
                    root_node("A", BrowseNodeKind::Branch, Some("Root.A")),
                    root_node("B", BrowseNodeKind::Branch, Some("Root.B")),
                ],
                next_page_token: None,
                complete: true,
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                warning: None,
            }))
            .with_root_inventories(vec![Ok(handle_with_control(VecDeque::new(), control))]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(PathBuf::from(":memory:")),
    ));
    let InventoryHandle { mut stream, .. } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .expect("one root should use coordinated inventory");
    let error = stream
        .next()
        .await
        .expect("registration failure should produce an event")
        .expect_err("registration failure must be terminal");
    assert!(error.to_string().contains("test pacing update failure"));
}

#[tokio::test]
async fn coordinated_inventory_suppresses_registration_error_after_stop_request() {
    let parent = CoordinatedInventoryControl::new(InventoryPacing::default());
    let control = Arc::new(RegisterStoppingControl {
        parent: Arc::clone(&parent),
    });
    let control_for_handle = Arc::clone(&control);
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![]).with_root_inventories(vec![Ok(handle_with_control(
            VecDeque::new(),
            control_for_handle,
        ))]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(PathBuf::from(":memory:")),
    ));
    let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
    let worker = tokio::spawn(Arc::clone(&manager).run_inventory_worker(
        "S".into(),
        0,
        queue,
        coordinator_limits(),
        parent,
        sender,
    ));
    assert!(matches!(
        receiver.recv().await,
        Some(WorkerInventoryMessage::Finished { _worker_id: 0 })
    ));
    worker.await.unwrap();
    control.pause();
    control.resume();
    control.cancel();
}

#[tokio::test]
async fn coordinated_inventory_cancels_after_outer_entry_send_failure() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![Ok(InventoryHandle {
                stream: Box::new(BlockingInventoryStream {
                    started: Arc::clone(&started),
                    release: Arc::clone(&release),
                    event: Some(Ok(InventoryEvent::Entry(inventory_entry(
                        "Entry",
                        "Root.A.Entry",
                    )))),
                }),
                control: Arc::new(RecordingInventoryControl::default()),
            })]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(PathBuf::from(":memory:")),
    ));
    let InventoryHandle { stream, control } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .expect("two roots should use coordinated inventory");
    started.notified().await;
    drop(stream);
    release.notify_one();
    for _ in 0..100 {
        if control.is_cancelled() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("coordinator did not observe the closed output channel");
}

#[tokio::test]
async fn coordinated_worker_sender_failures_stop_and_clean_up_the_worker() {
    let events = vec![
        (completed_inventory(), false),
        (
            InventoryEvent::Entry(inventory_entry("Entry", "Entry.Item")),
            true,
        ),
        (InventoryEvent::Progress(zero_inventory_progress()), true),
        (
            InventoryEvent::Slice(InventorySliceObservation {
                sequence: 1,
                backend: InventorySliceBackend::Da2,
                nodes_returned: 1,
                has_more: false,
                native_operations: 1,
                elapsed_ms: 1,
                entries_seen: 1,
                unique_items: 1,
            }),
            true,
        ),
        (completed_inventory(), true),
    ];
    for (event, wait_for_started) in events {
        let (stream, gate) = if wait_for_started {
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            (
                Box::new(BlockingInventoryStream {
                    started: Arc::clone(&started),
                    release: Arc::clone(&release),
                    event: Some(Ok(event)),
                }) as Box<dyn InventoryStream>,
                Some((started, release)),
            )
        } else {
            (
                Box::new(VecInventoryStream {
                    events: VecDeque::from([Ok(event)]),
                }) as Box<dyn InventoryStream>,
                None,
            )
        };
        let client = Arc::new(
            LifecycleClient::new(vec![], vec![]).with_root_inventories(vec![Ok(InventoryHandle {
                stream,
                control: Arc::new(RecordingInventoryControl::default()),
            })]),
        );
        let manager = Arc::new(IndexManager::new(
            client,
            settings(PathBuf::from(":memory:")),
        ));
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(Arc::clone(&manager).run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            control,
            sender,
        ));
        if wait_for_started {
            assert!(matches!(
                receiver.recv().await,
                Some(WorkerInventoryMessage::Started { worker_id: 0 })
            ));
            let (started, release) = gate.unwrap();
            started.notified().await;
            drop(receiver);
            release.notify_one();
        } else {
            drop(receiver);
        }
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn coordinated_inventory_falls_back_without_safe_roots() {
    let client = Arc::new(MockOpcClient::default());
    *client.capabilities_result.lock().unwrap() = Ok(BrowseCapabilities {
        organization: NamespaceOrganization::Flat,
        source: BrowseSource::Flat,
        supports_browse_sessions: false,
        supports_search: false,
        max_page_size: 100,
    });
    let mut config = settings(PathBuf::from(":memory:"));
    config.worker_count = 4;
    let manager = Arc::new(IndexManager::new(client.clone(), config));

    assert!(
        manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(client.inventory_root_start_count.load(Ordering::Acquire), 0);

    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("flat-fallback.sqlite3")),
    ));
    let handle = manager
        .start_refresh_inventory("S", &Arc::new(()), coordinator_limits())
        .await
        .unwrap()
        .expect("unsafe partitioning should fall back to full-root inventory");
    assert_eq!(client.inventory_start_count.load(Ordering::Acquire), 1);
    drop(handle);
}

#[tokio::test]
async fn coordinated_inventory_falls_back_when_root_page_has_too_few_workers() {
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![]).with_root_browse_page(Ok(BrowsePage {
            nodes: vec![root_node(
                "Only branch",
                BrowseNodeKind::Branch,
                Some("Root"),
            )],
            next_page_token: None,
            complete: true,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        })),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(PathBuf::from(":memory:")),
    ));
    assert!(
        manager
            .start_coordinated_inventory("S", coordinator_limits())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coordinated_inventory_rejects_root_continuation_pages() {
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![]).with_root_browse_page(Ok(BrowsePage {
            nodes: Vec::new(),
            next_page_token: Some("next".into()),
            complete: false,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
            warning: None,
        })),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(PathBuf::from(":memory:")),
    ));
    let error = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .err()
        .expect("root continuation should be rejected");
    assert!(error.to_string().contains("continuation page"));
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_forwards_control_to_all_root_workers() {
    let directory = tempdir().unwrap();
    let first_control = Arc::new(RecordingInventoryControl::default());
    let second_control = Arc::new(RecordingInventoryControl::default());
    let started = Arc::new(Notify::new());
    let started_count = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![
                Ok(InventoryHandle {
                    stream: Box::new(ControlledInventoryStream {
                        started: Arc::clone(&started),
                        started_count: Arc::clone(&started_count),
                        release: Arc::clone(&release),
                        event: Some(Ok(completed_inventory())),
                        shutdowns: Arc::clone(&shutdowns),
                    }),
                    control: Arc::clone(&first_control) as Arc<dyn InventoryControl>,
                }),
                Ok(InventoryHandle {
                    stream: Box::new(ControlledInventoryStream {
                        started: Arc::clone(&started),
                        started_count: Arc::clone(&started_count),
                        release: Arc::clone(&release),
                        event: Some(Ok(completed_inventory())),
                        shutdowns: Arc::clone(&shutdowns),
                    }),
                    control: Arc::clone(&second_control) as Arc<dyn InventoryControl>,
                }),
            ]),
    );
    let mut config = settings(directory.path().join("control-forwarding.sqlite3"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client, config));
    let InventoryHandle {
        stream: mut inventory_stream,
        control,
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    let reader = tokio::spawn(async move { inventory_stream.next().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while started_count.load(Ordering::Acquire) < 2 {
            started.notified().await;
        }
    })
    .await
    .unwrap();
    control.pause();
    control.resume();
    control.cancel();

    assert!(!first_control.paused.load(Ordering::Acquire));
    assert!(!second_control.paused.load(Ordering::Acquire));
    assert_eq!(first_control.pause_count.load(Ordering::Acquire), 1);
    assert_eq!(second_control.pause_count.load(Ordering::Acquire), 1);
    assert_eq!(first_control.resume_count.load(Ordering::Acquire), 1);
    assert_eq!(second_control.resume_count.load(Ordering::Acquire), 1);
    assert!(first_control.is_cancelled());
    assert!(second_control.is_cancelled());
    release.notify_waiters();
    assert!(reader.await.unwrap().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_cancellation_stops_active_workers_and_joins_them() {
    let directory = tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let first_control = Arc::new(CancellationInventoryControl::default());
    let second_control = Arc::new(CancellationInventoryControl::default());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![
                Ok(InventoryHandle {
                    stream: Box::new(CancellationAwareInventoryStream {
                        started: Arc::clone(&started),
                        control: Arc::clone(&first_control),
                        shutdowns: Arc::clone(&shutdowns),
                        emitted: false,
                    }),
                    control: Arc::clone(&first_control) as Arc<dyn InventoryControl>,
                }),
                Ok(InventoryHandle {
                    stream: Box::new(CancellationAwareInventoryStream {
                        started: Arc::clone(&started),
                        control: Arc::clone(&second_control),
                        shutdowns: Arc::clone(&shutdowns),
                        emitted: false,
                    }),
                    control: Arc::clone(&second_control) as Arc<dyn InventoryControl>,
                }),
            ]),
    );
    let mut config = settings(directory.path().join("cancellation.sqlite3"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client, config));
    let InventoryHandle {
        stream: mut inventory_stream,
        control,
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    started.notified().await;
    first_control.pause();
    first_control.resume();
    control.cancel();
    let event = inventory_stream.next().await;
    assert!(matches!(
        event,
        Some(Ok(InventoryEvent::Completed(result)))
            if result.cancelled && !result.complete
    ));
    assert_eq!(shutdowns.load(Ordering::Acquire), 2);
    assert!(first_control.is_cancelled());
    assert!(second_control.is_cancelled());
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_handles_closed_coordinator_senders() {
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    ));
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let (sender, receiver) =
        tokio::sync::mpsc::unbounded_channel::<anyhow::Result<InventoryEvent>>();
    drop(receiver);
    manager
        .run_coordinated_inventory(
            "S".into(),
            InventoryRootPlan {
                root_entries: vec![inventory_entry("Root", "Root.Item")],
                worker_roots: Vec::new(),
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
            },
            1,
            coordinator_limits(),
            control,
            sender,
        )
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_worker_handles_registration_cancellation_and_failures() {
    let cases = [
        ("registration-cancelled", true, false),
        ("registration-failed", false, true),
    ];
    for (name, cancel_during_start, fail_registration) in cases {
        let directory = tempdir().unwrap();
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let parent = Arc::clone(&control);
        let inventory_control = Arc::new(RecordingInventoryControl::default());
        if fail_registration {
            inventory_control.fail_pacing_on_call(1);
        }
        let client = LifecycleClient::new(vec![], vec![])
            .with_root_inventory_start_hook(move || {
                if cancel_during_start {
                    parent.cancel();
                }
            })
            .with_root_inventories(vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&inventory_control),
            ))]);
        let manager = Arc::new(IndexManager::new(
            Arc::new(client),
            settings(directory.path().join(name)),
        ));
        let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(manager.run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            control,
            sender,
        ));
        let mut saw_finished = false;
        while let Some(message) = receiver.recv().await {
            match message {
                WorkerInventoryMessage::Failed { error, .. } => {
                    assert!(fail_registration);
                    assert!(error.contains("test pacing update failure"));
                }
                WorkerInventoryMessage::Finished { .. } => {
                    saw_finished = true;
                    break;
                }
                _ => {}
            }
        }
        worker.await.unwrap();
        assert!(saw_finished);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_worker_stops_after_cancelled_root_start_failure() {
    let directory = tempdir().unwrap();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let parent = Arc::clone(&control);
    let client = LifecycleClient::new(vec![], vec![])
        .with_root_inventory_start_hook(move || parent.cancel())
        .with_root_inventories(vec![Err("root start failed".into())]);
    let manager = Arc::new(IndexManager::new(
        Arc::new(client),
        settings(directory.path().join("cancelled-root-start.sqlite3")),
    ));
    let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
    let worker = tokio::spawn(manager.run_inventory_worker(
        "S".into(),
        0,
        queue,
        coordinator_limits(),
        control,
        sender,
    ));
    assert!(matches!(
        receiver.recv().await,
        Some(WorkerInventoryMessage::Finished { _worker_id: 0 })
    ));
    assert!(receiver.recv().await.is_none());
    worker.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_worker_handles_closed_event_senders() {
    let events = vec![
        Some(Ok(InventoryEvent::Entry(inventory_entry(
            "Entry", "S.Entry",
        )))),
        Some(Ok(InventoryEvent::Progress(zero_progress()))),
        Some(Ok(InventoryEvent::Slice(InventorySliceObservation {
            sequence: 1,
            backend: InventorySliceBackend::Da2,
            nodes_returned: 1,
            has_more: false,
            native_operations: 1,
            elapsed_ms: 1,
            entries_seen: 1,
            unique_items: 1,
        }))),
        Some(Ok(completed_inventory())),
        Some(Err(anyhow::anyhow!("stream error"))),
        None,
    ];
    for event in events {
        let directory = tempdir().unwrap();
        let client = LifecycleClient::new(vec![], vec![]).with_root_inventories(vec![Ok(
            handle_with_control(
                event.into_iter().collect(),
                Arc::new(RecordingInventoryControl::default()),
            ),
        )]);
        let manager = Arc::new(IndexManager::new(
            Arc::new(client),
            settings(directory.path().join("closed-sender.sqlite3")),
        ));
        let queue = Arc::new(Mutex::new(VecDeque::from(["Root".to_string()])));
        let control = CoordinatedInventoryControl::new(InventoryPacing::default());
        let (sender, mut receiver) =
            tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
        let worker = tokio::spawn(manager.run_inventory_worker(
            "S".into(),
            0,
            queue,
            coordinator_limits(),
            Arc::clone(&control),
            sender,
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(WorkerInventoryMessage::Started { worker_id: 0 })
        ));
        drop(receiver);
        worker.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_worker_reports_a_poisoned_root_queue() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("poisoned-queue.sqlite3")),
    ));
    let queue = Arc::new(Mutex::new(VecDeque::<String>::new()));
    let poison_queue = Arc::clone(&queue);
    let _ = std::thread::spawn(move || {
        let _guard = poison_queue.lock().unwrap();
        panic!("poison root queue");
    })
    .join();
    let control = CoordinatedInventoryControl::new(InventoryPacing::default());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<WorkerInventoryMessage>();
    let worker = tokio::spawn(manager.run_inventory_worker(
        "S".into(),
        0,
        queue,
        coordinator_limits(),
        control,
        sender,
    ));
    assert!(matches!(
        receiver.recv().await,
        Some(WorkerInventoryMessage::Failed { error, .. })
            if error.contains("root queue lock poisoned")
    ));
    worker.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_detects_worker_task_panic() {
    let directory = tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![
                Ok(InventoryHandle {
                    stream: Box::new(PanicAfterReleaseInventoryStream {
                        started: Arc::clone(&started),
                        release: Arc::clone(&release),
                    }),
                    control: Arc::new(RecordingInventoryControl::default()),
                }),
                Ok(immediate_inventory_handle()),
            ]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("worker-panic.sqlite3")),
    ));
    let InventoryHandle {
        stream: mut inventory_stream,
        ..
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    started.notified().await;
    release.notify_one();
    let event = inventory_stream.next().await;
    assert!(matches!(
        event,
        Some(Err(error)) if error.to_string().contains("panicked")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_reports_root_worker_start_failure() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![
                Err("root worker failed to start".into()),
                Ok(immediate_inventory_handle()),
            ]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("worker-start-failure.sqlite3")),
    ));
    let InventoryHandle {
        stream: mut inventory_stream,
        ..
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    let event = inventory_stream.next().await;
    assert!(matches!(
        event,
        Some(Err(error)) if error.to_string().contains("root worker failed to start")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_reports_incomplete_worker_termination() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![
                Ok(InventoryHandle {
                    stream: Box::new(VecInventoryStream {
                        events: VecDeque::from([Ok(InventoryEvent::Completed(
                            InventoryCompleted {
                                complete: false,
                                cancelled: false,
                                truncated: false,
                                warning: Some("worker ended early".into()),
                                organization: NamespaceOrganization::Hierarchical,
                                source: BrowseSource::Da2,
                            },
                        ))]),
                    }),
                    control: Arc::new(RecordingInventoryControl::default()),
                }),
                Ok(immediate_inventory_handle()),
            ]),
    );
    let mut config = settings(directory.path().join("worker-incomplete.sqlite3"));
    config.worker_count = 2;
    let manager = Arc::new(IndexManager::new(client, config));
    let InventoryHandle {
        stream: mut inventory_stream,
        ..
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    let event = inventory_stream.next().await;
    assert!(matches!(
        event,
        Some(Err(error)) if error.to_string().contains("ended before completion")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_reports_root_browse_close_failure() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_close_result(Err("root browse close failed".into())),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("root-close-failure.sqlite3")),
    ));
    let result = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await;
    assert!(result.is_err());
    let error = result.err().unwrap();
    assert!(error.to_string().contains("root browse close failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn coordinated_inventory_shutdown_panic_is_reported() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(vec![], vec![])
            .with_root_browse_page(Ok(coordinator_root_page()))
            .with_root_inventories(vec![Ok(InventoryHandle {
                stream: Box::new(CompletedThenShutdownPanicInventoryStream { emitted: false }),
                control: Arc::new(RecordingInventoryControl::default()),
            })]),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("shutdown-panic.sqlite3")),
    ));
    let InventoryHandle {
        stream: mut inventory_stream,
        ..
    } = manager
        .start_coordinated_inventory("S", coordinator_limits())
        .await
        .unwrap()
        .unwrap();
    let event = inventory_stream.next().await;
    assert!(matches!(
        event,
        Some(Err(error)) if error.to_string().contains("panicked")
    ));
}

fn coordinator_limits() -> InventoryLimits {
    InventoryLimits {
        item_rate_per_second: 0,
        batch_size: 25,
        duty_cycle_percent: 100,
    }
}

fn coordinator_root_page() -> BrowsePage {
    BrowsePage {
        nodes: vec![
            root_node("Branch A", BrowseNodeKind::Branch, Some("Root.A")),
            root_node("Branch B", BrowseNodeKind::Branch, Some("Root.B")),
        ],
        next_page_token: None,
        complete: true,
        organization: NamespaceOrganization::Hierarchical,
        source: BrowseSource::Da2,
        warning: None,
    }
}

#[test]
fn build_telemetry_aggregates_slices_entries_and_commits() {
    let mut telemetry = BuildTelemetry::default();
    assert_eq!(telemetry.commit_latency_percentile(50), None);
    telemetry.record_progress();
    telemetry.record_entry(InventoryNodeKind::Item);
    telemetry.record_entry(InventoryNodeKind::BranchAndItem);
    telemetry.record_slice(&InventorySliceObservation {
        sequence: 1,
        backend: InventorySliceBackend::Da2,
        nodes_returned: 10,
        has_more: true,
        native_operations: 4,
        elapsed_ms: 25,
        entries_seen: 10,
        unique_items: 8,
    });
    telemetry.record_slice(&InventorySliceObservation {
        sequence: 2,
        backend: InventorySliceBackend::Da3,
        nodes_returned: 5,
        has_more: false,
        native_operations: 2,
        elapsed_ms: 40,
        entries_seen: 14,
        unique_items: 11,
    });
    telemetry.record_commit(10, Duration::from_millis(7), false);
    telemetry.record_commit(0, Duration::from_millis(12), true);
    telemetry.record_terminal_event(Duration::from_millis(123));

    assert_eq!(telemetry.progress_events, 1);
    assert_eq!(telemetry.item_entries, 1);
    assert_eq!(telemetry.branch_and_item_entries, 1);
    assert_eq!(telemetry.slice_count, 2);
    assert_eq!(telemetry.slice_nodes_returned, 15);
    assert_eq!(telemetry.slice_native_operations, 6);
    assert_eq!(telemetry.slice_elapsed_ms, 65);
    assert_eq!(telemetry.slice_elapsed_max_ms, 40);
    assert_eq!(telemetry.slice_entries_delta, 14);
    assert_eq!(telemetry.slice_entries_delta_max, 10);
    assert_eq!(telemetry.slice_unique_items_delta, 11);
    assert_eq!(telemetry.da2_slices, 1);
    assert_eq!(telemetry.da3_slices, 1);
    assert_eq!(telemetry.commit_attempts, 2);
    assert_eq!(telemetry.commit_failures, 1);
    assert_eq!(telemetry.committed_entries, 10);
    assert_eq!(telemetry.commit_elapsed_ms, 19);
    assert_eq!(telemetry.commit_elapsed_max_ms, 12);
    assert_eq!(telemetry.commit_latency_percentile(50), Some(7));
    assert_eq!(telemetry.commit_latency_percentile(95), Some(12));
    assert_eq!(telemetry.terminal_event_ms, Some(123));
}

#[test]
fn build_telemetry_keeps_only_the_latest_commit_latency_samples() {
    let mut telemetry = BuildTelemetry::default();
    for elapsed_ms in 0..=256 {
        telemetry.record_commit(0, Duration::from_millis(elapsed_ms), false);
    }

    assert_eq!(telemetry.commit_latency_samples_ms.len(), 256);
    assert_eq!(telemetry.commit_latency_samples_ms.front(), Some(&1));
    assert_eq!(telemetry.commit_latency_samples_ms.back(), Some(&256));
    assert_eq!(telemetry.commit_latency_percentile(50), Some(128));
}

#[test]
fn terminal_counts_distinguish_progress_snapshot_from_persisted_rows() {
    let config = settings(PathBuf::from("test-index.sqlite3"));
    let mut state = BuildRunState::new(&config, None);
    state.last_progress = InventoryProgress {
        entries_seen: 11,
        unique_items: 9,
        ..zero_progress()
    };
    state.persisted_item_count = 10;
    state.drained_event_count = 4;
    state.received_entry_count = 3;
    state.pending = vec![
        inventory_entry("Pending A", "A.pending"),
        inventory_entry("Pending A duplicate", "A.pending"),
        inventory_entry("Pending B", "B.pending"),
    ];

    assert_eq!(
        state.terminal_counts(),
        TerminalBuildCounts {
            last_progress_entries_seen: 11,
            last_progress_unique_items: 9,
            persisted_items: 10,
            drained_events: 4,
            received_entry_events: 3,
            pending_entries: 3,
            pending_unique_items: 2,
        }
    );
}

#[tokio::test]
async fn terminal_counts_reconcile_entries_drained_before_stream_termination() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let mut handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("A", "A.item"))),
                Ok(InventoryEvent::Progress(zero_progress())),
                Ok(InventoryEvent::Entry(inventory_entry("B", "B.item"))),
            ]),
        }),
        control,
    };
    let mut state = BuildRunState::new(&settings(PathBuf::from(":memory:")), None);

    let outcome = manager
        .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
        .await;

    assert!(matches!(
        outcome,
        BuildLoopOutcome::Failed(error)
            if error == "inventory stream ended before completion"
    ));
    let counts = state.terminal_counts();
    assert_eq!(counts.drained_events, 3);
    assert_eq!(counts.received_entry_events, 2);
    assert_eq!(counts.persisted_items, 0);
    assert_eq!(counts.pending_entries, 2);
    assert_eq!(counts.pending_unique_items, 2);
    assert_eq!(
        counts.received_entry_events,
        counts.persisted_items + counts.pending_entries
    );
    assert_eq!(
        namespace_string(NamespaceOrganization::Unspecified),
        "unspecified"
    );
    assert_eq!(namespace_string(NamespaceOrganization::Flat), "flat");
}

#[test]
fn in_memory_build_locks_are_not_file_backed() {
    let lock = BuildFileLock::acquire(Path::new(":memory:"), "S").unwrap();

    assert!(lock.file.is_none());
    assert!(!BuildFileLock::is_held(Path::new(":memory:"), "S").unwrap());
}

#[test]
fn adaptive_limits_translate_to_native_operation_pacing() {
    let pacing = pacing_for_limits(InventoryLimits {
        item_rate_per_second: 100,
        batch_size: 10,
        duty_cycle_percent: 50,
    });
    assert_eq!(pacing.min_interval, Duration::ZERO);
    assert_eq!(pacing.item_rate_per_second, Some(100));
    assert_eq!(pacing.batch_size, Some(10));
    assert_eq!(
        pacing_for_limits(InventoryLimits {
            item_rate_per_second: 3,
            batch_size: 1,
            duty_cycle_percent: 1,
        })
        .min_interval,
        Duration::ZERO
    );
    assert_eq!(
        pacing_for_limits(InventoryLimits {
            item_rate_per_second: 0,
            batch_size: 1,
            duty_cycle_percent: 1,
        })
        .min_interval,
        Duration::ZERO
    );
    assert_eq!(
        pacing_for_limits(InventoryLimits {
            item_rate_per_second: 0,
            batch_size: 1,
            duty_cycle_percent: 1,
        })
        .item_rate_per_second,
        None
    );
    assert_eq!(
        pacing_for_limits(InventoryLimits {
            item_rate_per_second: 100,
            batch_size: MAX_NATIVE_INVENTORY_BATCH_SIZE + 1,
            duty_cycle_percent: 50,
        })
        .batch_size,
        Some(MAX_NATIVE_INVENTORY_BATCH_SIZE)
    );
    assert_ne!(stable_server_hash("S"), stable_server_hash("T"));
}

#[test]
fn slice_observations_feed_adaptive_controller_health_state() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    let slice = InventorySliceObservation {
        sequence: 1,
        backend: InventorySliceBackend::Da2,
        nodes_returned: 0,
        has_more: false,
        native_operations: 0,
        elapsed_ms: 1,
        entries_seen: 0,
        unique_items: 0,
    };
    let observation = manager.controller_observation_for_slice("S", &slice);
    assert!(observation.inventory_error);
    assert!(!observation.foreground_active);
}

#[tokio::test]
async fn rate_limiter_and_wait_helpers_honor_cancellation() {
    let control_impl = Arc::new(TestInventoryControl::default());
    let control: Arc<dyn InventoryControl> = control_impl.clone();
    control_impl.pause();
    control_impl.resume();

    let mut disabled = ItemRateLimiter::new(0, 0);
    assert!(disabled.acquire(&control).await);
    control_impl.cancel();
    assert!(!disabled.acquire(&control).await);

    let active_impl = Arc::new(TestInventoryControl::default());
    let active: Arc<dyn InventoryControl> = active_impl.clone();
    assert!(wait_with_cancellation(&active, Duration::ZERO).await);
    active_impl.cancel();
    let mut cancelled_limiter = ItemRateLimiter::new(1, 1);
    assert!(!cancelled_limiter.acquire(&active).await);

    let active_impl = Arc::new(TestInventoryControl::default());
    let active: Arc<dyn InventoryControl> = active_impl.clone();
    let mut limited = ItemRateLimiter::new(10_000, 1);
    assert!(limited.acquire(&active).await);
    assert!(limited.acquire(&active).await);

    let waiting_impl = Arc::new(TestInventoryControl::default());
    let waiting: Arc<dyn InventoryControl> = waiting_impl.clone();
    let canceller = Arc::clone(&waiting_impl);
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        canceller.cancel();
    });
    assert!(!wait_with_cancellation(&waiting, Duration::from_millis(200)).await);
    task.await.unwrap();
}

#[test]
fn promotion_uses_inventory_metadata_without_scanning_entries() {
    let directory = tempdir().unwrap();
    let mut db = IndexDb::open(&directory.path().join("duplicates.sqlite3")).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    drop_table(&mut db, "entries");
    db.connection
        .execute_batch(
            "CREATE TABLE entries (
                 server TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 item_id TEXT NOT NULL,
                 item_id_norm TEXT NOT NULL,
                 display_name TEXT NOT NULL,
                 display_name_norm TEXT NOT NULL,
                 kind INTEGER NOT NULL,
                 breadcrumbs TEXT NOT NULL
             );
             INSERT INTO entries VALUES
               ('S', 1, 'duplicate', 'duplicate', 'One', 'one', 1, '[]'),
               ('S', 1, 'duplicate', 'duplicate', 'Two', 'two', 1, '[]');",
        )
        .unwrap();
    let progress = InventoryProgress {
        entries_seen: 2,
        unique_items: 1,
        ..zero_progress()
    };
    db.promote("S", generation, "2", &progress).unwrap();
    let row = db.status_rows("S").unwrap().remove(0);
    assert_eq!(row.state, "active");
    assert_eq!(row.entry_count, 1);
    assert_eq!(row.unique_item_count, 1);
}

#[tokio::test]
async fn refresh_capability_failure_cancels_inventory_and_records_error() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::new(),
            Arc::clone(&control),
        ))],
        vec![Err("capabilities failed".into())],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));

    assert_eq!(
        manager.refresh("S", true).await.unwrap_err().to_string(),
        "capabilities failed"
    );
    assert!(control.cancelled.load(Ordering::Acquire));
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Failed);
    assert_eq!(status.last_error.as_deref(), Some("capabilities failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_database_failure_cancels_inventory_and_records_error() {
    let directory = tempdir().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let control = Arc::new(RecordingInventoryControl::default());
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::new(),
            Arc::clone(&control),
        ))],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| {
            drop_table(db, "generations");
            Ok(())
        })
        .unwrap();

    let error = manager.refresh("S", true).await.unwrap_err().to_string();
    assert!(error.contains("no such table"));
    assert!(control.cancelled.load(Ordering::Acquire));
    assert_eq!(
        manager
            .runtime
            .lock()
            .unwrap()
            .get("S")
            .unwrap()
            .last_error
            .clone(),
        Some(error)
    );
}

#[tokio::test]
async fn refresh_rejects_a_duplicate_in_process_build_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let manager = Arc::new(IndexManager::new(
        Arc::new(LifecycleClient::new(vec![], vec![])),
        settings(path.clone()),
    ));
    let lock = BuildFileLock::acquire(&path, "S").unwrap();
    manager.build_locks.lock().unwrap().insert("S".into(), lock);

    let error = manager.refresh("S", true).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("build lock is already held in this process")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_honors_cancel_during_inventory_startup() {
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
            vec![Ok(default_capabilities())],
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

    let status = manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    assert_eq!(status.state, IndexState::Partial);
    inventory_release.notify_one();

    let status = tokio::time::timeout(Duration::from_secs(1), refresh)
        .await
        .expect("refresh should finish after startup cancellation")
        .expect("refresh task should not panic")
        .unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    assert!(control.cancelled.load(Ordering::Acquire));
    assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn completed_inventory_warning_keeps_generation_active_and_searchable() {
    let directory = tempdir().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let warning = "skipped 1 DA2 branch name(s) rejected by the server";
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
                Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: Some(warning.into()),
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                })),
            ]),
            Arc::new(RecordingInventoryControl::default()),
        ))],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));

    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Ready).await;

    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::Ready);
    assert_eq!(status.active_generation, 1);
    assert_eq!(status.entry_count, 1);
    assert_eq!(status.unique_item_count, 1);
    assert_eq!(status.last_error.as_deref(), Some(warning));
    assert_eq!(
        manager
            .search("S", "tag", 3, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
    let rows = manager.with_database(|db| db.status_rows("S")).unwrap();
    assert_eq!(rows[0].state, "active");
    assert_eq!(rows[0].last_error.as_deref(), Some(warning));
}

#[tokio::test(flavor = "current_thread")]
async fn build_reports_batch_progress_and_promotion_database_failures() {
    let directory = tempdir().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let mut successful_config = settings(directory.path().join("successful-batch.sqlite3"));
    successful_config.batch_size = 1;
    let successful_manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        successful_config,
    ));
    successful_manager.refresh("S", true).await.unwrap();
    wait_for_build(&successful_manager, IndexState::Ready).await;
    assert_eq!(successful_manager.status("S").await.unwrap().entry_count, 1);

    let mut batch_config = settings(directory.path().join("batch.sqlite3"));
    batch_config.batch_size = 1;
    batch_config.commit_batch_size = 1;
    let batch_client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([Ok(InventoryEvent::Entry(InventoryEntry {
                display_name: "Invalid".into(),
                item_id: String::new(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec![],
            }))]),
            Arc::new(RecordingInventoryControl::default()),
        ))],
        vec![],
    ));
    let batch_manager = Arc::new(IndexManager::new(batch_client, batch_config));
    batch_manager.refresh("S", true).await.unwrap();
    wait_for_state(&batch_manager, "S", IndexState::Failed).await;
    assert_eq!(
        batch_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .as_deref(),
        Some("inventory entry has an empty ItemID")
    );

    let progress_started = Arc::new(Notify::new());
    let progress_release = Arc::new(Notify::new());
    let progress_manager = manager_with_blocking_event(
        directory.path().join("progress.sqlite3"),
        Ok(InventoryEvent::Progress(zero_progress())),
        Arc::clone(&progress_started),
        Arc::clone(&progress_release),
    );
    progress_manager.refresh("S", true).await.unwrap();
    progress_started.notified().await;
    progress_manager
        .with_database(|db| {
            db.connection.execute_batch(
                "CREATE TRIGGER fail_progress
                 BEFORE UPDATE OF entry_count ON generations
                 BEGIN
                   SELECT RAISE(FAIL, 'progress write failed');
                 END;",
            )?;
            Ok(())
        })
        .unwrap();
    progress_release.notify_one();
    wait_for_state(&progress_manager, "S", IndexState::Failed).await;
    assert!(
        progress_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .unwrap()
            .contains("progress write failed")
    );

    let promotion_started = Arc::new(Notify::new());
    let promotion_release = Arc::new(Notify::new());
    let promotion_manager = manager_with_blocking_event(
        directory.path().join("promotion.sqlite3"),
        Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: true,
            cancelled: false,
            truncated: false,
            warning: None,
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        })),
        Arc::clone(&promotion_started),
        Arc::clone(&promotion_release),
    );
    promotion_manager.refresh("S", true).await.unwrap();
    promotion_started.notified().await;
    promotion_manager
        .with_database(|db| {
            db.connection.execute_batch(
                "CREATE TRIGGER fail_promotion
                 BEFORE UPDATE OF state ON generations
                 WHEN NEW.state = 'active'
                 BEGIN
                   SELECT RAISE(FAIL, 'promotion failed');
                 END;",
            )?;
            Ok(())
        })
        .unwrap();
    promotion_release.notify_one();
    wait_for_state(&promotion_manager, "S", IndexState::Failed).await;
    assert!(
        promotion_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .unwrap()
            .contains("promotion failed")
    );

    let final_insert_client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("Final", "S.Final"))),
                Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                })),
            ]),
            Arc::new(RecordingInventoryControl::default()),
        ))],
        vec![],
    ));
    let final_insert_manager = Arc::new(IndexManager::new(
        final_insert_client,
        settings(directory.path().join("final-insert.sqlite3")),
    ));
    final_insert_manager.refresh("S", true).await.unwrap();
    final_insert_manager
        .with_database(|db| {
            db.connection.execute_batch(
                "CREATE TRIGGER fail_final_insert
                 BEFORE INSERT ON entries
                 BEGIN
                   SELECT RAISE(FAIL, 'final insert failed');
                 END;",
            )?;
            Ok(())
        })
        .unwrap();
    wait_for_state(&final_insert_manager, "S", IndexState::Failed).await;
    assert!(
        final_insert_manager
            .status("S")
            .await
            .unwrap()
            .last_error
            .unwrap()
            .contains("final insert failed")
    );
}

#[tokio::test]
async fn build_stops_when_maintenance_health_or_rate_limit_control_is_cancelled() {
    async fn run_cancelled_case(path: PathBuf, maintenance_windows: Vec<String>) -> IndexStatus {
        let control = Arc::new(RecordingInventoryControl::default());
        control.cancel();
        let client = Arc::new(LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        ));
        let mut config = settings(path);
        config.maintenance_windows = maintenance_windows;
        let manager = Arc::new(IndexManager::new(client, config));
        manager.refresh("S", true).await.unwrap();
        wait_for_state(&manager, "S", IndexState::Failed).await;
        manager.status("S").await.unwrap()
    }

    let directory = tempdir().unwrap();
    let health = run_cancelled_case(directory.path().join("health.sqlite3"), vec![]).await;
    assert_eq!(
        health.last_error.as_deref(),
        Some("inventory stream ended before completion")
    );

    let now = Local::now();
    let minute = (now.hour() * 60 + now.minute()) as u16;
    let maintenance = format!(
        "{:02}:{:02}-{:02}:{:02}",
        ((minute + 2) % 1440) / 60,
        ((minute + 2) % 1440) % 60,
        ((minute + 3) % 1440) / 60,
        ((minute + 3) % 1440) % 60
    );
    let maintenance = run_cancelled_case(
        directory.path().join("maintenance.sqlite3"),
        vec![maintenance],
    )
    .await;
    assert_eq!(
        maintenance.last_error.as_deref(),
        Some("inventory stream ended before completion")
    );

    let control = Arc::new(RecordingInventoryControl::default());
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(InventoryHandle {
            stream: Box::new(CancellingEntryStream {
                control: Arc::clone(&control),
                yielded: false,
            }),
            control: control.clone(),
        })],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("rate.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Failed).await;
    assert!(control.cancelled.load(Ordering::Acquire));
}

#[test]
fn finish_build_checks_control_identity_without_an_ownership_token() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("defensive-finalization.sqlite3")),
    ));
    let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&current));
    manager.coordination.build_owners.lock().unwrap().clear();
    let obsolete: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        manager.finish_build_inner("S", Some(&obsolete), None, None);
        manager.finish_build_inner("S", None, None, None);
    });
    assert!(
        manager
            .runtime
            .lock()
            .unwrap()
            .get("S")
            .unwrap()
            .build
            .is_none()
    );
    assert!(manager.active_builds.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn finish_build_for_control_handles_current_obsolete_and_poisoned_runtime() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&current));
    manager.finish_build_for_control("S", &current, Some("failed".into()));
    let state = manager.runtime.lock().unwrap();
    assert!(state.get("S").unwrap().build.is_none());
    assert_eq!(
        state.get("S").unwrap().last_error.as_deref(),
        Some("failed")
    );
    drop(state);

    let current: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let obsolete: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&current));
    manager.finish_build_for_control("S", &obsolete, None);
    manager.finish_build_for_control("Missing", &obsolete, None);

    let runtime = Arc::clone(&manager.runtime);
    let _ = std::panic::catch_unwind(move || {
        let _guard = runtime.lock().unwrap();
        panic!("poison index runtime for finalization error-path coverage");
    });
    manager.finish_build_for_control("S", &obsolete, None);
}

#[tokio::test]
async fn control_reports_a_poisoned_runtime_lock() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager
        .with_database(|db| db.enroll("S", &timestamp_now()))
        .unwrap();
    let runtime = Arc::clone(&manager.runtime);
    let _ = std::panic::catch_unwind(move || {
        let _guard = runtime.lock().unwrap();
        panic!("poison index runtime for error-path coverage");
    });
    assert_eq!(
        manager
            .control("S", IndexControlAction::Pause)
            .await
            .unwrap_err()
            .to_string(),
        "index runtime lock poisoned"
    );
}

#[tokio::test]
async fn maintenance_duty_cycle_and_rate_limit_honor_pause_and_cancellation() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.duty_cycle_percent = 50;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: Some(Arc::clone(&trait_control)),
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

    assert!(
        manager
            .wait_for_maintenance(
                &trait_control,
                "S",
                &[MaintenanceWindow {
                    start_minute: 0,
                    end_minute: 0,
                }],
            )
            .await
    );
    assert!(control.resume_count.load(Ordering::Relaxed) > 0);

    manager
        .enforce_duty_cycle(&trait_control, "S", Duration::from_millis(1), 50)
        .await;
    assert!(control.pause_count.load(Ordering::Relaxed) > 0);
    assert!(!control.paused.load(Ordering::Acquire));

    let now = Local::now();
    let current_minute = (now.hour() * 60 + now.minute()) as u16;
    let inactive = MaintenanceWindow {
        start_minute: (current_minute + 2) % (24 * 60),
        end_minute: (current_minute + 3) % (24 * 60),
    };
    control.cancel();
    assert!(
        !manager
            .wait_for_maintenance(&trait_control, "S", &[inactive])
            .await
    );

    let delayed_cancel = Arc::new(RecordingInventoryControl::default());
    let delayed_trait: Arc<dyn InventoryControl> = delayed_cancel.clone();
    let canceller = Arc::clone(&delayed_cancel);
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        canceller.cancel();
    });
    assert!(
        !manager
            .wait_for_maintenance(&delayed_trait, "S", &[inactive])
            .await
    );
    cancel_task.await.unwrap();

    let rate_control = Arc::new(RecordingInventoryControl::default());
    let rate_trait: Arc<dyn InventoryControl> = rate_control.clone();
    let mut limiter = ItemRateLimiter::new(1, 1);
    assert!(limiter.acquire(&rate_trait).await);
    let canceller = Arc::clone(&rate_control);
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        canceller.cancel();
    });
    assert!(!limiter.acquire(&rate_trait).await);
    cancel_task.await.unwrap();

    let progress_control = Arc::new(RecordingInventoryControl::default());
    progress_control.cancel_on_pause();
    let progress_client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([Ok(InventoryEvent::Progress(InventoryProgress {
                branches_visited: 1,
                entries_seen: 1,
                unique_items: 1,
                active_time_ms: 1,
                paused_time_ms: 0,
                items_per_second: 1.0,
                estimated_remaining_ms: None,
            }))]),
            Arc::clone(&progress_control),
        ))],
        vec![],
    ));
    let mut progress_config = settings(directory.path().join("duty-progress.sqlite3"));
    progress_config.duty_cycle_percent = 50;
    let progress_manager = Arc::new(IndexManager::new(progress_client, progress_config));
    progress_manager.refresh("S", true).await.unwrap();
    wait_for_state(&progress_manager, "S", IndexState::Failed).await;
    assert!(progress_control.cancelled.load(Ordering::Acquire));
}

#[tokio::test]
async fn unhealthy_probe_backs_off_and_stops_when_cancelled() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Err("server unavailable".into())],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    manager.runtime.lock().unwrap().insert(
        "S".into(),
        RuntimeState {
            build: Some(RuntimeBuild {
                control: Some(Arc::clone(&trait_control)),
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
    let mut next_probe = Instant::now();
    let mut backoff = Duration::from_secs(1);
    let canceller = Arc::clone(&control);
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        canceller.cancel();
    });

    assert!(
        !manager
            .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff,)
            .await
    );
    cancel_task.await.unwrap();
    assert_eq!(backoff, Duration::from_secs(2));
    assert!(next_probe > Instant::now());
    assert!(control.pause_count.load(Ordering::Relaxed) > 0);

    let delayed_client = Arc::new(
        LifecycleClient::new(vec![], vec![Ok(default_capabilities())])
            .with_capability_delay(Duration::from_millis(2)),
    );
    let mut delayed_config = settings(directory.path().join("delayed.sqlite3"));
    delayed_config.health_latency_threshold_ms = 0;
    let delayed_manager = Arc::new(IndexManager::new(delayed_client, delayed_config));
    let delayed_control = Arc::new(RecordingInventoryControl::default());
    let delayed_trait: Arc<dyn InventoryControl> = delayed_control.clone();
    insert_runtime_build(&delayed_manager, Arc::clone(&delayed_trait));
    let delayed_canceller = Arc::clone(&delayed_control);
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        delayed_canceller.cancel();
    });
    let mut delayed_next_probe = Instant::now();
    let mut delayed_backoff = Duration::from_secs(1);
    assert!(
        !delayed_manager
            .wait_for_health(
                &delayed_trait,
                "S",
                &mut delayed_next_probe,
                &mut delayed_backoff,
            )
            .await
    );
    cancel_task.await.unwrap();

    let recovery_client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Err("temporary failure".into())],
    ));
    let recovery_manager = Arc::new(IndexManager::new(
        recovery_client,
        settings(directory.path().join("recovery.sqlite3")),
    ));
    let recovery_control = Arc::new(RecordingInventoryControl::default());
    let recovery_trait: Arc<dyn InventoryControl> = recovery_control.clone();
    insert_runtime_build(&recovery_manager, Arc::clone(&recovery_trait));
    let mut next_probe = Instant::now();
    let mut no_delay = Duration::ZERO;
    assert!(
        recovery_manager
            .wait_for_health(&recovery_trait, "S", &mut next_probe, &mut no_delay,)
            .await
    );
    assert!(recovery_control.resume_count.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn adaptive_hard_pause_recovers_without_waiting_for_an_inventory_slice() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("recovery.sqlite3"));
    config.adaptive = true;
    config.adaptive_recovery_delay_seconds = 0;
    config.adaptive_healthy_window_seconds = 1;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));
    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    let paused = controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    assert!(paused.paused);
    manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
    assert!(control.paused.load(Ordering::Acquire));

    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);
    tokio::time::timeout(
        Duration::from_secs(2),
        manager.wait_for_controller_recovery(&trait_control, "S", &mut controller),
    )
    .await
    .expect("controller did not recover")
    .expect("controller recovery pacing update failed")
    .then_some(())
    .expect("controller recovery was cancelled");
    assert!(!control.paused.load(Ordering::Acquire));
    assert!(control.resume_count.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn health_wait_rechecks_after_a_delayed_probe_becomes_due() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("delayed-health.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));
    manager.set_pause_overlay("S", None, Some(true));

    let mut next_probe = Instant::now() + Duration::from_millis(25);
    let mut backoff = Duration::from_secs(1);
    assert!(
        manager
            .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff,)
            .await
    );

    assert!(!manager.health_overlay_active("S"));
    assert_eq!(backoff, Duration::from_secs(1));
}

#[tokio::test]
async fn initial_pacing_failure_is_returned_and_recorded() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    control.fail_pacing_on_call(1);
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::new(),
            Arc::clone(&control),
        ))],
        vec![],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));

    let error = manager.refresh("S", true).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("unable to apply initial inventory pacing")
    );
    assert!(control.is_cancelled());
    assert_eq!(
        manager.status("S").await.unwrap().last_error.as_deref(),
        Some("unable to apply initial inventory pacing: test pacing update failure")
    );
}

#[tokio::test]
async fn build_readiness_failure_stops_the_loop_and_cancels_control() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("readiness-failure.sqlite3"));
    config.adaptive = true;
    config.adaptive_recovery_delay_seconds = 0;
    config.adaptive_healthy_window_seconds = 1;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    control.fail_pacing_on_call(1);
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    let _ownership = insert_runtime_build(&manager, Arc::clone(&trait_control));

    let started = Instant::now() - Duration::from_secs(2);
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    let paused = controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
    let mut state = BuildRunState::new(&manager.settings, Some(controller));
    let mut handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&trait_control),
    };

    let outcome = manager
        .run_build_loop("S", 1, &mut handle, &[], &mut state, Instant::now())
        .await;

    match outcome {
        BuildLoopOutcome::Failed(error) => {
            assert!(error.contains("unable to update inventory pacing while recovering"));
        }
        BuildLoopOutcome::Finished => panic!("readiness failure unexpectedly finished"),
    }
    assert!(control.is_cancelled());
}

#[tokio::test]
async fn build_readiness_reports_controller_recovery_cancellation() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("readiness-cancelled.sqlite3"));
    config.adaptive = true;
    config.adaptive_recovery_delay_seconds = 1;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));

    let mut controller = AdaptiveIndexController::new(manager.controller_config(), Instant::now());
    let paused = controller.observe(
        Instant::now(),
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
    let mut state = BuildRunState::new(&manager.settings, Some(controller));
    let cancel_control = Arc::clone(&control);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel_control.cancel();
    });

    assert!(matches!(
        manager
            .wait_for_build_readiness(&trait_control, "S", &[], &mut state)
            .await,
        BuildReadiness::Cancelled
    ));
}

#[tokio::test]
async fn startup_grace_and_manual_policy_prevent_automatic_first_builds() {
    let directory = tempdir().unwrap();
    let grace_client = Arc::new(MockOpcClient::default());
    let mut grace_config = settings(directory.path().join("grace.sqlite3"));
    grace_config.startup_grace_period_seconds = 60;
    let grace_manager = Arc::new(IndexManager::new(Arc::clone(&grace_client), grace_config));
    grace_manager.start_background_indexing();
    tokio::task::yield_now().await;
    grace_manager.shutdown_background_indexing().await;
    assert_eq!(
        grace_client.inventory_start_count.load(Ordering::Relaxed),
        0
    );

    let manual_client = Arc::new(MockOpcClient::default());
    let manual_manager = Arc::new(IndexManager::new(
        Arc::clone(&manual_client),
        settings(directory.path().join("manual.sqlite3")),
    ));
    manual_manager.refresh_if_due("S").await;
    assert_eq!(
        manual_client.inventory_start_count.load(Ordering::Relaxed),
        0
    );
    manual_manager.start_background_indexing();
    assert_eq!(
        manual_manager.background_refresh_delay("S").await,
        Duration::from_secs(3600)
    );
    manual_manager.shutdown_background_indexing().await;
}

#[tokio::test]
async fn disk_guard_and_sentinel_health_paths_are_reported() {
    let directory = tempdir().unwrap();
    let mut disk_config = settings(directory.path().join("disk.sqlite3"));
    disk_config.minimum_free_space_bytes = u64::MAX;
    let disk_manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        disk_config,
    ));
    let error = disk_manager.refresh("S", true).await.unwrap_err();
    assert!(error.to_string().contains("insufficient free space"));
    assert!(
        disk_manager
            .controller_observation("S", false)
            .insufficient_disk_space
    );

    let client = Arc::new(MockOpcClient::default());
    *client.read_tag_values_result.lock().unwrap() = Ok(vec![TagValue {
        tag_id: "Health.PV".into(),
        value: "1".into(),
        quality: "Bad".into(),
        timestamp: "0".into(),
    }]);
    let mut sentinel_config = settings(directory.path().join("sentinel.sqlite3"));
    sentinel_config.sentinel_tag = Some("Health.PV".into());
    let sentinel_manager = Arc::new(IndexManager::new(client, sentinel_config));
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel_on_pause();
    let trait_control: Arc<dyn InventoryControl> = control;
    insert_runtime_build(&sentinel_manager, Arc::clone(&trait_control));
    let mut next_probe = Instant::now();
    let mut backoff = Duration::from_secs(1);
    assert!(
        !sentinel_manager
            .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff)
            .await
    );
    assert_eq!(
        sentinel_manager.status("S").await.unwrap().health,
        HealthProbeState::Unhealthy
    );
}

#[tokio::test]
async fn sentinel_read_errors_are_reported_as_unhealthy() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    *client.read_tag_values_result.lock().unwrap() = Err("sentinel transport failed".into());
    let mut config = settings(directory.path().join("sentinel-error.sqlite3"));
    config.sentinel_tag = Some("Health.PV".into());
    let manager = Arc::new(IndexManager::new(client, config));
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel_on_pause();
    let trait_control: Arc<dyn InventoryControl> = control.clone();
    insert_runtime_build(&manager, Arc::clone(&trait_control));

    let mut next_probe = Instant::now();
    let mut backoff = Duration::ZERO;
    assert!(
        !manager
            .wait_for_health(&trait_control, "S", &mut next_probe, &mut backoff)
            .await
    );
    assert_eq!(
        manager.status("S").await.unwrap().health,
        HealthProbeState::Unhealthy
    );
}

#[tokio::test]
async fn controller_recovery_pacing_failure_is_returned() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("recovery-failure.sqlite3"));
    config.adaptive = true;
    config.adaptive_recovery_delay_seconds = 0;
    config.adaptive_healthy_window_seconds = 1;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    control.fail_pacing_on_call(1);
    let trait_control: Arc<dyn InventoryControl> = control;
    insert_runtime_build(&manager, Arc::clone(&trait_control));

    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    let paused = controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
    let error = manager
        .wait_for_controller_recovery(&trait_control, "S", &mut controller)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unable to update inventory pacing while recovering")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_cancellation_covers_startup_failure_boundaries() {
    let directory = tempdir().unwrap();

    let pacing_control = Arc::new(RecordingInventoryControl::default());
    pacing_control.fail_pacing_on_call(1);
    let pacing_started = Arc::new(Notify::new());
    let pacing_release = Arc::new(Notify::new());
    let pacing_manager = Arc::new(IndexManager::new(
        Arc::new(
            LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&pacing_control),
                ))],
                vec![],
            )
            .with_inventory_gate(Arc::clone(&pacing_started), Arc::clone(&pacing_release)),
        ),
        settings(directory.path().join("pacing-cancel.sqlite3")),
    ));
    let refresh_manager = Arc::clone(&pacing_manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
    pacing_started.notified().await;
    pacing_manager
        .control("S", IndexControlAction::Cancel)
        .await
        .unwrap();
    pacing_release.notify_one();
    assert_eq!(
        refresh.await.unwrap().unwrap().state,
        IndexState::NotIndexed
    );

    let capability_control = Arc::new(RecordingInventoryControl::default());
    let capability_started = Arc::new(Notify::new());
    let capability_release = Arc::new(Notify::new());
    let capability_manager = Arc::new(IndexManager::new(
        Arc::new(
            LifecycleClient::new(
                vec![Ok(handle_with_control(
                    VecDeque::new(),
                    Arc::clone(&capability_control),
                ))],
                vec![Err("capability failure".into())],
            )
            .with_capability_gate(
                Arc::clone(&capability_started),
                Arc::clone(&capability_release),
            ),
        ),
        settings(directory.path().join("capability-cancel.sqlite3")),
    ));
    let refresh_manager = Arc::clone(&capability_manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
    capability_started.notified().await;
    capability_control.cancel();
    capability_release.notify_one();
    assert_eq!(
        refresh.await.unwrap().unwrap().state,
        IndexState::NotIndexed
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_treats_cancelled_inventory_start_failure_as_noop() {
    let directory = tempdir().unwrap();
    let inventory_started = Arc::new(Notify::new());
    let inventory_release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(vec![Err("inventory failure".into())], vec![]).with_inventory_gate(
            Arc::clone(&inventory_started),
            Arc::clone(&inventory_release),
        ),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("inventory-cancel.sqlite3")),
    ));

    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
    inventory_started.notified().await;
    assert_eq!(
        manager
            .control("S", IndexControlAction::Cancel)
            .await
            .unwrap()
            .state,
        IndexState::Partial
    );
    inventory_release.notify_one();

    assert_eq!(
        refresh.await.unwrap().unwrap().state,
        IndexState::NotIndexed
    );
    assert!(manager.pending_cancels.lock().unwrap().is_empty());
    assert!(build_lock_path(&directory.path().join("inventory-cancel.sqlite3"), "S").exists());
}

#[tokio::test]
async fn attach_control_handles_pending_cancel_after_build_disappears() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("attach-cancel.sqlite3")),
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
    manager.pending_cancels.lock().unwrap().insert("S".into());
    let handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&control),
    };

    assert!(
        manager
            .attach_refresh_control("S", &ownership, &handle, manager.initial_inventory_limits(),)
            .unwrap()
            .is_none()
    );
    assert!(manager.pending_cancels.lock().unwrap().is_empty());
    assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
    assert!(manager.active_builds.lock().unwrap().is_empty());

    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("attach-cancel-control.sqlite3")),
    ));
    let control_impl = Arc::new(RecordingInventoryControl::default());
    control_impl.cancel_on_pacing();
    let control: Arc<dyn InventoryControl> = control_impl;
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
    let handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&control),
    };
    assert!(
        manager
            .attach_refresh_control("S", &ownership, &handle, manager.initial_inventory_limits(),)
            .unwrap()
            .is_none()
    );

    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("attach-error.sqlite3")),
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
    let handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&control),
    };
    let error = manager
        .attach_refresh_control("S", &ownership, &handle, manager.initial_inventory_limits())
        .unwrap_err();
    assert_eq!(error.to_string(), "index build disappeared before start");
    assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
    assert!(manager.active_builds.lock().unwrap().is_empty());
}

#[test]
fn control_action_helpers_reconcile_resume_and_queue_cancel() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("control-actions.sqlite3")),
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&control));
    {
        let mut runtime = manager.runtime.lock().unwrap();
        let build = runtime
            .get_mut("S")
            .and_then(|state| state.build.as_mut())
            .unwrap();
        build.operator_paused = true;
        build.foreground_users = 0;
        build.quiet_until = Some(Instant::now() - Duration::from_secs(1));
    }

    manager
        .apply_control_action("S", IndexControlAction::Resume)
        .unwrap();
    {
        let runtime = manager.runtime.lock().unwrap();
        let build = runtime
            .get("S")
            .and_then(|state| state.build.as_ref())
            .unwrap();
        assert!(!build.operator_paused);
        assert!(build.quiet_until.is_none());
    }

    manager
        .runtime
        .lock()
        .unwrap()
        .get_mut("S")
        .unwrap()
        .build
        .as_mut()
        .unwrap()
        .control = None;
    manager
        .apply_control_action("S", IndexControlAction::Cancel)
        .unwrap();
    assert!(manager.pending_cancels.lock().unwrap().contains("S"));
}

#[tokio::test]
async fn health_and_recovery_cancellation_edges_are_bounded() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("health-wait.sqlite3"));
    config.sentinel_tag = Some("Health.PV".into());
    config.sentinel_probe_interval_seconds = 3_600;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&control));
    manager
        .runtime
        .lock()
        .unwrap()
        .get_mut("S")
        .unwrap()
        .sentinel_checked_at = Some(Instant::now());
    let mut next_probe = Instant::now() + Duration::from_secs(60);
    let mut backoff = Duration::from_secs(1);
    assert!(
        manager
            .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
            .await
    );

    struct CancelOnSecondPoll(AtomicUsize);

    impl InventoryControl for CancelOnSecondPoll {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {}

        fn is_cancelled(&self) -> bool {
            self.0.fetch_add(1, Ordering::AcqRel) > 0
        }
    }

    let mut recovery_config = settings(directory.path().join("recovery-cancel.sqlite3"));
    recovery_config.adaptive = true;
    let recovery = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        recovery_config,
    ));
    let recovery_impl = CancelOnSecondPoll(AtomicUsize::new(0));
    recovery_impl.pause();
    recovery_impl.resume();
    recovery_impl.cancel();
    let recovery_control: Arc<dyn InventoryControl> = Arc::new(recovery_impl);
    insert_runtime_build(&recovery, Arc::clone(&recovery_control));
    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(recovery.controller_config(), started);
    controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    assert!(
        !recovery
            .wait_for_controller_recovery(&recovery_control, "S", &mut controller)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn paused_health_wait_and_runtime_poisoning_fail_closed() {
    struct CancelOnSecondPoll(AtomicUsize);

    impl InventoryControl for CancelOnSecondPoll {
        fn pause(&self) {}

        fn resume(&self) {}

        fn cancel(&self) {}

        fn is_cancelled(&self) -> bool {
            self.0.fetch_add(1, Ordering::AcqRel) > 0
        }
    }

    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("health-overlay.sqlite3")),
    ));
    let control_impl = CancelOnSecondPoll(AtomicUsize::new(0));
    control_impl.pause();
    control_impl.resume();
    control_impl.cancel();
    let control: Arc<dyn InventoryControl> = Arc::new(control_impl);
    insert_runtime_build(&manager, Arc::clone(&control));
    manager.pause_overlays.lock().unwrap().insert(
        "S".into(),
        PauseOverlayState {
            maintenance: false,
            health: true,
        },
    );
    let mut next_probe = Instant::now() + Duration::from_secs(60);
    let mut backoff = Duration::from_secs(1);
    assert!(
        !manager
            .wait_for_health(&control, "S", &mut next_probe, &mut backoff)
            .await
    );

    let runtime = Arc::clone(&manager.runtime);
    let _ = std::panic::catch_unwind(move || {
        let _guard = runtime.lock().unwrap();
        panic!("poison runtime");
    });
    manager.reconcile_pause_state("S");
}

#[tokio::test]
async fn run_build_handles_promoting_lock_and_periodic_commit_failures() {
    struct DelayedCompletionStream {
        phase: u8,
    }

    #[async_trait::async_trait]
    impl InventoryStream for DelayedCompletionStream {
        async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
            match self.phase {
                0 => {
                    self.phase = 1;
                    Some(Ok(InventoryEvent::Entry(inventory_entry(
                        "Periodic",
                        "S.Periodic",
                    ))))
                }
                1 => {
                    self.phase = 2;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    Some(Ok(InventoryEvent::Slice(InventorySliceObservation {
                        sequence: 1,
                        backend: InventorySliceBackend::Da2,
                        nodes_returned: 1,
                        has_more: false,
                        native_operations: 1,
                        elapsed_ms: 1,
                        entries_seen: 1,
                        unique_items: 1,
                    })))
                }
                _ => Some(Ok(InventoryEvent::Completed(InventoryCompleted {
                    complete: true,
                    cancelled: false,
                    truncated: false,
                    warning: None,
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                }))),
            }
        }
    }

    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("periodic-commit.sqlite3"));
    config.commit_interval_ms = 1;
    config.commit_batch_size = 100;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let generation = manager
        .with_database(|db| {
            let generation = db
                .start_generation(
                    "S",
                    NamespaceOrganization::Hierarchical,
                    BrowseSource::Da2,
                    "1",
                )
                .unwrap();
            db.connection
                .execute_batch(
                    "CREATE TRIGGER reject_periodic_insert
                 BEFORE INSERT ON entries
                 BEGIN
                   SELECT RAISE(FAIL, 'periodic insert rejected');
                 END;",
                )
                .unwrap();
            Ok(generation)
        })
        .unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    Arc::clone(&manager)
        .run_build(
            "S".into(),
            generation,
            InventoryHandle {
                stream: Box::new(DelayedCompletionStream { phase: 0 }),
                control,
            },
            ownership,
        )
        .await;
    assert_eq!(manager.status("S").await.unwrap().state, IndexState::Failed);

    let mut successful_config = settings(directory.path().join("periodic-success.sqlite3"));
    successful_config.commit_interval_ms = 1;
    successful_config.commit_batch_size = 100;
    let successful = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        successful_config,
    ));
    let generation = successful
        .with_database(|db| {
            db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
        })
        .unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&successful, Arc::clone(&control));
    Arc::clone(&successful)
        .run_build(
            "S".into(),
            generation,
            InventoryHandle {
                stream: Box::new(DelayedCompletionStream { phase: 0 }),
                control,
            },
            ownership,
        )
        .await;
    assert_eq!(
        successful.status("S").await.unwrap().state,
        IndexState::Ready
    );

    let promotion = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("promotion-lock.sqlite3")),
    ));
    let generation = promotion
        .with_database(|db| {
            db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
        })
        .unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&promotion, Arc::clone(&control));
    let promoting = Arc::clone(&promotion.promoting);
    let _ = std::panic::catch_unwind(move || {
        let _guard = promoting.lock().unwrap();
        panic!("poison promotion lock");
    });
    Arc::clone(&promotion)
        .run_build(
            "S".into(),
            generation,
            InventoryHandle {
                stream: Box::new(VecInventoryStream {
                    events: VecDeque::from([Ok(InventoryEvent::Completed(InventoryCompleted {
                        complete: true,
                        cancelled: false,
                        truncated: false,
                        warning: None,
                        organization: NamespaceOrganization::Hierarchical,
                        source: BrowseSource::Da2,
                    }))]),
                }),
                control,
            },
            ownership,
        )
        .await;
    assert_eq!(
        promotion.status("S").await.unwrap().state,
        IndexState::Failed
    );
}

#[tokio::test]
async fn commit_batching_flushes_thresholds_and_final_pending_entries() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("commit-boundaries.sqlite3"));
    config.commit_batch_size = 3;
    config.commit_interval_ms = 60_000;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config.clone(),
    ));
    let generation = manager
        .with_database(|db| {
            db.start_generation(
                "S",
                NamespaceOrganization::Hierarchical,
                BrowseSource::Da2,
                "1",
            )
        })
        .unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    insert_runtime_build(&manager, Arc::clone(&control));
    let mut state = BuildRunState::new(&config, None);

    for index in 0..7 {
        assert!(matches!(
            manager
                .handle_entry_event(
                    "S",
                    generation,
                    &control,
                    &mut state,
                    inventory_entry(&format!("Entry {index}"), &format!("S.Entry{index}"),),
                )
                .await,
            BuildEventOutcome::Continue
        ));

        match index {
            2 => {
                assert!(state.pending.is_empty());
                assert_eq!(state.persisted_item_count, 3);
            }
            5 => {
                assert!(state.pending.is_empty());
                assert_eq!(state.persisted_item_count, 6);
            }
            6 => {
                assert_eq!(state.pending.len(), 1);
                assert_eq!(state.persisted_item_count, 6);
            }
            _ => {}
        }
    }

    let inserted = manager
        .commit_pending_entries("S", generation, &mut state.pending)
        .unwrap();
    state.persisted_item_count = state.persisted_item_count.saturating_add(inserted);
    assert_eq!(inserted, 1);
    assert!(state.pending.is_empty());
    assert_eq!(state.persisted_item_count, 7);

    let stored_entries = manager
        .with_database(|db| {
            db.connection
                .query_row(
                    "SELECT COUNT(*) FROM entries
                     WHERE server = ?1 AND generation = ?2",
                    rusqlite::params!["S", generation as i64],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(stored_entries, 7);
}

#[tokio::test(flavor = "current_thread")]
async fn unexpected_build_unwind_releases_ownership_and_resumes_cleanup() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("unwind.sqlite3")),
    ));
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::ERROR)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);
    let generation = manager
        .with_database(|db| {
            db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        })
        .unwrap();
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    let result = tokio::spawn(Arc::clone(&manager).run_build(
        "S".into(),
        generation,
        InventoryHandle {
            stream: Box::new(PanickingInventoryStream),
            control,
        },
        ownership,
    ))
    .await;
    assert!(result.is_err());
    manager.background_tasks.wait_for_idle().await;
    assert!(manager.active_builds.lock().unwrap().is_empty());
    assert!(manager.cleanup_tasks.lock().unwrap().is_empty());
    assert_eq!(
        manager.with_database(|db| db.status_rows("S")).unwrap()[0].state,
        "failed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn capability_cancellation_cleans_up_generation_start_boundaries() {
    async fn cancel_after_capability(
        path: PathBuf,
        break_generations: bool,
    ) -> anyhow::Result<IndexStatus> {
        let control = Arc::new(RecordingInventoryControl::default());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let manager = Arc::new(IndexManager::new(
            Arc::new(
                LifecycleClient::new(
                    vec![Ok(handle_with_control(
                        VecDeque::new(),
                        Arc::clone(&control),
                    ))],
                    vec![Ok(default_capabilities())],
                )
                .with_capability_gate(Arc::clone(&started), Arc::clone(&release)),
            ),
            settings(path),
        ));
        if break_generations {
            manager
                .with_database(|db| {
                    drop_table(db, "generations");
                    Ok(())
                })
                .unwrap();
        }
        let refresh_manager = Arc::clone(&manager);
        let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
        started.notified().await;
        control.cancel();
        release.notify_one();
        refresh.await.unwrap().map_err(anyhow::Error::from)
    }

    let directory = tempdir().unwrap();
    assert!(
        cancel_after_capability(directory.path().join("generation.sqlite3"), true)
            .await
            .unwrap_err()
            .to_string()
            .contains("no such table")
    );
    assert_eq!(
        cancel_after_capability(directory.path().join("attached.sqlite3"), false)
            .await
            .unwrap()
            .state,
        IndexState::NotIndexed
    );
}

#[tokio::test]
async fn controller_recovery_returns_false_for_an_already_cancelled_control() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("recovery-cancelled.sqlite3"));
    config.adaptive = true;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(TestInventoryControl::default());
    control.cancel();
    let control: Arc<dyn InventoryControl> = control;
    insert_runtime_build(&manager, Arc::clone(&control));
    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    assert!(
        !manager
            .wait_for_controller_recovery(&control, "S", &mut controller)
            .await
            .unwrap()
    );
}

#[test]
fn completion_event_shape_is_typed() {
    let event = InventoryEvent::Completed(InventoryCompleted {
        complete: true,
        cancelled: false,
        truncated: false,
        warning: None,
        organization: NamespaceOrganization::Flat,
        source: BrowseSource::Flat,
    });
    assert!(matches!(event, InventoryEvent::Completed(_)));
}

#[derive(Default)]
struct TestInventoryControl {
    cancelled: AtomicBool,
}

impl InventoryControl for TestInventoryControl {
    fn pause(&self) {}

    fn resume(&self) {}

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct RegisterCancellingControl {
    parent: Arc<CoordinatedInventoryControl>,
    cancelled: AtomicBool,
}

struct RegisterStoppingControl {
    parent: Arc<CoordinatedInventoryControl>,
}

impl InventoryControl for RegisterStoppingControl {
    fn pause(&self) {}

    fn resume(&self) {}

    fn cancel(&self) {}

    fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
        self.parent.stop_workers();
        anyhow::bail!("test registration stop");
    }
}

impl InventoryControl for RegisterCancellingControl {
    fn pause(&self) {}

    fn resume(&self) {}

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn set_pacing(&self, _pacing: InventoryPacing) -> anyhow::Result<()> {
        self.parent.cancel();
        Ok(())
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct PanickingInventoryStream;

struct PanicAfterReleaseInventoryStream {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct CompletedThenShutdownPanicInventoryStream {
    emitted: bool,
}

struct ControlledInventoryStream {
    started: Arc<Notify>,
    started_count: Arc<AtomicUsize>,
    release: Arc<Notify>,
    event: Option<anyhow::Result<InventoryEvent>>,
    shutdowns: Arc<AtomicUsize>,
}

#[derive(Default)]
struct CancellationInventoryControl {
    cancelled: AtomicBool,
    cancellation: Notify,
}

struct CancellationAwareInventoryStream {
    started: Arc<Notify>,
    control: Arc<CancellationInventoryControl>,
    shutdowns: Arc<AtomicUsize>,
    emitted: bool,
}

#[async_trait::async_trait]
impl InventoryStream for PanickingInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        panic!("injected inventory stream panic");
    }
}

#[async_trait::async_trait]
impl InventoryStream for PanicAfterReleaseInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        self.started.notify_one();
        self.release.notified().await;
        panic!("injected coordinated inventory stream panic");
    }
}

#[async_trait::async_trait]
impl InventoryStream for CompletedThenShutdownPanicInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        if self.emitted {
            None
        } else {
            self.emitted = true;
            Some(Ok(completed_inventory()))
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        panic!("injected coordinated inventory shutdown panic");
    }
}

#[async_trait::async_trait]
impl InventoryStream for ControlledInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        let event = self.event.take()?;
        self.started_count.fetch_add(1, Ordering::AcqRel);
        self.started.notify_one();
        self.release.notified().await;
        Some(event)
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.shutdowns.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

impl InventoryControl for CancellationInventoryControl {
    fn pause(&self) {}

    fn resume(&self) {}

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancellation.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl InventoryStream for CancellationAwareInventoryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        if self.emitted {
            return None;
        }
        self.emitted = true;
        self.started.notify_one();
        let cancellation = self.control.cancellation.notified();
        if !self.control.is_cancelled() {
            cancellation.await;
        }
        Some(Ok(InventoryEvent::Completed(InventoryCompleted {
            complete: false,
            cancelled: true,
            truncated: false,
            warning: Some("test cancellation".into()),
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da2,
        })))
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.shutdowns.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct CancellingEntryStream {
    control: Arc<RecordingInventoryControl>,
    yielded: bool,
}

#[async_trait::async_trait]
impl InventoryStream for CancellingEntryStream {
    async fn next(&mut self) -> Option<anyhow::Result<InventoryEvent>> {
        if self.yielded {
            return None;
        }
        self.yielded = true;
        self.control.cancel();
        Some(Ok(InventoryEvent::Entry(inventory_entry(
            "Cancelled",
            "S.Cancelled",
        ))))
    }
}

#[test]
fn split_store_propagates_database_errors() {
    let mut failed_attempt = IndexDb::open(Path::new(":memory:")).unwrap();
    failed_attempt
        .connection
        .execute_batch(
            "CREATE TRIGGER reject_failed_attempt
             BEFORE INSERT ON generations
             BEGIN
               SELECT RAISE(FAIL, 'failed attempt rejected');
             END;",
        )
        .unwrap();
    assert!(
        failed_attempt
            .record_failed_attempt("S", "failed")
            .unwrap_err()
            .to_string()
            .contains("failed attempt rejected")
    );

    let mut obsolete = IndexDb::open(Path::new(":memory:")).unwrap();
    drop_table(&mut obsolete, "generations");
    assert!(obsolete.obsolete_servers().is_err());

    let mut enrollment = IndexDb::open(Path::new(":memory:")).unwrap();
    drop_table(&mut enrollment, "enrolled_servers");
    assert!(enrollment.enroll("S", "1").is_err());
    assert!(enrollment.set_auto_refresh("S", false).is_err());

    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE generations (server TEXT NOT NULL);
             INSERT INTO generations(server) VALUES ('S');
             CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );",
        )
        .unwrap();
    assert!(migrate_schema_3_to_4(&mut connection).is_err());
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'enrolled_servers'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn split_traversal_handles_paused_state_and_missing_runtime() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
    let build = RuntimeBuild {
        control: None,
        progress: None,
        started_at: "1".into(),
        foreground_users: 0,
        operator_paused: false,
        quiet_until: None,
        effective_limits: None,
        controller_state: Some(crate::controller::ControllerState::Paused(
            crate::controller::PauseReason::OpcHealth,
        )),
        pause_reason: None,
        recovery_deadline: None,
        last_commit_latency_ms: None,
    };
    assert!(!IndexManager::<MockOpcClient>::build_can_resume(
        &build,
        PauseOverlayState::default()
    ));
    manager.update_runtime_after_build(&mut HashMap::new(), "Missing", None);
    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    let unchanged = controller.observe(started, ControllerObservation::default());
    assert!(!unchanged.transitioned);
    IndexManager::<MockOpcClient>::log_controller_transition("S", &unchanged);
}

#[tokio::test(flavor = "current_thread")]
async fn controller_recovery_updates_runtime_after_a_pause() {
    let directory = tempdir().unwrap();
    let mut config = settings(directory.path().join("recovery-transition.sqlite3"));
    config.adaptive = true;
    config.adaptive_recovery_delay_seconds = 1;
    config.adaptive_max_recovery_delay_seconds = 1;
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        config,
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    let trait_control: Arc<dyn InventoryControl> = control;
    insert_runtime_build(&manager, Arc::clone(&trait_control));
    let started = Instant::now();
    let mut controller = AdaptiveIndexController::new(manager.controller_config(), started);
    let paused = controller.observe(
        started,
        ControllerObservation {
            foreground_bad_quality: true,
            ..ControllerObservation::default()
        },
    );
    manager.update_runtime_controller("S", paused.limits, paused.state, paused.recovery_at);
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    assert!(
        manager
            .wait_for_controller_recovery(&trait_control, "S", &mut controller)
            .await
            .unwrap()
    );
    assert_eq!(
        controller.state(),
        crate::controller::ControllerState::Ramping
    );
}

#[test]
fn reconcile_pause_state_handles_a_build_without_a_control() {
    let manager = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(PathBuf::from(":memory:")),
    );
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
            ..RuntimeState::default()
        },
    );
    manager.reconcile_pause_state("S");
}

#[tokio::test(flavor = "current_thread")]
async fn build_finalization_log_keeps_the_index_module_target_and_fields() {
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
        let manager = Arc::new(IndexManager::new(
            Arc::new(MockOpcClient::default()),
            settings(PathBuf::from(":memory:")),
        ));
        let control: Arc<dyn InventoryControl> = Arc::new(TestInventoryControl::default());
        drop(BuildFinalizationGuard::new(
            manager,
            "characterization".to_string(),
            7,
            control,
            Arc::new(()),
        ));
    });
    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("ERROR opcda_bridge_gateway::index:"),
        "{output}"
    );
    assert!(
        output.contains("namespace index build unwound unexpectedly; ownership was released"),
        "{output}"
    );
    for field in [
        "process_id=",
        "database=:memory:",
        "server=characterization",
        "generation=7",
    ] {
        assert!(output.contains(field), "{output}");
    }
}
