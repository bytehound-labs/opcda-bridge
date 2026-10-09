use super::*;

#[test]
fn legacy_opted_out_indexes_migrate_to_always_participating_without_losing_data() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("legacy-opted-out.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    db.enroll("S", "1").unwrap();
    let generation = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    db.insert_entries(
        "S",
        generation,
        &[inventory_entry("Persisted", "Persisted.Tag")],
    )
    .unwrap();
    db.promote("S", generation, "2", &completed_progress(1))
        .unwrap();
    db.connection
        .execute_batch(
            "ALTER TABLE enrolled_servers RENAME TO legacy_fixture_enrollment;
             CREATE TABLE enrolled_servers (
                 server TEXT PRIMARY KEY NOT NULL,
                 auto_refresh_enabled INTEGER NOT NULL DEFAULT 1
                   CHECK (auto_refresh_enabled IN (0, 1)),
                 enrolled_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL
             );
             INSERT INTO enrolled_servers
               SELECT server, 0, enrolled_at, enrolled_at FROM legacy_fixture_enrollment;
             DROP TABLE legacy_fixture_enrollment;
             UPDATE index_meta SET value = '4' WHERE key = 'schema_version';",
        )
        .unwrap();
    drop(db);

    let db = IndexDb::open(&path).unwrap();
    assert_eq!(db.scheduled_servers().unwrap(), vec!["S"]);
    let obsolete_columns: u32 = db
        .connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('enrolled_servers')
             WHERE name IN ('auto_refresh_enabled', 'updated_at')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(obsolete_columns, 0);
    assert_eq!(db.status_rows("S").unwrap()[0].generation, generation);
    assert_eq!(db.status_rows("S").unwrap()[0].entry_count, 1);
}

#[tokio::test]
async fn generation_start_failure_is_recorded_and_reported() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Ok(default_capabilities())],
    ));
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("generation-lock-poisoned.sqlite3")),
    ));
    manager.with_database(|db| db.enroll("S", "0")).unwrap();
    let ownership = manager
        .reserve_refresh_build("S", true)
        .unwrap()
        .expect("build reservation should succeed");
    let control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    manager
        .with_database(|database| {
            database
                .connection
                .execute_batch(
                    "CREATE TRIGGER reject_staging_generation
                 BEFORE INSERT ON generations
                 WHEN NEW.state = 'staging'
                 BEGIN
                     SELECT RAISE(ABORT, 'staging generation rejected');
                 END;",
                )
                .unwrap();
            Ok(())
        })
        .unwrap();

    let error = manager
        .start_refresh_generation("S", &control, &ownership, false)
        .await
        .expect_err("the staging generation trigger should fail generation start");
    assert!(error.to_string().contains("staging generation rejected"));
}

#[test]
fn sqlite_quarantine_preserves_database_and_sidecars_as_one_bundle() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("invalid.sqlite3");
    let quarantine = directory.path().join("invalid.quarantine");
    let database = b"database contents";
    let wal = b"wal contents";
    let shm = b"shm contents";

    fs::write(&path, database).unwrap();
    fs::write(IndexDb::sqlite_sidecar_path(&path, "-wal"), wal).unwrap();
    fs::write(IndexDb::sqlite_sidecar_path(&path, "-shm"), shm).unwrap();

    assert!(quarantine_index_files(&path, &quarantine).unwrap());
    assert!(!path.exists());
    assert!(!IndexDb::sqlite_sidecar_path(&path, "-wal").exists());
    assert!(!IndexDb::sqlite_sidecar_path(&path, "-shm").exists());
    assert_eq!(fs::read(&quarantine).unwrap(), database);
    assert_eq!(
        fs::read(IndexDb::sqlite_sidecar_path(&quarantine, "-wal")).unwrap(),
        wal
    );
    assert_eq!(
        fs::read(IndexDb::sqlite_sidecar_path(&quarantine, "-shm")).unwrap(),
        shm
    );
}

#[test]
fn sqlite_sidecars_append_to_custom_database_names() {
    let path = PathBuf::from("/tmp/custom-index.db");
    assert_eq!(
        IndexDb::sqlite_sidecar_path(&path, "-wal"),
        PathBuf::from("/tmp/custom-index.db-wal")
    );
    assert_eq!(
        IndexDb::sqlite_sidecar_path(&path, "-shm"),
        PathBuf::from("/tmp/custom-index.db-shm")
    );
}

#[test]
fn failed_attempt_and_enrollment_state_persist_through_index_db() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("enrollment.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();

    db.record_failed_attempt("S", "inventory failed").unwrap();
    let failed = db.status_rows("S").unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].state, "failed");
    assert_eq!(failed[0].last_error.as_deref(), Some("inventory failed"));

    assert!(!db.is_enrolled("S").unwrap());
    db.enroll("S", "1").unwrap();
    assert!(db.is_enrolled("S").unwrap());
    assert!(!db.is_enrolled("missing").unwrap());
    db.enroll("S", "2").unwrap();
    assert!(db.is_enrolled("S").unwrap());
    assert_eq!(
        db.connection
            .query_row(
                "SELECT enrolled_at FROM enrolled_servers WHERE server = 'S'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "1"
    );
    assert!(db.scheduled_servers().unwrap().is_empty());
}

#[test]
fn only_corrupt_or_incompatible_index_errors_are_quarantinable() {
    assert!(is_quarantinable_index_error(&anyhow::anyhow!(
        "unsupported namespace index schema version 99"
    )));
    assert!(is_quarantinable_index_error(&anyhow::anyhow!(
        "invalid namespace index schema version \"corrupt\""
    )));
    assert!(is_quarantinable_index_error(&anyhow::anyhow!(
        "SQLite error: file is not a database"
    )));
    assert!(!is_quarantinable_index_error(&anyhow::anyhow!(
        "FOREIGN KEY constraint failed"
    )));
    assert!(!is_quarantinable_index_error(&anyhow::anyhow!(
        "database is locked"
    )));
    assert_eq!(
        parse_namespace("hierarchical"),
        NamespaceOrganization::Hierarchical
    );
    assert_eq!(
        parse_namespace("unknown"),
        NamespaceOrganization::Unspecified
    );
    assert_eq!(source_string(BrowseSource::Unspecified), "unspecified");
    assert_eq!(source_string(BrowseSource::Da3), "da3");
    assert_eq!(source_string(BrowseSource::Derived), "derived");
    assert_eq!(parse_source("unknown"), BrowseSource::Unspecified);
    assert_eq!(node_kind_number(InventoryNodeKind::Item), 1);
    assert_eq!(node_kind_number(InventoryNodeKind::BranchAndItem), 2);
}

#[test]
fn profile_compatibility_preserves_negotiated_da2_fallbacks() {
    assert!(index_profile_is_compatible(
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        true,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da3,
    ));
    assert!(!index_profile_is_compatible(
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        false,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da3,
    ));
    assert!(!index_profile_is_compatible(
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da3,
        false,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
    ));
    assert!(!index_profile_is_compatible(
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        true,
        NamespaceOrganization::Flat,
        BrowseSource::Da2,
    ));
}

#[test]
fn sqlite_open_quarantines_invalid_schema_and_recovers_interrupted_builds() {
    let directory = tempdir().unwrap();
    let memory = IndexDb::open(Path::new(":memory:")).unwrap();
    assert_eq!(memory.storage_diagnostics().main_bytes, 0);
    drop(memory);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let readonly = directory.path().join("readonly");
        fs::create_dir(&readonly).unwrap();
        fs::set_permissions(&readonly, fs::Permissions::from_mode(0o500)).unwrap();
        let result = IndexDb::open(&readonly.join("index.sqlite3"));
        fs::set_permissions(&readonly, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err());
    }

    let invalid_path = directory.path().join("invalid/index.sqlite3");
    fs::create_dir_all(invalid_path.parent().unwrap()).unwrap();
    let invalid = Connection::open(&invalid_path).unwrap();
    invalid
        .execute_batch(
            "CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
             INSERT INTO index_meta(key, value)
             VALUES ('schema_version', '999');",
        )
        .unwrap();
    drop(invalid);

    let invalid_version_path = directory.path().join("invalid-version.sqlite3");
    let invalid_version = Connection::open(&invalid_version_path).unwrap();
    invalid_version
        .execute_batch(
            "CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
             INSERT INTO index_meta(key, value)
             VALUES ('schema_version', 'corrupt');",
        )
        .unwrap();
    drop(invalid_version);
    let error = IndexDb::open_once(&invalid_version_path)
        .err()
        .expect("invalid schema version should fail");
    assert!(
        error
            .to_string()
            .contains("invalid namespace index schema version")
    );

    let schema2_path = directory.path().join("schema2.sqlite3");
    let schema2 = Connection::open(&schema2_path).unwrap();
    schema2
        .execute_batch(
            "PRAGMA foreign_keys = OFF;
             CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
             CREATE TABLE generations (
                 server TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 state TEXT NOT NULL,
                 organization TEXT NOT NULL,
                 source TEXT NOT NULL,
                 started_at TEXT NOT NULL,
                 completed_at TEXT,
                 entry_count INTEGER NOT NULL DEFAULT 0,
                 unique_item_count INTEGER NOT NULL DEFAULT 0,
                 last_error TEXT,
                 PRIMARY KEY (server, generation)
             );
             CREATE TABLE entries (
                 server TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 item_id TEXT NOT NULL,
                 item_id_norm TEXT NOT NULL,
                 display_name TEXT NOT NULL,
                 display_name_norm TEXT NOT NULL,
                 kind INTEGER NOT NULL,
                 breadcrumbs TEXT NOT NULL,
                 PRIMARY KEY (server, generation, item_id),
                 FOREIGN KEY (server, generation)
                   REFERENCES generations(server, generation)
                   ON DELETE CASCADE
             );
             CREATE INDEX entries_display_prefix
               ON entries(server, generation, display_name_norm);
             CREATE INDEX entries_item_prefix
               ON entries(server, generation, item_id_norm);
             CREATE VIRTUAL TABLE entries_fts USING fts5(
                 server UNINDEXED,
                 generation UNINDEXED,
                 item_id,
                 display_name,
                 breadcrumbs,
                 tokenize = 'trigram'
             );
             INSERT INTO index_meta(key, value)
             VALUES ('schema_version', '2');
             INSERT INTO generations (
                 server, generation, state, organization, source, started_at,
                 completed_at, entry_count, unique_item_count, last_error
             ) VALUES
                 ('S', 1, 'active', 'hierarchical', 'da2', '1', '2', 1, 1, NULL),
                 ('Failed', 1, 'failed', 'flat', 'da2', '3', NULL, 0, 0, 'failed');
             INSERT INTO entries (
                 server, generation, item_id, item_id_norm, display_name,
                 display_name_norm, kind, breadcrumbs
             ) VALUES (
                 'S', 1, 'S.Active', 's.active', 'Active', 'active', 1, '[\"Active\"]'
             );
             INSERT INTO entries_fts(server, generation, item_id, display_name, breadcrumbs)
             VALUES ('S', 1, 'S.Active', 'Active', 'Active');",
        )
        .unwrap();
    drop(schema2);

    let rollback_path = directory.path().join("schema2-rollback.sqlite3");
    fs::copy(&schema2_path, &rollback_path).unwrap();
    let rollback = Connection::open(&rollback_path).unwrap();
    rollback
        .execute_batch(
            "CREATE TRIGGER reject_schema_version_update
             BEFORE INSERT ON index_meta
             BEGIN
               SELECT RAISE(FAIL, 'schema migration metadata update rejected');
             END;",
        )
        .unwrap();
    drop(rollback);
    let migration_error = IndexDb::open_once(&rollback_path)
        .err()
        .expect("schema migration failure should be surfaced");
    assert!(
        migration_error
            .to_string()
            .contains("schema migration metadata update rejected")
    );
    let rolled_back = Connection::open(&rollback_path).unwrap();
    assert_eq!(
        rolled_back
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "2"
    );
    let generation_columns = rolled_back
        .prepare("PRAGMA table_info(generations)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(
        !generation_columns
            .iter()
            .any(|column| column == "compatibility_fallback")
    );
    drop(rolled_back);

    let migrated = IndexDb::open_once(&schema2_path).unwrap();
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT compatibility_fallback FROM generations WHERE server = 'S' AND generation = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT COUNT(*) FROM enrolled_servers WHERE server = 'S'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert!(
        !migrated
            .active_profile("S")
            .unwrap()
            .unwrap()
            .compatibility_fallback
    );
    assert_eq!(migrated.status_rows("S").unwrap().len(), 1);
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert!(migrated.is_enrolled("S").unwrap());
    assert!(migrated.is_enrolled("Failed").unwrap());
    assert_eq!(migrated.scheduled_servers().unwrap(), vec!["S"]);
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries_fts WHERE server = 'S' AND generation = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(migrated);

    let reopened = IndexDb::open_once(&schema2_path).unwrap();
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        SCHEMA_VERSION.to_string()
    );
    assert_eq!(
        reopened
            .connection
            .query_row("SELECT COUNT(*) FROM enrolled_servers", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
    drop(reopened);

    let rejected_metadata_path = directory.path().join("rejected-metadata.sqlite3");
    drop(IndexDb::open(&rejected_metadata_path).unwrap());
    let rejected_metadata = Connection::open(&rejected_metadata_path).unwrap();
    rejected_metadata
        .execute_batch(
            "CREATE TRIGGER reject_index_meta_insert
             BEFORE INSERT ON index_meta
             BEGIN
               SELECT RAISE(FAIL, 'index metadata update rejected');
             END;",
        )
        .unwrap();
    drop(rejected_metadata);
    assert!(
        IndexDb::open_once(&rejected_metadata_path)
            .err()
            .expect("rejected metadata write should fail")
            .to_string()
            .contains("index metadata update rejected")
    );

    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .finish();
    let recovered =
        tracing::subscriber::with_default(subscriber, || IndexDb::open(&invalid_path).unwrap());
    assert!(recovered.status_rows("S").unwrap().is_empty());
    assert!(
        directory
            .path()
            .join("invalid")
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains("quarantine-"))
    );
    drop(recovered);

    let interrupted_path = directory.path().join("interrupted.sqlite3");
    let mut interrupted = IndexDb::open(&interrupted_path).unwrap();
    let active = interrupted
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    interrupted
        .insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
        .unwrap();
    interrupted
        .promote(
            "S",
            active,
            "2",
            &InventoryProgress {
                entries_seen: 1,
                unique_items: 1,
                ..zero_progress()
            },
        )
        .unwrap();
    let generation = interrupted
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    interrupted
        .insert_entries("S", generation, &[inventory_entry("Interrupted", "S.Tag")])
        .unwrap();
    drop(interrupted);

    let reopened = IndexDb::open(&interrupted_path).unwrap();
    let rows = reopened.status_rows("S").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "active");
    assert_eq!(rows[0].generation, active);
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT state FROM generations
                 WHERE server = 'S' AND generation = ?1",
                [generation as i64],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "superseded"
    );
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT last_error FROM generations
                 WHERE server = 'S' AND generation = ?1",
                [generation as i64],
                |row| row.get::<_, Option<String>>(0)
            )
            .unwrap()
            .as_deref(),
        Some("namespace index build interrupted by gateway restart")
    );
    assert_eq!(reopened.search_generation("S").unwrap(), Some(active));
    assert_eq!(
        reopened.search("S", active, "active", 1, 10).unwrap().len(),
        1
    );
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                [generation as i64],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries_fts WHERE server = 'S' AND generation = ?1",
                [generation as i64],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );

    let initial_path = directory.path().join("interrupted-initial.sqlite3");
    let mut initial = IndexDb::open(&initial_path).unwrap();
    let initial_generation = initial
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    initial
        .insert_entries(
            "S",
            initial_generation,
            &[inventory_entry("Interrupted", "S.Tag")],
        )
        .unwrap();
    drop(initial);

    let reopened_initial = IndexDb::open(&initial_path).unwrap();
    let initial_rows = reopened_initial.status_rows("S").unwrap();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].state, "failed");
    assert_eq!(
        initial_rows[0].last_error.as_deref(),
        Some("namespace index build interrupted by gateway restart")
    );
}

#[test]
fn sqlite_open_preserves_staging_owned_by_a_live_build_lock() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("live-build.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let active = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
        .unwrap();
    db.promote("S", active, "2", &completed_progress(1))
        .unwrap();
    let staging = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "3",
        )
        .unwrap();
    db.insert_entries("S", staging, &[inventory_entry("Staging", "S.Staging")])
        .unwrap();
    drop(db);

    let lock = BuildFileLock::acquire(&path, "S").unwrap();
    let reopened = IndexDb::open(&path).unwrap();
    assert_eq!(
        reopened
            .connection
            .query_row(
                "SELECT state FROM generations
                 WHERE server = 'S' AND generation = ?1",
                [staging as i64],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "staging"
    );
    drop(reopened);
    drop(lock);

    let recovered = IndexDb::open(&path).unwrap();
    assert_eq!(
        recovered
            .connection
            .query_row(
                "SELECT state FROM generations
                 WHERE server = 'S' AND generation = ?1",
                [staging as i64],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "superseded"
    );
}

#[test]
fn schema_migration_rejects_an_invalid_server_value() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("invalid-server.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
             CREATE TABLE generations (
                 server BLOB NOT NULL,
                 state TEXT NOT NULL
             );
             INSERT INTO index_meta(key, value) VALUES ('schema_version', '3');
             INSERT INTO generations(server, state) VALUES (X'00', 'active');",
        )
        .unwrap();
    drop(connection);

    assert!(IndexDb::open_once(&path).is_err());
}

#[test]
fn sqlite_migrates_schema_3_and_preserves_indexed_data() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("v3.sqlite3");
    drop(IndexDb::open(&path).unwrap());
    let legacy = Connection::open(&path).unwrap();
    legacy
        .execute_batch(
            "DROP TABLE enrolled_servers;
             INSERT INTO generations (
                 server, generation, state, organization, source, started_at,
                 completed_at, entry_count, unique_item_count
             ) VALUES
                 ('Active', 1, 'active', 'hierarchical', 'da2', '1', '2', 1, 1),
                 ('Failed', 1, 'failed', 'flat', 'da2', '3', NULL, 0, 0);
             INSERT INTO entries (
                 server, generation, item_id, item_id_norm, display_name,
                 display_name_norm, kind, breadcrumbs
             ) VALUES (
                 'Active', 1, 'Area.Loop.PV', 'area.loop.pv', 'PV',
                 'pv', 1, '[\"Area\",\"Loop\",\"PV\"]'
             );
             INSERT INTO entries_fts (
                 server, generation, item_id, display_name, breadcrumbs
             ) VALUES (
                 'Active', 1, 'Area.Loop.PV', 'PV', 'Area Loop PV'
             );
             UPDATE index_meta SET value = '3' WHERE key = 'schema_version';",
        )
        .unwrap();
    drop(legacy);

    let migrated = IndexDb::open(&path).unwrap();
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "5"
    );
    assert!(migrated.is_enrolled("Active").unwrap());
    assert!(migrated.is_enrolled("Failed").unwrap());
    assert_eq!(migrated.scheduled_servers().unwrap(), vec!["Active"]);
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries
                 WHERE server = 'Active' AND generation = 1",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT COUNT(*) FROM entries_fts
                 WHERE server = 'Active' AND generation = 1",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
}

#[test]
fn sqlite_quarantines_inconsistent_full_text_data() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("fts-inconsistent.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
        .unwrap();
    db.promote("S", generation, "2", &completed_progress(1))
        .unwrap();
    db.connection
        .execute("DELETE FROM entries_fts", [])
        .unwrap();
    drop(db);

    let recovered = IndexDb::open(&path).unwrap();
    assert!(recovered.status_rows("S").unwrap().is_empty());
    assert!(
        directory
            .path()
            .read_dir()
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains("quarantine-"))
    );
}

#[test]
fn sqlite_validation_failure_discard_and_clear_paths_work() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    assert!(
        db.insert_entries(
            "S",
            generation,
            &[InventoryEntry {
                display_name: "Invalid".into(),
                item_id: String::new(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec![],
            }],
        )
        .is_err()
    );
    db.insert_entries("S", generation, &[inventory_entry("Valid", "S.Valid")])
        .unwrap();
    db.update_progress(
        "S",
        generation,
        &InventoryProgress {
            branches_visited: 1,
            entries_seen: 1,
            unique_items: 1,
            active_time_ms: 1,
            paused_time_ms: 0,
            items_per_second: 1.0,
            estimated_remaining_ms: Some(10),
        },
    )
    .unwrap();
    assert_eq!(db.status_rows("S").unwrap()[0].entry_count, 1);
    assert!(
        db.update_progress(
            "S",
            generation,
            &InventoryProgress {
                unique_items: u64::MAX,
                ..zero_progress()
            },
        )
        .unwrap_err()
        .to_string()
        .contains("unique item count exceeds SQLite range")
    );

    db.connection
        .execute("UPDATE entries SET kind = 99 WHERE server = 'S'", [])
        .unwrap();
    assert!(db.search("S", generation, "valid", 1, 10).is_err());
    assert!(db.search("S", generation, "valid", 3, 10).is_err());
    db.connection
        .execute(
            "UPDATE entries SET kind = 1, breadcrumbs = 'not-json'
             WHERE server = 'S'",
            [],
        )
        .unwrap();
    assert!(db.search("S", generation, "valid", 1, 10).is_err());
    assert!(db.search("S", generation, "valid", 3, 10).is_err());
    assert!(
        db.promote("S", generation + 1, "2", &zero_progress())
            .is_err()
    );

    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, || db.fail_generation("S", generation, "failed"))
        .unwrap();
    let failed = db.status_rows("S").unwrap();
    assert_eq!(failed[0].state, "failed");
    assert_eq!(failed[0].last_error.as_deref(), Some("failed"));

    let replacement = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da3,
            "3",
        )
        .unwrap();
    assert_eq!(db.status_rows("S").unwrap().len(), 2);
    assert!(db.discard_empty_generation("S", replacement).unwrap());
    assert_eq!(db.status_rows("S").unwrap().len(), 1);

    let other = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "4",
        )
        .unwrap();
    db.insert_entries("S", other, &[inventory_entry("Other", "S.Other")])
        .unwrap();
    db.clear_server("S").unwrap();
    assert!(db.status_rows("S").unwrap().is_empty());
    assert_eq!(db.search_generation("S").unwrap(), None);
}

#[test]
fn sqlite_corruption_errors_propagate_from_each_persistence_operation() {
    let directory = tempdir().unwrap();

    let collision_path = directory.path().join("collision.sqlite3");
    let collision = Connection::open(&collision_path).unwrap();
    collision
        .execute_batch(
            "CREATE TABLE seed(value INTEGER);
             CREATE INDEX index_meta ON seed(value);",
        )
        .unwrap();
    drop(collision);
    assert!(IndexDb::open_once(&collision_path).is_err());

    let malformed_path = directory.path().join("malformed.sqlite3");
    let malformed = Connection::open(&malformed_path).unwrap();
    malformed
        .execute_batch(
            "CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 wrong_column TEXT NOT NULL
             );",
        )
        .unwrap();
    drop(malformed);
    assert!(IndexDb::open_once(&malformed_path).is_err());

    let schema_path = directory.path().join("schema.sqlite3");
    let schema = Connection::open(&schema_path).unwrap();
    schema
        .execute_batch(
            "CREATE TABLE index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );
            CREATE TABLE generations (server TEXT NOT NULL);
            CREATE TABLE entries (server TEXT NOT NULL);",
        )
        .unwrap();
    drop(schema);
    assert!(IndexDb::open_once(&schema_path).is_err());

    let cleanup_path = directory.path().join("cleanup.sqlite3");
    let mut cleanup = IndexDb::open(&cleanup_path).unwrap();
    let active = cleanup
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    cleanup
        .promote("S", active, "0", &completed_progress(0))
        .unwrap();
    let cleanup_generation = cleanup
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    cleanup
        .insert_entries(
            "S",
            cleanup_generation,
            &[inventory_entry("Cleanup", "S.Cleanup")],
        )
        .unwrap();
    cleanup
        .connection
        .execute_batch(
            "CREATE TRIGGER fail_cleanup
             BEFORE DELETE ON entries
             BEGIN
               SELECT RAISE(FAIL, 'cleanup failed');
             END;",
        )
        .unwrap();
    cleanup
        .fail_generation("S", cleanup_generation, "failed")
        .unwrap();
    drop(cleanup);
    assert!(cleanup_obsolete_generations(&cleanup_path, "S", &BackgroundTasks::new(),).is_err());

    let rebuild_path = directory.path().join("rebuild.sqlite3");
    let mut rebuild = IndexDb::open(&rebuild_path).unwrap();
    let rebuild_generation = rebuild
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    rebuild
        .insert_entries(
            "S",
            rebuild_generation,
            &[inventory_entry("Rebuild", "S.Rebuild")],
        )
        .unwrap();
    rebuild
        .promote("S", rebuild_generation, "2", &zero_progress())
        .unwrap();
    drop_table(&mut rebuild, "entries_fts");
    rebuild
        .connection
        .execute_batch(
            "CREATE TABLE entries_fts (
                 server TEXT,
                 generation INTEGER CHECK (generation < 0),
                 item_id TEXT,
                 display_name TEXT,
                 breadcrumbs TEXT
             );",
        )
        .unwrap();
    drop(rebuild);
    assert!(IndexDb::open_once(&rebuild_path).is_err());

    let mut start_db = IndexDb::open(&directory.path().join("start.sqlite3")).unwrap();
    drop_table(&mut start_db, "generations");
    assert!(
        start_db
            .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1",)
            .is_err()
    );

    let mut insert_db = IndexDb::open(&directory.path().join("insert.sqlite3")).unwrap();
    let generation = insert_db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    drop_table(&mut insert_db, "entries");
    assert!(
        insert_db
            .insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
            .is_err()
    );

    let mut fts_db = IndexDb::open(&directory.path().join("fts.sqlite3")).unwrap();
    let generation = fts_db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    drop_table(&mut fts_db, "entries_fts");
    assert!(
        fts_db
            .insert_entries("S", generation, &[inventory_entry("Tag", "S.Tag")])
            .is_err()
    );

    let mut progress_db = IndexDb::open(&directory.path().join("progress.sqlite3")).unwrap();
    drop_table(&mut progress_db, "generations");
    assert!(
        progress_db
            .update_progress("S", 1, &zero_progress())
            .is_err()
    );

    let mut promote_db = IndexDb::open(&directory.path().join("promote.sqlite3")).unwrap();
    let generation = promote_db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    drop_table(&mut promote_db, "entries");
    assert!(
        promote_db
            .promote("S", generation, "2", &zero_progress())
            .is_ok()
    );

    let mut fail_db = IndexDb::open(&directory.path().join("fail.sqlite3")).unwrap();
    drop_table(&mut fail_db, "generations");
    assert!(fail_db.fail_generation("S", 1, "failed").is_err());

    let mut discard_db = IndexDb::open(&directory.path().join("discard.sqlite3")).unwrap();
    drop_table(&mut discard_db, "entries_fts");
    assert!(discard_db.discard_empty_generation("S", 1).is_err());

    let mut clear_db = IndexDb::open(&directory.path().join("clear.sqlite3")).unwrap();
    drop_table(&mut clear_db, "entries_fts");
    assert!(clear_db.clear_server("S").is_err());

    let mut status_db = IndexDb::open(&directory.path().join("status.sqlite3")).unwrap();
    drop_table(&mut status_db, "generations");
    assert!(status_db.status_rows("S").is_err());
    assert!(status_db.search_generation("S").is_err());

    let mut search_db = IndexDb::open(&directory.path().join("search.sqlite3")).unwrap();
    drop_table(&mut search_db, "entries");
    assert!(search_db.search("S", 1, "tag", 1, 10).is_err());
}

#[tokio::test]
async fn completed_inventory_profile_replaces_startup_capabilities() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([
                Ok(InventoryEvent::Entry(inventory_entry("Tag", "S.Tag"))),
                Ok(InventoryEvent::Progress(InventoryProgress {
                    branches_visited: 2,
                    entries_seen: 3,
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
                    organization: NamespaceOrganization::Hierarchical,
                    source: BrowseSource::Da2,
                })),
            ]),
            Arc::new(RecordingInventoryControl::default()),
        ))],
        vec![Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da3,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 100,
        })],
    ));
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("effective-profile.sqlite3")),
    ));

    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Ready).await;
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.organization, NamespaceOrganization::Hierarchical);
    assert_eq!(status.source, BrowseSource::Da2);
    assert_eq!(status.entry_count, 1);
    assert_eq!(status.unique_item_count, 1);
}

#[test]
fn failed_activation_keeps_the_previous_generation_active() {
    let directory = tempdir().unwrap();
    let mut db = IndexDb::open(&directory.path().join("activation.sqlite3")).unwrap();
    let previous = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.insert_entries("S", previous, &[inventory_entry("Previous", "S.Previous")])
        .unwrap();
    db.promote("S", previous, "2", &completed_progress(1))
        .unwrap();

    let target = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
        .unwrap();
    db.insert_entries("S", target, &[inventory_entry("Target", "S.Target")])
        .unwrap();
    db.connection
        .execute_batch(
            "CREATE TRIGGER reject_target_activation
             BEFORE UPDATE OF state ON generations
             WHEN NEW.generation = 2 AND NEW.state = 'active'
             BEGIN
               SELECT RAISE(FAIL, 'target activation rejected');
             END;",
        )
        .unwrap();

    assert!(
        db.promote("S", target, "4", &completed_progress(1))
            .unwrap_err()
            .to_string()
            .contains("target activation rejected")
    );
    let rows = db.status_rows("S").unwrap();
    assert_eq!(rows[0].state, "active");
    assert_eq!(rows[0].generation, previous);
    assert_eq!(rows[1].state, "staging");
    assert_eq!(rows[1].generation, target);
    assert_eq!(
        db.search("S", previous, "previous", 1, 10).unwrap().len(),
        1
    );
}

#[test]
fn activation_defers_superseded_data_to_bounded_cleanup() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("deferred-cleanup.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let previous = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    let obsolete_entries = synthetic_entries("Obsolete", CLEANUP_BATCH_SIZE + 1);
    db.insert_entries("S", previous, &obsolete_entries).unwrap();
    db.promote(
        "S",
        previous,
        "2",
        &completed_progress(obsolete_entries.len() as u64),
    )
    .unwrap();

    let active = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "3")
        .unwrap();
    db.insert_entries("S", active, &[inventory_entry("Active", "S.Active")])
        .unwrap();
    db.promote("S", active, "4", &completed_progress(1))
        .unwrap();
    assert_eq!(
        db.connection
            .query_row(
                "SELECT state FROM generations WHERE server = 'S' AND generation = ?1",
                [previous as i64],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "superseded"
    );
    assert_eq!(
        db.connection
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                [previous as i64],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        (CLEANUP_BATCH_SIZE + 1) as i64
    );

    let stats = cleanup_obsolete_generations(&path, "S", &BackgroundTasks::new()).unwrap();
    assert!(stats.batches >= 2);
    assert_eq!(stats.entries, (CLEANUP_BATCH_SIZE + 1) as u64);
    assert_eq!(stats.fts_entries, (CLEANUP_BATCH_SIZE + 1) as u64);
    assert_eq!(stats.generations, 1);
    assert_eq!(db.status_rows("S").unwrap().len(), 1);
    assert_eq!(db.search_generation("S").unwrap(), Some(active));
    assert_eq!(db.search("S", active, "active", 1, 10).unwrap().len(), 1);
}

#[test]
fn cleanup_precheck_avoids_a_write_when_no_obsolete_generation_exists() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("cleanup-precheck.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    db.promote("S", generation, "2", &zero_progress()).unwrap();
    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let started = Instant::now();
    let stats = cleanup_obsolete_generations(&path, "S", &BackgroundTasks::new()).unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(stats.batches, 0);
    drop(blocker);
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_errors_do_not_change_a_successfully_activated_generation() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("cleanup-error.sqlite3")),
    ));
    let active = manager
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
                    "CREATE TRIGGER fail_obsolete_cleanup
                 BEFORE DELETE ON entries
                 WHEN OLD.generation = 1
                 BEGIN
                   SELECT RAISE(FAIL, 'obsolete cleanup rejected');
                 END;",
                )
                .unwrap();
            Ok(active)
        })
        .unwrap();
    manager.schedule_cleanup("S");
    manager.background_tasks.wait_for_idle().await;

    let status = manager.status("S").await.unwrap();
    assert!(matches!(
        status.state,
        IndexState::Ready | IndexState::Stale
    ));
    assert_eq!(status.active_generation, active);
    assert_eq!(
        manager
            .search("S", "active", 1, 10)
            .await
            .unwrap()
            .matches
            .len(),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn abandoning_a_first_generation_preserves_its_failure_for_manual_retry() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("abandon.sqlite3")),
    ));
    let generation = manager
        .with_database(|db| {
            let generation =
                db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")?;
            let entries = synthetic_entries("Abandoned", CLEANUP_BATCH_SIZE + 1);
            db.insert_entries("S", generation, &entries)?;
            Ok(generation)
        })
        .unwrap();

    manager
        .with_database(|db| {
            db.connection.execute_batch("BEGIN IMMEDIATE")?;
            Ok(())
        })
        .unwrap();
    manager.abandon_generation("S", generation, "inventory cancelled");
    let failed = manager.with_database(|db| db.status_rows("S")).unwrap();
    assert_eq!(failed[0].state, "failed");
    assert_eq!(
        manager
            .with_database(|db| {
                db.connection
                    .query_row(
                        "SELECT COUNT(*) FROM entries WHERE server = 'S' AND generation = ?1",
                        [generation as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(Into::into)
            })
            .unwrap(),
        (CLEANUP_BATCH_SIZE + 1) as i64
    );
    manager
        .with_database(|db| {
            db.connection.execute_batch("COMMIT")?;
            Ok(())
        })
        .unwrap();
    manager.background_tasks.wait_for_idle().await;
    assert_eq!(
        manager.with_database(|db| db.status_rows("S")).unwrap()[0].state,
        "failed"
    );
}

#[cfg(not(coverage))]
#[test]
#[ignore = "production-scale regression: one million rows exercises activation without scans or cleanup"]
fn large_synthetic_generation_promotes_without_a_validation_scan() {
    let directory = tempdir().unwrap();
    let mut db = IndexDb::open(&directory.path().join("large-generation.sqlite3")).unwrap();
    let generation = db
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    const STRESS_ROWS: usize = 1_000_000;
    for offset in (0..STRESS_ROWS).step_by(1_000) {
        let entries = synthetic_entries("Stress", 1_000)
            .into_iter()
            .enumerate()
            .map(|(index, mut entry)| {
                let sequence = offset + index;
                entry.display_name = format!("Stress-{sequence}");
                entry.item_id = format!("Stress.{sequence}");
                entry
            })
            .collect::<Vec<_>>();
        db.insert_entries("S", generation, &entries).unwrap();
    }
    let promotion_started = Instant::now();
    db.promote(
        "S",
        generation,
        "2",
        &completed_progress(STRESS_ROWS as u64),
    )
    .unwrap();
    assert_eq!(
        db.status_rows("S").unwrap()[0].entry_count,
        STRESS_ROWS as u64
    );
    assert!(
        promotion_started.elapsed() < Duration::from_secs(5),
        "activation should only update generation metadata"
    );
}

#[test]
fn sqlite_generations_search_and_restart_cleanup_work() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    let generation = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "1",
        )
        .unwrap();
    db.insert_entries(
        "S",
        generation,
        &[
            InventoryEntry {
                display_name: "PV".into(),
                item_id: "FCS0201!204FI00510.PV".into(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec!["FCS0201".into(), "204FI00510".into()],
            },
            InventoryEntry {
                display_name: "Pressure".into(),
                item_id: "FCS0201!204FI00510.PV".into(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec!["FCS0201".into()],
            },
            InventoryEntry {
                display_name: "Temperature".into(),
                item_id: "FCS0201!204TI00510.PV".into(),
                kind: InventoryNodeKind::BranchAndItem,
                breadcrumbs: vec!["FCS0201".into()],
            },
            InventoryEntry {
                display_name: "Tag".into(),
                item_id: "Unique.Tag".into(),
                kind: InventoryNodeKind::Item,
                breadcrumbs: vec!["Area".into()],
            },
        ],
    )
    .unwrap();
    db.promote(
        "S",
        generation,
        "2",
        &InventoryProgress {
            branches_visited: 2,
            entries_seen: 3,
            unique_items: 2,
            active_time_ms: 1,
            paused_time_ms: 0,
            items_per_second: 2.0,
            estimated_remaining_ms: None,
        },
    )
    .unwrap();
    assert_eq!(db.search("S", generation, "PV", 1, 10).unwrap().len(), 1);
    assert_eq!(
        db.search("S", generation, "FCS0201!204FI00510.PV", 1, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.search("S", generation, "FCS0201!204FI", 2, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.search("S", generation, "fcs0201!204fi", 3, 10)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.search("S", generation, "pv", 3, 10).unwrap().len(), 2);
    assert_eq!(db.search("S", generation, "area", 3, 10).unwrap().len(), 1);
    assert_eq!(db.search("S", generation, "ar", 3, 10).unwrap().len(), 1);
    assert_eq!(db.search("S", generation, "temp", 2, 10).unwrap().len(), 1);
    let second_generation = db
        .start_generation(
            "S",
            NamespaceOrganization::Hierarchical,
            BrowseSource::Da2,
            "3",
        )
        .unwrap();
    db.insert_entries(
        "S",
        second_generation,
        &[InventoryEntry {
            display_name: "Second".into(),
            item_id: "second".into(),
            kind: InventoryNodeKind::Item,
            breadcrumbs: vec![],
        }],
    )
    .unwrap();
    db.promote(
        "S",
        second_generation,
        "4",
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
    .unwrap();
    assert_eq!(db.status_rows("S").unwrap().len(), 1);
    assert_eq!(
        db.status_rows("S").unwrap().first().unwrap().generation,
        second_generation
    );
    assert_eq!(
        db.status_rows("S").unwrap().first().unwrap().state,
        "active"
    );
    drop(db);

    let reopened = IndexDb::open(&path).unwrap();
    assert_eq!(
        reopened.status_rows("S").unwrap().first().unwrap().state,
        "active"
    );
    let read_only = IndexDb::open_read_only(&path).unwrap();
    assert_eq!(
        read_only
            .search("S", second_generation, "second", 3, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(
        read_only
            .connection
            .execute("DELETE FROM entries", [])
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_discards_generation_when_shutdown_is_requested_before_spawn() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let capability_started = Arc::new(Notify::new());
    let capability_release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(
            vec![Ok(handle_with_control(
                VecDeque::new(),
                Arc::clone(&control),
            ))],
            vec![],
        )
        .with_capability_gate(
            Arc::clone(&capability_started),
            Arc::clone(&capability_release),
        ),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));
    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });

    capability_started.notified().await;
    manager.background_tasks.request_shutdown();
    capability_release.notify_one();

    let status = refresh.await.unwrap().unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    assert!(control.cancelled.load(Ordering::Acquire));
    assert!(
        manager
            .with_database(|db| db.status_rows("S"))
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn refresh_discards_generation_when_runtime_build_disappears() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    let recovery_handle = immediate_inventory_handle();
    let capability_started = Arc::new(Notify::new());
    let capability_release = Arc::new(Notify::new());
    let client = Arc::new(
        LifecycleClient::new(
            vec![
                Ok(handle_with_control(VecDeque::new(), Arc::clone(&control))),
                Ok(recovery_handle),
            ],
            vec![],
        )
        .with_capability_gate(
            Arc::clone(&capability_started),
            Arc::clone(&capability_release),
        ),
    );
    let manager = Arc::new(IndexManager::new(
        client,
        settings(directory.path().join("index.sqlite3")),
    ));
    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move { refresh_manager.refresh("S", true).await });
    capability_started.notified().await;
    manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
    capability_release.notify_one();

    assert_eq!(
        refresh.await.unwrap().unwrap_err().to_string(),
        "index build disappeared before start"
    );
    assert!(control.cancelled.load(Ordering::Acquire));
    assert!(
        manager
            .with_database(|db| db.status_rows("S"))
            .unwrap()
            .is_empty()
    );
    assert!(manager.active_builds.lock().unwrap().is_empty());
    assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
    assert!(manager.build_locks.lock().unwrap().is_empty());
    assert!(manager.pause_overlays.lock().unwrap().is_empty());
    assert!(manager.pending_cancels.lock().unwrap().is_empty());

    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Ready).await;
}

#[tokio::test]
async fn manager_promotes_success_and_rolls_back_failed_refresh() {
    let directory = tempdir().unwrap();
    let client = Arc::new(MockOpcClient::default());
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
    let ready = manager.status("S").await.unwrap();
    assert_eq!(ready.active_generation, 1);
    let ready_search = manager.search("S", "mock", 3, 10).await.unwrap();
    assert_eq!(ready_search.status.active_generation, 1);
    assert_eq!(
        ready_search.matches,
        vec![IndexedMatch {
            item_id: "Mock.Tag".into(),
            display_name: "Mock tag".into(),
            kind: InventoryNodeKind::Item,
            breadcrumbs: vec!["Mock".into()],
        }]
    );

    client
        .inventory_events
        .lock()
        .unwrap()
        .push_back(Err("inventory failed".into()));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Failed).await;
    assert!(build_lock_path(&directory.path().join("index.sqlite3"), "S").exists());
    let failed = manager.status("S").await.unwrap();
    assert_eq!(failed.active_generation, 1);
    assert_eq!(failed.state, IndexState::Failed);
    let failed_search = manager.search("S", "mock", 3, 10).await.unwrap();
    assert_eq!(failed_search.status.active_generation, 1);
    assert_eq!(failed_search.matches, ready_search.matches);
}

#[tokio::test]
async fn runtime_pacing_failure_fails_the_active_generation() {
    let directory = tempdir().unwrap();
    let control = Arc::new(RecordingInventoryControl::default());
    control.fail_pacing_on_call(2);
    let client = Arc::new(LifecycleClient::new(
        vec![Ok(handle_with_control(
            VecDeque::from([
                Ok(InventoryEvent::Slice(InventorySliceObservation {
                    sequence: 1,
                    backend: InventorySliceBackend::Da2,
                    nodes_returned: 1,
                    has_more: false,
                    native_operations: 1,
                    elapsed_ms: 1,
                    entries_seen: 1,
                    unique_items: 1,
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
            Arc::clone(&control),
        ))],
        vec![],
    ));
    let mut config = settings(directory.path().join("index.sqlite3"));
    config.adaptive = true;
    let manager = Arc::new(IndexManager::new(client, config));

    manager.refresh("S", true).await.unwrap();
    wait_for_state(&manager, "S", IndexState::Failed).await;

    let status = manager.status("S").await.unwrap();
    assert_eq!(
        status.last_error.as_deref(),
        Some(
            "unable to update adaptive inventory pacing after slice 1: \
             test pacing update failure"
        )
    );
    assert!(control.is_cancelled());
}

#[tokio::test]
async fn active_profile_check_handles_missing_invalid_and_unavailable_data() {
    let directory = tempdir().unwrap();
    let missing = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("missing-profile.sqlite3")),
    );
    assert!(!missing.active_profile_changed("S").await.unwrap());

    let invalid = IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("invalid-profile.sqlite3")),
    );
    invalid
        .with_database(|db| {
            db.connection
                .execute_batch("DROP TABLE generations")
                .unwrap();
            Ok(())
        })
        .unwrap();
    assert!(!invalid.active_profile_changed("S").await.unwrap());

    let unavailable_client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Err("unavailable".into()), Err("unavailable-refresh".into())],
    ));
    let unavailable = Arc::new(IndexManager::new(
        unavailable_client,
        settings(directory.path().join("unavailable-profile.sqlite3")),
    ));
    seed_active_generation(
        &unavailable,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    let error = unavailable
        .active_profile_changed("S")
        .await
        .expect_err("capability errors must be surfaced");
    assert!(error.to_string().contains("unavailable"));
    unavailable.refresh_if_due("S").await;

    let maintenance_client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da3,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 100,
        })],
    ));
    let mut maintenance_config = settings(directory.path().join("maintenance-profile.sqlite3"));
    maintenance_config.maintenance_windows = vec!["invalid".into()];
    let maintenance = Arc::new(IndexManager::new(maintenance_client, maintenance_config));
    seed_active_generation(
        &maintenance,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );
    maintenance.refresh_if_due("S").await;

    let mut initial_config = settings(directory.path().join("maintenance-initial.sqlite3"));
    initial_config.maintenance_windows = vec!["invalid".into()];
    let initial = IndexManager::new(Arc::new(MockOpcClient::default()), initial_config);
    let initial_status = initial.status("S").await.unwrap();
    assert!(!initial.automatic_refresh_allowed(&initial_status));
}

#[tokio::test(flavor = "current_thread")]
async fn active_profile_check_times_out_a_stalled_capability_probe() {
    let directory = tempdir().unwrap();
    let client = Arc::new(
        LifecycleClient::new(
            vec![],
            vec![Ok(BrowseCapabilities {
                organization: NamespaceOrganization::Hierarchical,
                source: BrowseSource::Da2,
                supports_browse_sessions: true,
                supports_search: true,
                max_page_size: 100,
            })],
        )
        .with_capability_delay(Duration::from_secs(2)),
    );
    let mut config = settings(directory.path().join("profile-timeout.sqlite3"));
    config.operation_timeout_seconds = 1;
    let manager = Arc::new(IndexManager::new(client, config));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );

    let error = manager
        .active_profile_changed("S")
        .await
        .expect_err("stalled capability probes must be bounded");
    assert!(error.to_string().contains("timed out"));
}

#[tokio::test]
async fn negotiated_da2_profile_does_not_trigger_profile_invalidation() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da3,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 100,
        })],
    ));
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("negotiated-da2.sqlite3")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da3,
        &timestamp_now(),
    );
    manager
        .with_database(|db| {
            db.connection
                .execute(
                    "UPDATE generations
                 SET source = 'da2', compatibility_fallback = 1
                 WHERE server = 'S' AND state = 'active'",
                    [],
                )
                .unwrap();
            Ok(())
        })
        .unwrap();
    let profile = manager
        .with_database(|db| {
            db.active_profile("S")?
                .ok_or_else(|| anyhow::anyhow!("active profile missing"))
        })
        .unwrap();
    assert_eq!(profile.source, BrowseSource::Da2);
    assert!(profile.compatibility_fallback);

    manager.refresh_if_due("S").await;

    assert_eq!(manager.status("S").await.unwrap().active_generation, 1);
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn genuine_da2_profile_triggers_da3_invalidation() {
    let directory = tempdir().unwrap();
    let client = Arc::new(LifecycleClient::new(
        vec![],
        vec![Ok(BrowseCapabilities {
            organization: NamespaceOrganization::Hierarchical,
            source: BrowseSource::Da3,
            supports_browse_sessions: true,
            supports_search: true,
            max_page_size: 100,
        })],
    ));
    let manager = Arc::new(IndexManager::new(
        Arc::clone(&client),
        settings(directory.path().join("genuine-da2.sqlite3")),
    ));
    seed_active_generation(
        &manager,
        NamespaceOrganization::Hierarchical,
        BrowseSource::Da2,
        &timestamp_now(),
    );

    manager.refresh_if_due("S").await;

    assert_eq!(manager.status("S").await.unwrap().active_generation, 0);
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn delete_index_removes_enrollment_generations_and_retry_metadata() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("index.sqlite3")),
    ));
    manager.refresh("S", true).await.unwrap();
    wait_for_build(&manager, IndexState::Ready).await;
    manager
        .with_database(|db| {
            db.set_retry_state(
                "S",
                Some(SystemTime::now() + Duration::from_secs(60)),
                2,
                true,
            )
        })
        .unwrap();

    let status = manager
        .control("S", IndexControlAction::Delete)
        .await
        .unwrap();
    assert_eq!(status.state, IndexState::Deleting);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if manager.status("S").await.unwrap().state == IndexState::NotIndexed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let status = manager.status("S").await.unwrap();
    assert_eq!(status.state, IndexState::NotIndexed);
    manager
        .with_database(|db| {
            assert!(!db.is_enrolled("S")?);
            assert!(db.status_rows("S")?.is_empty());
            assert_eq!(db.retry_state("S")?, (None, 0, false));
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn persisted_retry_circuit_state_blocks_restart_until_forced_refresh() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("retry.sqlite3");
    let mut config = settings(path.clone());
    config.circuit_failure_threshold = 1;
    let failing = Arc::new(IndexManager::new(
        Arc::new(LifecycleClient::new(
            vec![Err("start failed".into())],
            vec![],
        )),
        config.clone(),
    ));
    assert!(failing.refresh("S", true).await.is_err());
    failing.background_tasks.wait_for_idle().await;
    drop(failing);

    let client = Arc::new(MockOpcClient::default());
    let restarted = Arc::new(IndexManager::new(Arc::clone(&client), config));
    let blocked = restarted.refresh("S", false).await.unwrap();
    assert_eq!(blocked.state, IndexState::Failed);
    assert_eq!(blocked.scheduler.consecutive_failures, 1);
    assert!(blocked.scheduler.circuit_open);
    assert!(blocked.scheduler.retry_after.is_some());
    assert_eq!(client.inventory_start_count.load(Ordering::Relaxed), 0);
    assert!(
        restarted
            .with_database(|db| db.scheduled_servers())
            .unwrap()
            .is_empty()
    );

    restarted.refresh("S", true).await.unwrap();
    wait_for_build(&restarted, IndexState::Ready).await;
    let recovered = restarted.status("S").await.unwrap();
    assert_eq!(recovered.scheduler.consecutive_failures, 0);
    assert!(!recovered.scheduler.circuit_open);
    assert!(recovered.scheduler.retry_after.is_none());
}

#[test]
fn start_generation_stops_when_a_cancelled_control_loses_its_build() {
    let directory = tempdir().unwrap();
    let manager = Arc::new(IndexManager::new(
        Arc::new(LifecycleClient::new(
            vec![],
            vec![Ok(default_capabilities())],
        )),
        settings(directory.path().join("generation-cancel.sqlite3")),
    ));
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel();
    let control: Arc<dyn InventoryControl> = control;
    let ownership = insert_runtime_build(&manager, Arc::clone(&control));
    manager.runtime.lock().unwrap().get_mut("S").unwrap().build = None;
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
    let handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&control),
    };

    assert!(
        manager
            .launch_refresh_build("S", generation, handle, ownership, false)
            .is_ok()
    );
    assert!(
        manager
            .with_database(|db| db.status_rows("S"))
            .unwrap()
            .is_empty()
    );
    assert!(manager.coordination.build_owners.lock().unwrap().is_empty());
    assert!(manager.active_builds.lock().unwrap().is_empty());

    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(directory.path().join("generation-mismatch.sqlite3")),
    ));
    let wrong_control: Arc<dyn InventoryControl> = Arc::new(RecordingInventoryControl::default());
    let ownership = insert_runtime_build(&manager, Arc::clone(&wrong_control));
    let control = Arc::new(RecordingInventoryControl::default());
    control.cancel();
    let control: Arc<dyn InventoryControl> = control;
    manager
        .runtime
        .lock()
        .unwrap()
        .get_mut("S")
        .unwrap()
        .build
        .as_mut()
        .unwrap()
        .control = Some(Arc::clone(&wrong_control));
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
    let handle = InventoryHandle {
        stream: Box::new(VecInventoryStream {
            events: VecDeque::new(),
        }),
        control: Arc::clone(&control),
    };
    assert!(
        manager
            .launch_refresh_build("S", generation, handle, ownership, false)
            .is_ok()
    );
    assert!(
        manager
            .with_database(|db| db.status_rows("S"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn restart_recovery_surfaces_staging_update_errors() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("staging-recovery.sqlite3");
    let mut db = IndexDb::open(&path).unwrap();
    db.start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "1")
        .unwrap();
    drop(db);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_restart_recovery
             BEFORE UPDATE OF state ON generations
             BEGIN
               SELECT RAISE(FAIL, 'restart recovery rejected');
             END;",
        )
        .unwrap();
    drop(connection);
    let error = IndexDb::open(&path)
        .err()
        .expect("restart recovery should fail");
    assert!(error.to_string().contains("restart recovery rejected"));
}

#[test]
fn quarantine_errors_restore_moved_files_and_report_rollback_failures() {
    use std::io::{Error, ErrorKind};

    let directory = tempdir().unwrap();
    let path = directory.path().join("index.sqlite3");
    let quarantine = directory.path().join("quarantine.sqlite3");
    let wal = IndexDb::sqlite_sidecar_path(&path, "-wal");
    fs::write(&path, b"database").unwrap();
    fs::write(&wal, b"wal").unwrap();

    let metadata_error = store::quarantine_index_files_with(
        &path,
        &quarantine,
        |_| Err(Error::new(ErrorKind::PermissionDenied, "metadata failure")),
        |source, destination| fs::rename(source, destination),
    )
    .unwrap_err();
    assert!(metadata_error.to_string().contains("metadata failure"));

    let first_rename_error = store::quarantine_index_files_with(
        &path,
        &quarantine,
        |source| fs::symlink_metadata(source),
        |_, _| Err(Error::other("first rename failure")),
    )
    .unwrap_err();
    assert!(
        first_rename_error
            .to_string()
            .contains("first rename failure")
    );

    let mut rename_count = 0;
    let rollback_error = store::quarantine_index_files_with(
        &path,
        &quarantine,
        |source| fs::symlink_metadata(source),
        |source, destination| {
            rename_count += 1;
            if rename_count == 2 {
                Err(Error::other("sidecar rename failure"))
            } else {
                fs::rename(source, destination)
            }
        },
    )
    .unwrap_err();
    assert!(
        rollback_error
            .to_string()
            .contains("sidecar rename failure")
    );
    assert!(path.exists());
    assert!(wal.exists());
    assert!(!quarantine.exists());

    let mut rename_count = 0;
    let rollback_error = store::quarantine_index_files_with(
        &path,
        &quarantine,
        |source| fs::symlink_metadata(source),
        |source, destination| {
            rename_count += 1;
            match rename_count {
                2 => Err(Error::other("sidecar rename failure")),
                3 => Err(Error::other("rollback rename failure")),
                _ => fs::rename(source, destination),
            }
        },
    )
    .unwrap_err();
    assert!(rollback_error.to_string().contains("rollback also failed"));
    assert!(!path.exists());
    assert!(quarantine.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn first_database_write_schedules_cleanup_for_obsolete_generations() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("obsolete.sqlite3");
    let mut database = IndexDb::open(&path).unwrap();
    let active = database
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    database
        .promote("S", active, "1", &completed_progress(0))
        .unwrap();
    let failed = database
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "2")
        .unwrap();
    database.fail_generation("S", failed, "failed").unwrap();
    drop(database);

    let manager = Arc::new(IndexManager::new(
        Arc::new(MockOpcClient::default()),
        settings(path),
    ));
    manager.with_database_write(|_| Ok(())).unwrap();
    manager.background_tasks.wait_for_idle().await;
    assert!(
        manager
            .with_database_read(|db| db.obsolete_servers())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn store_quarantine_logger_handles_no_files_to_move() {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        store::log_quarantine_result(
            Path::new("index.sqlite3"),
            Path::new("index.quarantine"),
            &anyhow::anyhow!("invalid schema"),
            false,
        );
    });
}

#[test]
fn store_open_reports_fts_corruption_and_preserves_a_live_staging_generation() {
    let directory = tempdir().unwrap();
    let corrupt_path = directory.path().join("corrupt-fts.sqlite3");
    let database = IndexDb::open(&corrupt_path).unwrap();
    database
        .connection
        .execute_batch("DROP TABLE entries_fts_data")
        .unwrap();
    drop(database);
    let error = IndexDb::open_once(&corrupt_path)
        .err()
        .expect("a missing FTS backing table should fail validation");
    assert!(
        format!("{error:#}").contains("corrupt"),
        "unexpected FTS validation error: {error:#}"
    );

    let staging_path = directory.path().join("live-staging.sqlite3");
    let mut database = IndexDb::open(&staging_path).unwrap();
    let generation = database
        .start_generation("S", NamespaceOrganization::Flat, BrowseSource::Flat, "0")
        .unwrap();
    drop(database);
    let lock = BuildFileLock::acquire(&staging_path, "S").unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let reopened =
        tracing::subscriber::with_default(subscriber, || IndexDb::open(&staging_path).unwrap());
    assert_eq!(reopened.status_rows("S").unwrap()[0].generation, generation);
    assert_eq!(reopened.status_rows("S").unwrap()[0].state, "staging");
    drop(reopened);
    drop(lock);
}
