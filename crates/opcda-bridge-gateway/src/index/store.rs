use super::{
    BuildFileLock, DbStatus, Enrollment, IndexDb, IndexManager, SCHEMA_VERSION, StorageDiagnostics,
    StoredIndexProfile, namespace_string, node_kind_number, normalize_query, parse_namespace,
    parse_source, source_string, status, timestamp_now,
};
use crate::opc::{
    BrowseSource, InventoryEntry, InventoryProgress, NamespaceOrganization, OpcClient,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

impl<C: OpcClient> IndexManager<C> {
    #[cfg(test)]
    pub(super) fn with_database<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        self.with_database_read(operation)
    }

    pub(super) fn with_database_read<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        let needs_open = self
            .database
            .lock()
            .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?
            .is_none();
        let cleanup_servers = if needs_open {
            let _writer_guard = self
                .writer_gate
                .lock()
                .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            self.initialize_database(&mut database)?
        } else {
            Vec::new()
        };
        let result = {
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            operation(database.as_mut().expect("database initialized"))
        };
        for server in cleanup_servers {
            self.schedule_cleanup(&server);
        }
        result
    }

    pub(super) fn with_database_write<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut IndexDb) -> anyhow::Result<R>,
    {
        let _writer_guard = self
            .writer_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("index writer gate poisoned"))?;
        let (result, cleanup_servers) = {
            let mut database = self
                .database
                .lock()
                .map_err(|_| anyhow::anyhow!("index database lock poisoned"))?;
            let cleanup_servers = self.initialize_database(&mut database)?;
            (
                operation(database.as_mut().expect("database initialized")),
                cleanup_servers,
            )
        };
        for server in cleanup_servers {
            self.schedule_cleanup(&server);
        }
        result
    }

    pub(super) fn initialize_database(
        &self,
        database: &mut Option<IndexDb>,
    ) -> anyhow::Result<Vec<String>> {
        if database.is_none() {
            tracing::debug!(target: "opcda_bridge_gateway::index",
                process_id = std::process::id(),
                database = %self.settings.database_path.display(),
                "initializing namespace index database handle"
            );
            *database = Some(IndexDb::open(&self.settings.database_path)?);
            Ok(database
                .as_ref()
                .expect("database initialized")
                .obsolete_servers()?)
        } else {
            Ok(Vec::new())
        }
    }
}

impl IndexDb {
    pub(super) fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %path.display(),
            "opening namespace index database"
        );
        match Self::open_once(path) {
            Ok(db) => Ok(db),
            Err(error) => {
                if !is_quarantinable_index_error(&error) {
                    return Err(error);
                }
                let quarantine = path.with_extension(format!("quarantine-{}", Uuid::new_v4()));
                let moved = quarantine_index_files(path, &quarantine)?;
                log_quarantine_result(path, &quarantine, &error, moved);
                Self::open_once(path)
            }
        }
    }

    pub(super) fn open_once(path: &Path) -> anyhow::Result<Self> {
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS index_meta (
                 key TEXT PRIMARY KEY NOT NULL,
                 value TEXT NOT NULL
             );",
        )?;
        let schema_version = connection
            .query_row(
                "SELECT value FROM index_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|version| {
                version.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("invalid namespace index schema version {version:?}")
                })
            })
            .transpose()?;
        if let Some(version) = schema_version {
            match version {
                2 => {
                    migrate_schema_2_to_3(&mut connection)?;
                    migrate_schema_3_to_4(&mut connection)?;
                }
                3 => migrate_schema_3_to_4(&mut connection)?,
                SCHEMA_VERSION => {}
                _ => anyhow::bail!("unsupported namespace index schema version {version}"),
            }
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS enrolled_servers (
                 server TEXT PRIMARY KEY NOT NULL,
                 auto_refresh_enabled INTEGER NOT NULL DEFAULT 1
                   CHECK (auto_refresh_enabled IN (0, 1)),
                 enrolled_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS generations (
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
                 compatibility_fallback INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (server, generation)
             );
             CREATE TABLE IF NOT EXISTS entries (
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
             CREATE INDEX IF NOT EXISTS entries_display_prefix
               ON entries(server, generation, display_name_norm);
             CREATE INDEX IF NOT EXISTS entries_item_prefix
               ON entries(server, generation, item_id_norm);
             CREATE INDEX IF NOT EXISTS entries_display_exact
               ON entries(server, generation, display_name_norm, item_id_norm, item_id);
             CREATE INDEX IF NOT EXISTS entries_item_exact
               ON entries(
                   server, generation, item_id_norm, length(display_name_norm),
                   display_name_norm, item_id
               );
             CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
                 server UNINDEXED,
                 generation UNINDEXED,
                 item_id,
                 display_name,
                 breadcrumbs,
                 tokenize = 'trigram'
             );",
        )?;
        connection.execute(
            "INSERT OR REPLACE INTO index_meta(key, value) VALUES ('schema_version', ?1)",
            [SCHEMA_VERSION.to_string()],
        )?;
        let relational_entries_exist =
            connection.query_row("SELECT EXISTS(SELECT 1 FROM entries LIMIT 1)", [], |row| {
                row.get::<_, bool>(0)
            })?;
        let full_text_entries_exist = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM entries_fts LIMIT 1)",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if relational_entries_exist != full_text_entries_exist {
            anyhow::bail!("namespace index relational and full-text data are inconsistent");
        }

        let staging_servers = {
            let mut statement = connection
                .prepare("SELECT DISTINCT server FROM generations WHERE state = 'staging'")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for server in staging_servers {
            if BuildFileLock::is_held(path, &server)? {
                tracing::debug!(target: "opcda_bridge_gateway::index",
                    database = %path.display(),
                    server = %server,
                    "preserving namespace index staging generation owned by a live process"
                );
                continue;
            }
            connection.execute(
                "UPDATE generations AS interrupted
                 SET state = CASE
                         WHEN EXISTS (
                             SELECT 1 FROM generations AS active
                             WHERE active.server = interrupted.server
                               AND active.state = 'active'
                         )
                         THEN 'superseded'
                         ELSE 'failed'
                     END,
                     last_error = COALESCE(
                         last_error,
                         'namespace index build interrupted by gateway restart'
                     )
                 WHERE interrupted.server = ?1
                   AND interrupted.state = 'staging'",
                [server],
            )?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            connection,
            #[cfg(test)]
            reject_next_prefix_query_map: AtomicBool::new(false),
        })
    }

    pub(super) fn open_read_only(path: &Path) -> anyhow::Result<Self> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "query_only", true)?;
        Ok(Self {
            path: path.to_path_buf(),
            connection,
            #[cfg(test)]
            reject_next_prefix_query_map: AtomicBool::new(false),
        })
    }

    pub(super) fn storage_diagnostics(&self) -> StorageDiagnostics {
        status::storage_diagnostics_for_path(&self.path)
    }

    pub(super) fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        PathBuf::from(sidecar)
    }

    pub(super) fn retry_state(
        &self,
        server: &str,
    ) -> anyhow::Result<(Option<SystemTime>, u32, bool)> {
        let get = |key: String| -> anyhow::Result<Option<String>> {
            Ok(self
                .connection
                .query_row(
                    "SELECT value FROM index_meta WHERE key = ?1",
                    [key],
                    |row| row.get(0),
                )
                .optional()?)
        };
        let retry_after = get(format!("retry_after:{server}"))?
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)));
        let failures = get(format!("failures:{server}"))?
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let circuit_open = get(format!("circuit:{server}"))?.is_some_and(|value| value == "1");
        Ok((retry_after, failures, circuit_open))
    }

    pub(super) fn set_retry_state(
        &self,
        server: &str,
        retry_after: Option<SystemTime>,
        failures: u32,
        circuit_open: bool,
    ) -> anyhow::Result<()> {
        let retry = retry_after
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_millis().to_string())
            .unwrap_or_default();
        self.connection.execute_batch("BEGIN IMMEDIATE;")?;
        let result = (|| {
            for (key, value) in [
                (format!("retry_after:{server}"), retry),
                (format!("failures:{server}"), failures.to_string()),
                (
                    format!("circuit:{server}"),
                    if circuit_open { "1" } else { "0" }.to_string(),
                ),
            ] {
                self.connection.execute(
                    "INSERT OR REPLACE INTO index_meta(key, value) VALUES (?1, ?2)",
                    params![key, value],
                )?;
            }
            Ok::<(), rusqlite::Error>(())
        })();
        match result {
            Ok(()) => self.connection.execute_batch("COMMIT;")?,
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK;");
                return Err(error.into());
            }
        }
        Ok(())
    }

    pub(super) fn start_generation(
        &mut self,
        server: &str,
        organization: NamespaceOrganization,
        source: BrowseSource,
        started_at: &str,
    ) -> anyhow::Result<u64> {
        self.enroll(server, started_at)?;
        let generation = self.connection.query_row(
            "SELECT COALESCE(MAX(generation), 0) + 1
                 FROM generations WHERE server = ?1",
            [server],
            |row| row.get::<_, i64>(0),
        )?;
        let public_generation = u64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation is negative"))?;
        self.connection.execute(
            "INSERT INTO generations
             (server, generation, state, organization, source, started_at)
             VALUES (?1, ?2, 'staging', ?3, ?4, ?5)",
            params![
                server,
                generation,
                namespace_string(organization),
                source_string(source),
                started_at
            ],
        )?;
        tracing::debug!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation = public_generation,
            "started namespace index generation"
        );
        Ok(public_generation)
    }

    pub(super) fn record_failed_attempt(
        &mut self,
        server: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        let generation = self.connection.query_row(
            "SELECT COALESCE(MAX(generation), 0) + 1
             FROM generations WHERE server = ?1",
            [server],
            |row| row.get::<_, i64>(0),
        )?;
        self.connection.execute(
            "INSERT INTO generations
             (server, generation, state, organization, source, started_at, last_error)
             VALUES (?1, ?2, 'failed', 'unspecified', 'unspecified', ?3, ?4)",
            params![server, generation, timestamp_now(), error],
        )?;
        Ok(())
    }

    pub(super) fn insert_entries(
        &mut self,
        server: &str,
        generation: u64,
        entries: &[InventoryEntry],
    ) -> anyhow::Result<u64> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let transaction = self.connection.transaction()?;
        let mut inserted_count = 0_u64;
        for entry in entries {
            if entry.item_id.is_empty() {
                anyhow::bail!("inventory entry has an empty ItemID");
            }
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO entries
                 (server, generation, item_id, item_id_norm, display_name,
                  display_name_norm, kind, breadcrumbs)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    server,
                    generation,
                    entry.item_id,
                    normalize_query(&entry.item_id),
                    entry.display_name,
                    normalize_query(&entry.display_name),
                    node_kind_number(entry.kind),
                    serde_json::to_string(&entry.breadcrumbs)?
                ],
            )?;
            if inserted > 0 {
                inserted_count = inserted_count.saturating_add(u64::try_from(inserted)?);
                transaction.execute(
                    "INSERT INTO entries_fts
                     (server, generation, item_id, display_name, breadcrumbs)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        server,
                        generation,
                        entry.item_id,
                        entry.display_name,
                        entry.breadcrumbs.join(" ")
                    ],
                )?;
            }
        }
        transaction.commit()?;
        tracing::debug!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            batch_size = entries.len(),
            inserted_count,
            "committed namespace index entries"
        );
        Ok(inserted_count)
    }

    pub(super) fn update_progress(
        &self,
        server: &str,
        generation: u64,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        let entry_count = i64::try_from(progress.entries_seen)
            .map_err(|_| anyhow::anyhow!("namespace index entry count exceeds SQLite range"))?;
        let unique_item_count = i64::try_from(progress.unique_items).map_err(|_| {
            anyhow::anyhow!("namespace index unique item count exceeds SQLite range")
        })?;
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        self.connection.execute(
            "UPDATE generations SET entry_count = ?1, unique_item_count = ?2
             WHERE server = ?3 AND generation = ?4 AND state = 'staging'",
            params![entry_count, unique_item_count, server, generation],
        )?;
        tracing::debug!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            entries_seen = progress.entries_seen,
            unique_items = progress.unique_items,
            "updated namespace index progress"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn promote(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        progress: &InventoryProgress,
    ) -> anyhow::Result<()> {
        self.promote_with_profile(
            server,
            generation,
            completed_at,
            progress.unique_items,
            None,
            None,
        )
    }

    pub(super) fn promote_with_profile(
        &mut self,
        server: &str,
        generation: u64,
        completed_at: &str,
        searchable_item_count: u64,
        profile: Option<(NamespaceOrganization, BrowseSource)>,
        warning: Option<&str>,
    ) -> anyhow::Result<()> {
        let activation_started = Instant::now();
        let searchable_item_count = i64::try_from(searchable_item_count)
            .map_err(|_| anyhow::anyhow!("namespace index entry count exceeds SQLite range"))?;
        let (organization, source) = profile
            .map(|(organization, source)| {
                (
                    Some(namespace_string(organization)),
                    Some(source_string(source)),
                )
            })
            .unwrap_or((None, None));
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE generations SET state = 'superseded'
             WHERE server = ?1 AND state = 'active'",
            [server],
        )?;
        let promoted = transaction.execute(
            "UPDATE generations
             SET state = 'active', completed_at = ?1,
                 entry_count = ?2, unique_item_count = ?2,
                 organization = COALESCE(?3, organization),
                 compatibility_fallback =
                   CASE WHEN source = 'da3' AND ?4 = 'da2' THEN 1 ELSE 0 END,
                 source = COALESCE(?4, source), last_error = ?5
             WHERE server = ?6 AND generation = ?7 AND state = 'staging'",
            params![
                completed_at,
                searchable_item_count,
                organization,
                source,
                warning,
                server,
                generation
            ],
        )?;
        if promoted != 1 {
            anyhow::bail!("namespace index generation is not staging");
        }
        transaction.commit()?;
        tracing::info!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            entry_count = searchable_item_count,
            unique_item_count = searchable_item_count,
            effective_organization = organization.unwrap_or(""),
            effective_source = source.unwrap_or(""),
            warning = warning.unwrap_or(""),
            activation_duration_ms = activation_started.elapsed().as_millis() as u64,
            "activated namespace index generation"
        );
        Ok(())
    }

    pub(super) fn fail_generation(
        &self,
        server: &str,
        generation: u64,
        error: &str,
    ) -> anyhow::Result<()> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        self.connection.execute(
            "UPDATE generations SET state = 'failed', last_error = ?1
             WHERE server = ?2 AND generation = ?3 AND state = 'staging'",
            params![error, server, generation],
        )?;
        tracing::warn!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.path.display(),
            server,
            generation,
            error,
            "marked namespace index generation failed"
        );
        Ok(())
    }

    pub(super) fn discard_empty_generation(
        &self,
        server: &str,
        generation: u64,
    ) -> anyhow::Result<bool> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        Ok(self.connection.execute(
            "DELETE FROM generations
             WHERE server = ?1 AND generation = ?2 AND state = 'staging'
               AND NOT EXISTS (
                   SELECT 1 FROM entries
                   WHERE entries.server = generations.server
                     AND entries.generation = generations.generation
               )
               AND NOT EXISTS (
                   SELECT 1 FROM entries_fts
                   WHERE entries_fts.server = generations.server
                     AND entries_fts.generation = generations.generation
               )",
            params![server, generation],
        )? == 1)
    }

    pub(super) fn obsolete_servers(&self) -> anyhow::Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT server FROM generations
             WHERE state = 'superseded'
                OR (state = 'failed' AND EXISTS (
                    SELECT 1 FROM generations AS active
                    WHERE active.server = generations.server
                      AND active.state = 'active'
                ))",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(super) fn has_obsolete_generations(&self, server: &str) -> anyhow::Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM generations
                     WHERE server = ?1
                       AND (state = 'superseded'
                            OR (state = 'failed' AND EXISTS (
                                SELECT 1 FROM generations AS active
                                WHERE active.server = generations.server
                                  AND active.state = 'active'
                            )))
                 )",
                [server],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub(super) fn clear_server(&mut self, server: &str) -> anyhow::Result<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute("DELETE FROM entries_fts WHERE server = ?1", [server])?;
        transaction.execute("DELETE FROM generations WHERE server = ?1", [server])?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn enrollment(&self, server: &str) -> anyhow::Result<Option<Enrollment>> {
        self.connection
            .query_row(
                "SELECT auto_refresh_enabled FROM enrolled_servers WHERE server = ?1",
                [server],
                |row| {
                    Ok(Enrollment {
                        auto_refresh_enabled: row.get::<_, bool>(0)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub(super) fn enroll(&self, server: &str, timestamp: &str) -> anyhow::Result<()> {
        self.connection.execute(
            "INSERT OR IGNORE INTO enrolled_servers
             (server, auto_refresh_enabled, enrolled_at, updated_at)
             VALUES (?1, 1, ?2, ?2)",
            params![server, timestamp],
        )?;
        Ok(())
    }

    pub(super) fn set_auto_refresh(&self, server: &str, enabled: bool) -> anyhow::Result<bool> {
        Ok(self.connection.execute(
            "UPDATE enrolled_servers
             SET auto_refresh_enabled = ?1, updated_at = ?2
             WHERE server = ?3",
            params![enabled, timestamp_now(), server],
        )? == 1)
    }

    pub(super) fn scheduled_servers(&self) -> anyhow::Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT enrolled.server
             FROM enrolled_servers AS enrolled
             WHERE enrolled.auto_refresh_enabled = 1
               AND EXISTS (
                   SELECT 1 FROM generations AS generation
                   WHERE generation.server = enrolled.server
                     AND generation.state = 'active'
               )
             ORDER BY enrolled.server",
        )?;
        Ok(statement
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub(super) fn delete_index(&mut self, server: &str) -> anyhow::Result<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute("DELETE FROM entries_fts WHERE server = ?1", [server])?;
        transaction.execute("DELETE FROM generations WHERE server = ?1", [server])?;
        transaction.execute(
            "DELETE FROM index_meta
             WHERE key IN (?1, ?2, ?3)",
            params![
                format!("retry_after:{server}"),
                format!("failures:{server}"),
                format!("circuit:{server}"),
            ],
        )?;
        transaction.execute("DELETE FROM enrolled_servers WHERE server = ?1", [server])?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn status_rows(&self, server: &str) -> anyhow::Result<Vec<DbStatus>> {
        let mut statement = self.connection.prepare(
            "SELECT generation, state, organization, source, started_at,
                        completed_at, entry_count, unique_item_count, last_error
                 FROM generations
                 WHERE server = ?1 AND state IN ('active', 'staging', 'failed')
                 ORDER BY CASE state WHEN 'active' THEN 0 WHEN 'staging' THEN 1 ELSE 2 END,
                          generation DESC
                 ",
        )?;
        let rows = statement.query_map([server], |row| {
            Ok(DbStatus {
                generation: row.get::<_, i64>(0)? as u64,
                state: row.get(1)?,
                organization: parse_namespace(&row.get::<_, String>(2)?),
                source: parse_source(&row.get::<_, String>(3)?),
                started_at: row.get(4)?,
                completed_at: row.get(5)?,
                entry_count: row.get::<_, i64>(6)? as u64,
                unique_item_count: row.get::<_, i64>(7)? as u64,
                last_error: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(super) fn active_profile(
        &self,
        server: &str,
    ) -> anyhow::Result<Option<StoredIndexProfile>> {
        self.connection
            .query_row(
                "SELECT organization, source, compatibility_fallback
                 FROM generations
                 WHERE server = ?1 AND state = 'active'
                 ORDER BY generation DESC
                 LIMIT 1",
                [server],
                |row| {
                    Ok(StoredIndexProfile {
                        organization: parse_namespace(&row.get::<_, String>(0)?),
                        source: parse_source(&row.get::<_, String>(1)?),
                        compatibility_fallback: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }
}

pub(super) fn migrate_schema_2_to_3(connection: &mut Connection) -> anyhow::Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "ALTER TABLE generations
           ADD COLUMN compatibility_fallback INTEGER NOT NULL DEFAULT 0;
         INSERT OR REPLACE INTO index_meta(key, value)
           VALUES ('schema_version', '3');",
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn migrate_schema_3_to_4(connection: &mut Connection) -> anyhow::Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS enrolled_servers (
             server TEXT PRIMARY KEY NOT NULL,
             auto_refresh_enabled INTEGER NOT NULL DEFAULT 1
               CHECK (auto_refresh_enabled IN (0, 1)),
             enrolled_at TEXT NOT NULL,
             updated_at TEXT NOT NULL
         );",
    )?;

    let servers = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT server
             FROM generations
             ORDER BY server",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let migrated_at = timestamp_now();
    for server in servers {
        let has_active_generation = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM generations
                 WHERE server = ?1 AND state = 'active'
             )",
            [&server],
            |row| row.get::<_, bool>(0),
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO enrolled_servers
             (server, auto_refresh_enabled, enrolled_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)",
            params![server, has_active_generation, migrated_at],
        )?;
    }
    transaction.execute(
        "INSERT OR REPLACE INTO index_meta(key, value)
         VALUES ('schema_version', ?1)",
        [SCHEMA_VERSION.to_string()],
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn quarantine_index_files(path: &Path, quarantine: &Path) -> anyhow::Result<bool> {
    quarantine_index_files_with(
        path,
        quarantine,
        |source| fs::symlink_metadata(source),
        |source, destination| fs::rename(source, destination),
    )
}

pub(super) fn log_quarantine_result(
    path: &Path,
    quarantine: &Path,
    error: &anyhow::Error,
    moved: bool,
) {
    if moved {
        tracing::warn!(target: "opcda_bridge_gateway::index",
            database = %path.display(),
            quarantine = %quarantine.display(),
            error = %error,
            "quarantined invalid namespace index"
        );
    }
}

pub(super) fn quarantine_index_files_with<M, R>(
    path: &Path,
    quarantine: &Path,
    mut metadata: M,
    mut rename: R,
) -> anyhow::Result<bool>
where
    M: FnMut(&Path) -> std::io::Result<std::fs::Metadata>,
    R: FnMut(&Path, &Path) -> std::io::Result<()>,
{
    let files = [
        (path.to_path_buf(), quarantine.to_path_buf()),
        (
            IndexDb::sqlite_sidecar_path(path, "-wal"),
            IndexDb::sqlite_sidecar_path(quarantine, "-wal"),
        ),
        (
            IndexDb::sqlite_sidecar_path(path, "-shm"),
            IndexDb::sqlite_sidecar_path(quarantine, "-shm"),
        ),
    ];
    let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
    for (source, destination) in files {
        match metadata(&source) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
        if let Err(error) = rename(&source, &destination) {
            let mut rollback_errors = Vec::new();
            for (moved_source, moved_destination) in moved.into_iter().rev() {
                if let Err(rollback_error) = rename(&moved_destination, &moved_source) {
                    rollback_errors.push(rollback_error);
                }
            }
            if rollback_errors.is_empty() {
                return Err(error.into());
            }
            return Err(anyhow::anyhow!(
                "failed to quarantine namespace index file {}: {error}; \
                 rollback also failed for {} file(s)",
                source.display(),
                rollback_errors.len()
            ));
        }

        moved.push((source, destination));
    }
    Ok(!moved.is_empty())
}

pub(super) fn is_quarantinable_index_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("unsupported namespace index schema version")
        || message.contains("invalid namespace index schema version")
        || message.contains("namespace index relational and full-text data are inconsistent")
        || message.contains("file is not a database")
        || message.contains("database disk image is malformed")
}
