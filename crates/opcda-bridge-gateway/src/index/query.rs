use super::*;

impl<C: OpcClient> IndexManager<C> {
    #[cfg(test)]
    pub(super) fn install_search_gate(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.search_gate.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }

    pub async fn search(
        &self,
        server: &str,
        query: &str,
        mode: i32,
        limit: u32,
    ) -> anyhow::Result<IndexedSearch> {
        if normalize_query(query).is_empty() {
            anyhow::bail!("search query must not be empty");
        }
        let limit = limit.max(1).min(self.settings.max_results);
        let status = self.status(server).await?;
        if status.state == IndexState::Deleting {
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status,
            });
        }
        let normalized_query = normalize_query(query);
        let generation = if status.active_generation > 0 {
            Some(status.active_generation)
        } else if status.state == IndexState::Promoting {
            None
        } else {
            self.with_database_read(|db| db.search_generation(server))?
        };
        let Some(generation) = generation else {
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status,
            });
        };
        let key = CacheKey {
            server: server.to_string(),
            generation,
            query: normalized_query.clone(),
            mode,
            limit,
        };
        if status.active_generation == generation
            && let Some(mut value) = self
                .cache
                .lock()
                .map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?
                .get(&key)
        {
            value.status = status;
            return Ok(value);
        }

        let search_started = Instant::now();
        let database_path = self.settings.database_path.clone();
        let server_name = server.to_string();
        let query_name = query.to_string();
        #[cfg(test)]
        let search_gate = self.search_gate.lock().unwrap().take();
        let mut matches = if database_path == Path::new(":memory:") {
            self.with_database_read(|db| db.search(server, generation, query, mode, limit))?
        } else {
            tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some((started, release)) = search_gate {
                    let _ = started.send(());
                    let _ = release.blocking_recv();
                }
                let db = IndexDb::open_read_only(&database_path)?;
                db.search(&server_name, generation, &query_name, mode, limit)
            })
            .await??
        };
        if self.is_deleting(server)? {
            let status = self.status(server).await?;
            return Ok(IndexedSearch {
                matches: Vec::new(),
                has_more: false,
                status,
            });
        }
        let has_more = matches.len() > limit as usize;
        matches.truncate(limit as usize);
        tracing::debug!(target: "opcda_bridge_gateway::index",
            process_id = std::process::id(),
            database = %self.settings.database_path.display(),
            server,
            generation,
            mode,
            limit,
            matches = matches.len(),
            has_more,
            duration_ms = search_started.elapsed().as_millis() as u64,
            "completed namespace index search"
        );
        let value = IndexedSearch {
            matches,
            has_more,
            status,
        };
        if value.status.active_generation == generation {
            self.cache
                .lock()
                .map_err(|_| anyhow::anyhow!("index cache lock poisoned"))?
                .insert(key, value.clone());
        }
        Ok(value)
    }
}

impl IndexDb {
    pub(super) fn search_generation(&self, server: &str) -> anyhow::Result<Option<u64>> {
        self.connection
            .query_row(
                "SELECT generation FROM generations
                 WHERE server = ?1 AND state IN ('active', 'staging')
                 ORDER BY CASE state WHEN 'active' THEN 0 ELSE 1 END,
                          generation DESC
                 LIMIT 1",
                [server],
                |row| row.get::<_, i64>(0).map(|value| value as u64),
            )
            .optional()
            .map_err(Into::into)
    }

    pub(super) fn search(
        &self,
        server: &str,
        generation: u64,
        query: &str,
        mode: i32,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let normalized_query = normalize_query(query);
        if mode == 1 {
            return self.search_exact(server, generation, &normalized_query, limit);
        }
        if mode == 2 {
            return self.search_prefix(server, generation, &normalized_query, limit);
        }
        let fts_compatible = normalized_query
            .split_whitespace()
            .all(|term| term.chars().count() >= 3);
        if normalized_query.chars().count() >= 3 && fts_compatible {
            return self.search_full_text(server, generation, &normalized_query, limit);
        }
        let mut sql = format!(
            "SELECT e.item_id, e.display_name, e.kind, e.breadcrumbs FROM entries e
             WHERE e.server = ? AND e.generation = {generation}"
        );
        let mut values = vec![server.to_string()];
        let pattern = format!("%{}%", escape_like(&normalized_query));
        sql.push_str(
            " AND (e.display_name_norm LIKE ? ESCAPE '\\'
                OR e.item_id_norm LIKE ? ESCAPE '\\'
                OR e.breadcrumbs LIKE ? ESCAPE '\\')",
        );
        values.extend([pattern.clone(), pattern.clone(), pattern]);
        sql.push_str(
            " ORDER BY CASE
                 WHEN e.display_name_norm = ? THEN 0
                 WHEN e.item_id_norm = ? THEN 1
                 WHEN e.display_name_norm LIKE ? ESCAPE '\\' THEN 2
                 WHEN e.item_id_norm LIKE ? ESCAPE '\\' THEN 3
                 WHEN e.display_name_norm LIKE ? ESCAPE '\\' THEN 4
                 WHEN e.item_id_norm LIKE ? ESCAPE '\\' THEN 5
                 ELSE 6 END,
                 length(e.display_name_norm), e.display_name_norm, e.item_id_norm
             LIMIT ",
        );
        values.extend([
            normalized_query.clone(),
            normalized_query.clone(),
            format!("{}%", escape_like(&normalized_query)),
            format!("{}%", escape_like(&normalized_query)),
            format!("%{}%", escape_like(&normalized_query)),
            format!("%{}%", escape_like(&normalized_query)),
        ]);
        sql.push_str(&(limit.saturating_add(1)).to_string());
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values.iter()), |row| {
            let kind = parse_indexed_kind(row.get::<_, i64>(2)?)?;
            let breadcrumbs = parse_indexed_breadcrumbs_at(row.get::<_, String>(3)?, 3)?;
            Ok(IndexedMatch {
                item_id: row.get(0)?,
                display_name: row.get(1)?,
                kind,
                breadcrumbs,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub(super) fn search_prefix(
        &self,
        server: &str,
        generation: u64,
        normalized_query: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let candidate_limit = i64::from(limit.saturating_add(1));
        let upper_bound = prefix_upper_bound(normalized_query);
        let mut candidates: HashMap<String, (SearchCandidate, IndexedMatch)> = HashMap::new();

        for column in ["display_name_norm", "item_id_norm"] {
            let (range, display_prefix_condition, item_prefix_condition, limit_parameter) =
                match upper_bound.as_deref() {
                    Some(_) => (
                        format!("e.{column} >= ?3 AND e.{column} < ?4"),
                        "e.display_name_norm >= ?3 AND e.display_name_norm < ?4".to_string(),
                        "e.item_id_norm >= ?3 AND e.item_id_norm < ?4".to_string(),
                        5,
                    ),
                    None => (
                        format!(
                            "e.{column} >= ?3
                             AND substr(e.{column}, 1, length(?3)) = ?3"
                        ),
                        "substr(e.display_name_norm, 1, length(?3)) = ?3".to_string(),
                        "substr(e.item_id_norm, 1, length(?3)) = ?3".to_string(),
                        4,
                    ),
                };
            let sql = format!(
                "SELECT e.item_id, e.display_name, e.display_name_norm,
                        e.item_id_norm, e.kind, e.breadcrumbs
                 FROM entries e
                 WHERE e.server = ?1 AND e.generation = ?2 AND {range}
                 ORDER BY CASE
                            WHEN e.display_name_norm = ?3 THEN 0
                            WHEN e.item_id_norm = ?3 THEN 1
                            WHEN {display_prefix_condition} THEN 2
                            WHEN {item_prefix_condition} THEN 3
                            ELSE 6
                          END,
                          length(e.display_name_norm), e.display_name_norm,
                          e.item_id_norm, e.item_id
                 LIMIT ?{limit_parameter}"
            );
            let mut statement = self.connection.prepare(&sql)?;
            let row_mapper = |row: &rusqlite::Row<'_>| {
                let item_id = row.get::<_, String>(0)?;
                let display_name = row.get::<_, String>(1)?;
                let display_name_norm = row.get::<_, String>(2)?;
                let item_id_norm = row.get::<_, String>(3)?;
                let kind = parse_indexed_kind(row.get::<_, i64>(4)?)?;
                let breadcrumbs = parse_indexed_breadcrumbs(row.get::<_, String>(5)?)?;
                let candidate = SearchCandidate {
                    rank: SearchRank {
                        tier: search_rank(normalized_query, &display_name_norm, &item_id_norm),
                        display_name_len: display_name_norm.chars().count(),
                        display_name_norm,
                        item_id_norm,
                    },
                    item_id: item_id.clone(),
                };
                Ok((
                    candidate,
                    IndexedMatch {
                        item_id,
                        display_name,
                        kind,
                        breadcrumbs,
                    },
                ))
            };
            let mut query_parameters: Vec<&dyn rusqlite::ToSql> =
                vec![&server, &generation, &normalized_query];
            if let Some(upper_bound) = upper_bound.as_ref() {
                query_parameters.push(upper_bound);
            }
            query_parameters.push(&candidate_limit);
            #[cfg(test)]
            if self.take_prefix_query_map_rejection() {
                query_parameters.truncate(1);
            }
            let rows = statement.query_map(
                rusqlite::params_from_iter(query_parameters.iter().copied()),
                &row_mapper,
            )?;
            for row in rows {
                let (candidate, value) = row?;
                candidates
                    .entry(value.item_id.clone())
                    .or_insert((candidate, value));
            }
        }

        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        candidates.truncate(usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX));
        Ok(candidates.into_iter().map(|(_, value)| value).collect())
    }

    pub(super) fn search_exact(
        &self,
        server: &str,
        generation: u64,
        normalized_query: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let candidate_limit = i64::from(limit.saturating_add(1));
        let candidate_limit_usize = usize::try_from(candidate_limit).unwrap_or(usize::MAX);
        let mut candidates: HashMap<String, (SearchCandidate, IndexedMatch)> = HashMap::new();

        let exact_queries = [
            "SELECT e.item_id, e.display_name, e.display_name_norm,
                    e.item_id_norm, e.kind, e.breadcrumbs
             FROM entries e
             WHERE e.server = ?1 AND e.generation = ?2
               AND e.display_name_norm = ?3
             ORDER BY e.item_id_norm, e.item_id
             LIMIT ?4",
            "SELECT e.item_id, e.display_name, e.display_name_norm,
                    e.item_id_norm, e.kind, e.breadcrumbs
             FROM entries e
             WHERE e.server = ?1 AND e.generation = ?2
               AND e.item_id_norm = ?3
               AND e.display_name_norm <> ?3
             ORDER BY length(e.display_name_norm), e.display_name_norm,
                      e.item_id_norm, e.item_id
             LIMIT ?4",
        ];
        for (query_index, sql) in exact_queries.iter().enumerate() {
            if query_index == 1 && candidates.len() >= candidate_limit_usize {
                break;
            }
            let mut statement = self.connection.prepare(sql)?;
            let query_params = params![server, generation, normalized_query, candidate_limit];
            let row_mapper = |row: &rusqlite::Row<'_>| {
                let item_id = row.get::<_, String>(0)?;
                let display_name = row.get::<_, String>(1)?;
                let display_name_norm = row.get::<_, String>(2)?;
                let item_id_norm = row.get::<_, String>(3)?;
                let kind = parse_indexed_kind(row.get::<_, i64>(4)?)?;
                let breadcrumbs = parse_indexed_breadcrumbs(row.get::<_, String>(5)?)?;
                let candidate = SearchCandidate {
                    rank: SearchRank {
                        tier: search_rank(normalized_query, &display_name_norm, &item_id_norm),
                        display_name_len: display_name_norm.chars().count(),
                        display_name_norm,
                        item_id_norm,
                    },
                    item_id: item_id.clone(),
                };
                Ok((
                    candidate,
                    IndexedMatch {
                        item_id,
                        display_name,
                        kind,
                        breadcrumbs,
                    },
                ))
            };
            let rows = statement.query_map(query_params, row_mapper)?;
            let rows = rows.collect::<Result<Vec<_>, _>>()?;
            for (candidate, value) in rows {
                candidates
                    .entry(value.item_id.clone())
                    .or_insert((candidate, value));
            }
        }

        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        candidates.truncate(usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX));
        Ok(candidates.into_iter().map(|(_, value)| value).collect())
    }

    pub(super) fn search_full_text(
        &self,
        server: &str,
        generation: u64,
        normalized_query: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<IndexedMatch>> {
        let generation = i64::try_from(generation)
            .map_err(|_| anyhow::anyhow!("namespace index generation exceeds SQLite range"))?;
        let fts_query = build_fts_query(normalized_query);
        let mut statement = self.connection.prepare(
            "SELECT item_id, display_name
             FROM entries_fts
             WHERE entries_fts MATCH ?1 AND server = ?2 AND generation = ?3",
        )?;
        let rows = statement.query_map(params![fts_query, server, generation], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let capacity = usize::try_from(limit.saturating_add(1)).unwrap_or(usize::MAX);
        let mut candidates = BinaryHeap::with_capacity(capacity);
        for row in rows {
            let (item_id, display_name) = row?;
            let display_name_norm = normalize_query(&display_name);
            let item_id_norm = normalize_query(&item_id);
            let candidate = SearchCandidate {
                rank: SearchRank {
                    tier: search_rank(normalized_query, &display_name_norm, &item_id_norm),
                    display_name_len: display_name_norm.chars().count(),
                    display_name_norm,
                    item_id_norm,
                },
                item_id,
            };
            if candidates.len() < capacity {
                candidates.push(candidate);
            } else if candidates.peek().is_some_and(|worst| candidate < *worst) {
                candidates.pop();
                candidates.push(candidate);
            }
        }
        drop(statement);

        let mut statement = self.connection.prepare(
            "SELECT display_name, kind, breadcrumbs
             FROM entries
             WHERE server = ?1 AND generation = ?2 AND item_id = ?3",
        )?;
        let mut matches = Vec::with_capacity(candidates.len());
        for candidate in candidates.into_sorted_vec() {
            let result =
                statement.query_row(params![server, generation, candidate.item_id], |row| {
                    let kind = parse_indexed_kind(row.get::<_, i64>(1)?)?;
                    let breadcrumbs = parse_indexed_breadcrumbs(row.get::<_, String>(2)?)?;
                    Ok(IndexedMatch {
                        item_id: candidate.item_id.clone(),
                        display_name: row.get(0)?,
                        kind,
                        breadcrumbs,
                    })
                });
            match result {
                Ok(value) => matches.push(value),
                Err(rusqlite::Error::QueryReturnedNoRows) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(matches)
    }
}

pub(super) fn search_rank(query: &str, display_name_norm: &str, item_id_norm: &str) -> u8 {
    if display_name_norm == query {
        0
    } else if item_id_norm == query {
        1
    } else if display_name_norm.starts_with(query) {
        2
    } else if item_id_norm.starts_with(query) {
        3
    } else if display_name_norm.contains(query) {
        4
    } else if item_id_norm.contains(query) {
        5
    } else {
        6
    }
}

pub(super) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    for index in (0..chars.len()).rev() {
        let value = chars[index] as u32;
        let next = match value {
            0x10ffff => None,
            0xd7ff => char::from_u32(0xe000),
            _ => char::from_u32(value + 1),
        };
        if let Some(next) = next {
            chars[index] = next;
            chars.truncate(index + 1);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

pub(super) fn build_fts_query(query: &str) -> String {
    query
        .split_whitespace()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub(super) fn parse_indexed_kind(value: i64) -> rusqlite::Result<InventoryNodeKind> {
    match value {
        1 => Ok(InventoryNodeKind::Item),
        2 => Ok(InventoryNodeKind::BranchAndItem),
        value => Err(rusqlite::Error::InvalidParameterName(format!(
            "unknown indexed node kind {value}"
        ))),
    }
}

pub(super) fn parse_indexed_breadcrumbs(value: String) -> rusqlite::Result<Vec<String>> {
    parse_indexed_breadcrumbs_at(value, 2)
}

fn parse_indexed_breadcrumbs_at(
    value: String,
    column_index: usize,
) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column_index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub(super) fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
