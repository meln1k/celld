// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The scripted cell host; `sim_cell_host_tests.rs` describes it.

use crate::engine_api::{HttpResponse, RuntimeFetch, StopMode};
use crate::ltx_repl::{LtxConfig, LtxRepl};
use crate::replication::ActivationOptions;
use anyhow::{anyhow, Context as _};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// One node's scripted cell host. Clones share the node.
#[derive(Clone)]
pub struct SimCellHost {
    inner: Arc<HostInner>,
}

struct HostInner {
    data_dir: PathBuf,
    vfs: String,
    ltx: Arc<LtxRepl>,
    cells: Mutex<BTreeMap<String, Resident>>,
}

/// A started cell: its open database, and whether requests may reach it.
struct Resident {
    epoch: u64,
    published: bool,
    connection: Arc<Mutex<rusqlite::Connection>>,
}

impl SimCellHost {
    /// Start a node's host over `store` and `disk`, keeping cells under
    /// `data_dir`. Runs in the node's domain, which the replicator's loops
    /// belong to.
    pub fn start(
        data_dir: &Path,
        store: &crate::conformance_sim_store::SimStore,
        disk: &crate::conformance_sim_store::SimDisk,
    ) -> anyhow::Result<Self> {
        crate::asyncrt::fs()
            .create_dir_all(data_dir)
            .with_context(|| format!("create {}", data_dir.display()))?;
        let ltx = LtxRepl::start_with_store_on_vfs(
            data_dir,
            Arc::new(store.clone()),
            None,
            0,
            disk.vfs_name(),
            LtxConfig {
                // A paged restore needs a chain far larger than any
                // scripted cell writes.
                paged_restore: false,
                ..LtxConfig::from_env()
            },
        );
        Ok(Self {
            inner: Arc::new(HostInner {
                data_dir: data_dir.to_path_buf(),
                vfs: disk.vfs_name().to_string(),
                ltx: Arc::new(ltx),
                cells: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.inner
            .data_dir
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    fn published(&self, cell: &str) -> anyhow::Result<Arc<Mutex<rusqlite::Connection>>> {
        let cells = self.inner.cells.lock().unwrap();
        cells
            .get(cell)
            .filter(|resident| resident.published)
            .map(|resident| resident.connection.clone())
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))
    }

    /// Remove the runtime of `cell` at `epoch`, closing its database.
    fn take_runtime(&self, cell: &str, epoch: u64) {
        let mut cells = self.inner.cells.lock().unwrap();
        if cells
            .get(cell)
            .is_some_and(|resident| resident.epoch == epoch)
        {
            cells.remove(cell);
        }
    }

    pub(crate) async fn fetch_cell_for_world(
        &self,
        cell: String,
        _name: Option<String>,
        request: crate::engine_api::RuntimeFetch,
        _cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<crate::engine_api::HttpResponse> {
        let connection = self.published(&cell)?;
        let connection = connection.lock().unwrap();
        let RuntimeFetch { method, body, .. } = request;
        let (body, write_position) = match method.as_str() {
            "POST" => {
                let value = String::from_utf8_lossy(body.bytes()).into_owned();
                connection.execute("INSERT INTO sim_log(v) VALUES (?1)", [&value])?;
                (b"appended".to_vec(), Some(write_position(&connection)?))
            }
            "GET" => {
                let values: Vec<String> = connection
                    .prepare("SELECT v FROM sim_log ORDER BY rowid")?
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<_, _>>()?;
                (values.join(",").into_bytes(), None)
            }
            other => anyhow::bail!("the scripted cell has no {other} handler"),
        };
        Ok(HttpResponse {
            status: 200,
            body,
            stream: None,
            headers: Vec::new(),
            websocket: None,
            write_position,
            observed_position: None,
        })
    }

    pub(crate) fn local_reload_cells(&self) -> anyhow::Result<Vec<celld_logic::LocalCell>> {
        Ok(self.inner.ltx.local_cells())
    }

    pub(crate) async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        let activated = self
            .inner
            .ltx
            .activate_with(
                ActivationOptions {
                    cell,
                    epoch: spec.epoch,
                    fresh: spec.fresh,
                    took_over: spec.took_over,
                    resume_local: spec.resume_local,
                    prior: spec.prior.clone(),
                },
                false,
            )
            .await?;
        let expected = self.db_path(cell, spec.epoch);
        anyhow::ensure!(
            activated.path == expected,
            "replication restored {} instead of {}",
            activated.path.display(),
            expected.display()
        );
        Ok(celld_logic::RestoreOutcome {
            restored: activated.restored,
            alarm: None,
        })
    }

    pub(crate) async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        _fresh: bool,
    ) -> anyhow::Result<celld_logic::isolate::HeapId> {
        let path = self.db_path(&cell, epoch);
        let connection = rusqlite::Connection::open_with_flags_and_vfs(
            &path,
            rusqlite::OpenFlags::default(),
            &self.inner.vfs,
        )
        .with_context(|| format!("open {}", path.display()))?;
        crate::storage::schema(&connection)?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS sim_log(v TEXT NOT NULL)")?;
        let mut cells = self.inner.cells.lock().unwrap();
        anyhow::ensure!(
            !cells.contains_key(&cell),
            "cell runtime already exists: {cell}"
        );
        cells.insert(
            cell,
            Resident {
                epoch,
                published: false,
                connection: Arc::new(Mutex::new(connection)),
            },
        );
        // One isolate holds every scripted cell.
        Ok(celld_logic::isolate::HeapId::new(1))
    }

    pub(crate) async fn swap_out_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.take_runtime(cell, epoch);
        Ok(())
    }

    pub(crate) fn publish_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let mut cells = self.inner.cells.lock().unwrap();
        match cells.get_mut(cell) {
            Some(resident) if resident.epoch == epoch && !resident.published => {
                resident.published = true;
                Ok(())
            }
            _ => Err(anyhow!("no started cell runtime for {cell} epoch {epoch}")),
        }
    }

    pub(crate) async fn ensure_durable(
        &self,
        cell: &str,
        epoch: u64,
        revocable: bool,
    ) -> anyhow::Result<()> {
        self.inner.ltx.handoff_gate(cell, epoch, revocable).await
    }

    pub(crate) async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        self.inner.ltx.await_durable(cell, epoch, position).await
    }

    pub(crate) async fn stop_cell(
        &self,
        cell: &str,
        epoch: u64,
        mode: StopMode,
    ) -> anyhow::Result<()> {
        self.take_runtime(cell, epoch);
        let ltx = &self.inner.ltx;
        match mode {
            StopMode::Evict {
                preserve_local,
                abandon,
            } => ltx
                .evict(cell, epoch, preserve_local, abandon.as_deref())
                .await
                .map(|_| ()),
            StopMode::Rebase => ltx.release(cell, epoch).await,
            StopMode::CloseInPlace => ltx.close_in_place(cell, epoch).await,
            StopMode::Discard => {
                ltx.discard(cell, epoch);
                Ok(())
            }
        }
    }

    pub(crate) fn with_alarm<T>(
        &self,
        _cell: &str,
        f: impl FnOnce(celld_logic::wake::AlarmSnapshot, bool) -> T,
    ) -> T {
        f(celld_logic::wake::AlarmSnapshot::without_wake(None), true)
    }

    pub(crate) fn observes_activity_alarms(&self) -> bool {
        false
    }

    pub(crate) async fn fire_alarm(
        &self,
        _op: celld_logic::OpId,
        cell: String,
        _scheduled_ms: i64,
    ) -> anyhow::Result<(celld_logic::wake::AlarmSnapshot, bool, Option<u64>)> {
        Err(anyhow!("the scripted host runs no alarms: {cell}"))
    }
}

/// The committed-write position of `connection`, as the engine samples it:
/// every change this connection made, plus every commit another connection
/// made, plus the schema version.
fn write_position(connection: &rusqlite::Connection) -> anyhow::Result<u64> {
    let sample = |sql: &str| connection.query_row(sql, [], |row| row.get::<_, i64>(0));
    let position = sample("SELECT total_changes()")?
        + sample("PRAGMA data_version")?
        + sample("PRAGMA schema_version")?;
    Ok(position as u64)
}
