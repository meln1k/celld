// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Tests of the simulated store and disk, included into `sim_store_tests.rs`.

use super::*;
use crate::asyncrt::sim::{Simulation, WALL_EPOCH_MS};
use celld_ltx::FileSystem as _;
use object_store::{PutMode, UpdateVersion};

fn put_mode(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..Default::default()
    }
}

/// Create and update are compare-and-swap on the tag, the clock is the
/// simulation's, and a listing groups by `/`.
#[test]
fn the_store_swaps_on_tags_and_stamps_virtual_time() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let store = SimStore::new(StoreFaults {
        latency_ms: 10..11,
        ..Default::default()
    });
    let client = store.clone();
    sim.block_on(&node, async move {
        let store = client;
        let key = Path::from("cells/a/own.json");
        let first = store
            .put_opts(&key, "1".into(), put_mode(PutMode::Create))
            .await
            .unwrap();
        let again = store
            .put_opts(&key, "2".into(), put_mode(PutMode::Create))
            .await;
        assert!(
            matches!(again, Err(object_store::Error::AlreadyExists { .. })),
            "{again:?}"
        );

        let stale = UpdateVersion {
            e_tag: Some("999".into()),
            version: None,
        };
        let lost = store
            .put_opts(&key, "3".into(), put_mode(PutMode::Update(stale)))
            .await;
        assert!(
            matches!(lost, Err(object_store::Error::Precondition { .. })),
            "{lost:?}"
        );

        let current = UpdateVersion {
            e_tag: first.e_tag.clone(),
            version: None,
        };
        let second = store
            .put_opts(&key, "4".into(), put_mode(PutMode::Update(current)))
            .await
            .unwrap();
        assert_ne!(second.e_tag, first.e_tag);

        let meta = store.head(&key).await.unwrap();
        // Two failed writes and two applied ones, 10 ms each, then half a
        // head: the store stamped the second applied write at 35 ms.
        assert_eq!(meta.last_modified.timestamp_millis(), WALL_EPOCH_MS + 35);
        assert_eq!(store.get(&key).await.unwrap().bytes().await.unwrap(), "4");

        store
            .put(&Path::from("cells/b/ltx/e1/x"), "x".into())
            .await
            .unwrap();
        let listed = store
            .list_with_delimiter(Some(&Path::from("cells")))
            .await
            .unwrap();
        let prefixes: Vec<String> = listed
            .common_prefixes
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert_eq!(prefixes, ["cells/a", "cells/b"]);
    });
}

/// An unavailable request changes nothing; a lost response changes the
/// store all the same; a partitioned domain reaches nothing.
#[test]
fn a_failed_request_may_or_may_not_have_applied() {
    let sim = Simulation::new(1);
    let node = sim.domain("n0");
    let store = SimStore::default();
    let client = store.clone();
    sim.block_on(&node, async move {
        let store = client;
        store.set_faults(StoreFaults {
            unavailable_per_mille: 1_000,
            ..Default::default()
        });
        assert!(store
            .put(&Path::from("unavailable"), "x".into())
            .await
            .is_err());
        assert_eq!(store.object("unavailable"), None);

        store.set_faults(StoreFaults {
            lost_response_per_mille: 1_000,
            ..Default::default()
        });
        assert!(store.put(&Path::from("lost"), "x".into()).await.is_err());
        assert_eq!(store.object("lost").as_deref(), Some(&b"x"[..]));

        store.set_faults(StoreFaults::default());
        store.set_partitioned("n0", true);
        assert!(store
            .put(&Path::from("partitioned"), "x".into())
            .await
            .is_err());
        store.set_partitioned("n0", false);
        store.put(&Path::from("healed"), "x".into()).await.unwrap();
    });
    let mutations: Vec<(String, bool)> = store
        .mutations()
        .into_iter()
        .map(|mutation| (mutation.path, mutation.answered))
        .collect();
    assert_eq!(mutations, [("lost".into(), false), ("healed".into(), true)]);
}

fn rows(connection: &rusqlite::Connection) -> Vec<i64> {
    connection
        .prepare("SELECT v FROM t ORDER BY v")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn open(disk: &SimDisk, path: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_with_flags_and_vfs(
        path,
        rusqlite::OpenFlags::default(),
        disk.vfs_name(),
    )
    .unwrap();
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    connection
}

/// SQLite runs a WAL database on the disk, and the filesystem side reads
/// the very WAL it wrote.
#[test]
fn sqlite_and_the_filesystem_share_the_disk() {
    let disk = SimDisk::new();
    disk.create_dir_all(std::path::Path::new("/node/cells"))
        .unwrap();
    let connection = open(&disk, "/node/cells/db.sqlite");
    connection
        .pragma_update(None, "wal_autocheckpoint", 0)
        .unwrap();
    connection
        .execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1), (2);")
        .unwrap();
    let wal = disk
        .read(std::path::Path::new("/node/cells/db.sqlite-wal"))
        .unwrap();
    assert_eq!(&wal[..4], &[0x37, 0x7f, 0x06, 0x82], "a WAL header");
    let reader = open(&disk, "/node/cells/db.sqlite");
    assert_eq!(rows(&reader), [1, 2]);
    let names: Vec<_> = disk
        .read_dir(std::path::Path::new("/node/cells"))
        .unwrap()
        .into_iter()
        .map(|entry| entry.file_name.into_string().unwrap())
        .collect();
    assert_eq!(names, ["db.sqlite", "db.sqlite-wal"]);
}

/// A process crash keeps every write; a power loss keeps only what was
/// synced. With `synchronous=NORMAL` a WAL commit is not synced, so the
/// rows a power loss takes are exactly the ones after the last sync.
#[test]
fn a_power_loss_takes_what_was_never_synced() {
    for power_loss in [false, true] {
        let disk = SimDisk::new();
        disk.create_dir_all(std::path::Path::new("/node")).unwrap();
        let connection = open(&disk, "/node/db.sqlite");
        connection
            .pragma_update(None, "synchronous", "FULL")
            .unwrap();
        connection
            .execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .unwrap();
        connection.execute("INSERT INTO t VALUES (2)", []).unwrap();

        if power_loss {
            disk.power_loss();
        } else {
            disk.crash();
        }
        // The dead process cannot write, and closing it cannot checkpoint.
        assert!(connection.execute("INSERT INTO t VALUES (3)", []).is_err());
        drop(connection);

        let reopened = open(&disk, "/node/db.sqlite");
        let expected: &[i64] = if power_loss { &[1] } else { &[1, 2] };
        assert_eq!(rows(&reopened), expected, "power loss {power_loss}");
    }
}
