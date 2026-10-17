// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// In-crate storage tests, included into `storage.rs`.

/// The fault VFS fails writes only while the fault is armed, and the
/// database it leaves behind holds exactly the writes that succeeded.
#[test]
fn the_write_fault_fails_writes_until_disarmed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.db");
    let path = path.to_str().unwrap();
    let connection = crate::fault::open_database(path).unwrap();
    connection
        .execute_batch("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();

    crate::fault::set_write_fault(true);
    let faulted = connection.execute("INSERT INTO t VALUES (2)", []);
    crate::fault::set_write_fault(false);
    assert!(faulted.is_err(), "a write under the fault succeeded");

    connection.execute("INSERT INTO t VALUES (3)", []).unwrap();
    drop(connection);
    let reopened = rusqlite::Connection::open(path).unwrap();
    let values: Vec<i64> = reopened
        .prepare("SELECT v FROM t ORDER BY v")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(values, vec![1, 3]);
}
