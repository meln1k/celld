// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The SQLite fault VFS, `crate::fault`.
//
// A thin wrapper over SQLite's default VFS. Every method forwards, except
// `xOpen`, which labels the file with the connection role in force on the
// opening thread, and `xWrite`, which fails with `SQLITE_IOERR_WRITE` while
// the write fault is armed. A role names who opened a connection, so a test
// can tell the cell's own connection from celld's replication connections.

use rusqlite::ffi;
use std::cell::RefCell;
use std::ffi::{c_void, CString};
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// The name the fault VFS registers under.
pub const VFS_NAME: &str = "celld-fault";

/// The role of a connection opened outside [`with_connection_role`].
pub const DEFAULT_ROLE: &str = "cell";

static WRITE_FAULT: AtomicBool = AtomicBool::new(false);

thread_local! {
    static ROLE: RefCell<&'static str> = const { RefCell::new(DEFAULT_ROLE) };
}

/// Arm or disarm the write fault: while armed, every write through the fault
/// VFS fails.
pub fn set_write_fault(enabled: bool) {
    WRITE_FAULT.store(enabled, Ordering::SeqCst);
}

/// Whether the write fault is armed.
pub fn write_fault() -> bool {
    WRITE_FAULT.load(Ordering::SeqCst)
}

/// Run `open` with `role` as the role of every file it opens.
pub fn with_connection_role<T>(role: &'static str, open: impl FnOnce() -> T) -> T {
    let previous = ROLE.with(|current| current.replace(role));
    struct Restore(&'static str);
    impl Drop for Restore {
        fn drop(&mut self) {
            ROLE.with(|current| *current.borrow_mut() = self.0);
        }
    }
    let _restore = Restore(previous);
    open()
}

/// Open `path` through the fault VFS.
pub fn open_database(path: &str) -> anyhow::Result<rusqlite::Connection> {
    register();
    Ok(rusqlite::Connection::open_with_flags_and_vfs(
        path,
        rusqlite::OpenFlags::default(),
        VFS_NAME,
    )?)
}

/// Register the fault VFS once per process; later calls find it.
pub fn register() {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| unsafe {
        let base = ffi::sqlite3_vfs_find(std::ptr::null());
        assert!(!base.is_null(), "no default sqlite VFS");
        let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
            iVersion: 2,
            szOsFile: std::mem::size_of::<FaultFile>() as c_int,
            mxPathname: (*base).mxPathname,
            pNext: std::ptr::null_mut(),
            zName: CString::new(VFS_NAME).unwrap().into_raw(),
            pAppData: base as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: (*base).xDlOpen,
            xDlError: (*base).xDlError,
            xDlSym: (*base).xDlSym,
            xDlClose: (*base).xDlClose,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: None,
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        let rc = ffi::sqlite3_vfs_register(vfs, 0);
        assert_eq!(rc, ffi::SQLITE_OK, "register the fault VFS");
    });
}

/// Our `sqlite3_file`: SQLite reads `methods`; `base` is the default VFS's
/// file and `role` the role it was opened under.
#[repr(C)]
struct FaultFile {
    methods: *const ffi::sqlite3_io_methods,
    base: *mut ffi::sqlite3_file,
    role: &'static str,
}

unsafe fn base_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    (*vfs).pAppData as *mut ffi::sqlite3_vfs
}

unsafe fn base_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    (*(file as *mut FaultFile)).base
}

unsafe fn base_methods(file: *mut ffi::sqlite3_file) -> *const ffi::sqlite3_io_methods {
    (*base_file(file)).pMethods
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let base = base_vfs(vfs);
    let inner = ffi::sqlite3_malloc((*base).szOsFile) as *mut ffi::sqlite3_file;
    if inner.is_null() {
        return ffi::SQLITE_NOMEM;
    }
    std::ptr::write_bytes(inner as *mut u8, 0, (*base).szOsFile as usize);
    let rc = ((*base).xOpen.unwrap())(base, name, inner, flags, out_flags);
    if rc != ffi::SQLITE_OK {
        ffi::sqlite3_free(inner as *mut c_void);
        return rc;
    }
    let fault = file as *mut FaultFile;
    (*fault).methods = &FAULT_IO_METHODS;
    (*fault).base = inner;
    (*fault).role = ROLE.with(|role| *role.borrow());
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    if write_fault() {
        return ffi::SQLITE_IOERR_WRITE;
    }
    ((*base_methods(file)).xWrite.unwrap())(base_file(file), buf, amt, ofst)
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let inner = base_file(file);
    let rc = match (*base_methods(file)).xClose {
        Some(close) => close(inner),
        None => ffi::SQLITE_OK,
    };
    ffi::sqlite3_free(inner as *mut c_void);
    rc
}

macro_rules! forward_io {
    ($name:ident ( $($arg:ident : $ty:ty),* ) $(-> $ret:ty)? => $field:ident) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file $(, $arg: $ty)*) $(-> $ret)? {
            ((*base_methods(file)).$field.unwrap())(base_file(file) $(, $arg)*)
        }
    };
}

forward_io!(x_read(buf: *mut c_void, amt: c_int, ofst: ffi::sqlite3_int64) -> c_int => xRead);
forward_io!(x_truncate(size: ffi::sqlite3_int64) -> c_int => xTruncate);
forward_io!(x_sync(flags: c_int) -> c_int => xSync);
forward_io!(x_file_size(size: *mut ffi::sqlite3_int64) -> c_int => xFileSize);
forward_io!(x_lock(lock: c_int) -> c_int => xLock);
forward_io!(x_unlock(lock: c_int) -> c_int => xUnlock);
forward_io!(x_check_reserved_lock(out: *mut c_int) -> c_int => xCheckReservedLock);
forward_io!(x_file_control(op: c_int, arg: *mut c_void) -> c_int => xFileControl);
forward_io!(x_sector_size() -> c_int => xSectorSize);
forward_io!(x_device_characteristics() -> c_int => xDeviceCharacteristics);
forward_io!(x_shm_map(pg: c_int, sz: c_int, ext: c_int, out: *mut *mut c_void) -> c_int => xShmMap);
forward_io!(x_shm_lock(ofst: c_int, n: c_int, flags: c_int) -> c_int => xShmLock);
forward_io!(x_shm_unmap(delete: c_int) -> c_int => xShmUnmap);

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    if let Some(barrier) = (*base_methods(file)).xShmBarrier {
        barrier(base_file(file));
    }
}

static FAULT_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

macro_rules! forward_vfs {
    ($name:ident ( $($arg:ident : $ty:ty),* ) $(-> $ret:ty)? => $field:ident) => {
        unsafe extern "C" fn $name(vfs: *mut ffi::sqlite3_vfs $(, $arg: $ty)*) $(-> $ret)? {
            let base = base_vfs(vfs);
            ((*base).$field.unwrap())(base $(, $arg)*)
        }
    };
}

forward_vfs!(x_delete(name: *const c_char, sync: c_int) -> c_int => xDelete);
forward_vfs!(x_access(name: *const c_char, flags: c_int, out: *mut c_int) -> c_int => xAccess);
forward_vfs!(x_full_pathname(name: *const c_char, n: c_int, out: *mut c_char) -> c_int => xFullPathname);
forward_vfs!(x_randomness(n: c_int, out: *mut c_char) -> c_int => xRandomness);
forward_vfs!(x_sleep(micros: c_int) -> c_int => xSleep);
forward_vfs!(x_current_time(out: *mut f64) -> c_int => xCurrentTime);
forward_vfs!(x_get_last_error(n: c_int, out: *mut c_char) -> c_int => xGetLastError);
