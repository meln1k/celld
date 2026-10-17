// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! One simulated node's local disk.
//!
//! A `SimDisk` is an in-memory filesystem that both halves of a node's
//! storage use: the node's code through `celld_ltx::FileSystem`, installed
//! as the domain's `asyncrt::fs()`, and SQLite through a VFS of the disk's
//! own, so the replicator reads exactly the WAL that SQLite wrote.
//!
//! The disk keeps what a real one keeps across failures:
//!
//! - A file's bytes are durable from its last sync (`sync_all`, `xSync`).
//!   Names are durable at once: a file created and never synced comes back
//!   empty, as ext4 leaves it.
//! - [`SimDisk::crash`] is a process death. Nothing on the disk changes,
//!   and every file the dead process had open goes inert: its writes,
//!   locks and shared memory fail, so a closing connection cannot
//!   checkpoint or delete its WAL on the way out.
//! - [`SimDisk::power_loss`] is a crash that also reverts every file to its
//!   durable bytes.
//!
//! The wal-index (`-shm`) lives in memory beside its database and is
//! dropped when no connection maps it, so the next connection recovers it
//! from the WAL, as after a reboot.

use celld_ltx::{FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata};
use rusqlite::ffi;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_void, CStr, CString};
use std::io;
use std::os::raw::{c_char, c_int};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// The handle of one simulated disk. Clones share the disk.
#[derive(Clone)]
pub struct SimDisk {
    inner: Arc<DiskInner>,
}

struct DiskInner {
    vfs_name: String,
    vfs: *mut ffi::sqlite3_vfs,
    /// Bumped by every crash. A handle opened before it is inert.
    incarnation: Arc<AtomicU64>,
    state: Mutex<DiskState>,
}

// SAFETY: `vfs` is a registration SQLite reads, never written after it is
// built; the rest is behind the mutex.
unsafe impl Send for DiskInner {}
unsafe impl Sync for DiskInner {}

#[derive(Default)]
struct DiskState {
    files: BTreeMap<PathBuf, Arc<Node>>,
    dirs: BTreeSet<PathBuf>,
    next_handle: u64,
}

/// One file, open or not. Handles hold it, so an unlinked file lives on
/// for whoever still has it open.
#[derive(Default)]
struct Node {
    content: Mutex<Content>,
    /// The wal-index of a database, while some connection maps it.
    shm: Mutex<Option<Arc<Shm>>>,
}

#[derive(Default)]
struct Content {
    data: Vec<u8>,
    durable: Vec<u8>,
    modified_ms: i64,
    /// The SQLite lock level of each handle holding one.
    locks: BTreeMap<u64, c_int>,
}

/// The shared memory SQLite keeps beside a WAL database.
#[derive(Default)]
struct Shm {
    regions: Mutex<Vec<Box<[u8]>>>,
    /// Per lock slot: the handles holding it shared, and the one holding it
    /// exclusive.
    locks: Mutex<[(BTreeSet<u64>, Option<u64>); ffi::SQLITE_SHM_NLOCK as usize]>,
    mappers: Mutex<BTreeSet<u64>>,
}

fn now_ms() -> i64 {
    if crate::asyncrt::sim::current_domain().is_some() {
        crate::asyncrt::wall_ms()
    } else {
        crate::asyncrt::sim::WALL_EPOCH_MS
    }
}

fn normalize(path: &Path) -> PathBuf {
    path.components()
        .filter(|component| !matches!(component, Component::CurDir))
        .collect()
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("{}: no such file or directory", path.display()),
    )
}

impl Default for SimDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl SimDisk {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let vfs_name = format!("celld-sim-disk-{}", NEXT.fetch_add(1, Ordering::Relaxed));
        let inner = Arc::new_cyclic(|weak: &Weak<DiskInner>| {
            let app = Box::into_raw(Box::new(weak.clone()));
            let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
                iVersion: 2,
                szOsFile: std::mem::size_of::<DiskFile>() as c_int,
                mxPathname: 1024,
                pNext: std::ptr::null_mut(),
                zName: CString::new(vfs_name.clone()).unwrap().into_raw(),
                pAppData: app as *mut c_void,
                xOpen: Some(x_open),
                xDelete: Some(x_delete),
                xAccess: Some(x_access),
                xFullPathname: Some(x_full_pathname),
                xDlOpen: None,
                xDlError: None,
                xDlSym: None,
                xDlClose: None,
                xRandomness: Some(x_randomness),
                xSleep: Some(x_sleep),
                xCurrentTime: Some(x_current_time),
                xGetLastError: Some(x_get_last_error),
                xCurrentTimeInt64: Some(x_current_time_int64),
                xSetSystemCall: None,
                xGetSystemCall: None,
                xNextSystemCall: None,
            }));
            let mut state = DiskState::default();
            state.dirs.insert(PathBuf::from("/"));
            DiskInner {
                vfs_name,
                vfs,
                incarnation: Arc::new(AtomicU64::new(0)),
                state: Mutex::new(state),
            }
        });
        // SAFETY: the registration and its name are leaked, so they outlive
        // every connection that found the VFS through them.
        let rc = unsafe { ffi::sqlite3_vfs_register(inner.vfs, 0) };
        assert_eq!(rc, ffi::SQLITE_OK, "register the simulated disk's VFS");
        Self { inner }
    }

    /// The SQLite VFS that reads and writes this disk.
    pub fn vfs_name(&self) -> &str {
        &self.inner.vfs_name
    }

    /// The disk as a domain's `asyncrt::fs()`.
    pub fn filesystem(&self) -> Arc<dyn FileSystem> {
        Arc::new(self.clone())
    }

    /// The process using the disk died: what it had open goes inert.
    pub fn crash(&self) {
        self.inner.incarnation.fetch_add(1, Ordering::SeqCst);
        let state = self.inner.state.lock().unwrap();
        for node in state.files.values() {
            node.content.lock().unwrap().locks.clear();
            node.shm.lock().unwrap().take();
        }
    }

    /// The machine lost power: a crash, and every file reverts to what its
    /// last sync made durable.
    pub fn power_loss(&self) {
        self.crash();
        let state = self.inner.state.lock().unwrap();
        for node in state.files.values() {
            let mut content = node.content.lock().unwrap();
            content.data = content.durable.clone();
        }
    }

    /// Every file on the disk with its current length, for a trace.
    pub fn listing(&self) -> Vec<(PathBuf, usize)> {
        let state = self.inner.state.lock().unwrap();
        state
            .files
            .iter()
            .map(|(path, node)| (path.clone(), node.content.lock().unwrap().data.len()))
            .collect()
    }

    fn node(&self, path: &Path) -> io::Result<Arc<Node>> {
        let path = normalize(path);
        let state = self.inner.state.lock().unwrap();
        state
            .files
            .get(&path)
            .cloned()
            .ok_or_else(|| not_found(&path))
    }

    /// The file at `path`, created empty (or truncated) when `truncate`,
    /// else only when absent.
    fn create_node(&self, path: &Path, truncate: bool) -> io::Result<Arc<Node>> {
        let path = normalize(path);
        let mut state = self.inner.state.lock().unwrap();
        let parent = path.parent().ok_or_else(|| not_found(&path))?;
        if !state.dirs.contains(parent) {
            return Err(not_found(parent));
        }
        if state.dirs.contains(&path) {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("{} is a directory", path.display()),
            ));
        }
        let node = state.files.entry(path).or_default().clone();
        drop(state);
        let mut content = node.content.lock().unwrap();
        if truncate {
            content.data.clear();
            content.modified_ms = now_ms();
        }
        drop(content);
        Ok(node)
    }

    fn next_handle(&self) -> u64 {
        let mut state = self.inner.state.lock().unwrap();
        state.next_handle += 1;
        state.next_handle
    }

    fn incarnation(&self) -> u64 {
        self.inner.incarnation.load(Ordering::SeqCst)
    }
}

impl Drop for DiskInner {
    fn drop(&mut self) {
        // The registration itself stays allocated: a connection can still
        // hold it, and closing that connection reads it.
        unsafe {
            ffi::sqlite3_vfs_unregister(self.vfs);
        }
    }
}

// ── the node filesystem ─────────────────────────────────────────────────────

/// An open file through the filesystem interface: a cursor over its node.
struct DiskHostFile {
    node: Arc<Node>,
    cursor: u64,
    writable: bool,
    incarnation: u64,
    disk_incarnation: Arc<AtomicU64>,
}

impl DiskHostFile {
    fn live(&self) -> io::Result<()> {
        if self.disk_incarnation.load(Ordering::SeqCst) != self.incarnation {
            return Err(io::Error::other("the process that opened this file died"));
        }
        Ok(())
    }
}

impl HostFileIo for DiskHostFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.live()?;
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the file is open read-only",
            ));
        }
        let mut content = self.node.content.lock().unwrap();
        write_at(&mut content.data, self.cursor, bytes);
        content.modified_ms = now_ms();
        self.cursor += bytes.len() as u64;
        Ok(())
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.live()?;
        let content = self.node.content.lock().unwrap();
        let start = offset as usize;
        let end = start
            .checked_add(len)
            .filter(|end| *end <= content.data.len());
        match end {
            Some(end) => Ok(content.data[start..end].to_vec()),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past the end of the file",
            )),
        }
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.live()?;
        let mut content = self.node.content.lock().unwrap();
        content.durable = content.data.clone();
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.live()?;
        Ok(self.node.content.lock().unwrap().data.len() as u64)
    }
}

fn write_at(data: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
    let start = offset as usize;
    let end = start + bytes.len();
    if data.len() < end {
        data.resize(end, 0);
    }
    data[start..end].copy_from_slice(bytes);
}

impl SimDisk {
    fn host_file(&self, node: Arc<Node>, writable: bool) -> HostFile {
        HostFile::from_io(DiskHostFile {
            node,
            cursor: 0,
            writable,
            incarnation: self.incarnation(),
            disk_incarnation: self.inner.incarnation.clone(),
        })
    }
}

impl FileSystem for SimDisk {
    fn temporary_file(&self, _directory: Option<&Path>) -> io::Result<HostFile> {
        Ok(self.host_file(Arc::default(), true))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        Ok(self.node(path)?.content.lock().unwrap().data.clone())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        let path = normalize(path);
        let state = self.inner.state.lock().unwrap();
        if !state.dirs.contains(&path) {
            return Err(not_found(&path));
        }
        let child = |candidate: &Path| candidate.parent() == Some(path.as_path());
        let mut entries: Vec<HostDirEntry> = state
            .dirs
            .iter()
            .filter(|dir| child(dir))
            .map(|dir| (dir, true))
            .chain(
                state
                    .files
                    .keys()
                    .filter(|file| child(file))
                    .map(|file| (file, false)),
            )
            .map(|(entry, is_dir)| HostDirEntry {
                path: entry.clone(),
                file_name: entry.file_name().unwrap_or_default().to_owned(),
                is_dir,
            })
            .collect();
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        Ok(entries)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        let path = normalize(path);
        let state = self.inner.state.lock().unwrap();
        if state.dirs.contains(&path) {
            return Ok(HostMetadata {
                len: 0,
                is_dir: true,
                is_file: false,
                modified_unix_millis: 0,
            });
        }
        let node = state.files.get(&path).ok_or_else(|| not_found(&path))?;
        let content = node.content.lock().unwrap();
        Ok(HostMetadata {
            len: content.data.len() as u64,
            is_dir: false,
            is_file: true,
            modified_unix_millis: content.modified_ms,
        })
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        let node = self.create_node(path, true)?;
        Ok(self.host_file(node, true))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        let node = self.node(path)?;
        Ok(self.host_file(node, false))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let node = self.create_node(path, true)?;
        let mut content = node.content.lock().unwrap();
        content.data = bytes.to_vec();
        content.modified_ms = now_ms();
        Ok(())
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        let path = normalize(path);
        let state = self.inner.state.lock().unwrap();
        if state.dirs.contains(&path) {
            // Names are durable already.
            return Ok(());
        }
        let node = state.files.get(&path).ok_or_else(|| not_found(&path))?;
        let mut content = node.content.lock().unwrap();
        content.durable = content.data.clone();
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (from, to) = (normalize(from), normalize(to));
        let mut state = self.inner.state.lock().unwrap();
        let to_parent = to.parent().ok_or_else(|| not_found(&to))?;
        if !state.dirs.contains(to_parent) {
            return Err(not_found(to_parent));
        }
        if let Some(node) = state.files.remove(&from) {
            state.files.insert(to, node);
            return Ok(());
        }
        if !state.dirs.contains(&from) {
            return Err(not_found(&from));
        }
        let moved = |path: &Path| to.join(path.strip_prefix(&from).unwrap());
        let dirs: Vec<PathBuf> = state
            .dirs
            .iter()
            .filter(|dir| dir.starts_with(&from))
            .cloned()
            .collect();
        for dir in dirs {
            state.dirs.remove(&dir);
            state.dirs.insert(moved(&dir));
        }
        let files: Vec<PathBuf> = state
            .files
            .keys()
            .filter(|file| file.starts_with(&from))
            .cloned()
            .collect();
        for file in files {
            let node = state.files.remove(&file).unwrap();
            state.files.insert(moved(&file), node);
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let path = normalize(path);
        let mut state = self.inner.state.lock().unwrap();
        state
            .files
            .remove(&path)
            .map(|_| ())
            .ok_or_else(|| not_found(&path))
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let path = normalize(path);
        let mut state = self.inner.state.lock().unwrap();
        if !state.dirs.contains(&path) {
            return Err(not_found(&path));
        }
        state.dirs.retain(|dir| !dir.starts_with(&path));
        state.files.retain(|file, _| !file.starts_with(&path));
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let path = normalize(path);
        let mut state = self.inner.state.lock().unwrap();
        for ancestor in path.ancestors() {
            if state.files.contains_key(ancestor) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{} is a file", ancestor.display()),
                ));
            }
            state.dirs.insert(ancestor.to_path_buf());
        }
        Ok(())
    }
}

// ── the SQLite VFS ──────────────────────────────────────────────────────────

/// Our `sqlite3_file`. SQLite reads `methods`; the handle is ours.
#[repr(C)]
struct DiskFile {
    methods: *const ffi::sqlite3_io_methods,
    handle: *mut Handle,
}

/// One SQLite file handle on the disk.
struct Handle {
    id: u64,
    node: Arc<Node>,
    /// Removed from the disk when the handle closes.
    delete_on_close: Option<(Weak<DiskInner>, PathBuf)>,
    readonly: bool,
    incarnation: u64,
    disk_incarnation: Arc<AtomicU64>,
    /// The wal-index this handle mapped, kept alive while SQLite may hold
    /// pointers into it.
    shm: Option<Arc<Shm>>,
}

impl Handle {
    fn live(&self) -> bool {
        self.disk_incarnation.load(Ordering::SeqCst) == self.incarnation
    }
}

unsafe fn disk_of(vfs: *mut ffi::sqlite3_vfs) -> Option<SimDisk> {
    let weak = &*((*vfs).pAppData as *const Weak<DiskInner>);
    weak.upgrade().map(|inner| SimDisk { inner })
}

unsafe fn handle<'a>(file: *mut ffi::sqlite3_file) -> &'a mut Handle {
    &mut *(*(file as *mut DiskFile)).handle
}

unsafe fn path_of(name: *const c_char) -> PathBuf {
    PathBuf::from(CStr::from_ptr(name).to_string_lossy().into_owned())
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    // SQLite calls xClose only on a file whose xOpen succeeded, and reads
    // `methods` to find it, so a failure leaves it null.
    (*(file as *mut DiskFile)).methods = std::ptr::null();
    let Some(disk) = disk_of(vfs) else {
        return ffi::SQLITE_CANTOPEN;
    };
    let create = flags & ffi::SQLITE_OPEN_CREATE != 0;
    let exclusive = flags & ffi::SQLITE_OPEN_EXCLUSIVE != 0;
    let (node, path) = if name.is_null() {
        (Arc::<Node>::default(), None)
    } else {
        let path = normalize(&path_of(name));
        let node = match (disk.node(&path), create) {
            (Ok(_), true) if exclusive => return ffi::SQLITE_CANTOPEN,
            (Ok(node), _) => node,
            (Err(_), true) => match disk.create_node(&path, false) {
                Ok(node) => node,
                Err(_) => return ffi::SQLITE_CANTOPEN,
            },
            (Err(_), false) => return ffi::SQLITE_CANTOPEN,
        };
        (node, Some(path))
    };
    let delete_on_close = (flags & ffi::SQLITE_OPEN_DELETEONCLOSE != 0)
        .then(|| path.map(|path| (Arc::downgrade(&disk.inner), path)))
        .flatten();
    let handle = Box::new(Handle {
        id: disk.next_handle(),
        node,
        delete_on_close,
        readonly: flags & ffi::SQLITE_OPEN_READONLY != 0,
        incarnation: disk.incarnation(),
        disk_incarnation: disk.inner.incarnation.clone(),
        shm: None,
    });
    let disk_file = file as *mut DiskFile;
    (*disk_file).handle = Box::into_raw(handle);
    (*disk_file).methods = &DISK_IO_METHODS;
    if !out_flags.is_null() {
        *out_flags = flags;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _sync: c_int,
) -> c_int {
    let Some(disk) = disk_of(vfs) else {
        return ffi::SQLITE_IOERR_DELETE;
    };
    match disk.remove_file(&path_of(name)) {
        Ok(()) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_IOERR_DELETE_NOENT,
    }
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _flags: c_int,
    out: *mut c_int,
) -> c_int {
    let exists = disk_of(vfs).is_some_and(|disk| disk.metadata(&path_of(name)).is_ok());
    *out = c_int::from(exists);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    n: c_int,
    out: *mut c_char,
) -> c_int {
    let bytes = CStr::from_ptr(name).to_bytes_with_nul();
    if bytes.len() > n as usize {
        return ffi::SQLITE_CANTOPEN;
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, out, bytes.len());
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_randomness(
    _vfs: *mut ffi::sqlite3_vfs,
    n: c_int,
    out: *mut c_char,
) -> c_int {
    std::ptr::write_bytes(out, 0, n as usize);
    n
}

unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    micros
}

unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    *out = 2_440_587.5 + now_ms() as f64 / 86_400_000.0;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_current_time_int64(
    _vfs: *mut ffi::sqlite3_vfs,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
    *out = 210_866_760_000_000 + now_ms();
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_get_last_error(
    _vfs: *mut ffi::sqlite3_vfs,
    _n: c_int,
    _out: *mut c_char,
) -> c_int {
    0
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let disk_file = file as *mut DiskFile;
    let mut handle = Box::from_raw((*disk_file).handle);
    (*disk_file).handle = std::ptr::null_mut();
    handle.node.content.lock().unwrap().locks.remove(&handle.id);
    release_shm(&mut handle);
    if let Some((disk, path)) = handle.delete_on_close.take() {
        if let Some(inner) = disk.upgrade() {
            let _ = SimDisk { inner }.remove_file(&path);
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    let handle = handle(file);
    if !handle.live() {
        return ffi::SQLITE_IOERR_READ;
    }
    let content = handle.node.content.lock().unwrap();
    let out = std::slice::from_raw_parts_mut(buf as *mut u8, amt as usize);
    let start = (ofst as usize).min(content.data.len());
    let available = (content.data.len() - start).min(out.len());
    out[..available].copy_from_slice(&content.data[start..start + available]);
    if available < out.len() {
        out[available..].fill(0);
        return ffi::SQLITE_IOERR_SHORT_READ;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    let handle = handle(file);
    if !handle.live() || handle.readonly {
        return ffi::SQLITE_IOERR_WRITE;
    }
    let bytes = std::slice::from_raw_parts(buf as *const u8, amt as usize);
    let mut content = handle.node.content.lock().unwrap();
    write_at(&mut content.data, ofst as u64, bytes);
    content.modified_ms = now_ms();
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    let handle = handle(file);
    if !handle.live() || handle.readonly {
        return ffi::SQLITE_IOERR_TRUNCATE;
    }
    let mut content = handle.node.content.lock().unwrap();
    content.data.resize(size as usize, 0);
    content.modified_ms = now_ms();
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    let handle = handle(file);
    if !handle.live() {
        return ffi::SQLITE_IOERR_FSYNC;
    }
    let mut content = handle.node.content.lock().unwrap();
    content.durable = content.data.clone();
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut ffi::sqlite3_int64,
) -> c_int {
    let handle = handle(file);
    if !handle.live() {
        return ffi::SQLITE_IOERR_FSTAT;
    }
    *size = handle.node.content.lock().unwrap().data.len() as ffi::sqlite3_int64;
    ffi::SQLITE_OK
}

/// POSIX-style SQLite file locks between the handles of one file.
unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let handle = handle(file);
    if !handle.live() {
        return ffi::SQLITE_IOERR_LOCK;
    }
    let mut content = handle.node.content.lock().unwrap();
    let current = content
        .locks
        .get(&handle.id)
        .copied()
        .unwrap_or(ffi::SQLITE_LOCK_NONE);
    if current >= level {
        return ffi::SQLITE_OK;
    }
    let others = content.locks.iter().filter(|(id, _)| **id != handle.id);
    let highest_other = others
        .clone()
        .map(|(_, level)| *level)
        .max()
        .unwrap_or(ffi::SQLITE_LOCK_NONE);
    let other_readers = others
        .filter(|(_, level)| **level >= ffi::SQLITE_LOCK_SHARED)
        .count();
    let granted = match level {
        ffi::SQLITE_LOCK_SHARED => highest_other < ffi::SQLITE_LOCK_PENDING,
        ffi::SQLITE_LOCK_RESERVED | ffi::SQLITE_LOCK_PENDING => {
            highest_other <= ffi::SQLITE_LOCK_SHARED
        }
        ffi::SQLITE_LOCK_EXCLUSIVE => {
            if highest_other > ffi::SQLITE_LOCK_SHARED {
                false
            } else if other_readers > 0 {
                // Hold PENDING so no new reader starts while this one waits.
                content.locks.insert(handle.id, ffi::SQLITE_LOCK_PENDING);
                return ffi::SQLITE_BUSY;
            } else {
                true
            }
        }
        _ => false,
    };
    if !granted {
        return ffi::SQLITE_BUSY;
    }
    content.locks.insert(handle.id, level);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let handle = handle(file);
    let mut content = handle.node.content.lock().unwrap();
    if level == ffi::SQLITE_LOCK_NONE {
        content.locks.remove(&handle.id);
    } else if let Some(current) = content.locks.get_mut(&handle.id) {
        *current = (*current).min(level);
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    let handle = handle(file);
    if !handle.live() {
        return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
    }
    let content = handle.node.content.lock().unwrap();
    let reserved = content
        .locks
        .iter()
        .any(|(id, level)| *id != handle.id && *level >= ffi::SQLITE_LOCK_RESERVED);
    *out = c_int::from(reserved);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(
    _file: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(_file: *mut ffi::sqlite3_file) -> c_int {
    4096
}

unsafe extern "C" fn x_device_characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let handle = handle(file);
    *out = std::ptr::null_mut();
    if !handle.live() {
        return ffi::SQLITE_IOERR_SHMMAP;
    }
    let shm = match &handle.shm {
        Some(shm) => shm.clone(),
        None => {
            let shm = handle
                .node
                .shm
                .lock()
                .unwrap()
                .get_or_insert_with(Arc::default)
                .clone();
            shm.mappers.lock().unwrap().insert(handle.id);
            handle.shm = Some(shm.clone());
            shm
        }
    };
    let mut regions = shm.regions.lock().unwrap();
    let region = region as usize;
    if region >= regions.len() {
        if extend == 0 {
            return ffi::SQLITE_OK;
        }
        while regions.len() <= region {
            regions.push(vec![0u8; size as usize].into_boxed_slice());
        }
    }
    *out = regions[region].as_mut_ptr() as *mut c_void;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    let handle = handle(file);
    let Some(shm) = &handle.shm else {
        return ffi::SQLITE_IOERR_SHMLOCK;
    };
    let mut locks = shm.locks.lock().unwrap();
    let slots = offset as usize..(offset + n) as usize;
    let id = handle.id;
    if flags & ffi::SQLITE_SHM_UNLOCK != 0 {
        for (shared, exclusive) in &mut locks[slots] {
            shared.remove(&id);
            if *exclusive == Some(id) {
                *exclusive = None;
            }
        }
        return ffi::SQLITE_OK;
    }
    if !handle.live() {
        return ffi::SQLITE_IOERR_SHMLOCK;
    }
    if flags & ffi::SQLITE_SHM_SHARED != 0 {
        if locks[slots.clone()]
            .iter()
            .any(|(_, exclusive)| exclusive.is_some_and(|holder| holder != id))
        {
            return ffi::SQLITE_BUSY;
        }
        for (shared, _) in &mut locks[slots] {
            shared.insert(id);
        }
    } else {
        if locks[slots.clone()].iter().any(|(shared, exclusive)| {
            exclusive.is_some_and(|holder| holder != id)
                || shared.iter().any(|holder| *holder != id)
        }) {
            return ffi::SQLITE_BUSY;
        }
        for (_, exclusive) in &mut locks[slots] {
            *exclusive = Some(id);
        }
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_barrier(_file: *mut ffi::sqlite3_file) {
    std::sync::atomic::fence(Ordering::SeqCst);
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, _delete: c_int) -> c_int {
    release_shm(handle(file));
    ffi::SQLITE_OK
}

/// Drop `handle`'s mapping and its shm locks; the last mapper drops the
/// wal-index itself.
fn release_shm(handle: &mut Handle) {
    let Some(shm) = handle.shm.take() else {
        return;
    };
    for (shared, exclusive) in shm.locks.lock().unwrap().iter_mut() {
        shared.remove(&handle.id);
        if *exclusive == Some(handle.id) {
            *exclusive = None;
        }
    }
    let mut mappers = shm.mappers.lock().unwrap();
    mappers.remove(&handle.id);
    if mappers.is_empty() {
        let mut current = handle.node.shm.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &shm))
        {
            *current = None;
        }
    }
}

static DISK_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
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
