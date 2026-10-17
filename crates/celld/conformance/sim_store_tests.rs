// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The simulated object store and node disk, `crate::conformance_sim_store`.
//
// `SimStore` is the bucket every node of a simulated fleet shares: an
// in-memory object store with compare-and-swap writes, stamped by the
// simulation's clock, whose requests take seeded time and can fail. A
// request that fails before the store applies it changed nothing; one whose
// response is lost changed the store, and the caller cannot tell the two
// apart, as with a real bucket. A partitioned domain reaches nothing.
//
// `SimDisk` is one node's local disk, in `disk.rs`.

#[path = "sim_store/disk.rs"]
mod disk;

pub use disk::SimDisk;

use crate::asyncrt;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt as _};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use rand::Rng as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// How the store's requests behave. Each request draws its latency and its
/// fate from the calling domain's `sim-store` stream.
#[derive(Clone, Debug)]
pub struct StoreFaults {
    /// The round trip of one request, drawn uniformly from this range and
    /// spent half before the store applies it and half after.
    pub latency_ms: std::ops::Range<u64>,
    /// Requests per thousand that fail before the store sees them.
    pub unavailable_per_mille: u32,
    /// Requests per thousand that the store applies and whose response is
    /// then lost.
    pub lost_response_per_mille: u32,
}

impl Default for StoreFaults {
    fn default() -> Self {
        Self {
            latency_ms: 5..30,
            unavailable_per_mille: 0,
            lost_response_per_mille: 0,
        }
    }
}

/// One change the store applied, in the order it applied them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mutation {
    pub at_ms: i64,
    pub domain: String,
    pub op: &'static str,
    pub path: String,
    /// The new object's tag, or `None` for a delete.
    pub e_tag: Option<String>,
    /// Whether the caller heard back.
    pub answered: bool,
}

/// The shared simulated bucket.
#[derive(Clone, Debug, Default)]
pub struct SimStore {
    inner: Arc<Mutex<StoreState>>,
}

#[derive(Debug, Default)]
struct StoreState {
    next_etag: u64,
    objects: BTreeMap<Path, Object>,
    faults: StoreFaults,
    partitioned: BTreeSet<String>,
    mutations: Vec<Mutation>,
}

#[derive(Clone, Debug)]
struct Object {
    data: Bytes,
    e_tag: u64,
    last_modified_ms: i64,
    attributes: Attributes,
}

impl Object {
    fn meta(&self, location: &Path) -> ObjectMeta {
        ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::from_timestamp_millis(self.last_modified_ms)
                .expect("a simulated time is a valid timestamp"),
            size: self.data.len() as u64,
            e_tag: Some(self.e_tag.to_string()),
            version: None,
        }
    }
}

/// Who changes the store, and when: the request being applied.
struct Stamp {
    at_ms: i64,
    domain: String,
    op: &'static str,
}

/// What a request does once it reaches the store.
type Apply<T> = Box<dyn FnOnce(&mut StoreState, &Stamp) -> object_store::Result<T> + Send>;

fn unreachable_error(op: &str, why: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "SimStore",
        source: format!("{op}: {why}").into(),
    }
}

impl std::fmt::Display for SimStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SimStore")
    }
}

impl SimStore {
    pub fn new(faults: StoreFaults) -> Self {
        let store = Self::default();
        store.set_faults(faults);
        store
    }

    pub fn set_faults(&self, faults: StoreFaults) {
        self.inner.lock().unwrap().faults = faults;
    }

    /// Cut `domain` off from the store, or reconnect it.
    pub fn set_partitioned(&self, domain: &str, partitioned: bool) {
        let mut state = self.inner.lock().unwrap();
        if partitioned {
            state.partitioned.insert(domain.to_string());
        } else {
            state.partitioned.remove(domain);
        }
    }

    /// Every change the store applied, oldest first.
    pub fn mutations(&self) -> Vec<Mutation> {
        self.inner.lock().unwrap().mutations.clone()
    }

    /// The object at `key`, read directly, outside any request.
    pub fn object(&self, key: &str) -> Option<Bytes> {
        let state = self.inner.lock().unwrap();
        state
            .objects
            .get(&Path::from(key))
            .map(|object| object.data.clone())
    }

    /// Every key under `prefix`, read directly, outside any request.
    pub fn keys(&self, prefix: &str) -> Vec<String> {
        let state = self.inner.lock().unwrap();
        state
            .objects
            .keys()
            .map(|path| path.to_string())
            .filter(|key| key.starts_with(prefix))
            .collect()
    }

    /// One request from the current domain: travel, apply, travel back.
    async fn request<T: Send + 'static>(
        &self,
        op: &'static str,
        apply: Apply<T>,
    ) -> object_store::Result<T> {
        let domain = asyncrt::sim::current_domain().expect("a store request runs in a domain");
        let (latency, unavailable, lost) = {
            let faults = self.inner.lock().unwrap().faults.clone();
            let mut rng = asyncrt::rng("sim-store");
            let latency = if faults.latency_ms.is_empty() {
                faults.latency_ms.start
            } else {
                rng.gen_range(faults.latency_ms.clone())
            };
            let fate = rng.gen_range(0..1_000);
            let unavailable = fate < faults.unavailable_per_mille;
            let lost = !unavailable
                && fate < faults.unavailable_per_mille + faults.lost_response_per_mille;
            (latency, unavailable, lost)
        };
        asyncrt::sleep(std::time::Duration::from_millis(latency / 2)).await;
        let (applied, first_mutation) = {
            let mut state = self.inner.lock().unwrap();
            if unavailable || state.partitioned.contains(&domain) {
                return Err(unreachable_error(op, "the store was unreachable"));
            }
            let first_mutation = state.mutations.len();
            let stamp = Stamp {
                at_ms: asyncrt::sim::world_wall_ms(),
                domain: domain.clone(),
                op,
            };
            (apply(&mut state, &stamp), first_mutation)
        };
        asyncrt::sleep(std::time::Duration::from_millis(latency - latency / 2)).await;
        let mut state = self.inner.lock().unwrap();
        if lost || state.partitioned.contains(&domain) {
            return Err(unreachable_error(op, "the response was lost"));
        }
        for mutation in &mut state.mutations[first_mutation..] {
            mutation.answered = true;
        }
        applied
    }
}

impl StoreState {
    fn get(&self, location: &Path) -> object_store::Result<&Object> {
        self.objects
            .get(location)
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: "no such object".into(),
            })
    }

    fn record(&mut self, stamp: &Stamp, location: &Path, e_tag: Option<u64>) {
        self.mutations.push(Mutation {
            at_ms: stamp.at_ms,
            domain: stamp.domain.clone(),
            op: stamp.op,
            path: location.to_string(),
            e_tag: e_tag.map(|e_tag| e_tag.to_string()),
            answered: false,
        });
    }

    fn put(&mut self, location: &Path, data: Bytes, attributes: Attributes, stamp: &Stamp) -> u64 {
        self.next_etag += 1;
        let e_tag = self.next_etag;
        self.objects.insert(
            location.clone(),
            Object {
                data,
                e_tag,
                last_modified_ms: stamp.at_ms,
                attributes,
            },
        );
        self.record(stamp, location, Some(e_tag));
        e_tag
    }

    fn put_opts(
        &mut self,
        location: &Path,
        data: Bytes,
        opts: PutOptions,
        stamp: &Stamp,
    ) -> object_store::Result<PutResult> {
        match (&opts.mode, self.objects.get(location)) {
            (PutMode::Overwrite, _) => {}
            (PutMode::Create, Some(_)) => {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "the object exists".into(),
                })
            }
            (PutMode::Create, None) => {}
            (PutMode::Update(version), existing) => {
                let current = existing.map(|object| object.e_tag.to_string());
                if version.e_tag.is_none() || current != version.e_tag {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: format!("{current:?} does not match {:?}", version.e_tag).into(),
                    });
                }
            }
        }
        let e_tag = self.put(location, data, opts.attributes, stamp);
        Ok(PutResult {
            e_tag: Some(e_tag.to_string()),
            version: None,
        })
    }

    fn list(&self, prefix: Option<&Path>) -> Vec<ObjectMeta> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        self.objects
            .iter()
            .filter(|(key, _)| {
                key.prefix_match(prefix)
                    .is_some_and(|mut rest| rest.next().is_some())
            })
            .map(|(key, object)| object.meta(key))
            .collect()
    }

    fn list_with_delimiter(&self, prefix: Option<&Path>) -> ListResult {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for (key, object) in &self.objects {
            let Some(mut rest) = key.prefix_match(prefix) else {
                continue;
            };
            let Some(first) = rest.next() else {
                continue;
            };
            if rest.next().is_some() {
                common_prefixes.insert(prefix.child(first));
            } else {
                objects.push(object.meta(key));
            }
        }
        ListResult {
            objects,
            common_prefixes: common_prefixes.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let location = location.clone();
        let data = Bytes::from(payload);
        self.request(
            "put",
            Box::new(move |state, stamp| state.put_opts(&location, data, opts, stamp)),
        )
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(SimUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: opts.attributes,
            parts: Vec::new(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let location = location.clone();
        self.request(
            "get",
            Box::new(move |state, _| {
                let object = state.get(&location)?;
                let meta = object.meta(&location);
                options.check_preconditions(&meta)?;
                let range = match &options.range {
                    Some(range) => range.as_range(object.data.len() as u64).map_err(|error| {
                        object_store::Error::Generic {
                            store: "SimStore",
                            source: error.to_string().into(),
                        }
                    })?,
                    None => 0..object.data.len() as u64,
                };
                let data = if options.head {
                    Bytes::new()
                } else {
                    object.data.slice(range.start as usize..range.end as usize)
                };
                Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        futures_util::stream::once(std::future::ready(Ok(data))).boxed(),
                    ),
                    attributes: object.attributes.clone(),
                    meta,
                    range,
                })
            }),
        )
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let location = location.clone();
        self.request(
            "delete",
            Box::new(move |state, stamp| {
                if state.objects.remove(&location).is_some() {
                    state.record(stamp, &location, None);
                }
                Ok(())
            }),
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        futures_util::stream::once(async move {
            store
                .request(
                    "list",
                    Box::new(move |state, _| Ok(state.list(prefix.as_ref()))),
                )
                .await
        })
        .flat_map(|listed| match listed {
            Ok(objects) => futures_util::stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => futures_util::stream::once(std::future::ready(Err(error))).boxed(),
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned();
        self.request(
            "list",
            Box::new(move |state, _| Ok(state.list_with_delimiter(prefix.as_ref()))),
        )
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.request(
            "copy",
            Box::new(move |state, stamp| {
                let object = state.get(&from)?.clone();
                state.put(&to, object.data, object.attributes, stamp);
                Ok(())
            }),
        )
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.request(
            "copy",
            Box::new(move |state, stamp| {
                let object = state.get(&from)?.clone();
                if state.objects.contains_key(&to) {
                    return Err(object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: "the object exists".into(),
                    });
                }
                state.put(&to, object.data, object.attributes, stamp);
                Ok(())
            }),
        )
        .await
    }
}

/// A multipart upload: parts stay with the client until `complete` puts
/// them as one object in one request.
#[derive(Debug)]
struct SimUpload {
    store: SimStore,
    location: Path,
    attributes: Attributes,
    parts: Vec<PutPayload>,
}

#[async_trait]
impl MultipartUpload for SimUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        self.parts.push(payload);
        Box::pin(std::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut data = Vec::new();
        for part in self.parts.drain(..) {
            for chunk in part.iter() {
                data.extend_from_slice(chunk);
            }
        }
        let location = self.location.clone();
        let attributes = std::mem::take(&mut self.attributes);
        self.store
            .request(
                "put",
                Box::new(move |state, stamp| {
                    let e_tag = state.put(&location, data.into(), attributes, stamp);
                    Ok(PutResult {
                        e_tag: Some(e_tag.to_string()),
                        version: None,
                    })
                }),
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        Ok(())
    }
}

/// A bucket over `store` with every key under `prefix`, the way a node
/// configures one from `CELLD_BUCKET`.
pub fn bucket(store: &SimStore, prefix: &str) -> crate::bucket::Bucket {
    let store: Arc<dyn ObjectStore> = Arc::new(store.clone());
    crate::bucket::Bucket::with_stores(
        store.clone(),
        store,
        crate::bucket::StorageBackend::S3,
        "sim".to_string(),
        prefix.to_string(),
    )
}

#[cfg(test)]
mod tests {
    include!("sim_store/tests.rs");
}
