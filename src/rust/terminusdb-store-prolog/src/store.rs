use crate::builder::*;
use crate::layer::*;
use crate::named_graph::*;
use std::io::{self, Cursor};
use std::path::PathBuf;
use swipl::prelude::*;
use terminus_store::storage::archive::DirectoryArchiveBackend;
use terminus_store::storage::archive::LruArchiveBackend;
use terminus_store::storage::CachedLayerStore;
use terminus_store::storage::LockingHashMapLayerCache;
use terminus_store::storage::{
    archive::ArchiveLayerStore, name_to_string, pack_layer_parents, string_to_name, PackError,
};
use terminus_store::store::{sync::*, Store};

use terminusdb_grpc_labelstore_client::GrpcLabelStore;

/// Get the current process resident set size (RSS) in bytes.
/// Returns None if the value cannot be determined on this platform.
#[cfg(target_os = "macos")]
fn get_process_rss_bytes() -> Option<usize> {
    #[repr(C)]
    struct MachTaskBasicInfo {
        suspend_count: i32,
        virtual_size: u64,
        resident_size: u64,
        user_time: [u32; 2],
        system_time: [u32; 2],
        policy: i32,
    }

    const MACH_TASK_BASIC_INFO: u32 = 20;
    const MACH_TASK_BASIC_INFO_COUNT: u32 =
        (std::mem::size_of::<MachTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;

    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            target_task: u32,
            flavor: u32,
            task_info_out: *mut MachTaskBasicInfo,
            task_info_count: *mut u32,
        ) -> i32;
    }

    unsafe {
        let mut info: MachTaskBasicInfo = std::mem::zeroed();
        let mut count = MACH_TASK_BASIC_INFO_COUNT;
        let result = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info,
            &mut count,
        );
        if result == 0 {
            Some(info.resident_size as usize)
        } else {
            None
        }
    }
}

/// Get the current process resident set size (RSS) in bytes.
/// Returns None if the value cannot be determined on this platform.
#[cfg(target_os = "linux")]
fn get_process_rss_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let kb_str = line.split_whitespace().nth(1)?;
            let kb: usize = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Get the current process resident set size (RSS) in bytes.
/// Returns None on unsupported platforms.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_process_rss_bytes() -> Option<usize> {
    None
}

/// Build the object-backed `SyncStore` shared by the two object-store openers.
///
/// `bucket` is the atom `memory` for an in-process bucket, or an S3 bucket
/// name. In the S3 case the
/// builder reads credentials, region and any endpoint override from the
/// environment, so no secret ever passes through a Prolog term.
///
/// Conditional PUT is set to ETag matching because the label store implements
/// its compare-and-swap with it; without that, concurrent head updates would
/// silently clobber one another.
fn open_object_store_impl<C: QueryableContextType>(
    context: &Context<C>,
    bucket_term: &Term,
    prefix_term: &Term,
    cache_size_term: &Term,
) -> PrologResult<SyncStore> {
    use terminus_store::object_store::{
        aws::{AmazonS3Builder, S3ConditionalPut},
        memory::InMemory,
        ObjectStore,
    };

    let prefix: PrologText = prefix_term.get_ex()?;
    let cache_size: usize = cache_size_term.get_ex::<u64>()? as usize;

    let bucket: std::sync::Arc<dyn ObjectStore> = if attempt(bucket_term.unify(atom!("memory")))? {
        std::sync::Arc::new(InMemory::new())
    } else {
        let bucket_name: PrologText = bucket_term.get_ex()?;
        // A `file://` local-directory backend is deliberately not offered.
        // `object_store`'s LocalFileSystem implements create-if-absent but not
        // the ETag-conditional update the label store's compare-and-swap needs,
        // so it can create a graph and then never accept a second commit.
        // Relaxing the CAS to accommodate it would give up the protection
        // against lost head updates, which is not a trade worth making. Use
        // `memory` for a single process, or MinIO/S3 across processes.
        let s3 = context.try_or_die_generic(
            AmazonS3Builder::from_env()
                .with_bucket_name(&*bucket_name)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .build(),
        )?;
        std::sync::Arc::new(s3)
    };

    Ok(SyncStore::wrap(terminus_store::open_object_store(
        bucket, &*prefix, cache_size,
    )))
}

predicates! {
    pub semidet fn open_memory_store(_context, term) {
        let store = open_sync_memory_store();
        term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    pub semidet fn open_directory_store(_context, dir_term, out_term) {
        let dir: PrologText = dir_term.get_ex()?;
        let store = open_sync_directory_store(&*dir);
        out_term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    pub semidet fn open_raw_archive_store(_context, dir_term, out_term) {
        let dir: PrologText = dir_term.get_ex()?;
        let store = open_sync_raw_archive_store(&*dir);
        out_term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    pub semidet fn open_archive_store(_context, dir_term, cache_size_term, out_term) {
        let dir: PrologText = dir_term.get_ex()?;
        let cache_size: usize = cache_size_term.get_ex::<u64>()? as usize;
        let store = open_sync_archive_store(&*dir, cache_size);
        out_term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    /// open_object_store(+Bucket, +Prefix, +CacheSize, -Store)
    ///
    /// Layers are materialized as usual; only the backing storage differs.
    /// `Bucket` is the atom `memory` (an in-process bucket, for tests) or an S3
    /// bucket name, in which case credentials and region come from the standard
    /// AWS environment variables -- they are never passed through Prolog.
    pub semidet fn open_object_store(context, bucket_term, prefix_term, cache_size_term, out_term) {
        let store = open_object_store_impl(context, bucket_term, prefix_term, cache_size_term)?;
        out_term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    /// open_diskless_object_store(+Bucket, +Prefix, +CacheSize, -Store)
    ///
    /// As `open_object_store/4`, but layers read from this store are *disk-less*:
    /// queries fetch only the blocks they touch via ranged GETs, and no whole
    /// layer is ever materialized.
    ///
    /// Reads are covered; writes are not. A builder opened on a disk-less layer
    /// raises an error rather than silently taking a slower path, so a write
    /// workload should open the same bucket with `open_object_store/4`.
    pub semidet fn open_diskless_object_store(context, bucket_term, prefix_term, cache_size_term, out_term) {
        let store = open_object_store_impl(context, bucket_term, prefix_term, cache_size_term)?;
        out_term.unify(&WrappedStore(ReadStore::diskless(store)))
    }

    /// store_diskless(+Store, -DisklessStore)
    ///
    /// A disk-less *view* of an already-open store: same bucket, same labels,
    /// same layers, but layers reached through it are read block-lazily instead
    /// of being materialized.
    ///
    /// This is the useful shape in practice. A process typically wants both --
    /// writes and history maintenance on the materialized handle, queries on the
    /// disk-less one -- and opening the bucket twice would give two independent
    /// caches (and, for an in-process bucket, two unrelated stores entirely).
    pub semidet fn store_diskless(_context, store_term, out_term) {
        let store: WrappedStore = store_term.get_ex()?;
        out_term.unify(&WrappedStore(ReadStore::diskless((**store).clone())))
    }

    /// store_materialized(+Store, -MaterializedStore)
    ///
    /// The inverse of `store_diskless/2`: a view of the same store whose layers
    /// are materialized. Lets a writer be derived from a disk-less reader.
    pub semidet fn store_materialized(_context, store_term, out_term) {
        let store: WrappedStore = store_term.get_ex()?;
        out_term.unify(&WrappedStore(ReadStore::materialized((**store).clone())))
    }

    pub semidet fn open_grpc_store(context, dir_term, address_term, initial_pool_term, cache_size_term, out_term) {
        let dir: PrologText = dir_term.get_ex()?;
        let address: PrologText = address_term.get_ex()?;
        let pool_size: u64 = initial_pool_term.get_ex()?;
        let cache_size: usize = cache_size_term.get_ex::<u64>()? as usize;
        let directory_layer_backend = DirectoryArchiveBackend::new((&*dir).into());
        let layer_backend = LruArchiveBackend::new(directory_layer_backend.clone(), directory_layer_backend, cache_size);
        let lru_ref = layer_backend.clone();
        let lru_evict_ref = layer_backend.clone();
        let layer_store = CachedLayerStore::new(ArchiveLayerStore::new(layer_backend.clone(), layer_backend), LockingHashMapLayerCache::new());

        let label_store = context.try_or_die_generic(task_sync(GrpcLabelStore::new(address.to_string(), pool_size as usize)))?;

        let store = SyncStore::wrap(Store::new(label_store, layer_store)
            .with_lru_used_bytes(move || lru_ref.used_bytes())
            .with_lru_evict(move |fraction| lru_evict_ref.evict_to_target(fraction)));

        out_term.unify(&WrappedStore(ReadStore::materialized(store)))
    }

    pub semidet fn open_write(context, store_or_graph_or_layer_term, builder_term) {
        let builder;
        if let Some(store) = attempt_opt(store_or_graph_or_layer_term.get::<WrappedStore>())? {
            builder = context.try_or_die(store.create_base_layer())?;
        }
        else if let Some(graph) = attempt_opt(store_or_graph_or_layer_term.get::<WrappedNamedGraph>())? {
            if let Some(layer) = context.try_or_die(graph.head())? {
                builder = context.try_or_die(layer.open_write())?;
            }
            else {
                return context.raise_exception(&term!{context: error(cannot_open_named_graph_without_base_layer, _)}?);
            }
        }
        else {
            let layer: WrappedLayer = store_or_graph_or_layer_term.get_ex()?;
            builder = context.try_or_die(layer.open_write())?;
        }

        builder_term.unify(WrappedBuilder(builder))
    }

    pub semidet fn pack_export(context, store_term, layer_ids_term, pack_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let layer_id_strings_list: Vec<String> = layer_ids_term.get_ex()?;
        let mut layer_ids_list = Vec::with_capacity(layer_id_strings_list.len());
        for layer_id_string in layer_id_strings_list {
            let layer_id = context.try_or_die(string_to_name(&layer_id_string))?;
            layer_ids_list.push(layer_id);
        }

        let result = context.try_or_die(store.export_layers(
            Box::new(layer_ids_list.into_iter())))?;

        pack_term.unify(result.as_slice())
    }

    pub semidet fn pack_layerids_and_parents(context, pack_term, layer_parents_term) {
        let pack: Vec<u8> = pack_term.get_ex()?;
        let layer_parent_map = context.try_or_die(pack_layer_parents(Cursor::new(pack))
                                                  .map_err(|e| {
                                                      // todo we're mapping to io error here for ease but should be something better
                                                      match e {
                                                          PackError::Io(e) => e,
                                                          PackError::LayerNotFound => io::Error::new(io::ErrorKind::NotFound, "a layer from the pack was not found"),
                                                          PackError::Utf8Error(e) => io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e))
                                                      }
                                                  }))?;

        let pair_functor = Functor::new("-", 2);
        let none_atom = Atom::new("none");
        let some_functor = Functor::new("some", 1);

        let mut result_terms = Vec::with_capacity(layer_parent_map.len());
        for (layer, parent) in layer_parent_map {
            let term = context.new_term_ref();
            term.unify(pair_functor)?;
            term.unify_arg(1, name_to_string(layer))?;
            match parent {
                Some(parent) => {
                    let parent_term = context.new_term_ref();
                    parent_term.unify(some_functor)?;
                    parent_term.unify_arg(1, name_to_string(parent))?;
                    term.unify_arg(2, &parent_term)?;
                },
                None => {
                    term.unify_arg(2, &none_atom)?;
                }
            }

            result_terms.push(term);
        }

        layer_parents_term.unify(result_terms.as_slice())
    }

    pub semidet fn pack_import(context, store_term, layer_ids_term, pack_term) {
        let store: WrappedStore = store_term.get_ex()?;

        let layer_id_strings: Vec<String> = layer_ids_term.get_ex()?;
        let mut layer_ids = Vec::with_capacity(layer_id_strings.len());
        for layer_id_string in layer_id_strings {
            let name = context.try_or_die(string_to_name(&layer_id_string))?;
            layer_ids.push(name);
        }

        let pack: Vec<u8> = pack_term.get_ex()?;

        context.try_or_die(store.import_layers(pack.as_slice(), Box::new(layer_ids.into_iter())))
    }

    pub semidet fn merge_base_layers(context, store_term, temp_dir_term, layer_ids_term, output_id_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let temp_dir: PrologText = temp_dir_term.get_ex()?;
        let temp_dir_path: PathBuf = (&*temp_dir).into();

        let layer_id_strings: Vec<String> = layer_ids_term.get_ex()?;
        let mut layer_ids = Vec::with_capacity(layer_id_strings.len());
        for layer_id_string in layer_id_strings {
            let name = context.try_or_die(string_to_name(&layer_id_string))?;
            layer_ids.push(name);
        }

        let result = context.try_or_die(store.merge_base_layers(&layer_ids, &temp_dir_path))?;
        let result_string = name_to_string(result);

        output_id_term.unify(result_string)
    }

    /// start_compaction(+Store, +MaxDepth, +IntervalSeconds)
    ///
    /// Start a background task that rolls up (never squashes) any label head
    /// whose effective layer stack exceeds MaxDepth, checked every
    /// IntervalSeconds. Keeps read depth bounded so disk-less reads stay cheap;
    /// non-destructive, so history and the audit trail are preserved.
    ///
    /// Fire-and-forget: the task lives for the process, so the handle is
    /// dropped. Start it once at server boot.
    pub semidet fn start_compaction(context, store_term, max_depth_term, interval_secs_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let max_depth: u64 = max_depth_term.get_ex()?;
        let interval_secs: u64 = interval_secs_term.get_ex()?;
        if max_depth == 0 || interval_secs == 0 {
            return context.raise_exception(
                &term!{context: error(domain_error(positive_integer, compaction_params), _)}?);
        }
        // ReadStore derefs to SyncStore; compaction runs on the materialized
        // layers regardless of how this store reads.
        store.spawn_compaction(
            max_depth as usize,
            std::time::Duration::from_secs(interval_secs),
        );
        Ok(())
    }

    /// Get layer cache statistics: (total_entries, live_entries, dead_entries)
    /// Dead entries are stale weak references that should be cleaned up.
    pub semidet fn layer_cache_stats(_context, store_term, total_term, live_term, dead_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let (total, live, dead) = store.layer_cache_stats();
        total_term.unify(total as u64)?;
        live_term.unify(live as u64)?;
        dead_term.unify(dead as u64)
    }

    /// Get bytes of backing data for layer cache entries: (total, live, dead).
    pub semidet fn layer_cache_memory_bytes(_context, store_term, total_term, live_term, dead_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let (total, live, dead) = store.layer_cache_memory_bytes();
        total_term.unify(total as u64)?;
        live_term.unify(live as u64)?;
        dead_term.unify(dead as u64)
    }

    /// Get current LRU archive cache usage in bytes.
    /// Fails if no LRU backend is configured (e.g. memory store).
    pub semidet fn lru_cache_used_bytes(_context, store_term, bytes_term) {
        let store: WrappedStore = store_term.get_ex()?;
        match store.lru_cache_used_bytes() {
            Some(bytes) => bytes_term.unify(bytes as u64),
            None => Err(PrologError::Failure),
        }
    }

    /// Remove stale (dead) weak references from the layer cache.
    /// Returns the number of entries removed.
    pub semidet fn cleanup_layer_cache(_context, store_term, removed_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let removed = store.cleanup_layer_cache();
        removed_term.unify(removed as u64)
    }

    /// Invalidate a specific layer from the cache, forcing reload from disk on next access.
    /// This is useful after rollup to ensure the rolled-up version is loaded.
    pub semidet fn invalidate_layer_cache_entry(context, store_term, layer_id_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let layer_id_string: PrologText = layer_id_term.get_ex()?;
        let layer_id = context.try_or_die(string_to_name(&layer_id_string))?;
        store.invalidate_layer(layer_id);
        Ok(())
    }

    /// Get the current process resident set size (RSS) in bytes.
    /// Fails if the value cannot be determined on this platform.
    pub semidet fn process_rss_bytes(_context, bytes_term) {
        match get_process_rss_bytes() {
            Some(bytes) => bytes_term.unify(bytes as u64),
            None => Err(PrologError::Failure),
        }
    }
}

/// A store plus how its layers should be read.
///
/// `Deref`s to the underlying `SyncStore`, so every existing call site keeps
/// working; only `store_id_layer` consults the flag, to decide whether to hand
/// out a materialized or a disk-less layer handle.
#[derive(Clone)]
pub struct ReadStore {
    inner: SyncStore,
    /// Read layers disk-lessly (block-granular ranged GETs) instead of
    /// materializing them. Opt-in, and only meaningful on an object backend.
    pub diskless: bool,
}

impl ReadStore {
    pub fn materialized(inner: SyncStore) -> Self {
        Self {
            inner,
            diskless: false,
        }
    }

    pub fn diskless(inner: SyncStore) -> Self {
        Self {
            inner,
            diskless: true,
        }
    }
}

impl std::ops::Deref for ReadStore {
    type Target = SyncStore;
    fn deref(&self) -> &SyncStore {
        &self.inner
    }
}

wrapped_clone_blob!("store", pub WrappedStore, ReadStore, defaults);
