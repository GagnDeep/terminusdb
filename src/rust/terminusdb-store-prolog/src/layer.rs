use super::value::*;
use crate::store::*;
use std::io::{self, Write};
use std::iter::Peekable;
use swipl::prelude::*;
use tdb_succinct::TypedDictEntry;
use terminus_store::layer::{IdTriple, ObjectType};
use terminus_store::storage::{name_to_string, string_to_name};
use terminus_store::store::sync::*;
use terminus_store::Layer;

type TripleIter = Box<dyn Iterator<Item = IdTriple> + Send>;

/// A layer as the Prolog boundary sees it: either fully materialized in RAM, or
/// read disk-lessly at block granularity straight from the object store.
///
/// Every read is fallible here, even on the materialized arm, because the
/// disk-less arm talks to the network. That uniformity is deliberate: it forces
/// each predicate through `try_or_die`, so a failed read surfaces as a Prolog
/// *exception* rather than as "no solution". Silently reporting a network
/// failure as an empty result would let a query return a wrong answer, which on
/// an audit store is the one outcome worth paying anything to avoid.
///
/// Writes and history rewriting (`open_write`, squash, rollup) stay on the
/// materialized arm only; the disk-less arm rejects them explicitly.
#[derive(Clone)]
pub enum ReadLayer {
    Materialized(SyncStoreLayer),
    Lazy(DisklessLayer),
}

/// A disk-less layer that can stand in for a materialized one.
///
/// `Layer` is an infallible trait -- it was designed for a layer already
/// resident in RAM, where a read is a pointer chase. A disk-less layer reads
/// over the network, so it *can* fail, and there is no honest infallible answer
/// to give when it does.
///
/// The resolution is a sticky error. A failed read records its error here and
/// yields an empty/`None` result, and the caller checks [`take_error`] once the
/// query is finished: if anything failed, the whole result is discarded and an
/// exception raised. So a disk-less query either returns a fully correct answer
/// or raises -- it never returns a partial one dressed up as complete, which is
/// the failure mode that actually matters on an audit store.
///
/// The sink is shared by every clone, so results still funnel back to one place
/// when a reader hands copies to worker threads (the document reader uses rayon).
///
/// [`take_error`]: DisklessLayer::take_error
#[derive(Clone)]
pub struct DisklessLayer {
    inner: SyncLazyLayer,
    first_error: std::sync::Arc<std::sync::Mutex<Option<io::Error>>>,
}

impl DisklessLayer {
    pub fn new(inner: SyncLazyLayer) -> Self {
        Self {
            inner,
            first_error: Default::default(),
        }
    }

    /// The underlying handle, for the fallible API where errors are propagated
    /// properly rather than recorded.
    pub fn inner(&self) -> &SyncLazyLayer {
        &self.inner
    }

    /// Record `result`'s error, if any, and fall back to `fallback`.
    ///
    /// Only the first error is kept: later ones are usually consequences of it,
    /// and the first is the one that explains the failure.
    fn or_record<T>(&self, result: io::Result<T>, fallback: T) -> T {
        match result {
            Ok(v) => v,
            Err(e) => {
                let mut slot = self
                    .first_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if slot.is_none() {
                    *slot = Some(e);
                }
                fallback
            }
        }
    }

    /// Take the first error recorded since the last call, if any.
    ///
    /// Callers must consult this after finishing a read and before reporting
    /// results, or a failed read silently becomes an empty one.
    pub fn take_error(&self) -> Option<io::Error> {
        self.first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

impl ReadLayer {
    /// The materialized layer, if this is one. Used by the write/admin
    /// predicates and by the Rust readers that are not yet disk-less aware.
    pub fn materialized(&self) -> Option<&SyncStoreLayer> {
        match self {
            Self::Materialized(l) => Some(l),
            Self::Lazy(_) => None,
        }
    }

    pub fn into_materialized(self) -> Option<SyncStoreLayer> {
        match self {
            Self::Materialized(l) => Some(l),
            Self::Lazy(_) => None,
        }
    }

    /// A layer usable where a real, built layer is required: installing a graph
    /// head, or applying a delta/diff into a builder. Those inputs are always
    /// layers this process just built, so they are always materialized.
    pub fn require_materialized_head(&self) -> io::Result<SyncStoreLayer> {
        self.materialized_layer("this operation")
    }

    /// A materialized view of this layer, materializing on demand if needed.
    ///
    /// Some operations inherently need a whole layer: building a child on top
    /// of it, squashing, rolling up, installing it as a graph head. They cannot
    /// be done block-lazily by definition, and refusing them would mean a
    /// disk-less store could not be written to at all.
    ///
    /// So this materializes rather than failing -- and costs exactly what the
    /// disk-less path exists to avoid. **Reads must never call it.**
    fn materialized_layer(&self, what: &str) -> io::Result<SyncStoreLayer> {
        match self {
            Self::Materialized(l) => Ok(l.clone()),
            Self::Lazy(l) => l.inner().materialize()?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{what}: layer could not be materialized"),
                )
            }),
        }
    }

    pub fn name(&self) -> [u32; 5] {
        match self {
            Self::Materialized(l) => l.name(),
            Self::Lazy(l) => l.inner().name(),
        }
    }

    /// Take the first error recorded while reading through the infallible
    /// [`Layer`] trait, if any.
    ///
    /// Anything that reads this layer as a `Layer` **must** call this before
    /// reporting results: on the disk-less arm a failed read yields an empty
    /// result rather than an error, so skipping the check silently turns a
    /// network failure into a wrong answer. Always `None` on the materialized
    /// arm, whose reads cannot fail.
    pub fn take_error(&self) -> Option<io::Error> {
        match self {
            Self::Materialized(_) => None,
            Self::Lazy(l) => l.take_error(),
        }
    }

    // ---- chain metadata ----

    pub fn try_node_and_value_count(&self) -> io::Result<u64> {
        match self {
            Self::Materialized(l) => Ok(l.node_and_value_count() as u64),
            Self::Lazy(l) => l.inner().node_and_value_count(),
        }
    }

    pub fn try_predicate_count(&self) -> io::Result<u64> {
        match self {
            Self::Materialized(l) => Ok(l.predicate_count() as u64),
            Self::Lazy(l) => l.inner().predicate_count(),
        }
    }

    pub fn parent(&self) -> io::Result<Option<ReadLayer>> {
        match self {
            Self::Materialized(l) => Ok(l.parent()?.map(ReadLayer::Materialized)),
            Self::Lazy(l) => Ok(l
                .inner()
                .parent()?
                .map(|p| ReadLayer::Lazy(DisklessLayer::new(p)))),
        }
    }

    pub fn retrieve_layer_stack_names(&self) -> io::Result<Vec<[u32; 5]>> {
        match self {
            Self::Materialized(l) => l.retrieve_layer_stack_names(),
            Self::Lazy(l) => l.inner().retrieve_layer_stack_names(),
        }
    }

    // ---- forward resolution (string -> id) ----

    pub fn try_subject_id(&self, subject: &str) -> io::Result<Option<u64>> {
        match self {
            Self::Materialized(l) => Ok(l.subject_id(subject)),
            Self::Lazy(l) => l.inner().subject_id(subject),
        }
    }

    pub fn try_predicate_id(&self, predicate: &str) -> io::Result<Option<u64>> {
        match self {
            Self::Materialized(l) => Ok(l.predicate_id(predicate)),
            Self::Lazy(l) => l.inner().predicate_id(predicate),
        }
    }

    pub fn try_object_node_id(&self, object: &str) -> io::Result<Option<u64>> {
        match self {
            Self::Materialized(l) => Ok(l.object_node_id(object)),
            Self::Lazy(l) => l.inner().object_node_id(object),
        }
    }

    pub fn try_object_value_id(&self, object: &TypedDictEntry) -> io::Result<Option<u64>> {
        match self {
            Self::Materialized(l) => Ok(l.object_value_id(object)),
            Self::Lazy(l) => l.inner().object_value_id(object),
        }
    }

    // ---- reverse resolution (id -> string/value) ----

    pub fn try_id_subject(&self, id: u64) -> io::Result<Option<String>> {
        match self {
            Self::Materialized(l) => Ok(l.id_subject(id)),
            Self::Lazy(l) => l.inner().id_subject(id),
        }
    }

    pub fn try_id_predicate(&self, id: u64) -> io::Result<Option<String>> {
        match self {
            Self::Materialized(l) => Ok(l.id_predicate(id)),
            Self::Lazy(l) => l.inner().id_predicate(id),
        }
    }

    pub fn try_id_object(&self, id: u64) -> io::Result<Option<ObjectType>> {
        match self {
            Self::Materialized(l) => Ok(l.id_object(id)),
            Self::Lazy(l) => l.inner().id_object(id),
        }
    }

    // ---- existence and traversal ----

    pub fn try_triple_exists(&self, subject: u64, predicate: u64, object: u64) -> io::Result<bool> {
        match self {
            Self::Materialized(l) => Ok(l.triple_exists(subject, predicate, object)),
            Self::Lazy(l) => l.inner().triple_exists(subject, predicate, object),
        }
    }

    pub fn try_triples(&self) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples()),
            Self::Lazy(l) => l.inner().triples(),
        }
    }

    pub fn try_triples_s(&self, subject: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_s(subject)),
            Self::Lazy(l) => Ok(Box::new(l.inner().triples_s(subject)?.into_iter())),
        }
    }

    pub fn try_triples_sp(&self, subject: u64, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_sp(subject, predicate)),
            Self::Lazy(l) => Ok(Box::new(
                l.inner().triples_sp(subject, predicate)?.into_iter(),
            )),
        }
    }

    pub fn try_triples_p(&self, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_p(predicate)),
            Self::Lazy(l) => Ok(Box::new(l.inner().triples_p(predicate)?.into_iter())),
        }
    }

    pub fn try_triples_o(&self, object: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_o(object)),
            Self::Lazy(l) => Ok(Box::new(l.inner().triples_o(object)?.into_iter())),
        }
    }

    // ---- deltas ----

    pub fn triple_addition_exists(&self, s: u64, p: u64, o: u64) -> io::Result<bool> {
        match self {
            Self::Materialized(l) => l.triple_addition_exists(s, p, o),
            Self::Lazy(l) => l.inner().triple_addition_exists(s, p, o),
        }
    }

    pub fn triple_removal_exists(&self, s: u64, p: u64, o: u64) -> io::Result<bool> {
        match self {
            Self::Materialized(l) => l.triple_removal_exists(s, p, o),
            Self::Lazy(l) => l.inner().triple_removal_exists(s, p, o),
        }
    }

    pub fn triple_additions(&self) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_additions(),
            Self::Lazy(l) => l.inner().triple_additions(),
        }
    }

    pub fn triple_removals(&self) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_removals(),
            Self::Lazy(l) => l.inner().triple_removals(),
        }
    }

    pub fn triple_additions_s(&self, subject: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_additions_s(subject),
            Self::Lazy(l) => l.inner().triple_additions_s(subject),
        }
    }

    pub fn triple_removals_s(&self, subject: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_removals_s(subject),
            Self::Lazy(l) => l.inner().triple_removals_s(subject),
        }
    }

    pub fn triple_additions_sp(&self, subject: u64, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_additions_sp(subject, predicate),
            Self::Lazy(l) => l.inner().triple_additions_sp(subject, predicate),
        }
    }

    pub fn triple_removals_sp(&self, subject: u64, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_removals_sp(subject, predicate),
            Self::Lazy(l) => l.inner().triple_removals_sp(subject, predicate),
        }
    }

    pub fn triple_additions_p(&self, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_additions_p(predicate),
            Self::Lazy(l) => l.inner().triple_additions_p(predicate),
        }
    }

    pub fn triple_removals_p(&self, predicate: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_removals_p(predicate),
            Self::Lazy(l) => l.inner().triple_removals_p(predicate),
        }
    }

    pub fn triple_additions_o(&self, object: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_additions_o(object),
            Self::Lazy(l) => l.inner().triple_additions_o(object),
        }
    }

    pub fn triple_removals_o(&self, object: u64) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => l.triple_removals_o(object),
            Self::Lazy(l) => l.inner().triple_removals_o(object),
        }
    }

    // ---- counts ----
    //
    // These are chain-cumulative triple counts. The disk-less handle would have
    // to load every ancestor's adjacency to compute them. They do not: summing
    // the per-layer counts is a handful of cached metadata reads.

    pub fn triple_layer_addition_count(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => l.triple_layer_addition_count(),
            Self::Lazy(l) => l.inner().triple_layer_addition_count(),
        }
    }

    pub fn triple_layer_removal_count(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => l.triple_layer_removal_count(),
            Self::Lazy(l) => l.inner().triple_layer_removal_count(),
        }
    }

    pub fn try_triple_addition_count(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => Ok(l.triple_addition_count()),
            Self::Lazy(l) => l.inner().triple_addition_count(),
        }
    }

    pub fn try_triple_removal_count(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => Ok(l.triple_removal_count()),
            Self::Lazy(l) => l.inner().triple_removal_count(),
        }
    }

    pub fn try_triple_count(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => Ok(l.triple_count()),
            Self::Lazy(l) => l.inner().triple_count(),
        }
    }

    /// Byte size of this layer's backing data. Not tracked disk-lessly, and
    /// callers treat it as a metric rather than a result, so report unknown
    /// rather than materializing a whole layer to answer it.
    pub fn try_stored_size(&self) -> io::Result<usize> {
        match self {
            Self::Materialized(l) => Ok(l.stored_size()),
            Self::Lazy(_) => Ok(0),
        }
    }

    // ---- value ranges ----

    pub fn try_triples_value_range(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_value_range(low, high)),
            Self::Lazy(l) => Ok(Box::new(
                l.inner().triples_value_range(low, high)?.into_iter(),
            )),
        }
    }

    pub fn try_triples_value_range_rev(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> io::Result<TripleIter> {
        match self {
            Self::Materialized(l) => Ok(l.triples_value_range_rev(low, high)),
            Self::Lazy(l) => Ok(Box::new(
                l.inner().triples_value_range_rev(low, high)?.into_iter(),
            )),
        }
    }

    // ---- writes and history rewriting (materialized only) ----

    pub fn open_write(&self) -> io::Result<SyncStoreLayerBuilder> {
        self.materialized_layer("open_write")?.open_write()
    }

    pub fn squash(&self) -> io::Result<SyncStoreLayer> {
        self.materialized_layer("squash")?.squash()
    }

    pub fn squash_upto(&self, upto: &ReadLayer) -> io::Result<SyncStoreLayer> {
        let upto = upto.materialized_layer("squash_upto")?;
        self.materialized_layer("squash_upto")?.squash_upto(&upto)
    }

    pub fn rollup(&self) -> io::Result<()> {
        self.materialized_layer("rollup")?.rollup()
    }

    pub fn rollup_upto(&self, upto: &ReadLayer) -> io::Result<()> {
        let upto = upto.materialized_layer("rollup_upto")?;
        self.materialized_layer("rollup_upto")?.rollup_upto(&upto)
    }

    pub fn imprecise_rollup_upto(&self, upto: &ReadLayer) -> io::Result<()> {
        let upto = upto.materialized_layer("imprecise_rollup_upto")?;
        self.materialized_layer("imprecise_rollup_upto")?
            .imprecise_rollup_upto(&upto)
    }
}

/// `ReadLayer` is a `Layer`, so every reader written against the trait works on
/// either arm unchanged.
///
/// On the disk-less arm a failed read records its error and yields an empty
/// result; see [`DisklessLayer`] for why, and for the obligation that creates on
/// callers. Anything reading through this trait must call
/// [`ReadLayer::take_error`] before treating the results as complete.
impl Layer for ReadLayer {
    fn name(&self) -> [u32; 5] {
        ReadLayer::name(self)
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        match self {
            Self::Materialized(l) => l.parent_name(),
            Self::Lazy(l) => l.or_record(l.inner().parent_name(), None),
        }
    }

    fn node_and_value_count(&self) -> usize {
        match self {
            Self::Materialized(l) => l.node_and_value_count(),
            Self::Lazy(l) => l.or_record(l.inner().node_and_value_count(), 0) as usize,
        }
    }

    fn predicate_count(&self) -> usize {
        match self {
            Self::Materialized(l) => l.predicate_count(),
            Self::Lazy(l) => l.or_record(l.inner().predicate_count(), 0) as usize,
        }
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self {
            Self::Materialized(l) => l.subject_id(subject),
            Self::Lazy(l) => l.or_record(l.inner().subject_id(subject), None),
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self {
            Self::Materialized(l) => l.predicate_id(predicate),
            Self::Lazy(l) => l.or_record(l.inner().predicate_id(predicate), None),
        }
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        match self {
            Self::Materialized(l) => l.object_node_id(object),
            Self::Lazy(l) => l.or_record(l.inner().object_node_id(object), None),
        }
    }

    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64> {
        match self {
            Self::Materialized(l) => l.object_value_id(object),
            Self::Lazy(l) => l.or_record(l.inner().object_value_id(object), None),
        }
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        match self {
            Self::Materialized(l) => l.id_subject(id),
            Self::Lazy(l) => l.or_record(l.inner().id_subject(id), None),
        }
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        match self {
            Self::Materialized(l) => l.id_predicate(id),
            Self::Lazy(l) => l.or_record(l.inner().id_predicate(id), None),
        }
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        match self {
            Self::Materialized(l) => l.id_object(id),
            Self::Lazy(l) => l.or_record(l.inner().id_object(id), None),
        }
    }

    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        match self {
            Self::Materialized(l) => l.id_object_is_node(id),
            Self::Lazy(l) => l.or_record(
                l.inner()
                    .id_object(id)
                    .map(|o| o.map(|o| matches!(o, ObjectType::Node(_)))),
                None,
            ),
        }
    }

    fn all_counts(&self) -> terminus_store::layer::LayerCounts {
        match self {
            Self::Materialized(l) => l.all_counts(),
            Self::Lazy(l) => terminus_store::layer::LayerCounts {
                node_count: l.or_record(l.inner().node_and_value_count(), 0) as usize,
                predicate_count: l.or_record(l.inner().predicate_count(), 0) as usize,
                value_count: 0,
            },
        }
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        match self {
            Self::Materialized(l) => l.triple_exists(subject, predicate, object),
            Self::Lazy(l) => {
                l.or_record(l.inner().triple_exists(subject, predicate, object), false)
            }
        }
    }

    fn triples(&self) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples(),
            Self::Lazy(l) => l.or_record(l.inner().triples(), empty_triples()),
        }
    }

    fn triples_s(&self, subject: u64) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_s(subject),
            Self::Lazy(l) => boxed(l.or_record(l.inner().triples_s(subject), Vec::new())),
        }
    }

    fn triples_sp(&self, subject: u64, predicate: u64) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_sp(subject, predicate),
            Self::Lazy(l) => {
                boxed(l.or_record(l.inner().triples_sp(subject, predicate), Vec::new()))
            }
        }
    }

    fn triples_p(&self, predicate: u64) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_p(predicate),
            Self::Lazy(l) => boxed(l.or_record(l.inner().triples_p(predicate), Vec::new())),
        }
    }

    fn triples_o(&self, object: u64) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_o(object),
            Self::Lazy(l) => boxed(l.or_record(l.inner().triples_o(object), Vec::new())),
        }
    }

    fn triples_value_range(&self, low: &TypedDictEntry, high: &TypedDictEntry) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_value_range(low, high),
            Self::Lazy(l) => {
                boxed(l.or_record(l.inner().triples_value_range(low, high), Vec::new()))
            }
        }
    }

    fn triples_value_range_rev(&self, low: &TypedDictEntry, high: &TypedDictEntry) -> TripleIter {
        match self {
            Self::Materialized(l) => l.triples_value_range_rev(low, high),
            Self::Lazy(l) => {
                boxed(l.or_record(l.inner().triples_value_range_rev(low, high), Vec::new()))
            }
        }
    }

    fn triple_addition_count(&self) -> usize {
        match self {
            Self::Materialized(l) => l.triple_addition_count(),
            Self::Lazy(l) => l.or_record(l.inner().triple_addition_count(), 0),
        }
    }

    fn triple_removal_count(&self) -> usize {
        match self {
            Self::Materialized(l) => l.triple_removal_count(),
            Self::Lazy(l) => l.or_record(l.inner().triple_removal_count(), 0),
        }
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        match self {
            Self::Materialized(l) => l.single_triple_sp(subject, predicate),
            Self::Lazy(l) => l
                .or_record(l.inner().triples_sp(subject, predicate), Vec::new())
                .into_iter()
                .next(),
        }
    }

    fn stored_size(&self) -> usize {
        match self {
            Self::Materialized(l) => l.stored_size(),
            // Not tracked disk-lessly, and callers treat it as a metric rather
            // than a result, so report unknown rather than poisoning the query.
            Self::Lazy(_) => 0,
        }
    }
}

fn empty_triples() -> TripleIter {
    Box::new(std::iter::empty())
}

fn boxed(v: Vec<IdTriple>) -> TripleIter {
    Box::new(v.into_iter())
}

impl PartialEq for ReadLayer {
    /// Layers are content-addressed, so identity is the name. A disk-less and a
    /// materialized handle on the same layer are the same layer.
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

predicates! {
    pub semidet fn node_and_value_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.try_node_and_value_count())?;

        count_term.unify(count)
    }

    pub semidet fn predicate_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.try_predicate_count())?;

        count_term.unify(count)
    }

    pub semidet fn subject_to_id(context, layer_term, subject_term, id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let subject: PrologText = subject_term.get_ex()?;

        match context.try_or_die(layer.try_subject_id(&subject))? {
            Some(id) => id_term.unify(id),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn id_to_subject(context, layer_term, id_term, subject_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let id: u64 = id_term.get_ex()?;

        match context.try_or_die(layer.try_id_subject(id))? {
            Some(subject) => subject_term.unify(subject),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn predicate_to_id(context, layer_term, predicate_term, id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let predicate: PrologText = predicate_term.get_ex()?;

        match context.try_or_die(layer.try_predicate_id(&predicate))? {
            Some(id) => id_term.unify(id),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn id_to_predicate(context, layer_term, id_term, predicate_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let id: u64 = id_term.get_ex()?;

        match context.try_or_die(layer.try_id_predicate(id))? {
            Some(predicate) => predicate_term.unify(predicate),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn object_to_id(context, layer_term, object_term, id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;

        let inner = context.new_term_ref();
        let ty = context.new_term_ref();
        let id: Option<u64>;
        if attempt(object_term.unify(term!{context: node(#&inner)}?))? {
            let object: PrologText = inner.get_ex()?;
            id = context.try_or_die(layer.try_object_node_id(&object))?;
        }
        else if attempt(object_term.unify(term!{context: value(#&inner,#&ty)}?))? {
            let entry = make_entry_from_term(context,&inner,&ty)?;
            id = context.try_or_die(layer.try_object_value_id(&entry))?;
        }
        else if attempt(object_term.unify(term!{context: lang(#&inner,#&ty)}?))? {
            let entry = make_entry_from_lang_term(context,&inner,&ty)?;
            id = context.try_or_die(layer.try_object_value_id(&entry))?;
        }
        else {
            return context.raise_exception(&term!{context: error(domain_error(oneof([node(), value()]), #object_term), _)}?);
        }


        match id {
            Some(id) => id_term.unify(id),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn id_to_object(context, layer_term, id_term, object_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let id: u64 = id_term.get_ex()?;

        match context.try_or_die(layer.try_id_object(id))? {
            Some(ObjectType::Node(object)) => {
                object_term.unify(functor!("node/1"))?;
                object_term.unify_arg(1, object)
            }
            Some(ObjectType::Value(object)) => {
                unify_entry(context, &object, object_term)
            }
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn parent(context, layer_term, parent_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        match context.try_or_die(layer.parent())? {
            Some(p) => parent_term.unify(WrappedLayer(p)),
            None => Err(PrologError::Failure)
        }
    }

    pub semidet fn squash(context, layer_term, squashed_layer_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let squashed = context.try_or_die(layer.squash())?;
        squashed_layer_term.unify(&WrappedLayer(ReadLayer::Materialized(squashed)))
    }

    pub semidet fn squash_upto(context, layer_term, upto_term, squashed_layer_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let upto: WrappedLayer = upto_term.get_ex()?;
        let squashed = context.try_or_die(layer.squash_upto(&upto))?;
        squashed_layer_term.unify(&WrappedLayer(ReadLayer::Materialized(squashed)))
    }

    pub semidet fn rollup(context, layer_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        context.try_or_die(layer.rollup())
    }

    pub semidet fn rollup_upto(context, layer_term, upto_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let upto: WrappedLayer = upto_term.get_ex()?;
        context.try_or_die(layer.rollup_upto(&upto))
    }

    pub semidet fn imprecise_rollup_upto(context, layer_term, upto_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let upto: WrappedLayer = upto_term.get_ex()?;
        context.try_or_die(layer.imprecise_rollup_upto(&upto))
    }

    pub semidet fn layer_addition_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.triple_layer_addition_count())? as u64;

        count_term.unify(count)
    }

    pub semidet fn layer_removal_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.triple_layer_removal_count())? as u64;

        count_term.unify(count)
    }

    pub semidet fn layer_total_addition_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.try_triple_addition_count())? as u64;

        count_term.unify(count)
    }

    pub semidet fn layer_total_removal_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.try_triple_removal_count())? as u64;

        count_term.unify(count)
    }

    pub semidet fn layer_total_triple_count(context, layer_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let count = context.try_or_die(layer.try_triple_count())? as u64;

        count_term.unify(count)
    }

    pub semidet fn layer_to_id(_context, layer_term, id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let name = name_to_string(layer.name());

        id_term.unify(name)
    }

    pub semidet fn store_id_layer(context, store_term, id_term, layer_term) {
        if layer_term.is_var() {
            let store: WrappedStore = store_term.get_ex()?;
            let id: PrologText = id_term.get_ex()?;
            let name = context.try_or_die(string_to_name(&id))?;

            if store.diskless {
                // A disk-less handle is valid for any layer the store knows
                // about, so existence still has to be checked explicitly --
                // otherwise an unknown id would silently yield a handle that
                // errors on first use instead of failing here.
                if !context.try_or_die(store.layer_exists(name))? {
                    return Err(PrologError::Failure);
                }
                let layer = DisklessLayer::new(store.lazy_layer(name));
                layer_term.unify(&WrappedLayer(ReadLayer::Lazy(layer)))
            }
            else {
                match context.try_or_die(store.get_layer_from_id(name))? {
                    Some(layer) => layer_term.unify(&WrappedLayer(ReadLayer::Materialized(layer))),
                    None => Err(PrologError::Failure)
                }
            }
        }
        else {
            let layer: WrappedLayer = layer_term.get_ex()?;
            let name = name_to_string(layer.name());

            id_term.unify(name)
        }
    }

    pub nondet fn id_triple<Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>>(context, layer_term, subject_id_term, predicate_id_term, object_id_term) {
        setup => {
            let layer: WrappedLayer = layer_term.get_ex()?;

            let iter: Box<dyn Iterator<Item=IdTriple>+Send>;
            if let Some(subject_id) = attempt_opt(subject_id_term.get::<u64>())? {
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // everything is known
                        if context.try_or_die(layer.try_triple_exists(subject_id, predicate_id, object_id))? {
                            return Ok(None);
                        }
                        else {
                            return Err(PrologError::Failure)
                        }
                    }
                    else {
                        // subject and predicate are known, object is not
                        iter = context.try_or_die(layer.try_triples_sp(subject_id, predicate_id))?;
                    }
                }
                else {
                    // subject is known, predicate is not. object may or may not be bound already.
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // object is known so predicate is the only unknown
                        iter = Box::new(context.try_or_die(layer.try_triples_s(subject_id))?
                                        .filter(move |t| t.object == object_id));
                    }
                    else {
                        // both predicate and object are unknown
                        iter = context.try_or_die(layer.try_triples_s(subject_id))?;
                    }
                }
            }
            else if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                // subject is unknown
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    // predicate is known
                    iter = Box::new(context.try_or_die(layer.try_triples_o(object_id))?
                                    .filter(move |t| t.predicate == predicate_id));
                }
                else {
                    // predicate is unknown, only object is known
                    iter = context.try_or_die(layer.try_triples_o(object_id))?
                }
            }
            else if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                // only predicate is known
                iter = context.try_or_die(layer.try_triples_p(predicate_id))?;
            }
            else {
                // nothing is known so return everything
                iter = context.try_or_die(layer.try_triples())?;
            }

            // lets make it peekable
            let iter = iter.peekable();

            Ok(Some(iter))
        },
        call(iter) => {
            if let Some(triple) = iter.next() {
                subject_id_term.unify(triple.subject)?;
                predicate_id_term.unify(triple.predicate)?;
                object_id_term.unify(triple.object)?;

                Ok(iter.peek().is_some())
            }
            else {
                Err(PrologError::Failure)
            }
        }
    }

    pub semidet fn sp_card(context, layer_term, subject_id_term, predicate_id_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let subject_id: u64 = subject_id_term.get_ex()?;
        let predicate_id: u64 = predicate_id_term.get_ex()?;
        let iter = context.try_or_die(layer.try_triples_sp(subject_id, predicate_id))?;
        let count = iter.count() as u64;
        count_term.unify(count)
    }

    pub semidet fn op_card(context, layer_term, object_id_term, predicate_id_term, count_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let object_id: u64 = object_id_term.get_ex()?;
        let predicate_id: u64 = predicate_id_term.get_ex()?;
        let count = context.try_or_die(layer.try_triples_o(object_id))?
            .filter(|t| t.predicate == predicate_id)
            .count() as u64;
        count_term.unify(count)
    }

    pub nondet fn id_triple_addition<Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>>(context, layer_term, subject_id_term, predicate_id_term, object_id_term) {
        setup => {
            let layer: WrappedLayer = layer_term.get_ex()?;

            let iter: Box<dyn Iterator<Item=IdTriple>+Send>;
            if let Some(subject_id) = attempt_opt(subject_id_term.get::<u64>())? {
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // everything is known
                        if context.try_or_die(layer.triple_addition_exists(subject_id, predicate_id, object_id))? {
                            return Ok(None);
                        }
                        else {
                            return Err(PrologError::Failure)
                        }
                    }
                    else {
                        // subject and predicate are known, object is not
                        iter = context.try_or_die(layer.triple_additions_sp(subject_id, predicate_id))?;
                    }
                }
                else {
                    // subject is known, predicate is not. object may or may not be bound already.
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // object is known so predicate is the only unknown
                        iter = Box::new(context.try_or_die(layer.triple_additions_s(subject_id))?
                                        .filter(move |t| t.object == object_id));
                    }
                    else {
                        // both predicate and object are unknown
                        iter = context.try_or_die(layer.triple_additions_s(subject_id))?;
                    }
                }
            }
            else if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                // subject is unknown
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    // predicate is known
                    iter = Box::new(context.try_or_die(layer.triple_additions_o(object_id))?
                                    .filter(move |t| t.predicate == predicate_id));
                }
                else {
                    // predicate is unknown, only object is known
                    iter = context.try_or_die(layer.triple_additions_o(object_id))?
                }
            }
            else if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                // only predicate is known
                iter = context.try_or_die(layer.triple_additions_p(predicate_id))?;
            }
            else {
                // nothing is known so return everything
                iter = context.try_or_die(layer.triple_additions())?;
            }

            // lets make it peekable
            let iter = iter.peekable();

            Ok(Some(iter))
        },
        call(iter) => {
            if let Some(triple) = iter.next() {
                subject_id_term.unify(triple.subject)?;
                predicate_id_term.unify(triple.predicate)?;
                object_id_term.unify(triple.object)?;

                Ok(iter.peek().is_some())
            }
            else {
                Err(PrologError::Failure)
            }
        }
    }

    pub nondet fn id_triple_removal<Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>>(context, layer_term, subject_id_term, predicate_id_term, object_id_term) {
        setup => {
            let layer: WrappedLayer = layer_term.get_ex()?;

            let iter: Box<dyn Iterator<Item=IdTriple>+Send>;
            if let Some(subject_id) = attempt_opt(subject_id_term.get::<u64>())? {
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // everything is known
                        if context.try_or_die(layer.triple_removal_exists(subject_id, predicate_id, object_id))? {
                            return Ok(None);
                        }
                        else {
                            return Err(PrologError::Failure)
                        }
                    }
                    else {
                        // subject and predicate are known, object is not
                        iter = context.try_or_die(layer.triple_removals_sp(subject_id, predicate_id))?;
                    }
                }
                else {
                    // subject is known, predicate is not. object may or may not be bound already.
                    if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                        // object is known so predicate is the only unknown
                        iter = Box::new(context.try_or_die(layer.triple_removals_s(subject_id))?
                                        .filter(move |t| t.object == object_id));
                    }
                    else {
                        // both predicate and object are unknown
                        iter = context.try_or_die(layer.triple_removals_s(subject_id))?;
                    }
                }
            }
            else if let Some(object_id) = attempt_opt(object_id_term.get::<u64>())? {
                // subject is unknown
                if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                    // predicate is known
                    iter = Box::new(context.try_or_die(layer.triple_removals_o(object_id))?
                                    .filter(move |t| t.predicate == predicate_id));
                }
                else {
                    // predicate is unknown, only object is known
                    iter = context.try_or_die(layer.triple_removals_o(object_id))?
                }
            }
            else if let Some(predicate_id) = attempt_opt(predicate_id_term.get::<u64>())? {
                // only predicate is known
                iter = context.try_or_die(layer.triple_removals_p(predicate_id))?;
            }
            else {
                // nothing is known so return everything
                iter = context.try_or_die(layer.triple_removals())?;
            }

            // lets make it peekable
            let iter = iter.peekable();

            Ok(Some(iter))
        },
        call(iter) => {
            if let Some(triple) = iter.next() {
                subject_id_term.unify(triple.subject)?;
                predicate_id_term.unify(triple.predicate)?;
                object_id_term.unify(triple.object)?;

                Ok(iter.peek().is_some())
            }
            else {
                Err(PrologError::Failure)
            }
        }
    }

    pub nondet fn id_triple_value_range<Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>>(context, layer_term, low_term, high_term, subject_id_term, predicate_id_term, object_id_term) {
        setup => {
            let layer: WrappedLayer = layer_term.get_ex()?;

            let low_inner = context.new_term_ref();
            let low_ty = context.new_term_ref();
            let low_entry;
            if attempt(low_term.unify(term!{context: value(#&low_inner, #&low_ty)}?))? {
                low_entry = make_entry_from_term(context, &low_inner, &low_ty)?;
            } else if attempt(low_term.unify(term!{context: lang(#&low_inner, #&low_ty)}?))? {
                low_entry = make_entry_from_lang_term(context, &low_inner, &low_ty)?;
            } else {
                return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #low_term), _)}?);
            }

            let high_inner = context.new_term_ref();
            let high_ty = context.new_term_ref();
            let high_entry;
            if attempt(high_term.unify(term!{context: value(#&high_inner, #&high_ty)}?))? {
                high_entry = make_entry_from_term(context, &high_inner, &high_ty)?;
            } else if attempt(high_term.unify(term!{context: lang(#&high_inner, #&high_ty)}?))? {
                high_entry = make_entry_from_lang_term(context, &high_inner, &high_ty)?;
            } else {
                return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #high_term), _)}?);
            }

            let iter = context.try_or_die(layer.try_triples_value_range(&low_entry, &high_entry))?.peekable();

            Ok(Some(iter))
        },
        call(iter) => {
            if let Some(triple) = iter.next() {
                subject_id_term.unify(triple.subject)?;
                predicate_id_term.unify(triple.predicate)?;
                object_id_term.unify(triple.object)?;

                Ok(iter.peek().is_some())
            }
            else {
                Err(PrologError::Failure)
            }
        }
    }

    pub nondet fn id_triple_value_range_rev<Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>>(context, layer_term, low_term, high_term, subject_id_term, predicate_id_term, object_id_term) {
        setup => {
            let layer: WrappedLayer = layer_term.get_ex()?;

            let low_inner = context.new_term_ref();
            let low_ty = context.new_term_ref();
            let low_entry;
            if attempt(low_term.unify(term!{context: value(#&low_inner, #&low_ty)}?))? {
                low_entry = make_entry_from_term(context, &low_inner, &low_ty)?;
            } else if attempt(low_term.unify(term!{context: lang(#&low_inner, #&low_ty)}?))? {
                low_entry = make_entry_from_lang_term(context, &low_inner, &low_ty)?;
            } else {
                return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #low_term), _)}?);
            }

            let high_inner = context.new_term_ref();
            let high_ty = context.new_term_ref();
            let high_entry;
            if attempt(high_term.unify(term!{context: value(#&high_inner, #&high_ty)}?))? {
                high_entry = make_entry_from_term(context, &high_inner, &high_ty)?;
            } else if attempt(high_term.unify(term!{context: lang(#&high_inner, #&high_ty)}?))? {
                high_entry = make_entry_from_lang_term(context, &high_inner, &high_ty)?;
            } else {
                return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #high_term), _)}?);
            }

            let iter = context.try_or_die(layer.try_triples_value_range_rev(&low_entry, &high_entry))?.peekable();

            Ok(Some(iter))
        },
        call(iter) => {
            if let Some(triple) = iter.next() {
                subject_id_term.unify(triple.subject)?;
                predicate_id_term.unify(triple.predicate)?;
                object_id_term.unify(triple.object)?;

                Ok(iter.peek().is_some())
            }
            else {
                Err(PrologError::Failure)
            }
        }
    }

    pub semidet fn id_triple_sp_value_next(context, layer_term, subject_id_term, predicate_id_term, reference_term, result_object_id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let subject: u64 = subject_id_term.get_ex()?;
        let predicate: u64 = predicate_id_term.get_ex()?;

        let ref_inner = context.new_term_ref();
        let ref_ty = context.new_term_ref();
        let reference;
        if attempt(reference_term.unify(term!{context: value(#&ref_inner, #&ref_ty)}?))? {
            reference = make_entry_from_term(context, &ref_inner, &ref_ty)?;
        } else if attempt(reference_term.unify(term!{context: lang(#&ref_inner, #&ref_ty)}?))? {
            reference = make_entry_from_lang_term(context, &ref_inner, &ref_ty)?;
        } else {
            return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #reference_term), _)}?);
        }

        let ref_dt = reference.datatype();
        let mut best: Option<(u64, TypedDictEntry)> = None;

        for triple in context.try_or_die(layer.try_triples_sp(subject, predicate))? {
            if let Some(ObjectType::Value(entry)) = context.try_or_die(layer.try_id_object(triple.object))? {
                if entry.datatype() == ref_dt && entry > reference {
                    if let Some((_, ref best_entry)) = best {
                        if entry < *best_entry {
                            best = Some((triple.object, entry));
                        }
                    } else {
                        best = Some((triple.object, entry));
                    }
                }
            }
        }

        match best {
            Some((oid, _)) => result_object_id_term.unify(oid),
            None => Err(PrologError::Failure),
        }
    }

    pub semidet fn id_triple_sp_value_previous(context, layer_term, subject_id_term, predicate_id_term, reference_term, result_object_id_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let subject: u64 = subject_id_term.get_ex()?;
        let predicate: u64 = predicate_id_term.get_ex()?;

        let ref_inner = context.new_term_ref();
        let ref_ty = context.new_term_ref();
        let reference;
        if attempt(reference_term.unify(term!{context: value(#&ref_inner, #&ref_ty)}?))? {
            reference = make_entry_from_term(context, &ref_inner, &ref_ty)?;
        } else if attempt(reference_term.unify(term!{context: lang(#&ref_inner, #&ref_ty)}?))? {
            reference = make_entry_from_lang_term(context, &ref_inner, &ref_ty)?;
        } else {
            return context.raise_exception(&term!{context: error(domain_error(oneof([value(), lang()]), #reference_term), _)}?);
        }

        let ref_dt = reference.datatype();
        let mut best: Option<(u64, TypedDictEntry)> = None;

        for triple in context.try_or_die(layer.try_triples_sp(subject, predicate))? {
            if let Some(ObjectType::Value(entry)) = context.try_or_die(layer.try_id_object(triple.object))? {
                if entry.datatype() == ref_dt && entry < reference {
                    if let Some((_, ref best_entry)) = best {
                        if entry > *best_entry {
                            best = Some((triple.object, entry));
                        }
                    } else {
                        best = Some((triple.object, entry));
                    }
                }
            }
        }

        match best {
            Some((oid, _)) => result_object_id_term.unify(oid),
            None => Err(PrologError::Failure),
        }
    }

    pub semidet fn retrieve_layer_stack_names(context, layer_term, layer_stack_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;

        let names = context.try_or_die(layer.retrieve_layer_stack_names())?;
        let name_strings: Vec<String> = names.into_iter()
            .map(name_to_string)
            .collect();

        layer_stack_term.unify(name_strings.as_slice())
    }

    pub semidet fn layer_stored_size(context, layer_term, size_term) {
        let layer: WrappedLayer = layer_term.get_ex()?;
        let size = context.try_or_die(layer.try_stored_size())?;
        size_term.unify(size as u64)
    }

    pub semidet fn layer_equals(_context, layer1_term, layer2_term) {
        let layer1: WrappedLayer = layer1_term.get_ex()?;
        let layer2: WrappedLayer = layer2_term.get_ex()?;

        into_prolog_result(*layer1 == *layer2)
    }
}

wrapped_clone_blob!("layer", pub WrappedLayer, ReadLayer);

impl CloneBlobImpl for WrappedLayer {
    fn write(&self, stream: &mut PrologStream) -> io::Result<()> {
        write!(stream, "<layer {}>", name_to_string(self.name()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use terminus_store::object_store::{memory::InMemory, ObjectStore};
    use terminus_store::store::sync::SyncStore;
    use terminus_store::ValueTriple;

    /// A base+child graph in an in-process bucket, plus the head's name.
    fn graph() -> (SyncStore, [u32; 5]) {
        let bucket: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let store = SyncStore::wrap(terminus_store::open_object_store(bucket, "", 1 << 30));
        let db = store.create("g").unwrap();

        // Enough entries that the dictionaries are genuinely block-addressed,
        // so the disk-less arm exercises block-lazy reads rather than the
        // small-dictionary whole-load fallback.
        let builder = store.create_base_layer().unwrap();
        for i in 0..700 {
            builder
                .add_value_triple(ValueTriple::new_string_value(
                    &format!("s{:04}", i),
                    "p",
                    &format!("o{:04}", i),
                ))
                .unwrap();
            builder
                .add_value_triple(ValueTriple::new_node(
                    &format!("s{:04}", i),
                    "rel",
                    &format!("s{:04}", (i + 1) % 700),
                ))
                .unwrap();
        }
        let mut layer = builder.commit().unwrap();
        db.set_head(&layer).unwrap();

        // a child, so the head has a real delta to query
        let builder = layer.open_write().unwrap();
        for i in 700..740 {
            builder
                .add_value_triple(ValueTriple::new_string_value(
                    &format!("s{:04}", i),
                    "p",
                    &format!("o{:04}", i),
                ))
                .unwrap();
        }
        for i in 0..20 {
            builder
                .remove_value_triple(ValueTriple::new_string_value(
                    &format!("s{:04}", i),
                    "p",
                    &format!("o{:04}", i),
                ))
                .unwrap();
        }
        layer = builder.commit().unwrap();
        db.set_head(&layer).unwrap();

        let head = layer.name();
        (store, head)
    }

    fn arms() -> (ReadLayer, ReadLayer) {
        let (store, head) = graph();
        (
            ReadLayer::Materialized(store.get_layer_from_id(head).unwrap().unwrap()),
            ReadLayer::Lazy(DisklessLayer::new(store.lazy_layer(head))),
        )
    }

    fn sorted(it: TripleIter) -> Vec<IdTriple> {
        let mut v: Vec<IdTriple> = it.collect();
        v.sort();
        v
    }

    /// The whole read surface Phase 1 routes must answer identically on both
    /// arms. Ids are comparable directly: both arms read the same layers, so
    /// they share one id space.
    #[test]
    fn both_arms_agree_on_the_whole_read_surface() {
        let (m, l) = arms();

        assert_eq!(m.name(), l.name());
        assert!(m == l, "same layer, so equal regardless of arm");

        // chain metadata
        assert_eq!(
            m.try_node_and_value_count().unwrap(),
            l.try_node_and_value_count().unwrap()
        );
        assert_eq!(
            m.try_predicate_count().unwrap(),
            l.try_predicate_count().unwrap()
        );
        assert_eq!(
            m.retrieve_layer_stack_names().unwrap(),
            l.retrieve_layer_stack_names().unwrap()
        );
        assert_eq!(
            m.parent().unwrap().map(|p| p.name()),
            l.parent().unwrap().map(|p| p.name())
        );
        assert!(
            m.parent().unwrap().is_some(),
            "test graph must have a chain"
        );

        // forward resolution
        let sid = m.try_subject_id("s0100").unwrap();
        assert!(sid.is_some(), "resolution check must not be vacuous");
        assert_eq!(sid, l.try_subject_id("s0100").unwrap());
        assert_eq!(
            m.try_predicate_id("p").unwrap(),
            l.try_predicate_id("p").unwrap()
        );
        assert_eq!(
            m.try_object_node_id("s0101").unwrap(),
            l.try_object_node_id("s0101").unwrap()
        );
        assert_eq!(
            m.try_subject_id("nonexistent").unwrap(),
            l.try_subject_id("nonexistent").unwrap()
        );
        assert!(m.try_subject_id("nonexistent").unwrap().is_none());

        let entry = <String as tdb_succinct::TdbDataType>::make_entry(&"o0100");
        assert_eq!(
            m.try_object_value_id(&entry).unwrap(),
            l.try_object_value_id(&entry).unwrap()
        );

        // reverse resolution
        let sid = sid.unwrap();
        assert_eq!(
            m.try_id_subject(sid).unwrap(),
            l.try_id_subject(sid).unwrap()
        );
        assert_eq!(m.try_id_subject(sid).unwrap().as_deref(), Some("s0100"));
        let pid = m.try_predicate_id("p").unwrap().unwrap();
        assert_eq!(
            m.try_id_predicate(pid).unwrap(),
            l.try_id_predicate(pid).unwrap()
        );
        let oid = m.try_object_value_id(&entry).unwrap().unwrap();
        assert_eq!(m.try_id_object(oid).unwrap(), l.try_id_object(oid).unwrap());

        // existence, both ways
        assert!(m.try_triple_exists(sid, pid, oid).unwrap());
        assert_eq!(
            m.try_triple_exists(sid, pid, oid).unwrap(),
            l.try_triple_exists(sid, pid, oid).unwrap()
        );
        let absent = m
            .try_object_value_id(&<String as tdb_succinct::TdbDataType>::make_entry(&"o0101"))
            .unwrap()
            .unwrap();
        assert!(!m.try_triple_exists(sid, pid, absent).unwrap());
        assert_eq!(
            m.try_triple_exists(sid, pid, absent).unwrap(),
            l.try_triple_exists(sid, pid, absent).unwrap()
        );

        // traversal
        assert!(!sorted(m.try_triples_s(sid).unwrap()).is_empty());
        assert_eq!(
            sorted(m.try_triples_s(sid).unwrap()),
            sorted(l.try_triples_s(sid).unwrap())
        );
        assert_eq!(
            sorted(m.try_triples_sp(sid, pid).unwrap()),
            sorted(l.try_triples_sp(sid, pid).unwrap())
        );
        assert_eq!(
            sorted(m.try_triples_o(oid).unwrap()),
            sorted(l.try_triples_o(oid).unwrap())
        );
        assert_eq!(
            sorted(m.try_triples_p(pid).unwrap()),
            sorted(l.try_triples_p(pid).unwrap())
        );
        assert_eq!(
            sorted(m.try_triples().unwrap()),
            sorted(l.try_triples().unwrap())
        );

        // deltas -- the head is a child, so these are non-empty
        let adds = sorted(m.triple_additions().unwrap());
        let rems = sorted(m.triple_removals().unwrap());
        assert!(!adds.is_empty() && !rems.is_empty());
        assert_eq!(adds, sorted(l.triple_additions().unwrap()));
        assert_eq!(rems, sorted(l.triple_removals().unwrap()));

        let a = adds[0];
        assert_eq!(
            m.triple_addition_exists(a.subject, a.predicate, a.object)
                .unwrap(),
            l.triple_addition_exists(a.subject, a.predicate, a.object)
                .unwrap()
        );
        let r = rems[0];
        assert_eq!(
            m.triple_removal_exists(r.subject, r.predicate, r.object)
                .unwrap(),
            l.triple_removal_exists(r.subject, r.predicate, r.object)
                .unwrap()
        );
        assert_eq!(
            sorted(m.triple_additions_s(a.subject).unwrap()),
            sorted(l.triple_additions_s(a.subject).unwrap())
        );
        assert_eq!(
            sorted(m.triple_additions_sp(a.subject, a.predicate).unwrap()),
            sorted(l.triple_additions_sp(a.subject, a.predicate).unwrap())
        );
        assert_eq!(
            sorted(m.triple_additions_p(a.predicate).unwrap()),
            sorted(l.triple_additions_p(a.predicate).unwrap())
        );
        assert_eq!(
            sorted(m.triple_additions_o(a.object).unwrap()),
            sorted(l.triple_additions_o(a.object).unwrap())
        );
        assert_eq!(
            sorted(m.triple_removals_s(r.subject).unwrap()),
            sorted(l.triple_removals_s(r.subject).unwrap())
        );
        assert_eq!(
            sorted(m.triple_removals_sp(r.subject, r.predicate).unwrap()),
            sorted(l.triple_removals_sp(r.subject, r.predicate).unwrap())
        );
        assert_eq!(
            sorted(m.triple_removals_p(r.predicate).unwrap()),
            sorted(l.triple_removals_p(r.predicate).unwrap())
        );
        assert_eq!(
            sorted(m.triple_removals_o(r.object).unwrap()),
            sorted(l.triple_removals_o(r.object).unwrap())
        );
    }

    /// The `Layer` trait impl is what the Rust readers (GraphQL, documents,
    /// paths) see. It must answer the same as the materialized layer.
    #[test]
    fn layer_trait_reads_agree_on_both_arms() {
        let (m, l) = arms();
        let (m, l): (&dyn Layer, &dyn Layer) = (&m, &l);

        assert_eq!(m.name(), l.name());
        assert_eq!(m.parent_name(), l.parent_name());
        assert_eq!(m.node_and_value_count(), l.node_and_value_count());
        assert_eq!(m.predicate_count(), l.predicate_count());
        assert_eq!(m.triple_addition_count(), l.triple_addition_count());
        assert_eq!(m.triple_removal_count(), l.triple_removal_count());

        let sid = m.subject_id("s0100").expect("must resolve");
        assert_eq!(Some(sid), l.subject_id("s0100"));
        let pid = m.predicate_id("p").expect("must resolve");
        assert_eq!(Some(pid), l.predicate_id("p"));
        assert_eq!(m.object_node_id("s0101"), l.object_node_id("s0101"));
        assert_eq!(m.id_subject(sid), l.id_subject(sid));
        assert_eq!(m.id_predicate(pid), l.id_predicate(pid));

        let entry = <String as tdb_succinct::TdbDataType>::make_entry(&"o0100");
        let oid = m.object_value_id(&entry).expect("must resolve");
        assert_eq!(Some(oid), l.object_value_id(&entry));
        assert_eq!(m.id_object(oid), l.id_object(oid));
        assert_eq!(m.id_object_is_node(oid), l.id_object_is_node(oid));

        assert!(m.triple_exists(sid, pid, oid));
        assert_eq!(
            m.triple_exists(sid, pid, oid),
            l.triple_exists(sid, pid, oid)
        );

        // `single_triple_sp` is the single hottest call in the readers
        assert!(m.single_triple_sp(sid, pid).is_some());
        assert_eq!(m.single_triple_sp(sid, pid), l.single_triple_sp(sid, pid));

        let mut mt: Vec<IdTriple> = m.triples_s(sid).collect();
        let mut lt: Vec<IdTriple> = l.triples_s(sid).collect();
        mt.sort();
        lt.sort();
        assert!(!mt.is_empty());
        assert_eq!(mt, lt);

        // value ranges, block-lazy on the disk-less arm too
        let lo = <String as tdb_succinct::TdbDataType>::make_entry(&"o0100");
        let hi = <String as tdb_succinct::TdbDataType>::make_entry(&"o0200");
        let mut mr: Vec<IdTriple> = m.triples_value_range(&lo, &hi).collect();
        let mut lr: Vec<IdTriple> = l.triples_value_range(&lo, &hi).collect();
        mr.sort();
        lr.sort();
        assert!(!mr.is_empty(), "value-range check must not be vacuous");
        assert!(
            mr.len() < m.triples().count(),
            "range must be a strict subset"
        );
        assert_eq!(mr, lr);

        let mut mr: Vec<IdTriple> = m.triples_value_range_rev(&lo, &hi).collect();
        let mut lr: Vec<IdTriple> = l.triples_value_range_rev(&lo, &hi).collect();
        mr.sort();
        lr.sort();
        assert_eq!(mr, lr);

        for (a, b) in [
            (m.triples_sp(sid, pid), l.triples_sp(sid, pid)),
            (m.triples_o(oid), l.triples_o(oid)),
            (m.triples_p(pid), l.triples_p(pid)),
            (m.triples(), l.triples()),
        ] {
            let mut a: Vec<IdTriple> = a.collect();
            let mut b: Vec<IdTriple> = b.collect();
            a.sort();
            b.sort();
            assert_eq!(a, b);
        }
    }

    /// The safety property the whole disk-less `Layer` impl rests on: a failed
    /// read yields an empty result *and* records the error, so a caller that
    /// checks cannot mistake the failure for a legitimately empty answer.
    #[test]
    fn a_failed_disk_less_read_is_recorded_not_silently_empty() {
        let (store, _) = graph();
        // a well-formed name that was never stored: every read against it fails
        let missing = ReadLayer::Lazy(DisklessLayer::new(
            store.lazy_layer([0xdead, 0xbeef, 0, 0, 0]),
        ));

        assert!(
            missing.take_error().is_none(),
            "nothing read yet, so nothing recorded"
        );

        // reads through the infallible trait look empty...
        assert_eq!(Layer::subject_id(&missing, "s0100"), None);
        assert_eq!(Layer::triples_s(&missing, 1).count(), 0);

        // ...but the failure was recorded, which is what turns it back into an
        // exception at the query boundary
        let err = missing
            .take_error()
            .expect("a failed read must be recorded, not silently empty");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");

        // taking clears it, so the next query starts clean
        assert!(missing.take_error().is_none());

        // and a materialized layer never records anything
        let (m, _) = arms();
        assert_eq!(Layer::subject_id(&m, "nonexistent"), None);
        assert!(m.take_error().is_none());
    }

    /// Chain-cumulative counts must agree on both arms. These looked like they
    /// had to be materialized-only -- computing them seemed to need every
    /// ancestor's adjacency -- but summing the per-layer counts is a handful of
    /// cached metadata reads.
    #[test]
    fn counts_agree_without_materializing() {
        let (m, l) = arms();

        assert!(m.try_triple_count().unwrap() > 0, "not vacuous");
        assert_eq!(m.try_triple_count().unwrap(), l.try_triple_count().unwrap());
        assert_eq!(
            m.try_triple_addition_count().unwrap(),
            l.try_triple_addition_count().unwrap()
        );
        assert_eq!(
            m.try_triple_removal_count().unwrap(),
            l.try_triple_removal_count().unwrap()
        );
        assert!(
            m.try_triple_removal_count().unwrap() > 0,
            "the test graph has removals, so this is not vacuous either"
        );
        assert_eq!(
            m.triple_layer_addition_count().unwrap(),
            l.triple_layer_addition_count().unwrap()
        );
        assert_eq!(
            m.triple_layer_removal_count().unwrap(),
            l.triple_layer_removal_count().unwrap()
        );
        // and none of that left an error behind
        assert!(l.take_error().is_none());
    }

    /// Writing to a disk-less layer must work, by materializing on demand.
    ///
    /// Building a child on top of a layer inherently needs the whole layer, so
    /// there is no block-lazy version. Refusing instead would mean a disk-less
    /// store could not be written to at all, which would make it useless as a
    /// drop-in store.
    #[test]
    fn writes_on_a_disk_less_layer_materialize_rather_than_fail() {
        let (m, l) = arms();

        let builder = l
            .open_write()
            .expect("a disk-less layer must still be writable");
        builder
            .add_value_triple(ValueTriple::new_string_value("s9999", "p", "o9999"))
            .unwrap();
        let child = builder.commit().unwrap();
        assert_eq!(child.parent_name(), Some(l.name()));

        // the write really landed, and is visible from a disk-less read of it
        assert!(
            m.rollup().is_ok(),
            "history ops work on the materialized arm"
        );
        assert!(
            l.squash().is_ok(),
            "and on the disk-less arm, by materializing"
        );
    }

    /// `into_materialized` is what keeps the not-yet-disk-less Rust readers
    /// honest: it must distinguish the arms rather than silently unwrapping.
    #[test]
    fn materialized_accessor_distinguishes_the_arms() {
        let (m, l) = arms();
        assert!(m.materialized().is_some());
        assert!(l.materialized().is_none());
        assert!(m.into_materialized().is_some());
        assert!(l.into_materialized().is_none());
    }
}
