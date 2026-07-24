use crate::layer::*;
use crate::store::*;
use std::io::{self, Write};
use swipl::prelude::*;
use terminus_store::store::sync::*;
use terminus_store::Layer;

predicates! {
    pub semidet fn create_named_graph(context, store_term, graph_name_term, graph_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let graph_name: PrologText = graph_name_term.get_ex()?;

        let graph = context.try_or_die(store.create(&graph_name))?;
        graph_term.unify(&WrappedNamedGraph(ReadNamedGraph::new(graph, (**store).clone(), store.diskless)))
    }

    pub semidet fn open_named_graph(context, store_term, graph_name_term, graph_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let graph_name: PrologText = graph_name_term.get_ex()?;

        match context.try_or_die(store.open(&graph_name))? {
            None => Err(PrologError::Failure),
            Some(graph) => graph_term.unify(&WrappedNamedGraph(ReadNamedGraph::new(graph, (**store).clone(), store.diskless))),
        }
    }

    pub semidet fn delete_named_graph(context, store_term, graph_name_term) {
        let store: WrappedStore = store_term.get_ex()?;
        let graph_name: PrologText = graph_name_term.get_ex()?;

        into_prolog_result(context.try_or_die(store.delete(&graph_name))?)
    }

    #[name("head")]
    pub semidet fn head2(context, graph_term, layer_term) {
        let graph: WrappedNamedGraph = graph_term.get_ex()?;
        match context.try_or_die(graph.head())? {
            None => Err(PrologError::Failure),
            Some(layer) => layer_term.unify(&WrappedLayer(graph.wrap_head(layer))),
        }
    }

    #[name("head")]
    pub semidet fn head3(context, graph_term, layer_term, version_term) {
        let graph: WrappedNamedGraph = graph_term.get_ex()?;
        let (layer_opt, version) = context.try_or_die(graph.head_version())?;
        version_term.unify(version)?;

        if let Some(layer) = layer_opt {
            layer_term.unify(&WrappedLayer(graph.wrap_head(layer)))?;
        }

        Ok(())
    }

    pub semidet fn nb_set_head(context, graph_term, layer_term) {
        let graph: WrappedNamedGraph = graph_term.get_ex()?;
        let layer: WrappedLayer = layer_term.get_ex()?;

        into_prolog_result(context.try_or_die(graph.set_head(&context.try_or_die(layer.require_materialized_head())?))?)
    }

    pub semidet fn nb_force_set_head(context, graph_term, layer_term) {
        let graph: WrappedNamedGraph = graph_term.get_ex()?;
        let layer: WrappedLayer = layer_term.get_ex()?;

        context.try_or_die(graph.force_set_head(&context.try_or_die(layer.require_materialized_head())?))?;

        Ok(())
    }

    #[name("nb_force_set_head")]
    pub semidet fn nb_force_set_head_version(context, graph_term, layer_term, version_term) {
        let graph: WrappedNamedGraph = graph_term.get_ex()?;
        let layer: WrappedLayer = layer_term.get_ex()?;

        let version: u64 = version_term.get_ex()?;

        let result = context.try_or_die(graph.force_set_head_version(&context.try_or_die(layer.require_materialized_head())?, version))?;

        into_prolog_result(result)
    }
}

/// A named graph plus how the layers reached through it should be read.
///
/// The flag is carried from the store that opened the graph, because `head/2`
/// is the main way Prolog obtains a layer -- a disk-less store whose `head`
/// handed back materialized layers would never actually read disk-lessly.
#[derive(Clone)]
pub struct ReadNamedGraph {
    inner: SyncNamedGraph,
    /// The store this graph came from. `SyncNamedGraph` does not expose one,
    /// and minting a disk-less handle needs it.
    store: SyncStore,
    diskless: bool,
}

impl ReadNamedGraph {
    pub fn new(inner: SyncNamedGraph, store: SyncStore, diskless: bool) -> Self {
        Self {
            inner,
            store,
            diskless,
        }
    }

    /// Present a head layer the way this graph's store reads layers.
    fn wrap_head(&self, layer: SyncStoreLayer) -> ReadLayer {
        if self.diskless {
            // The head is already known to exist -- it was just read from the
            // label -- so no existence check is needed here.
            ReadLayer::Lazy(DisklessLayer::new(self.store.lazy_layer(layer.name())))
        } else {
            ReadLayer::Materialized(layer)
        }
    }
}

impl std::ops::Deref for ReadNamedGraph {
    type Target = SyncNamedGraph;
    fn deref(&self) -> &SyncNamedGraph {
        &self.inner
    }
}

wrapped_clone_blob!("named_graph", pub WrappedNamedGraph, ReadNamedGraph);

impl CloneBlobImpl for WrappedNamedGraph {
    fn write(&self, stream: &mut PrologStream) -> io::Result<()> {
        write!(stream, "<named_graph {}>", self.name())
    }
}
