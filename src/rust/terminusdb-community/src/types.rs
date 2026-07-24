use crate::swipl::prelude::*;
use terminusdb_store_prolog::{
    builder::WrappedBuilder, layer::*, terminus_store::store::sync::SyncStoreLayerBuilder,
};

use crate::swipl::atom;

pub fn transaction_instance_layer<C: QueryableContextType>(
    context: &Context<C>,
    transaction_term: &Term,
) -> PrologResult<Option<ReadLayer>> {
    let instance_atom = atom!("instance_objects");
    let read_atom = atom!("read");

    let frame = context.open_frame();
    let list_term = frame.new_term_ref();
    transaction_term.get_dict_key_term(&instance_atom, &list_term)?;

    if let Some(item) = frame.term_list_iter(&list_term).next() {
        let layer: Option<WrappedLayer> = attempt_opt(item.get_dict_key(&read_atom))?;
        Ok(layer.map(|l| l.0.clone()))
    } else {
        Ok(None)
    }
}

pub fn transaction_schema_layer<C: QueryableContextType>(
    context: &Context<C>,
    transaction_term: &Term,
) -> PrologResult<Option<ReadLayer>> {
    let schema_atom = atom!("schema_objects");
    let read_atom = atom!("read");

    let frame = context.open_frame();
    let list_term = frame.new_term_ref();
    transaction_term.get_dict_key_term(&schema_atom, &list_term)?;

    if let Some(item) = frame.term_list_iter(&list_term).next() {
        let layer: Option<WrappedLayer> = attempt_opt(item.get_dict_key(&read_atom))?;
        Ok(layer.map(|l| l.0.clone()))
    } else {
        Ok(None)
    }
}

pub fn transaction_instance_builder<C: QueryableContextType>(
    context: &Context<C>,
    transaction_term: &Term,
) -> PrologResult<Option<SyncStoreLayerBuilder>> {
    let instance_atom = atom!("instance_objects");
    let write_atom = atom!("write");

    let frame = context.open_frame();
    let list_term = frame.new_term_ref();
    transaction_term.get_dict_key_term(&instance_atom, &list_term)?;

    if let Some(item) = frame.term_list_iter(&list_term).next() {
        let layer: Option<WrappedBuilder> = attempt_opt(item.get_dict_key(&write_atom))?;
        Ok(layer.map(|l| l.0))
    } else {
        Ok(None)
    }
}

/// Raise if any disk-less read failed while producing a result.
///
/// Reads through the [`Layer`] trait cannot report failure -- the trait is
/// infallible -- so a disk-less layer records the error and yields an empty
/// result instead. This turns that record back into an exception, and **must**
/// be called before results computed from `layers` are reported to Prolog.
/// Skipping it converts a network failure into a plausible-looking wrong
/// answer, which is precisely the outcome the disk-less path must never
/// produce.
///
/// A no-op for materialized layers, whose reads cannot fail.
///
/// [`Layer`]: terminusdb_store_prolog::terminus_store::Layer
pub fn check_diskless_reads<C: QueryableContextType>(
    context: &Context<C>,
    layers: &[&ReadLayer],
) -> PrologResult<()> {
    for layer in layers {
        if let Some(e) = layer.take_error() {
            // Reuse the crate's io::Error -> Prolog exception conversion.
            context.try_or_die::<(), _>(Err(e))?;
        }
    }
    Ok(())
}
