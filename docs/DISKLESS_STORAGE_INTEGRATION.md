# Disk-less storage integration plan

**Status:** design / not yet implemented. This document is a precise, file:line
plan for routing TerminusDB's read paths through the **disk-less** query surface
that now exists in `terminus-store` (the `feat/object-store` branch of the fork
`GagnDeep/terminusdb-store`). It was written from a read-only reconnaissance of
this workspace; no code here is changed by it.

> Why: on an object-storage (S3/MinIO/R2) backend, a stateless replica with **no
> local disk** can already answer the common query classes by fetching only the
> blocks a query touches (a selective read moves ~8% of a whole-layer read).
> Everything below is about letting TerminusDB's engine actually *use* that path
> instead of materializing whole layers via `get_layer_from_id`.

---

## 1. What terminus-store now provides

On the fork (`GagnDeep/terminusdb-store@feat/object-store`), the storage crate
exposes a disk-less query handle, both async and sync:

- `terminus_store::store::LazyLayer` (async) and
- `terminus_store::store::sync::SyncLazyLayer` (sync — the one relevant here),
  obtained via `SyncStore::lazy_layer(head: [u32;5]) -> SyncLazyLayer`.

`SyncLazyLayer` mirrors the read methods of the `Layer` trait that
`SyncStoreLayer` implements — existence, traversal (`triples`, `triples_s/sp/p/o`),
forward resolution (`subject_id`, `predicate_id`, `object_node_id`,
`object_value_id`, `value_triple_to_id`), reverse resolution (`id_subject`,
`id_predicate`, `id_object`, `id_triple_to_string`) — **but every method returns
`io::Result<…>`** because a disk-less read is fallible (it does network I/O),
whereas the `Layer` trait is infallible.

That `Result` is the whole crux of the integration: errors must be handled at the
FFI boundary, not swallowed in a trait impl.

Durable batched writes are also disk-less now (`BufferedNamedGraph::open_with_object_wal`,
a bucket-backed WAL), but this document is about reads; writes already go through
one immutable-object PUT + label CAS and need no change.

---

## 2. The read architecture in this workspace (map)

All reads use the **synchronous facade** `SyncStore`/`SyncStoreLayer`; the async
`Store` is only touched transiently in `open_grpc_store`.

- **Store construction:** `terminusdb-store-prolog/src/store.rs:80-123`
  (`open_archive_store` at :101 is the production path). Blob:
  `wrapped_clone_blob!("store", pub WrappedStore, SyncStore, …)` at store.rs:290.
- **Layer materialization (the single seam):** `store_id_layer/3` at
  `terminusdb-store-prolog/src/layer.rs:192`, body at :198 —
  `store.get_layer_from_id(name) -> WrappedLayer(layer)`. This is the *only* place
  a `SyncStoreLayer` is born from the store. Blob:
  `wrapped_clone_blob!("layer", pub WrappedLayer, SyncStoreLayer)` at layer.rs:643.
- **The Prolog read chokepoint:** the nondet FFI predicate **`id_triple`** at
  `terminusdb-store-prolog/src/layer.rs:211`, returning
  `Peekable<Box<dyn Iterator<Item=IdTriple>+Send>>`, dispatching on bound args:
  - all bound → `triple_exists` (:220); S+P → `triples_sp` (:229);
    S+O → `triples_s(..).filter` (:236); S → `triples_s` (:241);
    O+P → `triples_o(..).filter` (:249); O → `triples_o` (:254);
    P → `triples_p` (:259); none → `triples()` (:263).
  - id↔name converters in the same file: `subject_to_id`→`subject_id` (:31),
    `id_to_subject`→`id_subject`, `predicate_to_id`→`predicate_id` (:51),
    `id_to_predicate`→`id_predicate`, `object_to_id`→`object_node_id`/`object_value_id`
    (:75/:79), `id_to_object`→`id_object` (:97). Cardinality: `sp_card`→`triples_sp`
    (:289), `op_card`→`triples_o` (:298). Parallel `id_triple_addition` (:304) and
    `id_triple_removal` (:378) over `triple_additions_*`/`triple_removals_*`.
  - Registered for Prolog in `terminusdb-store-prolog/src/lib.rs:61-69`.
- **The Prolog side above the boundary** never sees a layer type — only the opaque
  `WrappedLayer` blob. WOQL: `xrdf/4` (`src/core/triple/triplestore.pl:356`) →
  `xrdf_db/4` (:502) → `triple/4` (`src/library/terminus_store.pl:549`) →
  `id_triple/4` (:567). Documents call `terminus_store:id_triple(Layer,…)` directly
  (`src/core/document/instance.pl:464/490/501/580/721/760`, `json.pl:3178`).
- **GraphQL is a Rust reader** (`terminusdb-community/src/graphql/query.rs`, ~42
  `Layer`-method hits; also `top.rs`, `system.rs`, `doc/*`, `schema.rs`,
  `path/compile.rs`, `changes.rs`). It iterates `&SyncStoreLayer` directly but
  still *receives* that layer from a Prolog transaction term via
  `transaction_instance_layer` (`terminusdb-community/src/types.rs:10`).

**Implication:** routing the Prolog-side `layer.rs` predicates covers the entire
WOQL + document engine transparently. GraphQL/doc Rust readers are a second,
separable phase (they take a concrete `&SyncStoreLayer`, so they need to become
generic over a read trait to accept a disk-less handle).

---

## 3. Dependency wiring (one line)

`src/rust/Cargo.toml` has a workspace-wide `[patch.crates-io]` for `terminus-store`
with a commented local option. Point it at the fork or a local checkout:

```toml
[patch.crates-io]
# fork with the disk-less API:
terminus-store = { git = "https://github.com/GagnDeep/terminusdb-store", branch = "feat/object-store" }
# or local:
# terminus-store = { path = "../../../terminusdb-store" }
```

Because it's a `crates-io` patch it rewrites `terminus-store` for the whole
workspace; the per-crate git dep in `terminusdb-store-prolog/Cargo.toml` follows
automatically. (Rebuild needs SWI-Prolog dev headers — the `swipl` crate links
`libswipl`.)

---

## 4. Integration — Phase 1 (Prolog/WOQL path, highest leverage, contained)

Goal: an **opt-in** disk-less store whose layers answer `id_triple` and the
converters block-lazily, transparent to all of `src/core/**/*.pl`.

### 4a. A layer that can be either materialized or disk-less
Make `WrappedLayer` hold an enum instead of a bare `SyncStoreLayer`, in
`terminusdb-store-prolog/src/layer.rs`:

```rust
pub enum ReadLayer {
    Materialized(SyncStoreLayer),
    Lazy(SyncLazyLayer),
}
// wrapped_clone_blob!("layer", pub WrappedLayer, ReadLayer)
```

Keep `SyncStoreLayer`'s `open_write`/delta/rollup methods working by having the
write/admin predicates require `ReadLayer::Materialized` (writes stay on the
materialized path; only reads go disk-less).

### 4b. Route the read predicates (all in layer.rs)
For each read predicate, match on `ReadLayer` and, for the `Lazy` arm, call the
`SyncLazyLayer` method and convert `io::Result` → a Prolog error, using this
crate's existing error path (the `context_error!`/`PrologError` machinery already
used elsewhere in store-prolog). Sites, by line:

| Predicate | line | materialized call | disk-less call |
|---|---|---|---|
| `id_triple` | 211 (dispatch 220–263) | `triple_exists`/`triples_*` | same on `SyncLazyLayer`, `?`-propagated |
| `subject_to_id` | 31 | `subject_id` | `subject_id(..)?` |
| `id_to_subject` | ~40 | `id_subject` | `id_subject(..)?` |
| `predicate_to_id` | 51 | `predicate_id` | `predicate_id(..)?` |
| `id_to_predicate` | ~60 | `id_predicate` | `id_predicate(..)?` |
| `object_to_id` | 75/79 | `object_node_id`/`object_value_id` | same `?` |
| `id_to_object` | 97 | `id_object` | `id_object(..)?` |
| `sp_card` | 289 | `triples_sp` | `triples_sp(..)?` |
| `op_card` | 298 | `triples_o` | `triples_o(..)?` |
| `id_triple_addition` / `_removal` | 304 / 378 | `triple_additions_*`/`_removals_*` | same on `SyncLazyLayer` (now available — see §6) |

The `triples_*` methods return `Vec<IdTriple>` on `SyncLazyLayer` (vs a lazy
iterator on `SyncStoreLayer`); wrap in `.into_iter()` and `Peekable<Box<dyn …>>`
to keep `id_triple`'s return type unchanged.

### 4c. An opt-in disk-less store
Add a store-open variant in `terminusdb-store-prolog/src/store.rs` (near :80-123),
e.g. `open_diskless_object_store` / a flag on `open_archive_store`, that builds a
`SyncStore` over the object backend and marks it so `store_id_layer/3` (layer.rs:198)
yields `ReadLayer::Lazy(store.lazy_layer(name))` instead of
`ReadLayer::Materialized(get_layer_from_id(name))`. Default path unchanged.

This is the entire Phase-1 surface: **`store-prolog/src/{store.rs, layer.rs}` only.**
No change to `terminusdb-community`, none to `src/core/**/*.pl`.

---

## 5. Integration — Phase 2 (Rust GraphQL/document readers)

The Rust readers (`terminusdb-community/src/graphql/query.rs`, `top.rs`,
`system.rs`, `doc/*`, `schema.rs`, `path/compile.rs`, `changes.rs`) take a concrete
`&SyncStoreLayer`. To let them read disk-lessly, make them generic over a small
read trait both `SyncStoreLayer` and `SyncLazyLayer` implement (they already call
only trait-shaped methods, so the recon flagged this as mechanical). Two sub-tasks:

1. Define the trait in terminus-store (or a local newtype) covering the read
   methods these files use; impl it for both handles. Decide the error policy
   (the trait can return `io::Result` and callers `?`-propagate, or a fallible
   iterator).
2. Change `transaction_instance_layer`/`transaction_schema_layer`
   (`terminusdb-community/src/types.rs:10/29`) and the readers to be generic.

Phase 2 is larger and can follow Phase 1 once the Prolog path is proven.

---

## 6. Prerequisites in terminus-store — DONE

- **`SyncLazyLayer::triple_additions_*` / `triple_removals_*`** (and the async
  `LazyLayer` equivalents), for the delta predicates `id_triple_addition`/`_removal`
  (layer.rs:304/378): **implemented** on `GagnDeep/terminusdb-store@feat/object-store`.
  `SyncLazyLayer` now exposes `triple_addition_exists` / `triple_removal_exists`,
  `triple_additions` / `triple_removals`, and the `_s/_sp/_p/_o` filtered variants
  of each — disk-less (each loads only this one layer's adjacency), differential-
  tested against the materialized `SyncStoreLayer` over a base+child graph.
- `SyncLazyLayer::triples_*` return `Vec<IdTriple>` (wrap in `.into_iter()` for the
  `Peekable<Box<dyn Iterator>>` the FFI expects); the delta variants return
  `Box<dyn Iterator<Item=IdTriple> + Send>` directly.

So `SyncLazyLayer` now covers the **entire** read surface the store-prolog
boundary uses; Phase 1 needs nothing further from terminus-store.

---

## 7. Testing (requires SWI-Prolog)

1. **Rust unit tests** in `terminusdb-store-prolog` (no Prolog runtime needed for
   the pure-Rust dispatch, if factored out): a disk-less `ReadLayer` answers each
   predicate the same as a materialized one, over an object `InMemory` store.
2. **Prolog integration:** `xrdf/4` and a document round-trip against a disk-less
   store give identical results to the materialized store (the existing
   `tests/` + `src/core/**` test suites, run with the disk-less store variant).
3. **Benchmark:** point `terminus-store`'s `examples/bench_object_store.rs` — or a
   TerminusDB-level query benchmark — at MinIO to confirm the disk-less path's
   byte/latency/request profile end-to-end, and run the concurrency load test to
   check request-rate against the S3 GET-per-prefix cap.

---

## 8. Risks / open decisions

- **Error semantics.** A disk-less read can fail (network). Phase 1 converts
  `io::Result::Err` to a Prolog error at the FFI boundary — good, but the WOQL
  engine must treat a read error as an error, not as "no solution." Verify `id_triple`'s
  nondet contract propagates the exception rather than failing silently.
- **Query planner.** WOQL/GraphQL planning may assume O(1) resident access and
  favour "materialize then filter." On a disk-less layer that is the worst pattern;
  prefer the index-driven predicates (`triples_s/sp/o`). Phase 1 keeps existing
  plans (they already call the index-shaped predicates), but heavy full-scan plans
  will be slower disk-less — measure before enabling for such workloads.
- **Request-rate throttling.** The disk-less path issues many small GETs; at high
  query concurrency this approaches S3's ~5,500 GET/s-per-prefix cap. If the
  benchmark's requests/s figure hits it, add request coalescing in terminus-store
  before rolling out broadly.
- **GraphQL stays materialized until Phase 2** — acceptable; document it.
- **Opt-in only.** Ship behind the disk-less store variant; the default archive/
  directory paths are unchanged, so nothing regresses for existing deployments.
