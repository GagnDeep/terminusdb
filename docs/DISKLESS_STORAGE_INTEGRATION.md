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

## 3. Dependency wiring (two edges, not one) — DONE

`terminus-store` reaches this workspace over **two independent edges**. Both must
move together, or cargo resolves two incompatible copies of the crate and every
type crossing between them fails to unify.

1. **`terminusdb-store-prolog` declares it as a *git* dependency.** A
   `[patch.crates-io]` entry does **not** cover a git dependency, and neither does
   the `[patch."<git-url>"]` form when the fork's version differs from what the
   original source resolves to — cargo silently reports `patch ... was not used`
   and carries on with upstream. It has to be repointed in that package's own
   manifest:

   ```toml
   # terminusdb-store-prolog/Cargo.toml
   terminus-store = { git = "https://github.com/GagnDeep/terminusdb-store", \
                      branch = "feat/object-store", features = ["object-store"] }
   ```

   The `features = ["object-store"]` is not optional: without it the entire
   disk-less path is compiled out and the fork behaves exactly like upstream.

2. **`terminusdb-grpc-labelstore-client` depends on it from crates.io.** That edge
   *is* covered by the workspace `[patch.crates-io]`, which must name the same fork.

Verify with `cargo tree -i terminus-store` — exactly one copy should appear, and
`Cargo.lock` should show a single `terminus-store` entry pointing at the fork.

### Build environment

The fork is rebased onto `terminusdb-org/terminusdb-store` main (0.21.7), the
upstream this workspace actually tracks, so no feature declarations need dropping.

Three native dependencies are needed, none of which require root:

```bash
# protoc  — for terminusdb-grpc-labelstore-proto
curl -sSL -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v25.3/protoc-25.3-linux-x86_64.zip
mkdir -p ~/.local/protoc && (cd ~/.local/protoc && unzip -oq /tmp/protoc.zip)

# SWI-Prolog 10.0.1 — for swipl-fli; matches the Makefile's SWIPL_VERSION.
# Needs cmake (rootless tarball from Kitware) plus gcc/make and the
# gmp/zlib/openssl/ncurses/readline headers. Build docs OFF: doc generation
# references the `archive` package, which is skipped without libarchive-dev.
cmake -DCMAKE_INSTALL_PREFIX=$HOME/.local/swipl -DCMAKE_BUILD_TYPE=Release \
      -DINSTALL_DOCUMENTATION=OFF .. && cmake --build . -j$(nproc) && cmake --install .

# libclang — for bindgen inside swipl-fli (any glibc-linked libclang.so works)
```

Then:

```bash
cd src/rust
PATH=$HOME/.local/swipl/bin:$PATH \
PROTOC=$HOME/.local/protoc/bin/protoc \
LIBCLANG_PATH=$HOME/.local/libclang/lib \
BINDGEN_EXTRA_CLANG_ARGS="-I/usr/lib/gcc/x86_64-linux-gnu/11/include" \
LD_LIBRARY_PATH=$HOME/.local/swipl/lib/swipl/lib/x86_64-linux:$LD_LIBRARY_PATH \
cargo build --release
```

`BINDGEN_EXTRA_CLANG_ARGS` supplies clang's builtin headers (`stddef.h`); without
it bindgen fails parsing `/usr/include/unistd.h`. `LD_LIBRARY_PATH` is needed at
run time because the dylib links `libswipl.so.10` from the rootless prefix.

Verified: the full workspace builds and all 39 Rust tests pass against the fork.

---

## 4. Integration — Phase 1 — DONE

Implemented and verified. What landed differs from the original sketch in three
places; each is noted below.

### 4a. `ReadLayer` — a layer that is either materialized or disk-less

`terminusdb-store-prolog/src/layer.rs`:

```rust
pub enum ReadLayer {
    Materialized(SyncStoreLayer),
    Lazy(SyncLazyLayer),
}
wrapped_clone_blob!("layer", pub WrappedLayer, ReadLayer);
```

Rather than matching on the enum at ~30 predicate call sites, `ReadLayer` carries
the whole read surface as inherent methods that dispatch internally, so the
predicates keep reading as `layer.subject_id(..)`.

**Every method returns `io::Result`, including on the materialized arm.** That
uniformity is the point: it forces each predicate through `try_or_die`, so a
failed disk-less read becomes a Prolog *exception*. Reporting a network failure
as "no solution" would let a query return a wrong answer, which is the outcome
an audit store can least afford. This resolves the error-semantics risk in §8.

Writes and history rewriting (`open_write`, squash, rollup) are materialized-only
and rejected explicitly on the disk-less arm, as are the chain-cumulative triple
counts and `stored_size` — a disk-less handle would have to load every ancestor's
adjacency to compute them, which defeats the purpose, so they error rather than
being quietly slow.

### 4b. Routed predicates

All of `id_triple`, `id_triple_addition`, `id_triple_removal`, `subject_to_id`,
`id_to_subject`, `predicate_to_id`, `id_to_predicate`, `object_to_id`,
`id_to_object`, `sp_card`, `op_card`, `node_and_value_count`, `predicate_count`,
`parent`, `retrieve_layer_stack_names` and `layer_equals` dispatch to whichever
arm the layer carries.

**Routed since the block-lazy range work:** `id_triple_value_range` and
`id_triple_value_range_rev` also dispatch to both arms.
(`id_triple_sp_value_next/previous` always worked disk-lessly — they are
implemented as an `sp` scan.)

**Materialized-only:** writes, history rewriting (squash/rollup), the
chain-cumulative triple counts, and `stored_size`. A disk-less handle would have
to load every ancestor's adjacency to compute the counts, which defeats the
purpose, so they raise rather than being quietly slow.

### 4c. Opting in

Three predicates in `terminusdb-store-prolog/src/store.rs`:

| Predicate | Effect |
|---|---|
| `open_object_store(+Bucket, +Prefix, +CacheSize, -Store)` | Object-backed, layers materialized |
| `open_diskless_object_store(+Bucket, +Prefix, +CacheSize, -Store)` | Object-backed, layers read block-lazily |
| `store_diskless(+Store, -DisklessStore)` / `store_materialized(+Store, -Store)` | A *view* of an already-open store |

`Bucket` is the atom `memory` (in-process, for tests) or an S3 bucket name, in
which case credentials, region and any endpoint override come from the standard
AWS environment variables — no secret passes through a Prolog term.

The view predicates were not in the original sketch and turn out to be the useful
shape: a process usually wants both handles — writes on the materialized one,
queries on the disk-less one — and opening the bucket twice would give two
independent caches (and, for an in-process bucket, two unrelated stores).

**`WrappedStore` and `WrappedNamedGraph` now wrap `ReadStore`/`ReadNamedGraph`**,
thin structs carrying the flag and `Deref`-ing to the underlying handle so every
existing call site is untouched. The named graph has to carry it too, because
`head/2` — not `store_id_layer/3` — is how Prolog usually obtains a layer; a
disk-less store whose `head` returned materialized layers would never actually
read disk-lessly.

### 4d. `terminusdb-community` — two call sites, contrary to the original plan

The plan claimed Phase 1 touched `store-prolog` only. It does not:
`terminusdb-community/src/types.rs` reads `WrappedLayer.0` as a concrete
`SyncStoreLayer`, so changing the blob's payload breaks it.

`transaction_instance_layer` / `transaction_schema_layer` now raise
`diskless_layer_unsupported_by_rust_readers` when handed a disk-less layer.
Returning `None` would have been a quieter change and a much worse one — a
GraphQL query on a disk-less store would answer as though the database were
empty. Making the Rust readers disk-less aware is Phase 2.

### Verification

- Rust differential tests (`layer.rs`): both arms agree across the entire routed
  read surface over a base+child chain, with non-vacuity assertions; unsupported
  operations are rejected with `ErrorKind::Unsupported`.
- Prolog differential test (`tests/manual/diskless_storage.pl`): one graph built
  on an object store, then queried through both a materialized and a disk-less
  layer — 20 probed values agree — and `open_write` on the disk-less layer raises.
  The layer is sized so its dictionaries are genuinely block-addressed.

```
MATCH: disk-less and materialized agree on all 20 probed values
  nv=1480 chain-depth=2 triples=720 additions=40 removals=20
open_write on a disk-less layer raised:
  error(rust_io_error(Unsupported,open_write requires a materialized layer; ...))
```

---

## 5. Integration — Phase 2 — DONE

The Rust readers (GraphQL, documents, path queries, change detection) now read
through whichever arm the layer carries.

### 5a. A concrete type, not a type parameter

The plan proposed making the readers generic over a read trait. That turned out
to be unnecessary: the readers only ever call `Layer` trait methods, so
implementing `Layer` for `ReadLayer` and **swapping the concrete type**
`SyncStoreLayer` → `ReadLayer` reaches the same place without threading a type
parameter through juniper's generated code. Nine files, one type.

### 5b. The error policy — a sticky error

`Layer` is infallible: it was designed for a layer already resident in RAM,
where a read is a pointer chase. A disk-less layer reads over the network, and
there is no honest infallible answer when that fails.

`DisklessLayer` resolves this with a sticky error. A failed read records its
error and yields an empty/`None` result; the query boundary then calls
`check_diskless_reads`, which turns the record back into an exception, and the
whole result is discarded. **A disk-less query either returns a fully correct
answer or raises — it never returns a partial one dressed up as complete.**

Only the first error is kept (later ones are usually consequences), and the sink
is shared by every clone, so results still funnel back to one place when a reader
hands copies to worker threads — the document reader parallelizes with rayon.

Two supporting facts make this safe rather than merely hopeful:

- Many readers do `layer.id_subject(id).expect(...)`. On a failed disk-less read
  that `None` panics — but `predicates!` wraps every body in
  `prolog_catch_unwind`, so it surfaces as a Prolog exception, not an abort.
  Loud, which is the point.
- The remaining risk is a reader that *tolerates* an empty result and returns
  successfully. That is exactly what `check_diskless_reads` covers.

Checks are installed at all 15 reader entry points: the two GraphQL executions,
the seven document printers, the delete-all path, and the change-detection
predicates.

### 5c. Method-name collision — the one real trap

`ReadLayer`'s inherent fallible methods (`subject_id -> io::Result<Option<u64>>`)
would **shadow** the identically-named `Layer` trait methods, silently giving the
readers the wrong ones. Inherent methods win over trait methods in Rust, so this
is not a compile error at the definition site — it surfaced only as type errors
deep in the readers.

The fallible API is therefore named `try_*` (`try_subject_id`, `try_triples_sp`,
…). The Prolog predicates use those; anything reading through `Layer` gets the
trait methods. The naming also makes the distinction visible at each call site.

### Verification

`layer_trait_reads_agree_on_both_arms` exercises the trait surface the readers
actually use — including `single_triple_sp`, the hottest call in the readers —
against a materialized layer over a base+child chain.

`a_failed_disk_less_read_is_recorded_not_silently_empty` pins the safety
property: a read against a layer that does not exist returns empty *and* records
`NotFound`, taking clears it, and a materialized layer never records anything.

Not yet exercised: a full GraphQL or document query end to end against a
disk-less store. That needs a populated TerminusDB transaction, which is a
server-level fixture rather than a unit test. The readers are covered through the
trait they call, not through a live query.

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

## 7. Testing

SWI-Prolog 10.0.1 is available rootless (see §3, *Build environment*), so all
three tiers below are runnable.

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

- **Error semantics — RESOLVED.** Every `ReadLayer` method returns `io::Result`
  and every predicate routes it through `try_or_die`, so a failed disk-less read
  raises rather than failing. `id_triple` resolves its iterator during `setup`,
  where an error propagates as an exception; once setup succeeds the iterator is
  materialized, so `call` cannot fail mid-solution.
- **Query planner.** WOQL/GraphQL planning may assume O(1) resident access and
  favour "materialize then filter." On a disk-less layer that is the worst pattern;
  prefer the index-driven predicates (`triples_s/sp/o`). Phase 1 keeps existing
  plans (they already call the index-shaped predicates), but heavy full-scan plans
  will be slower disk-less — measure before enabling for such workloads.
- **Request-rate throttling.** The disk-less path issues many small GETs; at high
  query concurrency this approaches S3's ~5,500 GET/s-per-prefix cap. If the
  benchmark's requests/s figure hits it, add request coalescing in terminus-store
  before rolling out broadly.
- **GraphQL is disk-less as of Phase 2**, subject to the sticky-error contract
  in §5b: anything reading a `ReadLayer` through the `Layer` trait must call
  `check_diskless_reads` before reporting results. A new reader entry point that
  forgets to is the live hazard — the type system cannot enforce it.
- **Value-range queries are disk-less as of the block-lazy range work.** For
  each layer in the chain the two bounds are binary-searched in that layer's
  value dictionary and the ids between them mapped into the global id space, so
  cost scales with the width of the range rather than the size of the
  dictionary. No read predicate is materialized-only any more.
- **Opt-in only.** Ship behind the disk-less store variant; the default archive/
  directory paths are unchanged, so nothing regresses for existing deployments.
