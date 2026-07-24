% Differential test of the disk-less storage path (Phase 1).
%
% Builds one graph on an object-backed store, then queries it twice -- once
% through a materialized layer, once through a disk-less one -- and asserts the
% two answer every routed predicate identically. The layer is deliberately big
% enough (700+ entries) that its dictionaries are block-addressed, so the
% disk-less arm exercises real block-lazy reads rather than the small-dictionary
% whole-load fallback, and the head is a child layer so the delta predicates
% have something to find.
%
% Run it against a built dylib:
%
%   cp src/rust/target/release/libterminusdb_dylib.so /some/dir/librust.so
%   cd /some/dir && swipl -g true .../tests/manual/diskless_storage.pl
%
% (LD_LIBRARY_PATH must include the directory holding libswipl.so.)

:- use_foreign_library('librust.so').

xsd_string('http://www.w3.org/2001/XMLSchema#string').

build(Store, Graph) :-
    terminus_store:open_object_store(memory, 'p/', 100000000, Store),
    terminus_store:create_named_graph(Store, "g", Graph),
    terminus_store:open_write(Store, B),
    xsd_string(X),
    forall(between(0, 699, I),
           ( format(atom(S), "s~|~`0t~d~4+", [I]),
             format(atom(O), "o~|~`0t~d~4+", [I]),
             terminus_store:nb_add_object_triple(B, S, "p", value(O, X)) )),
    terminus_store:nb_commit(B, L0),
    terminus_store:nb_set_head(Graph, L0),
    % child layer: additions + removals, so deltas are non-trivial
    terminus_store:open_write(Graph, B2),
    forall(between(700, 739, I),
           ( format(atom(S), "s~|~`0t~d~4+", [I]),
             format(atom(O), "o~|~`0t~d~4+", [I]),
             terminus_store:nb_add_object_triple(B2, S, "p", value(O, X)) )),
    forall(between(0, 19, I),
           ( format(atom(S), "s~|~`0t~d~4+", [I]),
             format(atom(O), "o~|~`0t~d~4+", [I]),
             terminus_store:nb_remove_object_triple(B2, S, "p", value(O, X)) )),
    terminus_store:nb_commit(B2, L),
    terminus_store:nb_set_head(Graph, L).

% Everything Phase 1 routes, gathered from one layer handle.
probe(Layer, probe(Sid,Pid,Oid,Subj,Pred,Obj,NV,PC,Stack,Exists,NotExists,
                   NTriples,NS,NSP,NO,NP,NAdds,NRems,AddEx,RemEx)) :-
    xsd_string(X),
    terminus_store:subject_to_id(Layer, "s0100", Sid),
    terminus_store:predicate_to_id(Layer, "p", Pid),
    terminus_store:object_to_id(Layer, value("o0100", X), Oid),
    terminus_store:id_to_subject(Layer, Sid, Subj),
    terminus_store:id_to_predicate(Layer, Pid, Pred),
    terminus_store:id_to_object(Layer, Oid, Obj),
    terminus_store:node_and_value_count(Layer, NV),
    terminus_store:predicate_count(Layer, PC),
    terminus_store:retrieve_layer_stack_names(Layer, Stack),
    ( terminus_store:id_triple(Layer, Sid, Pid, Oid) -> Exists = true ; Exists = false ),
    terminus_store:object_to_id(Layer, value("o0101", X), Other),
    ( terminus_store:id_triple(Layer, Sid, Pid, Other) -> NotExists = true ; NotExists = false ),
    findall(t(S,P,O), terminus_store:id_triple(Layer,S,P,O), Ts), length(Ts, NTriples),
    findall(x, terminus_store:id_triple(Layer,Sid,_,_), L1), length(L1, NS),
    findall(x, terminus_store:id_triple(Layer,Sid,Pid,_), L2), length(L2, NSP),
    findall(x, terminus_store:id_triple(Layer,_,_,Oid), L3), length(L3, NO),
    findall(x, terminus_store:id_triple(Layer,_,Pid,_), L4), length(L4, NP),
    findall(t(S5,P5,O5), terminus_store:id_triple_addition(Layer,S5,P5,O5), Adds), length(Adds, NAdds),
    findall(t(S6,P6,O6), terminus_store:id_triple_removal(Layer,S6,P6,O6), Rems), length(Rems, NRems),
    Adds = [t(AS,AP,AO)|_],
    ( terminus_store:id_triple_addition(Layer,AS,AP,AO) -> AddEx = true ; AddEx = false ),
    Rems = [t(RS,RP,RO)|_],
    ( terminus_store:id_triple_removal(Layer,RS,RP,RO) -> RemEx = true ; RemEx = false ).

main :-
    build(Store, Graph),

    % materialized view
    terminus_store:head(Graph, MLayer),
    probe(MLayer, MP),

    % disk-less view of the very same store/bucket
    terminus_store:store_diskless(Store, DStore),
    terminus_store:open_named_graph(DStore, "g", DGraph),
    terminus_store:head(DGraph, DLayer),
    probe(DLayer, DP),

    ( MP == DP
    -> format("MATCH: disk-less and materialized agree on all ~w probed values~n", [20]),
       MP = probe(_,_,_,_,_,_,NV,_,Stack,_,_,NT,_,_,_,_,NA,NR,_,_),
       length(Stack, Depth),
       format("  nv=~w chain-depth=~w triples=~w additions=~w removals=~w~n",[NV,Depth,NT,NA,NR])
    ;  format("MISMATCH~n  materialized: ~w~n  disk-less   : ~w~n", [MP, DP]), fail ),

    % writes must be refused on the disk-less handle, loudly
    ( catch(terminus_store:open_write(DLayer, _), E, true)
    -> ( var(E) -> format("FAIL: open_write silently succeeded disk-less~n"), fail
       ; format("open_write on a disk-less layer raised: ~w~n", [E]) )
    ;  format("FAIL: open_write failed silently instead of raising~n"), fail ).

:- initialization(main, main).
