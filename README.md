# fongo-sync

A sync kernel for local-first applications. Each device keeps its own SQLite database and exchanges
field-level changes with the others; devices that have seen the same changes hold the same data.
The kernel knows nothing about any particular application.

| crate | holds |
|---|---|
| `fongo-sync-core` | identities, hybrid logical clocks, change records and merge rules. No I/O. |
| `fongo-sync-sqlite` | change capture by trigger, and applying remote changes to an application's own tables. |
| `fongo-sync-protocol` | the wire protocol: batch encoding, keys, encrypted and signed batches, request signatures, recovery codes and server messages. Builds for `wasm32-unknown-unknown`. |

An application declares its synced tables in a `Registry`, calls `Replica::install` when it opens
its database, `Replica::collect` to turn local writes into changes, and `Replica::apply` to merge
changes from elsewhere. Its schema is left as it is; the kernel keeps its state in tables prefixed
`fongo_`.

What replicas must converge to is stated twice, once to run and once to prove; see
[Verification](#verification).

`lean/` restates the core in [Lean 4](https://lean-lang.org) and proves more of it; `make lean`
builds it (it is not part of `make check`). Proven, about the model:

- a register merges commutatively, associatively and idempotently under `Lww` and `Max`, so a
  replica's state depends only on the set of changes it has applied, given that no two changes to
  one field share a clock with different values (and an example shows that condition is needed);
- every `Clock::tick` is later than every reading issued or observed before it, provided observed
  readings have a wall time `Hlc::encode` can write; without that, `observe` can move the clock
  back, and an example shows it;
- alias groups and their canonical gids (the smallest member) depend only on the set of edges;
- the view (live groups, values from live members, references read through canonical gids)
  depends only on the set of changes.

Not covered: several collections and gid claiming, natural keys, `together` sets, `Real` values,
the SQLite layer, and any link between the Lean model and the Rust beyond reading both.

Devices exchange changes through a server that stores encrypted batches and learns nothing about
their content: it sees a library id, a device id, a key epoch, a size and an arrival time.
`fongo-sync-protocol` defines everything that crosses the wire, so a client and any server agree
by depending on it.

Not yet here: an HTTP client, a server, and blob transfer.

```
make check
```

## Verification

Three layers, each checked against the one above it:

1. **Laws, in [Bend](https://bend-lang.com).** `model.bend` is a miniature of the merge rules:
   registers under both strategies, a replica's registers as a fold over the changes it received,
   alias groups as labels computed from a list of aliases, and liveness and group state computed
   from registers and labels. `LAWS.bend` states what they must guarantee, among them that a
   replica's registers, groups and view do not depend on the order changes arrive in or on
   duplicates, and that the view depends on nothing but registers and groups. `PROOF.bend` proves
   every law; `make proof` checks the proofs, and `make verdict` rechecks them with Bend's small
   kernel, which itself has a proof in Lean (it needs Lean installed).
2. **The reference model, in Rust.** `fongo_sync_core::model::view` states the same semantics in
   full and executably: a slow, plain function from a schema and a set of changes to what an
   application sees. Its own tests check that it ignores order and duplicates and hold one case
   for each rule.
3. **The storage adapter.** `fongo-sync-sqlite` will be tested by comparing what each replica
   shows with `model::view` of the changes it has seen, not only with other replicas.

Nothing mechanically links the Bend model to the Rust: `model.bend` mirrors
`crates/core/src/change.rs`, `clock.rs` and `model.rs` by hand, in miniature (a gid is a number, a
value is a number, a collection has one field), so a proof covers the Rust only as far as that
transcription is faithful.

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
