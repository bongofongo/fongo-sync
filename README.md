# fongo-sync

A sync kernel for local-first applications. Each device keeps its own SQLite database and exchanges
field-level changes with the others; devices that have seen the same changes hold the same data.
The kernel knows nothing about any particular application.

| crate | holds |
|---|---|
| `fongo-sync-core` | identities, hybrid logical clocks, change records and merge rules, and `model`, the reference model of what replicas converge to. No I/O. |
| `fongo-sync-sqlite` | change capture by trigger, and applying remote changes to an application's own tables. |
| `fongo-sync-protocol` | the wire protocol: batch encoding, keys, encrypted and signed batches, request signatures, recovery codes and server messages. Builds for `wasm32-unknown-unknown`. |

An application declares its synced tables in a `Registry`, calls `Replica::install` when it opens
its database, `Replica::collect` to turn local writes into changes, and `Replica::apply` to merge
changes from elsewhere. Its schema is left as it is; the kernel keeps its state in tables prefixed
`fongo_`.

Each collection in the registry is a table with a key: minted (a local integer id the kernel maps
to a global one) or natural (key fields every replica derives the same id from). Its fields are
plain values or references to minted collections, merged by clock or by greatest value; a
reference may own its row, which then goes with its target. A collection can also declare:

- identities, such as an ISBN column. When two replicas create the same thing independently,
  the two entities become one on every replica: the kernel records that their ids are aliases,
  keeps one row, points every reference at it, and merges the live members' fields by clock.
- fields that change together, which merge as one write.
- fields that report conflicts, so an application can combine concurrent edits to text.
- columns it fills when inserting a row, and tables outside the registry whose rows follow a
  merged row.

`Replica::suspend` and `resume` let an application migrate its schema without each write being
collected; on resuming, rows changed meanwhile are sent as edits or kept as this replica's own, as
the application asks. `uninstall` removes everything the kernel added.

What replicas must converge to is stated twice, once to run and once to prove; see
[Verification](#verification).

Devices exchange changes through a server that stores encrypted batches and learns nothing about
their content: it sees a library id, a device id, a key epoch, a size and an arrival time.
`fongo-sync-protocol` defines everything that crosses the wire, so a client and any server agree
by depending on it.

Not yet here: an HTTP client, a server, and blob transfer.

```
make check
```

## Verification

One chain, each link checked against the one above it:

1. **Laws, in [Lean 4](https://lean-lang.org).** `lean/` restates the core: clocks, registers
   under both strategies, a replica's state as a fold over the changes it received, alias groups,
   liveness through owners, and the view over several collections. `lean/FongoSync/Laws.lean` states every law in one
   place; the other files prove them. Among them: a replica's registers, groups and view do not
   depend on the order changes arrive in or on duplicates, given that no two changes to one
   register share a clock with different values (an example shows that condition is needed); the
   view depends on nothing but registers, the gids seen and the groups; and every `Clock::tick`
   is later than every reading issued or observed before it, for a clock that ignores observed
   readings `Hlc::encode` cannot hold. `make lean` builds the proofs and fails if any law rests
   on an unproven step; `make check` includes it.
2. **The reference model, in Rust.** `fongo_sync_core::model::view` states the same semantics in
   full and executably: a slow, plain function from a schema and a set of changes to what an
   application sees. Its own tests check that it ignores order and duplicates and hold one case
   for each rule.
3. **The storage adapter.** `fongo-sync-sqlite`'s tests compare each replica with the model of
   the changes it has taken in, not only with other replicas. After every apply and collect its
   registers and alias groups must be the model's, and every row it is not holding back the
   model's row; once nothing waits, its rows must be exactly `model::view`, with no two sharing
   an identity. What a replica sends is checked against what its application wrote, told apart
   from what the kernel wrote by comparing tables between kernel steps, since a replica that
   sends a value it only held back makes every replica agree on a wrong answer. The tests read
   the kernel only through `Replica::inspect` and the public lookups, so they hold any
   implementation to the same account, and `Registry::model_schema` gives the model's schema; a
   registry the model cannot answer for is refused. Random scripts run in `tests/property.rs`,
   every script up to a small length in `tests/exhaustive.rs`, both repeatable from a seed;
   `make deep` runs them longer. `tests/departures.rs` lists, as ignored tests, the ways the
   kernel is known to depart from the model.

Nothing mechanically links the Lean to the Rust. `lean/` mirrors `crates/core/src/change.rs`,
`clock.rs` and `model.rs` by hand, so a proof covers the Rust only as far as that transcription is
faithful. The one cross-check is `make lean-diff` (part of `make check`): it runs a handful of
fixed cases through the single-collection Lean view, computed and proven equal to its
specification, and through `model::view`, and compares the output. The Lean view covers several collections, which collection a gid belongs to, the
changes `model::view` ignores, `together` sets and natural keys, but not `Real` values, and it is
a specification: it states the least groups and least live set rather than computing them.

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
