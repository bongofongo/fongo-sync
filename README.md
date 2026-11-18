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

The merge rules are also stated as laws and proven in [Bend](https://bend-lang.com): `model.bend`
restates them, `LAWS.bend` says what they must guarantee, and `PROOF.bend` proves it. The proofs
are about the model, which mirrors `crates/core/src/change.rs` and `clock.rs` by hand.

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

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
