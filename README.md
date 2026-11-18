# fongo-sync

A sync kernel for local-first applications. Each device keeps its own SQLite database and exchanges
field-level changes with the others; devices that have seen the same changes hold the same data.
The kernel knows nothing about any particular application.

| crate | holds |
|---|---|
| `fongo-sync-core` | identities, hybrid logical clocks, change records and merge rules. No I/O. |
| `fongo-sync-sqlite` | change capture by trigger, and applying remote changes to an application's own tables. |

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

The merge rules are also stated as laws and proven in [Bend](https://bend-lang.com): `model.bend`
restates them, `LAWS.bend` says what they must guarantee, and `PROOF.bend` proves it. The proofs
are about the model, which mirrors `crates/core/src/change.rs` and `clock.rs` by hand.

Not yet here: the wire protocol, encryption, and a server.

```
make check
```

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
