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

A collection can declare identities, such as an ISBN column. When two replicas create the same
thing independently, the two entities become one on every replica: the kernel records that their
ids are aliases, keeps one row, points every reference at it, and merges their fields by clock.

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
