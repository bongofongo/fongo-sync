# fongo-sync

A sync kernel for local-first applications. Each device keeps its own SQLite database and exchanges
field-level changes with the others; devices that have seen the same changes hold the same data.
The kernel knows nothing about any particular application.

| crate | holds |
|---|---|
| `fongo-sync-core` | identities, hybrid logical clocks, change records and merge rules; `model`, the reference model of what replicas converge to; the evaluator and row planner that make every merge decision. No I/O. |
| `fongo-sync-sqlite` | the storage adapter: change capture by trigger, the core's facts kept in SQLite, and every row written as the core plans it. |
| `fongo-sync-protocol` | the wire protocol: batch encoding, keys, encrypted and signed batches, request signatures, recovery codes and server messages. Builds for `wasm32-unknown-unknown`. |
| `fongo-sync-client` | a device's side: creating and joining a library, and one sync round against a server, over a transport the application implements with its own HTTP client. |

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
- fields that report conflicts, so an application can combine concurrent edits to text. Through
  `fongo-sync-client`, of two concurrent edits only the device whose envelope lands later in the
  log is told; the other takes the winning value without a report.
- columns it fills when inserting a row, and tables outside the registry whose rows follow a
  merged row.

When two replicas each make a row that is valid alone and together break a rule of the
application's own, such as a partial unique index, the kernel holds one of them back and the
application settles it. `Replica::made` says which replica made an entity and
`Replica::written_here` whether this replica wrote the value a field shows, each read from the
merged changes alone, so that one replica decides and the others wait for its write. Nothing
guarantees the maker ever acts: it may be gone, may no longer register the collection, or may
have uninstalled and installed again under a new id, so an application that waits for it needs
its own way on.

`Replica::suspend` and `resume` let an application migrate its schema without each write being
collected; on resuming, rows changed meanwhile are sent as edits or kept as this replica's own, as
the application asks. `uninstall` removes everything the kernel added.

What replicas must converge to is stated twice, once to run and once to prove; see
[Verification](#verification).

Devices exchange changes through a server that stores encrypted batches and learns nothing about
their content: it sees a library id, a device id, a key epoch, a size and an arrival time.
`fongo-sync-protocol` defines everything that crosses the wire, so a client and any server agree
by depending on it.

`fongo-sync-client` drives a `Replica` against such a server: `sync` reads what devices appended
after its cursor, refuses envelopes it cannot trust, waits at one from a signer the device list
does not hold yet, applies the rest, then seals and appends the outbox. Credentials stay with the application; the client stores only its cursor,
the envelope it is sending and where its recent envelopes landed, in tables of its own.

Not yet here: a server, an HTTP transport, and blob transfer.

```
make check
```

## Structure

Three layers, each tested against the one above it:

1. **Specification.** `model::view` says what every replica must show for a set of changes. It
   is slow and plain on purpose.
2. **Decisions, as pure functions in `fongo-sync-core`.**
   - `source`: what a replica keeps of each change (`admit`), and the `Source` trait: the
     indexed questions the evaluator asks of those facts.
   - `eval`: `Session::target` computes one entity's group, liveness and row on demand, pulling
     only the facts it needs, within explicit `Limits`; `Session::origin`, the writes that made
     the group and that put each value in its row. `Evaluator::affected` names the entities
     whose rows may change when a batch is merged.
   - `plan`: `edits` and `record` turn an application's row edits (a row compared with its
     image) into changes; `write` turns a target into a row action; `clashes` finds groups an
     identity key says are one; `order` orders row actions for tables with NOT NULL, unique and
     foreign-key constraints, parking only the fields that break a cycle and reporting rows it
     cannot write. Its module documentation lists what an adapter must do.
3. **Mechanics.** A storage adapter keeps facts, rows, row images and a mapping from entities to
   local rows; it asks the core what to do and does it. `crates/core/tests/sim` is such an
   adapter over in-memory tables, with no SQL: the executable design of one. `fongo-sync-sqlite`
   is the same design over SQLite, answering the core's questions with indexed queries inside
   one transaction per operation.

What is tested against what (`crates/core/tests`):

| test | checks |
|---|---|
| `eval.rs` | `target` for every gid equals `model::view`, and `origin` the writes behind it, over schemas from a small family and random schemas, with hostile change sets; answers under tight limits do not depend on what was asked before; hostile rewrites of a natural key neither hide the entity nor grow the store |
| `affected.rs` | the incremental law: re-evaluating only what `affected` names after each batch, delivered in random order with repeats, keeps the rows equal to `model::view`, over both kinds of schema |
| `plan.rs` | recorded edits round-trip; `edits` never reports what an adapter wrote; `clashes` equals `model::unmerged_identities`; every step of an ordered plan keeps NOT NULL, unique and foreign-key constraints, and nothing is parked or stuck where no cycle forces it |
| `scaling.rs` | the work of `affected` and `target` grows with what changed, not with what is stored, checked as a ratio between two sizes |
| `work.rs` | counted work of `affected`, `target` and `order` stays within a stated bound over random schemas, batches and plans and a set of named hard shapes (needs the `work-counters` feature) |
| `sim` | several replicas over tables with real constraints, random edits and partial, reordered, per-device, repeated delivery: rows equal `model::view` of what each replica merged after every step and once everything is delivered; no replica sends a value its application did not write, and every write an application made is in the merged state; also every short script over a small alphabet |

`FONGO_SIM_CASES` and `FONGO_EXHAUSTIVE_LENGTH` raise the volume of the last two.

## Verification

One chain, each link checked against the one above it:

1. **Laws, in [Lean 4](https://lean-lang.org).** `lean/` restates the core: clocks, registers
   under both strategies, a replica's state as a fold over the changes it received, alias groups,
   liveness through owners, and the view over several collections. `lean/FongoSync/Laws.lean` states every law in one
   place; the other files prove them. Among them: a replica's registers, groups and view do not
   depend on the order changes arrive in or on duplicates, given that no two changes to one
   register share a clock with different values (an example shows that condition is needed); the
   view depends on nothing but registers, the gids seen and the groups; which writes to a natural
   key field count depends only on the set of writes; and every `Clock::tick` is later than every
   reading issued or observed before it, for a clock that ignores observed readings in or past
   the last millisecond `Hlc::encode` holds and has not yet issued the last reading it holds.
   `make lean` builds the proofs and fails if any law rests on an unproven step; `make check`
   includes it.
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
   sends a value it only held back makes every replica agree on a wrong answer; and every value
   the application wrote must be sent and, once everything is exchanged, be in the merged
   state. Who made each live group and wrote each field, as `Replica::made` and
   `Replica::written_here` answer, is checked against the model at the same steps. The tests read
   the kernel only through `Replica::inspect` and the public lookups, so they hold any
   implementation to the same account, and `Registry::model_schema` gives the model's schema; a
   registry the model cannot answer for is refused. Random scripts run in `tests/property.rs`,
   every script up to a small length in `tests/exhaustive.rs`, both repeatable from a seed;
   `make deep` runs them longer. `tests/departures.rs` holds the ways an earlier adapter was
   seen to depart from the model; `tests/crash.rs` stops an apply and a collect at every
   statement and requires the database unchanged and a second run complete; `tests/bench.rs`
   requires a small batch to run the same statements in a large library as in a small one.
   Where the core plans no write (rows that wait on each other through columns none of which
   accepts NULL), rows wait for the application: ignored tests in `crates/core/tests/plan.rs`,
   `tests/converge.rs` and `tests/property.rs` show it.

Nothing mechanically links the Lean to the Rust. `lean/` mirrors `crates/core/src/change.rs`,
`clock.rs` and `model.rs` by hand, so a proof covers the Rust only as far as that transcription is
faithful. The one cross-check is `make lean-diff` (part of `make check`), which compares output
on fixed cases: `model::view` against the single-collection Lean view, computed and proven equal
to its specification, and against a computed view over several collections, whose liveness is
proven to be its specification's but whose keys and groups are computed without proof;
`Schema::check` against `Schema.Restricted` on schemas that differ in their `together` sets,
ownership and natural keys; and `Clock` against `Clock.lean` on fixed histories. The Lean view
covers several collections, which collection a gid belongs to, the changes `model::view` ignores,
`together` sets and natural keys, but not `Real` values, and it is a specification: it states the
least groups and least live set rather than computing them.

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
