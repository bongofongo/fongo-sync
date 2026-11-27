# fongo-sync

A sync kernel for local-first applications. Each device keeps its own SQLite database and exchanges
field-level changes with the others; devices that have seen the same changes hold the same data.
The kernel knows nothing about any particular application.

| crate | holds |
|---|---|
| `fongo-sync-core` | identities, hybrid logical clocks, change records and merge rules; `model`, the reference model of what replicas converge to; the evaluator and row planner that make every merge decision. No I/O. |
| `fongo-sync-sqlite` | the storage adapter: change capture by trigger, the core's facts kept in SQLite, and every row written as the core plans it. |
| `fongo-sync-protocol` | the wire protocol: batch encoding, keys, encrypted and signed batches, request signatures, recovery codes, the format of stored files and server messages. Builds for `wasm32-unknown-unknown`. |
| `fongo-sync-client` | a device's side: creating and joining a library, and one sync round against a server, over a transport the application implements with its own HTTP client. |

An application declares its synced tables in a `Registry`, calls `Replica::install` when it opens
its database, `Replica::collect` to turn local writes into changes, and `Replica::apply` to merge
changes from elsewhere. Its schema is left as it is; the kernel keeps its state in tables prefixed
`fongo_`. `Replica::collect_step` collects in steps, each a transaction of its own that says
whether more is left, so that many local writes are collected without holding SQLite's write lock
for long; `Replica::install_step` installs and takes the first step. A step ends only where the
next row is marked with a later time, and the steps end as some placing of ordinary collects
among the same writes would; in every case the tests try, also as one collect of everything,
but where an alias is stamped or a reference names another member of the group it joins. A
step takes about the rows asked for, more where rows depend on later ones: a new row written
again after the rows naming it is collected with all of them, and rows written in one
millisecond are collected together. The rows found in a collection's tables when it is first
installed in steps are recorded by that install and adopted by the steps after it, in batches,
as a whole install at that moment would adopt them; what the application writes meanwhile is
collected after them as its edits. The kernel's tables are of layout 4: a build that knows only
layout 3 refuses to install over them, and this one upgrades layout 3 in place at its next
install. Every call reads the layout first: this build refuses tables of a later layout in every
call, and layout 3 in every call but `install`, `install_step`, `resume`, `uninstall` and
`Replica::suspended`; with no kernel tables, a call that needs them fails with
`Error::NotInstalled`, and with some of them missing every call but `uninstall` fails with
`Error::Corrupt`.

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
`Replica::written_here` whether this replica made the latest write among a list of its fields,
such as those a unique index reads. Each is read from
the merged changes alone, so that one replica decides and the others wait for its write. The
replica that decides acts on its own row of the entity that gives way, found by key: that row
may be held back at an older state than the merge shows, away from the clash. Nothing
guarantees the maker or the latest writer ever acts: it may be gone, may no longer register the
collection, or may have uninstalled and installed again under a new id, so an application that
waits for it needs its own way on.

The merge rules are a function of the set of changes seen, with no condition on clocks, and so
are `model::view` and a replica whose registry had a collection when that collection's changes
arrived. A field merged by clock takes the change with the later clock and, of two at exactly one
clock, the greater value; a field merged by greatest value takes the greater value and, of two
equal ones, the later clock. Values have one total order: NULL, then numbers, then text, blobs and
references, each by bytes. Numbers compare exactly as numbers, an integer just below a real of
equal value, and two values are equal only when identical, so the integer 1 and the real 1.0 stay
different values.

A replica keeps the changes to a collection its registry lacks for a later registry that has it,
one value for each field and clock: the greater of two, but never one the later registry would
ignore in place of one it keeps. That is exact for every field merged by clock or by greatest
value. It is not exact in two cases: two aliases from one gid at one clock, and a key field of a
natural collection written twice at one clock, where the later registry keeps both. Once its
registry has the collection, such a replica can show other rows than the rest, until the change
it dropped is delivered to it again.

Honest clocks never repeat. Two databases that write as one node do repeat them: a copied
database file, or a backup restored over a database that went on without it. Each still converges
with every other, but the kernel cannot tell their writes apart: both answer `Replica::made` and
`Replica::written_here` for the other's entities, conflicts between their edits are not reported,
and two writes at one clock to fields that change together can show a mix, each field taking the
greater value. `Replica::apply` reports what it finds of this as `Applied::shared`, a list of
`SharedNode`, which `fongo-sync-client` passes on in its sync report: a change carrying this
database's own node that this database did not make, or two values written to one field at one
clock by any node. The report is sound but not complete, it is not stored, and nothing is held
back or refused for it.

Builds before this rule kept whichever of two changes to one field at one clock arrived first,
and compared an integer beyond 2^53 with a real near it by converting the integer to a real. On
such changes an older build can show other values than this one, and nothing stops the two from
syncing. A database that settled such a tie under the older rule keeps that answer after it
upgrades, until it is given again the change it then discarded, as reading the log from its start
again would.

`Replica::suspend` and `resume` let an application migrate its schema without each write being
collected; on resuming, rows changed meanwhile are sent as edits or kept as this replica's own, as
the application asks. Suspending in the middle of a first install in steps adopts the rest of what
it found first, in one transaction. The suspension is recorded in the database until `resume`.
Capture also counts as suspended when a collection that was installed lacks any of its triggers
on its table, as when a build that recorded no suspension dropped them, or the application
rebuilt the table or renamed it away; `resume` then compares only those collections' tables.
With `Resume::Baseline`, a row whose edit the triggers captured before capture stopped is still
sent as an edit, unless the triggers may have marked writes to a table renamed away, which a
resume, or a suspend as it drops them, finds by where they are. Then the table is compared alone:
no row it holds is removed or made again, a row removed and made again at its minted id before
capture stopped is the old entity changed, and with `Resume::Baseline` an edit captured and not
collected is kept rather than sent; collecting before a migration avoids these two. While
capture is suspended, `install`, `collect`, `apply` and their bounded forms fail with
`Error::Suspended`, so an application that ends while suspended calls `resume` when it opens
again, and `Replica::suspended` says whether it must. When capture is intact, `resume` is
`install`. `uninstall` removes everything the kernel added, and the client's tables with it.

Each call that writes, made with no transaction open, takes SQLite's write lock with its first
statement and so waits for another connection's write in the busy handler; made inside the
application's own transaction, it relies on that transaction having been begun with
`BEGIN IMMEDIATE`.

What replicas must converge to is stated twice, once to run and once to prove; see
[Verification](#verification).

Devices exchange changes through a server that stores encrypted batches and cannot read them: of
each it sees a library id, a device id, a key epoch, its exact size and an arrival time, and, once
files travel, the stored names of the files the batch claims, though not what the files are.
`fongo-sync-protocol` defines everything that crosses the wire, so a client and any server agree
by depending on it.

The protocol also defines how files travel beside the changes. A change names a file by its
content hash and size, as an ordinary blob value; the server stores the file under an HMAC of
that hash keyed from the library secret, as padded chunks each sealed under a key derived from the
secret and the hash, with each chunk's nonce derived from the chunk itself, so the same file
seals to the same bytes on every device of a library and different bytes never share a nonce. It
defines the routes to upload and read such files, to ask which a server holds, to claim them in
the same step as appending the changes that refer to them, and to retire them, with rules under
which a server deletes a file only once no change in the library can refer to it; the protocol's
documentation states the conditions and limits of that guarantee. It also defines removing a
device: the server then refuses that device and stops waiting for it, which does not exclude
anyone who holds the library secret. No client or server in this repository implements any of
this yet.

`fongo-sync-client` drives a `Replica` against such a server: `sync` reads what devices appended
after its cursor, refuses envelopes it cannot trust, waits at one from a signer the device list
does not hold yet, stops at one a later version of the protocol may have written and reports it,
so that the application can ask for an update and nothing is skipped, applies the rest, then seals
and appends the outbox. `step_past` lets an application give up one such envelope that no build
will read, on the user's say. A sync refuses, before reading or writing anything, a database whose
capture is suspended or whose kernel tables are missing or of another layout. Each sync also
checks that the server still holds the log it read and wrote: it reads the envelope at its cursor
again and compares it with a digest it kept, looks for each envelope it appended at the place the
server named, and takes an append answered at a position it has read for reuse once the envelope
read back there is another. Reading the envelope at the cursor costs its bytes in every sync, up to
the server's largest envelope, and one page read more where the new envelopes fill whole pages. A
log rolled back, restored from a backup, rewritten at the cursor or missing an acknowledged
envelope fails one of these; the sync stores a `LogFault` naming the cause and stops, and every
sync after it, on any connection, fails with it before reaching the server, applying, sealing and
settling nothing, until the application resolves it. A server must never change bytes it has
served at a position, not even to repair damaged storage: a device reports bytes changed at its
cursor as a rewrite. `resend_all` puts everything the device holds in its outbox again, which
entities are one included, and reads the log from its start, so that once it has synced the
server's log again holds every change the device had, and a new device shows its rows; applying a
change twice changes nothing. The device can instead be rebuilt from the server, or left stopped.
The client also keeps a fingerprint of the log it has read, a hash chained over every envelope in
order, which two devices can compare at one position to find a server showing them different
logs; nothing in this stage compares them by itself, and a rollback to a point after everything a
device read and wrote, a rewrite behind its cursor, or a server withholding the newest envelopes,
is not detected. Credentials
stay with the application; the client stores only its cursor, the digest of the envelope there
and the fingerprint, the envelope it is sending, where its recent envelopes landed and where its
pull stopped, and a fault it found, in tables of its own, which record their own layout.
A device makes its credentials and stores them before creating or joining a library, so that a
create or join whose response is lost can be repeated with the same result; each registers the
device's wrap key and a membership proof, which shows the library's other devices that whoever
registered it held the library secret.
`sync_with` takes an `Apply` that applies each batch in the client's transaction, so what an
application derives from the rows an apply touched is committed with them and the cursor, and that
is called once a pull has read the whole log, before the outbox is sent.
A sync shares the database with other writers: local writes are collected in bounded steps
before each apply, each apply takes a bounded number of changes in whole envelopes, each envelope
sent holds a bounded number, and once a sync has held SQLite's write lock for a while it leaves it
free long enough for a writer waiting in SQLite's busy handler to take it. `Apply::collect` runs
each step of collecting in the client's transaction, as `Apply::apply` runs each apply, and is told
what the step wrote; `Apply::between`, called in each gap between transactions, lets an
application send its own writers through, and `Apply::pace` sets the bounds.
Syncs may also run at once on several connections to one database: each seals and records what
it sends in one transaction, so no two seal the same changes, and none erases what another
recorded as being sent or where another's pull stopped. One that comes to send while another's
envelope is in flight sends nothing and says so in its report, and should be run again soon. One
that starts while another's envelope is in flight cannot tell it from one a crash left, and
appends the same bytes again.

Not yet here: a server, an HTTP transport, and blob transfer.

```
make check
```

`make check` formats, lints, tests, builds the protocol for wasm and checks the proofs, as CI
does. It needs [cargo-nextest](https://nexte.st), which runs every test in a process of its own
with all test binaries at once, and [Lean](#verification) through `elan`.

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
| `sim` | several replicas over tables with real constraints, random edits and partial, reordered, per-device, repeated delivery, and replicas replaced by a copy of another's database or by a backup of their own, which issue clocks already issued: rows equal `model::view` of what each replica merged after every step and once everything is delivered; no replica sends a value its application did not write, and every write an application made is in the merged state, or lost a tie at its clock to a greater value; also every short script over a small alphabet |

`FONGO_WORK_CASES` raises the number of random cases in `work.rs`, and `FONGO_SIM_CASES` and
`FONGO_EXHAUSTIVE_LENGTH` those of `sim`: its random scripts and the length of its short ones.
Every property test draws its cases from a fixed seed unless `PROPTEST_RNG_SEED` names another,
so a run is repeatable; `make deep` passes a new seed each time and prints it.

## Verification

One chain, each link checked against the one above it:

1. **Laws, in [Lean 4](https://lean-lang.org).** `lean/` restates the core: clocks, registers
   under both strategies, a replica's state as a fold over the changes it received, alias groups,
   liveness through owners, and the view over several collections. `lean/FongoSync/Laws.lean` states every law in one
   place; the other files prove them. Among them: a replica's registers, groups and view do not
   depend on the order changes arrive in or on duplicates, with no condition on clocks; of two
   changes to one register at one clock the greater value wins, and a register holds the greatest
   of what it was given in the order its strategy names; the
   view depends on nothing but registers, the gids seen and the groups; which writes to a natural
   key field count depends only on the set of writes; a `together` set shows the fields one live
   member wrote at the latest clock any live member wrote to the set, one write where no two
   changes to a register share a clock and otherwise, field by field, the greater value; and
   every `Clock::tick` is later than every reading issued or observed before it, for a clock that
   ignores observed readings in or past the last millisecond `Hlc::encode` holds and has not yet
   issued the last reading it holds.
   The Lean values have no reals: that an integer and a real compare exactly is checked in Rust,
   against a comparison of the two by integer arithmetic.
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
   state. Who made each live group, and wrote the latest of each field and of lists of its
   fields, as `Replica::made` and `Replica::written_here` answer, is checked against the model at
   the same steps. The tests read
   the kernel only through `Replica::inspect` and the public lookups, so they hold any
   implementation to the same account, and `Registry::model_schema` gives the model's schema; a
   registry the model cannot answer for is refused. Random scripts run in `tests/property.rs`,
   every script up to a small length in `tests/exhaustive.rs`, both repeatable from a seed;
   `make deep` runs them longer; both, and `tests/differential.rs`, also collect in bounded
   steps of sizes each case picks, checking the model after every step, some on a clock that moves
   only now and then, so that many writes share a millisecond, and with databases copied or
   restored from a backup, whose writes share clocks; where no database is copied, no apply may
   report a shared node. `tests/copies.rs` holds directed cases of those, and of what an apply
   reports and must not report. `tests/steps.rs` requires
   collecting in steps to send, hold and show what one collect does, and `tests/placings.rs`
   what collects placed between the same writes do. `tests/departures.rs`
   holds the ways an earlier adapter was seen to depart from the model; `tests/crash.rs` stops an
   apply, a collect, a suspend, a resume and every step of a bounded install or collect at every
   statement and requires the database unchanged and the rest to complete; `tests/guards.rs`
   calls every entry point with no kernel tables, with capture suspended and with tables of
   other layouts, and makes another connection write in the middle of every call that writes;
   `tests/bench.rs` requires a small batch to run the same statements in a large library as in a
   small one.
   Where the core plans no write (rows that wait on each other through columns none of which
   accepts NULL), rows wait for the application: ignored tests in `crates/core/tests/plan.rs`,
   `tests/converge.rs` and `tests/property.rs` show it.

Nothing mechanically links the Lean to the Rust. `lean/` mirrors `crates/core/src/change.rs`,
`clock.rs` and `model.rs` by hand, so a proof covers the Rust only as far as that transcription is
faithful. The one cross-check is `make lean-diff` (part of `make check`), which compares output on
fixed cases: `model::view` against the single-collection Lean view, computed and proven equal to
its specification, and against a computed view over several collections, also proven equal to its
specification, keys, groups and liveness; `Schema::check` against `Schema.Restricted`, which
states every rule of `Schema::check` but those on identity keys, on schemas that each break or
keep one rule (a collection owning its own rows also breaks the rule against ownership cycles,
which on both sides refuses it alone, so the rule against owning its own rows is never the only
one tested); and `Clock` against `Clock.lean` on fixed histories. The Lean view covers several
collections, which collection a gid belongs to, the changes `model::view` ignores, `together` sets
and natural keys, but not `Real` values, and it is a specification: it states the first key that
derives a gid, the least groups and the least live set rather than computing them.

## Licence

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT licence](LICENSE-MIT), at your option.

## Contributing

This repository is not accepting contributions at the moment. Pull requests will be closed
unread; bug reports are welcome as issues.
