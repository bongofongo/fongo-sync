//! Every script up to a small length, rather than a sample of long ones: replicas of a tiny
//! library write and send each write as a batch, and take one another's batches in any order,
//! any number of times. Then every replica sends and takes what it lacks, odd ones newest first, until no one
//! sends anything. After every apply and collect each replica must agree with the reference model,
//! and at the end each must show the model's view.
//!
//! `EXHAUSTIVE_STEPS` sets the script length (default 3) and `EXHAUSTIVE_REPLICAS` the number of
//! replicas (default 2). `EXHAUSTIVE_SEEDS` runs each script under that many worlds, which order
//! node ids differently (default 1). Each script and world collects whole or in bounded steps of
//! one or two rows, by turns, and the model is checked after every step. Each script runs with
//! every database alone with its node, where no apply may report a shared one; each in which
//! replica 1 writes runs again, collecting whole, with replicas 0 and 1 copies of one database,
//! whose writes share clocks: every third of them, or every one with `EXHAUSTIVE_COPIED_EVERY=1`,
//! as `make deep` sets.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use fongo_sync_sqlite::core::Change;
use fongo_sync_sqlite::{Collection, Registry, Replica};
use sqlx::SqliteConnection;
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        title TEXT)",
    "CREATE TABLE tags (
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (shelf_id, tag))",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("shelves", "shelves", "id")
            .field("code")
            .field("name")
            .identity(&["code"]),
        Collection::minted("books", "books", "id")
            .owner("shelf_id", "shelves")
            .field("title"),
        Collection::natural("tags", "tags", &["shelf_id", "tag"])
            .owner("shelf_id", "shelves")
            .field("tag")
            .field("note"),
    ])
    .unwrap()
}

/// What one replica's application does, always to its first row of a kind. Making a shelf also
/// puts a book and a tag on it, so short scripts reach merges that carry children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Write {
    MakeShelf,
    RenameShelf,
    Recode,
    DeleteShelf,
    AddBook,
    Retitle,
    DeleteBook,
    NoteTag,
    DeleteTag,
}

const WRITES: &[Write] = &[
    Write::MakeShelf,
    Write::RenameShelf,
    Write::Recode,
    Write::DeleteShelf,
    Write::AddBook,
    Write::Retitle,
    Write::DeleteBook,
    Write::NoteTag,
    Write::DeleteTag,
];

impl Write {
    fn sql(self, replica: usize) -> Vec<String> {
        let shelf = "(SELECT min(id) FROM shelves)";
        let one = match self {
            Write::MakeShelf => {
                return vec![
                    "INSERT OR IGNORE INTO shelves (code, name) VALUES ('a', 'new')".into(),
                    format!(
                        "INSERT INTO books (shelf_id, title)
                         SELECT {shelf}, 'new' WHERE {shelf} IS NOT NULL"
                    ),
                    format!(
                        "INSERT OR IGNORE INTO tags (shelf_id, tag)
                         SELECT {shelf}, 't' WHERE {shelf} IS NOT NULL"
                    ),
                ];
            }
            Write::RenameShelf => {
                format!("UPDATE shelves SET name = 'by {replica}' WHERE id = {shelf}")
            }
            Write::Recode => format!(
                "UPDATE OR IGNORE shelves
                    SET code = CASE code WHEN 'a' THEN 'b' ELSE 'a' END WHERE id = {shelf}"
            ),
            Write::DeleteShelf => format!("DELETE FROM shelves WHERE id = {shelf}"),
            Write::AddBook => format!(
                "INSERT INTO books (shelf_id, title) SELECT {shelf}, 'new' WHERE {shelf} IS NOT NULL"
            ),
            Write::Retitle => format!(
                "UPDATE books SET title = 'by {replica}' WHERE id = (SELECT min(id) FROM books)"
            ),
            Write::DeleteBook => "DELETE FROM books WHERE id = (SELECT min(id) FROM books)".into(),
            Write::NoteTag => format!(
                "UPDATE tags SET note = 'by {replica}'
                  WHERE rowid = (SELECT min(rowid) FROM tags)"
            ),
            Write::DeleteTag => {
                "DELETE FROM tags WHERE rowid = (SELECT min(rowid) FROM tags)".into()
            }
        };
        vec![one]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// The replica writes, then collects and sends its outbox as one batch.
    Write(usize, Write),
    /// The replica takes the batch of the `n`th write in the script, which another replica sent,
    /// again if it already has.
    Take(usize, usize),
}

/// Every script of `steps` operations among `replicas`, with replica 0 acting first, since
/// replicas are interchangeable.
fn scripts(replicas: usize, steps: usize) -> Vec<Vec<Op>> {
    fn extend(replicas: usize, steps: usize, script: &mut Vec<Op>, out: &mut Vec<Vec<Op>>) {
        if script.len() == steps {
            out.push(script.clone());
            return;
        }
        let senders: Vec<usize> = script
            .iter()
            .filter_map(|op| match op {
                Op::Write(r, _) => Some(*r),
                Op::Take(..) => None,
            })
            .collect();
        let actors = if script.is_empty() { 1 } else { replicas };
        let mut next = Vec::new();
        for r in 0..actors {
            next.extend(WRITES.iter().map(|w| Op::Write(r, *w)));
            for (n, sender) in senders.iter().enumerate() {
                if *sender != r {
                    next.push(Op::Take(r, n));
                }
            }
        }
        for op in next {
            script.push(op);
            extend(replicas, steps, script, out);
            script.pop();
        }
    }
    let mut out = Vec::new();
    extend(replicas, steps, &mut Vec::new(), &mut out);
    out
}

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    /// Rows a bounded step of collecting takes, or `None` to collect whole.
    step: Option<usize>,
    taken: Vec<Change>,
    /// Which batches this peer has taken.
    has: Vec<bool>,
    writes: support::writes::Writes,
}

impl Peer {
    async fn new(world: &World, n: u64, step: Option<usize>) -> Peer {
        let mut conn = world.connect(n).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let replica = Replica::new(registry());
        replica.install(&mut conn).await.unwrap();
        Peer {
            conn,
            replica,
            step,
            taken: Vec::new(),
            has: Vec::new(),
            writes: Default::default(),
        }
    }

    async fn send(&mut self, batches: &mut Vec<Vec<Change>>) -> Result<(), String> {
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        match self.step {
            None => {
                self.replica.collect(&mut self.conn).await.unwrap();
            }
            Some(rows) => loop {
                let collected = self
                    .replica
                    .collect_step(&mut self.conn, rows)
                    .await
                    .unwrap();
                self.writes.after(&mut self.conn, &self.replica).await;
                let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
                let mut seen = self.taken.clone();
                seen.extend(outbox.changes);
                support::midway(&mut self.conn, &self.replica, &seen).await?;
                if !collected.more {
                    break;
                }
                self.writes
                    .before(&mut self.conn, &self.replica, &seen)
                    .await;
            },
        }
        self.writes.after(&mut self.conn, &self.replica).await;
        let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
        let unwritten = self
            .writes
            .unwritten(&mut self.conn, &self.replica, &self.taken, &outbox.changes)
            .await;
        if !unwritten.is_empty() {
            return Err(unwritten.join("\n"));
        }
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.taken.extend(outbox.changes.iter().cloned());
        batches.push(outbox.changes);
        support::midway(&mut self.conn, &self.replica, &self.taken).await
    }

    /// Takes batch `n`. With `alone`, no two databases share a node, so the apply must report
    /// none.
    async fn take(&mut self, batches: &[Vec<Change>], n: usize, alone: bool) -> Result<(), String> {
        self.has.resize(batches.len(), false);
        self.has[n] = true;
        self.taken.extend(batches[n].iter().cloned());
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        let applied = self
            .replica
            .apply(&mut self.conn, &batches[n])
            .await
            .unwrap();
        self.writes.touched(&applied.touched);
        self.writes.after(&mut self.conn, &self.replica).await;
        if alone && !applied.shared.is_empty() {
            return Err(format!("an apply reports {:?}", applied.shared));
        }
        support::midway(&mut self.conn, &self.replica, &self.taken).await
    }
}

/// Runs `script`. With `copied`, replica 0 first makes a shelf and sends it, and replica 1's
/// database is then a copy of replica 0's, which issues the clocks replica 0 issues: with the
/// clock held still from there on, the two give their writes the same clocks.
async fn run(
    seed: u64,
    replicas: usize,
    script: &[Op],
    step: Option<usize>,
    copied: bool,
) -> Result<(), String> {
    let world = World::new(seed);
    let mut peers = Vec::new();
    for n in 0..replicas {
        peers.push(Peer::new(&world, n as u64, step).await);
    }
    let mut batches: Vec<Vec<Change>> = Vec::new();
    if copied {
        for sql in Write::MakeShelf.sql(0) {
            let _ = sqlx::query(&sql).execute(&mut peers[0].conn).await;
        }
        peers[0].send(&mut batches).await?;
        peers[0].has = vec![true];
        world.hold(true);
        let conn = world.copy(&mut peers[0].conn, replicas as u64).await;
        let copy = Peer {
            conn,
            replica: peers[0].replica.clone(),
            step,
            taken: peers[0].taken.clone(),
            has: vec![true],
            writes: peers[0].writes.clone(),
        };
        peers[1] = copy;
    }
    let first = batches.len();
    for (step, op) in script.iter().enumerate() {
        let done = match *op {
            Op::Write(r, w) => {
                for sql in w.sql(r) {
                    let _ = sqlx::query(&sql).execute(&mut peers[r].conn).await;
                }
                peers[r].send(&mut batches).await
            }
            Op::Take(r, n) => peers[r].take(&batches, first + n, !copied).await,
        };
        done.map_err(|e| format!("after step {step}, {op:?}: {e}"))?;
    }
    for round in 0..20 {
        let before = batches.iter().filter(|b| !b.is_empty()).count();
        for (r, peer) in peers.iter_mut().enumerate() {
            peer.send(&mut batches)
                .await
                .map_err(|e| format!("finishing, round {round}, replica {r} sending: {e}"))?;
        }
        for (r, peer) in peers.iter_mut().enumerate() {
            peer.has.resize(batches.len(), false);
            // Odd replicas take what they lack newest first.
            let order: Vec<usize> = if r % 2 == 1 {
                (0..batches.len()).rev().collect()
            } else {
                (0..batches.len()).collect()
            };
            for n in order {
                if !peer.has[n] {
                    peer.take(&batches, n, !copied).await.map_err(|e| {
                        format!("finishing, round {round}, replica {r} taking {n}: {e}")
                    })?;
                }
            }
        }
        let after = batches.iter().filter(|b| !b.is_empty()).count();
        if after == before {
            let everything: Vec<Change> = batches.iter().flatten().cloned().collect();
            for (r, peer) in peers.iter_mut().enumerate() {
                support::settled(&mut peer.conn, &peer.replica, &peer.taken)
                    .await
                    .map_err(|e| format!("replica {r}: {e}"))?;
                peer.writes
                    .kept(peer.replica.registry(), &everything)
                    .map_err(|e| format!("replica {r}: {e}"))?;
            }
            return Ok(());
        }
    }
    Err("the replicas never stopped sending".into())
}

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn every_short_script_agrees_with_the_model_at_every_step() {
    every_short_script(false);
}

/// The scripts in which replica 1 writes, every `EXHAUSTIVE_COPIED_EVERY`th of them (default 3,
/// all under `make deep`), where replicas 0 and 1 are copies of one database, so that their
/// writes share clocks and differ in value.
#[test]
fn every_short_script_over_a_copied_database_agrees_with_the_model_at_every_step() {
    every_short_script(true);
}

fn every_short_script(copied: bool) {
    let replicas = setting("EXHAUSTIVE_REPLICAS", 2);
    let steps = setting("EXHAUSTIVE_STEPS", 3);
    let seeds = setting("EXHAUSTIVE_SEEDS", 1) as u64;
    let mut all = scripts(replicas, steps);
    if copied {
        // Only a write of replica 1's can share a clock with one of replica 0's.
        all.retain(|s| s.iter().any(|op| matches!(op, Op::Write(1, _))));
        let every = setting("EXHAUSTIVE_COPIED_EVERY", 3).max(1);
        all = all.into_iter().step_by(every).collect();
    }
    let all = Arc::new(all);
    let next = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let started = std::time::Instant::now();
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let (all, next, failures) = (all.clone(), next.clone(), failures.clone());
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(script) = all.get(i) else { break };
                    for seed in 0..seeds {
                        let step = match copied {
                            false => [None, Some(1), Some(2)][(i + seed as usize) % 3],
                            true => None,
                        };
                        let ran = runtime.block_on(run(seed, replicas, script, step, copied));
                        if let Err(e) = ran {
                            failures.lock().unwrap().push((
                                script.clone(),
                                seed,
                                format!("steps {step:?}: {e}"),
                            ));
                        }
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let failures = failures.lock().unwrap();
    eprintln!(
        "{} scripts of {steps} steps over {replicas} replicas, {seeds} seed(s) each, in {:?}",
        all.len(),
        started.elapsed()
    );
    if let Some((script, seed, error)) = failures.iter().min_by_key(|(s, _, _)| s.len()) {
        panic!(
            "{} of {} scripts fail; one of them, seed {seed}:\n{script:?}\n{error}",
            failures.len(),
            all.len() * seeds as usize
        );
    }
}
