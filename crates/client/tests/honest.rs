//! Honest servers under losses, failures, copies and crashes, with an upgrade from layout 2 on
//! the way: no sync may find a fault, and every device ends with the same fingerprint, the one the
//! documented definition gives. Then servers that roll back, resolved by resending: every device
//! and a new one end alike. Each run is a function of its seed; `FONGO_HONEST_FROM` and the counts
//! below choose them.

#[allow(dead_code)]
mod support;

use std::sync::{Arc, Mutex};

use fongo_sync_client::{Error, LogFault, create_library, fingerprint, join_library, log_fault};
use support::{Peer, Server, assert_converged, now, scratch};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const LAYOUT_TWO: [&str; 4] = [
    "CREATE TABLE IF NOT EXISTS fongo_client_meta (layout INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    sending_through INTEGER,
    sending_envelope BLOB,
    sending_after INTEGER,
    stopped_seq INTEGER,
    stopped_digest BLOB,
    passing_seq INTEGER,
    passing_digest BLOB)",
    "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    digest BLOB,
    PRIMARY KEY (library, seq))",
    "INSERT INTO fongo_client_meta (layout) VALUES (2)",
];
const STATE_TWO: &str = "library, cursor, sending_through, sending_envelope, sending_after, \
                         stopped_seq, stopped_digest, passing_seq, passing_digest";

async fn as_layout_two(peer: &mut Peer) {
    for sql in [
        "CREATE TEMP TABLE old_state AS SELECT * FROM fongo_client_state".to_string(),
        "CREATE TEMP TABLE old_appended AS SELECT * FROM fongo_client_appended".to_string(),
        "DROP TABLE fongo_client_meta".to_string(),
        "DROP TABLE fongo_client_state".to_string(),
        "DROP TABLE fongo_client_appended".to_string(),
        "DROP TABLE fongo_client_passed".to_string(),
    ]
    .into_iter()
    .chain(LAYOUT_TWO.map(String::from))
    .chain([
        format!("INSERT INTO fongo_client_state SELECT {STATE_TWO} FROM temp.old_state"),
        "INSERT INTO fongo_client_appended SELECT library, seq, through, digest
         FROM temp.old_appended"
            .to_string(),
        "DROP TABLE temp.old_state".to_string(),
        "DROP TABLE temp.old_appended".to_string(),
    ]) {
        peer.run(&sql).await;
    }
}

/// Loses or fails requests at random, from a shared stream.
fn chaos(server: &Server, rng: &Arc<Mutex<Rng>>, lose_in: u64, fail_in: u64) {
    let r = rng.clone();
    server.lose_if(move |_| r.lock().unwrap().below(lose_in) == 0);
    let r = rng.clone();
    server.fail_if(move |_| r.lock().unwrap().below(fail_in) == 0);
}

fn honest(result: &Result<fongo_sync_client::SyncReport, Error>, seed: u64, what: &str) {
    match result {
        Ok(_) | Err(Error::Transport(_)) => {}
        Err(Error::LogFault(f)) => panic!("seed {seed}: {what}: false alarm {f:?}"),
        Err(e) => panic!("seed {seed}: {what}: {e:?}"),
    }
}

async fn write(peer: &mut Peer, rng: &mut Rng, tag: &str) {
    let n = rng.below(3);
    for _ in 0..n {
        let k = rng.below(6);
        let v = rng.next();
        peer.run(&format!(
            "INSERT OR REPLACE INTO settings VALUES ('{tag}{k}', '{v}')"
        ))
        .await;
    }
}

async fn drop_midway(peer: &mut Peer, server: &Server, yields: u64) {
    let fut = peer.sync(server);
    tokio::pin!(fut);
    let mut left = yields;
    let done = std::future::poll_fn(|cx| {
        if fut.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(true);
        }
        if left == 0 {
            return std::task::Poll::Ready(false);
        }
        left -= 1;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await;
    let _ = done;
}

use std::future::Future;

/// The fingerprint as its documentation defines it, written apart from the crate's.
fn independent_chain(library: [u8; 16], log: &[Vec<u8>]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let d = b"fongo-sync/v1/log-chain\0";
    let mut h = Sha256::new();
    h.update(d);
    h.update([0u8]);
    h.update(library);
    let mut f: [u8; 32] = h.finalize().into();
    for (i, e) in log.iter().enumerate() {
        let mut h = Sha256::new();
        h.update(d);
        h.update([1u8]);
        h.update(f);
        h.update(((i + 1) as u64).to_be_bytes());
        h.update(Sha256::digest(e));
        f = h.finalize().into();
    }
    f
}

async fn losses_run(seed: u64) {
    let mut rng = Rng(seed);
    let shared = Arc::new(Mutex::new(Rng(seed ^ 0xabcdef)));
    let server = Server::new();
    let page = 1 + rng.below(4) as u32;
    let again = rng.below(2) == 0;
    server.with(|s| {
        s.caps.max_page_batches = Some(page);
        s.store_again = again;
    });
    let dir = scratch(&format!("honest-{seed}"));
    let path = dir.join("a.db");
    let made = create_library(&server, now).await.unwrap();
    let joined = join_library(&server, &made.recovery_code(), now)
        .await
        .unwrap();
    let id = made.library_id();
    let mut a1 = Peer::new_at(&path, made.clone()).await;
    a1.run("PRAGMA journal_mode = wal").await;
    a1.run("PRAGMA busy_timeout = 10000").await;
    let mut a2 = Peer::open(&path, made.clone()).await;
    a2.run("PRAGMA busy_timeout = 10000").await;
    let mut b = Peer::new(joined).await;
    let upgrade_at = rng.below(40);
    let mut upgraded = false;
    for step in 0..60 {
        write(&mut a1, &mut rng, "a").await;
        write(&mut b, &mut rng, "b").await;
        if step >= upgrade_at && !upgraded {
            let made: Vec<String> = a1
                .lines("SELECT name FROM sqlite_master WHERE name = 'fongo_client_passed'")
                .await;
            if !made.is_empty() {
                as_layout_two(&mut a1).await;
                upgraded = true;
            }
        }
        let lose_in = 2 + rng.below(6);
        let fail_in = 2 + rng.below(8);
        chaos(&server, &shared, lose_in, fail_in);
        match rng.below(5) {
            0 => {
                let r = b.sync(&server).await;
                honest(&r, seed, "b");
            }
            1 => {
                let r = a1.sync(&server).await;
                honest(&r, seed, "a1");
            }
            2 => {
                let (r1, r2) = tokio::join!(a1.sync(&server), a2.sync(&server));
                honest(&r1, seed, "a1 with a2");
                honest(&r2, seed, "a2 with a1");
            }
            3 => {
                let (r1, r2) = tokio::join!(a2.sync(&server), b.sync(&server));
                honest(&r1, seed, "a2 with b");
                honest(&r2, seed, "b with a2");
            }
            _ => {
                let y = rng.below(40);
                drop_midway(&mut a2, &server, y).await;
                drop(a2);
                a2 = Peer::open(&path, made.clone()).await;
                a2.run("PRAGMA busy_timeout = 10000").await;
            }
        }
        server.behave();
        server.with(|s| s.store_again = again);
        for p in [&mut a1, &mut a2] {
            let f = log_fault(&mut p.conn, id).await.unwrap();
            assert_eq!(f, None, "seed {seed} step {step}");
        }
    }
    server.behave();
    server.with(|s| s.store_again = again);
    let mut peers = [a1, b];
    for _ in 0..40 {
        let mut moved = false;
        for p in peers.iter_mut() {
            let r = p.sync(&server).await;
            honest(&r, seed, "settle");
            let r = r.unwrap();
            moved |= r.sent > 0 || r.received > 0 || r.more;
        }
        if !moved {
            break;
        }
    }
    let log = server.log(id).len() as u64;
    let f0 = fingerprint(&mut peers[0].conn, id).await.unwrap();
    let f1 = fingerprint(&mut peers[1].conn, id).await.unwrap();
    assert_eq!(f0.seq, log, "seed {seed}");
    assert_eq!(f0, f1, "seed {seed}");
    assert_eq!(
        f0.digest,
        independent_chain(id.0, &server.log(id)),
        "seed {seed}"
    );
    assert_converged(&mut peers).await;
    eprintln!("seed {seed}: ok, upgraded {upgraded}, log {log}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn an_honest_server_under_losses_copies_and_crashes_raises_no_fault() {
    let from = env("FONGO_HONEST_FROM", 0);
    for seed in from..from + env("FONGO_HONEST_SEEDS", 4) {
        losses_run(seed).await;
    }
}

/// A device away for thousands of envelopes, pages of one, losses everywhere, then upgraded
/// from layout 2 while two other devices append between every sync, with two connections.
#[tokio::test]
async fn an_upgrade_over_a_growing_log_under_losses_ends_and_sends() {
    for seed in env("FONGO_HONEST_FROM", 0)
        ..env("FONGO_HONEST_FROM", 0) + env("FONGO_HONEST_UPGRADE_SEEDS", 1)
    {
        let mut rng = Rng(seed);
        let shared = Arc::new(Mutex::new(Rng(seed ^ 7)));
        let server = Server::new();
        let page = 1 + rng.below(3) as u32;
        server.with(|s| s.caps.max_page_batches = Some(page));
        let dir = scratch(&format!("honest-upgrade-{seed}"));
        let path = dir.join("a.db");
        let made = create_library(&server, now).await.unwrap();
        let id = made.library_id();
        let joined = join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap();
        let mut a1 = Peer::new_at(&path, made.clone()).await;
        a1.run("PRAGMA journal_mode = wal").await;
        let mut b = Peer::new(joined).await;
        b.run("INSERT INTO settings VALUES ('b0', 'x')").await;
        b.sync(&server).await.unwrap();
        a1.run("INSERT INTO settings VALUES ('a0', 'x')").await;
        a1.sync(&server).await.unwrap();
        let one = server.log(id)[0].clone();
        let len = 500 + rng.below(2500) as usize;
        for _ in 0..len {
            server.inject(id, one.clone());
        }
        // The device away reads the log under losses.
        let mut guard = 0;
        loop {
            chaos(&server, &shared, 300, 500);
            let r = a1.sync(&server).await;
            honest(&r, seed, "catching up");
            server.behave();
            if let Ok(r) = r
                && !r.more
            {
                break;
            }
            guard += 1;
            assert!(guard < 10_000, "seed {seed}: never caught up");
        }
        let cursor = a1.cursor().await;
        as_layout_two(&mut a1).await;
        a1.run("INSERT INTO settings VALUES ('a1', 'y')").await;
        let mut a2 = Peer::open(&path, made.clone()).await;
        a2.run("PRAGMA busy_timeout = 10000").await;
        a1.run("PRAGMA busy_timeout = 10000").await;
        let mut ok_syncs = 0;
        let mut sent = false;
        let bound = (cursor as f64 / (1000.0 * page as f64)).ceil() as usize + 1;
        for round in 0..60 {
            // Others append as fast as a sync reads.
            for _ in 0..(1000 * page as usize + rng.below(200) as usize) {
                server.inject(id, one.clone());
            }
            b.run(&format!(
                "INSERT OR REPLACE INTO settings VALUES ('b', '{round}')"
            ))
            .await;
            let r = b.sync(&server).await;
            honest(&r, seed, "b");
            chaos(&server, &shared, 1500, 3000);
            let (r1, r2) = tokio::join!(a1.sync(&server), a2.sync(&server));
            server.behave();
            honest(&r1, seed, "a1");
            honest(&r2, seed, "a2");
            for r in [&r1, &r2].into_iter().flatten() {
                ok_syncs += 1;
                sent |= r.sent > 0;
            }
            if sent {
                break;
            }
        }
        assert!(sent, "seed {seed}: never sent");
        eprintln!(
            "seed {seed}: page {page}, cursor at upgrade {cursor}, successful syncs before sending {ok_syncs}, bound per connection {bound}"
        );
        assert_eq!(log_fault(&mut a1.conn, id).await.unwrap(), None::<LogFault>);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

async fn random_edit(peer: &mut Peer, rng: &mut Rng, tag: &str) {
    let k = rng.below(4);
    let v = rng.below(1000);
    let sql = match rng.below(7) {
        0 | 1 => format!("INSERT INTO books (title, year) VALUES ('{tag}{v}', {k})"),
        2 => format!(
            "UPDATE books SET title = '{tag}t{v}' WHERE id = (SELECT id FROM books ORDER BY id LIMIT 1 OFFSET {k})"
        ),
        3 => format!(
            "DELETE FROM books WHERE id = (SELECT id FROM books ORDER BY id LIMIT 1 OFFSET {k})"
        ),
        4 => format!(
            "INSERT INTO notes (book_id, body) SELECT id, '{tag}n{v}' FROM books ORDER BY id LIMIT 1 OFFSET {k}"
        ),
        5 => format!("INSERT OR REPLACE INTO settings VALUES ('s{k}', '{tag}{v}')"),
        _ => format!("DELETE FROM settings WHERE key = 's{k}'"),
    };
    peer.run(&sql).await;
}

/// Syncs, resolving any fault by resending; returns whether anything moved.
async fn sync_resolving(peer: &mut Peer, server: &Server, seed: u64, faults: &mut usize) -> bool {
    let id = peer.credentials.library_id();
    match peer.sync(server).await {
        Ok(r) => r.sent > 0 || r.received > 0 || r.more,
        Err(Error::LogFault(_)) => {
            *faults += 1;
            fongo_sync_client::resend_all(&mut peer.conn, &peer.replica, id)
                .await
                .unwrap();
            true
        }
        Err(Error::Transport(_)) => true,
        Err(e) => panic!("seed {seed}: {e:?}"),
    }
}

async fn resolution_run(seed: u64) {
    let mut rng = Rng(seed);
    let server = Server::new();
    let page = 1 + rng.below(4) as u32;
    server.with(|s| s.caps.max_page_batches = Some(page));
    let made = create_library(&server, now).await.unwrap();
    let id = made.library_id();
    let mut peers = vec![Peer::new(made.clone()).await];
    for _ in 0..2 {
        let joined = join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap();
        peers.push(Peer::new(joined).await);
    }
    let rollbacks = [rng.below(30), 10 + rng.below(30)];
    let lossy = env("FONGO_HONEST_LOSSY", 0) == 1;
    let shared = Arc::new(Mutex::new(Rng(seed ^ 99)));
    if lossy {
        chaos(&server, &shared, 5, 7);
    }
    let mut faults = 0;
    for round in 0..50 {
        let who = rng.below(3) as usize;
        for _ in 0..1 + rng.below(3) {
            random_edit(&mut peers[who], &mut rng, &format!("p{who}")).await;
        }
        let who = rng.below(3) as usize;
        sync_resolving(&mut peers[who], &server, seed, &mut faults).await;
        if rollbacks.contains(&round) {
            let len = server.log(id).len();
            let to = rng.below(len as u64 + 1) as usize;
            server.roll_back(id, to);
        }
    }
    server.behave();
    for _ in 0..60 {
        let mut moved = false;
        for p in peers.iter_mut() {
            moved |= sync_resolving(p, &server, seed, &mut faults).await;
        }
        if !moved {
            break;
        }
    }
    let joined = join_library(&server, &made.recovery_code(), now)
        .await
        .unwrap();
    let mut fresh = Peer::new(joined).await;
    while fresh.sync(&server).await.unwrap().more {}
    peers.push(fresh);
    eprintln!("seed {seed}: {faults} faults, log {}", server.log(id).len());
    let want = peers[0].dump().await;
    for (i, p) in peers.iter_mut().enumerate() {
        assert_eq!(
            p.dump().await,
            want,
            "seed {seed}: peer {i} ({faults} faults)"
        );
        assert_eq!(log_fault(&mut p.conn, id).await.unwrap(), None);
    }
    let first = fingerprint(&mut peers[0].conn, id).await.unwrap();
    for p in peers.iter_mut() {
        assert_eq!(
            fingerprint(&mut p.conn, id).await.unwrap(),
            first,
            "seed {seed}"
        );
    }
    assert_eq!(
        first.digest,
        independent_chain(id.0, &server.log(id)),
        "seed {seed}"
    );
}

#[tokio::test]
async fn rollbacks_resolved_by_resending_converge_with_a_new_device() {
    let from = env("FONGO_HONEST_FROM", 0);
    for seed in from..from + env("FONGO_HONEST_RESOLVE_SEEDS", 6) {
        resolution_run(seed).await;
    }
}
