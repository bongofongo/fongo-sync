//! A replica with its own in-memory database, for directed tests. It records every change it
//! takes in and checks itself against the model after every apply and collect.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fongo_sync_sqlite::core::{Change, Gid};
use fongo_sync_sqlite::{Applied, Registry, Replica, Resume};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

pub struct Peer {
    pub conn: SqliteConnection,
    pub replica: Replica,
    /// How much of a shared log this peer has taken.
    pub cursor: usize,
    /// Every change applied here or sent from here.
    pub taken: Vec<Change>,
    /// Whether every step is checked against the model; off only where the checks would cost
    /// more than the test.
    pub checking: bool,
    pub writes: super::writes::Writes,
}

impl Peer {
    pub async fn bare(schema: &[&str], registry: Registry) -> Peer {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        for sql in schema {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        Peer {
            conn,
            replica: Replica::new(registry),
            cursor: 0,
            taken: Vec::new(),
            checking: true,
            writes: Default::default(),
        }
    }

    pub async fn new(schema: &[&str], registry: Registry) -> Peer {
        let mut peer = Peer::bare(schema, registry).await;
        peer.install().await;
        peer
    }

    pub async fn install(&mut self) {
        let waiting = self.writes.installing(&mut self.conn, &self.replica).await;
        self.replica.install(&mut self.conn).await.unwrap();
        self.writes
            .installed(&mut self.conn, &self.replica, waiting)
            .await;
        self.midway().await;
    }

    /// Installs another registry, as an application does when it upgrades.
    pub async fn upgrade(&mut self, registry: Registry) {
        self.replica = Replica::new(registry);
        self.install().await;
    }

    pub async fn suspend(&mut self) {
        self.replica.suspend(&mut self.conn).await.unwrap();
    }

    /// Resumes; what the application wrote while suspended stays its own, to be collected.
    pub async fn resume(&mut self, how: Resume) {
        let waiting = self.writes.installing(&mut self.conn, &self.replica).await;
        let replica = self.replica.clone();
        replica.resume(&mut self.conn, how).await.unwrap();
        self.writes
            .installed(&mut self.conn, &self.replica, waiting)
            .await;
        self.midway().await;
    }

    pub async fn midway(&mut self) {
        if !self.checking {
            return;
        }
        if let Err(e) = super::midway(&mut self.conn, &self.replica, &self.taken).await {
            panic!("{e}");
        }
    }

    pub async fn settled(&mut self) {
        if let Err(e) = super::settled(&mut self.conn, &self.replica, &self.taken).await {
            panic!("{e}");
        }
    }

    pub async fn run(&mut self, sql: &str) {
        sqlx::query(sql).execute(&mut self.conn).await.unwrap();
    }

    pub async fn lines(&mut self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    pub async fn int(&mut self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&mut self.conn)
            .await
            .unwrap()
    }

    pub async fn gid(&mut self, collection: &str, id: i64) -> Gid {
        let gid = self.replica.gid_of(&mut self.conn, collection, id).await;
        gid.unwrap().unwrap()
    }

    /// Collects and sends to `log`, returning what was sent.
    pub async fn push(&mut self, log: &mut Vec<Change>) -> Vec<Change> {
        self.writes.before(&mut self.conn, &self.replica).await;
        self.replica.collect(&mut self.conn).await.unwrap();
        self.writes.after(&mut self.conn, &self.replica).await;
        let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
        let unwritten = self
            .writes
            .unwritten(&mut self.conn, &self.replica, &self.taken, &outbox.changes)
            .await;
        if self.checking && !unwritten.is_empty() {
            panic!("{}", unwritten.join("\n"));
        }
        log.extend(outbox.changes.iter().cloned());
        self.taken.extend(outbox.changes.iter().cloned());
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.midway().await;
        outbox.changes
    }

    pub async fn apply(&mut self, changes: &[Change]) -> Applied {
        self.taken.extend(changes.iter().cloned());
        self.writes.before(&mut self.conn, &self.replica).await;
        let applied = self.replica.apply(&mut self.conn, changes).await.unwrap();
        self.writes.touched(&applied.touched);
        self.writes.after(&mut self.conn, &self.replica).await;
        self.midway().await;
        applied
    }

    /// Takes what `hub` holds past this peer's cursor.
    pub async fn pull(&mut self, hub: &[Change]) -> Applied {
        let applied = self.apply(&hub[self.cursor..]).await;
        self.cursor = hub.len();
        applied
    }

    pub async fn sync(&mut self, hub: &mut Vec<Change>) -> Applied {
        self.push(hub).await;
        self.pull(hub).await
    }

    /// Counts the work SQLite does on this connection from now on, in thousands of virtual
    /// machine instructions.
    pub async fn count_work(&mut self) -> Arc<AtomicU64> {
        let work = Arc::new(AtomicU64::new(0));
        let counter = work.clone();
        let mut handle = self.conn.lock_handle().await.unwrap();
        handle.set_progress_handler(1000, move || {
            counter.fetch_add(1, Ordering::Relaxed);
            true
        });
        work
    }
}

pub async fn make(n: usize, schema: &[&str], registry: impl Fn() -> Registry) -> Vec<Peer> {
    let mut all = Vec::new();
    for _ in 0..n {
        all.push(Peer::new(schema, registry()).await);
    }
    all
}

/// Every peer sends and takes everything, until a round sends nothing.
pub async fn settle(peers: &mut [Peer], hub: &mut Vec<Change>) {
    for _ in 0..20 {
        let before = hub.len();
        for peer in peers.iter_mut() {
            peer.sync(hub).await;
        }
        let mut quiet = hub.len() == before && peers.iter().all(|p| p.cursor == hub.len());
        // A change made by the last apply of a round is not sent until the next.
        for peer in peers.iter_mut() {
            peer.replica.collect(&mut peer.conn).await.unwrap();
            let left = peer.replica.outbox(&mut peer.conn, 1).await.unwrap();
            quiet &= left.changes.is_empty();
        }
        if quiet {
            return;
        }
    }
    panic!("the peers never stopped sending");
}

/// Each peer shows the model's view of what it has taken in, and all show the same rows.
pub async fn same_everywhere(peers: &mut [Peer], rows: &str) -> Vec<String> {
    for peer in peers.iter_mut() {
        peer.settled().await;
    }
    let first = peers[0].lines(rows).await;
    for (i, peer) in peers.iter_mut().enumerate().skip(1) {
        assert_eq!(peer.lines(rows).await, first, "peer {i} shows other rows");
    }
    first
}

pub fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}
