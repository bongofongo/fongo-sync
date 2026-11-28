//! Two or three syncs at once on one database, each on a connection of its own, against one
//! server: each change reaches the log once, the records of what is in flight and of where a pull
//! stopped survive the other syncs, and every device converges.

#[allow(dead_code)]
mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fongo_sync_client::protocol::api::{ReadBatches, Route};
use fongo_sync_client::protocol::{ENVELOPE_FORMAT, encode_batch, random_nonce};
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Error, Pace, Refusal, Request, SyncReport, create_library, join_library, step_past,
    sync, sync_with,
};
use sqlx::SqliteConnection;
use support::{
    Peer, Server, assert_converged, assert_no_change_sent_twice, is_append, is_read, now, scratch,
    seal_as, settle,
};
use tokio::sync::oneshot;

/// One device's database opened on two connections, and a second device.
async fn devices(dir: &Path, server: &Server) -> (Peer, Peer, Peer) {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    let path = dir.join("x.db");
    let first = Peer::new_at(&path, made.clone()).await;
    let second = Peer::open(&path, made).await;
    (first, second, Peer::new(joined).await)
}

async fn write_some(peer: &mut Peer) {
    for sql in [
        "INSERT INTO books (title, year) VALUES ('Middlemarch', 1871)",
        "INSERT INTO books (title, year) VALUES ('Persuasion', 1817)",
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow start')",
        "INSERT INTO settings VALUES ('theme', 'dark')",
    ] {
        peer.run(sql).await;
    }
}

/// The envelope a connection's database records as in flight, if any.
async fn in_flight(peer: &mut Peer) -> Option<Vec<u8>> {
    sqlx::query_scalar("SELECT sending_envelope FROM fongo_client_state")
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

async fn outbox_len(peer: &mut Peer) -> usize {
    peer.replica
        .outbox(&mut peer.conn, u32::MAX)
        .await
        .unwrap()
        .changes
        .len()
}

/// Waits, once a pull has caught up, at the next gap before sending, until told to go on.
struct Park {
    armed: bool,
    parked: Option<oneshot::Sender<()>>,
    go: Option<oneshot::Receiver<()>>,
}

impl Park {
    fn new() -> (Park, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (parked, on_parked) = oneshot::channel();
        let (go, on_go) = oneshot::channel();
        let park = Park {
            armed: false,
            parked: Some(parked),
            go: Some(on_go),
        };
        (park, on_parked, go)
    }
}

impl Apply for Park {
    async fn caught_up(
        &mut self,
        _: &mut SqliteConnection,
        _: &Replica,
    ) -> Result<Option<Applied>, Error> {
        self.armed = true;
        Ok(None)
    }

    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        if self.armed
            && let Some(parked) = self.parked.take()
        {
            parked.send(()).unwrap();
            self.go.take().unwrap().await.unwrap();
        }
        Ok(())
    }
}

/// After both connections have synced: each change is in the log once, both connections read the
/// whole log and hold nothing unsent, and the other device converges with them.
async fn check_after(server: &Server, a: &mut Peer, b: &mut Peer, z: Peer, changes: usize) {
    let keys = a.credentials.library().clone();
    assert_no_change_sent_twice(server, &keys);
    assert_eq!(server.changes(&keys).len(), changes);
    let log = server.log(keys.id).len() as u64;
    for peer in [&mut *a, &mut *b] {
        assert_eq!(in_flight(peer).await, None);
        assert_eq!(outbox_len(peer).await, 0);
    }
    let mut peers = [z];
    settle(&mut peers, server).await;
    let [mut z] = peers;
    assert_eq!(z.cursor().await, log);
    let b_report = b.sync(server).await.unwrap();
    assert_eq!((b_report.sent, b_report.received), (0, 0));
    assert_eq!(a.cursor().await, log);
    assert_eq!(b.cursor().await, log);
    assert_eq!(a.dump().await, z.dump().await);
    assert_eq!(a.merged().await, z.merged().await);
}

/// The number of changes `write_some` makes, counted on a database of its own.
async fn some_changes() -> usize {
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut peer = Peer::new(made).await;
    write_some(&mut peer).await;
    peer.sync(&server).await.unwrap().sent
}

/// One sync waits after reading the outbox and before sending it; the other runs to the end.
#[tokio::test]
async fn a_sync_paused_before_sending_does_not_send_what_another_sent_meanwhile() {
    let n = some_changes().await;
    let dir = scratch("paused-before-sending");
    let server = Server::new();
    let (mut a, mut b, z) = devices(&dir, &server).await;
    write_some(&mut a).await;
    let (mut park, parked, go) = Park::new();
    let first = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut park,
    );
    let second = async {
        parked.await.unwrap();
        let report = sync(&mut b.conn, &b.replica, &server, &b.credentials, now).await;
        go.send(()).unwrap();
        report
    };
    let (first, second) = tokio::join!(first, second);
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_no_change_sent_twice(&server, a.credentials.library());
    assert_eq!((second.sent, second.received), (n, 0));
    assert_eq!((first.sent, first.received), (0, 0));
    check_after(&server, &mut a, &mut b, z, n).await;
}

/// One sync waits before sending; the other claims the outbox and its append is held, before the
/// server stores it or after, while the first goes on to the end.
#[tokio::test]
async fn a_sync_that_finds_another_sending_sends_nothing() {
    let n = some_changes().await;
    for handled in [false, true] {
        let dir = scratch(&format!("finds-another-sending-{handled}"));
        let server = Server::new();
        let (mut a, mut b, z) = devices(&dir, &server).await;
        write_some(&mut a).await;
        let (mut park, parked, go) = Park::new();
        let (done, on_done) = oneshot::channel();
        let waiting = async {
            let report = sync_with(
                &mut b.conn,
                &b.replica,
                &server,
                &b.credentials,
                now,
                &mut park,
            )
            .await;
            done.send(()).unwrap();
            report
        };
        let sending = async {
            parked.await.unwrap();
            let hold = server.hold_if(is_append, handled);
            let mut first = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
            tokio::select! {
                _ = &mut first => panic!("the append was never held"),
                () = hold.reached() => {}
            }
            go.send(()).unwrap();
            on_done.await.unwrap();
            hold.release();
            first.await
        };
        let (waited, sent) = tokio::join!(waiting, sending);
        let (waited, sent) = (waited.unwrap(), sent.unwrap());
        assert_no_change_sent_twice(&server, a.credentials.library());
        assert_eq!((waited.sent, waited.received), (0, 0), "handled: {handled}");
        assert!(waited.sending_elsewhere, "handled: {handled}");
        assert!(!sent.sending_elsewhere, "handled: {handled}");
        assert_eq!((sent.sent, sent.received), (n, 0), "handled: {handled}");
        check_after(&server, &mut a, &mut b, z, n).await;
    }
}

/// One sync is held at each of its requests in turn, before the server handles it and after; the
/// other runs to the end meanwhile.
#[tokio::test]
async fn a_sync_held_at_any_request_while_another_runs_sends_each_change_once() {
    let n = some_changes().await;
    for at in 1..12 {
        for handled in [false, true] {
            let dir = scratch(&format!("held-at-{at}-{handled}"));
            let server = Server::new();
            let (mut a, mut b, z) = devices(&dir, &server).await;
            write_some(&mut a).await;
            let mut seen = 0;
            let hold = server.hold_if(
                move |_| {
                    seen += 1;
                    seen == at
                },
                handled,
            );
            let mut first = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
            let held = tokio::select! {
                report = &mut first => Some(report),
                () = hold.reached() => None,
            };
            server.behave();
            let second = sync(&mut b.conn, &b.replica, &server, &b.credentials, now).await;
            hold.release();
            let first = match held {
                Some(report) => {
                    drop(first);
                    report
                }
                None => first.await,
            };
            let (first, second) = (first.unwrap(), second.unwrap());
            assert_no_change_sent_twice(&server, a.credentials.library());
            let case = format!("held at request {at}, handled: {handled}");
            for sent in [first.sent, second.sent] {
                assert!(sent == 0 || sent == n, "{case}: {first:?} {second:?}");
            }
            assert!(first.sent + second.sent >= n, "{case}");
            for report in [&first, &second] {
                assert!(report.received == 0 || report.received == n, "{case}");
                assert!(report.applied.conflicts.is_empty(), "{case}");
            }
            check_after(&server, &mut a, &mut b, z, n).await;
        }
    }
}

/// A sync whose pull began before another sync on the same database appended an envelope reads
/// that envelope as this device's own: it releases the bases of its edits there, so a change made
/// after it, knowing those edits, is not taken for a conflict.
#[tokio::test]
async fn a_pull_knows_an_envelope_another_sync_appended_as_its_own() {
    let dir = scratch("knows-own");
    let server = Server::new();
    let (mut a, mut b, mut z) = devices(&dir, &server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'first')")
        .await;
    a.sync(&server).await.unwrap();
    z.sync(&server).await.unwrap();
    a.run("UPDATE notes SET body = 'edited here'").await;
    z.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    z.sync(&server).await.unwrap();

    let reading = server.hold_if(is_read, false);
    let mut first = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
    tokio::select! {
        _ = &mut first => panic!("the read was never held"),
        () = reading.reached() => {}
    }
    let appending = server.hold_if(is_append, false);
    let mut second = Box::pin(sync(&mut b.conn, &b.replica, &server, &b.credentials, now));
    tokio::select! {
        _ = &mut second => panic!("the append was never held"),
        () = appending.reached() => {}
    }
    z.run("INSERT INTO settings VALUES ('size', 'large')").await;
    z.sync(&server).await.unwrap();
    appending.release();
    let sent = second.await.unwrap();
    assert_eq!(sent.sent, 1);
    z.sync(&server).await.unwrap();
    z.run("UPDATE notes SET body = 'edited there'").await;
    z.sync(&server).await.unwrap();

    reading.release();
    let report = first.await.unwrap();
    assert_eq!(report.sent, 0);
    assert!(
        report.applied.conflicts.is_empty(),
        "{:#?}",
        report.applied.conflicts
    );
    assert_no_change_sent_twice(&server, a.credentials.library());
    let mut peers = [a, z];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
}

/// Fails the second gap it is called in once armed: past the acknowledgment of what it sent, and
/// before what it would send next.
struct StopAfterAcknowledging {
    armed: Arc<AtomicBool>,
    seen: usize,
}

impl Apply for StopAfterAcknowledging {
    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        if self.armed.load(Ordering::SeqCst) {
            self.seen += 1;
            if self.seen == 2 {
                return Err(Error::Application("stopped".into()));
            }
        }
        Ok(())
    }
}

/// A sync acknowledges an envelope left recorded by an earlier failure while another sync, which
/// recorded the next envelope and whose append the server stored, crashes before its answer. The
/// next sync on a fresh connection must find that envelope where it landed: not send its changes
/// again, and not take a change made after it as a conflict with them.
#[tokio::test]
async fn a_record_in_flight_survives_another_syncs_acknowledgment_and_a_crash() {
    let dir = scratch("record-survives");
    let server = Server::new();
    let (mut a, mut b, mut z) = devices(&dir, &server).await;
    let path = dir.join("x.db");
    let credentials = a.credentials.clone();
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'first')")
        .await;
    a.sync(&server).await.unwrap();
    z.sync(&server).await.unwrap();

    a.run("INSERT INTO books (title) VALUES ('Persuasion')")
        .await;
    server.fail_if(is_append);
    assert!(a.sync(&server).await.is_err());
    server.behave();
    let left = in_flight(&mut a).await.unwrap();
    a.run("UPDATE notes SET body = 'edited here'").await;

    let armed = Arc::new(AtomicBool::new(false));
    let mut stop = StopAfterAcknowledging {
        armed: armed.clone(),
        seen: 0,
    };
    let recovering = server.hold_if(is_append, false);
    let mut second = Box::pin(sync_with(
        &mut b.conn,
        &b.replica,
        &server,
        &b.credentials,
        now,
        &mut stop,
    ));
    tokio::select! {
        _ = &mut second => panic!("the append was never held"),
        () = recovering.reached() => {}
    }
    let mut appends = 0;
    let crashing = server.hold_if(
        move |request| {
            appends += usize::from(is_append(request));
            appends == 2
        },
        true,
    );
    {
        let mut first = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
        tokio::select! {
            _ = &mut first => panic!("the second append was never held"),
            () = crashing.reached() => {}
        }
    }
    let crashed = in_flight(&mut a).await.unwrap();
    assert_ne!(crashed, left);
    drop(a);
    server.behave();

    armed.store(true, Ordering::SeqCst);
    recovering.release();
    let stopped = second.await;
    assert!(matches!(stopped, Err(Error::Application(_))), "{stopped:?}");
    assert_eq!(
        in_flight(&mut b).await.as_deref(),
        Some(&crashed[..]),
        "another sync's acknowledgment erased the record of the envelope in flight"
    );

    z.sync(&server).await.unwrap();
    z.run("UPDATE notes SET body = 'edited there'").await;
    z.sync(&server).await.unwrap();

    drop(b);
    let mut restarted = Peer::open(&path, credentials).await;
    let report = restarted.sync(&server).await.unwrap();
    assert_no_change_sent_twice(&server, restarted.credentials.library());
    assert!(
        report.applied.conflicts.is_empty(),
        "{:#?}",
        report.applied.conflicts
    );
    assert_eq!(report.sent, 1);
    let log = server.log(restarted.credentials.library_id()).len() as u64;
    assert_eq!(restarted.cursor().await, log);
    let mut peers = [restarted, z];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
}

/// The changes of one setting, as a later version's envelope carries them.
fn setting(name: &str) -> Vec<Change> {
    let gid = Gid::derive("settings", &[Value::Text(name.into())]);
    let clock = Hlc {
        wall_ms: 1,
        counter: 0,
        node: Gid([9; 16]),
    };
    ["key", "value"]
        .into_iter()
        .map(|field| Change {
            collection: "settings".into(),
            gid,
            field: field.into(),
            value: Value::Text(name.into()),
            clock,
        })
        .collect()
}

fn reads_after(request: &Request, after: u64) -> bool {
    is_read(request)
        && Route::parse(&request.target)
            .and_then(|(_, query)| ReadBatches::parse_query(query.unwrap_or("")).ok())
            .is_some_and(|read| read.after == after)
}

/// A sync that fails part-way through its pull, before it reaches the envelope another sync
/// stopped at meanwhile, keeps that sync's record of the stop, so the application can still step
/// past it.
#[tokio::test]
async fn a_failed_pull_keeps_the_stop_another_sync_recorded() {
    let dir = scratch("stop-survives");
    let server = Server::new();
    let (mut a, mut b, mut z) = devices(&dir, &server).await;
    for year in 0..4 {
        z.run(&format!(
            "INSERT INTO books (title, year) VALUES ('Middlemarch', {year})"
        ))
        .await;
        z.sync(&server).await.unwrap();
    }
    let library = z.credentials.library().clone();
    let later = seal_as(
        ENVELOPE_FORMAT + 1,
        &encode_batch(&setting("theme")),
        &library,
        0,
        z.credentials.device(),
        random_nonce().unwrap(),
    );
    server.inject(library.id, later);
    assert_eq!(server.log(library.id).len(), 5);

    let hold = server.hold_if(|request| reads_after(request, 4), false);
    let mut failing = Box::pin(sync(&mut b.conn, &b.replica, &server, &b.credentials, now));
    tokio::select! {
        _ = &mut failing => panic!("the second page was never held"),
        () = hold.reached() => {}
    }
    let stopped = sync(&mut a.conn, &a.replica, &server, &a.credentials, now)
        .await
        .unwrap();
    let newer = stopped.newer.unwrap();
    assert_eq!(newer.seq, 5);
    server.fail_if(|request| reads_after(request, 4));
    hold.release();
    assert!(failing.await.is_err());
    server.behave();
    assert_eq!(a.cursor().await, 4);

    step_past(&mut a.conn, library.id, newer.seq, newer.digest)
        .await
        .unwrap();
    let past: SyncReport = a.sync(&server).await.unwrap();
    assert_eq!(past.newer, None);
    assert_eq!(past.refused.len(), 1);
    assert_eq!(past.refused[0].reason, Refusal::SteppedPast);
    assert_eq!(a.cursor().await, 5);
    assert_eq!(b.cursor().await, 5);
}

/// Seals envelopes of one change each, without pausing.
struct OneByOne;

impl Apply for OneByOne {
    fn pace(&self) -> Pace {
        Pace {
            seal: 1,
            pause: Duration::ZERO,
            ..Pace::default()
        }
    }
}

/// Three syncs really at once, each on a thread and a connection of its own to one database file:
/// every one succeeds, and no change is sealed into two envelopes, however their statements fall.
#[test]
fn syncs_on_threads_of_their_own_seal_each_change_once() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    for round in 0..10 {
        let dir = scratch(&format!("threads-{round}"));
        let path = dir.join("x.db");
        let server = Server::new();
        let made = runtime().block_on(async {
            let made = create_library(&server, now).await.unwrap();
            let mut first = Peer::new_at(&path, made.clone()).await;
            for k in 0..30 {
                first
                    .run(&format!(
                        "INSERT INTO books (title, year) VALUES ('b{k}', {k})"
                    ))
                    .await;
            }
            made
        });
        std::thread::scope(|scope| {
            let syncs: Vec<_> = (0..3)
                .map(|_| {
                    scope.spawn(|| {
                        runtime().block_on(async {
                            let mut peer = Peer::open(&path, made.clone()).await;
                            for _ in 0..3 {
                                sync_with(
                                    &mut peer.conn,
                                    &peer.replica,
                                    &server,
                                    &peer.credentials,
                                    now,
                                    &mut OneByOne,
                                )
                                .await
                                .unwrap();
                            }
                        })
                    })
                })
                .collect();
            for sync in syncs {
                sync.join().unwrap();
            }
        });
        assert_no_change_sent_twice(&server, made.library());
    }
}

/// A sync whose pull began while one envelope was to be stepped past steps past it; the
/// application then steps past a later one, which another sync stopped at; the first sync, which
/// did not see that second step, stops at the later envelope. The second step must stay for the
/// next sync.
#[tokio::test]
async fn a_pull_keeps_a_step_past_recorded_after_it_began() {
    let dir = scratch("passing-survives");
    let server = Server::new();
    let (mut a, mut b, mut z) = devices(&dir, &server).await;
    let library = z.credentials.library().clone();
    let device = z.credentials.device().clone();
    let later = |name: &str| {
        seal_as(
            ENVELOPE_FORMAT + 1,
            &encode_batch(&setting(name)),
            &library,
            0,
            &device,
            random_nonce().unwrap(),
        )
    };
    for year in 0..2 {
        z.run(&format!(
            "INSERT INTO books (title, year) VALUES ('Middlemarch', {year})"
        ))
        .await;
        z.sync(&server).await.unwrap();
    }
    server.inject(library.id, later("theme"));
    z.run("INSERT INTO books (title, year) VALUES ('Persuasion', 1817)")
        .await;
    z.sync(&server).await.unwrap();
    server.inject(library.id, later("font"));
    assert_eq!(server.log(library.id).len(), 5);

    let first = a.sync(&server).await.unwrap().newer.unwrap();
    assert_eq!(first.seq, 3);
    step_past(&mut a.conn, library.id, first.seq, first.digest)
        .await
        .unwrap();
    // The pull's first read begins at the envelope at its cursor, 2.
    let hold = server.hold_if(|request| reads_after(request, 1), false);
    let mut held = Box::pin(sync(&mut b.conn, &b.replica, &server, &b.credentials, now));
    tokio::select! {
        _ = &mut held => panic!("the read was never held"),
        () = hold.reached() => {}
    }
    let second = a.sync(&server).await.unwrap().newer.unwrap();
    assert_eq!(second.seq, 5);
    step_past(&mut a.conn, library.id, second.seq, second.digest)
        .await
        .unwrap();
    hold.release();
    let report = held.await.unwrap();
    assert_eq!(report.newer.map(|n| n.seq), Some(5));
    let past = a.sync(&server).await.unwrap();
    assert_eq!(
        past.newer, None,
        "the step past the later envelope was lost"
    );
    assert_eq!(a.cursor().await, 5);
}

/// Waits at the first gap between transactions until told to go on.
struct ParkFirst {
    parked: Option<oneshot::Sender<()>>,
    go: Option<oneshot::Receiver<()>>,
}

impl Apply for ParkFirst {
    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        if let Some(parked) = self.parked.take() {
            parked.send(()).unwrap();
            self.go.take().unwrap().await.unwrap();
        }
        Ok(())
    }
}

/// A sync loads the envelope another sync is sending, and its pull ends before that envelope is
/// stored; the other sync then appends it and is acknowledged. The first must not append the same
/// bytes again: on a server that stores an identical envelope again, that would be a copy.
#[tokio::test]
async fn a_sync_does_not_append_again_an_envelope_another_acknowledged_meanwhile() {
    let dir = scratch("loaded-acknowledged");
    let server = Server::new();
    server.with(|s| s.store_again = true);
    let (mut a, mut b, _z) = devices(&dir, &server).await;
    write_some(&mut a).await;
    let library = a.credentials.library().id;
    let before = server.log(library).len();

    let hold = server.hold_if(is_append, false);
    let mut sending = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
    tokio::select! {
        _ = &mut sending => panic!("the append was never held"),
        () = hold.reached() => {}
    }
    let (parked, on_parked) = oneshot::channel();
    let (go, on_go) = oneshot::channel();
    let mut park = ParkFirst {
        parked: Some(parked),
        go: Some(on_go),
    };
    let mut loaded = Box::pin(sync_with(
        &mut b.conn,
        &b.replica,
        &server,
        &b.credentials,
        now,
        &mut park,
    ));
    tokio::select! {
        _ = &mut loaded => panic!("the second sync never parked"),
        _ = on_parked => {}
    }
    hold.release();
    sending.await.unwrap();
    go.send(()).unwrap();
    let report = loaded.await.unwrap();
    assert_eq!(
        server.log(library).len(),
        before + 1,
        "the envelope was appended twice"
    );
    assert_eq!(report.sent, 0);
}

/// A sync that finds another sync's envelope in flight sends nothing and says so; when that other
/// sync dies, the next sync sends the envelope and the rest.
#[tokio::test]
async fn a_sync_that_leaves_its_changes_to_another_says_so() {
    let n = some_changes().await;
    let dir = scratch("left-to-another");
    let server = Server::new();
    let (mut a, mut b, _z) = devices(&dir, &server).await;
    write_some(&mut a).await;
    let (mut park, parked, go) = Park::new();
    let mut waiting = Box::pin(sync_with(
        &mut b.conn,
        &b.replica,
        &server,
        &b.credentials,
        now,
        &mut park,
    ));
    tokio::select! {
        _ = &mut waiting => panic!("the sync never parked"),
        _ = parked => {}
    }
    let hold = server.hold_if(is_append, false);
    {
        let mut dying = Box::pin(sync(&mut a.conn, &a.replica, &server, &a.credentials, now));
        tokio::select! {
            _ = &mut dying => panic!("the append was never held"),
            () = hold.reached() => {}
        }
    }
    drop(a);
    server.behave();
    go.send(()).unwrap();
    let left = waiting.await.unwrap();
    assert_eq!(left.sent, 0);
    assert!(left.sending_elsewhere);
    let again = b.sync(&server).await.unwrap();
    assert_eq!(again.sent, n);
    assert!(!again.sending_elsewhere);
    assert_no_change_sent_twice(&server, b.credentials.library());
}
