//! A server whose log is not the one a device read or wrote: rolled back, rolled back and refilled
//! by other devices, an acknowledged envelope dropped or replaced, a position used twice, two
//! readers served different logs. Each is found, stored and reported on every sync until resolved;
//! what an honest server does is never taken for one.

#[allow(dead_code)]
mod support;

use std::sync::Arc;

use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Error, LogFault, Method, Pace, create_library, fingerprint, join_library, log_fault,
    resend_all, sync, sync_with,
};
use sha2::{Digest, Sha256};
use sqlx::{Connection, SqliteConnection};
use support::{
    Peer, Server, assert_converged, assert_no_change_sent_twice, is_append, is_read, now, scratch,
    settle,
};
use tokio::sync::oneshot;

async fn pair(server: &Server) -> [Peer; 2] {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    [Peer::new(made).await, Peer::new(joined).await]
}

async fn another(server: &Server, of: &Peer) -> Peer {
    let joined = join_library(server, &of.credentials.library().recovery_code(), now)
        .await
        .unwrap();
    Peer::new(joined).await
}

/// Writes one setting and sends it, in an envelope of its own.
async fn send(peer: &mut Peer, server: &Server, key: &str) {
    peer.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
        .await;
    let id = peer.credentials.library_id();
    let before = server.log(id).len();
    assert!(peer.sync(server).await.unwrap().sent > 0);
    assert_eq!(server.log(id).len(), before + 1);
}

fn requests(server: &Server) -> usize {
    server.with(|s| s.requests.len())
}

async fn outbox_len(peer: &mut Peer) -> usize {
    peer.replica
        .outbox(&mut peer.conn, u32::MAX)
        .await
        .unwrap()
        .changes
        .len()
}

async fn stored(peer: &mut Peer) -> Option<LogFault> {
    log_fault(&mut peer.conn, peer.credentials.library_id())
        .await
        .unwrap()
}

/// Every later sync fails with `found` before reaching the server, and sends nothing.
async fn stays_stopped(peer: &mut Peer, server: &Server, found: LogFault) {
    assert_eq!(stored(peer).await, Some(found));
    let asked = requests(server);
    let log = server.log(peer.credentials.library_id()).len();
    for _ in 0..2 {
        let again = peer.sync(server).await;
        assert!(
            matches!(again, Err(Error::LogFault(f)) if f == found),
            "{again:?}"
        );
    }
    assert_eq!(requests(server), asked);
    assert_eq!(server.log(peer.credentials.library_id()).len(), log);
}

#[tokio::test]
async fn a_log_rolled_back_before_the_cursor_is_behind_and_stops_every_sync() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    assert_eq!(a.cursor().await, 3);
    server.roll_back(id, 1);
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let found = a.sync(&server).await;
    let behind = LogFault::Behind { cursor: 3 };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == behind),
        "{found:?}"
    );
    assert_eq!(server.log(id).len(), 1);
    assert_eq!(a.cursor().await, 3);
    assert!(
        a.replica
            .uncollected_rows(&mut a.conn, usize::MAX)
            .await
            .unwrap()
            > 0
    );
    stays_stopped(&mut a, &server, behind).await;
}

#[tokio::test]
async fn a_log_rolled_back_and_refilled_by_another_device_is_rewritten() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    let mut c = another(&server, &a).await;
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    server.roll_back(id, 1);
    // A device that never read past the rollback fills the same positions with other envelopes.
    for key in ["c1", "c2", "c3"] {
        send(&mut c, &server, key).await;
    }
    assert_eq!(server.log(id).len(), 4);
    let found = a.sync(&server).await;
    let rewritten = LogFault::Rewritten { seq: 3 };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == rewritten),
        "{found:?}"
    );
    stays_stopped(&mut a, &server, rewritten).await;
    // The author of the lost envelopes finds it too: its own envelope at its cursor is gone.
    assert!(matches!(
        b.sync(&server).await,
        Err(Error::LogFault(LogFault::Rewritten { seq: 3 }))
    ));
    // The device that came after the rollback cannot tell.
    c.sync(&server).await.unwrap();
}

#[tokio::test]
async fn a_rollback_to_exactly_the_cursor_is_seen_only_by_who_read_further() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    send(&mut b, &server, "b1").await;
    send(&mut b, &server, "b2").await;
    a.sync(&server).await.unwrap();
    send(&mut b, &server, "b3").await;
    server.roll_back(id, 2);
    // Everything `a` read is still there: it cannot tell.
    a.sync(&server).await.unwrap();
    assert_eq!(stored(&mut a).await, None);
    assert!(matches!(
        b.sync(&server).await,
        Err(Error::LogFault(LogFault::Behind { cursor: 3 }))
    ));
}

/// `a` reads the log to 2 and appends; `b` appends meanwhile, so `a`'s envelope lands at 4,
/// past `a`'s cursor, and stays recorded there.
async fn appended_past_the_cursor(server: &Server, a: &mut Peer, b: &mut Peer) {
    send(b, server, "b1").await;
    send(b, server, "b2").await;
    a.sync(server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let hold = server.hold_if(is_append, false);
    let syncing = a.sync(server);
    let meanwhile = async {
        hold.reached().await;
        send(b, server, "b3").await;
        hold.release();
    };
    let (synced, ()) = tokio::join!(syncing, meanwhile);
    assert!(synced.unwrap().sent > 0);
    assert_eq!(a.cursor().await, 2);
    assert_eq!(server.log(a.credentials.library_id()).len(), 4);
}

#[tokio::test]
async fn an_acknowledged_append_missing_from_the_log_is_found() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    appended_past_the_cursor(&server, &mut a, &mut b).await;
    // Rolled back to `a`'s cursor: its anchor is there, its append is not.
    server.roll_back(id, 2);
    let found = a.sync(&server).await;
    let missing = LogFault::AppendMissing { seq: 4 };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == missing),
        "{found:?}"
    );
    assert_eq!(a.cursor().await, 2);
    stays_stopped(&mut a, &server, missing).await;
}

#[tokio::test]
async fn an_acknowledged_append_replaced_in_the_log_is_found() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    appended_past_the_cursor(&server, &mut a, &mut b).await;
    let other = server.log(id)[2].clone();
    server.replace(id, 4, other);
    let found = a.sync(&server).await;
    let replaced = LogFault::AppendReplaced { seq: 4 };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == replaced),
        "{found:?}"
    );
    // Nothing from the page that held it was applied.
    assert_eq!(a.cursor().await, 2);
    assert_eq!(a.settings().await, ["a=x", "b1=x", "b2=x"]);
    stays_stopped(&mut a, &server, replaced).await;
}

/// Rolled back after a pull and before the append, which lands at a position the device read
/// past. The answer names the envelope where it is, so it is acknowledged; the next sync finds the
/// log ends before the cursor.
#[tokio::test]
async fn an_append_into_a_rolled_back_log_is_found_by_the_next_sync() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let hold = server.hold_if(is_append, false);
    let syncing = a.sync(&server);
    let meanwhile = async {
        hold.reached().await;
        server.roll_back(id, 1);
        hold.release();
    };
    let (synced, ()) = tokio::join!(syncing, meanwhile);
    assert!(synced.unwrap().sent > 0);
    assert_eq!(server.log(id).len(), 2);
    stays_stopped_after_finding(&mut a, &server, LogFault::Behind { cursor: 3 }).await;
}

/// Answers every append with position `at`, wherever the server stored it.
struct AnswersAt<'a> {
    server: &'a Server,
    at: u64,
}

impl fongo_sync_client::Transport for AnswersAt<'_> {
    async fn send(
        &self,
        request: fongo_sync_client::Request,
    ) -> Result<fongo_sync_client::Response, fongo_sync_client::TransportError> {
        let append = is_append(&request);
        let mut response = fongo_sync_client::Transport::send(self.server, request).await?;
        if append && response.status == 200 {
            let answer = fongo_sync_client::protocol::api::Appended { seq: self.at };
            response.body = serde_json::to_vec(&answer).unwrap();
        }
        Ok(response)
    }
}

#[tokio::test]
async fn an_append_answered_at_a_position_holding_other_bytes_is_reuse() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let wrong = AnswersAt {
        server: &server,
        at: 2,
    };
    let found = sync(&mut a.conn, &a.replica, &wrong, &a.credentials, now).await;
    let reused = LogFault::Reused {
        seq: 2,
        furthest: 3,
    };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == reused),
        "{found:?}"
    );
    assert_eq!(server.log(id).len(), 4);
    // Not acknowledged: the changes stay in the outbox.
    assert_eq!(outbox_len(&mut a).await, 3);
    stays_stopped(&mut a, &server, reused).await;
}

/// The read that would prove reuse fails: no fault, nothing acknowledged, and the next sync
/// appends the envelope again.
#[tokio::test]
async fn a_failed_read_of_an_answered_position_is_no_fault() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let wrong = AnswersAt {
        server: &server,
        at: 2,
    };
    let mut appended = false;
    server.fail_if(move |r| {
        let after_append = appended && is_read(r);
        appended |= is_append(r);
        after_append
    });
    let failed = sync(&mut a.conn, &a.replica, &wrong, &a.credentials, now).await;
    assert!(matches!(failed, Err(Error::Transport(_))), "{failed:?}");
    server.behave();
    assert_eq!(stored(&mut a).await, None);
    assert_eq!(outbox_len(&mut a).await, 3);
    assert!(a.sync(&server).await.unwrap().sent > 0);
    assert_eq!(server.log(id).len(), 4);
    assert_eq!(outbox_len(&mut a).await, 0);
}

#[tokio::test]
async fn two_syncs_of_one_database_served_different_logs_diverge() {
    let dir = scratch("diverge");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    let other = server.log(id)[0].clone();
    // `x`'s page is read and held; the server then serves `y` other bytes at 2.
    let hold = server.hold_if(is_read, true);
    let first = x.sync(&server);
    let second = async {
        hold.reached().await;
        server.replace(id, 2, other);
        y.sync(&server).await.unwrap();
        hold.release();
    };
    let (found, ()) = tokio::join!(first, second);
    let diverged = LogFault::Diverged { seq: 3 };
    assert!(
        matches!(found, Err(Error::LogFault(f)) if f == diverged),
        "{found:?}"
    );
    assert!(matches!(
        y.sync(&server).await,
        Err(Error::LogFault(f)) if f == diverged
    ));
}

#[tokio::test]
async fn a_failed_read_of_the_anchor_is_no_fault() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    server.fail_if(is_read);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    server.lose_if(is_read);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    assert_eq!(stored(&mut a).await, None);
    send(&mut b, &server, "b3").await;
    assert!(a.sync(&server).await.unwrap().received > 0);
}

#[tokio::test]
async fn a_new_device_and_an_empty_log_raise_nothing() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for _ in 0..3 {
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
    }
    send(&mut a, &server, "a1").await;
    let mut c = another(&server, &a).await;
    for _ in 0..3 {
        c.sync(&server).await.unwrap();
    }
    assert_eq!(c.settings().await, ["a1=x"]);
    // The anchor read is the one envelope more each sync costs, in the same request.
    let reads = server.count(Method::Get, "/v1/libraries");
    c.sync(&server).await.unwrap();
    assert_eq!(server.count(Method::Get, "/v1/libraries") - reads, 1);
}

#[tokio::test]
async fn a_stored_fault_survives_a_restart_and_stops_the_other_connections() {
    let dir = scratch("survives");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut a = Peer::new_at(&path, made.clone()).await;
    let mut other = Peer::open(&path, made.clone()).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    server.roll_back(id, 0);
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    let behind = LogFault::Behind { cursor: 2 };
    stays_stopped(&mut other, &server, behind).await;
    a.conn.close().await.unwrap();
    let mut reopened = Peer::open(&path, made).await;
    stays_stopped(&mut reopened, &server, behind).await;
}

/// Waits, once a pull has caught up, at the next gap before sending, until told to go on.
struct Park {
    armed: bool,
    parked: Option<oneshot::Sender<()>>,
    go: Option<oneshot::Receiver<()>>,
}

fn park() -> (Park, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (parked, on_parked) = oneshot::channel();
    let (go, on_go) = oneshot::channel();
    let park = Park {
        armed: false,
        parked: Some(parked),
        go: Some(on_go),
    };
    (park, on_parked, go)
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

#[tokio::test]
async fn a_fault_found_by_one_sync_stops_another_running_on_the_same_database() {
    let dir = scratch("one-of-two");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    y.sync(&server).await.unwrap();
    x.run("INSERT INTO settings VALUES ('x', 'x')").await;
    let (mut parked_at, parked, go) = park();
    let first = sync_with(
        &mut x.conn,
        &x.replica,
        &server,
        &x.credentials,
        now,
        &mut parked_at,
    );
    let second = async {
        parked.await.unwrap();
        server.roll_back(id, 1);
        let found = sync(&mut y.conn, &y.replica, &server, &y.credentials, now).await;
        go.send(()).unwrap();
        found
    };
    let (first, second) = tokio::join!(first, second);
    let behind = LogFault::Behind { cursor: 2 };
    assert!(
        matches!(second, Err(Error::LogFault(f)) if f == behind),
        "{second:?}"
    );
    assert!(
        matches!(first, Err(Error::LogFault(f)) if f == behind),
        "{first:?}"
    );
    assert_eq!(server.log(id).len(), 1);
    assert!(outbox_len(&mut x).await > 0);
}

#[tokio::test]
async fn a_fault_not_stored_before_a_crash_is_found_again() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    server.roll_back(id, 1);
    a.run(
        "CREATE TEMP TRIGGER crash BEFORE UPDATE OF fault_kind ON fongo_client_state
         WHEN NEW.fault_kind IS NOT NULL BEGIN SELECT RAISE(ABORT, 'crash'); END",
    )
    .await;
    assert!(matches!(a.sync(&server).await, Err(Error::Database(_))));
    assert_eq!(stored(&mut a).await, None);
    assert_eq!(a.cursor().await, 2);
    a.run("DROP TRIGGER temp.crash").await;
    stays_stopped_after_finding(&mut a, &server, LogFault::Behind { cursor: 2 }).await;
}

async fn stays_stopped_after_finding(peer: &mut Peer, server: &Server, found: LogFault) {
    let again = peer.sync(server).await;
    assert!(
        matches!(again, Err(Error::LogFault(f)) if f == found),
        "{again:?}"
    );
    stays_stopped(peer, server, found).await;
}

#[tokio::test]
async fn a_rollback_with_an_envelope_in_flight_is_resolved_by_sending_everything() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    // Stored at 4; the answer is lost, so the envelope stays recorded as being sent.
    server.lose_if(is_append);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    assert_eq!(server.log(id).len(), 4);
    server.roll_back(id, 2);
    stays_stopped_after_finding(&mut a, &server, LogFault::Behind { cursor: 3 }).await;
    let sent = resend_all(&mut a.conn, &a.replica, id).await.unwrap();
    assert!(sent > 0);
    assert_eq!(stored(&mut a).await, None);
    a.sync(&server).await.unwrap();
    // `b` read to 3 too, and resolves the same way.
    assert!(matches!(b.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut b.conn, &b.replica, id).await.unwrap();
    let c = another(&server, &a).await;
    let mut peers = [a, b, c];
    settle(&mut peers, &server).await;
    let rows = assert_converged(&mut peers).await;
    for key in ["a", "b1", "b2", "b3"] {
        assert!(rows.contains(&format!("setting {key}=x")), "{rows:#?}");
    }
    let [_, _, ref mut c] = peers;
    assert_eq!(stored(c).await, None);
}

#[tokio::test]
async fn resending_needs_a_fault_and_a_crash_in_it_keeps_the_fault() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    assert!(matches!(
        resend_all(&mut a.conn, &a.replica, id).await,
        Err(Error::NoLogFault)
    ));
    send(&mut b, &server, "b1").await;
    a.sync(&server).await.unwrap();
    assert!(matches!(
        resend_all(&mut a.conn, &a.replica, id).await,
        Err(Error::NoLogFault)
    ));
    assert_eq!(outbox_len(&mut a).await, 0);
    server.roll_back(id, 0);
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    a.run(
        "CREATE TEMP TRIGGER crash BEFORE INSERT ON fongo_outbox
         BEGIN SELECT RAISE(ABORT, 'crash'); END",
    )
    .await;
    assert!(matches!(
        resend_all(&mut a.conn, &a.replica, id).await,
        Err(Error::Database(_))
    ));
    assert_eq!(stored(&mut a).await, Some(LogFault::Behind { cursor: 1 }));
    assert_eq!(a.cursor().await, 1);
    assert_eq!(outbox_len(&mut a).await, 0);
    a.run("DROP TRIGGER temp.crash").await;
    resend_all(&mut a.conn, &a.replica, id).await.unwrap();
    assert_eq!(a.cursor().await, 0);
    assert!(a.sync(&server).await.unwrap().sent > 0);
    assert!(matches!(b.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut b.conn, &b.replica, id).await.unwrap();
    settle(&mut [a, b], &server).await;
}

/// Seals one change per envelope.
struct OneEach;

impl Apply for OneEach {
    fn pace(&self) -> Pace {
        Pace {
            seal: 1,
            ..Pace::default()
        }
    }
}

#[tokio::test]
async fn a_rollback_while_resending_is_found_and_resolved_again() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for n in 0..4 {
        a.run(&format!("INSERT INTO settings VALUES ('a{n}', 'x')"))
            .await;
    }
    a.sync(&server).await.unwrap();
    send(&mut b, &server, "b1").await;
    a.sync(&server).await.unwrap();
    server.roll_back(id, 0);
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut a.conn, &a.replica, id).await.unwrap();
    // Three envelopes of the resend reach the log, then the server fails.
    let appends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = appends.clone();
    server.fail_if(move |r| {
        is_append(r) && counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 3
    });
    let failed = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut OneEach,
    )
    .await;
    assert!(matches!(failed, Err(Error::Transport(_))), "{failed:?}");
    server.behave();
    assert_eq!(server.log(id).len(), 3);
    // Rolled back again, past some of what was resent.
    server.roll_back(id, 1);
    let again = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut OneEach,
    )
    .await;
    assert!(matches!(again, Err(Error::LogFault(_))), "{again:?}");
    resend_all(&mut a.conn, &a.replica, id).await.unwrap();
    a.sync(&server).await.unwrap();
    assert!(matches!(b.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut b.conn, &b.replica, id).await.unwrap();
    let c = another(&server, &a).await;
    let mut peers = [a, b, c];
    settle(&mut peers, &server).await;
    let rows = assert_converged(&mut peers).await;
    for key in ["a0", "a1", "a2", "a3", "b1"] {
        assert!(rows.contains(&format!("setting {key}=x")), "{rows:#?}");
    }
}

#[tokio::test]
async fn a_sync_running_while_a_fault_is_resolved_writes_nothing_more() {
    let dir = scratch("resolved-meanwhile");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    y.sync(&server).await.unwrap();
    x.run("INSERT INTO settings VALUES ('x', 'x')").await;
    let (mut parked_at, parked, go) = park();
    let first = sync_with(
        &mut x.conn,
        &x.replica,
        &server,
        &x.credentials,
        now,
        &mut parked_at,
    );
    let second = async {
        parked.await.unwrap();
        server.roll_back(id, 1);
        assert!(matches!(y.sync(&server).await, Err(Error::LogFault(_))));
        resend_all(&mut y.conn, &y.replica, id).await.unwrap();
        go.send(()).unwrap();
    };
    let (first, ()) = tokio::join!(first, second);
    assert!(matches!(first, Err(Error::ResolvedMeanwhile)), "{first:?}");
    assert_eq!(server.log(id).len(), 1);
    assert_eq!(y.cursor().await, 0);
    y.sync(&server).await.unwrap();
    assert!(matches!(b.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut b.conn, &b.replica, id).await.unwrap();
    let mut peers = [y, b];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
    assert_no_change_sent_twice_but_resent(&server, &peers[0]);
}

/// A resend repeats changes on purpose: what is checked is only that the log holds them.
fn assert_no_change_sent_twice_but_resent(server: &Server, peer: &Peer) {
    let _ = assert_no_change_sent_twice;
    assert!(!server.changes(peer.credentials.library()).is_empty());
}

fn chain(library: [u8; 16], log: &[Vec<u8>]) -> [u8; 32] {
    let domain = b"fongo-sync/v1/log-chain\0";
    let mut at: [u8; 32] = Sha256::new()
        .chain_update(domain)
        .chain_update([0])
        .chain_update(library)
        .finalize()
        .into();
    for (i, envelope) in log.iter().enumerate() {
        at = Sha256::new()
            .chain_update(domain)
            .chain_update([1])
            .chain_update(at)
            .chain_update((i as u64 + 1).to_be_bytes())
            .chain_update(Sha256::digest(envelope))
            .finalize()
            .into();
    }
    at
}

#[tokio::test]
async fn the_fingerprint_hashes_every_envelope_read_whatever_was_done_with_it() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    let empty = fingerprint(&mut a.conn, id).await.unwrap();
    assert_eq!((empty.seq, empty.digest), (0, chain(id.0, &[])));
    send(&mut a, &server, "a1").await;
    server.inject(id, vec![0; 40]);
    send(&mut b, &server, "b1").await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.refused_total, 1);
    send(&mut a, &server, "a2").await;
    let log = server.log(id);
    for peer in [&mut a, &mut b] {
        peer.sync(&server).await.unwrap();
        let print = fingerprint(&mut peer.conn, id).await.unwrap();
        assert_eq!(print.seq, 4);
        assert_eq!(print.digest, chain(id.0, &log));
    }
}

/// Lets every request through but an append, which waits for one permit.
struct GateAppends<'a> {
    server: &'a Server,
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    reached: std::sync::Arc<tokio::sync::Notify>,
}

impl fongo_sync_client::Transport for GateAppends<'_> {
    async fn send(
        &self,
        request: fongo_sync_client::Request,
    ) -> Result<fongo_sync_client::Response, fongo_sync_client::TransportError> {
        if is_append(&request) {
            self.reached.notify_one();
            self.permits.acquire().await.unwrap().forget();
        }
        fongo_sync_client::Transport::send(self.server, request).await
    }
}

/// A server that stores an identical envelope again, as the protocol allows. Two syncs of one
/// database both append the envelope a failed sync left recorded; the answer placing the first
/// copy comes back after the second copy was acknowledged.
#[tokio::test]
async fn copies_of_one_envelope_answered_out_of_order_are_no_fault() {
    let dir = scratch("copies-out-of-order");
    let path = dir.join("a.db");
    let server = Server::new();
    server.with(|s| s.store_again = true);
    let made = create_library(&server, now).await.unwrap();
    let joined = join_library(&server, &made.recovery_code(), now)
        .await
        .unwrap();
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let mut b = Peer::new(joined).await;
    for k in 0..2 {
        b.run(&format!("INSERT INTO settings VALUES ('b{k}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    x.sync(&server).await.unwrap();
    // A sync records an envelope and fails before its append reaches the server.
    x.run("INSERT INTO settings VALUES ('a1', 'x')").await;
    server.fail_if(is_append);
    assert!(matches!(x.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    server.with(|s| s.store_again = true);
    let gate = GateAppends {
        server: &server,
        permits: std::sync::Arc::new(tokio::sync::Semaphore::new(0)),
        reached: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    let mut ys = Box::pin(sync(&mut y.conn, &y.replica, &gate, &y.credentials, now));
    tokio::select! {
        _ = &mut ys => panic!("y's append was not gated"),
        _ = gate.reached.notified() => {}
    }
    // x appends the recorded envelope: stored at 3, its answer held.
    let hold = server.hold_if(is_append, true);
    let mut xs = Box::pin(sync(&mut x.conn, &x.replica, &server, &x.credentials, now));
    tokio::select! {
        _ = &mut xs => panic!("x's append was not held"),
        _ = hold.reached() => {}
    }
    // y appends the same bytes: a copy at 4, acknowledged.
    gate.permits.add_permits(1);
    let r = (&mut ys).await;
    assert!(r.is_ok(), "y: {r:?}");
    hold.release();
    let r = xs.await;
    assert!(r.is_ok(), "x: {r:?}");
}

/// A conflict the device was told of before the fault is not reported again when it reads the
/// whole log after resending, nor is one with an edit it had not sent yet.
#[tokio::test]
async fn reading_the_log_again_after_a_resend_reports_no_conflict_again() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    b.run("UPDATE notes SET body = 'theirs'").await;
    a.run("UPDATE notes SET body = 'mine'").await;
    b.sync(&server).await.unwrap();
    let told = a.sync(&server).await.unwrap();
    assert_eq!(told.applied.conflicts.len(), 1, "{told:#?}");
    b.sync(&server).await.unwrap();
    // Edits not sent yet when the fault is found, one of them to the field that conflicted.
    a.run("INSERT INTO settings VALUES ('unsent', 'x')").await;
    a.run("UPDATE notes SET body = 'mine, again'").await;
    server.roll_back(id, 0);
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut a.conn, &a.replica, id).await.unwrap();
    for _ in 0..3 {
        let report = a.sync(&server).await.unwrap();
        assert_eq!(report.applied.conflicts, [], "{report:#?}");
    }
    assert!(matches!(b.sync(&server).await, Err(Error::LogFault(_))));
    resend_all(&mut b.conn, &b.replica, id).await.unwrap();
    for _ in 0..3 {
        for peer in [&mut a, &mut b] {
            let report = peer.sync(&server).await.unwrap();
            assert_eq!(report.applied.conflicts, [], "{report:#?}");
        }
    }
    let rows = assert_converged(&mut [a, b]).await;
    assert!(
        rows.iter().any(|r| r.contains("setting unsent=x")),
        "{rows:#?}"
    );
}

/// A damaged envelope is refused and the cursor moves onto it; the server then puts the bytes as
/// first written back in its place. Bytes once served never change, so the repair is a rewrite.
#[tokio::test]
async fn bytes_repaired_in_place_at_the_cursor_are_a_rewrite() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    send(&mut b, &server, "b0").await;
    let good = server.log(id)[0].clone();
    let mut damaged = good.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    server.replace(id, 1, damaged);
    assert_eq!(a.sync(&server).await.unwrap().refused_total, 1);
    assert_eq!(a.cursor().await, 1);
    server.replace(id, 1, good);
    stays_stopped_after_finding(&mut a, &server, LogFault::Rewritten { seq: 1 }).await;
}

/// An envelope behind every device's cursor is rewritten: no sync finds it, and only a device
/// that reads the log anew has another fingerprint at the same position.
#[tokio::test]
async fn a_rewrite_behind_the_cursor_goes_unseen_but_in_a_new_devices_fingerprint() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b0", "b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    let other = server.log(id)[2].clone();
    server.replace(id, 1, other);
    send(&mut b, &server, "b4").await;
    a.sync(&server).await.unwrap();
    assert_eq!(stored(&mut a).await, None);
    assert_eq!(stored(&mut b).await, None);
    let mut c = another(&server, &a).await;
    c.sync(&server).await.unwrap();
    let ours = fingerprint(&mut a.conn, id).await.unwrap();
    let theirs = fingerprint(&mut c.conn, id).await.unwrap();
    assert_eq!(ours.seq, theirs.seq);
    assert_ne!(ours.digest, theirs.digest);
    assert_ne!(a.settings().await, c.settings().await);
}

fn page_reads(server: &Server, from: usize) -> usize {
    server.with(|s| {
        s.requests[from..]
            .iter()
            .filter(|(m, t)| *m == Method::Get && t.contains("/batches"))
            .count()
    })
}

/// What reading the envelope at the cursor costs: its bytes in every sync, whatever its size, and
/// one page read more where the new envelopes fill whole pages.
#[tokio::test]
async fn the_anchor_costs_its_bytes_and_a_page_where_new_envelopes_fill_pages() {
    for page in [1u32, 4, 256] {
        let server = Server::new();
        server.with(|s| s.caps.max_page_batches = Some(page));
        let [mut a, mut b] = pair(&server).await;
        let id = a.credentials.library_id();
        b.run("INSERT INTO settings VALUES ('big', hex(randomblob(100000)))")
            .await;
        b.sync(&server).await.unwrap();
        a.sync(&server).await.unwrap();
        let anchor = server.log(id)[0].len();
        assert!(anchor > 200_000);
        let pages = format!("/v1/libraries/{}/batches", id.to_hex());
        let from = server.with(|s| s.requests.len());
        a.sync(&server).await.unwrap();
        assert_eq!(page_reads(&server, from), 1, "page {page}");
        let bytes = server.bytes_since(from, &pages);
        assert!(
            (anchor..anchor + 64).contains(&bytes),
            "page {page}: {bytes} bytes"
        );
        for new in [1usize, 4] {
            for k in 0..new {
                send(&mut b, &server, &format!("n{new}{k}")).await;
            }
            let from = server.with(|s| s.requests.len());
            a.sync(&server).await.unwrap();
            let without = new.div_ceil(page as usize);
            let with = (new + 1).div_ceil(page as usize);
            assert_eq!(page_reads(&server, from), with, "page {page}, {new} new");
            assert!(with <= without + 1);
        }
    }
}

/// An append whose request the sync had given to the transport when another connection stored a
/// fault reaches the server; the sync then fails with the fault and acknowledges nothing.
#[tokio::test]
async fn an_append_given_to_the_transport_before_a_fault_is_stored_still_reaches_the_server() {
    let dir = scratch("append-on-its-way");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    x.sync(&server).await.unwrap();
    x.run("INSERT INTO settings VALUES ('x', 'x')").await;
    let hold = server.hold_if(is_append, false);
    let mut held = Box::pin(sync(&mut x.conn, &x.replica, &server, &x.credentials, now));
    tokio::select! {
        _ = &mut held => panic!("the append was not held"),
        () = hold.reached() => {}
    }
    server.roll_back(id, 1);
    assert!(matches!(y.sync(&server).await, Err(Error::LogFault(_))));
    hold.release();
    assert!(matches!(held.await, Err(Error::LogFault(_))));
    assert_eq!(server.log(id).len(), 2);
    assert_eq!(outbox_len(&mut x).await, 3);
}

fn reads_after(request: &fongo_sync_client::Request, after: u64) -> bool {
    is_read(request)
        && fongo_sync_client::protocol::api::Route::parse(&request.target)
            .and_then(|(_, query)| {
                fongo_sync_client::protocol::api::ReadBatches::parse_query(query.unwrap_or("")).ok()
            })
            .is_some_and(|read| read.after == after)
}

/// A sync looking for its recorded envelope in the log when another connection stores a fault
/// does not append it.
#[tokio::test]
async fn a_recorded_envelope_is_not_appended_once_another_connection_stores_a_fault() {
    let dir = scratch("recorded-not-sent");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    x.sync(&server).await.unwrap();
    x.run("INSERT INTO settings VALUES ('x', 'x')").await;
    server.fail_if(is_append);
    assert!(matches!(x.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    // An envelope of a later format stops the pull, so the sync looks for its recorded envelope.
    let keys = x.credentials.library().clone();
    let later = support::seal_as(
        fongo_sync_client::protocol::ENVELOPE_FORMAT + 1,
        b"later",
        &keys,
        0,
        b.credentials.device(),
        [7; 24],
    );
    server.inject(id, later);
    let hold = server.hold_if(|r| reads_after(r, 3), false);
    let mut held = Box::pin(sync(&mut x.conn, &x.replica, &server, &x.credentials, now));
    tokio::select! {
        _ = &mut held => panic!("the look for the recorded envelope was not held"),
        () = hold.reached() => {}
    }
    server.roll_back(id, 1);
    assert!(matches!(y.sync(&server).await, Err(Error::LogFault(_))));
    let appends = server.count(Method::Post, "/v1/libraries");
    hold.release();
    assert!(matches!(held.await, Err(Error::LogFault(_))));
    assert_eq!(server.count(Method::Post, "/v1/libraries"), appends);
    assert_eq!(server.log(id).len(), 1);
}

/// Two faults rest on what the server served one sync: one not stored, because writing it failed,
/// is not found again by a server that then serves the log honestly.
#[tokio::test]
async fn reuse_and_divergence_not_stored_are_not_found_again() {
    let crash = "CREATE TEMP TRIGGER crash BEFORE UPDATE OF fault_kind ON fongo_client_state
                 WHEN NEW.fault_kind IS NOT NULL BEGIN SELECT RAISE(ABORT, 'crash'); END";
    // Reuse: the answer names other bytes; the next sync finds the envelope where it landed.
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    a.run(crash).await;
    let wrong = AnswersAt {
        server: &server,
        at: 2,
    };
    let failed = sync(&mut a.conn, &a.replica, &wrong, &a.credentials, now).await;
    assert!(matches!(failed, Err(Error::Database(_))), "{failed:?}");
    a.run("DROP TRIGGER temp.crash").await;
    a.sync(&server).await.unwrap();
    assert_eq!(stored(&mut a).await, None);
    assert_eq!(outbox_len(&mut a).await, 0);

    // Divergence: the sync that read the other log stores nothing and reads the log again.
    let dir = scratch("divergence-lost");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    x.sync(&server).await.unwrap();
    for key in ["b1", "b2", "b3"] {
        send(&mut b, &server, key).await;
    }
    let other = server.log(id)[0].clone();
    x.run(crash).await;
    let hold = server.hold_if(is_read, true);
    let first = x.sync(&server);
    let second = async {
        hold.reached().await;
        server.replace(id, 2, other);
        y.sync(&server).await.unwrap();
        hold.release();
    };
    let (failed, ()) = tokio::join!(first, second);
    assert!(matches!(failed, Err(Error::Database(_))), "{failed:?}");
    x.run("DROP TRIGGER temp.crash").await;
    x.sync(&server).await.unwrap();
    assert_eq!(stored(&mut x).await, None);
}

async fn in_flight(peer: &mut Peer) -> Option<Vec<u8>> {
    sqlx::query_scalar("SELECT sending_envelope FROM fongo_client_state")
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

/// The server refuses a recorded envelope while another connection stores a fault: the sync
/// that hears the refusal writes nothing more, and the record stays for the resolution.
#[tokio::test]
async fn a_refused_envelope_is_not_cleared_once_another_connection_stores_a_fault() {
    let dir = scratch("refused-after-fault");
    let path = dir.join("x.db");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut x = Peer::new_at(&path, made.clone()).await;
    let mut y = Peer::open(&path, made).await;
    let id = x.credentials.library_id();
    for key in ["b1", "b2"] {
        send(&mut b, &server, key).await;
    }
    x.sync(&server).await.unwrap();
    x.run("INSERT INTO settings VALUES ('x', 'x')").await;
    server.fail_if(is_append);
    assert!(matches!(x.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    let recorded = in_flight(&mut x).await;
    assert!(recorded.is_some());
    // The server now refuses it as too large, and its answer waits.
    server.with(|s| s.caps.max_batch_bytes = Some(64));
    let hold = server.hold_if(is_append, true);
    let mut held = Box::pin(sync(&mut x.conn, &x.replica, &server, &x.credentials, now));
    tokio::select! {
        _ = &mut held => panic!("the append was not held"),
        () = hold.reached() => {}
    }
    server.roll_back(id, 1);
    assert!(matches!(y.sync(&server).await, Err(Error::LogFault(_))));
    hold.release();
    assert!(matches!(held.await, Err(Error::LogFault(_))));
    assert_eq!(in_flight(&mut x).await, recorded);
}
