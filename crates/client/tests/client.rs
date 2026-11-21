mod support;

use std::future::Future;
use std::sync::Mutex;

use fongo_sync_client::protocol::api::{
    BatchPage, ErrorBody, ErrorCode, Notification, ReadBatches, Route, StoredBatch, devices_path,
};
use fongo_sync_client::protocol::{
    DeviceId, LibraryId, LibraryKeys, SigningKey, generate_signing_key, random_nonce, seal,
};
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{Conflict, Held, Replica};
use fongo_sync_client::{
    Blocked, Credentials, Error, Method, Refusal, Request, Response, Transport, TransportError,
    add_device, create_library, join_library, sync, worth_syncing,
};
use sqlx::SqliteConnection;
use support::{Peer, Server, assert_converged, assert_no_change_sent_twice, now, scratch, settle};

/// A library made on one device and joined from its recovery code on another.
async fn pair(server: &Server) -> [Peer; 2] {
    let made = create_library(server, now).await.unwrap();
    let code = made.recovery_code();
    let joined = join_library(server, &code, now).await.unwrap();
    assert_eq!(joined.library_id(), made.library_id());
    assert_ne!(joined.device_id(), made.device_id());
    [Peer::new(made).await, Peer::new(joined).await]
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

#[tokio::test]
async fn a_library_made_on_one_device_is_joined_from_its_recovery_code_and_syncs_both_ways() {
    let server = Server::new();
    let mut peers = pair(&server).await;
    peers[0]
        .run("INSERT INTO books (title, year) VALUES ('Middlemarch', 1871)")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'slow start')")
        .await;
    let sent = peers[0].sync(&server).await.unwrap();
    assert!(sent.sent > 0);
    assert_eq!(sent.received, 0);

    let got = peers[1].sync(&server).await.unwrap();
    assert_eq!(got.received, sent.sent);
    assert_eq!(got.applied.touched.len(), 2, "{got:#?}");
    assert!(got.refused.is_empty());
    peers[1]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    peers[1].run("UPDATE books SET year = 1872").await;
    peers[1].sync(&server).await.unwrap();
    let back = peers[0].sync(&server).await.unwrap();
    assert!(back.received > 0);
    assert_eq!(back.head, 2);

    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 3, "{state:#?}");
    assert!(state[0].ends_with("Middlemarch y=1872"), "{state:#?}");
}

#[tokio::test]
async fn two_replicas_converge_through_interleaved_edits() {
    let server = Server::new();
    let mut peers = pair(&server).await;
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0].sync(&server).await.unwrap();
    peers[1].sync(&server).await.unwrap();
    for round in 0..4 {
        peers[0]
            .run(&format!("UPDATE books SET year = {}", 1870 + round))
            .await;
        peers[1]
            .run(&format!("UPDATE books SET title = 'title{round}'"))
            .await;
        peers[1]
            .run(&format!(
                "INSERT INTO notes (book_id, body) SELECT id, 'note{round}' FROM books"
            ))
            .await;
        peers[0]
            .run(&format!(
                "INSERT INTO settings VALUES ('k{}', 'v{round}')
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                round % 2
            ))
            .await;
        let (first, second) = peers.split_at_mut(1);
        if round % 2 == 0 {
            first[0].sync(&server).await.unwrap();
            second[0].sync(&server).await.unwrap();
        } else {
            second[0].sync(&server).await.unwrap();
            first[0].sync(&server).await.unwrap();
        }
    }
    settle(&mut peers, &server).await;
    let state = assert_converged(&mut peers).await;
    assert!(state[0].ends_with("title3 y=1873"), "{state:#?}");
    assert_eq!(state.len(), 1 + 4 + 2, "{state:#?}");
}

#[tokio::test]
async fn three_replicas_converge_one_of_them_added_by_another_device() {
    let server = Server::new();
    let [a, b] = pair(&server).await;
    let key = generate_signing_key().unwrap();
    add_device(&server, &a.credentials, &key.verifying_key(), now)
        .await
        .unwrap();
    let c = Peer::new(Credentials::new(a.credentials.library().clone(), key, 0)).await;
    let mut peers = [a, b, c];

    for (who, sql) in [
        (0, "INSERT INTO books (title) VALUES ('Middlemarch')"),
        (2, "INSERT INTO books (title) VALUES ('Persuasion')"),
        (1, "INSERT INTO settings VALUES ('theme', 'dark')"),
        (2, "INSERT INTO settings VALUES ('theme', 'light')"),
        (0, "UPDATE books SET year = 1871"),
    ] {
        peers[who].run(sql).await;
        peers[(who + 1) % 3].sync(&server).await.unwrap();
        peers[who].sync(&server).await.unwrap();
    }
    peers[1]
        .run("INSERT INTO notes (book_id, body) SELECT id, 'shared' FROM books")
        .await;
    peers[2]
        .run("DELETE FROM books WHERE title = 'Persuasion'")
        .await;
    settle(&mut peers, &server).await;

    let state = assert_converged(&mut peers).await;
    let books = state.iter().filter(|l| l.starts_with("book ")).count();
    let settings = state.iter().filter(|l| l.starts_with("setting ")).count();
    assert_eq!((books, settings), (1, 1), "{state:#?}");
    assert!(
        state
            .iter()
            .all(|l| !l.starts_with("note ") || l.contains("shared")),
        "{state:#?}"
    );
}

#[tokio::test]
async fn credentials_stored_and_read_back_still_sync() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    a.sync(&server).await.unwrap();
    let stored = b.credentials.to_bytes();
    b.credentials = Credentials::from_bytes(&stored[..]).unwrap();
    b.sync(&server).await.unwrap();
    assert_eq!(b.settings().await, ["theme=dark"]);
}

#[tokio::test]
async fn a_new_database_with_old_credentials_takes_back_what_the_device_sent() {
    let server = Server::new();
    let [mut a, _] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.sync(&server).await.unwrap();
    a.sync(&server).await.unwrap();

    let mut again = Peer::new(a.credentials.clone()).await;
    let report = again.sync(&server).await.unwrap();
    assert!(report.received > 0);
    assert!(report.refused.is_empty());
    assert_converged(&mut [a, again]).await;
}

#[tokio::test]
async fn a_database_restored_from_a_backup_takes_back_what_the_device_sent_since() {
    let dir = scratch("restore");
    let backup = dir.join("backup.db");
    let server = Server::new();
    let [made, mut b] = pair(&server).await;
    let mut a = Peer::new_at(&dir.join("live.db"), made.credentials).await;
    a.run("INSERT INTO settings VALUES ('early', 'x')").await;
    a.sync(&server).await.unwrap();
    a.back_up(&backup).await;
    a.run("INSERT INTO settings VALUES ('later', 'x')").await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.sync(&server).await.unwrap();
    b.run("INSERT INTO settings VALUES ('theirs', 'x')").await;
    b.sync(&server).await.unwrap();

    let mut restored = Peer::open(&backup, a.credentials.clone()).await;
    let report = restored.sync(&server).await.unwrap();
    assert!(report.refused.is_empty(), "{report:#?}");
    assert_eq!(
        restored.settings().await,
        ["early=x", "later=x", "theirs=x"]
    );
    let mut peers = [a, b, restored];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn two_databases_sharing_one_set_of_credentials_converge() {
    let server = Server::new();
    let [a, b] = pair(&server).await;
    let twin = Peer::new(a.credentials.clone()).await;
    let mut peers = [a, b, twin];
    for (who, sql) in [
        (0, "INSERT INTO settings VALUES ('one', 'x')"),
        (2, "INSERT INTO settings VALUES ('two', 'x')"),
        (1, "INSERT INTO books (title) VALUES ('Middlemarch')"),
        (2, "UPDATE settings SET value = 'y' WHERE key = 'one'"),
        (0, "INSERT INTO settings VALUES ('three', 'x')"),
    ] {
        peers[who].run(sql).await;
        peers[who].sync(&server).await.unwrap();
        peers[(who + 2) % 3].sync(&server).await.unwrap();
    }
    settle(&mut peers, &server).await;
    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 4, "{state:#?}");
}

fn setting(name: &str, clock: u64) -> Vec<Change> {
    let gid = Gid::derive("settings", &[Value::Text(name.into())]);
    let clock = Hlc {
        wall_ms: clock,
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

fn sealed_by(changes: &[Change], keys: &LibraryKeys, key: &SigningKey) -> Vec<u8> {
    seal(changes, keys, 0, key, random_nonce().unwrap())
}

#[tokio::test]
async fn the_recovery_keys_envelope_is_refused_and_an_unlisted_signer_blocks_the_pull() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    b.run("INSERT INTO settings VALUES ('before', 'x')").await;
    b.sync(&server).await.unwrap();
    server.inject(
        id,
        sealed_by(&setting("recovery", 1), &keys, &keys.recovery_signing_key()),
    );
    let stranger = generate_signing_key().unwrap();
    server.inject(id, sealed_by(&setting("stranger", 1), &keys, &stranger));
    b.run("INSERT INTO settings VALUES ('after', 'x')").await;
    b.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;

    let lists = || server.count(Method::Get, &devices_path(id));
    let listed = lists();
    let report = a.sync(&server).await.unwrap();
    let refused: Vec<(u64, Refusal)> = report.refused.iter().map(|r| (r.seq, r.reason)).collect();
    assert_eq!(refused, [(2, Refusal::RecoveryKey)]);
    let blocked = Some(Blocked {
        seq: 3,
        device: DeviceId::of(&stranger.verifying_key()),
    });
    assert_eq!(report.blocked, blocked);
    // Nothing from the blocked envelope on is applied, but what is before it is, and the
    // outbox still went out.
    assert_eq!(a.settings().await, ["before=x", "mine=x"]);
    assert_eq!(a.cursor().await, 2);
    assert!(report.sent > 0);
    // The unlisted signer made the client fetch the list once more, and only once.
    assert_eq!(lists() - listed, 2);

    let again = a.sync(&server).await.unwrap();
    assert_eq!((again.blocked, again.received), (blocked, 0));
    assert_eq!(a.cursor().await, 2);

    server.register(id, stranger.verifying_key());
    let unblocked = a.sync(&server).await.unwrap();
    assert_eq!(unblocked.blocked, None);
    assert_eq!(
        a.settings().await,
        ["after=x", "before=x", "mine=x", "stranger=stranger"]
    );
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_server_that_keeps_leaving_a_device_out_blocks_at_its_envelope_every_time() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let hidden = generate_signing_key().unwrap();
    let mut c = Peer::new(Credentials::new(keys.clone(), hidden.clone(), 0)).await;
    add_device(&server, &b.credentials, &hidden.verifying_key(), now)
        .await
        .unwrap();
    server.with(|s| s.hidden.push(hidden.verifying_key()));
    b.run("INSERT INTO settings VALUES ('b1', 'x')").await;
    b.sync(&server).await.unwrap();
    c.run("INSERT INTO settings VALUES ('c', 'x')").await;
    c.sync(&server).await.unwrap();
    b.run("INSERT INTO settings VALUES ('b2', 'x')").await;
    b.sync(&server).await.unwrap();

    for _ in 0..3 {
        let report = a.sync(&server).await.unwrap();
        assert_eq!(report.blocked.map(|b| b.seq), Some(2));
        assert_eq!(a.settings().await, ["b1=x"]);
        assert_eq!(a.cursor().await, 1);
    }
    server.with(|s| s.hidden.clear());
    let mut peers = [a, b, c];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
}

/// Lets a new device join and write, and a listed device write after it, each time the client
/// fetches the device list.
struct JoinsWhileListing<'a> {
    server: &'a Server,
    keys: LibraryKeys,
    joiners: Mutex<Vec<SigningKey>>,
    listed: SigningKey,
}

impl Transport for JoinsWhileListing<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        let listing = request.method == Method::Get && request.target.ends_with("/devices");
        let response = self.server.send(request).await;
        let joiner = listing.then(|| self.joiners.lock().unwrap().pop());
        if let Some(Some(key)) = joiner {
            let id = self.keys.id;
            self.server.register(id, key.verifying_key());
            let name = DeviceId::of(&key.verifying_key()).to_hex();
            let changes = setting(&name, 5);
            self.server
                .inject(id, sealed_by(&changes, &self.keys, &key));
            let changes = setting(&format!("after-{name}"), 5);
            self.server
                .inject(id, sealed_by(&changes, &self.keys, &self.listed));
        }
        response
    }
}

#[tokio::test]
async fn devices_that_join_during_a_sync_are_found_by_that_sync() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let [mut a, mut b] = pair(&server).await;
    for n in 0..2 {
        b.run(&format!("INSERT INTO settings VALUES ('b{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    let keys = a.credentials.library().clone();
    let joining = JoinsWhileListing {
        server: &server,
        keys,
        joiners: Mutex::new(vec![
            generate_signing_key().unwrap(),
            generate_signing_key().unwrap(),
        ]),
        listed: b.credentials.device().clone(),
    };
    let report = sync(&mut a.conn, &a.replica, &joining, &a.credentials, now)
        .await
        .unwrap();
    assert!(report.refused.is_empty(), "{report:#?}");
    assert_eq!(report.blocked, None);
    assert_eq!(a.settings().await.len(), 6);
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn an_envelope_for_another_library_garbage_or_a_forgery_is_skipped_and_counted() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let elsewhere = LibraryKeys::new(LibraryId([7; 16]), [8; 32]);
    server.inject(
        keys.id,
        sealed_by(&setting("elsewhere", 1), &elsewhere, b.credentials.device()),
    );
    server.inject(keys.id, b"not an envelope".to_vec());
    // A listed key whose holder lacks the library secret.
    let forger = generate_signing_key().unwrap();
    server.register(keys.id, forger.verifying_key());
    let wrong_secret = LibraryKeys::new(keys.id, [0; 32]);
    server.inject(
        keys.id,
        sealed_by(&setting("forged", 1), &wrong_secret, &forger),
    );
    b.run("INSERT INTO settings VALUES ('real', 'x')").await;
    b.sync(&server).await.unwrap();

    let report = a.sync(&server).await.unwrap();
    let reasons: Vec<Refusal> = report.refused.iter().map(|r| r.reason).collect();
    assert_eq!(
        reasons,
        [
            Refusal::WrongLibrary,
            Refusal::Malformed,
            Refusal::Undecryptable
        ]
    );
    assert_eq!(report.refused[1].device, None);
    assert_eq!(a.settings().await, ["real=x"]);
    assert_eq!(a.cursor().await, 4);
}

#[tokio::test]
async fn an_envelope_forged_in_this_devices_name_is_refused_and_counted() {
    let server = Server::new();
    let [mut a, _b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    a.sync(&server).await.unwrap();
    let forger = generate_signing_key().unwrap();
    let mut forged = sealed_by(&setting("forged", 1), &keys, &forger);
    // Format byte, then the library id, then the device id.
    forged[17..33].copy_from_slice(&a.credentials.device_id().0);
    server.inject(keys.id, forged);

    let report = a.sync(&server).await.unwrap();
    let refused: Vec<_> = report
        .refused
        .iter()
        .map(|r| (r.seq, r.device, r.reason))
        .collect();
    assert_eq!(
        refused,
        [(2, Some(a.credentials.device_id()), Refusal::BadSignature)]
    );
    assert_eq!(report.refused_total, 1);
    assert_eq!(a.settings().await, ["mine=x"]);
    assert_eq!(a.cursor().await, 2);
}

#[tokio::test]
async fn a_device_missing_from_a_stale_list_is_found_on_the_one_refresh() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.sync(&server).await.unwrap();
    server.with(|s| s.stale_devices = true);
    let report = a.sync(&server).await.unwrap();
    assert!(report.refused.is_empty(), "{report:#?}");
    assert_eq!(a.settings().await, ["theme=dark"]);
}

/// Three envelopes from the second device follow one the first device has already read.
async fn behind(server: &Server) -> [Peer; 2] {
    let [mut a, mut b] = pair(server).await;
    b.run("INSERT INTO settings VALUES ('s0', 'x')").await;
    b.sync(server).await.unwrap();
    a.sync(server).await.unwrap();
    for n in 1..4 {
        b.run(&format!("INSERT INTO settings VALUES ('s{n}', 'x')"))
            .await;
        b.sync(server).await.unwrap();
    }
    assert_eq!(a.cursor().await, 1);
    [a, b]
}

type PageTamper = fn(BatchPage) -> Vec<u8>;

fn swap(mut page: BatchPage) -> Vec<u8> {
    page.batches.swap(0, 1);
    page.encode()
}

fn gap(mut page: BatchPage) -> Vec<u8> {
    page.batches.remove(1);
    page.encode()
}

fn repeat(mut page: BatchPage) -> Vec<u8> {
    let first = page.batches[0].clone();
    page.batches.insert(1, first);
    page.batches.truncate(3);
    page.encode()
}

fn back_past_the_cursor(mut page: BatchPage) -> Vec<u8> {
    let old = StoredBatch {
        seq: 1,
        envelope: page.batches[0].envelope.clone(),
    };
    page.batches.insert(0, old);
    page.batches.truncate(3);
    page.encode()
}

fn empty_but_more(_: BatchPage) -> Vec<u8> {
    BatchPage {
        batches: Vec::new(),
        more: true,
    }
    .encode()
}

fn garbage(_: BatchPage) -> Vec<u8> {
    b"\x01\x00\xff\xff\xff\xff\x0f".to_vec()
}

#[tokio::test]
async fn a_page_that_does_not_continue_the_log_is_an_error_and_moves_nothing() {
    let tampers: [(&str, PageTamper); 6] = [
        ("reordered", swap),
        ("gapped", gap),
        ("repeated", repeat),
        ("at or below the cursor", back_past_the_cursor),
        ("empty but more", empty_but_more),
        ("garbage", garbage),
    ];
    for (what, tamper) in tampers {
        let server = Server::new();
        let [mut a, mut b] = behind(&server).await;
        server.tamper(tamper);
        let result = a.sync(&server).await;
        assert!(
            matches!(result, Err(Error::MalformedResponse(_))),
            "{what}: {result:?}"
        );
        assert_eq!(a.cursor().await, 1, "{what}");
        assert_eq!(a.settings().await, ["s0=x"], "{what}");

        server.behave();
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

#[tokio::test]
async fn a_page_longer_than_asked_for_is_an_error() {
    let server = Server::new();
    let [mut a, _b] = behind(&server).await;
    server.with(|s| s.caps.max_page_batches = Some(2));
    server.tamper(|mut page| {
        page.batches.push(StoredBatch {
            seq: page.batches.last().unwrap().seq + 1,
            envelope: Vec::new(),
        });
        page.encode()
    });
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::MalformedResponse(_))
    ));
    assert_eq!(a.cursor().await, 1);
}

/// Answers every page read with one envelope that is not an envelope, and says more follow.
struct EndlessJunk<'a> {
    server: &'a Server,
}

impl Transport for EndlessJunk<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        if let (Method::Get, Some((Route::Batches(_), Some(query)))) =
            (request.method, Route::parse(&request.target))
        {
            let read = ReadBatches::parse_query(query).unwrap();
            let page = BatchPage {
                batches: vec![StoredBatch {
                    seq: read.after + 1,
                    envelope: vec![0; 64],
                }],
                more: true,
            };
            return Ok(Response {
                status: 200,
                headers: Vec::new(),
                body: page.encode(),
            });
        }
        self.server.send(request).await
    }
}

#[tokio::test]
async fn endless_junk_pages_end_the_call_with_progress_kept_and_more_reported() {
    let server = Server::new();
    let [mut a, _] = pair(&server).await;
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let junk = EndlessJunk { server: &server };
    let mut cursor = 0;
    for _ in 0..2 {
        let report = sync(&mut a.conn, &a.replica, &junk, &a.credentials, now)
            .await
            .unwrap();
        assert!(report.more);
        assert!(report.refused_total > 100);
        assert_eq!(report.refused.len(), 100);
        assert!(
            report
                .refused
                .iter()
                .all(|r| r.reason == Refusal::Malformed)
        );
        assert_eq!(a.cursor().await, cursor + report.refused_total as u64);
        cursor = a.cursor().await;
    }
    assert_eq!(server.log(a.credentials.library_id()).len(), 1);
}

#[tokio::test]
async fn pages_read_before_a_failure_stay_applied() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let [mut a, mut b] = pair(&server).await;
    for n in 0..3 {
        b.run(&format!("INSERT INTO settings VALUES ('b{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    // Capabilities, the first page, the device list, the second page, then the third fails.
    server.fail_at(5);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    assert_eq!(a.cursor().await, 2);
    assert_eq!(a.settings().await, ["b0=x", "b1=x"]);
    server.behave();
    a.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

type Respond = fn(&fongo_sync_client::Request) -> Option<Response>;

fn answer_if(when: bool, status: u16, body: &[u8]) -> Option<Response> {
    when.then(|| Response {
        status,
        headers: Vec::new(),
        body: body.to_vec(),
    })
}

#[tokio::test]
async fn garbage_and_refusals_from_the_server_are_typed_errors() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('font', 'serif')").await;

    let cases: [(&str, Respond); 5] = [
        ("garbage capabilities", |r| {
            answer_if(r.target.ends_with("capabilities"), 200, b"{")
        }),
        ("garbage device list", |r| {
            answer_if(r.target.ends_with("/devices"), 200, b"[]")
        }),
        ("garbage append answer", |r| {
            answer_if(r.method == Method::Post, 200, br#"{"seq":"one"}"#)
        }),
        ("append at sequence zero", |r| {
            answer_if(r.method == Method::Post, 200, br#"{"seq":0}"#)
        }),
        ("an html error page", |r| {
            answer_if(r.target.contains("/batches?"), 502, b"<html>")
        }),
    ];
    for (what, respond) in cases {
        server.respond(respond);
        let result = a.sync(&server).await;
        match what {
            "an html error page" => assert!(
                matches!(
                    result,
                    Err(Error::Server {
                        status: 502,
                        code: ErrorCode::Unknown
                    })
                ),
                "{what}: {result:?}"
            ),
            _ => assert!(
                matches!(result, Err(Error::MalformedResponse(_))),
                "{what}: {result:?}"
            ),
        }
        server.behave();
    }
    server.respond(|r| {
        let full = serde_json::to_vec(&ErrorBody::new(ErrorCode::LibraryFull)).unwrap();
        answer_if(r.method == Method::Post, 507, &full)
    });
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::Server {
            status: 507,
            code: ErrorCode::LibraryFull
        })
    ));
    server.behave();
    settle(&mut [a, b], &server).await;
}

#[tokio::test]
async fn a_server_speaking_a_newer_protocol_or_a_wrong_clock_is_named() {
    let server = Server::new();
    let [mut a, _] = pair(&server).await;
    server.with(|s| s.caps.protocol_version += 1);
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::NewerProtocol {
            server: 2,
            client: 1
        })
    ));
    assert!(matches!(
        create_library(&server, now).await,
        Err(Error::NewerProtocol { .. })
    ));
    server.with(|s| s.caps.protocol_version -= 1);

    server.with(|s| s.now += 3600);
    assert!(matches!(a.sync(&server).await, Err(Error::ClockSkew)));
    server.with(|s| s.now -= 3600);
    a.sync(&server).await.unwrap();
}

#[tokio::test]
async fn a_mistyped_recovery_code_is_refused_before_anything_is_sent() {
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut code = made.recovery_code();
    code.replace_range(0..1, if code.starts_with('0') { "1" } else { "0" });
    let before = server.with(|s| s.requests.len());
    assert!(matches!(
        join_library(&server, &code, now).await,
        Err(Error::RecoveryCode(_))
    ));
    assert_eq!(server.with(|s| s.requests.len()), before);
}

/// The first device has a book and a note to send and the second device's setting to read; its
/// sync makes four requests: capabilities, a page, the device list and one append.
async fn crash_scene(server: &Server) -> [Peer; 2] {
    let [mut a, mut b] = pair(server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'slow start')")
        .await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.sync(server).await.unwrap();
    [a, b]
}

#[tokio::test]
async fn a_sync_cut_short_at_any_request_loses_and_repeats_nothing() {
    for lose in [false, true] {
        for n in 1..=5 {
            let server = Server::new();
            let [mut a, b] = crash_scene(&server).await;
            if lose {
                server.lose_at(n);
            } else {
                server.fail_at(n);
            }
            let result = a.sync(&server).await;
            assert_eq!(result.is_ok(), n == 5, "lose={lose} n={n}: {result:?}");
            // The page is applied with the cursor or not at all.
            let applied = a.settings().await == ["theme=dark"];
            assert_eq!(applied, a.cursor().await >= 1, "lose={lose} n={n}");
            assert_eq!(applied, n >= 4, "lose={lose} n={n}");

            server.behave();
            let mut peers = [a, b];
            settle(&mut peers, &server).await;
            let state = assert_converged(&mut peers).await;
            assert_eq!(state.len(), 3, "lose={lose} n={n}: {state:#?}");
            let keys = peers[0].credentials.library().clone();
            assert_eq!(server.log(keys.id).len(), 2, "lose={lose} n={n}");
            assert_no_change_sent_twice(&server, &keys);
        }
    }
}

#[tokio::test]
async fn a_lost_append_is_sent_again_byte_for_byte_before_anything_new() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    let keys = a.credentials.library().clone();
    let theirs = server.changes(&keys).len();
    server.lose_at(4);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    a.run("INSERT INTO settings VALUES ('font', 'serif')").await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(server.log(keys.id).len(), 3);
    assert_no_change_sent_twice(&server, &keys);
    assert_eq!(report.sent, server.changes(&keys).len() - theirs);
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_recorded_envelope_the_server_now_refuses_as_too_large_is_sealed_again_to_fit() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for n in 0..10 {
        a.run(&format!(
            "INSERT INTO settings VALUES ('key{n}', 'value{n}')"
        ))
        .await;
    }
    // Capabilities, a page, then the append never arrives: the envelope stays recorded.
    server.fail_at(3);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    // A passing failure keeps the record for next time.
    server.behave();
    server.respond(|r| {
        let internal = serde_json::to_vec(&ErrorBody::new(ErrorCode::Internal)).unwrap();
        answer_if(r.method == Method::Post, 500, &internal)
    });
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::Server { status: 500, .. })
    ));
    server.behave();
    server.with(|s| s.caps.max_batch_bytes = Some(400));

    let report = a.sync(&server).await.unwrap();
    let keys = a.credentials.library().clone();
    let log = server.log(keys.id);
    assert!(log.len() > 1 && log.iter().all(|e| e.len() <= 400));
    assert_eq!(report.sent, server.changes(&keys).len());
    assert_no_change_sent_twice(&server, &keys);
    b.sync(&server).await.unwrap();
    assert_eq!(b.settings().await.len(), 10);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_large_outbox_is_split_to_fit_the_servers_limit() {
    let server = Server::new();
    server.with(|s| s.caps.max_batch_bytes = Some(400));
    let [mut a, mut b] = pair(&server).await;
    for n in 0..20 {
        a.run(&format!(
            "INSERT INTO settings VALUES ('key{n:02}', 'value{n}')"
        ))
        .await;
    }
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.sent, server.changes(a.credentials.library()).len());
    let log = server.log(a.credentials.library_id());
    assert!(log.len() > 5, "{}", log.len());
    assert!(log.iter().all(|envelope| envelope.len() <= 400));
    assert_eq!(report.head, log.len() as u64);
    assert_eq!(a.cursor().await, log.len() as u64);
    b.sync(&server).await.unwrap();
    assert_eq!(b.settings().await.len(), 20);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_change_too_large_for_any_envelope_is_an_error_naming_it() {
    let server = Server::new();
    server.with(|s| s.caps.max_batch_bytes = Some(400));
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run(&format!(
        "INSERT INTO notes (book_id, body) VALUES (1, '{}')",
        "x".repeat(500)
    ))
    .await;
    for _ in 0..2 {
        match a.sync(&server).await {
            Err(Error::ChangeTooLarge {
                collection,
                field,
                bytes,
                max,
            }) => {
                assert_eq!(
                    (collection.as_str(), field.as_str(), max),
                    ("notes", "body", 400)
                );
                assert!(bytes > 400);
            }
            other => panic!("{other:?}"),
        }
    }
    b.sync(&server).await.unwrap();
    assert_eq!(b.lines("SELECT title FROM books").await, ["Middlemarch"]);
}

#[tokio::test]
async fn a_maximum_below_any_envelope_is_named_as_such() {
    let server = Server::new();
    server.with(|s| s.caps.max_batch_bytes = Some(100));
    let [mut a, _] = pair(&server).await;
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::MaxBatchTooSmall { max: 100 })
    ));
}

#[tokio::test]
async fn a_concurrent_edit_to_a_field_that_reports_conflicts_reaches_the_report() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();

    a.run("UPDATE notes SET body = 'one'").await;
    later();
    b.run("UPDATE notes SET body = 'two'").await;
    assert!(a.sync(&server).await.unwrap().applied.conflicts.is_empty());
    let report = b.sync(&server).await.unwrap();
    let note = b.replica.gid_of(&mut b.conn, "notes", 1).await.unwrap();
    assert_eq!(
        report.applied.conflicts,
        [Conflict {
            collection: "notes".into(),
            gid: note.unwrap(),
            key: Some("[1]".into()),
            field: "body".into(),
            base: Some(Value::Text("base".into())),
            local: Value::Text("two".into()),
            remote: Value::Text("one".into()),
            remote_won: false,
        }]
    );
    settle(&mut [a, b], &server).await;
}

#[tokio::test]
async fn an_edit_made_after_reading_this_devices_edit_is_not_a_conflict() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();

    a.run("UPDATE notes SET body = 'one'").await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    later();
    b.run("UPDATE notes SET body = 'two'").await;
    b.sync(&server).await.unwrap();
    let report = a.sync(&server).await.unwrap();
    assert!(report.applied.conflicts.is_empty(), "{report:#?}");
    let mut peers = [a, b];
    settle(&mut peers, &server).await;
    let state = assert_converged(&mut peers).await;
    assert!(state.iter().any(|l| l.contains(" two b=")), "{state:#?}");
}

#[tokio::test]
async fn edits_before_this_devices_envelope_in_the_log_conflict_and_edits_after_it_do_not() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    let note = a.replica.gid_of(&mut a.conn, "notes", 1).await.unwrap();

    // An edit made without seeing a's, from a device a does not list yet: a's pull stops before
    // it and a's own envelope lands after it.
    later();
    let stranger = generate_signing_key().unwrap();
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let concurrent = Change {
        collection: "notes".into(),
        gid: note.unwrap(),
        field: "body".into(),
        value: Value::Text("stranger".into()),
        clock: Hlc {
            wall_ms,
            counter: 0,
            node: Gid([9; 16]),
        },
    };
    server.inject(keys.id, sealed_by(&[concurrent], &keys, &stranger));
    later();
    a.run("UPDATE notes SET body = 'one'").await;
    let report = a.sync(&server).await.unwrap();
    assert!(report.blocked.is_some());
    assert_eq!((a.cursor().await, report.head), (1, 3));

    server.register(keys.id, stranger.verifying_key());
    b.sync(&server).await.unwrap();
    later();
    b.run("UPDATE notes SET body = 'two'").await;
    b.sync(&server).await.unwrap();

    // a reads the stranger's edit, its own envelope and b's edit in one pull.
    let report = a.sync(&server).await.unwrap();
    let remote: Vec<&Value> = report.applied.conflicts.iter().map(|c| &c.remote).collect();
    assert_eq!(remote, [&Value::Text("stranger".into())], "{report:#?}");
    assert_eq!(report.received, 3);
    let mut peers = [a, b];
    settle(&mut peers, &server).await;
    assert_converged(&mut peers).await;
}

#[tokio::test]
async fn a_local_id_another_entity_takes_in_a_sync_reports_both_entities() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for peer in [&mut a, &mut b] {
        peer.run("CREATE UNIQUE INDEX books_title ON books (title)")
            .await;
    }
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    let old = b.replica.gid_of(&mut b.conn, "books", 1).await.unwrap();

    // b can write the new row only once the old one is gone, and it then takes its local id.
    a.run("DELETE FROM books").await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.sync(&server).await.unwrap();
    let new = a.replica.gid_of(&mut a.conn, "books", 1).await.unwrap();
    let report = b.sync(&server).await.unwrap();
    let touched: Vec<_> = report
        .applied
        .touched
        .iter()
        .map(|t| {
            (
                t.collection.as_str(),
                t.key.as_str(),
                Some(t.gid),
                t.deleted,
            )
        })
        .collect();
    assert_eq!(
        touched,
        [("books", "[1]", old, true), ("books", "[1]", new, false)]
    );
    assert_ne!(old, new);
    let mut peers = [a, b];
    assert_converged(&mut peers).await;
}

#[tokio::test]
async fn a_row_this_database_refuses_is_reported_held_until_a_sync_can_write_it() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    b.run(
        "CREATE TRIGGER no_drafts BEFORE INSERT ON books WHEN NEW.title = 'draft'
         BEGIN SELECT RAISE(ABORT, 'no drafts'); END",
    )
    .await;
    a.run("INSERT INTO books (title) VALUES ('draft')").await;
    a.sync(&server).await.unwrap();
    let book = a.replica.gid_of(&mut a.conn, "books", 1).await.unwrap();

    let report = b.sync(&server).await.unwrap();
    assert_eq!(report.received, 2);
    assert_eq!(report.applied.waiting, 1);
    assert_eq!(
        report.applied.held,
        [Held {
            collection: "books".into(),
            gid: book.unwrap(),
            key: None,
        }]
    );
    assert!(b.dump().await.is_empty());

    b.run("DROP TRIGGER no_drafts").await;
    let report = b.sync(&server).await.unwrap();
    assert_eq!(report.received, 0);
    assert_eq!(report.applied.waiting, 0);
    assert!(report.applied.held.is_empty());
    let mut peers = [a, b];
    assert_converged(&mut peers).await;
}

#[tokio::test]
async fn a_notification_is_worth_a_sync_only_when_it_is_past_the_cursor() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    let mine = a.sync(&server).await.unwrap();
    // Nothing came between, so the cursor is already past this device's own envelope.
    assert_eq!(a.cursor().await, mine.head);
    assert!(!worth_syncing(
        &Notification { seq: mine.head },
        a.cursor().await
    ));

    b.run("INSERT INTO settings VALUES ('font', 'serif')").await;
    let theirs = b.sync(&server).await.unwrap();
    let ping = Notification { seq: theirs.head };
    assert!(worth_syncing(&ping, a.cursor().await));
    a.sync(&server).await.unwrap();
    assert!(!worth_syncing(&ping, a.cursor().await));
}

fn assert_send<F: Future + Send>(_: F) {}

#[allow(dead_code)]
fn every_future_is_send(
    conn: &mut SqliteConnection,
    replica: &Replica,
    server: &Server,
    credentials: &Credentials,
) {
    let key = credentials.device().verifying_key();
    assert_send(sync(conn, replica, server, credentials, now));
    assert_send(create_library(server, now));
    assert_send(join_library(server, "", now));
    assert_send(add_device(server, credentials, &key, now));
}
