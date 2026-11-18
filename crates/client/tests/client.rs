mod support;

use std::future::Future;

use fongo_sync_client::protocol::api::{
    BatchPage, ErrorBody, ErrorCode, Notification, StoredBatch, devices_path,
};
use fongo_sync_client::protocol::{
    LibraryId, LibraryKeys, SigningKey, generate_signing_key, random_nonce, seal,
};
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{Conflict, Replica};
use fongo_sync_client::{
    Credentials, Error, Method, Refusal, Response, add_device, create_library, join_library, sync,
    worth_syncing,
};
use sqlx::SqliteConnection;
use support::{Peer, Server, assert_converged, assert_no_change_sent_twice, now, settle};

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
async fn envelopes_signed_by_the_recovery_key_or_an_unlisted_key_are_refused_and_the_rest_applied()
{
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

    let lists = || server.count(Method::Get, &devices_path(id));
    let listed = lists();
    let report = a.sync(&server).await.unwrap();
    let refused: Vec<(u64, Refusal)> = report.refused.iter().map(|r| (r.seq, r.reason)).collect();
    assert_eq!(
        refused,
        [(2, Refusal::RecoveryKey), (3, Refusal::UnknownDevice)]
    );
    assert_eq!(
        report.refused[1].device,
        Some(fongo_sync_client::protocol::DeviceId::of(
            &stranger.verifying_key()
        ))
    );
    assert_eq!(a.settings().await, ["after=x", "before=x"]);
    assert_eq!(a.cursor().await, 4);
    // The unlisted key made the client look at the device list once more, and only once.
    assert_eq!(lists() - listed, 2);
    let again = a.sync(&server).await.unwrap();
    assert!(again.refused.is_empty() && again.received == 0);
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
