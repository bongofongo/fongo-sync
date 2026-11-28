//! Envelopes this build cannot read: those a later version of the protocol may have written stop
//! the pull where they are, and those that are damaged or forged are skipped and counted.

#[allow(dead_code)]
mod support;

use fongo_sync_client::protocol::api::{Appended, BatchPage, ReadBatches, batches_path};
use fongo_sync_client::protocol::{
    BATCH_FORMAT, DeviceId, ENVELOPE_FORMAT, Envelope, LibraryId, LibraryKeys, SigningKey,
    encode_batch, generate_signing_key, random_nonce, seal,
};
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Credentials, Error, Method, Newer, Refusal, Request, Response, Transport,
    TransportError, Unknown, add_device, create_library, join_library, sync_with,
};
use sqlx::SqliteConnection;
use support::{
    Peer, Server, assert_converged, assert_no_change_sent_twice, batch_as, batch_with_value, now,
    re_signed, seal_as,
};

fn digest(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).into()
}

/// What a report says of where it stopped.
fn seen(newer: Option<Newer>) -> Option<(u64, Option<DeviceId>, Unknown, [u8; 32])> {
    newer.map(|n| (n.seq, n.device, n.unknown, n.digest))
}

async fn pair(server: &Server) -> [Peer; 2] {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    [Peer::new(made).await, Peer::new(joined).await]
}

/// The two changes that write the setting `name`.
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

fn nonce() -> [u8; 24] {
    random_nonce().unwrap()
}

#[test]
fn the_test_writer_writes_what_seal_writes_in_the_current_formats() {
    let keys = LibraryKeys::new(LibraryId([1; 16]), [2; 32]);
    let device = SigningKey::from_bytes(&[3; 32]);
    let changes = setting("theme");
    for epoch in [0, 1, 7] {
        let plaintext = encode_batch(&changes);
        assert_eq!(
            seal_as(ENVELOPE_FORMAT, &plaintext, &keys, epoch, &device, [4; 24]),
            seal(&changes, &keys, epoch, &device, [4; 24]),
        );
    }
    assert_eq!(batch_as(BATCH_FORMAT, 0, &changes), encode_batch(&changes));
    let first = &changes[0];
    let Value::Text(text) = &first.value else {
        unreachable!()
    };
    let mut payload = vec![text.len() as u8];
    payload.extend_from_slice(text.as_bytes());
    assert_eq!(
        batch_with_value(&changes, 3, &payload),
        encode_batch(&changes)
    );
    let envelope = Envelope::decode(&seal(&changes, &keys, 0, &device, [4; 24])).unwrap();
    assert_eq!(re_signed(&envelope, &device), envelope.encode());
}

/// An envelope a later version might write, carrying `changes`, signed by `device`.
struct Later {
    unknown: Unknown,
    /// Whether the report can name the device.
    named: bool,
    write: fn(&[Change], &LibraryKeys, &SigningKey) -> Vec<u8>,
}

const LATER_ENVELOPE_FORMAT: Later = Later {
    unknown: Unknown::EnvelopeFormat(ENVELOPE_FORMAT + 1),
    named: false,
    write: |changes, keys, device| {
        let plaintext = encode_batch(changes);
        seal_as(ENVELOPE_FORMAT + 1, &plaintext, keys, 0, device, nonce())
    },
};

const LATER_KEY: Later = Later {
    unknown: Unknown::Epoch(1),
    named: true,
    write: |changes, keys, device| {
        let rotated = LibraryKeys::new(keys.id, [0x77; 32]);
        seal(changes, &rotated, 1, device, nonce())
    },
};

const LATER_BATCH_FORMAT: Later = Later {
    unknown: Unknown::BatchFormat(BATCH_FORMAT + 1),
    named: true,
    write: |changes, keys, device| {
        let plaintext = batch_as(BATCH_FORMAT + 1, 0, changes);
        seal_as(ENVELOPE_FORMAT, &plaintext, keys, 0, device, nonce())
    },
};

const BATCH_FLAGS: Later = Later {
    unknown: Unknown::BatchFlags(1),
    named: true,
    write: |changes, keys, device| {
        let plaintext = batch_as(BATCH_FORMAT, 1, changes);
        seal_as(ENVELOPE_FORMAT, &plaintext, keys, 0, device, nonce())
    },
};

const VALUE_TAG: Later = Later {
    unknown: Unknown::ValueTag(6),
    named: true,
    write: |changes, keys, device| {
        let plaintext = batch_with_value(changes, 6, &[0, 0, 0, 1]);
        seal_as(ENVELOPE_FORMAT, &plaintext, keys, 0, device, nonce())
    },
};

/// The setting a device of a later version writes, in an envelope this build cannot read, at
/// sequence 6, on the log's second page; then a device of this version writes after it, as does
/// the device under test, which stops there, sends its own writes, stops there again on every
/// sync, and reads on once it can read the envelope.
async fn stops_at_and_reads_on_from(later: Later) {
    let server = Server::new();
    assert_eq!(server.with(|s| s.caps.max_page_batches), Some(4));
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    let added = Credentials::for_new_device(keys.clone()).unwrap();
    let writer = added.device().clone();
    add_device(&server, &b.credentials, &added.device_offer().unwrap(), now)
        .await
        .unwrap();
    for n in 1..=5 {
        b.run(&format!("INSERT INTO settings VALUES ('s{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    let unreadable = (later.write)(&setting("later"), &keys, &writer);
    server.inject(id, unreadable.clone());
    let newer = Some((
        6,
        later.named.then(|| DeviceId::of(&writer.verifying_key())),
        later.unknown,
        digest(&unreadable),
    ));

    // A device of this version that also writes after it stops there too, and still sends.
    b.run("INSERT INTO settings VALUES ('after', 'x')").await;
    let report = b.sync(&server).await.unwrap();
    assert_eq!(seen(report.newer), newer);
    assert!(report.sent > 0);

    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(seen(report.newer), newer, "{report:#?}");
    assert_eq!(report.blocked, None);
    assert!(report.refused.is_empty(), "{report:#?}");
    assert!(!report.more);
    assert_eq!(
        report.head, 8,
        "its own envelope landed after the one it stopped at"
    );
    assert_eq!(
        a.settings().await,
        ["mine=x", "s1=x", "s2=x", "s3=x", "s4=x", "s5=x"]
    );
    assert_eq!(a.cursor().await, 5);
    assert!(report.sent > 0, "{report:#?}");
    assert!(
        server
            .changes(&keys)
            .iter()
            .any(|c| c.value == Value::Text("mine".into())),
        "this device's own write went up"
    );

    // Each later sync of this build reads the envelope at its cursor and the one it stopped at,
    // stops there again, and with nothing of its own to send appends nothing.
    let page = ReadBatches {
        after: 4,
        limit: Some(2),
    }
    .target(id);
    let appends = server.count(Method::Post, &batches_path(id));
    for _ in 0..3 {
        let before = server.with(|s| s.requests.len());
        let report = a.sync(&server).await.unwrap();
        assert_eq!(seen(report.newer), newer);
        assert_eq!((report.received, report.sent), (0, 0), "{report:#?}");
        assert_eq!(a.cursor().await, 5);
        let pages: Vec<String> = server.with(|s| {
            s.requests[before..]
                .iter()
                .filter(|(m, t)| *m == Method::Get && t.starts_with(&batches_path(id)))
                .map(|(_, t)| t.clone())
                .collect()
        });
        assert_eq!(pages, std::slice::from_ref(&page));
        assert_eq!(server.count(Method::Post, &batches_path(id)), appends);
    }

    // What it writes meanwhile still goes up.
    a.run("INSERT INTO settings VALUES ('meanwhile', 'x')")
        .await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(seen(report.newer), newer);
    assert!(report.sent > 0);
    assert_eq!(a.cursor().await, 5);

    // A build that reads it continues from there and loses nothing: here the server serves, at
    // the same place, an envelope of the current formats carrying the same changes.
    let readable = seal(&setting("later"), &keys, 0, &writer, nonce());
    server.replace(id, 6, readable);
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer, None, "{report:#?}");
    assert!(report.refused.is_empty(), "{report:#?}");
    assert_eq!(
        a.settings().await,
        [
            "after=x",
            "later=later",
            "meanwhile=x",
            "mine=x",
            "s1=x",
            "s2=x",
            "s3=x",
            "s4=x",
            "s5=x"
        ]
    );
    assert_eq!(a.cursor().await, server.log(id).len() as u64);
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
    assert_no_change_sent_twice(&server, &keys);
}

#[tokio::test]
async fn an_envelope_of_a_later_format_stops_the_pull_until_it_can_be_read() {
    stops_at_and_reads_on_from(LATER_ENVELOPE_FORMAT).await;
}

#[tokio::test]
async fn an_envelope_under_a_later_key_epoch_this_device_lacks_stops_the_pull() {
    stops_at_and_reads_on_from(LATER_KEY).await;
}

#[tokio::test]
async fn a_batch_of_a_later_format_stops_the_pull() {
    stops_at_and_reads_on_from(LATER_BATCH_FORMAT).await;
}

#[tokio::test]
async fn a_batch_with_flags_this_build_lacks_stops_the_pull() {
    stops_at_and_reads_on_from(BATCH_FLAGS).await;
}

#[tokio::test]
async fn a_value_of_a_type_this_build_lacks_stops_the_pull() {
    stops_at_and_reads_on_from(VALUE_TAG).await;
}

#[tokio::test]
async fn a_later_epoch_under_the_same_secret_is_read() {
    let server = Server::new();
    let [mut a, b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    server.inject(
        id,
        seal(&setting("later"), &keys, 9, b.credentials.device(), nonce()),
    );
    let report = a.sync(&server).await.unwrap();
    assert_eq!((report.newer, report.refused_total), (None, 0));
    assert_eq!(a.settings().await, ["later=later"]);
    assert_eq!(a.cursor().await, 1);
}

/// Counts the calls to [`Apply::caught_up`].
#[derive(Default)]
struct CaughtUp(usize);

impl Apply for CaughtUp {
    async fn caught_up(
        &mut self,
        _: &mut SqliteConnection,
        _: &Replica,
    ) -> Result<Option<Applied>, Error> {
        self.0 += 1;
        Ok(None)
    }
}

#[tokio::test]
async fn a_pull_stopped_at_a_later_envelope_has_not_caught_up() {
    let server = Server::new();
    let [mut a, b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    server.inject(
        keys.id,
        (LATER_BATCH_FORMAT.write)(&setting("later"), &keys, b.credentials.device()),
    );
    let mut counted = CaughtUp::default();
    let report = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut counted,
    )
    .await
    .unwrap();
    assert_eq!(report.newer.map(|n| n.seq), Some(1));
    assert_eq!(counted.0, 0);

    server.replace(
        keys.id,
        1,
        seal(&setting("later"), &keys, 0, b.credentials.device(), nonce()),
    );
    sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut counted,
    )
    .await
    .unwrap();
    assert_eq!(counted.0, 1);
    assert_eq!(a.settings().await, ["later=later"]);
}

/// Sets the first byte of the envelope at `seq` on every page.
fn mark(seq: u64, byte: u8) -> impl FnMut(BatchPage) -> Vec<u8> + Send {
    move |mut page| {
        for batch in &mut page.batches {
            if batch.seq == seq {
                batch.envelope[0] = byte;
            }
        }
        page.encode()
    }
}

#[tokio::test]
async fn a_server_that_passes_an_envelope_off_as_later_stops_a_device_only_visibly() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for n in 1..=2 {
        b.run(&format!("INSERT INTO settings VALUES ('s{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    server.tamper(mark(1, 9));
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    for _ in 0..2 {
        let report = a.sync(&server).await.unwrap();
        let marked = report.newer.unwrap();
        assert_eq!(
            (marked.seq, marked.device, marked.unknown),
            (1, None, Unknown::EnvelopeFormat(9))
        );
        assert_eq!(a.settings().await, ["mine=x"]);
        assert_eq!(a.cursor().await, 0);
    }
    assert_eq!(server.log(id).len(), 3, "the device's write went up");

    server.behave();
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer, None);
    b.sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut [a, b]).await.len(), 3);
}

#[tokio::test]
async fn an_envelope_a_server_alters_is_skipped_and_counted_and_its_changes_are_not_read_again() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    for n in 1..=2 {
        b.run(&format!("INSERT INTO settings VALUES ('s{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    server.tamper(|mut page| {
        for batch in &mut page.batches {
            if batch.seq == 1 {
                let last = batch.envelope.len() - 1;
                batch.envelope[last] ^= 1;
            }
        }
        page.encode()
    });
    let report = a.sync(&server).await.unwrap();
    let refused: Vec<_> = report
        .refused
        .iter()
        .map(|r| (r.seq, r.device, r.reason))
        .collect();
    assert_eq!(
        refused,
        [(1, Some(b.credentials.device_id()), Refusal::BadSignature)]
    );
    assert_eq!(report.newer, None);
    assert_eq!(a.cursor().await, 2);
    assert_eq!(a.settings().await, ["s2=x"]);

    server.behave();
    a.sync(&server).await.unwrap();
    assert_eq!(a.settings().await, ["s2=x"]);
}

#[tokio::test]
async fn damaged_or_forged_envelopes_are_skipped_and_counted_and_never_stop_the_pull() {
    let server = Server::new();
    let [made, mut b] = pair(&server).await;
    // This device seals under epoch 2, so epochs 1 and 2 are ones it has keys for.
    let mut a = Peer::new(made.credentials.clone().with_epoch(2)).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    let writer = generate_signing_key().unwrap();
    server.register(id, writer.verifying_key());
    let changes = setting("damaged");
    let good = seal(&changes, &keys, 0, &writer, nonce());
    let wrong_secret = LibraryKeys::new(id, [0; 32]);
    let in_envelope =
        |plaintext: &[u8]| seal_as(ENVELOPE_FORMAT, plaintext, &keys, 0, &writer, nonce());

    let mut damaged: Vec<(&str, Vec<u8>, bool, Refusal)> = Vec::new();
    let mut format_zero = good.clone();
    format_zero[0] = 0;
    damaged.push(("envelope format 0", format_zero, false, Refusal::Malformed));
    damaged.push((
        "truncated envelope",
        good[..good.len() - 1].to_vec(),
        false,
        Refusal::Malformed,
    ));
    let mut longer = good.clone();
    longer.push(0);
    damaged.push(("trailing byte", longer, false, Refusal::Malformed));
    let mut flipped = good.clone();
    flipped[100] ^= 1;
    damaged.push(("altered ciphertext", flipped, true, Refusal::BadSignature));
    let mut envelope = Envelope::decode(&good).unwrap();
    envelope.ciphertext[0] ^= 1;
    damaged.push((
        "ciphertext altered and signed again",
        re_signed(&envelope, &writer),
        true,
        Refusal::Undecryptable,
    ));
    for epoch in [0, 1, 2] {
        damaged.push((
            "another secret at a known epoch",
            seal(&changes, &wrong_secret, epoch, &writer, nonce()),
            true,
            Refusal::Undecryptable,
        ));
    }
    damaged.push((
        "batch format 0",
        in_envelope(&batch_as(0, 0, &changes)),
        true,
        Refusal::Malformed,
    ));
    let batch = encode_batch(&changes);
    damaged.push((
        "truncated batch",
        in_envelope(&batch[..batch.len() - 1]),
        true,
        Refusal::Malformed,
    ));
    let mut longer = batch.clone();
    longer.push(0);
    damaged.push((
        "batch with a trailing byte",
        in_envelope(&longer),
        true,
        Refusal::Malformed,
    ));
    let mut not_utf8 = batch.clone();
    // Format, flags, a count of 2 and the collection name's length; then the name.
    not_utf8[4] = 0xff;
    damaged.push((
        "text not UTF-8",
        in_envelope(&not_utf8),
        true,
        Refusal::Malformed,
    ));
    let elsewhere = LibraryKeys::new(LibraryId([7; 16]), *keys.secret());
    damaged.push((
        "another library",
        seal(&changes, &elsewhere, 0, &writer, nonce()),
        true,
        Refusal::WrongLibrary,
    ));
    damaged.push((
        "the recovery key",
        seal(&changes, &keys, 0, &keys.recovery_signing_key(), nonce()),
        false,
        Refusal::RecoveryKey,
    ));
    for (_, bytes, _, _) in &damaged {
        server.inject(id, bytes.clone());
    }
    b.run("INSERT INTO settings VALUES ('real', 'x')").await;
    b.sync(&server).await.unwrap();

    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer, None, "{report:#?}");
    assert_eq!(report.blocked, None);
    assert_eq!(report.refused_total, damaged.len());
    for (n, ((what, _, named, reason), refused)) in damaged.iter().zip(&report.refused).enumerate()
    {
        assert_eq!(refused.seq, n as u64 + 1, "{what}");
        assert_eq!(refused.reason, *reason, "{what}");
        let device = match (named, *reason) {
            (false, Refusal::RecoveryKey) => {
                Some(DeviceId::of(&keys.recovery_signing_key().verifying_key()))
            }
            (false, _) => None,
            (true, _) => Some(DeviceId::of(&writer.verifying_key())),
        };
        assert_eq!(refused.device, device, "{what}");
    }
    assert_eq!(a.settings().await, ["real=x"]);
    assert_eq!(a.cursor().await, damaged.len() as u64 + 1);
}

/// Loses the answer to the next append, once.
fn lose_next_append(server: &Server) {
    let mut once = true;
    server.lose_if(move |r| r.method == Method::Post && std::mem::take(&mut once));
}

/// No envelope sits in the log twice.
fn assert_no_envelope_twice(server: &Server, id: LibraryId) {
    let log = server.log(id);
    let distinct: std::collections::HashSet<&Vec<u8>> = log.iter().collect();
    assert_eq!(distinct.len(), log.len(), "an envelope was appended twice");
}

/// A device whose append answer was lost while its pull stops before the envelope it appended,
/// then another device that reads that edit and edits after it, on a server that stores an
/// identical envelope again. `stop` puts what the first device stops at into the log, given the
/// second device's signing key, and `heal` lets it read on.
async fn a_lost_answer_while_stopped(
    server: Server,
    b_credentials: impl FnOnce(Credentials) -> Credentials,
    stop: impl FnOnce(&Server, &LibraryKeys, &SigningKey),
    heal: impl FnOnce(&Server, &LibraryKeys),
) {
    server.with(|s| s.store_again = true);
    let [mut a, made] = pair(&server).await;
    let mut b = Peer::new(b_credentials(made.credentials.clone())).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    stop(&server, &keys, b.credentials.device());
    let report = a.sync(&server).await.unwrap();
    assert!(report.newer.is_some() || report.blocked.is_some());

    a.run("UPDATE notes SET body = 'A1'").await;
    lose_next_append(&server);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    b.sync(&server).await.unwrap();
    assert_eq!(b.lines("SELECT body FROM notes").await, ["A1"]);
    b.run("UPDATE notes SET body = 'B2'").await;
    b.sync(&server).await.unwrap();

    let report = a.sync(&server).await.unwrap();
    assert!(report.newer.is_some() || report.blocked.is_some());
    assert_no_envelope_twice(&server, id);

    heal(&server, &keys);
    let report = a.sync(&server).await.unwrap();
    assert_eq!((report.newer, report.blocked), (None, None));
    assert_eq!(report.applied.conflicts, [], "{report:#?}");
    assert_no_envelope_twice(&server, id);
    b.sync(&server).await.unwrap();
    assert_eq!(a.lines("SELECT body FROM notes").await, ["B2"]);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn an_append_whose_answer_was_lost_while_stopped_is_found_past_the_stop() {
    let writer = generate_signing_key().unwrap();
    let listed = writer.verifying_key();
    let rotated = writer.clone();
    a_lost_answer_while_stopped(
        Server::new(),
        // The second device seals under epoch 1, so it skips what the first stops at.
        |credentials| credentials.with_epoch(1),
        move |server, keys, _| {
            server.register(keys.id, listed);
            let other = LibraryKeys::new(keys.id, [0x77; 32]);
            server.inject(keys.id, seal(&[], &other, 1, &rotated, nonce()));
        },
        move |server, keys| server.replace(keys.id, 2, seal(&[], keys, 0, &writer, nonce())),
    )
    .await;
}

#[tokio::test]
async fn an_append_whose_answer_was_lost_while_blocked_is_found_past_the_block() {
    let hidden = std::sync::Arc::new(std::sync::Mutex::new(None));
    let unhide = hidden.clone();
    a_lost_answer_while_stopped(
        Server::new(),
        |credentials| credentials,
        // The second device signs what blocks the first, and is left out of every list.
        move |server, keys, b| {
            server.inject(keys.id, seal(&[], keys, 0, b, nonce()));
            server.with(|s| s.hidden.push(b.verifying_key()));
            *hidden.lock().unwrap() = Some(b.verifying_key());
        },
        move |server, _| {
            let key = unhide.lock().unwrap().take().unwrap();
            server.with(|s| s.hidden.retain(|k| *k != key));
        },
    )
    .await;
}

/// Forwards to the server, but answers an append by storing, after the envelope, `between` and
/// then the envelope again, and naming that later copy.
struct AnswersWithALaterCopy<'a> {
    server: &'a Server,
    id: LibraryId,
    between: Vec<u8>,
}

impl Transport for AnswersWithALaterCopy<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        let append = request.method == Method::Post;
        let body = request.body.clone();
        let response = self.server.send(request).await?;
        if !append {
            return Ok(response);
        }
        self.server.inject(self.id, self.between.clone());
        self.server.inject(self.id, body);
        let seq = self.server.log(self.id).len() as u64;
        Ok(Response {
            body: serde_json::to_vec(&Appended { seq }).unwrap(),
            ..response
        })
    }
}

#[tokio::test]
async fn this_devices_envelope_is_read_back_at_the_first_copy_whichever_it_recorded() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    let note = a
        .replica
        .gid_of(&mut a.conn, "notes", 1)
        .await
        .unwrap()
        .unwrap();
    // A change that lands after this device's edit, and so may have been made knowing it.
    let after = Change {
        collection: "notes".into(),
        gid: note,
        field: "body".into(),
        value: Value::Text("B2".into()),
        clock: Hlc {
            wall_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 3_600_000,
            counter: 0,
            node: Gid([9; 16]),
        },
    };
    let later_copy = AnswersWithALaterCopy {
        server: &server,
        id: keys.id,
        between: seal(&[after], &keys, 0, b.credentials.device(), nonce()),
    };
    a.run("UPDATE notes SET body = 'A1'").await;
    fongo_sync_client::sync(&mut a.conn, &a.replica, &later_copy, &a.credentials, now)
        .await
        .unwrap();

    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.applied.conflicts, [], "{report:#?}");
    assert_eq!(a.lines("SELECT body FROM notes").await, ["B2"]);
}

#[tokio::test]
async fn a_stopped_sync_reads_the_envelope_at_its_cursor_and_one_more() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(256));
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    let writer = generate_signing_key().unwrap();
    server.register(id, writer.verifying_key());
    b.run("INSERT INTO settings VALUES ('s1', 'x')").await;
    b.sync(&server).await.unwrap();
    let junk = (LATER_ENVELOPE_FORMAT.write)(&setting("later"), &keys, &writer);
    server.inject(id, junk.clone());
    for n in 0..300 {
        let mut changes = setting(&format!("big{n}"));
        changes[1].value = Value::Text("x".repeat(4_000));
        server.inject(
            id,
            seal(&changes, &keys, 0, b.credentials.device(), nonce()),
        );
    }
    let pages = batches_path(id);

    let from = server.with(|s| s.requests.len());
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer.map(|n| n.seq), Some(2));
    let first = server.bytes_since(from, &pages);

    for _ in 0..3 {
        let from = server.with(|s| s.requests.len());
        let report = a.sync(&server).await.unwrap();
        assert_eq!(report.newer.map(|n| n.seq), Some(2));
        let requests: Vec<(Method, String)> = server.with(|s| s.requests[from..].to_vec());
        let read = ReadBatches {
            after: 0,
            limit: Some(2),
        }
        .target(id);
        let reads: Vec<&String> = requests
            .iter()
            .filter(|(m, t)| *m == Method::Get && t.starts_with(&pages))
            .map(|(_, t)| t)
            .collect();
        let bytes = server.bytes_since(from, &pages);
        assert_eq!(
            reads,
            [&read],
            "first stopped sync: {first} bytes; {requests:?}"
        );
        let anchor = server.log(id)[0].len();
        assert!(
            bytes <= anchor + junk.len() + 32,
            "first stopped sync: {first} bytes; this one: {bytes}"
        );
        assert_eq!(requests.len(), 2, "{requests:?}");
    }
}

/// Two devices of this version, and at sequence 2, after a setting from the second, bytes whose
/// first byte names a later envelope format: junk no build reads.
async fn stopped_at_junk(server: &Server) -> ([Peer; 2], Vec<u8>) {
    let [mut a, mut b] = pair(server).await;
    let id = a.credentials.library_id();
    b.run("INSERT INTO settings VALUES ('s1', 'x')").await;
    b.sync(server).await.unwrap();
    let junk = b"\x02 damaged in storage".to_vec();
    server.inject(id, junk.clone());
    b.run("INSERT INTO settings VALUES ('s3', 'x')").await;
    b.sync(server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let report = a.sync(server).await.unwrap();
    assert_eq!(
        seen(report.newer),
        Some((2, None, Unknown::EnvelopeFormat(2), digest(&junk)))
    );
    ([a, b], junk)
}

async fn step_past(peer: &mut Peer, seq: u64, digest: [u8; 32]) -> Result<(), Error> {
    let id = peer.credentials.library_id();
    fongo_sync_client::step_past(&mut peer.conn, id, seq, digest).await
}

#[tokio::test]
async fn stepping_past_junk_gives_up_only_that_envelope_and_reads_on() {
    let server = Server::new();
    let ([mut a, mut b], junk) = stopped_at_junk(&server).await;
    // Until stepped past, it is read again each time.
    let again = a.sync(&server).await.unwrap();
    assert_eq!(again.newer.map(|n| n.seq), Some(2));

    for peer in [&mut a, &mut b] {
        step_past(peer, 2, digest(&junk)).await.unwrap();
        let report = peer.sync(&server).await.unwrap();
        assert_eq!(report.newer, None, "{report:#?}");
        let refused: Vec<_> = report
            .refused
            .iter()
            .map(|r| (r.seq, r.device, r.reason))
            .collect();
        assert_eq!(refused, [(2, None, Refusal::SteppedPast)]);
    }
    a.sync(&server).await.unwrap();
    assert_eq!(a.settings().await, ["mine=x", "s1=x", "s3=x"]);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn stepping_past_other_bytes_or_another_place_is_refused_and_changes_nothing() {
    let server = Server::new();
    let ([mut a, _b], junk) = stopped_at_junk(&server).await;
    for (seq, named) in [(2, [0; 32]), (3, digest(&junk)), (1, digest(&junk))] {
        let refused = step_past(&mut a, seq, named).await;
        assert!(
            matches!(refused, Err(Error::NotStoppedAt { seq: s }) if s == seq),
            "{refused:?}"
        );
    }
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer.map(|n| n.seq), Some(2));
    assert_eq!(a.cursor().await, 1);
}

/// Other bytes put where a pull stopped, against the rule that bytes once served never change,
/// reach a device that has not passed that position as any envelope: read, applied, and not
/// stepped past, as only the bytes named are. One that has passed it finds a rewrite instead, as
/// `rollback.rs` shows.
#[tokio::test]
async fn bytes_changed_where_a_pull_stopped_are_read_and_not_stepped_past() {
    let server = Server::new();
    let ([mut a, b], junk) = stopped_at_junk(&server).await;
    let id = a.credentials.library_id();
    let keys = a.credentials.library().clone();
    server.replace(
        id,
        2,
        seal(
            &setting("replaced"),
            &keys,
            0,
            b.credentials.device(),
            nonce(),
        ),
    );
    step_past(&mut a, 2, digest(&junk)).await.unwrap();
    let report = a.sync(&server).await.unwrap();
    assert_eq!((report.newer, report.refused_total), (None, 0));
    assert_eq!(
        a.settings().await,
        ["mine=x", "replaced=replaced", "s1=x", "s3=x"]
    );
}

/// Other bytes put where a device has stepped past, while that is its cursor: a rewrite. Behind
/// the cursor they would go unseen, as any rewrite there.
#[tokio::test]
async fn bytes_changed_where_a_device_stepped_past_are_a_rewrite() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    let keys = a.credentials.library().clone();
    b.run("INSERT INTO settings VALUES ('s1', 'x')").await;
    b.sync(&server).await.unwrap();
    let junk = b"\x02 damaged in storage".to_vec();
    server.inject(id, junk.clone());
    assert_eq!(a.sync(&server).await.unwrap().newer.map(|n| n.seq), Some(2));
    step_past(&mut a, 2, digest(&junk)).await.unwrap();
    a.sync(&server).await.unwrap();
    assert_eq!(a.cursor().await, 2);
    server.replace(
        id,
        2,
        seal(
            &setting("replaced"),
            &keys,
            0,
            b.credentials.device(),
            nonce(),
        ),
    );
    let found = a.sync(&server).await;
    assert!(
        matches!(
            found,
            Err(Error::LogFault(fongo_sync_client::LogFault::Rewritten {
                seq: 2
            }))
        ),
        "{found:?}"
    );
}

#[tokio::test]
async fn an_envelope_stepped_past_stays_skipped_after_a_build_that_reads_it() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let later = (LATER_ENVELOPE_FORMAT.write)(&setting("later"), &keys, b.credentials.device());
    server.inject(keys.id, later.clone());
    a.sync(&server).await.unwrap();
    step_past(&mut a, 1, digest(&later)).await.unwrap();
    a.sync(&server).await.unwrap();
    assert_eq!(a.cursor().await, 1);
    // Other bytes at the cursor itself are a rewritten log, so the cursor moves on first.
    b.run("INSERT INTO settings VALUES ('b', 'x')").await;
    b.sync(&server).await.unwrap();
    a.sync(&server).await.unwrap();
    assert_eq!(a.cursor().await, 2);

    server.replace(
        keys.id,
        1,
        seal(&setting("later"), &keys, 0, b.credentials.device(), nonce()),
    );
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.received, 0);
    assert_eq!(a.settings().await, ["b=x"]);
}

#[tokio::test]
async fn an_append_that_failed_while_stopped_costs_one_short_page_to_look_for() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(256));
    let [mut a, b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    let junk = b"\x02 damaged in storage".to_vec();
    server.inject(id, junk);
    for n in 0..1_000 {
        let mut changes = setting(&format!("big{n}"));
        changes[1].value = Value::Text("x".repeat(4_000));
        server.inject(
            id,
            seal(&changes, &keys, 0, b.credentials.device(), nonce()),
        );
    }
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let mut once = true;
    server.fail_if(move |r| r.method == Method::Post && std::mem::take(&mut once));
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));

    let from = server.with(|s| s.requests.len());
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer.map(|n| n.seq), Some(1));
    assert!(report.sent > 0);
    let requests: Vec<(Method, String)> = server.with(|s| s.requests[from..].to_vec());
    let bytes = server.bytes_since(from, &batches_path(id));
    let reads = requests.iter().filter(|(m, _)| *m == Method::Get).count();
    assert!(
        reads <= 3 && bytes < 100_000,
        "{bytes} bytes in {} requests: {requests:?}",
        requests.len()
    );
    assert_no_envelope_twice(&server, id);
}

#[tokio::test]
async fn a_step_past_an_envelope_this_device_has_since_learned_to_read_reads_it() {
    let server = Server::new();
    let [mut a, b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let id = keys.id;
    let later = (LATER_KEY.write)(&setting("later"), &keys, b.credentials.device());
    server.inject(id, later.clone());
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer.map(|n| n.unknown), Some(Unknown::Epoch(1)));
    step_past(&mut a, 1, digest(&later)).await.unwrap();

    // The device learns the key of the later epoch before its next sync.
    let rotated = LibraryKeys::new(id, [0x77; 32]);
    a.credentials = Credentials::new(rotated, a.credentials.device().clone(), 1);
    let report = a.sync(&server).await.unwrap();
    assert_eq!(
        (report.newer, report.refused_total),
        (None, 0),
        "{report:#?}"
    );
    assert_eq!(a.settings().await, ["later=later"]);
}
