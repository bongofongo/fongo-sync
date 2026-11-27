//! Envelopes this build cannot read: those a later version of the protocol may have written stop
//! the pull where they are, and those that are damaged or forged are skipped and counted.

#[allow(dead_code)]
mod support;

use fongo_sync_client::protocol::api::{BatchPage, ReadBatches, batches_path};
use fongo_sync_client::protocol::{
    BATCH_FORMAT, DeviceId, ENVELOPE_FORMAT, Envelope, LibraryId, LibraryKeys, SigningKey,
    encode_batch, generate_signing_key, random_nonce, seal,
};
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Error, Method, Newer, Refusal, Unknown, add_device, create_library, join_library,
    sync_with,
};
use sqlx::SqliteConnection;
use support::{
    Peer, Server, assert_converged, assert_no_change_sent_twice, batch_as, batch_with_value, now,
    re_signed, seal_as,
};

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
    let writer = generate_signing_key().unwrap();
    add_device(&server, &b.credentials, &writer.verifying_key(), now)
        .await
        .unwrap();
    for n in 1..=5 {
        b.run(&format!("INSERT INTO settings VALUES ('s{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    server.inject(id, (later.write)(&setting("later"), &keys, &writer));
    let newer = Newer {
        seq: 6,
        device: later.named.then(|| DeviceId::of(&writer.verifying_key())),
        unknown: later.unknown,
    };

    // A device of this version that also writes after it stops there too, and still sends.
    b.run("INSERT INTO settings VALUES ('after', 'x')").await;
    let report = b.sync(&server).await.unwrap();
    assert_eq!(report.newer, Some(newer));
    assert!(report.sent > 0);

    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.newer, Some(newer), "{report:#?}");
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

    // Each later sync of this build reads one page from where it stopped, stops there again,
    // and with nothing of its own to send appends nothing.
    let page = ReadBatches {
        after: 5,
        limit: Some(4),
    }
    .target(id);
    let appends = server.count(Method::Post, &batches_path(id));
    for _ in 0..3 {
        let before = server.with(|s| s.requests.len());
        let report = a.sync(&server).await.unwrap();
        assert_eq!(report.newer, Some(newer));
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
    assert_eq!(report.newer, Some(newer));
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
        let newer = Newer {
            seq: 1,
            device: None,
            unknown: Unknown::EnvelopeFormat(9),
        };
        assert_eq!(report.newer, Some(newer));
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
