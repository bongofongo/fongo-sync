use std::collections::{HashMap, HashSet};

use fongo_sync_protocol::api::{
    Appended, BatchPage, Devices, ErrorCode, ReadBatches, StoredBatch, batches_path, devices_path,
};
use fongo_sync_protocol::{
    DeviceId, Envelope, EnvelopeError, VerifyingKey, encode_batch, open, random_nonce, seal,
};
use fongo_sync_sqlite::core::{ALIAS, Change, DELETED};
use fongo_sync_sqlite::{Applied, Replica};
use sqlx::{Connection, SqliteConnection};

use crate::credentials::Credentials;
use crate::error::Error;
use crate::rpc::{Body, Rpc, json};
use crate::state::{self, Sending};
use crate::transport::{Method, Transport};

/// Envelopes asked for per page when the server states no maximum.
const DEFAULT_PAGE: u32 = 256;
/// Received changes held in memory before they are applied and the cursor moves.
const APPLY_CHUNK: usize = 50_000;
/// Pages read in one call; [`SyncReport::more`] says when the log held more.
const MAX_PAGES: usize = 1_000;
/// Refusals listed in [`SyncReport::refused`]; [`SyncReport::refused_total`] counts them all.
const REFUSALS_LISTED: usize = 100;
/// The encoded size of a batch of one change with an empty collection and field and a null value.
const SMALLEST_BATCH: usize = 3 + 1 + 16 + 1 + 1 + 8 + 4 + 16;
/// Most changes sealed into one envelope.
const OUTBOX_PAGE: u32 = 1_000;

/// Why an envelope from the server's log was skipped instead of applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// Not an envelope, or what it decrypts to is not a batch.
    Malformed,
    /// It names another library.
    WrongLibrary,
    /// Signed by the library's recovery key, which may add devices but never writes.
    RecoveryKey,
    /// Signed by a key the library's device list does not hold. Not reported by this version: such
    /// an envelope may become valid once the list catches up, so it blocks the pull instead (see
    /// [`SyncReport::blocked`]).
    UnknownDevice,
    /// Its signature does not verify against the key of the device it names.
    BadSignature,
    /// It does not decrypt under the library's key, so whoever wrote it lacks the library secret.
    Undecryptable,
}

/// One envelope skipped and never applied. It does not stop the sync, and the cursor moves past
/// it, so it is not read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused {
    pub seq: u64,
    /// The device the envelope names, when it could be read.
    pub device: Option<DeviceId>,
    pub reason: Refusal,
}

/// An envelope the pull stopped at: its signer is not in the library's device list, even fetched
/// afresh. An honest server stores only envelopes from listed devices, so the list is behind or
/// the server is faulty; either way the envelope may become valid, so the cursor stays before it
/// and every later sync tries it again. Change notifications keep reporting a sync worth running
/// meanwhile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blocked {
    pub seq: u64,
    pub device: DeviceId,
}

/// What one [`sync`] did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SyncReport {
    /// Changes received from other devices and applied, whether or not they won.
    pub received: usize,
    /// Changes this device sent.
    pub sent: usize,
    /// What applying them did, merged over every apply in the sync: changes accepted, rows
    /// touched (each row once, as it was left), conflicts, and entities still waiting.
    pub applied: Applied,
    /// The first refusals, at most 100.
    pub refused: Vec<Refused>,
    /// Every refusal in this sync, listed or not.
    pub refused_total: usize,
    /// Set when the pull stopped at an envelope from an unlisted signer. What came before it was
    /// applied and the outbox was still sent.
    pub blocked: Option<Blocked>,
    /// The pull stopped after the most pages one call reads, with more waiting. Everything read
    /// was applied and the outbox was sent; call again to read on.
    pub more: bool,
    /// The last sequence number this sync knows the library's log to hold.
    pub head: u64,
}

/// One round with the server: reads the envelopes after the stored cursor, applies what other
/// devices wrote, then seals and appends this device's outbox.
///
/// `now` gives the current unix time in seconds; every request is stamped with it.
///
/// The cursor is stored after every page read. Received changes are held across pages, up to a
/// bound, and applied together with the cursor in one transaction, so a crash at any point
/// leaves either both or neither; re-reading changes already applied is harmless. Local writes
/// are collected by that apply and compared with every change held, so a concurrent remote edit
/// to a field that reports conflicts is reported in [`SyncReport::applied`]. Whatever is held is
/// also applied before this returns an error, so a failure part-way keeps the progress made.
///
/// Each envelope to send is recorded in the database before it is appended, and the outbox
/// acknowledged and the record cleared in one transaction once the server confirms. When a sync
/// stops between the two, whether or not the server stored the envelope, the next sync appends
/// the same recorded bytes again before anything new; a server that stores an identical envelope
/// once makes that harmless, and otherwise the duplicate is applied idempotently by every device.
/// Changes are not re-sealed into a second, different envelope, with one exception: when the
/// server refuses the recorded envelope itself, as too large or not one it accepts, the record is
/// dropped and its changes, still in the outbox because they were never acknowledged, are sealed
/// afresh under the server's current limits. Should the server in fact hold the earlier envelope,
/// its changes then arrive twice, which every device applies idempotently.
///
/// Envelopes this device signed are applied like any other: applying a change already held is a
/// no-op, and a database restored from a backup, or a second database sharing these credentials,
/// may not hold them. An append the server places right after the cursor moves the cursor past
/// it, so a device does not usually read back what it sent.
pub async fn sync<T: Transport>(
    conn: &mut SqliteConnection,
    replica: &Replica,
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<SyncReport, Error> {
    let rpc = Rpc {
        transport,
        now: &now,
    };
    let caps = rpc.capabilities().await?;
    let library = credentials.library_id();
    let me = credentials.device_id();
    let saved = state::load(conn, library).await?;
    let mut cursor = saved.cursor;
    let limit = caps.max_page_batches.unwrap_or(DEFAULT_PAGE).max(1);
    let mut keys = Keys::new(credentials);
    let mut report = SyncReport::default();

    let mut pull = Pull {
        read: cursor,
        held: Vec::new(),
        applied: false,
    };
    let pulled = read_pages(
        &rpc,
        conn,
        replica,
        &mut keys,
        limit,
        &mut pull,
        &mut report,
    )
    .await;
    let collect = !pull.applied;
    let committed = pull
        .commit(conn, replica, library, &mut report, collect)
        .await;
    pulled.and(committed)?;
    cursor = pull.read;
    report.head = cursor.max(report.blocked.map_or(0, |b| b.seq));

    if let Some(sending) = saved.sending {
        let ours = Envelope::decode(&sending.envelope)
            .is_ok_and(|envelope| envelope.library == library && envelope.device == me);
        if ours {
            let count = open(&sending.envelope, credentials.library(), |_| {
                Some(credentials.device().verifying_key())
            })
            .map_or(0, |opened| opened.changes.len());
            match append(&rpc, credentials, sending.envelope).await {
                Ok(seq) => {
                    acknowledge(conn, replica, library, sending.through, seq, &mut cursor).await?;
                    report.sent += count;
                    report.head = report.head.max(seq);
                }
                Err(Error::Server { code, .. }) if refuses_the_envelope(code) => {
                    state::clear_sending(conn, library).await?;
                }
                Err(error) => return Err(error),
            }
        } else {
            state::clear_sending(conn, library).await?;
        }
    }

    let max = caps.max_batch_bytes;
    loop {
        let full = replica.outbox(conn, OUTBOX_PAGE).await?;
        let Some(first) = full.changes.first() else {
            break;
        };
        let mut n = fitting(&full.changes, max);
        let (outbox, envelope) = loop {
            if n == 0 {
                let max = max.unwrap_or(u64::MAX);
                let internal = first.field == DELETED || first.field == ALIAS;
                if internal || max < envelope_len(SMALLEST_BATCH) as u64 {
                    return Err(Error::MaxBatchTooSmall { max });
                }
                return Err(Error::ChangeTooLarge {
                    collection: first.collection.clone(),
                    field: first.field.clone(),
                    bytes: envelope_len(encode_batch(std::slice::from_ref(first)).len()),
                    max,
                });
            }
            let outbox = if n < full.changes.len() {
                replica.outbox(conn, n as u32).await?
            } else {
                full.clone()
            };
            let nonce = random_nonce().map_err(|_| Error::Randomness)?;
            let envelope = seal(
                &outbox.changes,
                credentials.library(),
                credentials.epoch(),
                credentials.device(),
                nonce,
            );
            if max.is_none_or(|max| envelope.len() as u64 <= max) {
                break (outbox, envelope);
            }
            n -= 1;
        };
        let sending = Sending {
            through: outbox.through,
            envelope,
        };
        state::record_sending(conn, library, &sending).await?;
        let seq = append(&rpc, credentials, sending.envelope).await?;
        acknowledge(conn, replica, library, sending.through, seq, &mut cursor).await?;
        report.sent += outbox.changes.len();
        report.head = report.head.max(seq);
    }
    Ok(report)
}

/// Where reading the log stands: envelopes through `read` are dealt with, and `held` has the
/// changes among them not yet applied.
struct Pull {
    read: u64,
    held: Vec<Change>,
    applied: bool,
}

impl Pull {
    /// Applies what is held and stores the cursor at `read`, in one transaction. With `collect`
    /// it applies even when nothing is held, so local writes are collected.
    async fn commit(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        library: fongo_sync_protocol::LibraryId,
        report: &mut SyncReport,
        collect: bool,
    ) -> Result<(), Error> {
        let mut tx = conn.begin().await?;
        let applied = if collect || !self.held.is_empty() {
            Some(replica.apply(&mut tx, &self.held).await?)
        } else {
            None
        };
        state::advance(&mut tx, library, self.read).await?;
        tx.commit().await?;
        if let Some(applied) = applied {
            self.applied = true;
            report.received += self.held.len();
            merge(&mut report.applied, applied);
        }
        self.held.clear();
        Ok(())
    }
}

/// Reads pages after `pull.read` until the log ends, an unlisted signer blocks, or the page limit.
/// Stores the cursor after each page unless changes are held to be applied together.
async fn read_pages<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    conn: &mut SqliteConnection,
    replica: &Replica,
    keys: &mut Keys<'_>,
    limit: u32,
    pull: &mut Pull,
    report: &mut SyncReport,
) -> Result<(), Error> {
    let library = keys.credentials.library_id();
    for _ in 0..MAX_PAGES {
        let query = ReadBatches {
            after: pull.read,
            limit: Some(limit),
        };
        let body = rpc
            .call(
                Method::Get,
                query.target(library),
                Body::Empty,
                Some(keys.credentials.device()),
            )
            .await?;
        let page = BatchPage::decode(&body)
            .map_err(|_| Error::MalformedResponse("a batch page does not decode"))?;
        check_page(&page, pull.read, limit)?;
        for batch in &page.batches {
            match keys.open(rpc, batch).await? {
                Opened::Changes(changes) => pull.held.extend(changes),
                Opened::Refused(refused) => {
                    report.refused_total += 1;
                    if report.refused.len() < REFUSALS_LISTED {
                        report.refused.push(refused);
                    }
                }
                Opened::Unlisted(device) => {
                    report.blocked = Some(Blocked {
                        seq: batch.seq,
                        device,
                    });
                    return Ok(());
                }
            }
            pull.read = batch.seq;
        }
        if !page.more {
            return Ok(());
        }
        if pull.held.is_empty() || pull.held.len() >= APPLY_CHUNK {
            pull.commit(conn, replica, library, report, false).await?;
        }
    }
    report.more = true;
    Ok(())
}

/// Refuses a page that does not continue the log from `after` one sequence number at a time, or
/// that could keep a reader asking forever.
fn check_page(page: &BatchPage, after: u64, limit: u32) -> Result<(), Error> {
    if page.batches.len() > limit as usize {
        return Err(Error::MalformedResponse(
            "a page holds more envelopes than were asked for",
        ));
    }
    if page.batches.is_empty() && page.more {
        return Err(Error::MalformedResponse(
            "an empty page says more envelopes follow",
        ));
    }
    let mut last = after;
    for batch in &page.batches {
        if batch.seq <= last {
            return Err(Error::MalformedResponse(
                "a page repeats or reorders sequence numbers, or goes back past the cursor",
            ));
        }
        if batch.seq != last + 1 || i64::try_from(batch.seq).is_err() {
            return Err(Error::MalformedResponse("a page skips sequence numbers"));
        }
        last = batch.seq;
    }
    Ok(())
}

/// The keys envelopes may be signed with: this device's own, and the library's device list,
/// fetched when first needed and again on meeting each device it lacks.
struct Keys<'a> {
    credentials: &'a Credentials,
    recovery: DeviceId,
    devices: Option<HashMap<DeviceId, (VerifyingKey, bool)>>,
    looked_for: HashSet<DeviceId>,
}

enum Opened {
    Changes(Vec<Change>),
    Refused(Refused),
    /// Signed by a device the list lacks even fetched afresh.
    Unlisted(DeviceId),
}

enum Signer {
    Key(VerifyingKey),
    Refused(Refusal),
    Unlisted,
}

impl<'a> Keys<'a> {
    fn new(credentials: &'a Credentials) -> Keys<'a> {
        let recovery = credentials.library().recovery_signing_key().verifying_key();
        Keys {
            credentials,
            recovery: DeviceId::of(&recovery),
            devices: None,
            looked_for: HashSet::new(),
        }
    }

    /// The changes in one stored envelope, its refusal, or that its signer is unlisted.
    async fn open<T: Transport, N: Fn() -> u64 + Sync>(
        &mut self,
        rpc: &Rpc<'_, T, N>,
        batch: &StoredBatch,
    ) -> Result<Opened, Error> {
        let refuse = |device, reason| {
            Ok(Opened::Refused(Refused {
                seq: batch.seq,
                device,
                reason,
            }))
        };
        let Ok(envelope) = Envelope::decode(&batch.envelope) else {
            return refuse(None, Refusal::Malformed);
        };
        let device = envelope.device;
        if envelope.library != self.credentials.library_id() {
            return refuse(Some(device), Refusal::WrongLibrary);
        }
        let key = match self.signer(rpc, device).await? {
            Signer::Key(key) => key,
            Signer::Refused(reason) => return refuse(Some(device), reason),
            Signer::Unlisted => return Ok(Opened::Unlisted(device)),
        };
        match open(&batch.envelope, self.credentials.library(), |_| Some(key)) {
            Ok(opened) => Ok(Opened::Changes(opened.changes)),
            Err(error) => refuse(Some(device), refusal(error)),
        }
    }

    async fn signer<T: Transport, N: Fn() -> u64 + Sync>(
        &mut self,
        rpc: &Rpc<'_, T, N>,
        device: DeviceId,
    ) -> Result<Signer, Error> {
        if device == self.recovery {
            return Ok(Signer::Refused(Refusal::RecoveryKey));
        }
        if device == self.credentials.device_id() {
            return Ok(Signer::Key(self.credentials.device().verifying_key()));
        }
        let listed = |devices: &Option<HashMap<_, _>>| {
            devices.as_ref().is_some_and(|d| d.contains_key(&device))
        };
        if self.devices.is_none() {
            self.devices = Some(self.fetch(rpc).await?);
        }
        if !listed(&self.devices) && self.looked_for.insert(device) {
            self.devices = Some(self.fetch(rpc).await?);
        }
        Ok(match self.devices.as_ref().and_then(|d| d.get(&device)) {
            None => Signer::Unlisted,
            Some((_, true)) => Signer::Refused(Refusal::RecoveryKey),
            Some((key, false)) => Signer::Key(*key),
        })
    }

    async fn fetch<T: Transport, N: Fn() -> u64 + Sync>(
        &self,
        rpc: &Rpc<'_, T, N>,
    ) -> Result<HashMap<DeviceId, (VerifyingKey, bool)>, Error> {
        let body = rpc
            .call(
                Method::Get,
                devices_path(self.credentials.library_id()),
                Body::Empty,
                Some(self.credentials.device()),
            )
            .await?;
        let devices: Devices = json(&body, "the device list is not the documented JSON")?;
        Ok(devices
            .devices
            .into_iter()
            .map(|d| (DeviceId::of(&d.public_key), (d.public_key, d.recovery)))
            .collect())
    }
}

/// Whether the server refused an append for what the envelope is, which sending the same bytes
/// again cannot change, rather than for the request or the moment.
fn refuses_the_envelope(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::TooLarge
            | ErrorCode::MalformedEnvelope
            | ErrorCode::EnvelopeWrongLibrary
            | ErrorCode::EnvelopeWrongDevice
            | ErrorCode::EnvelopeBadSignature
    )
}

fn refusal(error: EnvelopeError) -> Refusal {
    match error {
        EnvelopeError::Decode(_) | EnvelopeError::Batch(_) => Refusal::Malformed,
        EnvelopeError::WrongLibrary => Refusal::WrongLibrary,
        EnvelopeError::UnknownDevice => Refusal::UnknownDevice,
        EnvelopeError::WrongDevice | EnvelopeError::BadSignature => Refusal::BadSignature,
        EnvelopeError::Decrypt => Refusal::Undecryptable,
    }
}

async fn append<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    credentials: &Credentials,
    envelope: Vec<u8>,
) -> Result<u64, Error> {
    let body = rpc
        .call(
            Method::Post,
            batches_path(credentials.library_id()),
            Body::Binary(envelope),
            Some(credentials.device()),
        )
        .await?;
    let appended: Appended = json(&body, "an append's answer is not the documented JSON")?;
    if appended.seq == 0 || i64::try_from(appended.seq).is_err() {
        return Err(Error::MalformedResponse(
            "an append's sequence number is out of range",
        ));
    }
    Ok(appended.seq)
}

/// Drops what the server confirmed from the outbox. When the envelope landed right after the
/// cursor, nothing came between, so the cursor moves past it too.
async fn acknowledge(
    conn: &mut SqliteConnection,
    replica: &Replica,
    library: fongo_sync_protocol::LibraryId,
    through: i64,
    seq: u64,
    cursor: &mut u64,
) -> Result<(), Error> {
    let mut tx = conn.begin().await?;
    replica.acknowledge(&mut tx, through).await?;
    state::clear_sending(&mut tx, library).await?;
    let follows = Some(seq) == cursor.checked_add(1);
    if follows {
        state::advance(&mut tx, library, seq).await?;
    }
    tx.commit().await?;
    if follows {
        *cursor = seq;
    }
    Ok(())
}

/// How many of `changes`, from the front, seal into an envelope of at most `max` bytes.
fn fitting(changes: &[Change], max: Option<u64>) -> usize {
    let Some(max) = max else {
        return changes.len();
    };
    let mut plaintext = 2;
    let mut n = 0;
    for change in changes {
        let size = encode_batch(std::slice::from_ref(change)).len() - 3;
        if envelope_len(plaintext + size + varint_len(n + 1)) as u64 > max {
            break;
        }
        plaintext += size;
        n += 1;
    }
    n
}

/// The length of an envelope around a batch of `plaintext` bytes, laid out as the protocol
/// documents: header, ciphertext with its 16-byte tag, signature.
fn envelope_len(plaintext: usize) -> usize {
    let ciphertext = plaintext + 16;
    1 + 16 + 16 + 4 + 24 + varint_len(ciphertext) + ciphertext + 64
}

fn varint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

fn merge(into: &mut Applied, from: Applied) {
    into.accepted += from.accepted;
    into.waiting = from.waiting;
    into.conflicts.extend(from.conflicts);
    let mut at: HashMap<(String, String), usize> = into
        .touched
        .iter()
        .enumerate()
        .map(|(i, t)| ((t.collection.clone(), t.key.clone()), i))
        .collect();
    for touched in from.touched {
        match at.get(&(touched.collection.clone(), touched.key.clone())) {
            Some(&i) => into.touched[i] = touched,
            None => {
                at.insert(
                    (touched.collection.clone(), touched.key.clone()),
                    into.touched.len(),
                );
                into.touched.push(touched);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use fongo_sync_protocol::{LibraryId, LibraryKeys, SigningKey};
    use fongo_sync_sqlite::core::{Gid, Hlc, Value};

    use super::*;

    fn change(text: &str) -> Change {
        Change {
            collection: "notes".into(),
            gid: Gid([1; 16]),
            field: "body".into(),
            value: Value::Text(text.into()),
            clock: Hlc::zero(Gid([2; 16])),
        }
    }

    #[test]
    fn the_size_estimate_is_the_sealed_size() {
        let keys = LibraryKeys::new(LibraryId([1; 16]), [2; 32]);
        let device = SigningKey::from_bytes(&[3; 32]);
        for count in [0usize, 1, 2, 130] {
            for len in [0usize, 1, 100, 200] {
                let changes: Vec<Change> = (0..count).map(|_| change(&"x".repeat(len))).collect();
                let sealed = seal(&changes, &keys, 0, &device, [0; 24]).len();
                assert_eq!(envelope_len(encode_batch(&changes).len()), sealed);
                assert_eq!(fitting(&changes, Some(sealed as u64)), count);
                if count > 0 {
                    assert!(fitting(&changes, Some(sealed as u64 - 1)) < count);
                }
            }
        }
    }

    #[test]
    fn the_smallest_batch_is_one_empty_change() {
        let empty = Change {
            collection: String::new(),
            gid: Gid([1; 16]),
            field: String::new(),
            value: Value::Null,
            clock: Hlc::zero(Gid([2; 16])),
        };
        assert_eq!(encode_batch(&[empty]).len(), SMALLEST_BATCH);
    }

    #[test]
    fn a_page_must_continue_the_log_one_by_one() {
        let page = |seqs: &[u64], more| BatchPage {
            batches: seqs
                .iter()
                .map(|&seq| StoredBatch {
                    seq,
                    envelope: Vec::new(),
                })
                .collect(),
            more,
        };
        assert!(check_page(&page(&[4, 5, 6], true), 3, 3).is_ok());
        assert!(check_page(&page(&[], false), 3, 3).is_ok());
        for (bad, after) in [
            (page(&[4, 5, 6, 7], false), 3),
            (page(&[], true), 3),
            (page(&[5, 4], false), 3),
            (page(&[4, 6], false), 3),
            (page(&[5], false), 3),
            (page(&[3], false), 3),
            (page(&[1], false), 3),
            (page(&[4, 4], false), 3),
            (page(&[1 << 63], false), (1 << 63) - 1),
        ] {
            assert!(check_page(&bad, after, 3).is_err(), "{bad:?}");
        }
    }
}
