//! Control-plane messages, sent as JSON, the paths they go to, the error vocabulary and the
//! change notification. Every request except `GET` [`CAPABILITIES_PATH`] carries
//! [`RequestAuth`](crate::RequestAuth) headers.
//!
//! Any 2xx status is success; a client does not depend on which one a server sends. Every error
//! answer is an [`ErrorBody`], and a client acts on its [`ErrorCode`] alone.
//!
//! [`Capabilities::blobs`] says whether [`BATCHES_CLAIMING_PATH`], the blob routes
//! ([`BLOB_PATH`] to [`BLOBS_RETIRE_PATH`]), [`SETTLED_PATH`], [`STATUS_PATH`] and
//! [`ENTITLEMENT_PATH`] are served, all together; [`Capabilities::device_removal`] says whether
//! [`REMOVE_DEVICE_PATH`] is, and [`Capabilities::notifications`] whether [`NOTIFICATIONS_PATH`]
//! is. A route that is not served answers [`ErrorCode::NotFound`].
//!
//! The blob routes, [`SETTLED_PATH`], [`STATUS_PATH`], [`ENTITLEMENT_PATH`] and
//! [`REMOVE_DEVICE_PATH`] are for devices only: the recovery key is refused with
//! [`ErrorCode::Forbidden`]. A removed device is refused on every authenticated route with
//! [`ErrorCode::DeviceRemoved`], an append of an envelope the server already holds included. That
//! answer comes only once the request's signature verifies, so a request that does not verify is
//! refused for that, and only the holder of a key learns that it was removed.
//!
//! # Devices
//!
//! A device is added by `POST` [`AddDevice`] at [`DEVICES_PATH`]. A key not yet listed needs a
//! free slot: a library already listing [`Capabilities::max_devices`] keys that are neither removed
//! nor the recovery key refuses it with [`ErrorCode::DeviceLimit`]. Adding a key that is listed
//! and not removed succeeds and changes nothing, its stored wrap key and membership proof
//! included. Adding a removed key restores it: the server answers 201, needs a free slot as for a
//! new key ([`ErrorCode::DeviceLimit`] otherwise), keeps the [`Settled`] position the device
//! reported before it was removed, and stores the wrap key and membership proof the request
//! carries; a request that carries none may leave the restored record without them.
//!
//! A [`DeviceRecord`]'s `last_seen`, documented there as the time of the last accepted request, is
//! more precisely the start of the hour, a multiple of 3600, in which the key last made a request
//! that authenticated; a request refused after authenticating, such as one
//! from a removed device, may still move it.
//!
//! Whether a server checks a wrap key's proof before it finds the key already listed is not
//! settled: re-adding a listed key with a wrap key whose proof does not verify may succeed and
//! change nothing, or be refused with [`ErrorCode::InvalidProof`]. A client sends a valid proof or
//! none.
//!
//! # Limits on requests
//!
//! [`Capabilities::max_batch_bytes`] bounds the envelope alone: the ids before the envelope in a
//! [`ClaimingAppend`] do not count against it. [`Capabilities::max_query_ids`] bounds every list of
//! blob ids: a [`BlobIds`] sent to [`BLOBS_QUERY_PATH`] or [`BLOBS_CLAIM_PATH`], a [`RetireBlobs`]
//! and a [`ClaimingAppend`]. A list with more is refused with [`ErrorCode::MalformedRequest`]; an
//! id repeated in one list counts once. A server that states `max_query_ids` accepts a JSON body
//! of such a list up to 1024 bytes plus 67 for each id allowed, which a body this crate writes,
//! with no more ids and none repeated, never exceeds; it may refuse a longer one with
//! [`ErrorCode::TooLarge`] before reading it. A chunk body at [`BLOB_CHUNK_PATH`] or
//! [`BLOB_WHOLE_PATH`] longer than [`SEALED_CHUNK_LEN`](crate::SEALED_CHUNK_LEN) is refused with
//! [`ErrorCode::TooLarge`], before the body is read; a body of
//! any other wrong length is [`ErrorCode::MalformedRequest`].
//!
//! A server may bound how many blob records a library has: one for each id it holds a claim, an
//! upload or a complete blob for. For blobs it answers [`ErrorCode::LibraryFull`] only to a
//! request that needs a record for an id that has none (a begin, `whole`, a claim at
//! [`BLOBS_CLAIM_PATH`] or a [`ClaimingAppend`]) while the library has as many as it allows. A
//! claiming append so refused appends nothing. A server drops idle records of ids it holds no
//! complete blob for, whether or not devices have settled, so the refusal does not last for
//! ever. This version has no capability that states the bound.
//!
//! # Entitlement and allowance
//!
//! A library that is not entitled to the service is refused [`ErrorCode::NotEntitled`] when
//! appending an envelope (at either batch path), claiming, retiring or writing a blob; every `GET`,
//! a blob query, adding a device, [`STATUS_PATH`], [`ENTITLEMENT_PATH`], [`SETTLED_PATH`] and
//! [`REMOVE_DEVICE_PATH`] still work. A library over its allowance is refused
//! [`ErrorCode::QuotaExceeded`] when beginning a blob or uploading a chunk or `whole`, and
//! everything else works, `complete` included. At the edges:
//!
//! | request | not entitled | over its allowance |
//! |---|---|---|
//! | begin with the shape of a blob that is complete | success | success |
//! | a chunk upload or `whole` with the bytes a complete blob holds there | success | success |
//! | `complete` of a complete blob, with the matching digest | success | success |
//! | `complete` of a blob not yet complete | [`ErrorCode::NotEntitled`] | success |
//! | a [`ClaimingAppend`] of an envelope the server already holds, where it recognises it | success, and the claims are recorded | success, and the claims are recorded |
//!
//! # Appending an envelope twice
//!
//! A server may recognise an envelope identical to one it holds, at either batch path, or may not;
//! a device does not rely on either, since it applies a copy idempotently. Where a server
//! recognises one, it stores nothing, answers the sequence number it gave the envelope, and, for a
//! [`ClaimingAppend`], records its ids as claimed at a position no lower than that number; it may
//! record them at the library's head. A device relies only on that lower bound. A server that
//! recognises a [`ClaimingAppend`] so does this also when the library is not entitled. Where a
//! server does not recognise one, the request is an ordinary append, accepted or refused as any
//! other.
//!
//! # Blob routes at their edges
//!
//! Where two rows apply, a malformed request ([`ErrorCode::MalformedRequest`]) is answered before
//! a state error ([`ErrorCode::BlobMismatch`], [`ErrorCode::BlobIncomplete`]): a chunk index past
//! the last of a blob not yet complete is malformed, not incomplete. A blob that was deleted, or
//! an incomplete upload that was dropped, counts as never begun.
//!
//! | request | when | answer |
//! |---|---|---|
//! | begin | the shape is not valid ([`BlobInfo::is_valid`] is false) | [`ErrorCode::MalformedRequest`] |
//! | begin or `whole` | the stored size is above [`Capabilities::max_blob_bytes`] | [`ErrorCode::BlobTooLarge`] |
//! | begin or `whole` | an earlier begin, or a completion through `whole`, gave another shape | [`ErrorCode::BlobMismatch`] |
//! | begin | the blob is complete with the same shape | `{complete: true, have: every chunk}` |
//! | a chunk upload or `whole` | the body is longer than [`SEALED_CHUNK_LEN`](crate::SEALED_CHUNK_LEN) | [`ErrorCode::TooLarge`], before the body is read |
//! | `whole` | the body is not the stored form of a one-chunk blob | [`ErrorCode::MalformedRequest`] |
//! | `whole` | the blob was begun with the same one-chunk shape | stores the chunk and completes |
//! | `whole` | the blob is complete | success if the bytes equal the stored chunk, else [`ErrorCode::BlobMismatch`] |
//! | a chunk upload, `complete`, or a `GET` | the blob was never begun | [`ErrorCode::BlobNotFound`] |
//! | a chunk upload or `GET` of a chunk | the index is not below the chunk count | [`ErrorCode::MalformedRequest`] |
//! | a chunk upload | the body is not longer than [`SEALED_CHUNK_LEN`](crate::SEALED_CHUNK_LEN) and not that chunk's [`BlobInfo::stored_chunk_len`] | [`ErrorCode::MalformedRequest`] |
//! | a chunk upload | the blob is complete | success if the bytes equal the stored chunk, else [`ErrorCode::BlobMismatch`]; nothing changes |
//! | `complete` | the blob is complete | success if the digest matches, else [`ErrorCode::BlobMismatch`] |
//! | `GET` a chunk or the blob | the blob is not complete | [`ErrorCode::BlobIncomplete`] |
//! | [`RetireBlobs`] | `through` is above the library's current head | [`ErrorCode::MalformedRequest`] |
//! | [`RetireBlobs`] | an id is not held complete | that id is ignored; success |
//! | [`Settled`] | `seq` is above the library's current head | [`ErrorCode::MalformedRequest`] |
//! | [`Settled`] | `seq` is not above what the device reported before | success; nothing changes |
//!
//! # Deleting files
//!
//! A server deletes a complete blob only when no change in the library can refer to it any more.
//! The rules rest on one definition. A set of changes refers to a file when, for some field the
//! application marks as naming files and some collection and gid, the field's strategy winner
//! among the set's changes to that collection, gid and field is a reference to the file. Every
//! change to it whose clock is valid counts, whatever the row's liveness, group or collection: a
//! deleted or merged row still refers to its file until a later change to that field replaces it.
//! Adding changes to a set therefore makes it refer to a file only through an added change that
//! sets a reference to it. What an application shows is not monotone in this way (removing one
//! member of a merged group can show an older member's file again without any change naming it),
//! which is why it is not the definition.
//!
//! On a build that settles clock ties by value, a winner is a function of the set of changes and
//! is one of its values, the same on every replica, with no condition on clocks: of two changes
//! to one field at one clock, which a database restored from a backup or a copy of a database can
//! write, the greater value wins. That holds also on a replica that kept the changes for a
//! collection its registry lacked and declared it later: it keeps one value for each field and
//! clock, the greater, which is exact for a field merged by clock or by greatest value. A file
//! field is such a field, a plain field that is never a key field of a natural collection, so it
//! is not one of the two cases where such a replica keeps too few values (aliases, and key
//! fields of natural collections). So a replica never keeps a value that the retiring device's
//! winner replaced.
//!
//! The guarantee below holds under these conditions:
//!
//! - every device runs a build that settles clock ties by value, or no two changes to one field
//!   share a clock with different values, which honest devices write only when two databases
//!   write as one node. A build that keeps whichever of two such changes arrived first can keep a
//!   value that the retiring device's winner replaced.
//! - every device that sends file references sends their claims as described below.
//! - a field marked as naming files is merged by clock in every version of the application, is
//!   never a key field of a natural collection, and once marked is never unmarked: the definition
//!   and the treatment of rewritten key fields differ, and a device that does not count a field
//!   as naming files can retire what it names. A registry must not mark a key field.
//! - each device key is used by one database. Two databases on one key, such as a copy of a
//!   database or a backup restored while the database it was taken from went on, are one device
//!   to the server; see the limits.
//! - a device's state holds only its own changes and the changes of envelopes in the log, each
//!   envelope applied whole. A device that takes changes from outside the log, or applies only
//!   part of an envelope, can retire a file that the log refers to.
//! - the server holds to what a device relies on it for (see [the crate
//!   documentation](crate#what-a-device-relies-on-the-server-for)).
//!
//! A device:
//!
//! - sends each envelope any of whose changes sets a file reference to [`BATCHES_CLAIMING_PATH`],
//!   with the id of every file it sets, so that the claim is recorded at the envelope's sequence
//!   number in the same step as the append. An envelope it appends at [`BATCHES_PATH`] instead,
//!   because [`Capabilities::blobs`] was false when it sent it or because the claiming append was
//!   refused with [`ErrorCode::LibraryFull`], has its ids claimed at [`BLOBS_CLAIM_PATH`] before
//!   the device next reports [`Settled`].
//! - retires a file ([`RetireBlobs`]) only when it has merged every envelope through `through`,
//!   has nothing unsent, and the changes through `through` do not refer to the file; its rows held
//!   back count as referring. Only the changes through `through` count: its own envelopes appended
//!   after `through` are in its local state but not in that set. A device that skipped, refused or
//!   stepped past any envelope at or below `through` has not merged it, and does not retire.
//! - reports [`Settled`] `seq` only when it has dealt with every envelope through `seq`, by merging
//!   it or by skipping it and recording that it did, and has nothing unsent. Before each such
//!   report, and after it has dealt with every envelope through `seq`, it claims at
//!   [`BLOBS_CLAIM_PATH`] every file named by a row it holds back at that point, and it merges
//!   nothing between that claim and the report: it never reports a `seq` above the position it had
//!   dealt with when it claimed. The claim is then recorded at a head at or past `seq`, so it voids
//!   any retirement the report could otherwise let through. A device that skipped an envelope may
//!   still settle, since the guarantee uses its report only for having nothing unsent and for the
//!   claim before it.
//! - may also claim at [`BLOBS_CLAIM_PATH`] without appending, to learn which files the server
//!   lacks.
//!
//! A server:
//!
//! - records a claim, in a claiming append or at [`BLOBS_CLAIM_PATH`], for every id it names,
//!   whether it holds that blob complete, incomplete or not at all. A claim dropped because an
//!   upload was still incomplete would let an older retirement delete the blob once it completes.
//! - counts an id as claimed at the library's head whenever it may have missed or forgotten its
//!   claims: when it begins to offer [`Capabilities::blobs`], for the first time or again, since
//!   appends made meanwhile carried no claims, and when it drops the record of an id it holds no
//!   complete blob for, with that record's claims. It does so with a claim floor per library,
//!   raised at such a moment to a head at or above the one then, before it deletes anything: every
//!   id counts as claimed at the floor as well as wherever its own claims were recorded.
//! - treats a retirement `(through, id)` as void once the id has a claim recorded, or counts as
//!   claimed, at a sequence number at or above `through`, whether the claim arrived before or
//!   after the retirement.
//! - keeps, of the retirements of an id that are not void, the earliest to arrive, so that a
//!   retirement sent again never restarts the wait. It keeps at most four retirements of one id;
//!   keeping fewer only delays a deletion, and is always safe.
//! - deletes a complete blob only while it offers [`Capabilities::blobs`], and only when it has a
//!   retirement that is not void, every device in [`Devices::settling`] has reported [`Settled`]
//!   at or above that retirement's `through` (one that never reported counts as 0), and
//!   [`Capabilities::blob_wait_days`] have passed since the retirement arrived. A deletion and a
//!   claim of the same id are ordered: a claim after the deletion finds the blob missing.
//!
//! Why no change that is or will be in the log refers to a deleted file: an honest retirement at
//! `through` means the changes through `through` do not refer to it. Every later set of changes
//! holds those, so one that refers to the file holds a later change setting a reference to it. If
//! that change was appended with its claim, the claim is at a sequence number above `through`,
//! which voids the retirement; a claim recorded for a recognised copy of the envelope is at or
//! above the envelope's number, and so above `through` too. If it was appended without its claim
//! while the server was not offering blobs, the floor raised when it began again is above `through`
//! and voids the retirement in the same way. Otherwise it was appended without its claim, or is not
//! yet appended, and its device has not settled since making it: a device claims what it appended
//! without claims before it next settles, and a device holding unsent changes does not settle.
//! Every device that counts has settled at or above `through`, so that device made the change after
//! settling. A row a device holds back is not in the log, but the file it names was claimed after
//! that device had dealt with every envelope through its report and before the report, at a head at
//! or past it, so every retirement that report lets through is void.
//!
//! The limits:
//!
//! - any change a device makes after its [`Settled`] at or above `through` that refers to the file,
//!   and that it sends without its claim or after the deletion, can find the file deleted before
//!   its claim arrives: a claiming append refused with [`ErrorCode::LibraryFull`] and sent plainly,
//!   or one sent plainly because the device read [`Capabilities::blobs`] as false just before the
//!   server began to offer it, is claimed only later. The device recovers the file only if it still
//!   holds it, by uploading it again; if not, the file is unavailable. The same holds for a removed
//!   device that joins again and sends changes it had not sent. A device that refers anew to a file
//!   it does not hold can narrow the window by claiming it at [`BLOBS_CLAIM_PATH`] at once.
//! - a device catching up, newly joined or restored, passes through states that refer to files
//!   rightly deleted since. Fetching one answers [`ErrorCode::BlobNotFound`], which is not an error
//!   in the library.
//! - a retirement of an id that is not complete is ignored, so a blob completed later that nothing
//!   refers to stays.
//! - after a server begins to offer blobs again, or forgets an id's claims, every retirement
//!   through its floor is void, so deletion resumes only once the log has grown past the floor and
//!   a device retires the file again.
//! - a device that never reports [`Settled`] holds back every deletion until it is removed.
//! - a device that skipped an envelope may settle, and can then show a file that the skipped
//!   envelope replaced and that was rightly deleted; fetching it answers
//!   [`ErrorCode::BlobNotFound`].
//! - two databases that act as one device, against the conditions above, can lose a file: the
//!   [`Settled`] one reports can stand for unsent changes the other holds. Such a change that
//!   refers to a file and is sent after the file's deletion finds it missing, as in the first
//!   limit; so does one in a database restored from a backup, or made by a new database on a key
//!   an older one used.

mod blobs;
mod error;
mod notify;
mod route;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub use blobs::{
    BlobDigest, BlobIds, BlobInfo, BlobState, ClaimingAppend, CompleteBlob, CompleteBlobs,
    DigestError, Entitlement, HaveChunk, LibraryStatus, MissingBlobs, RemoveDevice, RetireBlobs,
    Settled,
};
pub use error::{ErrorBody, ErrorCode};
pub use notify::{CLOSE_DEVICE_REMOVED, CLOSE_SUPERSEDED, NOTIFICATION_FORMAT, Notification};
pub use route::{MalformedQuery, Route};

use crate::hex::{serde_key, serde_sig};
use crate::keys::{DeviceId, LibraryId, LibraryKeys, WrapKey};
use crate::member::{ListedMemberProof, MemberCheck, MemberProof};
use crate::wire::{DecodeError, Reader, put_blob, put_varint};

/// `GET`: [`Capabilities`].
pub const CAPABILITIES_PATH: &str = "/v1/capabilities";
/// `POST` [`CreateLibrary`]: [`Devices`]. Authenticated by the new device's own key.
pub const LIBRARIES_PATH: &str = "/v1/libraries";
/// `GET`: [`Devices`]. `POST` [`AddDevice`]: [`Devices`]. `{library}` is the library id in hex.
pub const DEVICES_PATH: &str = "/v1/libraries/{library}/devices";
/// `POST` envelope bytes: [`Appended`]. `GET` with [`ReadBatches`] as the query: a [`BatchPage`].
pub const BATCHES_PATH: &str = "/v1/libraries/{library}/batches";
/// `POST` a [`ClaimingAppend`] (binary): [`Appended`]. An append that also claims the blobs its
/// envelope's changes set references to.
pub const BATCHES_CLAIMING_PATH: &str = "/v1/libraries/{library}/batches/claiming";
/// `GET` upgraded to a WebSocket: a stream of [`Notification`]s. Offered when
/// [`Capabilities::notifications`] is true.
pub const NOTIFICATIONS_PATH: &str = "/v1/libraries/{library}/notifications";

/// `POST` [`BlobInfo`]: [`BlobState`], beginning or resuming an upload; idempotent, and
/// [`ErrorCode::BlobMismatch`] if an earlier begin gave another size or chunk count. `GET`:
/// [`BlobInfo`] of a complete blob, else [`ErrorCode::BlobNotFound`] or
/// [`ErrorCode::BlobIncomplete`]. `{id}` is the [`BlobId`](crate::BlobId) in hex.
pub const BLOB_PATH: &str = "/v1/libraries/{library}/blobs/{id}";
/// `POST` one stored chunk (binary) of a begun blob, of the length [`BlobInfo::stored_chunk_len`]
/// gives, answered with no body. Until the blob is complete it replaces whatever was stored at that
/// index; see [`BlobState`]. `GET`: the stored chunk (binary) of a complete blob. `{n}` is the chunk
/// index in decimal.
pub const BLOB_CHUNK_PATH: &str = "/v1/libraries/{library}/blobs/{id}/chunks/{n}";
/// `POST` [`CompleteBlob`]: [`BlobInfo`], or [`ErrorCode::BlobIncomplete`] unless every chunk is
/// stored, or [`ErrorCode::BlobMismatch`] if the chunks stored are not those the digest names,
/// leaving the blob incomplete. From then on the blob is readable and never changes. Idempotent.
pub const BLOB_COMPLETE_PATH: &str = "/v1/libraries/{library}/blobs/{id}/complete";
/// `POST` the only stored chunk (binary) of a one-chunk blob: [`BlobInfo`]. Begins, stores and
/// completes in one request; it needs no [`CompleteBlob`], since the request itself holds the
/// chunk it completes with.
pub const BLOB_WHOLE_PATH: &str = "/v1/libraries/{library}/blobs/{id}/whole";
/// `POST` [`BlobIds`]: [`CompleteBlobs`].
pub const BLOBS_QUERY_PATH: &str = "/v1/libraries/{library}/blobs/query";
/// `POST` [`BlobIds`]: [`MissingBlobs`].
pub const BLOBS_CLAIM_PATH: &str = "/v1/libraries/{library}/blobs/claim";
/// `POST` [`RetireBlobs`], answered with no body, also when every id was ignored.
pub const BLOBS_RETIRE_PATH: &str = "/v1/libraries/{library}/blobs/retire";
/// `POST` [`Settled`], answered with no body.
pub const SETTLED_PATH: &str = "/v1/libraries/{library}/settled";
/// `GET`: [`LibraryStatus`].
pub const STATUS_PATH: &str = "/v1/libraries/{library}/status";
/// `POST` [`Entitlement`]: [`LibraryStatus`].
pub const ENTITLEMENT_PATH: &str = "/v1/libraries/{library}/entitlement";
/// `POST` [`RemoveDevice`]: [`Devices`]. Naming the recovery key is [`ErrorCode::Forbidden`].
pub const REMOVE_DEVICE_PATH: &str = "/v1/libraries/{library}/devices/remove";

/// Content type of envelope request bodies and [`BatchPage`] responses.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

const PROOF_LABEL: &[u8] = b"fongo-sync/v1/create-library\0";
const WRAP_PROOF_LABEL: &[u8] = b"fongo-sync/v1/wrap-key\0";
const PAGE_FORMAT: u8 = 1;
const PAGE_MORE: u8 = 1;

/// [`DEVICES_PATH`] for one library.
pub fn devices_path(library: LibraryId) -> String {
    DEVICES_PATH.replace("{library}", &library.to_hex())
}

/// [`BATCHES_PATH`] for one library.
pub fn batches_path(library: LibraryId) -> String {
    BATCHES_PATH.replace("{library}", &library.to_hex())
}

/// [`NOTIFICATIONS_PATH`] for one library.
pub fn notifications_path(library: LibraryId) -> String {
    NOTIFICATIONS_PATH.replace("{library}", &library.to_hex())
}

/// What a server offers. Absent quotas are unlimited or undisclosed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub protocol_version: u32,
    /// Whether [`BATCHES_CLAIMING_PATH`], the blob routes ([`BLOB_PATH`] to
    /// [`BLOBS_RETIRE_PATH`]), [`SETTLED_PATH`], [`STATUS_PATH`] and [`ENTITLEMENT_PATH`] are
    /// served. While it is false the server deletes no blob; see
    /// [the module documentation](self#deleting-files).
    pub blobs: bool,
    /// Whether [`NOTIFICATIONS_PATH`] is served.
    pub notifications: bool,
    /// How far a request timestamp may be from the server's clock, either side, in seconds.
    pub skew_secs: u64,
    /// Largest envelope accepted, in bytes. In a [`ClaimingAppend`] only the envelope counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_bytes: Option<u64>,
    /// Largest total of stored envelope bytes per library.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_library_bytes: Option<u64>,
    /// Most devices per library, not counting the recovery key or removed devices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_devices: Option<u32>,
    /// Most envelopes in one [`BatchPage`], whatever `limit` asks for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_page_batches: Option<u32>,
    /// Most envelope bytes in one [`BatchPage`]; a single larger envelope still comes alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_page_bytes: Option<u64>,
    /// Largest blob accepted, in stored bytes ([`BlobInfo::size`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_blob_bytes: Option<u64>,
    /// Most distinct ids in one list of blob ids: a [`BlobIds`] sent to [`BLOBS_QUERY_PATH`] or
    /// [`BLOBS_CLAIM_PATH`], a [`RetireBlobs`] or a [`ClaimingAppend`]. A list with more is
    /// [`ErrorCode::MalformedRequest`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_query_ids: Option<u32>,
    /// Days a retired blob is kept at least, counted from when its retirement arrived; see
    /// [the module documentation](self#deleting-files).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_wait_days: Option<u32>,
    /// Days without activity after which an incomplete upload may be dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_incomplete_days: Option<u32>,
    /// Whether [`REMOVE_DEVICE_PATH`] is served.
    #[serde(default, skip_serializing_if = "is_false")]
    pub device_removal: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Registers a library with its first device and its recovery key. The server must refuse an id
/// already taken and a `proof` that does not verify.
///
/// A device that repeats a create whose answer it lost is refused the id; it learns that its
/// first create took effect by reading the device list with its own key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawCreateLibrary")]
pub struct CreateLibrary {
    pub library: LibraryId,
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// [`LibraryKeys::recovery_signing_key`](crate::LibraryKeys::recovery_signing_key)'s public half.
    #[serde(with = "serde_key")]
    pub recovery_key: VerifyingKey,
    /// The device key's signature over `fongo-sync/v1/create-library` and a zero byte, the
    /// library id, the device key and the recovery key.
    #[serde(with = "serde_sig")]
    pub proof: Signature,
    /// The first device's wrap key, stored and returned in its [`DeviceRecord`].
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
    /// The device's [`MemberProof`] over its key and the wrap key this message carries, made by
    /// [`CreateLibrary::with_member_proof`]. The server stores it and returns it unchanged in the
    /// device's [`DeviceRecord`], and cannot check it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_proof: Option<MemberProof>,
    /// An opaque token for the server's own policy. Nothing signs it: whoever holds it may
    /// present it, as with [`Entitlement`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entitlement: Option<String>,
}

impl CreateLibrary {
    pub fn new(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
    ) -> CreateLibrary {
        let device_key = device.verifying_key();
        CreateLibrary {
            library,
            device_key,
            recovery_key,
            proof: device.sign(&proof_message(library, &device_key, &recovery_key)),
            wrap: None,
            member_proof: None,
            entitlement: None,
        }
    }

    /// A request that also registers the first device's wrap key, signed by the device key.
    pub fn with_wrap_key(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
        wrap_key: WrapKey,
    ) -> CreateLibrary {
        CreateLibrary {
            wrap: Some(SignedWrapKey::new(library, device, wrap_key)),
            ..CreateLibrary::new(library, device, recovery_key)
        }
    }

    /// The same request with the [`MemberProof`] `keys` make for its device key and wrap key.
    /// [`OtherLibrary`] if `keys` are not those of `library`, whose proof would never check.
    pub fn with_member_proof(self, keys: &LibraryKeys) -> Result<CreateLibrary, OtherLibrary> {
        if keys.id != self.library {
            return Err(OtherLibrary);
        }
        Ok(CreateLibrary {
            member_proof: Some(member_proof(keys, &self.device_key, self.wrap.as_ref())),
            ..self
        })
    }

    /// Whether `proof` shows the caller holds `device_key`'s private half.
    pub fn verify(&self) -> bool {
        self.device_key
            .verify_strict(
                &proof_message(self.library, &self.device_key, &self.recovery_key),
                &self.proof,
            )
            .is_ok()
    }

    /// Whether the request carries a wrap key that the device key signed for this library.
    pub fn verify_wrap(&self) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(self.library, &self.device_key))
    }
}

fn proof_message(library: LibraryId, device: &VerifyingKey, recovery: &VerifyingKey) -> Vec<u8> {
    let mut out = PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(recovery.as_bytes());
    out
}

/// A device's [`WrapKey`] with the device key's signature over it, so that any device shown it
/// can check that the device chose it and not whoever passed it on. In JSON it is the two fields
/// `wrap_key` and `wrap_proof` of the message that carries it, both or neither.
///
/// `wrap_proof` signs `fongo-sync/v1/wrap-key` and a zero byte, the library id, the device's public
/// key and the wrap key. A server refuses, with [`ErrorCode::InvalidProof`], a wrap key whose proof
/// does not verify in a request that would store it; for a key already listed, whether the proof is
/// checked before the server finds the key listed is not settled (see [the module
/// documentation](self#devices)). The proof carries no counter, so a server can present any wrap
/// key a device ever signed for the library; replacing a wrap key will need a new label or a
/// counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SignedWrapKey {
    pub wrap_key: WrapKey,
    #[serde(with = "serde_sig")]
    pub wrap_proof: Signature,
}

impl SignedWrapKey {
    /// `wrap_key`, signed by `device` for `library`.
    pub fn new(library: LibraryId, device: &SigningKey, wrap_key: WrapKey) -> SignedWrapKey {
        SignedWrapKey {
            wrap_key,
            wrap_proof: device.sign(&wrap_message(library, &device.verifying_key(), &wrap_key)),
        }
    }

    /// Whether `device_key` signed this wrap key for `library`.
    pub fn verify(&self, library: LibraryId, device_key: &VerifyingKey) -> bool {
        device_key
            .verify_strict(
                &wrap_message(library, device_key, &self.wrap_key),
                &self.wrap_proof,
            )
            .is_ok()
    }
}

fn wrap_message(library: LibraryId, device: &VerifyingKey, wrap_key: &WrapKey) -> Vec<u8> {
    let mut out = WRAP_PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(&wrap_key.0);
    out
}

/// Adds a device. Signed, through [`RequestAuth`](crate::RequestAuth), by a device already in the
/// library or by its recovery key.
///
/// A key already listed and not removed is accepted and changes nothing, its stored wrap key and
/// membership proof included, so a device can repeat an add whose answer it lost. A removed one is
/// restored with the wrap key and membership proof this request carries. See
/// [the module documentation](self#devices) for slots and status codes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawAddDevice")]
pub struct AddDevice {
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// The new device's wrap key, signed by the new device's key, stored and returned in its
    /// [`DeviceRecord`].
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
    /// The device's [`MemberProof`] over its key and the wrap key this message carries, made by
    /// [`AddDevice::with_member_proof`]. The server stores it and returns it unchanged in the
    /// device's [`DeviceRecord`], and cannot check it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_proof: Option<MemberProof>,
}

impl AddDevice {
    pub fn new(device_key: VerifyingKey) -> AddDevice {
        AddDevice {
            device_key,
            wrap: None,
            member_proof: None,
        }
    }

    /// Adds `device` to `library` with its wrap key. A device that adds another without its
    /// signing key takes the [`SignedWrapKey`] the new device made and sets `wrap` itself.
    pub fn with_wrap_key(library: LibraryId, device: &SigningKey, wrap_key: WrapKey) -> AddDevice {
        AddDevice {
            device_key: device.verifying_key(),
            wrap: Some(SignedWrapKey::new(library, device, wrap_key)),
            member_proof: None,
        }
    }

    /// The same request with the [`MemberProof`] the library's `keys` make for its device key
    /// and wrap key. `keys` must be those of the library the request is sent to: the request does
    /// not name its library, so only a wrap key, signed for one, shows a mismatch, and
    /// [`OtherLibrary`] is returned if the request carries a wrap key its device key did not sign
    /// for `keys`' library.
    pub fn with_member_proof(self, keys: &LibraryKeys) -> Result<AddDevice, OtherLibrary> {
        if self.wrap.is_some() && !self.verify_wrap(keys.id) {
            return Err(OtherLibrary);
        }
        Ok(AddDevice {
            member_proof: Some(member_proof(keys, &self.device_key, self.wrap.as_ref())),
            ..self
        })
    }

    /// Whether the request carries a wrap key that the new device's key signed for `library`.
    pub fn verify_wrap(&self, library: LibraryId) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(library, &self.device_key))
    }
}

/// One key the library knows. Its device id is [`DeviceId::of`](crate::DeviceId::of) the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawDeviceRecord")]
pub struct DeviceRecord {
    #[serde(with = "serde_key")]
    pub public_key: VerifyingKey,
    /// The library's recovery key: it may authenticate requests but never signs envelopes.
    pub recovery: bool,
    /// The wrap key the device registered, if any.
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
    /// The [`MemberProof`] the device registered, if any, as the server listed it; see
    /// [`DeviceRecord::check_member_proof`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_proof: Option<ListedMemberProof>,
    /// Removed through [`REMOVE_DEVICE_PATH`]: the server refuses its requests, and it no longer
    /// holds back the deletion of retired blobs.
    #[serde(default, skip_serializing_if = "is_false")]
    pub removed: bool,
    /// Unix seconds of the device's last accepted request, possibly coarse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<u64>,
}

impl DeviceRecord {
    /// Whether the record carries a wrap key that its own key signed for `library`. The server
    /// can drop a wrap key, but cannot put in one the device did not sign.
    pub fn verify_wrap(&self, library: LibraryId) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(library, &self.public_key))
    }

    /// Checks the record's [`MemberProof`] against the library's `keys`, for the record's key
    /// and its wrap key as listed, whether or not that wrap key's own proof verifies; a caller
    /// sealing to the wrap key also checks [`DeviceRecord::verify_wrap`].
    pub fn check_member_proof(&self, keys: &LibraryKeys) -> MemberCheck {
        match &self.member_proof {
            None => MemberCheck::Absent,
            Some(ListedMemberProof::Unreadable) => MemberCheck::Invalid,
            Some(ListedMemberProof::Readable(proof)) => {
                let wrap = self.wrap.as_ref().map(|wrap| &wrap.wrap_key);
                if keys.verify_member_proof(&self.public_key, wrap, proof) {
                    MemberCheck::Valid
                } else {
                    MemberCheck::Invalid
                }
            }
        }
    }
}

fn member_proof(
    keys: &LibraryKeys,
    device: &VerifyingKey,
    wrap: Option<&SignedWrapKey>,
) -> MemberProof {
    keys.member_proof(device, wrap.map(|wrap| &wrap.wrap_key))
}

#[derive(Deserialize)]
struct RawCreateLibrary {
    library: LibraryId,
    #[serde(with = "serde_key")]
    device_key: VerifyingKey,
    #[serde(with = "serde_key")]
    recovery_key: VerifyingKey,
    #[serde(with = "serde_sig")]
    proof: Signature,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
    #[serde(default)]
    member_proof: Option<MemberProof>,
    #[serde(default)]
    entitlement: Option<String>,
}

#[derive(Deserialize)]
struct RawAddDevice {
    #[serde(with = "serde_key")]
    device_key: VerifyingKey,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
    #[serde(default)]
    member_proof: Option<MemberProof>,
}

#[derive(Deserialize)]
struct RawDeviceRecord {
    #[serde(with = "serde_key")]
    public_key: VerifyingKey,
    recovery: bool,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
    #[serde(default)]
    member_proof: Option<ListedMemberProof>,
    #[serde(default)]
    removed: Option<bool>,
    #[serde(default)]
    last_seen: Option<u64>,
}

#[derive(Deserialize)]
struct ProofHex(#[serde(with = "serde_sig")] Signature);

/// Why a membership proof was not added to a request: the keys given are another library's than
/// the request's, so their proof would never check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the keys are another library's than the request's")]
pub struct OtherLibrary;

/// Why a message was refused that carried a wrap key without its proof, or a proof without its
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("wrap_key and wrap_proof come together or not at all")]
pub struct UnpairedWrapKey;

fn pair(
    wrap_key: Option<WrapKey>,
    wrap_proof: Option<ProofHex>,
) -> Result<Option<SignedWrapKey>, UnpairedWrapKey> {
    match (wrap_key, wrap_proof) {
        (None, None) => Ok(None),
        (Some(wrap_key), Some(ProofHex(wrap_proof))) => Ok(Some(SignedWrapKey {
            wrap_key,
            wrap_proof,
        })),
        _ => Err(UnpairedWrapKey),
    }
}

impl TryFrom<RawCreateLibrary> for CreateLibrary {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawCreateLibrary) -> Result<CreateLibrary, UnpairedWrapKey> {
        Ok(CreateLibrary {
            library: raw.library,
            device_key: raw.device_key,
            recovery_key: raw.recovery_key,
            proof: raw.proof,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
            member_proof: raw.member_proof,
            entitlement: raw.entitlement,
        })
    }
}

impl TryFrom<RawAddDevice> for AddDevice {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawAddDevice) -> Result<AddDevice, UnpairedWrapKey> {
        Ok(AddDevice {
            device_key: raw.device_key,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
            member_proof: raw.member_proof,
        })
    }
}

impl TryFrom<RawDeviceRecord> for DeviceRecord {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawDeviceRecord) -> Result<DeviceRecord, UnpairedWrapKey> {
        Ok(DeviceRecord {
            public_key: raw.public_key,
            recovery: raw.recovery,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
            member_proof: raw.member_proof,
            removed: raw.removed.unwrap_or(false),
            last_seen: raw.last_seen,
        })
    }
}

/// Every key a library knows, in the order they were added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Devices {
    pub devices: Vec<DeviceRecord>,
}

impl Devices {
    /// The devices a server waits for before deleting a retired blob: every key that is neither
    /// removed nor the recovery key.
    pub fn settling(&self) -> impl Iterator<Item = &DeviceRecord> {
        self.devices.iter().filter(|d| !d.removed && !d.recovery)
    }

    /// Applies a [`RemoveDevice`]: marks `device` removed. The recovery key is
    /// [`ErrorCode::Forbidden`]; an id not listed, or a device already removed, changes nothing.
    pub fn remove(&mut self, device: DeviceId) -> Result<(), ErrorCode> {
        match self
            .devices
            .iter_mut()
            .find(|d| DeviceId::of(&d.public_key) == device)
        {
            Some(d) if d.recovery => Err(ErrorCode::Forbidden),
            Some(d) => {
                d.removed = true;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

/// The response to an appended envelope: the sequence number the server gave it. Sequence numbers
/// start at 1 and increase by one per library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
    pub seq: u64,
}

/// Query of a batch read: envelopes with sequence numbers greater than `after`, oldest first, at
/// most `limit` of them (at least 1). Sent as [`ReadBatches::to_query`] and read with
/// [`ReadBatches::parse_query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadBatches {
    pub after: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One stored envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBatch {
    pub seq: u64,
    pub envelope: Vec<u8>,
}

/// A page of envelopes in sequence order. `more` says the server holds later ones.
///
/// Bytes: format, flags (bit 0 is `more`), a LEB128 count, then per batch a big-endian u64
/// sequence number and the envelope as LEB128 length then bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchPage {
    pub batches: Vec<StoredBatch>,
    pub more: bool,
}

impl BatchPage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![PAGE_FORMAT, if self.more { PAGE_MORE } else { 0 }];
        put_varint(&mut out, self.batches.len() as u64);
        for batch in &self.batches {
            out.extend_from_slice(&batch.seq.to_be_bytes());
            put_blob(&mut out, &batch.envelope);
        }
        out
    }

    /// Fails on anything `encode` would not write. Allocation is bounded by a constant multiple of
    /// the input's length, because every count and length is checked against the remaining input
    /// first.
    pub fn decode(bytes: &[u8]) -> Result<BatchPage, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != PAGE_FORMAT {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let flags = r.u8()?;
        if flags & !PAGE_MORE != 0 {
            return Err(DecodeError::UnsupportedFlags(flags));
        }
        let count = r.len(8 + 1)?;
        let mut batches = Vec::with_capacity(count);
        for _ in 0..count {
            let seq = r.u64()?;
            let envelope = r.blob()?.to_vec();
            batches.push(StoredBatch { seq, envelope });
        }
        r.finish()?;
        Ok(BatchPage {
            batches,
            more: flags & PAGE_MORE != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn create_library_proves_the_device_key_and_survives_json() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let recovery = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let request = CreateLibrary::new(LibraryId([3; 16]), &device, recovery);
        assert!(request.verify());
        let text = serde_json::to_string(&request).unwrap();
        let back: CreateLibrary = serde_json::from_str(&text).unwrap();
        assert_eq!(back, request);
        assert!(back.verify());

        let mut forged = request.clone();
        forged.library = LibraryId([4; 16]);
        assert!(!forged.verify());
        let mut swapped = request.clone();
        swapped.recovery_key = device.verifying_key();
        assert!(!swapped.verify());

        let mut entitled = request.clone();
        entitled.entitlement = Some("token".into());
        assert!(entitled.verify());
        assert!(!request.verify_wrap());
    }

    #[test]
    fn a_wrap_key_is_signed_by_its_device_and_leaves_the_proof_alone() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let recovery = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let library = LibraryId([3; 16]);
        let request = CreateLibrary::with_wrap_key(library, &device, recovery, WrapKey([5; 32]));
        assert!(request.verify() && request.verify_wrap());
        assert_eq!(
            request.proof,
            CreateLibrary::new(library, &device, recovery).proof
        );
        let back: CreateLibrary =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back, request);

        let wrap = request.wrap.unwrap();
        let mut replaced = wrap;
        replaced.wrap_key = WrapKey([6; 32]);
        assert!(!replaced.verify(library, &device.verifying_key()));
        assert!(!wrap.verify(LibraryId([4; 16]), &device.verifying_key()));
        assert!(!wrap.verify(library, &recovery));

        let add = AddDevice::with_wrap_key(library, &device, WrapKey([5; 32]));
        assert!(add.verify_wrap(library) && !add.verify_wrap(LibraryId([4; 16])));
        assert!(!AddDevice::new(device.verifying_key()).verify_wrap(library));
        let record = DeviceRecord {
            public_key: device.verifying_key(),
            recovery: false,
            wrap: add.wrap,
            member_proof: None,
            removed: false,
            last_seen: None,
        };
        assert!(record.verify_wrap(library));
        let impostor = DeviceRecord {
            public_key: recovery,
            ..record
        };
        assert!(!impostor.verify_wrap(library));
    }

    #[test]
    fn a_record_shows_whether_its_member_proof_checks() {
        let keys = LibraryKeys::new(LibraryId([3; 16]), [7; 32]);
        let device = SigningKey::from_bytes(&[1; 32]);
        let add = AddDevice::with_wrap_key(keys.id, &device, WrapKey([5; 32]))
            .with_member_proof(&keys)
            .unwrap();
        let record = DeviceRecord {
            public_key: device.verifying_key(),
            recovery: false,
            wrap: add.wrap,
            member_proof: add.member_proof.map(ListedMemberProof::from),
            removed: false,
            last_seen: None,
        };
        assert_eq!(record.check_member_proof(&keys), MemberCheck::Valid);
        let wrong_secret = LibraryKeys::new(keys.id, [8; 32]);
        assert_eq!(
            record.check_member_proof(&wrong_secret),
            MemberCheck::Invalid
        );

        let mut tampered = record.clone();
        tampered.wrap.as_mut().unwrap().wrap_key = WrapKey([6; 32]);
        assert_eq!(tampered.check_member_proof(&keys), MemberCheck::Invalid);
        let mut dropped = record.clone();
        dropped.wrap = None;
        assert_eq!(dropped.check_member_proof(&keys), MemberCheck::Invalid);
        let moved = DeviceRecord {
            public_key: SigningKey::from_bytes(&[2; 32]).verifying_key(),
            ..record.clone()
        };
        assert_eq!(moved.check_member_proof(&keys), MemberCheck::Invalid);
        let bare = DeviceRecord {
            member_proof: None,
            ..record.clone()
        };
        assert_eq!(bare.check_member_proof(&keys), MemberCheck::Absent);

        let other = SigningKey::from_bytes(&[2; 32]);
        let other_add = AddDevice::with_wrap_key(keys.id, &other, WrapKey([5; 32]))
            .with_member_proof(&keys)
            .unwrap();
        let elsewhere = LibraryKeys::new(LibraryId([4; 16]), [7; 32]);
        let elsewhere_add = AddDevice::with_wrap_key(elsewhere.id, &device, WrapKey([5; 32]))
            .with_member_proof(&elsewhere)
            .unwrap();
        assert_ne!(other_add.member_proof, add.member_proof);
        assert_ne!(elsewhere_add.member_proof, add.member_proof);
        assert!(add.member_proof.is_some() && add.verify_wrap(keys.id));
    }

    #[test]
    fn a_listed_proof_that_is_not_hex_checks_as_invalid_and_spares_the_list() {
        let keys = LibraryKeys::new(LibraryId([3; 16]), [7; 32]);
        let device = SigningKey::from_bytes(&[1; 32]);
        let key_hex = crate::hex::encode(device.verifying_key().as_bytes());
        let good = AddDevice::new(device.verifying_key())
            .with_member_proof(&keys)
            .unwrap();
        let good_hex = good.member_proof.unwrap().to_hex();
        for (proof, expected) in [
            (json!("zz"), MemberCheck::Invalid),
            (json!(good_hex[..62]), MemberCheck::Invalid),
            (json!(7), MemberCheck::Invalid),
            (json!({"proof": good_hex}), MemberCheck::Invalid),
            (json!(null), MemberCheck::Absent),
            (json!(good_hex), MemberCheck::Valid),
        ] {
            let list: Devices = serde_json::from_value(json!({"devices": [
                {"public_key": key_hex, "recovery": false, "member_proof": proof},
                {"public_key": key_hex, "recovery": true},
            ]}))
            .unwrap();
            assert_eq!(
                list.devices[0].check_member_proof(&keys),
                expected,
                "{proof}"
            );
            let again = serde_json::to_value(&list.devices[0]).unwrap();
            let back: DeviceRecord = serde_json::from_value(again).unwrap();
            assert_eq!(back, list.devices[0]);
            if expected == MemberCheck::Invalid {
                assert_eq!(back.member_proof, Some(ListedMemberProof::Unreadable));
                let request = json!({"device_key": key_hex, "member_proof": proof});
                assert!(serde_json::from_value::<AddDevice>(request).is_err());
            }
        }
    }

    #[test]
    fn a_proof_from_another_librarys_keys_is_refused() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let elsewhere = LibraryKeys::new(LibraryId([4; 16]), [7; 32]);
        let create = CreateLibrary::new(LibraryId([3; 16]), &device, device.verifying_key());
        assert_eq!(create.with_member_proof(&elsewhere), Err(OtherLibrary));
        let add = AddDevice::with_wrap_key(LibraryId([3; 16]), &device, WrapKey([5; 32]));
        assert_eq!(add.with_member_proof(&elsewhere), Err(OtherLibrary));
        let bare = AddDevice::new(device.verifying_key());
        assert!(bare.with_member_proof(&elsewhere).is_ok());
    }

    #[test]
    fn a_wrap_key_without_its_proof_is_refused() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let key_hex = crate::hex::encode(device.verifying_key().as_bytes());
        let add = AddDevice::with_wrap_key(LibraryId([3; 16]), &device, WrapKey([5; 32]));
        let full = serde_json::to_value(&add).unwrap();
        let proof = full["wrap_proof"].clone();
        let wrap_key = "05".repeat(32);
        for body in [
            json!({"device_key": key_hex, "wrap_key": wrap_key}),
            json!({"device_key": key_hex, "wrap_proof": proof}),
        ] {
            assert!(serde_json::from_value::<AddDevice>(body.clone()).is_err());
            let mut record = body.clone();
            record["public_key"] = record["device_key"].take();
            record["recovery"] = json!(false);
            assert!(serde_json::from_value::<DeviceRecord>(record).is_err());
        }
        let request = CreateLibrary::with_wrap_key(
            LibraryId([3; 16]),
            &device,
            device.verifying_key(),
            WrapKey([5; 32]),
        );
        let mut half = serde_json::to_value(&request).unwrap();
        half.as_object_mut().unwrap().remove("wrap_proof");
        assert!(serde_json::from_value::<CreateLibrary>(half).is_err());
        assert_eq!(serde_json::from_value::<AddDevice>(full).unwrap(), add);
    }

    #[test]
    fn json_shapes_are_plain() {
        let caps = Capabilities {
            protocol_version: 1,
            blobs: false,
            notifications: true,
            skew_secs: 300,
            max_batch_bytes: Some(10),
            max_library_bytes: None,
            max_devices: None,
            max_page_batches: Some(256),
            max_page_bytes: None,
            max_blob_bytes: None,
            max_query_ids: None,
            blob_wait_days: None,
            blob_incomplete_days: None,
            device_removal: false,
        };
        assert_eq!(
            serde_json::to_value(&caps).unwrap(),
            json!({
                "protocol_version": 1,
                "blobs": false,
                "notifications": true,
                "skew_secs": 300,
                "max_batch_bytes": 10,
                "max_page_batches": 256,
            })
        );
        let key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let add = AddDevice::new(key);
        assert_eq!(
            serde_json::to_value(&add).unwrap(),
            json!({"device_key": crate::hex::encode(key.as_bytes())})
        );
        let read: ReadBatches = serde_json::from_value(json!({"after": 7})).unwrap();
        assert_eq!(
            read,
            ReadBatches {
                after: 7,
                limit: None
            }
        );
        assert!(serde_json::from_value::<AddDevice>(json!({"device_key": "00"})).is_err());
    }

    #[test]
    fn the_new_fields_are_plain_and_optional() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let key = device.verifying_key();
        let key_hex = crate::hex::encode(key.as_bytes());
        let wrap = SignedWrapKey::new(LibraryId([3; 16]), &device, WrapKey([2; 32]));
        let record = DeviceRecord {
            public_key: key,
            recovery: false,
            wrap: Some(wrap),
            member_proof: None,
            removed: true,
            last_seen: Some(1_700_000_000),
        };
        let text = serde_json::to_string(&record).unwrap();
        assert_eq!(
            text,
            format!(
                r#"{{"public_key":"{key_hex}","recovery":false,"wrap_key":"{}","wrap_proof":"{}","removed":true,"last_seen":1700000000}}"#,
                "02".repeat(32),
                crate::hex::encode(&wrap.wrap_proof.to_bytes()),
            )
        );
        assert_eq!(serde_json::from_str::<DeviceRecord>(&text).unwrap(), record);
        let old: DeviceRecord =
            serde_json::from_value(json!({"public_key": key_hex, "recovery": true})).unwrap();
        assert_eq!((old.wrap, old.removed, old.last_seen), (None, false, None));

        let caps: Capabilities = serde_json::from_value(json!({
            "protocol_version": 1,
            "blobs": true,
            "notifications": false,
            "skew_secs": 300,
            "max_blob_bytes": 1,
            "max_query_ids": 3,
            "blob_wait_days": 7,
            "blob_incomplete_days": 4,
            "device_removal": true,
            "from_the_future": [1],
        }))
        .unwrap();
        assert_eq!(
            (
                caps.max_blob_bytes,
                caps.max_query_ids,
                caps.blob_wait_days,
                caps.blob_incomplete_days,
                caps.device_removal
            ),
            (Some(1), Some(3), Some(7), Some(4), true)
        );
        assert!(
            serde_json::from_value::<AddDevice>(json!({"device_key": key_hex, "wrap_key": "ab", "wrap_proof": crate::hex::encode(&wrap.wrap_proof.to_bytes())}))
                .is_err()
        );
        let mut create = CreateLibrary::new(LibraryId([3; 16]), &device, key);
        create.entitlement = Some("token".into());
        let value = serde_json::to_value(&create).unwrap();
        assert_eq!(value["entitlement"], json!("token"));
        assert!(value.get("wrap_key").is_none() && value.get("wrap_proof").is_none());
    }

    #[test]
    fn removal_spares_the_recovery_key_and_ignores_strangers() {
        let record = |seed: u8, recovery: bool| DeviceRecord {
            public_key: SigningKey::from_bytes(&[seed; 32]).verifying_key(),
            recovery,
            wrap: None,
            member_proof: None,
            removed: false,
            last_seen: None,
        };
        let mut devices = Devices {
            devices: vec![record(1, false), record(2, true), record(3, false)],
        };
        let id = |seed: u8| DeviceId::of(&SigningKey::from_bytes(&[seed; 32]).verifying_key());
        assert_eq!(devices.settling().count(), 2);
        let before = devices.clone();
        assert_eq!(devices.remove(id(2)), Err(ErrorCode::Forbidden));
        assert_eq!(devices.remove(id(9)), Ok(()));
        assert_eq!(devices, before);
        assert_eq!(devices.remove(id(3)), Ok(()));
        assert_eq!(devices.remove(id(3)), Ok(()));
        assert!(devices.devices[2].removed);
        let settling: Vec<_> = devices.settling().map(|d| d.public_key).collect();
        assert_eq!(settling, [before.devices[0].public_key]);
    }

    #[test]
    fn a_null_removed_reads_as_false() {
        let key = crate::hex::encode(SigningKey::from_bytes(&[1; 32]).verifying_key().as_bytes());
        let record: DeviceRecord =
            serde_json::from_value(json!({"public_key": key, "recovery": false, "removed": null}))
                .unwrap();
        assert!(!record.removed);
    }

    #[test]
    fn paths_name_the_library_in_hex() {
        let id = LibraryId([0xab; 16]);
        assert_eq!(
            batches_path(id),
            format!("/v1/libraries/{}/batches", "ab".repeat(16))
        );
        assert_eq!(
            devices_path(id),
            format!("/v1/libraries/{}/devices", "ab".repeat(16))
        );
        assert_eq!(
            notifications_path(id),
            format!("/v1/libraries/{}/notifications", "ab".repeat(16))
        );
    }

    proptest! {
        #[test]
        fn pages_round_trip(
            raw in proptest::collection::vec((any::<u64>(), proptest::collection::vec(any::<u8>(), 0..64)), 0..8),
            more in any::<bool>(),
        ) {
            let page = BatchPage {
                batches: raw.into_iter().map(|(seq, envelope)| StoredBatch { seq, envelope }).collect(),
                more,
            };
            let bytes = page.encode();
            prop_assert_eq!(BatchPage::decode(&bytes), Ok(page));
            for cut in 0..bytes.len() {
                prop_assert!(BatchPage::decode(&bytes[..cut]).is_err());
            }
        }

        #[test]
        fn decoding_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = BatchPage::decode(&bytes);
        }
    }
}
