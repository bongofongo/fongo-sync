//! The contract between devices and a server, pinned beside `pinned.rs`: the error body, the header
//! values, the paths and their methods, every JSON field name of every message, the binary bodies
//! and the close codes. A change here breaks deployed peers, so it needs a new protocol version,
//! not an updated expectation.

use fongo_sync_protocol::api::{
    AddDevice, Appended, BATCHES_CLAIMING_PATH, BATCHES_PATH, BINARY_CONTENT_TYPE, BLOB_CHUNK_PATH,
    BLOB_COMPLETE_PATH, BLOB_PATH, BLOB_WHOLE_PATH, BLOBS_CLAIM_PATH, BLOBS_QUERY_PATH,
    BLOBS_RETIRE_PATH, BatchPage, BlobIds, BlobInfo, BlobState, CAPABILITIES_PATH,
    CLOSE_DEVICE_REMOVED, CLOSE_SUPERSEDED, Capabilities, ClaimingAppend, CompleteBlob,
    CompleteBlobs, CreateLibrary, DEVICES_PATH, DeviceRecord, Devices, ENTITLEMENT_PATH,
    Entitlement, ErrorBody, ErrorCode, HaveChunk, LIBRARIES_PATH, LibraryStatus, MissingBlobs,
    NOTIFICATION_FORMAT, NOTIFICATIONS_PATH, Notification, REMOVE_DEVICE_PATH, ReadBatches,
    RemoveDevice, RetireBlobs, Route, SETTLED_PATH, STATUS_PATH, Settled, StoredBatch,
};
use fongo_sync_protocol::{
    BlobId, DeviceId, LibraryId, LibraryKeys, PROTOCOL_VERSION, SigningKey, WrapKey, seal,
    sign_request, sign_request_digest, verify_request, verify_request_digest,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn device() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn recovery() -> SigningKey {
    SigningKey::from_bytes(&[4; 32])
}

const DEVICE_KEY: &str = "ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1";
const RECOVERY_KEY: &str = "ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c";
const LIBRARY: LibraryId = LibraryId([1; 16]);

/// Writes `value` as JSON, checks it against `expected`, and checks that `expected` reads back to
/// `value` and is written again unchanged.
fn pinned<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
    value: &T,
    expected: &str,
) {
    assert_eq!(serde_json::to_string(value).unwrap(), expected);
    let back: T = serde_json::from_str(expected).unwrap();
    assert_eq!(&back, value, "{expected}");
    assert_eq!(serde_json::to_string(&back).unwrap(), expected);
}

/// The codes and their statuses are pinned in `pinned.rs`; this adds the body's keys and the
/// reading of a code this version does not know.
#[test]
fn every_error_body_is_a_code_and_a_message() {
    for &code in ErrorCode::ALL {
        assert_eq!(ErrorCode::parse(code.as_str()), code);
        let body = serde_json::to_value(ErrorBody::new(code)).unwrap();
        assert_eq!(body["code"], code.as_str());
        let keys: Vec<&str> = body
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, ["code", "message"]);
    }
    assert_eq!(
        (ErrorCode::Unknown.as_str(), ErrorCode::Unknown.status()),
        ("unknown", 500)
    );
    pinned(
        &ErrorBody {
            code: ErrorCode::BlobMismatch,
            message: "x".into(),
        },
        r#"{"code":"blob_mismatch","message":"x"}"#,
    );
}

#[test]
fn paths_methods_and_constants_are_fixed() {
    assert_eq!(
        [
            CAPABILITIES_PATH,
            LIBRARIES_PATH,
            DEVICES_PATH,
            BATCHES_PATH,
            BATCHES_CLAIMING_PATH,
            NOTIFICATIONS_PATH,
            BLOB_PATH,
            BLOB_CHUNK_PATH,
            BLOB_COMPLETE_PATH,
            BLOB_WHOLE_PATH,
            BLOBS_QUERY_PATH,
            BLOBS_CLAIM_PATH,
            BLOBS_RETIRE_PATH,
            SETTLED_PATH,
            STATUS_PATH,
            ENTITLEMENT_PATH,
            REMOVE_DEVICE_PATH,
        ],
        [
            "/v1/capabilities",
            "/v1/libraries",
            "/v1/libraries/{library}/devices",
            "/v1/libraries/{library}/batches",
            "/v1/libraries/{library}/batches/claiming",
            "/v1/libraries/{library}/notifications",
            "/v1/libraries/{library}/blobs/{id}",
            "/v1/libraries/{library}/blobs/{id}/chunks/{n}",
            "/v1/libraries/{library}/blobs/{id}/complete",
            "/v1/libraries/{library}/blobs/{id}/whole",
            "/v1/libraries/{library}/blobs/query",
            "/v1/libraries/{library}/blobs/claim",
            "/v1/libraries/{library}/blobs/retire",
            "/v1/libraries/{library}/settled",
            "/v1/libraries/{library}/status",
            "/v1/libraries/{library}/entitlement",
            "/v1/libraries/{library}/devices/remove",
        ]
    );
    let library = LibraryId([0xab; 16]);
    let blob = BlobId([0xcd; 32]);
    let methods: Vec<(String, &[&str])> = [
        Route::Capabilities,
        Route::Libraries,
        Route::Devices(library),
        Route::Batches(library),
        Route::BatchesClaiming(library),
        Route::Notifications(library),
        Route::Blob(library, blob),
        Route::BlobChunk(library, blob, 3),
        Route::BlobComplete(library, blob),
        Route::BlobWhole(library, blob),
        Route::BlobsQuery(library),
        Route::BlobsClaim(library),
        Route::BlobsRetire(library),
        Route::Settled(library),
        Route::Status(library),
        Route::Entitlement(library),
        Route::RemoveDevice(library),
    ]
    .iter()
    .map(|route| {
        (
            route
                .path()
                .replace(&library.to_hex(), "L")
                .replace(&blob.to_hex(), "B"),
            route.methods(),
        )
    })
    .collect();
    let get: &[&str] = &["GET"];
    let post: &[&str] = &["POST"];
    let both: &[&str] = &["GET", "POST"];
    let expected: Vec<(String, &[&str])> = [
        ("/v1/capabilities", get),
        ("/v1/libraries", post),
        ("/v1/libraries/L/devices", both),
        ("/v1/libraries/L/batches", both),
        ("/v1/libraries/L/batches/claiming", post),
        ("/v1/libraries/L/notifications", get),
        ("/v1/libraries/L/blobs/B", both),
        ("/v1/libraries/L/blobs/B/chunks/3", both),
        ("/v1/libraries/L/blobs/B/complete", post),
        ("/v1/libraries/L/blobs/B/whole", post),
        ("/v1/libraries/L/blobs/query", post),
        ("/v1/libraries/L/blobs/claim", post),
        ("/v1/libraries/L/blobs/retire", post),
        ("/v1/libraries/L/settled", post),
        ("/v1/libraries/L/status", get),
        ("/v1/libraries/L/entitlement", post),
        ("/v1/libraries/L/devices/remove", post),
    ]
    .into_iter()
    .map(|(path, methods)| (path.to_owned(), methods))
    .collect();
    assert_eq!(methods, expected);
    assert_eq!(BINARY_CONTENT_TYPE, "application/octet-stream");
    assert_eq!(PROTOCOL_VERSION, 1);
    assert_eq!(NOTIFICATION_FORMAT, 1);
    assert_eq!(
        hex(&Notification { seq: u64::MAX - 1 }.encode()),
        "01fffffffffffffffe"
    );
}

#[test]
fn the_close_codes_are_fixed() {
    assert_eq!((CLOSE_SUPERSEDED, CLOSE_DEVICE_REMOVED), (4000, 4001));
}

#[test]
fn a_signature_over_a_digest_is_the_signature_over_its_body() {
    let path = "/v1/libraries/x/batches";
    let by_body = sign_request(&device(), "POST", path, b"body", 1_700_000_000, [6; 16]);
    let digest: [u8; 32] = Sha256::digest(b"body").into();
    let by_digest = sign_request_digest(&device(), "POST", path, &digest, 1_700_000_000, [6; 16]);
    assert_eq!(by_digest, by_body);
    assert_eq!(
        hex(&by_digest.signature.to_bytes()),
        "c00ce7d98565c00019243c37dfc032cd88684527005030e6e993dab6a5128d12270659ad04db71cecf55f2359dbda355e218feb83b529beeb6bc1c60d78be305"
    );
    let headers = by_body
        .headers()
        .map(|(name, value)| (name.to_owned(), value));
    assert_eq!(
        headers,
        [
            ("x-fongo-device", "b62e867fa2f33afe62d5d6b1642e1621"),
            ("x-fongo-timestamp", "1700000000"),
            ("x-fongo-nonce", "06060606060606060606060606060606"),
            ("x-fongo-signature", "c00ce7d98565c00019243c37dfc032cd88684527005030e6e993dab6a5128d12270659ad04db71cecf55f2359dbda355e218feb83b529beeb6bc1c60d78be305"),
        ]
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
    );
    let key = device().verifying_key();
    let at = 1_700_000_000;
    assert_eq!(
        verify_request(&by_body, &key, "POST", path, b"body", at, 0),
        Ok(())
    );
    assert_eq!(
        verify_request_digest(&by_body, &key, "POST", path, &digest, at, 0),
        Ok(())
    );
    let mut wrong = digest;
    wrong[0] ^= 1;
    assert!(verify_request_digest(&by_body, &key, "POST", path, &wrong, at, 0).is_err());
}

#[test]
fn the_create_library_proof_signs_these_bytes() {
    let mut message = b"fongo-sync/v1/create-library\0".to_vec();
    message.extend_from_slice(&LIBRARY.0);
    message.extend_from_slice(device().verifying_key().as_bytes());
    message.extend_from_slice(recovery().verifying_key().as_bytes());
    assert_eq!(
        hex(&message),
        [
            "666f6e676f2d73796e632f76312f6372656174652d6c69627261727900",
            &"01".repeat(16),
            DEVICE_KEY,
            RECOVERY_KEY,
        ]
        .concat()
    );
    let request = CreateLibrary::new(LIBRARY, &device(), recovery().verifying_key());
    assert!(
        device()
            .verifying_key()
            .verify_strict(&message, &request.proof)
            .is_ok()
    );
    assert_eq!(hex(&request.proof.to_bytes()), PROOF);
}

const PROOF: &str = "d45269cc2a61d30863f364c738afb608d66c66bbb2f14ff0016dd82909c5b78666725ac138b3314ee28eb2a7be93c252242fcc7f3d554948110b1e6c156b4c0d";
const WRAP_PROOF: &str = "924c4bdf5afe54d7c40d93340b62f477cffa57f44f8b9d38c553a6fab65d1ad149804a24060e419a284ebbbd6068907ab4b7224378f8e1367bbd3015ec2dae07";

#[test]
fn every_capability_field_is_named() {
    let caps = Capabilities {
        protocol_version: 1,
        blobs: true,
        notifications: true,
        skew_secs: 300,
        max_batch_bytes: Some(1),
        max_library_bytes: Some(2),
        max_devices: Some(3),
        max_page_batches: Some(4),
        max_page_bytes: Some(5),
        max_blob_bytes: Some(6),
        max_query_ids: Some(7),
        blob_wait_days: Some(8),
        blob_incomplete_days: Some(9),
        device_removal: true,
    };
    pinned(
        &caps,
        r#"{"protocol_version":1,"blobs":true,"notifications":true,"skew_secs":300,"max_batch_bytes":1,"max_library_bytes":2,"max_devices":3,"max_page_batches":4,"max_page_bytes":5,"max_blob_bytes":6,"max_query_ids":7,"blob_wait_days":8,"blob_incomplete_days":9,"device_removal":true}"#,
    );
}

#[test]
fn every_device_message_field_is_named() {
    let mut create = CreateLibrary::with_wrap_key(
        LIBRARY,
        &device(),
        recovery().verifying_key(),
        WrapKey([6; 32]),
    );
    create.entitlement = Some("token".into());
    let wrap = format!(
        r#""wrap_key":"{}","wrap_proof":"{WRAP_PROOF}""#,
        "06".repeat(32)
    );
    pinned(
        &create,
        &format!(
            r#"{{"library":"{}","device_key":"{DEVICE_KEY}","recovery_key":"{RECOVERY_KEY}","proof":"{PROOF}",{wrap},"entitlement":"token"}}"#,
            "01".repeat(16)
        ),
    );
    let add = AddDevice::with_wrap_key(LIBRARY, &device(), WrapKey([6; 32]));
    pinned(&add, &format!(r#"{{"device_key":"{DEVICE_KEY}",{wrap}}}"#));
    let record = DeviceRecord {
        public_key: device().verifying_key(),
        recovery: false,
        wrap: add.wrap,
        removed: true,
        last_seen: Some(1_700_002_800),
    };
    let record_json = format!(
        r#"{{"public_key":"{DEVICE_KEY}","recovery":false,{wrap},"removed":true,"last_seen":1700002800}}"#
    );
    pinned(&record, &record_json);
    let devices = Devices {
        devices: vec![
            record,
            DeviceRecord {
                public_key: recovery().verifying_key(),
                recovery: true,
                wrap: None,
                removed: false,
                last_seen: None,
            },
        ],
    };
    pinned(
        &devices,
        &format!(
            r#"{{"devices":[{record_json},{{"public_key":"{RECOVERY_KEY}","recovery":true}}]}}"#
        ),
    );
    pinned(
        &RemoveDevice {
            device: DeviceId::of(&device().verifying_key()),
        },
        r#"{"device":"b62e867fa2f33afe62d5d6b1642e1621"}"#,
    );
}

#[test]
fn every_batch_and_blob_message_field_is_named() {
    let a = "ab".repeat(32);
    let c = "cd".repeat(32);
    let ids = vec![BlobId([0xab; 32]), BlobId([0xcd; 32])];
    pinned(&Appended { seq: 9 }, r#"{"seq":9}"#);
    pinned(
        &ReadBatches {
            after: 7,
            limit: Some(100),
        },
        r#"{"after":7,"limit":100}"#,
    );
    pinned(
        &BlobInfo {
            size: 131_112,
            chunks: 2,
        },
        r#"{"size":131112,"chunks":2}"#,
    );
    pinned(
        &BlobState {
            complete: false,
            have: vec![HaveChunk {
                n: 1,
                sha256: [0xab; 32],
            }],
        },
        &format!(r#"{{"complete":false,"have":[{{"n":1,"sha256":"{a}"}}]}}"#),
    );
    pinned(
        &CompleteBlob { digest: [0xcd; 32] },
        &format!(r#"{{"digest":"{c}"}}"#),
    );
    pinned(
        &BlobIds { ids: ids.clone() },
        &format!(r#"{{"ids":["{a}","{c}"]}}"#),
    );
    pinned(
        &CompleteBlobs {
            complete: ids.clone(),
        },
        &format!(r#"{{"complete":["{a}","{c}"]}}"#),
    );
    pinned(
        &MissingBlobs {
            missing: ids.clone(),
        },
        &format!(r#"{{"missing":["{a}","{c}"]}}"#),
    );
    pinned(
        &RetireBlobs { through: 12, ids },
        &format!(r#"{{"through":12,"ids":["{a}","{c}"]}}"#),
    );
    pinned(&Settled { seq: 11 }, r#"{"seq":11}"#);
    pinned(
        &LibraryStatus {
            entitled: false,
            used_bytes: 5,
            allowance_bytes: Some(6),
        },
        r#"{"entitled":false,"used_bytes":5,"allowance_bytes":6}"#,
    );
    pinned(
        &Entitlement {
            entitlement: "token".into(),
        },
        r#"{"entitlement":"token"}"#,
    );
}

#[test]
fn a_batch_page_has_this_shape() {
    let page = BatchPage {
        batches: vec![
            StoredBatch {
                seq: 2,
                envelope: vec![0x11; 130],
            },
            StoredBatch {
                seq: u64::MAX,
                envelope: vec![0x22],
            },
        ],
        more: false,
    };
    let expected = [
        "01",
        "00",
        "02",
        "0000000000000002",
        "8201",
        &"11".repeat(130),
        "ffffffffffffffff",
        "0122",
    ]
    .concat();
    assert_eq!(hex(&page.encode()), expected);
    assert_eq!(BatchPage::decode(&page.encode()), Ok(page));
}

#[test]
fn a_claiming_append_has_this_body() {
    let keys = LibraryKeys::new(LIBRARY, [2; 32]);
    let envelope = seal(&[], &keys, 0, &device(), [5; 24]);
    let body = ClaimingAppend {
        ids: vec![BlobId([0xab; 32]), BlobId([0xcd; 32]), BlobId([0xab; 32])],
        envelope: envelope.clone(),
    };
    let bytes = body.encode();
    let expected = [
        "03",
        &"ab".repeat(32),
        &"cd".repeat(32),
        &"ab".repeat(32),
        &envelope_hex(),
    ]
    .concat();
    assert_eq!(hex(&envelope), envelope_hex());
    assert_eq!(hex(&bytes), expected);
    assert_eq!(ClaimingAppend::decode(&bytes), Ok(body));
    let empty = ClaimingAppend {
        ids: vec![],
        envelope,
    };
    assert_eq!(hex(&empty.encode()), ["00", &envelope_hex()].concat());
    let many = ClaimingAppend {
        ids: (0..130u8).map(|i| BlobId([i; 32])).collect(),
        envelope: empty.envelope,
    };
    let bytes = many.encode();
    let ids: String = (0..130u8).map(|i| hex(&[i; 32])).collect();
    assert_eq!(hex(&bytes), ["8201", &ids, &envelope_hex()].concat());
    assert_eq!(ClaimingAppend::decode(&bytes), Ok(many));
}

/// An empty batch sealed by `device()` for `LIBRARY` at epoch 0 with nonce `[5; 24]`: format,
/// library id, device id, epoch, nonce, ciphertext length and ciphertext, signature.
fn envelope_hex() -> String {
    [
        "01",
        &"01".repeat(16),
        "b62e867fa2f33afe62d5d6b1642e1621",
        "00000000",
        &"05".repeat(24),
        "13",
        "4cc1a382f87545d954bd6bad69edb7a4121f0f",
        "2afb5341d03e07c28d3cb0201279f39f928435819de8958c51f63c8ef0c7ee92b567dc351aea07fc4e6d39904cc783d593321b5e705b87be1b3e041a8506180b",
    ]
    .concat()
}
