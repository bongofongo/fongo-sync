//! Fixed inputs with their exact outputs. A change here breaks every stored batch and every
//! deployed peer, so it needs a new format version, not an updated expectation.

use chacha20poly1305::XChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use fongo_sync_core::{Change, Gid, Hlc, Value};
use fongo_sync_protocol::api::{
    AddDevice, Appended, BatchPage, Capabilities, CreateLibrary, DeviceRecord, Devices, ErrorBody,
    ErrorCode, Notification, ReadBatches, StoredBatch,
};
use fongo_sync_protocol::{
    ContentHash, DeviceId, FILE_CHUNK_LEN, FileHasher, FileOpener, FileRef, FileSealer,
    HEADER_DEVICE, HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP, LibraryId, LibraryKeys,
    SEALED_CHUNK_LEN, SealedChunk, SigningKey, WrapKey, encode_batch, padme, seal, sign_request,
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn keys() -> LibraryKeys {
    LibraryKeys::new(LibraryId([1; 16]), [2; 32])
}

fn device() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn changes() -> Vec<Change> {
    let clock = Hlc {
        wall_ms: 1_700_000_000_000,
        counter: 2,
        node: Gid([7; 16]),
    };
    [
        Value::Null,
        Value::Int(-1),
        Value::Real(0.5),
        Value::Text("hello".into()),
        Value::Blob(vec![0, 255]),
        Value::Ref(Gid([9; 16])),
    ]
    .into_iter()
    .map(|value| Change {
        collection: "things".into(),
        gid: Gid([4; 16]),
        field: "title".into(),
        value,
        clock,
    })
    .collect()
}

#[test]
fn the_wire_formats_do_not_drift() {
    let batch = encode_batch(&changes());
    let envelope = seal(&changes(), &keys(), 3, &device(), [5; 24]);
    let request = sign_request(
        &device(),
        "POST",
        "/v1/libraries/x/batches",
        b"body",
        1_700_000_000,
        [6; 16],
    );
    let actual = [
        hex(&Sha256::digest(&batch)),
        hex(&Sha256::digest(&envelope)),
        DeviceId::of(&device().verifying_key()).to_hex(),
        hex(&keys().recovery_signing_key().verifying_key().to_bytes()),
        hex(&request.signature.to_bytes()),
        keys().recovery_code(),
        hex(&Notification { seq: 258 }.encode()),
        ReadBatches {
            after: 7,
            limit: Some(100),
        }
        .target(keys().id),
    ];
    let expected = [
        "d9d01f5bc57e59bbb09610f48d56c58720278eec709066af48bcd1cbc9951d63",
        "bf270a22258467f3e155269816704a8f61474226b6decbcabb820df2cab3e7df",
        "b62e867fa2f33afe62d5d6b1642e1621",
        "9f6af73fffee10125a8b522b998d162a26d8916e71ed9682cd189744797c040c",
        "c00ce7d98565c00019243c37dfc032cd88684527005030e6e993dab6a5128d12270659ad04db71cecf55f2359dbda355e218feb83b529beeb6bc1c60d78be305",
        "040G2-08104-0G208-1040G-20810-40G40-G2081-040G2-08104-0G208-1040G-20810-40G20-81040-G2081-040MW-ZXCD0",
        "010000000000000102",
        "/v1/libraries/01010101010101010101010101010101/batches?after=7&limit=100",
    ];
    assert_eq!(actual, expected);
}

#[test]
fn a_batch_is_laid_out_as_documented() {
    let bytes = encode_batch(&changes()[..1]);
    let expected = [
        "0100",
        "01",
        "067468696e6773",
        &"04".repeat(16),
        "057469746c65",
        "00",
        "0000018bcfe56800",
        "00000002",
        &"07".repeat(16),
    ]
    .concat();
    assert_eq!(hex(&bytes), expected);
}

#[test]
fn the_existing_json_messages_do_not_drift() {
    let device = SigningKey::from_bytes(&[3; 32]);
    let recovery = SigningKey::from_bytes(&[4; 32]).verifying_key();
    let caps = Capabilities {
        protocol_version: 1,
        blobs: false,
        notifications: true,
        skew_secs: 300,
        max_batch_bytes: Some(1),
        max_library_bytes: Some(2),
        max_devices: Some(3),
        max_page_batches: Some(4),
        max_page_bytes: Some(5),
        max_blob_bytes: None,
        max_blobs_bytes: None,
        max_query_ids: None,
        blob_wait_days: None,
        blob_incomplete_days: None,
        device_removal: false,
    };
    let caps_least = Capabilities {
        notifications: false,
        max_batch_bytes: None,
        max_library_bytes: None,
        max_devices: None,
        max_page_batches: None,
        max_page_bytes: None,
        ..caps.clone()
    };
    let record = |public_key, recovery| DeviceRecord {
        public_key,
        recovery,
        wrap_key: None,
        removed: false,
        last_seen: None,
    };
    let devices = Devices {
        devices: vec![
            record(device.verifying_key(), false),
            record(recovery, true),
        ],
    };
    let written = [
        serde_json::to_string(&caps).unwrap(),
        serde_json::to_string(&caps_least).unwrap(),
        serde_json::to_string(&CreateLibrary::new(LibraryId([1; 16]), &device, recovery)).unwrap(),
        serde_json::to_string(&AddDevice {
            device_key: recovery,
            wrap_key: None,
        })
        .unwrap(),
        serde_json::to_string(&devices).unwrap(),
        serde_json::to_string(&Appended { seq: 9 }).unwrap(),
        serde_json::to_string(&ReadBatches {
            after: 7,
            limit: Some(100),
        })
        .unwrap(),
        serde_json::to_string(&ReadBatches {
            after: 7,
            limit: None,
        })
        .unwrap(),
        serde_json::to_string(&ErrorBody::new(ErrorCode::NotFound)).unwrap(),
    ];
    let expected = [
        r#"{"protocol_version":1,"blobs":false,"notifications":true,"skew_secs":300,"max_batch_bytes":1,"max_library_bytes":2,"max_devices":3,"max_page_batches":4,"max_page_bytes":5}"#,
        r#"{"protocol_version":1,"blobs":false,"notifications":false,"skew_secs":300}"#,
        r#"{"library":"01010101010101010101010101010101","device_key":"ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1","recovery_key":"ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c","proof":"d45269cc2a61d30863f364c738afb608d66c66bbb2f14ff0016dd82909c5b78666725ac138b3314ee28eb2a7be93c252242fcc7f3d554948110b1e6c156b4c0d"}"#,
        r#"{"device_key":"ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c"}"#,
        r#"{"devices":[{"public_key":"ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1","recovery":false},{"public_key":"ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c","recovery":true}]}"#,
        r#"{"seq":9}"#,
        r#"{"after":7,"limit":100}"#,
        r#"{"after":7}"#,
        r#"{"code":"not_found","message":"no such endpoint"}"#,
    ];
    assert_eq!(written, expected);

    fn reads_back<T: serde::Serialize + serde::de::DeserializeOwned>(text: &str) {
        let value: T = serde_json::from_str(text).unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), text);
    }
    reads_back::<Capabilities>(expected[0]);
    reads_back::<Capabilities>(expected[1]);
    reads_back::<CreateLibrary>(expected[2]);
    reads_back::<AddDevice>(expected[3]);
    reads_back::<Devices>(expected[4]);
    reads_back::<Appended>(expected[5]);
    reads_back::<ReadBatches>(expected[6]);
    reads_back::<ReadBatches>(expected[7]);
    reads_back::<ErrorBody>(expected[8]);
    let created: CreateLibrary = serde_json::from_str(expected[2]).unwrap();
    assert!(created.verify());
}

#[test]
fn the_create_library_proof_signs_the_documented_bytes() {
    let device = SigningKey::from_bytes(&[3; 32]);
    let recovery = SigningKey::from_bytes(&[4; 32]).verifying_key();
    let mut message = b"fongo-sync/v1/create-library\0".to_vec();
    message.extend_from_slice(&[1; 16]);
    message.extend_from_slice(device.verifying_key().as_bytes());
    message.extend_from_slice(recovery.as_bytes());
    let plain = CreateLibrary::new(LibraryId([1; 16]), &device, recovery);
    assert!(
        device
            .verifying_key()
            .verify_strict(&message, &plain.proof)
            .is_ok()
    );

    message.extend_from_slice(b"fongo-sync/v1/create-library/wrap-key\0");
    message.extend_from_slice(&[6; 32]);
    let wrapped =
        CreateLibrary::with_wrap_key(LibraryId([1; 16]), &device, recovery, WrapKey([6; 32]));
    assert!(
        device
            .verifying_key()
            .verify_strict(&message, &wrapped.proof)
            .is_ok()
    );
    assert_eq!(
        serde_json::to_string(&wrapped).unwrap(),
        format!(
            r#"{{"library":"{}","device_key":"{}","recovery_key":"{}","proof":"{}","wrap_key":"{}"}}"#,
            "01".repeat(16),
            hex(device.verifying_key().as_bytes()),
            hex(recovery.as_bytes()),
            "d4cb82ee441d47724a08ee550fe42e641883d6cfe79baff9733ed55d0eeeb52dedbf2f45cba44b9050ff4b500b6722dc836890c37e0550b0f33d6875711e3f03",
            "06".repeat(32),
        )
    );
}

#[test]
fn a_batch_page_is_laid_out_as_documented() {
    let page = BatchPage {
        batches: vec![
            StoredBatch {
                seq: 1,
                envelope: vec![0xaa, 0xbb],
            },
            StoredBatch {
                seq: 300,
                envelope: vec![],
            },
        ],
        more: true,
    };
    let expected = [
        "01",
        "01",
        "02",
        "0000000000000001",
        "02aabb",
        "000000000000012c",
        "00",
    ]
    .concat();
    assert_eq!(hex(&page.encode()), expected);
    assert_eq!(hex(&BatchPage::default().encode()), "010000");
}

#[test]
fn error_codes_and_headers_do_not_drift() {
    let codes: Vec<(&str, u16)> = ErrorCode::ALL
        .iter()
        .map(|c| (c.as_str(), c.status()))
        .collect();
    let expected = [
        ("malformed_request", 400),
        ("malformed_envelope", 400),
        ("invalid_proof", 400),
        ("unauthenticated", 401),
        ("unknown_device", 401),
        ("bad_signature", 401),
        ("stale_request", 401),
        ("replayed_request", 401),
        ("device_removed", 401),
        ("not_entitled", 402),
        ("forbidden", 403),
        ("device_limit", 403),
        ("not_found", 404),
        ("library_not_found", 404),
        ("blob_not_found", 404),
        ("method_not_allowed", 405),
        ("library_exists", 409),
        ("blob_incomplete", 409),
        ("blob_mismatch", 409),
        ("too_large", 413),
        ("blob_too_large", 413),
        ("envelope_wrong_library", 422),
        ("envelope_wrong_device", 422),
        ("envelope_bad_signature", 422),
        ("internal", 500),
        ("library_full", 507),
        ("quota_exceeded", 507),
    ];
    assert_eq!(codes, expected);
    assert_eq!(ErrorCode::parse("blob_from_the_future"), ErrorCode::Unknown);
    assert_eq!(
        [
            HEADER_DEVICE,
            HEADER_TIMESTAMP,
            HEADER_NONCE,
            HEADER_SIGNATURE
        ],
        [
            "x-fongo-device",
            "x-fongo-timestamp",
            "x-fongo-nonce",
            "x-fongo-signature"
        ]
    );
}

#[test]
fn the_file_derivations_do_not_drift() {
    let hash = ContentHash::of(b"Middlemarch");
    let root = keys().file_key_root();
    let actual = [
        hex(keys().file_name_key().as_bytes()),
        hex(root.as_bytes()),
        hex(root.file_key(&hash).as_bytes()),
        keys().file_name_key().blob_id(&hash).to_hex(),
        hex(&FileRef { hash, size: 11 }.encode()),
        hex(&FileRef { hash, size: 300 }.encode()),
    ];
    let expected = [
        "9fdfae35ce566579079122e04a5553add66fedf064e24c624243dca881f8c240",
        "7a9c53f76175e4c5a2ebdd15f9f163b75cea34858db07710d6885d1a36c7255c",
        "e1e6f179e8bc6c5f3a278cb2b014c8aeeb2299bc7d76dd0be3634bdff48fbde9",
        "2a867798770e03da32393cfe57288c14a2c4e596425fd1bf450eb0bd69ac3831",
        "0148020a629ef74272ca9e97f2854f146c3cf1311e36a3301b17cdd960daf7a2960b",
        "0148020a629ef74272ca9e97f2854f146c3cf1311e36a3301b17cdd960daf7a296ac02",
    ];
    assert_eq!(actual, expected);

    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(root.as_bytes()).unwrap();
    mac.update(b"fongo-sync/v1/file-key");
    mac.update(&hash.0);
    mac.update(&[1]);
    assert_eq!(
        root.file_key(&hash).as_bytes()[..],
        mac.finalize().into_bytes()[..]
    );
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(keys().file_name_key().as_bytes()).unwrap();
    mac.update(&hash.0);
    assert_eq!(
        keys().file_name_key().blob_id(&hash).0[..],
        mac.finalize().into_bytes()[..]
    );
    let mut name_key = [0; 32];
    Hkdf::<Sha256>::new(Some(&[1; 16]), &[2; 32])
        .expand(b"fongo-sync/v1/file-name-key", &mut name_key)
        .unwrap();
    assert_eq!(&name_key, keys().file_name_key().as_bytes());
}

#[test]
fn padme_does_not_drift() {
    let table = [
        (0, Some(0)),
        (1, Some(1)),
        (9, Some(10)),
        (100, Some(104)),
        (1_000, Some(1_024)),
        (4_194_305, Some(4_325_376)),
        (10_000_000, Some(10_223_616)),
        (272_629_760, Some(276_824_064)),
        (1 << 40, Some(1 << 40)),
        ((1 << 40) + 1, Some((1 << 40) + (1 << 34))),
        (u64::MAX - (1 << 57) + 1, Some(u64::MAX - (1 << 57) + 1)),
        (u64::MAX - (1 << 57) + 2, None),
        (u64::MAX, None),
    ];
    for (n, padded) in table {
        assert_eq!(padme(n), padded, "{n}");
    }
}

/// `len` bytes of a fixed pattern, given to `each` in pieces.
fn pattern(len: u64, mut each: impl FnMut(&[u8])) {
    let mut buf = vec![0u8; 1 << 16];
    let mut at = 0u64;
    while at < len {
        let n = (len - at).min(buf.len() as u64) as usize;
        for (i, b) in buf[..n].iter_mut().enumerate() {
            *b = ((at + i as u64) % 251) as u8;
        }
        each(&buf[..n]);
        at += n as u64;
    }
}

/// Seals the pattern of `len` bytes; returns its reference, each stored chunk's length and
/// SHA-256 of the whole stored stream, and opens every chunk back.
fn sealed_pattern(len: u64) -> (FileRef, Vec<usize>, String) {
    let mut hasher = FileHasher::new();
    pattern(len, |piece| hasher.update(piece));
    let file = hasher.finish();
    let mut sealer = FileSealer::new(&keys(), file).unwrap();
    let opener = FileOpener::new(&keys(), file).unwrap();
    let mut stream = Sha256::new();
    let mut lens = Vec::new();
    let mut check = FileHasher::new();
    let mut take = |chunk: SealedChunk, lens: &mut Vec<usize>| {
        assert_eq!(chunk.index, lens.len() as u64);
        stream.update(&chunk.bytes);
        lens.push(chunk.bytes.len());
        check.update(&opener.open(chunk.index, &chunk.bytes).unwrap());
    };
    pattern(len, |mut piece| {
        while !piece.is_empty() {
            let took = sealer.feed(piece).unwrap();
            piece = &piece[took..];
            while let Some(chunk) = sealer.next_chunk().unwrap() {
                take(chunk, &mut lens);
            }
        }
    });
    while let Some(chunk) = sealer.next_chunk().unwrap() {
        take(chunk, &mut lens);
    }
    sealer.finish().unwrap();
    check.verify(&file).unwrap();
    (file, lens, hex(&stream.finalize()))
}

#[test]
fn sealed_files_are_laid_out_as_documented() {
    for len in [0, 1, 9, FILE_CHUNK_LEN + 1] {
        let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let file = FileRef::of(&bytes);
        let mut sealer = FileSealer::new(&keys(), file).unwrap();
        let mut chunks = Vec::new();
        let mut rest = &bytes[..];
        loop {
            let took = sealer.feed(rest).unwrap();
            rest = &rest[took..];
            while let Some(chunk) = sealer.next_chunk().unwrap() {
                chunks.push(chunk.bytes);
            }
            if rest.is_empty() {
                break;
            }
        }
        sealer.finish().unwrap();

        let padded = padme(len as u64).unwrap() as usize;
        let mut stream = bytes.clone();
        stream.resize(padded, 0);
        let count = padded.div_ceil(FILE_CHUNK_LEN).max(1);
        assert_eq!(chunks.len(), count);

        let id = keys().file_name_key().blob_id(&file.hash);
        let key = keys().file_key_root().file_key(&file.hash);
        let cipher = XChaCha20Poly1305::new(key.as_bytes().into());
        let mut aad = b"fongo-sync/v1/file\0".to_vec();
        aad.extend_from_slice(&id.0);
        aad.extend_from_slice(&(count as u64).to_be_bytes());
        aad.extend_from_slice(&(len as u64).to_be_bytes());
        for (i, chunk) in chunks.iter().enumerate() {
            let mut nonce = [0u8; 24];
            nonce[15..23].copy_from_slice(&(i as u64).to_be_bytes());
            nonce[23] = u8::from(i + 1 == count);
            let plain = cipher
                .decrypt(
                    &nonce.into(),
                    Payload {
                        msg: chunk,
                        aad: &aad,
                    },
                )
                .unwrap();
            let end = ((i + 1) * FILE_CHUNK_LEN).min(padded);
            assert!(plain == stream[i * FILE_CHUNK_LEN..end], "{len} {i}");
        }
    }
}

#[test]
fn sealed_files_do_not_drift() {
    let empty = FileRef::of(b"");
    let mut sealer = FileSealer::new(&keys(), empty).unwrap();
    let empty_chunk = sealer.next_chunk().unwrap().unwrap();
    let one = FileRef::of(b"M");
    let mut sealer = FileSealer::new(&keys(), one).unwrap();
    sealer.feed(b"M").unwrap();
    let one_chunk = sealer.next_chunk().unwrap().unwrap();
    assert_eq!(
        [hex(&empty_chunk.bytes), hex(&one_chunk.bytes)],
        [
            "e9f6bd88d5cc5794ac528884eac98d8f",
            "429801e7e8f103e960ad8a8832f0855404"
        ]
    );

    let chunk = FILE_CHUNK_LEN as u64;
    let sealed = [chunk, chunk + 1, 2 * chunk, (1 << 28) + chunk].map(|len| {
        let (file, lens, digest) = sealed_pattern(len);
        (
            file.size,
            hex(&file.hash.0),
            lens.len(),
            *lens.last().unwrap(),
            digest,
        )
    });
    let expected: [(u64, &str, usize, usize, &str); 4] = [
        (
            chunk,
            "a117210941a0b00dcb2d8577e680d84b6fa0eaf760d2afc654c953b9859d54fa",
            1,
            SEALED_CHUNK_LEN,
            "9550abf52bcd1ba85bec1551e9d4b528b01df34d3b87472f1272161977a7555f",
        ),
        (
            chunk + 1,
            "f4711f6bc42a8dc6520e30e306743855a82c7bdecd6e7d3edfb04b6f4524f7bd",
            2,
            (1 << 17) + 16,
            "58643820bde31fecfa28d328167649506685f3d23d49afe47dc80b9d2b69c50e",
        ),
        (
            2 * chunk,
            "bdf23837181f5808331800c1ae2b4f7d7a839536b10d58491471c50dde23833a",
            2,
            SEALED_CHUNK_LEN,
            "1c5b81ad8859b7f2fa7cdebc5f34d50344e1273ce29bf54ca25f8a97bd8cad60",
        ),
        (
            (1 << 28) + chunk,
            "134a54cfa16d26cea723c333ee7b2f17835dbe255de356bc26c1c162e44365b8",
            66,
            SEALED_CHUNK_LEN,
            "c11e3bd371a254deade93c9035e76f540516aa4be268b3261f3e94996187ccf8",
        ),
    ];
    let actual = sealed
        .iter()
        .map(|(size, hash, count, last, digest)| {
            (*size, hash.as_str(), *count, *last, digest.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
