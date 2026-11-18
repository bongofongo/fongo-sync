//! Fixed inputs with their exact outputs. A change here breaks every stored batch and every
//! deployed peer, so it needs a new format version, not an updated expectation.

use fongo_sync_core::{Change, Gid, Hlc, Value};
use fongo_sync_protocol::{
    DeviceId, LibraryId, LibraryKeys, SigningKey, encode_batch, seal, sign_request,
};
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
    );
    let actual = [
        hex(&Sha256::digest(&batch)),
        hex(&Sha256::digest(&envelope)),
        DeviceId::of(&device().verifying_key()).to_hex(),
        hex(&keys().recovery_signing_key().verifying_key().to_bytes()),
        hex(&request.signature.to_bytes()),
        keys().recovery_code(),
    ];
    let expected = [
        "d9d01f5bc57e59bbb09610f48d56c58720278eec709066af48bcd1cbc9951d63",
        "bf270a22258467f3e155269816704a8f61474226b6decbcabb820df2cab3e7df",
        "b62e867fa2f33afe62d5d6b1642e1621",
        "9f6af73fffee10125a8b522b998d162a26d8916e71ed9682cd189744797c040c",
        "70dd12c4be795c0e9f6feb0113501114fc3ae157800a4f98060b58c7b72306c120439f58fdd6751e05607305e087954764e83f091b234fbaf9811ed36a95cf04",
        "040G2-08104-0G208-1040G-20810-40G40-G2081-040G2-08104-0G208-1040G-20810-40G20-81040-G2081-040MW-ZXCD0",
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
