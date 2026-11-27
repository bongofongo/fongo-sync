use fongo_sync_protocol::api::{
    AddDevice, CreateLibrary, Devices, ErrorCode, LIBRARIES_PATH, SignedWrapKey, devices_path,
};
use fongo_sync_protocol::{
    LibraryKeys, ListedMemberProof, MemberProof, SigningKey, VerifyingKey, WrapKey,
};

use crate::credentials::Credentials;
use crate::error::Error;
use crate::rpc::{Body, Rpc, json};
use crate::transport::{Method, Transport};

/// Registers a new library on the server with `credentials` as its first device, made by
/// [`Credentials::for_new_library`]: the device key, its wrap key, a membership proof, and the
/// recovery key derived from the library secret. Show the user [`Credentials::recovery_code`]: it
/// is the only way back in if every device is lost.
///
/// Store the credentials before calling. If the call fails with no answer, as when the response
/// was lost, call it again with the same credentials: a library this device key already created
/// with this recovery key counts as created. The server refuses a request without a timestamp
/// near its clock, so a repeat must come with a current `now`.
///
/// Fails with [`Error::RegisteredOtherwise`] when the server lists the device with another wrap
/// key, or with a membership proof other than the one these credentials make; a listing without
/// either is a success, reported in [`Registered`].
///
/// `now` gives the current unix time in seconds, for request timestamps.
pub async fn create_library_with<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<Registered, Error> {
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    let library = credentials.library();
    let device = credentials.device();
    let recovery = library.recovery_signing_key().verifying_key();
    let request = match credentials.wrap_key() {
        Some(wrap) => CreateLibrary::with_wrap_key(library.id, device, recovery, wrap),
        None => CreateLibrary::new(library.id, device, recovery),
    }
    .with_member_proof(library)
    .expect("the request is made for these keys' library");
    let body = serde_json::to_vec(&request).expect("protocol messages serialise");
    let devices = match rpc
        .call(
            Method::Post,
            LIBRARIES_PATH.to_owned(),
            Body::Json(body),
            Some(device),
        )
        .await
    {
        Ok(response) => json(&response, "the device list is not the documented JSON")?,
        Err(
            exists @ Error::Server {
                code: ErrorCode::LibraryExists,
                ..
            },
        ) => match created_before(&rpc, library, device, &recovery).await {
            Ok(Some(devices)) => devices,
            Ok(None) | Err(Error::Server { .. }) => return Err(exists),
            Err(error) => return Err(error),
        },
        Err(error) => return Err(error),
    };
    if !lists(&devices, &device.verifying_key(), false) || !lists(&devices, &recovery, true) {
        return Err(Error::MalformedResponse(
            "the new library's device list lacks its device or recovery key",
        ));
    }
    registered_as(
        &devices,
        &request.device_key,
        request.wrap.as_ref(),
        request.member_proof,
    )
}

/// The library's device list, read with `device`'s key, if it holds that key and `recovery` as
/// its recovery key: that is, if an earlier create with these keys took effect.
async fn created_before<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    library: &LibraryKeys,
    device: &SigningKey,
    recovery: &VerifyingKey,
) -> Result<Option<Devices>, Error> {
    let response = rpc
        .call(
            Method::Get,
            devices_path(library.id),
            Body::Empty,
            Some(device),
        )
        .await?;
    let devices: Devices = json(&response, "the device list is not the documented JSON")?;
    let created =
        lists(&devices, &device.verifying_key(), false) && lists(&devices, recovery, true);
    Ok(created.then_some(devices))
}

/// Makes credentials with [`Credentials::for_new_library`] and registers them with
/// [`create_library_with`], returning them.
///
/// The credentials exist only in the returned value, so if the call fails after the server
/// acted on it, as when the response is lost, the library is left on the server with no device
/// that can reach it, and a new call makes another. Use [`create_library_with`], storing the
/// credentials first, where that matters. It does not report whether the server kept the wrap
/// key and membership proof ([`Registered`]). When the library was made but lists the device
/// otherwise, the error, [`Error::RegisteredOtherwise`], carries the credentials.
pub async fn create_library<T: Transport>(
    transport: &T,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let credentials = Credentials::for_new_library()?;
    match create_library_with(transport, &credentials, now).await {
        Ok(_) => Ok(credentials),
        Err(error) => Err(keeping(credentials)(error)),
    }
}

/// Adds `credentials`' device to its library, made by [`Credentials::for_recovery_code`]: the
/// device key, its wrap key and a membership proof, signed by the library's recovery key.
///
/// Store the credentials before calling. If the call fails with no answer, call it again with the
/// same credentials: a key already listed is accepted and changes nothing, and a key the library
/// removed is restored. Because of that, an application told that this device was removed
/// ([`ErrorCode::DeviceRemoved`]) must not repeat this call in a loop: on a server that follows
/// the protocol, the repeat brings the device back.
///
/// Fails with [`Error::RegisteredOtherwise`] when the key is already listed with another wrap key
/// or a membership proof other than the one these credentials make; the listing is then left as
/// it was. A listing without a wrap key or proof is a success, reported in [`Registered`].
pub async fn join_library_with<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<Registered, Error> {
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    let library = credentials.library();
    let device = credentials.device();
    let request = match credentials.wrap_key() {
        Some(wrap) => AddDevice::with_wrap_key(library.id, device, wrap),
        None => AddDevice::new(device.verifying_key()),
    }
    .with_member_proof(library)
    .expect("the wrap key is signed for these keys' library");
    post_device(&rpc, library, &library.recovery_signing_key(), &request).await
}

/// Makes credentials with [`Credentials::for_recovery_code`] and adds them with
/// [`join_library_with`], returning them.
///
/// The credentials exist only in the returned value, so if the call fails after the server
/// acted on it, as when the response is lost, the library keeps a device slot whose key nobody
/// holds, and a new call takes another. Use [`join_library_with`], storing the credentials first,
/// where that matters. It does not report whether the server kept the wrap key and membership
/// proof ([`Registered`]). When the device was added but is listed otherwise, the error,
/// [`Error::RegisteredOtherwise`], carries the credentials.
pub async fn join_library<T: Transport>(
    transport: &T,
    recovery_code: &str,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let credentials = Credentials::for_recovery_code(recovery_code)?;
    match join_library_with(transport, &credentials, now).await {
        Ok(_) => Ok(credentials),
        Err(error) => Err(keeping(credentials)(error)),
    }
}

/// What a new device hands to a device already in the library so that it can add it with
/// [`add_device`]: the new device's public key and its wrap key signed by that key for the
/// library. The new device makes it with [`Credentials::device_offer`].
///
/// It holds no secret, but it must reach the adding device over a channel the server cannot
/// alter, as the recovery code must: anyone who knows the library id can make a valid offer for a
/// key of their own, and [`add_device`] vouches for it with a valid membership proof. An
/// application that relays offers through the sync server lets the server have a key of its own
/// added, and later sent a new library secret. It travels as [`DeviceOffer::to_bytes`]; the
/// adding device checks the signature before it sends anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceOffer {
    pub device_key: VerifyingKey,
    pub wrap: SignedWrapKey,
}

impl DeviceOffer {
    /// The length of [`DeviceOffer::to_bytes`].
    pub const LEN: usize = 32 + 32 + 64;

    /// The device key, the wrap key and the wrap key's signature: 128 bytes.
    pub fn to_bytes(&self) -> [u8; DeviceOffer::LEN] {
        let mut out = [0u8; DeviceOffer::LEN];
        out[..32].copy_from_slice(self.device_key.as_bytes());
        out[32..64].copy_from_slice(&self.wrap.wrap_key.0);
        out[64..].copy_from_slice(&self.wrap.wrap_proof.to_bytes());
        out
    }

    /// Reads what [`DeviceOffer::to_bytes`] wrote; `None` unless it is 128 bytes holding a valid
    /// Ed25519 public key. The signature is checked by [`add_device`], against the library.
    pub fn from_bytes(bytes: &[u8]) -> Option<DeviceOffer> {
        let bytes: &[u8; DeviceOffer::LEN] = bytes.try_into().ok()?;
        let device_key = VerifyingKey::from_bytes(bytes[..32].try_into().ok()?).ok()?;
        let wrap_key = WrapKey(bytes[32..64].try_into().ok()?);
        let wrap_proof = fongo_sync_protocol::Signature::from_bytes(bytes[64..].try_into().ok()?);
        Some(DeviceOffer {
            device_key,
            wrap: SignedWrapKey {
                wrap_key,
                wrap_proof,
            },
        })
    }
}

/// Adds another device to the library, signed by this device: the key and wrap key `offer`
/// names, with a membership proof made from this device's secret.
///
/// The new device first gets the library's keys from the application, makes its credentials with
/// [`Credentials::for_new_device`] and stores them, then hands over
/// [`Credentials::device_offer`]; this device adds it, after which the new device syncs.
///
/// The offer must come from the new device over a channel the server cannot alter: this device
/// vouches for whatever key the offer names, as [`DeviceOffer`] explains.
///
/// Fails with [`Error::WrapKeyNotSigned`], sending nothing, unless the offer's wrap key is signed
/// by its device key for this library. A repeat after a lost response is accepted and changes
/// nothing, and a key the library removed is restored. Because of that, an application told that
/// the added device was removed must not repeat this call in a loop: on a server that follows
/// the protocol, the repeat brings the device back. [`Error::RegisteredOtherwise`] says the key
/// is listed with another wrap key or proof.
/// A listing without a wrap key or proof is a success, reported in [`Registered`].
pub async fn add_device<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    offer: &DeviceOffer,
    now: impl Fn() -> u64 + Sync,
) -> Result<Registered, Error> {
    if !offer
        .wrap
        .verify(credentials.library_id(), &offer.device_key)
    {
        return Err(Error::WrapKeyNotSigned);
    }
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    let request = AddDevice {
        device_key: offer.device_key,
        wrap: Some(offer.wrap),
        member_proof: None,
    }
    .with_member_proof(credentials.library())
    .expect("the offer's wrap key was checked against this library");
    post_device(&rpc, credentials.library(), credentials.device(), &request).await
}

async fn post_device<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    library: &LibraryKeys,
    signer: &SigningKey,
    request: &AddDevice,
) -> Result<Registered, Error> {
    let body = serde_json::to_vec(request).expect("protocol messages serialise");
    let response = rpc
        .call(
            Method::Post,
            devices_path(library.id),
            Body::Json(body),
            Some(signer),
        )
        .await?;
    let devices: Devices = json(&response, "the device list is not the documented JSON")?;
    if !lists(&devices, &request.device_key, false) {
        return Err(Error::MalformedResponse(
            "the device list lacks the device just added",
        ));
    }
    registered_as(
        &devices,
        &request.device_key,
        request.wrap.as_ref(),
        request.member_proof,
    )
}

/// A registration that succeeded, as the server's answer lists it for the device. A server
/// built before wrap keys or membership proofs drops what it does not know, and the device is
/// registered all the same.
///
/// The flags catch a server that does not store these fields, not one that lies: a server can
/// list them in its answer and not keep them. What counts is the device list read when the
/// library's secret is changed. An application should tell the person when a flag is false: the
/// device will not be kept at a change of secret, or cannot be sent one.
/// [`create_library`] and [`join_library`] do not report the flags at all.
///
/// | `wrap` | `proof_kept` | the device |
/// |---|---|---|
/// | [`WrapListing::Kept`] | true | can be sent a new secret and recognised as a member |
/// | [`WrapListing::Kept`] | false | can be sent a new secret, but is not recognised as a member |
/// | [`WrapListing::Dropped`] | false | cannot be sent a new secret: the server dropped its wrap key, and a proof over a wrap key the record lacks shows nothing |
/// | [`WrapListing::NoneSent`] | true | cannot be sent a new secret, having no wrap key, but is recognised as a member |
/// | [`WrapListing::NoneSent`] | false | cannot be sent a new secret, and is not recognised as a member |
///
/// What was dropped is never added for this key while it is listed, since adding a listed key
/// again changes nothing. The device is recognised, and can be sent a new secret, only after one
/// of two things on a server that keeps these fields: it joins again under a new device key, or
/// someone removes it and its key is then added again, since a restored key takes the wrap key and
/// proof its request carries. The second needs the device removed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Registered {
    /// The wrap key the server lists for the device, against the one it sent.
    pub wrap: WrapListing,
    /// Whether the server lists the device with the membership proof it sent, over a wrap key
    /// the record also lists. Always false when `wrap` is [`WrapListing::Dropped`].
    pub proof_kept: bool,
}

/// What became of the wrap key a registration sent, as the server's answer lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WrapListing {
    /// Listed as sent: the device can be sent a new library secret.
    Kept,
    /// Sent, but the server lists none, as one built before wrap keys does.
    Dropped,
    /// None was sent, since the credentials hold no wrap secret ([`Credentials::new`], or
    /// credentials an earlier build wrote), and none is listed.
    NoneSent,
}

/// What the listed record of `key` says of the registration: a wrap key and proof, where listed,
/// must be the ones sent.
fn registered_as(
    devices: &Devices,
    key: &VerifyingKey,
    wrap: Option<&SignedWrapKey>,
    proof: Option<MemberProof>,
) -> Result<Registered, Error> {
    let otherwise = || Error::RegisteredOtherwise { credentials: None };
    let record = devices
        .devices
        .iter()
        .find(|d| d.public_key == *key && !d.recovery && !d.removed)
        .ok_or_else(otherwise)?;
    let listing = match (record.wrap.as_ref(), wrap) {
        (None, None) => WrapListing::NoneSent,
        (None, Some(_)) => WrapListing::Dropped,
        (Some(listed), Some(sent)) if listed == sent => WrapListing::Kept,
        _ => return Err(otherwise()),
    };
    let proof_kept = match (record.member_proof, proof) {
        (None, Some(_)) => false,
        (listed, sent) if listed == sent.map(ListedMemberProof::from) => true,
        _ => return Err(otherwise()),
    };
    Ok(Registered {
        wrap: listing,
        proof_kept: proof_kept && listing != WrapListing::Dropped,
    })
}

/// Hands the credentials back with [`Error::RegisteredOtherwise`], which comes once the server
/// has acted.
fn keeping(credentials: Credentials) -> impl FnOnce(Error) -> Error {
    move |error| match error {
        Error::RegisteredOtherwise { .. } => Error::RegisteredOtherwise {
            credentials: Some(Box::new(credentials)),
        },
        error => error,
    }
}

fn lists(devices: &Devices, key: &VerifyingKey, recovery: bool) -> bool {
    devices
        .devices
        .iter()
        .any(|d| d.public_key == *key && d.recovery == recovery && !d.removed)
}

#[cfg(test)]
mod tests {
    use fongo_sync_protocol::api::DeviceRecord;

    use super::*;

    #[test]
    fn a_removed_key_is_not_listed() {
        let key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let mut record = DeviceRecord {
            public_key: key,
            recovery: false,
            wrap: None,
            member_proof: None,
            removed: false,
            last_seen: Some(1),
        };
        assert!(lists(
            &Devices {
                devices: vec![record.clone()]
            },
            &key,
            false
        ));
        assert!(!lists(
            &Devices {
                devices: vec![record.clone()]
            },
            &key,
            true
        ));
        record.removed = true;
        assert!(!lists(
            &Devices {
                devices: vec![record]
            },
            &key,
            false
        ));
    }
}
