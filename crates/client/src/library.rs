use fongo_sync_protocol::api::{
    AddDevice, CreateLibrary, Devices, ErrorCode, LIBRARIES_PATH, devices_path,
};
use fongo_sync_protocol::{LibraryKeys, SigningKey, VerifyingKey};

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
/// `now` gives the current unix time in seconds, for request timestamps.
pub async fn create_library_with<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<(), Error> {
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
    .with_member_proof(library);
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
            Ok(true) => return Ok(()),
            Ok(false) | Err(Error::Server { .. }) => return Err(exists),
            Err(error) => return Err(error),
        },
        Err(error) => return Err(error),
    };
    if !lists(&devices, &device.verifying_key(), false) || !lists(&devices, &recovery, true) {
        return Err(Error::MalformedResponse(
            "the new library's device list lacks its device or recovery key",
        ));
    }
    Ok(())
}

/// Whether the library's device list, read with `device`'s key, holds that key and `recovery` as
/// its recovery key: that is, whether an earlier create with these keys took effect.
async fn created_before<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    library: &LibraryKeys,
    device: &SigningKey,
    recovery: &VerifyingKey,
) -> Result<bool, Error> {
    let response = rpc
        .call(
            Method::Get,
            devices_path(library.id),
            Body::Empty,
            Some(device),
        )
        .await?;
    let devices: Devices = json(&response, "the device list is not the documented JSON")?;
    Ok(lists(&devices, &device.verifying_key(), false) && lists(&devices, recovery, true))
}

/// Makes credentials with [`Credentials::for_new_library`] and registers them with
/// [`create_library_with`], returning them.
///
/// The credentials exist only in the returned value, so if the call fails after the server
/// acted on it, as when the response is lost, the library is left on the server with no device
/// that can reach it, and a new call makes another. Use [`create_library_with`], storing the
/// credentials first, where that matters.
pub async fn create_library<T: Transport>(
    transport: &T,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let credentials = Credentials::for_new_library()?;
    create_library_with(transport, &credentials, now).await?;
    Ok(credentials)
}

/// Adds `credentials`' device to its library, made by [`Credentials::for_recovery_code`]: the
/// device key, its wrap key and a membership proof, signed by the library's recovery key.
///
/// Store the credentials before calling. If the call fails with no answer, call it again with the
/// same credentials: a key already listed is accepted and changes nothing, and a key the library
/// removed is restored.
pub async fn join_library_with<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<(), Error> {
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
    .with_member_proof(library);
    post_device(&rpc, library, &library.recovery_signing_key(), &request).await
}

/// Makes credentials with [`Credentials::for_recovery_code`] and adds them with
/// [`join_library_with`], returning them.
///
/// The credentials exist only in the returned value, so if the call fails after the server
/// acted on it, as when the response is lost, the library keeps a device slot whose key nobody
/// holds, and a new call takes another. Use [`join_library_with`], storing the credentials first,
/// where that matters.
pub async fn join_library<T: Transport>(
    transport: &T,
    recovery_code: &str,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let credentials = Credentials::for_recovery_code(recovery_code)?;
    join_library_with(transport, &credentials, now).await?;
    Ok(credentials)
}

/// Adds another device's public key to the library, signed by this device, with no wrap key and
/// no membership proof. The new device also needs the library's keys, which the application hands
/// over itself; it then builds its credentials with [`Credentials::new`].
pub async fn add_device<T: Transport>(
    transport: &T,
    credentials: &Credentials,
    device_key: &VerifyingKey,
    now: impl Fn() -> u64 + Sync,
) -> Result<(), Error> {
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    post_device(
        &rpc,
        credentials.library(),
        credentials.device(),
        &AddDevice::new(*device_key),
    )
    .await
}

async fn post_device<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    library: &LibraryKeys,
    signer: &SigningKey,
    request: &AddDevice,
) -> Result<(), Error> {
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
    Ok(())
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
