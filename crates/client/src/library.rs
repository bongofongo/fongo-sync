use fongo_sync_protocol::api::{AddDevice, CreateLibrary, Devices, LIBRARIES_PATH, devices_path};
use fongo_sync_protocol::{LibraryKeys, SigningKey, VerifyingKey, generate_signing_key};

use crate::credentials::Credentials;
use crate::error::Error;
use crate::rpc::{Body, Rpc, json};
use crate::transport::{Method, Transport};

/// Makes a new library with this device as its first member and returns its credentials, epoch 0.
/// Show the user [`Credentials::recovery_code`]: it is the only way back in if every device is
/// lost.
///
/// `now` gives the current unix time in seconds, for request timestamps.
pub async fn create_library<T: Transport>(
    transport: &T,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    let library = LibraryKeys::generate().map_err(|_| Error::Randomness)?;
    let device = generate_signing_key().map_err(|_| Error::Randomness)?;
    let recovery = library.recovery_signing_key().verifying_key();
    let request = CreateLibrary::new(library.id, &device, recovery);
    let body = serde_json::to_vec(&request).expect("protocol messages serialise");
    let response = rpc
        .call(
            Method::Post,
            LIBRARIES_PATH.to_owned(),
            Body::Json(body),
            Some(&device),
        )
        .await?;
    let devices: Devices = json(&response, "the device list is not the documented JSON")?;
    if !lists(&devices, &device.verifying_key(), false) || !lists(&devices, &recovery, true) {
        return Err(Error::MalformedResponse(
            "the new library's device list lacks its device or recovery key",
        ));
    }
    Ok(Credentials::new(library, device, 0))
}

/// Joins a library from its recovery code: derives the library's keys, makes a new device key and
/// adds it, signed by the recovery key. Returns the credentials, epoch 0.
pub async fn join_library<T: Transport>(
    transport: &T,
    recovery_code: &str,
    now: impl Fn() -> u64 + Sync,
) -> Result<Credentials, Error> {
    let library = LibraryKeys::from_recovery_code(recovery_code)?;
    let rpc = Rpc {
        transport,
        now: &now,
    };
    rpc.capabilities().await?;
    let device = generate_signing_key().map_err(|_| Error::Randomness)?;
    let recovery = library.recovery_signing_key();
    post_device(&rpc, &library, &recovery, &device.verifying_key()).await?;
    Ok(Credentials::new(library, device, 0))
}

/// Adds another device's public key to the library, signed by this device. The new device also
/// needs the library's keys, which the application hands over itself; it then builds its
/// credentials with [`Credentials::new`].
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
        device_key,
    )
    .await
}

async fn post_device<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    library: &LibraryKeys,
    signer: &SigningKey,
    device_key: &VerifyingKey,
) -> Result<(), Error> {
    let body = serde_json::to_vec(&AddDevice {
        device_key: *device_key,
        wrap_key: None,
    })
    .expect("protocol messages serialise");
    let response = rpc
        .call(
            Method::Post,
            devices_path(library.id),
            Body::Json(body),
            Some(signer),
        )
        .await?;
    let devices: Devices = json(&response, "the device list is not the documented JSON")?;
    if !lists(&devices, device_key, false) {
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
        .any(|d| d.public_key == *key && d.recovery == recovery)
}
