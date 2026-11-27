use crate::wire::{DecodeError, Reader};

/// The notification format this crate writes, and the only one it reads.
pub const NOTIFICATION_FORMAT: u8 = 1;

/// The WebSocket close code a server sends when it closes a device's channel because the same
/// device has opened newer ones beyond what the server allows it. The newer channels carry the
/// notifications; the device does not reconnect this one.
pub const CLOSE_SUPERSEDED: u16 = 4000;

/// The WebSocket close code a server sends when the device was removed from the library
/// ([`RemoveDevice`](super::RemoveDevice)). It means what
/// [`ErrorCode::DeviceRemoved`](super::ErrorCode::DeviceRemoved) means on any other route; the
/// device does not reconnect.
pub const CLOSE_DEVICE_REMOVED: u16 = 4001;

/// "The library's log has reached sequence `seq`." Sent as one binary WebSocket message on
/// [`NOTIFICATIONS_PATH`](super::NOTIFICATIONS_PATH).
///
/// The upgrade request is a `GET` with an empty body, authenticated like any request and open to
/// devices only. The server sends the current head on connect and again after appends; it may
/// coalesce, since only the latest matters. A notification is a hint: the client reads batches
/// after its own cursor as usual, and treats one it cannot decode as a reason to read. A server
/// may close the channel at any time; when it closes it with [`CLOSE_SUPERSEDED`] or
/// [`CLOSE_DEVICE_REMOVED`] the client does not reconnect it, and after any other close it may.
///
/// Bytes: format, then `seq` as a big-endian u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Notification {
    pub seq: u64,
}

impl Notification {
    pub fn encode(&self) -> [u8; 9] {
        let mut out = [0u8; 9];
        out[0] = NOTIFICATION_FORMAT;
        out[1..].copy_from_slice(&self.seq.to_be_bytes());
        out
    }

    /// Fails on anything `encode` would not write.
    pub fn decode(bytes: &[u8]) -> Result<Notification, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != NOTIFICATION_FORMAT {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let seq = r.u64()?;
        r.finish()?;
        Ok(Notification { seq })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn notifications_round_trip(seq in any::<u64>()) {
            let bytes = Notification { seq }.encode();
            prop_assert_eq!(Notification::decode(&bytes), Ok(Notification { seq }));
            for cut in 0..bytes.len() {
                prop_assert_eq!(Notification::decode(&bytes[..cut]), Err(DecodeError::Truncated));
            }
        }

        #[test]
        fn decoding_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..16)) {
            let _ = Notification::decode(&bytes);
        }
    }

    #[test]
    fn other_versions_and_trailing_bytes_are_refused() {
        let mut bytes = Notification { seq: 7 }.encode().to_vec();
        assert_eq!(&bytes[..], &[1, 0, 0, 0, 0, 0, 0, 0, 7]);
        bytes.push(0);
        assert_eq!(
            Notification::decode(&bytes),
            Err(DecodeError::TrailingBytes)
        );
        bytes.pop();
        bytes[0] = 2;
        assert_eq!(
            Notification::decode(&bytes),
            Err(DecodeError::UnsupportedVersion(2))
        );
    }
}
