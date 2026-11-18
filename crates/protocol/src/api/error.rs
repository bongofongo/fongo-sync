use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::envelope::EnvelopeError;
use crate::request::RequestError;

macro_rules! codes {
    ($($variant:ident = $code:literal, $status:literal, $message:literal;)*) => {
        /// Why a server refused a request. The string forms are stable; a code this version does
        /// not know reads as [`ErrorCode::Unknown`], so a client survives a newer server.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $($variant,)*
            /// A code this version does not know. Never sent.
            Unknown,
        }

        impl ErrorCode {
            /// Every code a server may send.
            pub const ALL: &'static [ErrorCode] = &[$(ErrorCode::$variant,)*];

            /// The stable string form, e.g. `"not_found"`.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(ErrorCode::$variant => $code,)*
                    ErrorCode::Unknown => "unknown",
                }
            }

            /// Reads a string form; anything unrecognised is [`ErrorCode::Unknown`].
            pub fn parse(code: &str) -> ErrorCode {
                match code {
                    $($code => ErrorCode::$variant,)*
                    _ => ErrorCode::Unknown,
                }
            }

            /// The HTTP status a server sends with this code. `Unknown` maps to 500.
            pub fn status(self) -> u16 {
                match self {
                    $(ErrorCode::$variant => $status,)*
                    ErrorCode::Unknown => 500,
                }
            }

            /// A fixed sentence that never contains request content.
            pub fn message(self) -> &'static str {
                match self {
                    $(ErrorCode::$variant => $message,)*
                    ErrorCode::Unknown => "the server reported an error this client does not know",
                }
            }
        }
    };
}

codes! {
    MalformedRequest = "malformed_request", 400,
        "the request body, query or path parameter is malformed";
    MalformedEnvelope = "malformed_envelope", 400, "the body is not an envelope";
    InvalidProof = "invalid_proof", 400, "the proof does not show possession of the device key";
    Unauthenticated = "unauthenticated", 401,
        "request signature headers are missing or malformed";
    UnknownDevice = "unknown_device", 401, "the signing device is not registered in this library";
    BadSignature = "bad_signature", 401, "the request signature does not verify";
    StaleRequest = "stale_request", 401, "the request timestamp is outside the allowed clock skew";
    ReplayedRequest = "replayed_request", 401,
        "this device already used this request nonce within the clock skew";
    NotEntitled = "not_entitled", 402, "this library is not entitled to the service";
    Forbidden = "forbidden", 403, "the recovery key may only list and add devices";
    DeviceLimit = "device_limit", 403, "the library has as many devices as it may";
    NotFound = "not_found", 404, "no such endpoint";
    LibraryNotFound = "library_not_found", 404, "no such library";
    MethodNotAllowed = "method_not_allowed", 405, "method not allowed on this endpoint";
    LibraryExists = "library_exists", 409, "a library with this id already exists";
    TooLarge = "too_large", 413, "the request body is larger than this server accepts";
    EnvelopeWrongLibrary = "envelope_wrong_library", 422,
        "the envelope belongs to another library";
    EnvelopeWrongDevice = "envelope_wrong_device", 422,
        "the envelope was not written by the requesting device";
    EnvelopeBadSignature = "envelope_bad_signature", 422, "the envelope signature does not verify";
    Internal = "internal", 500, "internal server error";
    LibraryFull = "library_full", 507, "the library has reached its storage limit";
    QuotaExceeded = "quota_exceeded", 507, "the library has exceeded its quota";
}

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<ErrorCode, D::Error> {
        Ok(ErrorCode::parse(
            &<std::borrow::Cow<'de, str>>::deserialize(d)?,
        ))
    }
}

/// The JSON body of every error response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

impl ErrorBody {
    /// The body for `code`, with its fixed message.
    pub fn new(code: ErrorCode) -> ErrorBody {
        ErrorBody {
            code,
            message: code.message().to_owned(),
        }
    }
}

impl From<RequestError> for ErrorCode {
    fn from(error: RequestError) -> ErrorCode {
        match error {
            RequestError::Malformed(_) => ErrorCode::Unauthenticated,
            RequestError::WrongDevice | RequestError::BadSignature => ErrorCode::BadSignature,
            RequestError::Stale => ErrorCode::StaleRequest,
        }
    }
}

/// The code for an appended envelope that `verify_envelope` refused.
impl From<EnvelopeError> for ErrorCode {
    fn from(error: EnvelopeError) -> ErrorCode {
        match error {
            EnvelopeError::WrongLibrary => ErrorCode::EnvelopeWrongLibrary,
            EnvelopeError::WrongDevice | EnvelopeError::UnknownDevice => {
                ErrorCode::EnvelopeWrongDevice
            }
            EnvelopeError::BadSignature => ErrorCode::EnvelopeBadSignature,
            EnvelopeError::Decode(_) | EnvelopeError::Decrypt | EnvelopeError::Batch(_) => {
                ErrorCode::MalformedEnvelope
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn every_code_round_trips_through_its_string() {
        assert_eq!(ErrorCode::ALL.len(), 22);
        for &code in ErrorCode::ALL {
            assert_eq!(ErrorCode::parse(code.as_str()), code);
            assert!((400..600).contains(&code.status()));
            let text = serde_json::to_string(&ErrorBody::new(code)).unwrap();
            assert_eq!(
                serde_json::from_str::<ErrorBody>(&text).unwrap(),
                ErrorBody::new(code)
            );
        }
    }

    #[test]
    fn an_unknown_code_degrades_instead_of_failing() {
        let body: ErrorBody =
            serde_json::from_value(json!({"code": "from_the_future", "message": "x"})).unwrap();
        assert_eq!(body.code, ErrorCode::Unknown);
        assert_eq!(ErrorCode::parse("NOT_FOUND"), ErrorCode::Unknown);
    }

    #[test]
    fn the_body_is_code_and_message() {
        assert_eq!(
            serde_json::to_value(ErrorBody::new(ErrorCode::NotFound)).unwrap(),
            json!({"code": "not_found", "message": "no such endpoint"})
        );
        assert_eq!(ErrorCode::LibraryFull.status(), 507);
        assert_eq!(ErrorCode::MethodNotAllowed.status(), 405);
    }
}
