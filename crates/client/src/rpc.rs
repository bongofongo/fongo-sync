//! Signed requests and the checks every response gets.

use fongo_sync_protocol::api::{
    BINARY_CONTENT_TYPE, CAPABILITIES_PATH, Capabilities, ErrorBody, ErrorCode,
};
use fongo_sync_protocol::{PROTOCOL_VERSION, SigningKey, random_request_nonce, sign_request};
use serde::de::DeserializeOwned;

use crate::error::Error;
use crate::transport::{Method, Request, Transport};

pub(crate) const JSON_CONTENT_TYPE: &str = "application/json";

pub(crate) struct Rpc<'a, T, N> {
    pub(crate) transport: &'a T,
    pub(crate) now: &'a N,
}

pub(crate) enum Body {
    Empty,
    Json(Vec<u8>),
    Binary(Vec<u8>),
}

impl<T: Transport, N: Fn() -> u64 + Sync> Rpc<'_, T, N> {
    /// Sends one request, signed by `signer` when given, and returns the body of a 2xx response.
    pub(crate) async fn call(
        &self,
        method: Method,
        target: String,
        body: Body,
        signer: Option<&SigningKey>,
    ) -> Result<Vec<u8>, Error> {
        let (content_type, body) = match body {
            Body::Empty => (None, Vec::new()),
            Body::Json(body) => (Some(JSON_CONTENT_TYPE), body),
            Body::Binary(body) => (Some(BINARY_CONTENT_TYPE), body),
        };
        let mut headers = Vec::with_capacity(5);
        if let Some(content_type) = content_type {
            headers.push(("content-type", content_type.to_owned()));
        }
        if let Some(key) = signer {
            let nonce = random_request_nonce().map_err(|_| Error::Randomness)?;
            let auth = sign_request(key, method.as_str(), &target, &body, (self.now)(), nonce);
            headers.extend(auth.headers());
        }
        let request = Request {
            method,
            target,
            headers,
            body,
        };
        let response = self
            .transport
            .send(request)
            .await
            .map_err(Error::Transport)?;
        if (200..300).contains(&response.status) {
            return Ok(response.body);
        }
        let code = serde_json::from_slice::<ErrorBody>(&response.body)
            .map_or(ErrorCode::Unknown, |body| body.code);
        Err(match code {
            ErrorCode::StaleRequest => Error::ClockSkew,
            code => Error::Server {
                status: response.status,
                code,
            },
        })
    }

    /// The server's capabilities, refusing a server that speaks another protocol version.
    pub(crate) async fn capabilities(&self) -> Result<Capabilities, Error> {
        let body = self
            .call(Method::Get, CAPABILITIES_PATH.to_owned(), Body::Empty, None)
            .await?;
        let caps: Capabilities = json(&body, "capabilities are not the documented JSON")?;
        if caps.protocol_version > PROTOCOL_VERSION {
            return Err(Error::NewerProtocol {
                server: caps.protocol_version,
                client: PROTOCOL_VERSION,
            });
        }
        if caps.protocol_version < PROTOCOL_VERSION {
            return Err(Error::MalformedResponse(
                "capabilities name a protocol version older than the first",
            ));
        }
        Ok(caps)
    }
}

pub(crate) fn json<V: DeserializeOwned>(body: &[u8], what: &'static str) -> Result<V, Error> {
    serde_json::from_slice(body).map_err(|_| Error::MalformedResponse(what))
}
