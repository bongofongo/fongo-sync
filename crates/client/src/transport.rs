use std::fmt;
use std::future::Future;

/// The HTTP methods the protocol uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
        }
    }
}

/// One request to the server. `target` is the path and query, which the transport appends to the
/// server's origin unchanged: requests are signed over the target exactly as written, so a
/// transport that rewrites it, or a server mounted under a further path prefix, fails to verify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: Method,
    pub target: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

/// The server's answer, whatever its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// The request did not complete: no connection, a timeout, a reset. Whether the server acted on
/// it is unknown.
pub struct TransportError(Box<dyn std::error::Error + Send + Sync>);

impl TransportError {
    pub fn new(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> TransportError {
        TransportError(error.into())
    }

    pub fn into_inner(self) -> Box<dyn std::error::Error + Send + Sync> {
        self.0
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TransportError").field(&self.0).finish()
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// Carries requests to a sync server. An application implements it over its own HTTP client.
///
/// Implement it with `async fn send(&self, request: Request) -> Result<Response, TransportError>`;
/// the future must be `Send`. Any status is a response, not an error: a transport returns
/// `TransportError` only when it has no response to give.
pub trait Transport: Send + Sync {
    fn send(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, TransportError>> + Send;
}
