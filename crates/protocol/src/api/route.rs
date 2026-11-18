use thiserror::Error;

use super::{
    BATCHES_PATH, CAPABILITIES_PATH, DEVICES_PATH, LIBRARIES_PATH, NOTIFICATIONS_PATH, ReadBatches,
    batches_path, devices_path, notifications_path,
};
use crate::keys::LibraryId;

/// An endpoint, with the library it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Capabilities,
    Libraries,
    Devices(LibraryId),
    Batches(LibraryId),
    Notifications(LibraryId),
}

impl Route {
    /// Splits a request target into its route and its query (without the `?`). `None` unless the
    /// path is exactly one of the paths in this module, with the library id as 32 lowercase hex
    /// digits: no other case, trailing slash, extra segment or percent-encoding.
    pub fn parse(target: &str) -> Option<(Route, Option<&str>)> {
        let (path, query) = match target.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (target, None),
        };
        let route = if path == CAPABILITIES_PATH {
            Route::Capabilities
        } else if path == LIBRARIES_PATH {
            Route::Libraries
        } else if let Some(id) = library_in(path, DEVICES_PATH) {
            Route::Devices(id)
        } else if let Some(id) = library_in(path, BATCHES_PATH) {
            Route::Batches(id)
        } else {
            Route::Notifications(library_in(path, NOTIFICATIONS_PATH)?)
        };
        Some((route, query))
    }

    /// The canonical path, which [`Route::parse`] reads back.
    pub fn path(&self) -> String {
        match *self {
            Route::Capabilities => CAPABILITIES_PATH.to_owned(),
            Route::Libraries => LIBRARIES_PATH.to_owned(),
            Route::Devices(id) => devices_path(id),
            Route::Batches(id) => batches_path(id),
            Route::Notifications(id) => notifications_path(id),
        }
    }

    /// The methods the endpoint serves, for an `allow` header.
    pub fn methods(&self) -> &'static [&'static str] {
        match self {
            Route::Capabilities | Route::Notifications(_) => &["GET"],
            Route::Libraries => &["POST"],
            Route::Devices(_) | Route::Batches(_) => &["GET", "POST"],
        }
    }
}

fn library_in(path: &str, template: &str) -> Option<LibraryId> {
    let (prefix, suffix) = template.split_once("{library}")?;
    let hex = path.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    LibraryId::from_hex(hex)
}

/// A batch-read query that is not in the form [`ReadBatches::to_query`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the query is not `after=<n>` or `after=<n>&limit=<m>` with m at least 1")]
pub struct MalformedQuery;

impl ReadBatches {
    /// `after=<n>`, then `&limit=<m>` when there is a limit, in plain decimal.
    pub fn to_query(&self) -> String {
        match self.limit {
            Some(limit) => format!("after={}&limit={limit}", self.after),
            None => format!("after={}", self.after),
        }
    }

    /// The request target of this read for `library`.
    pub fn target(&self, library: LibraryId) -> String {
        format!("{}?{}", batches_path(library), self.to_query())
    }

    /// Reads exactly what [`ReadBatches::to_query`] writes: `after` first, decimal without
    /// leading zeros or sign, each value within its type, and `limit` at least 1.
    pub fn parse_query(query: &str) -> Result<ReadBatches, MalformedQuery> {
        let (after, limit) = match query.split_once('&') {
            Some((after, limit)) => (after, Some(limit)),
            None => (query, None),
        };
        let after = decimal(after.strip_prefix("after=").ok_or(MalformedQuery)?)?;
        let limit = match limit {
            None => None,
            Some(limit) => {
                let limit = decimal(limit.strip_prefix("limit=").ok_or(MalformedQuery)?)?;
                Some(
                    u32::try_from(limit)
                        .ok()
                        .filter(|&l| l > 0)
                        .ok_or(MalformedQuery)?,
                )
            }
        };
        Ok(ReadBatches { after, limit })
    }
}

fn decimal(text: &str) -> Result<u64, MalformedQuery> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    if !canonical {
        return Err(MalformedQuery);
    }
    text.parse().map_err(|_| MalformedQuery)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const ID: LibraryId = LibraryId([0xab; 16]);

    #[test]
    fn every_route_parses_from_its_path() {
        for route in [
            Route::Capabilities,
            Route::Libraries,
            Route::Devices(ID),
            Route::Batches(ID),
            Route::Notifications(ID),
        ] {
            assert_eq!(Route::parse(&route.path()), Some((route, None)));
        }
        assert_eq!(
            Route::parse(&format!("{}?after=1", batches_path(ID))),
            Some((Route::Batches(ID), Some("after=1")))
        );
        assert_eq!(
            Route::parse("/v1/capabilities?"),
            Some((Route::Capabilities, Some("")))
        );
    }

    #[test]
    fn anything_but_the_canonical_path_is_refused() {
        let hex = "ab".repeat(16);
        for bad in [
            format!("/v1/libraries/{}/batches", hex.to_uppercase()),
            format!("/v1/libraries/{hex}/batches/"),
            format!("/v1/libraries/{hex}/batches/x"),
            format!("/v1/libraries//{hex}/batches"),
            format!("/v1/libraries/{}/batches", &hex[1..]),
            format!("/v1/libraries/{hex}0/batches"),
            format!("/v1/libraries/%61{}/batches", &hex[1..]),
            format!("/v1/libraries/{hex}/blobs"),
            format!("/v1/libraries/{hex}"),
            "/v1/capabilities/".to_owned(),
            "/v1/libraries/".to_owned(),
            "/V1/capabilities".to_owned(),
            "v1/capabilities".to_owned(),
            String::new(),
        ] {
            assert_eq!(Route::parse(&bad), None, "{bad}");
        }
    }

    #[test]
    fn methods_are_listed_per_route() {
        assert_eq!(Route::Batches(ID).methods(), &["GET", "POST"]);
        assert_eq!(Route::Notifications(ID).methods(), &["GET"]);
        assert_eq!(Route::Libraries.methods(), &["POST"]);
    }

    proptest! {
        #[test]
        fn queries_round_trip(after in any::<u64>(), limit in proptest::option::of(1u32..)) {
            let read = ReadBatches { after, limit };
            prop_assert_eq!(ReadBatches::parse_query(&read.to_query()), Ok(read));
        }

        #[test]
        fn parsing_arbitrary_text_never_panics(text in ".{0,40}") {
            if let Ok(read) = ReadBatches::parse_query(&text) {
                prop_assert_eq!(read.to_query(), text);
            }
        }
    }

    #[test]
    fn only_the_canonical_query_is_accepted() {
        assert_eq!(
            ReadBatches::parse_query("after=0"),
            Ok(ReadBatches {
                after: 0,
                limit: None
            })
        );
        assert_eq!(
            ReadBatches::parse_query("after=3&limit=10"),
            Ok(ReadBatches {
                after: 3,
                limit: Some(10)
            })
        );
        for bad in [
            "",
            "after",
            "after=",
            "limit=3",
            "limit=1&after=0",
            "after=-1",
            "after=+1",
            "after=01",
            "after=1&after=2",
            "after=1&limit=0",
            "after=1&limit=4294967296",
            "after=1&cursor=2",
            "after=1&",
            "after=1&limit=2&x=3",
            "after=18446744073709551616",
            "AFTER=1",
        ] {
            assert_eq!(ReadBatches::parse_query(bad), Err(MalformedQuery), "{bad}");
        }
        assert_eq!(
            ReadBatches {
                after: 5,
                limit: Some(2)
            }
            .target(ID),
            format!("/v1/libraries/{}/batches?after=5&limit=2", "ab".repeat(16))
        );
    }
}
