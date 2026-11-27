use thiserror::Error;

use super::{
    BATCHES_CLAIMING_PATH, BATCHES_PATH, BLOB_CHUNK_PATH, BLOB_COMPLETE_PATH, BLOB_PATH,
    BLOB_WHOLE_PATH, BLOBS_CLAIM_PATH, BLOBS_QUERY_PATH, BLOBS_RETIRE_PATH, CAPABILITIES_PATH,
    DEVICES_PATH, ENTITLEMENT_PATH, LIBRARIES_PATH, NOTIFICATIONS_PATH, REMOVE_DEVICE_PATH,
    ReadBatches, SETTLED_PATH, STATUS_PATH, batches_path,
};
use crate::file::BlobId;
use crate::keys::LibraryId;

const LIBRARY_PREFIX: &str = "/v1/libraries/";

/// An endpoint, with the library, blob and chunk it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Capabilities,
    Libraries,
    Devices(LibraryId),
    Batches(LibraryId),
    BatchesClaiming(LibraryId),
    Notifications(LibraryId),
    Blob(LibraryId, BlobId),
    BlobChunk(LibraryId, BlobId, u64),
    BlobComplete(LibraryId, BlobId),
    BlobWhole(LibraryId, BlobId),
    BlobsQuery(LibraryId),
    BlobsClaim(LibraryId),
    BlobsRetire(LibraryId),
    Settled(LibraryId),
    Status(LibraryId),
    Entitlement(LibraryId),
    RemoveDevice(LibraryId),
}

impl Route {
    /// Splits a request target into its route and its query (without the `?`). `None` unless the
    /// path is exactly one of the paths in this module, with the library id as 32 lowercase hex
    /// digits, a blob id as 64, and a chunk index in decimal without sign or leading zeros: no
    /// other case, trailing slash, extra segment or percent-encoding.
    pub fn parse(target: &str) -> Option<(Route, Option<&str>)> {
        let (path, query) = match target.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (target, None),
        };
        let route = if path == CAPABILITIES_PATH {
            Route::Capabilities
        } else if path == LIBRARIES_PATH {
            Route::Libraries
        } else {
            let (library, rest) = path.strip_prefix(LIBRARY_PREFIX)?.split_once('/')?;
            let library = library_id(library)?;
            let mut parts = [""; 4];
            let mut count = 0;
            for part in rest.splitn(parts.len() + 1, '/') {
                *parts.get_mut(count)? = part;
                count += 1;
            }
            match parts[..count] {
                ["devices"] => Route::Devices(library),
                ["devices", "remove"] => Route::RemoveDevice(library),
                ["batches"] => Route::Batches(library),
                ["batches", "claiming"] => Route::BatchesClaiming(library),
                ["notifications"] => Route::Notifications(library),
                ["settled"] => Route::Settled(library),
                ["status"] => Route::Status(library),
                ["entitlement"] => Route::Entitlement(library),
                ["blobs", "query"] => Route::BlobsQuery(library),
                ["blobs", "claim"] => Route::BlobsClaim(library),
                ["blobs", "retire"] => Route::BlobsRetire(library),
                ["blobs", id] => Route::Blob(library, BlobId::from_hex(id)?),
                ["blobs", id, "complete"] => Route::BlobComplete(library, BlobId::from_hex(id)?),
                ["blobs", id, "whole"] => Route::BlobWhole(library, BlobId::from_hex(id)?),
                ["blobs", id, "chunks", n] => {
                    Route::BlobChunk(library, BlobId::from_hex(id)?, decimal(n).ok()?)
                }
                _ => return None,
            }
        };
        Some((route, query))
    }

    /// The canonical path, which [`Route::parse`] reads back.
    pub fn path(&self) -> String {
        let (template, library, blob, chunk) = match *self {
            Route::Capabilities => return CAPABILITIES_PATH.to_owned(),
            Route::Libraries => return LIBRARIES_PATH.to_owned(),
            Route::Devices(l) => (DEVICES_PATH, l, None, None),
            Route::Batches(l) => (BATCHES_PATH, l, None, None),
            Route::BatchesClaiming(l) => (BATCHES_CLAIMING_PATH, l, None, None),
            Route::Notifications(l) => (NOTIFICATIONS_PATH, l, None, None),
            Route::Blob(l, id) => (BLOB_PATH, l, Some(id), None),
            Route::BlobChunk(l, id, n) => (BLOB_CHUNK_PATH, l, Some(id), Some(n)),
            Route::BlobComplete(l, id) => (BLOB_COMPLETE_PATH, l, Some(id), None),
            Route::BlobWhole(l, id) => (BLOB_WHOLE_PATH, l, Some(id), None),
            Route::BlobsQuery(l) => (BLOBS_QUERY_PATH, l, None, None),
            Route::BlobsClaim(l) => (BLOBS_CLAIM_PATH, l, None, None),
            Route::BlobsRetire(l) => (BLOBS_RETIRE_PATH, l, None, None),
            Route::Settled(l) => (SETTLED_PATH, l, None, None),
            Route::Status(l) => (STATUS_PATH, l, None, None),
            Route::Entitlement(l) => (ENTITLEMENT_PATH, l, None, None),
            Route::RemoveDevice(l) => (REMOVE_DEVICE_PATH, l, None, None),
        };
        let mut path = template.replace("{library}", &library.to_hex());
        if let Some(id) = blob {
            path = path.replace("{id}", &id.to_hex());
        }
        if let Some(n) = chunk {
            path = path.replace("{n}", &n.to_string());
        }
        path
    }

    /// The methods the endpoint serves, for an `allow` header.
    pub fn methods(&self) -> &'static [&'static str] {
        match self {
            Route::Capabilities | Route::Notifications(_) | Route::Status(_) => &["GET"],
            Route::Libraries
            | Route::BatchesClaiming(_)
            | Route::BlobComplete(..)
            | Route::BlobWhole(..)
            | Route::BlobsQuery(_)
            | Route::BlobsClaim(_)
            | Route::BlobsRetire(_)
            | Route::Settled(_)
            | Route::Entitlement(_)
            | Route::RemoveDevice(_) => &["POST"],
            Route::Devices(_) | Route::Batches(_) | Route::Blob(..) | Route::BlobChunk(..) => {
                &["GET", "POST"]
            }
        }
    }
}

fn library_id(hex: &str) -> Option<LibraryId> {
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
    const BLOB: BlobId = BlobId([0xcd; 32]);

    fn every_route(library: LibraryId, blob: BlobId, chunk: u64) -> [Route; 17] {
        [
            Route::Capabilities,
            Route::Libraries,
            Route::Devices(library),
            Route::Batches(library),
            Route::Notifications(library),
            Route::Blob(library, blob),
            Route::BlobChunk(library, blob, chunk),
            Route::BlobComplete(library, blob),
            Route::BlobWhole(library, blob),
            Route::BlobsQuery(library),
            Route::BlobsClaim(library),
            Route::BlobsRetire(library),
            Route::Settled(library),
            Route::Status(library),
            Route::Entitlement(library),
            Route::RemoveDevice(library),
            Route::BatchesClaiming(library),
        ]
    }

    #[test]
    fn every_route_parses_from_its_path() {
        for route in every_route(ID, BLOB, 7) {
            assert_eq!(Route::parse(&route.path()), Some((route, None)));
        }
        let lib = "ab".repeat(16);
        let blob = "cd".repeat(32);
        let paths = [
            "/v1/capabilities".to_owned(),
            "/v1/libraries".to_owned(),
            format!("/v1/libraries/{lib}/devices"),
            format!("/v1/libraries/{lib}/batches"),
            format!("/v1/libraries/{lib}/notifications"),
            format!("/v1/libraries/{lib}/blobs/{blob}"),
            format!("/v1/libraries/{lib}/blobs/{blob}/chunks/7"),
            format!("/v1/libraries/{lib}/blobs/{blob}/complete"),
            format!("/v1/libraries/{lib}/blobs/{blob}/whole"),
            format!("/v1/libraries/{lib}/blobs/query"),
            format!("/v1/libraries/{lib}/blobs/claim"),
            format!("/v1/libraries/{lib}/blobs/retire"),
            format!("/v1/libraries/{lib}/settled"),
            format!("/v1/libraries/{lib}/status"),
            format!("/v1/libraries/{lib}/entitlement"),
            format!("/v1/libraries/{lib}/devices/remove"),
            format!("/v1/libraries/{lib}/batches/claiming"),
        ];
        for (route, path) in every_route(ID, BLOB, 7).iter().zip(paths) {
            assert_eq!(route.path(), path);
        }
        assert_eq!(
            Route::parse(&format!("/v1/libraries/{lib}/blobs/{blob}/chunks/0")),
            Some((Route::BlobChunk(ID, BLOB, 0), None))
        );
        assert_eq!(
            Route::parse(&format!(
                "/v1/libraries/{lib}/blobs/{blob}/chunks/18446744073709551615"
            )),
            Some((Route::BlobChunk(ID, BLOB, u64::MAX), None))
        );
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
        let blob = "cd".repeat(32);
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
            format!("/v1/libraries/{hex}/"),
            format!("/v1/libraries/{hex}/blobs/"),
            format!("/v1/libraries/{hex}/blobs/{blob}/"),
            format!("/v1/libraries/{hex}/blobs/{}", blob.to_uppercase()),
            format!("/v1/libraries/{hex}/blobs/{}", &blob[1..]),
            format!("/v1/libraries/{hex}/blobs/{blob}0"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/01"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/+1"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/-1"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/1/x"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/18446744073709551616"),
            format!("/v1/libraries/{hex}/blobs/{blob}/chunks/%31"),
            format!("/v1/libraries/{hex}/blobs/{blob}/complete/"),
            format!("/v1/libraries/{hex}/blobs/{blob}/Complete"),
            format!("/v1/libraries/{hex}/blobs/{blob}/whole/x"),
            format!("/v1/libraries/{hex}/blobs/query/x"),
            format!("/v1/libraries/{hex}/blobs//complete"),
            format!("/v1/libraries/{hex}/devices/remove/"),
            format!("/v1/libraries/{hex}/devices/{blob}"),
            format!("/v1/libraries/{hex}/settled/x"),
            format!("/v1/libraries/{hex}/Status"),
            format!("/v1/libraries/{hex}/batches/x/y/z/w"),
            format!("/v1/libraries/{hex}/batches/claiming/"),
            format!("/v1/libraries/{hex}/batches/Claiming"),
            format!("/v1/libraries/{hex}/batches/claiming/x"),
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
        assert_eq!(Route::Blob(ID, BLOB).methods(), &["GET", "POST"]);
        assert_eq!(Route::BlobChunk(ID, BLOB, 0).methods(), &["GET", "POST"]);
        assert_eq!(Route::Status(ID).methods(), &["GET"]);
        assert_eq!(Route::RemoveDevice(ID).methods(), &["POST"]);
        assert_eq!(Route::BatchesClaiming(ID).methods(), &["POST"]);
    }

    proptest! {
        #[test]
        fn routes_round_trip(library in any::<[u8; 16]>(), blob in any::<[u8; 32]>(), chunk in any::<u64>()) {
            for route in every_route(LibraryId(library), BlobId(blob), chunk) {
                let path = route.path();
                prop_assert_eq!(Route::parse(&path), Some((route, None)));
            }
        }

        #[test]
        fn a_parsed_target_is_its_canonical_path(
            parts in proptest::collection::vec(
                prop_oneof![
                    Just("blobs".to_owned()),
                    Just("chunks".to_owned()),
                    Just("devices".to_owned()),
                    Just("remove".to_owned()),
                    Just("complete".to_owned()),
                    Just("ab".repeat(16)),
                    Just("cd".repeat(32)),
                    "[0-9]{1,3}",
                    "[a-z0-9%+-]{0,4}",
                ],
                0..6,
            ),
        ) {
            let target = format!("/v1/libraries/{}", parts.join("/"));
            if let Some((route, None)) = Route::parse(&target) {
                prop_assert_eq!(route.path(), target);
            }
        }

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
