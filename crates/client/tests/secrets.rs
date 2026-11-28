//! No secret reaches the database file or its write-ahead log.
//!
//! What the client stores, by its code: in `fongo_client_state`, the cursor, the digest of the
//! envelope there, the log's fingerprint, the envelope being sent (sealed under the batch key and
//! signed, so its bytes are ciphertext, the library id, the device id, the epoch, the nonce and a
//! signature), the head known when it was recorded, where the pull stopped and the digest there,
//! an envelope to step past and its digest, a stored fault and a count of resolutions; in
//! `fongo_client_appended` and `fongo_client_passed`, positions, outbox positions and digests of
//! this device's envelopes; in `fongo_client_meta`, the layout. The kernel stores the
//! application's rows and the changes merged from them. Credentials are the application's to
//! keep: the library secret, every key derived from it, the device's signing key and its wrap
//! secret are stored nowhere in the database, in any form this test knows.

#[allow(dead_code)]
mod support;

use std::path::Path;

use fongo_sync_client::protocol::LibraryKeys;
use fongo_sync_client::{Credentials, Error, Method, add_device, join_library};
use sqlx::Connection;
use support::{Peer, Server, now, scratch};

const INFOS: [&[u8]; 4] = [
    b"fongo-sync/v1/recovery-key",
    b"fongo-sync/v1/file-name-key",
    b"fongo-sync/v1/file-key-root",
    b"fongo-sync/v1/member-key",
];

fn expand(keys: &LibraryKeys, info: &[u8]) -> Vec<u8> {
    let mut out = [0u8; 32];
    hkdf::Hkdf::<sha2::Sha256>::new(Some(&keys.id.0), keys.secret())
        .expand(info, &mut out)
        .unwrap();
    out.to_vec()
}

/// Every secret `credentials` holds or derives, by name: raw, and as hex in either case.
fn secrets(credentials: &Credentials) -> Vec<(String, Vec<u8>)> {
    let keys = credentials.library();
    let stored = credentials.to_bytes();
    let mut raw = vec![
        ("library secret".to_string(), keys.secret().to_vec()),
        (
            "device key".to_string(),
            credentials.device().to_bytes().to_vec(),
        ),
        (
            "wrap secret".to_string(),
            stored[stored.len() - 32..].to_vec(),
        ),
        ("stored credentials".to_string(), stored.to_vec()),
        (
            "recovery code".to_string(),
            credentials.recovery_code().into_bytes(),
        ),
    ];
    for epoch in 0..3u32 {
        let mut info = b"fongo-sync/v1/batch-key".to_vec();
        info.extend_from_slice(&epoch.to_be_bytes());
        raw.push((format!("batch key {epoch}"), expand(keys, &info)));
    }
    for info in INFOS {
        raw.push((String::from_utf8_lossy(info).into(), expand(keys, info)));
    }
    let mut all = Vec::new();
    for (name, bytes) in raw {
        let lower: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        all.push((format!("{name} as hex"), lower.clone().into_bytes()));
        all.push((format!("{name} as HEX"), lower.to_uppercase().into_bytes()));
        all.push((name, bytes));
    }
    all
}

/// Fails if any of `secrets` is in the database at `path` or beside it.
fn scan(path: &Path, secrets: &[(String, Vec<u8>)], when: &str) {
    let dir = path.parent().unwrap();
    let mut files = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let file = entry.unwrap().path();
        let bytes = std::fs::read(&file).unwrap();
        files += 1;
        for (name, secret) in secrets {
            assert!(
                !bytes.windows(secret.len()).any(|w| w == &secret[..]),
                "{when}: the {name} is in {}",
                file.display()
            );
        }
    }
    assert!(files >= 1);
}

async fn wal(peer: &mut Peer) {
    peer.run("PRAGMA journal_mode = WAL").await;
}

#[tokio::test]
async fn no_secret_is_in_the_database_or_its_log_after_create_join_and_sync() {
    let root = scratch("secrets");
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    fongo_sync_client::create_library_with(&server, &made, now)
        .await
        .unwrap();
    let joined = join_library(&server, &made.recovery_code(), now)
        .await
        .unwrap();
    let offered = Credentials::for_new_device(made.library().clone()).unwrap();
    add_device(&server, &made, &offered.device_offer().unwrap(), now)
        .await
        .unwrap();
    let mut peers = Vec::new();
    for (name, credentials) in [("a", &made), ("b", &joined), ("c", &offered)] {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.db");
        let mut peer = Peer::new_at(&path, credentials.clone()).await;
        wal(&mut peer).await;
        peers.push((path, peer));
    }
    let all: Vec<_> = peers
        .iter()
        .flat_map(|(_, peer)| secrets(&peer.credentials))
        .collect();
    for round in 0..3 {
        for (_, peer) in &mut peers {
            peer.run(&format!(
                "INSERT INTO books (title, year) VALUES ('Middlemarch', {round})"
            ))
            .await;
            peer.sync(&server).await.unwrap();
        }
    }
    // One envelope left recorded as being sent, its answer lost.
    let (_, a) = &mut peers[0];
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    let mut once = true;
    server.lose_if(move |r| r.method == Method::Post && std::mem::take(&mut once));
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    for (path, _) in &peers {
        scan(path, &all, "open");
    }
    for (path, peer) in peers {
        peer.conn.close().await.unwrap();
        scan(&path, &all, "closed");
    }
}
