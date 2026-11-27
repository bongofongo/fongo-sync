//! Registering a device with credentials made and stored first: a create or join whose response
//! was lost is repeated with the same result, and every device it registers carries a wrap key
//! and a membership proof that the library's other devices can check.

#[allow(dead_code)]
mod support;

use fongo_sync_client::protocol::api::{DeviceRecord, ErrorCode, LIBRARIES_PATH};
use fongo_sync_client::protocol::{
    LibraryKeys, ListedMemberProof, MemberCheck, SigningKey, WrapKey, WrapSecret,
};
use fongo_sync_client::{
    Credentials, Error, Method, Registered, Request, WrapListing, add_device, create_library,
    create_library_with, join_library, join_library_with,
};
use support::{Peer, Server, assert_converged, now};

/// Loses the response to the first request `pick` returns true for, once the server has
/// handled it.
fn lose_first(server: &Server, pick: impl Fn(&Request) -> bool + Send + 'static) {
    let mut lost = false;
    server.lose_if(move |request| {
        let lose = !lost && pick(request);
        lost |= lose;
        lose
    });
}

fn is_create(request: &Request) -> bool {
    request.method == Method::Post && request.target == LIBRARIES_PATH
}

fn is_add(request: &Request) -> bool {
    request.method == Method::Post && request.target.ends_with("/devices")
}

fn records(server: &Server, credentials: &Credentials) -> Vec<DeviceRecord> {
    server.with(|s| s.libraries[&credentials.library_id()].devices.clone())
}

fn record(server: &Server, credentials: &Credentials) -> DeviceRecord {
    let key = credentials.device().verifying_key();
    records(server, credentials)
        .into_iter()
        .find(|d| d.public_key == key)
        .expect("the device is listed")
}

fn assert_lost(result: Result<Registered, Error>) {
    match result {
        Err(Error::Transport(_)) => {}
        other => panic!("expected the response to be lost, got {other:?}"),
    }
}

#[tokio::test]
async fn a_create_whose_response_was_lost_is_repeated_with_the_same_result() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    lose_first(&server, is_create);
    assert_lost(create_library_with(&server, &made, now).await);
    assert_eq!(server.with(|s| s.libraries.len()), 1);

    create_library_with(&server, &made, now).await.unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    assert_eq!(server.with(|s| s.libraries.len()), 1);
    assert_eq!(records(&server, &made).len(), 2);

    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    let mut peers = [Peer::new(made).await, Peer::new(joined).await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0].sync(&server).await.unwrap();
    peers[1].sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut peers).await.len(), 1);
}

#[tokio::test]
async fn a_create_of_a_library_another_key_made_is_refused() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let rival = Credentials::new(made.library().clone(), SigningKey::from_bytes(&[9; 32]), 0);
    match create_library_with(&server, &rival, now).await {
        Err(Error::Server {
            code: ErrorCode::LibraryExists,
            ..
        }) => {}
        other => panic!("expected library_exists, got {other:?}"),
    }
    assert_eq!(records(&server, &made).len(), 2);
}

#[tokio::test]
async fn a_join_whose_response_was_lost_is_repeated_with_the_same_result() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    lose_first(&server, is_add);
    assert_lost(join_library_with(&server, &joined, now).await);
    assert_eq!(records(&server, &made).len(), 3);

    join_library_with(&server, &joined, now).await.unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    assert_eq!(records(&server, &made).len(), 3);
    assert_eq!(
        record(&server, &joined).check_member_proof(joined.library()),
        MemberCheck::Valid
    );

    let mut peers = [Peer::new(made).await, Peer::new(joined).await];
    peers[1]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[1].sync(&server).await.unwrap();
    peers[0].sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut peers).await.len(), 1);
}

#[tokio::test]
async fn registered_devices_carry_a_signed_wrap_key_and_a_proof_others_can_check() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();

    let first = record(&server, &made);
    let second = record(&server, &joined);
    assert!(first.verify_wrap(made.library_id()) && second.verify_wrap(made.library_id()));
    assert_eq!(first.wrap.unwrap().wrap_key, made.wrap_key().unwrap());
    assert_eq!(second.wrap.unwrap().wrap_key, joined.wrap_key().unwrap());
    assert_ne!(first.member_proof, second.member_proof);
    assert_eq!(joined.check_member(&first), MemberCheck::Valid);
    assert_eq!(made.check_member(&second), MemberCheck::Valid);
    let recovery = records(&server, &made)
        .into_iter()
        .find(|d| d.recovery)
        .unwrap();
    assert_eq!(made.check_member(&recovery), MemberCheck::Absent);

    let wrong = Credentials::new(
        LibraryKeys::new(made.library_id(), [0x77; 32]),
        SigningKey::from_bytes(&[9; 32]),
        0,
    );
    assert_eq!(wrong.check_member(&second), MemberCheck::Invalid);
    let mut tampered = second.clone();
    tampered.wrap.as_mut().unwrap().wrap_key = WrapKey([1; 32]);
    assert_eq!(made.check_member(&tampered), MemberCheck::Invalid);

    let other = Credentials::for_new_library().unwrap();
    create_library_with(&server, &other, now).await.unwrap();
    assert_eq!(
        made.check_member(&record(&server, &other)),
        MemberCheck::Invalid
    );
}

#[tokio::test]
async fn a_server_that_drops_the_proof_leaves_it_absent() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    server.with(|s| {
        for device in &mut s.libraries.get_mut(&made.library_id()).unwrap().devices {
            device.member_proof = None;
        }
    });
    assert_eq!(
        made.check_member(&record(&server, &joined)),
        MemberCheck::Absent
    );
}

#[tokio::test]
async fn a_device_added_by_another_registers_its_own_wrap_key_and_a_proof() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let added = Credentials::for_new_device(made.library().clone()).unwrap();
    let offer = added.device_offer().unwrap();
    add_device(&server, &made, &offer, now).await.unwrap();
    add_device(&server, &made, &offer, now).await.unwrap();

    let listed = record(&server, &added);
    assert_eq!(records(&server, &made).len(), 3);
    assert!(listed.verify_wrap(made.library_id()));
    assert_eq!(listed.wrap.unwrap().wrap_key, added.wrap_key().unwrap());
    assert_eq!(made.check_member(&listed), MemberCheck::Valid);
    assert_eq!(added.check_member(&listed), MemberCheck::Valid);

    let back = fongo_sync_client::DeviceOffer::from_bytes(&offer.to_bytes()).unwrap();
    assert_eq!(back, offer);
    let mut peers = [Peer::new(made).await, Peer::new(added).await];
    peers[1]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[1].sync(&server).await.unwrap();
    peers[0].sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut peers).await.len(), 1);
}

#[tokio::test]
async fn an_offer_whose_wrap_key_its_device_did_not_sign_is_refused_before_it_is_sent() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let added = Credentials::for_new_device(made.library().clone()).unwrap();
    let elsewhere = Credentials::for_new_library().unwrap();
    let mut forged = added.device_offer().unwrap();
    forged.wrap.wrap_key = WrapKey([1; 32]);
    let foreign = Credentials::for_new_device(elsewhere.library().clone())
        .unwrap()
        .device_offer()
        .unwrap();
    let before = server.count(Method::Post, "/v1/libraries/");
    for offer in [forged, foreign] {
        match add_device(&server, &made, &offer, now).await {
            Err(Error::WrapKeyNotSigned) => {}
            other => panic!("expected the offer to be refused, got {other:?}"),
        }
    }
    assert_eq!(server.count(Method::Post, "/v1/libraries/"), before);
    assert_eq!(records(&server, &made).len(), 2);
    assert!(
        Credentials::new(made.library().clone(), SigningKey::from_bytes(&[9; 32]), 0)
            .device_offer()
            .is_none()
    );
}

#[tokio::test]
async fn a_join_retried_after_the_device_was_removed_restores_it() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    server.mark_removed(made.library_id(), joined.device().verifying_key());
    assert!(record(&server, &joined).removed);

    join_library_with(&server, &joined, now).await.unwrap();
    let restored = record(&server, &joined);
    assert!(!restored.removed);
    assert_eq!(records(&server, &made).len(), 3);
    assert_eq!(made.check_member(&restored), MemberCheck::Valid);
}

#[tokio::test]
async fn a_removed_device_is_restored_only_into_a_free_slot() {
    let server = Server::new();
    server.with(|s| s.caps.max_devices = Some(2));
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    server.mark_removed(made.library_id(), joined.device().verifying_key());
    let third = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &third, now).await.unwrap();

    match join_library_with(&server, &joined, now).await {
        Err(Error::Server {
            code: ErrorCode::DeviceLimit,
            ..
        }) => {}
        other => panic!("expected device_limit, got {other:?}"),
    }
    assert!(record(&server, &joined).removed);
}

#[tokio::test]
async fn a_key_registered_with_another_wrap_key_is_reported_and_left_as_it_was() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    join_library_with(&server, &joined, now).await.unwrap();
    let before = record(&server, &joined);

    let rewrapped = Credentials::new(joined.library().clone(), joined.device().clone(), 0)
        .with_wrap_secret(WrapSecret::from_bytes(&[0x42; 32]));
    match join_library_with(&server, &rewrapped, now).await {
        Err(Error::RegisteredOtherwise { .. }) => {}
        other => panic!("expected the other registration to be reported, got {other:?}"),
    }
    assert_eq!(record(&server, &joined), before);
    let unwrapped = Credentials::new(joined.library().clone(), joined.device().clone(), 0);
    assert!(matches!(
        join_library_with(&server, &unwrapped, now).await,
        Err(Error::RegisteredOtherwise { .. })
    ));
    assert_eq!(record(&server, &joined), before);

    let recreated = Credentials::new(made.library().clone(), made.device().clone(), 0)
        .with_wrap_secret(WrapSecret::from_bytes(&[0x43; 32]));
    let first = record(&server, &made);
    assert!(matches!(
        create_library_with(&server, &recreated, now).await,
        Err(Error::RegisteredOtherwise { .. })
    ));
    assert_eq!(record(&server, &made), first);
    join_library_with(&server, &joined, now).await.unwrap();
    create_library_with(&server, &made, now).await.unwrap();
}

#[tokio::test]
async fn a_repeat_after_the_server_dropped_the_proof_succeeds_and_says_so() {
    let server = Server::new();
    let made = Credentials::for_new_library().unwrap();
    let first = create_library_with(&server, &made, now).await.unwrap();
    assert!(first.proof_kept && first.wrap == WrapListing::Kept);
    server.with(|s| {
        for device in &mut s.libraries.get_mut(&made.library_id()).unwrap().devices {
            device.member_proof = None;
        }
    });
    let again = create_library_with(&server, &made, now).await.unwrap();
    assert!(again.wrap == WrapListing::Kept && !again.proof_kept);
    server.with(|s| {
        for device in &mut s.libraries.get_mut(&made.library_id()).unwrap().devices {
            device.wrap = None;
        }
    });
    let bare = create_library_with(&server, &made, now).await.unwrap();
    assert!(bare.wrap == WrapListing::Dropped && !bare.proof_kept);
    let other = Credentials::for_new_device(made.library().clone()).unwrap();
    let foreign = other.device_offer().unwrap().wrap;
    server.with(|s| {
        for device in &mut s.libraries.get_mut(&made.library_id()).unwrap().devices {
            if !device.recovery {
                device.wrap = Some(foreign);
            }
        }
    });
    assert!(matches!(
        create_library_with(&server, &made, now).await,
        Err(Error::RegisteredOtherwise { credentials: None })
    ));
}

#[tokio::test]
async fn a_server_that_keeps_neither_wrap_keys_nor_proofs_registers_without_them() {
    let server = Server::new();
    server.with(|s| {
        s.stored_wrap = Some(None);
        s.stored_proof = Some(None);
    });
    let bare =
        |registered: Registered| registered.wrap == WrapListing::Dropped && !registered.proof_kept;
    let made = Credentials::for_new_library().unwrap();
    assert!(bare(
        create_library_with(&server, &made, now).await.unwrap()
    ));
    assert!(bare(
        create_library_with(&server, &made, now).await.unwrap()
    ));
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    assert!(bare(
        join_library_with(&server, &joined, now).await.unwrap()
    ));
    assert!(bare(
        join_library_with(&server, &joined, now).await.unwrap()
    ));
    let added = Credentials::for_new_device(made.library().clone()).unwrap();
    let offer = added.device_offer().unwrap();
    assert!(bare(add_device(&server, &made, &offer, now).await.unwrap()));
    let wrapped = create_library(&server, now).await.unwrap();
    let rejoined = join_library(&server, &wrapped.recovery_code(), now)
        .await
        .unwrap();
    assert_eq!(rejoined.library_id(), wrapped.library_id());

    assert_eq!(server.with(|s| s.libraries.len()), 2);
    assert_eq!(records(&server, &made).len(), 4);
    for credentials in [&made, &joined, &added] {
        let listed = record(&server, credentials);
        assert!(listed.wrap.is_none());
        assert_eq!(made.check_member(&listed), MemberCheck::Absent);
    }
    let mut peers = [
        Peer::new(made).await,
        Peer::new(joined).await,
        Peer::new(added).await,
    ];
    peers[2]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[2].sync(&server).await.unwrap();
    peers[0].sync(&server).await.unwrap();
    peers[1].sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut peers).await.len(), 1);
}

#[tokio::test]
async fn the_one_call_forms_hand_back_credentials_registered_otherwise() {
    let server = Server::new();
    let stranger = Credentials::for_new_library().unwrap();
    let foreign = stranger.device_offer().unwrap().wrap;
    server.with(|s| s.stored_wrap = Some(Some(foreign)));
    let made = match create_library(&server, now).await {
        Err(Error::RegisteredOtherwise {
            credentials: Some(credentials),
        }) => *credentials,
        other => panic!("expected the credentials back, got {other:?}"),
    };
    assert!(server.with(|s| s.libraries.contains_key(&made.library_id())));
    let listed = record(&server, &made);
    assert_eq!(listed.public_key, made.device().verifying_key());

    let joined = match join_library(&server, &made.recovery_code(), now).await {
        Err(Error::RegisteredOtherwise {
            credentials: Some(credentials),
        }) => *credentials,
        other => panic!("expected the credentials back, got {other:?}"),
    };
    assert_eq!(joined.library_id(), made.library_id());
    assert_eq!(
        record(&server, &joined).public_key,
        joined.device().verifying_key()
    );
    assert!(matches!(
        join_library_with(&server, &joined, now).await,
        Err(Error::RegisteredOtherwise { credentials: None })
    ));
}

#[tokio::test]
async fn a_server_that_keeps_the_wrap_key_and_drops_the_proof_registers_without_it() {
    let server = Server::new();
    server.with(|s| s.stored_proof = Some(None));
    let made = Credentials::for_new_library().unwrap();
    let created = create_library_with(&server, &made, now).await.unwrap();
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    let join = join_library_with(&server, &joined, now).await.unwrap();
    let added = Credentials::for_new_device(made.library().clone()).unwrap();
    let add = add_device(&server, &made, &added.device_offer().unwrap(), now)
        .await
        .unwrap();
    assert!(!created.proof_kept && !join.proof_kept && !add.proof_kept);
    assert!(
        !join_library_with(&server, &joined, now)
            .await
            .unwrap()
            .proof_kept
    );
    assert!(
        !create_library_with(&server, &made, now)
            .await
            .unwrap()
            .proof_kept
    );
    for credentials in [&made, &joined, &added] {
        let listed = record(&server, credentials);
        assert_eq!(
            listed.wrap.unwrap().wrap_key,
            credentials.wrap_key().unwrap()
        );
        assert!(listed.verify_wrap(made.library_id()));
        assert_eq!(made.check_member(&listed), MemberCheck::Absent);
    }
    let mut peers = [Peer::new(made).await, Peer::new(added).await];
    peers[1]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[1].sync(&server).await.unwrap();
    peers[0].sync(&server).await.unwrap();
    assert_eq!(assert_converged(&mut peers).await.len(), 1);
}

#[tokio::test]
async fn a_server_that_lists_an_unreadable_proof_is_reported() {
    let server = Server::new();
    server.with(|s| s.stored_proof = Some(Some(ListedMemberProof::Unreadable)));
    let made = Credentials::for_new_library().unwrap();
    assert!(matches!(
        create_library_with(&server, &made, now).await,
        Err(Error::RegisteredOtherwise { .. })
    ));
    let joined = Credentials::for_recovery_code(&made.recovery_code()).unwrap();
    assert!(matches!(
        join_library_with(&server, &joined, now).await,
        Err(Error::RegisteredOtherwise { .. })
    ));
    let added = Credentials::for_new_device(made.library().clone()).unwrap();
    assert!(matches!(
        add_device(&server, &made, &added.device_offer().unwrap(), now).await,
        Err(Error::RegisteredOtherwise { .. })
    ));
    assert_eq!(
        made.check_member(&record(&server, &joined)),
        MemberCheck::Invalid
    );
}

#[tokio::test]
async fn credentials_without_a_wrap_secret_are_reported_as_sending_none() {
    let server = Server::new();
    let keys = Credentials::for_new_library().unwrap().library().clone();
    let made = Credentials::new(keys.clone(), SigningKey::from_bytes(&[5; 32]), 0);
    let created = create_library_with(&server, &made, now).await.unwrap();
    assert_eq!(
        (created.wrap, created.proof_kept),
        (WrapListing::NoneSent, true)
    );
    let listed = record(&server, &made);
    assert!(listed.wrap.is_none());
    assert_eq!(made.check_member(&listed), MemberCheck::Valid);

    let joined = Credentials::new(keys.clone(), SigningKey::from_bytes(&[6; 32]), 0);
    server.with(|s| s.stored_proof = Some(None));
    let join = join_library_with(&server, &joined, now).await.unwrap();
    assert_eq!((join.wrap, join.proof_kept), (WrapListing::NoneSent, false));
    assert_eq!(
        made.check_member(&record(&server, &joined)),
        MemberCheck::Absent
    );

    let wrapped = Credentials::for_new_device(keys).unwrap();
    server.with(|s| s.stored_proof = None);
    let join = join_library_with(&server, &wrapped, now).await.unwrap();
    assert_eq!((join.wrap, join.proof_kept), (WrapListing::Kept, true));
}
