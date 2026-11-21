use fongo_sync_protocol::api::Notification;

/// Whether a change notification says the log holds something past `cursor`, the position
/// [`crate::cursor`] reports. A notification is only a hint; syncing anyway is always safe.
pub fn worth_syncing(notification: &Notification, cursor: u64) -> bool {
    notification.seq > cursor
}

/// [`worth_syncing`] for a notification message as received. One that does not decode is a
/// reason to sync, as the protocol asks.
pub fn message_worth_syncing(message: &[u8], cursor: u64) -> bool {
    Notification::decode(message)
        .ok()
        .is_none_or(|n| worth_syncing(&n, cursor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_later_head_or_an_unreadable_message_is_worth_a_sync() {
        let at = |seq| Notification { seq };
        assert!(worth_syncing(&at(5), 4));
        assert!(!worth_syncing(&at(4), 4));
        assert!(!worth_syncing(&at(3), 4));
        assert!(message_worth_syncing(&at(5).encode(), 4));
        assert!(!message_worth_syncing(&at(4).encode(), 4));
        assert!(message_worth_syncing(b"", 4));
        assert!(message_worth_syncing(&[2, 0, 0, 0, 0, 0, 0, 0, 1], 4));
    }
}
