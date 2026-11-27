use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::clock::Hlc;
use crate::value::{Gid, Value};

/// The field that carries an entity's tombstone: `Int(1)` deleted, `Int(0)` restored.
pub const DELETED: &str = "$deleted";

/// The field that says an entity is the same as another: its value is `Ref` of the other. Aliases
/// only accumulate, so it is never resolved by clock like an ordinary field.
pub const ALIAS: &str = "$alias";

/// One field of one entity took a value at a moment. The whole of what replicas exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub collection: String,
    pub gid: Gid,
    pub field: String,
    pub value: Value,
    pub clock: Hlc,
}

/// How concurrent changes to one field resolve. Each orders changes totally, so the winner is a
/// function of the set of changes seen, whatever order they arrived in: replicas that have seen
/// the same changes hold the same value, even where two changes share a clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// The change with the later clock wins; of two at one clock, the greater value under
    /// [`Value::total_cmp`]. Honest clocks are never equal; equal ones come from two databases
    /// sharing one identity, such as a copied file or a restored backup.
    Lww,
    /// The greater value wins, whenever it was written; of two equal values, the later clock.
    Max,
}

/// Whether an incoming change replaces what is stored: whether `(clock, value)` is greater than
/// what is stored for `Lww`, or `(value, clock)` for `Max`.
pub fn wins(
    strategy: Strategy,
    stored: Option<(&Value, &Hlc)>,
    value: &Value,
    clock: &Hlc,
) -> bool {
    let Some((stored_value, stored_clock)) = stored else {
        return true;
    };
    match strategy {
        Strategy::Lww => match clock.cmp(stored_clock) {
            Ordering::Greater => true,
            Ordering::Equal => value.total_cmp(stored_value) == Ordering::Greater,
            Ordering::Less => false,
        },
        Strategy::Max => match value.total_cmp(stored_value) {
            Ordering::Greater => true,
            Ordering::Equal => clock > stored_clock,
            Ordering::Less => false,
        },
    }
}

/// A replica's merged state held in memory: the reference the storage adapters are tested against.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct State {
    fields: BTreeMap<(String, Gid, String), (Value, Hlc)>,
}

impl State {
    pub fn apply(&mut self, change: &Change, strategy: Strategy) -> bool {
        let strategy = if change.field == DELETED {
            Strategy::Lww
        } else {
            strategy
        };
        let key = (change.collection.clone(), change.gid, change.field.clone());
        let stored = self.fields.get(&key).map(|(v, c)| (v, c));
        if !wins(strategy, stored, &change.value, &change.clock) {
            return false;
        }
        self.fields
            .insert(key, (change.value.clone(), change.clock));
        true
    }

    pub fn get(&self, collection: &str, gid: Gid, field: &str) -> Option<&(Value, Hlc)> {
        self.fields
            .get(&(collection.to_string(), gid, field.to_string()))
    }

    pub fn is_deleted(&self, collection: &str, gid: Gid) -> bool {
        matches!(self.get(collection, gid, DELETED), Some((Value::Int(1), _)))
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Change, DELETED, State, Strategy as Merge};
    use crate::clock::Hlc;
    use crate::value::{Gid, Value};

    fn strategy_of(field: &str) -> Merge {
        if field == "high" {
            Merge::Max
        } else {
            Merge::Lww
        }
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            (0i64..4).prop_map(Value::Int),
            "[ab]{0,2}".prop_map(Value::Text),
        ]
    }

    /// Changes whose clocks come from a small range, so two changes to one field often share a
    /// clock with different values.
    fn arb_changes() -> impl Strategy<Value = Vec<Change>> {
        let one = (
            0u8..3,
            prop_oneof![Just("title"), Just("high"), Just(DELETED)],
            arb_value(),
            0u64..3,
            0u32..2,
            0u8..2,
        );
        proptest::collection::vec(one, 0..40).prop_map(|raw| {
            raw.into_iter()
                .enumerate()
                .map(
                    |(i, (entity, field, value, wall_ms, counter, node))| Change {
                        collection: "things".into(),
                        gid: Gid([entity; 16]),
                        field: field.into(),
                        value: if field == DELETED {
                            Value::Int(i as i64 % 2)
                        } else {
                            value
                        },
                        clock: Hlc {
                            wall_ms,
                            counter,
                            node: Gid([node; 16]),
                        },
                    },
                )
                .collect()
        })
    }

    fn merged(changes: &[Change]) -> State {
        let mut state = State::default();
        for change in changes {
            state.apply(change, strategy_of(&change.field));
        }
        state
    }

    proptest! {
        #[test]
        fn any_delivery_order_reaches_the_same_state(
            (changes, shuffled) in arb_changes().prop_flat_map(|c| (Just(c.clone()), Just(c).prop_shuffle()))
        ) {
            prop_assert_eq!(merged(&changes), merged(&shuffled));
        }

        #[test]
        fn delivering_a_change_twice_changes_nothing(changes in arb_changes()) {
            let mut twice = changes.clone();
            twice.extend(changes.iter().cloned());
            prop_assert_eq!(merged(&changes), merged(&twice));
        }
    }

    #[test]
    fn of_two_changes_at_one_clock_the_greater_value_wins_in_either_order() {
        let at = Hlc {
            wall_ms: 5,
            counter: 0,
            node: Gid([1; 16]),
        };
        let change = |field: &str, value: Value| Change {
            collection: "things".into(),
            gid: Gid([0; 16]),
            field: field.into(),
            value,
            clock: at,
        };
        let pairs = [
            ("title", Value::Text("a".into()), Value::Text("b".into())),
            ("title", Value::Null, Value::Int(0)),
            ("high", Value::Int(1), Value::Int(2)),
            (DELETED, Value::Int(0), Value::Int(1)),
        ];
        for (field, smaller, greater) in pairs {
            for order in [
                [
                    change(field, smaller.clone()),
                    change(field, greater.clone()),
                ],
                [
                    change(field, greater.clone()),
                    change(field, smaller.clone()),
                ],
            ] {
                let state = merged(&order);
                assert_eq!(
                    state.get("things", Gid([0; 16]), field),
                    Some(&(greater.clone(), at)),
                    "{field}"
                );
            }
        }
        assert!(
            merged(&[
                change(DELETED, Value::Int(1)),
                change(DELETED, Value::Int(0))
            ])
            .is_deleted("things", Gid([0; 16]))
        );
    }

    #[test]
    fn the_later_write_wins_and_the_greater_value_wins() {
        let clock = |wall_ms| Hlc {
            wall_ms,
            counter: 0,
            node: Gid([1; 16]),
        };
        let change = |field: &str, value: i64, wall_ms| Change {
            collection: "things".into(),
            gid: Gid([0; 16]),
            field: field.into(),
            value: Value::Int(value),
            clock: clock(wall_ms),
        };
        let state = merged(&[
            change("title", 9, 1),
            change("title", 2, 5),
            change("high", 9, 1),
            change("high", 2, 5),
        ]);
        assert_eq!(
            state.get("things", Gid([0; 16]), "title").unwrap().0,
            Value::Int(2)
        );
        assert_eq!(
            state.get("things", Gid([0; 16]), "high").unwrap().0,
            Value::Int(9)
        );
    }
}
