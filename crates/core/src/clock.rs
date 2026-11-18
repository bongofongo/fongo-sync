use crate::value::Gid;

const WALL_MAX: u64 = 0xFFFF_FFFF_FFFF;

/// A hybrid logical clock reading. Ordered by wall time, then counter, then node, so no two
/// readings from different nodes are ever equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc {
    pub wall_ms: u64,
    pub counter: u32,
    pub node: Gid,
}

impl Hlc {
    pub fn zero(node: Gid) -> Hlc {
        Hlc {
            wall_ms: 0,
            counter: 0,
            node,
        }
    }

    /// Whether the text form holds this reading. One with a later wall time would be stored as
    /// another, so a replica that receives it ignores it rather than read it back differently.
    pub fn encodable(&self) -> bool {
        self.wall_ms <= WALL_MAX
    }

    /// Fixed-width text whose byte order is the clock order, so a database can compare it.
    pub fn encode(&self) -> String {
        format!(
            "{:012x}{:08x}{}",
            self.wall_ms.min(WALL_MAX),
            self.counter,
            self.node.to_hex()
        )
    }

    pub fn decode(text: &str) -> Option<Hlc> {
        if text.len() != 52 || !text.is_ascii() {
            return None;
        }
        Some(Hlc {
            wall_ms: u64::from_str_radix(&text[..12], 16).ok()?,
            counter: u32::from_str_radix(&text[12..20], 16).ok()?,
            node: Gid::from_hex(&text[20..])?,
        })
    }
}

/// One node's clock. Every reading it issues is greater than any it has issued or observed.
#[derive(Debug, Clone)]
pub struct Clock {
    last: Hlc,
}

impl Clock {
    pub fn new(node: Gid) -> Clock {
        Clock {
            last: Hlc::zero(node),
        }
    }

    pub fn resume(last: Hlc) -> Clock {
        Clock { last }
    }

    pub fn last(&self) -> Hlc {
        self.last
    }

    /// A reading for an event that happened at `wall_ms`, which may be in the past.
    pub fn tick(&mut self, wall_ms: u64) -> Hlc {
        let wall_ms = wall_ms.min(WALL_MAX);
        if wall_ms > self.last.wall_ms {
            self.last.wall_ms = wall_ms;
            self.last.counter = 0;
        } else if let Some(next) = self.last.counter.checked_add(1) {
            self.last.counter = next;
        } else {
            self.last.wall_ms += 1;
            self.last.counter = 0;
        }
        self.last
    }

    /// Moves past a reading from elsewhere. A reading the text form cannot hold is ignored.
    pub fn observe(&mut self, remote: &Hlc) {
        if remote.encodable()
            && (remote.wall_ms, remote.counter) > (self.last.wall_ms, self.last.counter)
        {
            self.last.wall_ms = remote.wall_ms;
            self.last.counter = remote.counter;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u8) -> Gid {
        Gid([n; 16])
    }

    #[test]
    fn readings_only_increase_even_when_the_wall_goes_backwards() {
        let mut clock = Clock::new(node(1));
        let a = clock.tick(1000);
        let b = clock.tick(900);
        let c = clock.tick(1000);
        let d = clock.tick(1001);
        assert!(a < b && b < c && c < d);
        assert_eq!(d.counter, 0);
    }

    #[test]
    fn a_reading_after_observing_is_later_than_what_was_observed() {
        let mut far = Clock::new(node(2));
        let remote = far.tick(5000);
        let mut clock = Clock::new(node(1));
        clock.observe(&remote);
        assert!(clock.tick(10) > remote);
    }

    #[test]
    fn the_text_form_round_trips_and_sorts_like_the_clock() {
        let mut clock = Clock::new(node(9));
        let a = clock.tick(1);
        let b = clock.tick(70_000);
        assert_eq!(Hlc::decode(&a.encode()), Some(a));
        assert!(a.encode() < b.encode());
        assert_eq!(a.encode().len(), 52);
    }

    #[test]
    fn a_reading_beyond_the_text_form_is_not_encodable_and_not_observed() {
        let beyond = Hlc {
            wall_ms: WALL_MAX + 1,
            counter: 0,
            node: node(7),
        };
        let edge = Hlc {
            wall_ms: WALL_MAX,
            ..beyond
        };
        assert!(!beyond.encodable());
        assert!(edge.encodable());
        assert_eq!(Hlc::decode(&edge.encode()), Some(edge));
        let mut clock = Clock::new(node(1));
        clock.observe(&beyond);
        assert_eq!(clock.last(), Hlc::zero(node(1)));
    }

    #[test]
    fn observing_a_reading_beyond_the_text_form_never_moves_the_clock_back() {
        for wall_ms in [WALL_MAX + 1, u64::MAX] {
            let last = Hlc {
                wall_ms: WALL_MAX,
                counter: 5,
                node: node(1),
            };
            let mut clock = Clock::resume(last);
            clock.observe(&Hlc {
                wall_ms,
                counter: 0,
                node: node(2),
            });
            assert_eq!(clock.last(), last);
            assert!(clock.tick(0) > last);
        }
    }

    #[test]
    fn a_full_counter_carries_into_the_wall() {
        let mut clock = Clock::resume(Hlc {
            wall_ms: 5,
            counter: u32::MAX,
            node: node(1),
        });
        let next = clock.tick(5);
        assert_eq!((next.wall_ms, next.counter), (6, 0));
    }
}
