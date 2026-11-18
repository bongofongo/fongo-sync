use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::value::Value;

/// A value ordered by [`Value::total_cmp`], so that it can key a map.
#[derive(Debug, Clone)]
pub(crate) struct Total(pub Value);

impl PartialEq for Total {
    fn eq(&self, other: &Total) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Total {}

impl PartialOrd for Total {
    fn partial_cmp(&self, other: &Total) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Total {
    fn cmp(&self, other: &Total) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

pub(crate) fn same(a: &Value, b: &Value) -> bool {
    a.total_cmp(b) == Ordering::Equal
}

pub(crate) fn same_row(a: &BTreeMap<String, Value>, b: &BTreeMap<String, Value>) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((fa, va), (fb, vb))| fa == fb && same(va, vb))
}
