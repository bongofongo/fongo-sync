use std::collections::{BTreeMap, BTreeSet};

use crate::change::Change;
use crate::eval::{Evaluator, Undetermined};
use crate::model::{FieldKind, Identity};
use crate::source::{Body, Overlay, Source, admit, alters};
use crate::value::{Gid, Value};

impl Evaluator<'_> {
    /// The (collection, canonical gid) pairs whose row may differ once `batch` is merged into
    /// `before`: every pair `model::view` shows before or after the merge whose row, or whether
    /// it is shown at all, differs. Call it before merging the batch.
    ///
    /// The result may name pairs whose row has not changed, and it names the canonical gids of
    /// both the old and the new groups wherever groups join or split. Bringing exactly these
    /// pairs to their new [`Target`](crate::eval::Target) (a row under the canonical gid when it
    /// is live and canonical, no row otherwise) turns the view before into the view after.
    ///
    /// Works outward from the gids named by the changes that alter what `before` keeps (a change
    /// already merged, or one that loses to what is kept, affects nothing). For each gid it
    /// compares the target before and after; where they differ it visits every member of both
    /// groups, and where the gid's canonical gid, its collection or its group's liveness differs
    /// it also visits every gid holding a reference to it in a reference, owner or natural key
    /// field, since those read nothing else about it. Cost: proportional
    /// to the size of the groups whose targets change and the number of gids referring to them,
    /// times the cost of [`Session::target`](crate::eval::Session::target) for each, which a
    /// session shares among a group's members.
    ///
    /// `Undetermined` if a target it needs is; [`Evaluator::everything`] is then the fallback.
    pub fn affected<S: Source>(
        &self,
        before: &S,
        batch: &[Change],
    ) -> Result<BTreeSet<(String, Gid)>, Undetermined> {
        let schema = self.schema;
        let batch: Vec<Change> = batch
            .iter()
            .filter(|c| admit(schema, c).is_some_and(|f| alters(before, &f)))
            .cloned()
            .collect();
        let after = Overlay::new(schema, before, &batch);
        let mut was = self.session(before);
        let mut will = self.session(&after);

        let mut referring: Vec<(&str, &str)> = Vec::new();
        for c in &schema.collections {
            let key: &[String] = match &c.identity {
                Identity::Natural(key) => key,
                Identity::Minted => &[],
            };
            for f in &c.fields {
                if matches!(f.kind, FieldKind::Reference(_) | FieldKind::Owner(_))
                    || key.contains(&f.name)
                {
                    referring.push((&c.name, &f.name));
                }
            }
        }

        let mut queue: Vec<Gid> = Vec::new();
        for change in &batch {
            if let Some(fact) = admit(schema, change) {
                queue.push(fact.gid);
                if let Body::Alias { target } = fact.body {
                    queue.push(target);
                }
            }
        }
        let mut seen = BTreeSet::new();
        let mut expanded = BTreeSet::new();
        let mut compared = BTreeMap::new();
        let mut queued = BTreeSet::new();
        let mut found = BTreeSet::new();
        while let Some(x) = queue.pop() {
            crate::work::add(1);
            if !seen.insert(x) {
                continue;
            }
            let mut homes = BTreeSet::new();
            homes.extend(was.home(x)?.map(|c| c.name.clone()));
            homes.extend(will.home(x)?.map(|c| c.name.clone()));
            for collection in homes {
                let old = was.shared(&collection, x)?;
                let new = will.shared(&collection, x)?;
                // Every member of a group shares its target, so a pair of groups is compared once,
                // and each group's members are queued once.
                let pair = (
                    collection.clone(),
                    (old.canonical, old.members.is_empty()),
                    (new.canonical, new.members.is_empty()),
                );
                let differs = *compared.entry(pair).or_insert_with(|| {
                    // Targets differ at the canonical gid or the number of members before the
                    // member lists are read.
                    let read =
                        old.canonical == new.canonical && old.members.len() == new.members.len();
                    crate::work::add(1 + if read { old.members.len() } else { 0 });
                    old != new
                });
                if !differs {
                    continue;
                }
                for (t, was_before) in [(&old, true), (&new, false)] {
                    if t.members.is_empty() {
                        continue;
                    }
                    if queued.insert((collection.clone(), t.canonical, was_before)) {
                        found.insert((collection.clone(), t.canonical));
                        queue.extend(t.members.iter());
                    }
                }
                // A reference to x reads x's canonical gid while its group is live, and a natural
                // key naming x reads its canonical gid: nothing else about x reaches them.
                let read_differently = old.canonical != new.canonical
                    || old.members.is_empty() != new.members.is_empty()
                    || old.row.is_some() != new.row.is_some();
                if read_differently && expanded.insert(x) {
                    for (c, f) in &referring {
                        queue.extend(after.holding(c, &[(f, &Value::Ref(x))]));
                    }
                }
            }
        }
        Ok(found)
    }
}
