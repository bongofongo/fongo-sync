use std::collections::BTreeSet;

use crate::change::Change;
use crate::eval::{Evaluator, Undetermined};
use crate::model::{FieldKind, Identity};
use crate::source::{Body, Overlay, Source, admit};
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
    /// Works outward from the gids the batch names, comparing each gid's target before and after
    /// and, where it differs, visiting every member of both groups and every gid holding a
    /// reference to one of them in a reference, owner or natural key field. Cost: proportional
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
        let after = Overlay::new(schema, before, batch);
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
        for change in batch {
            if let Some(fact) = admit(schema, change) {
                queue.push(fact.gid);
                if let Body::Alias { target } = fact.body {
                    queue.push(target);
                }
            }
        }
        let mut seen = BTreeSet::new();
        let mut expanded = BTreeSet::new();
        let mut found = BTreeSet::new();
        while let Some(x) = queue.pop() {
            if !seen.insert(x) {
                continue;
            }
            let mut homes = BTreeSet::new();
            homes.extend(was.home(x)?.map(|c| c.name.clone()));
            homes.extend(will.home(x)?.map(|c| c.name.clone()));
            for collection in homes {
                let old = was.target(&collection, x)?;
                let new = will.target(&collection, x)?;
                if old == new {
                    continue;
                }
                for t in [&old, &new] {
                    if !t.members.is_empty() {
                        found.insert((collection.clone(), t.canonical));
                    }
                }
                for &m in old.members.iter().chain(&new.members) {
                    queue.push(m);
                    if !expanded.insert(m) {
                        continue;
                    }
                    for (c, f) in &referring {
                        queue.extend(after.holding(c, &[(f, &Value::Ref(m))]));
                    }
                }
            }
        }
        Ok(found)
    }
}
