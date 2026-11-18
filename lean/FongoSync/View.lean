import FongoSync.State
import FongoSync.Alias

/-!
# The view

What an application sees, given a schema and a set of changes: the live alias groups of one
collection, each with its field values, references rewritten to canonical gids. It states what
replicas must converge to, cut down to one collection and to what is listed below.

Covered: registers (`Lww` and `Max`, `$deleted` merging `Lww`), alias edges and groups, liveness
through owners, per-field group values from live members, and reading references.

Not covered: several collections and which collection claims a gid; natural keys and the
closure of groups under equal keys; `together` sets; the filter on clocks that do not survive
`Hlc::encode`; and the `Real` variant of `Value`.

Liveness is the least set closed under the rule, stated as "live at some stage". It is a
proposition, not a computation, so `view` is a specification: it cannot be run.
-/

namespace FongoSync

open Std Classical

set_option linter.unusedSectionVars false

/-- `Value` without `Real`. -/
inductive Value where
  | null
  | int (i : Int)
  | text (s : String)
  | blob (b : List UInt8)
  | ref (g : Gid)
  deriving DecidableEq, Repr

def Value.rank : Value → Nat
  | .null => 0
  | .int _ => 1
  | .text _ => 2
  | .blob _ => 3
  | .ref _ => 4

/-- `Value::total_cmp` without `Real`: by kind, then by payload. -/
def Value.cmp : Value → Value → Ordering
  | .int a, .int b => compare a b
  | .text a, .text b => compare a b
  | .blob a, .blob b => compare a b
  | .ref a, .ref b => compare a b
  | a, b => compare a.rank b.rank

instance : Ord Value := ⟨Value.cmp⟩

instance : TransOrd Value where
  eq_swap {a b} := by
    change Value.cmp a b = (Value.cmp b a).swap
    cases a <;> cases b <;> simp only [Value.cmp, Value.rank] <;>
      first | rfl | exact OrientedOrd.eq_swap
  isLE_trans {a b c} hab hbc := by
    change (Value.cmp a b).isLE at hab
    change (Value.cmp b c).isLE at hbc
    change (Value.cmp a c).isLE
    cases a <;> cases b <;> cases c <;> simp only [Value.cmp, Value.rank] at * <;>
      first | rfl | exact TransOrd.isLE_trans hab hbc | contradiction

instance : LawfulEqOrd Value where
  eq_of_compare {a b} h := by
    change Value.cmp a b = .eq at h
    cases a <;> cases b <;> simp only [Value.cmp, Value.rank] at h <;>
      first
        | rfl
        | contradiction
        | rw [LawfulEqOrd.eq_of_compare h]

/-- `ALIAS`: the field whose value, a reference to another gid, says that the two are one. -/
def ALIAS : String := "$alias"

inductive FieldKind where
  | plain
  | reference
  | owner
  deriving DecidableEq, Repr

structure Field where
  name : String
  kind : FieldKind
  strategy : Strategy

/-- One collection: its name and declared fields. References and owners point within it. -/
structure Schema where
  collection : String
  fields : List Field

def Schema.strategyOf (sc : Schema) (_collection field : String) : Strategy :=
  ((sc.fields.find? (·.name = field)).map (·.strategy)).getD .lww

section View

variable (sc : Schema) (cs : List (Change Value))

/-- The changes to this collection. -/
def ours : List (Change Value) := cs.filter (·.collection = sc.collection)

/-- Every gid a change has named. -/
def known : List Gid := (ours sc cs).map (·.gid)

def register (g : Gid) (field : String) : Option (Entry Value) :=
  merged sc.strategyOf cs (sc.collection, g, field)

/-- The other end of an alias change, if it is one. -/
def aliasTarget (c : Change Value) : Option Gid :=
  if c.field = ALIAS then
    match c.value with
    | .ref t => if t ≠ c.gid then some t else none
    | _ => none
  else none

/-- Every alias edge seen whose target is a known gid. -/
def edges : List Edge :=
  (ours sc cs).filterMap fun c =>
    match aliasTarget c with
    | some t => if t ∈ known sc cs then some (c.gid, t) else none
    | none => none

def canon : Gid → Gid := canonical (edges sc cs)

def deleted (g : Gid) : Prop :=
  ∃ e, register sc cs g DELETED = some e ∧ e.value = .int 1

/-- `o` is a known gid whose group has a member in `live`. -/
def namesLive (live : Gid → Prop) (o : Gid) : Prop :=
  o ∈ known sc cs ∧ ∃ m ∈ known sc cs, canon sc cs m = canon sc cs o ∧ live m

/-- An owner field allows its entity to live: it is unset, null, or names a live group. -/
def ownerAllows (live : Gid → Prop) : Option (Entry Value) → Prop
  | none => True
  | some e =>
    match e.value with
    | .null => True
    | .ref o => namesLive sc cs live o
    | _ => False

/-- The members found live after `k` rounds of "add every member the rule now allows". -/
def liveAt : Nat → Gid → Prop
  | 0, _ => False
  | k + 1, g =>
    liveAt k g ∨
      (g ∈ known sc cs ∧ ¬ deleted sc cs g ∧
        ∀ f ∈ sc.fields, f.kind = .owner → ownerAllows sc cs (liveAt k) (register sc cs g f.name))

/-- A member is live if it is live at some stage: the least set closed under the rule. -/
def Live (g : Gid) : Prop := ∃ k, liveAt sc cs k g

noncomputable def liveMembers (r : Gid) : List Gid :=
  (known sc cs).filter fun m => decide (canon sc cs m = r ∧ Live sc cs m)

/-- A live group's register for a field: the strategy winner among its live members. -/
noncomputable def groupEntry (f : Field) (r : Gid) : Option (Entry Value) :=
  ((liveMembers sc cs r).filterMap fun m => register sc cs m f.name).foldl (apply f.strategy) none

/-- How a field reads: references to a live group read as its canonical gid, anything else as
null. -/
noncomputable def read (f : Field) : Option (Entry Value) → Value
  | none => .null
  | some e =>
    match f.kind with
    | .plain => e.value
    | .reference | .owner =>
      match e.value with
      | .ref o => if namesLive sc cs (Live sc cs) o then .ref (canon sc cs o) else .null
      | _ => .null

abbrev Row := List (String × Value)

/-- **The view**: for each canonical gid of a live group, its row. -/
noncomputable def view (r : Gid) : Option Row :=
  if ∃ m ∈ known sc cs, canon sc cs m = r ∧ Live sc cs m then
    some (sc.fields.map fun f => (f.name, read sc cs f (groupEntry sc cs f r)))
  else none

end View

/-! ## Sanity: the definitions are not vacuous -/

/-- A known, undeleted entity with no owner fields set is live. -/
theorem live_of_unowned (sc : Schema) (cs : List (Change Value)) (g : Gid)
    (hk : g ∈ known sc cs) (hd : ¬ deleted sc cs g)
    (ho : ∀ f ∈ sc.fields, f.kind = .owner → register sc cs g f.name = none) :
    Live sc cs g :=
  ⟨1, .inr ⟨hk, hd, fun f hf hkind => by rw [ho f hf hkind]; trivial⟩⟩

/-- A deleted entity is never live. -/
theorem not_live_of_deleted (sc : Schema) (cs : List (Change Value)) (g : Gid)
    (hd : deleted sc cs g) : ¬ Live sc cs g := by
  rintro ⟨k, hk⟩
  induction k with
  | zero => exact hk
  | succ k ih => rcases hk with hk | ⟨_, hnd, _⟩
                 · exact ih hk
                 · exact hnd hd

/-- An entity whose owner field names something unknown is never live. -/
theorem not_live_of_unknown_owner (sc : Schema) (cs : List (Change Value)) (g o : Gid) (f : Field)
    (hf : f ∈ sc.fields) (hkind : f.kind = .owner) (e : Entry Value)
    (he : register sc cs g f.name = some e) (hv : e.value = .ref o) (ho : o ∉ known sc cs) :
    ¬ Live sc cs g := by
  rintro ⟨k, hk⟩
  induction k with
  | zero => exact hk
  | succ k ih =>
    rcases hk with hk | ⟨_, _, hall⟩
    · exact ih hk
    · have := hall f hf hkind
      rw [he] at this
      simp only [ownerAllows, hv, namesLive] at this
      exact ho this.1

/-! ## The view depends only on the set of changes -/

section Invariance

variable (sc : Schema) {cs₁ cs₂ : List (Change Value)}
  (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁)

include same in
theorem mem_ours_iff (c : Change Value) : c ∈ ours sc cs₁ ↔ c ∈ ours sc cs₂ := by
  simp [ours, same]

include same in
theorem mem_known_iff (g : Gid) : g ∈ known sc cs₁ ↔ g ∈ known sc cs₂ := by
  simp only [known, List.mem_map, mem_ours_iff sc same]

include same unique in
theorem register_eq : register sc cs₁ = register sc cs₂ := by
  funext g f
  simp only [register, merged_eq_of_mem_iff sc.strategyOf cs₁ cs₂ same unique]

include same in
theorem mem_edges_iff (e : Edge) : e ∈ edges sc cs₁ ↔ e ∈ edges sc cs₂ := by
  simp only [edges, List.mem_filterMap, mem_ours_iff sc same, mem_known_iff sc same]

include same in
theorem canon_eq : canon sc cs₁ = canon sc cs₂ :=
  canonical_eq_of_mem_iff _ _ (mem_edges_iff sc same)

include same in
theorem namesLive_eq : namesLive sc cs₁ = namesLive sc cs₂ := by
  funext live o
  simp only [namesLive, mem_known_iff sc same, canon_eq sc same]

include same in
theorem ownerAllows_eq : ownerAllows sc cs₁ = ownerAllows sc cs₂ := by
  funext live v
  cases v with
  | none => rfl
  | some e => simp only [ownerAllows, namesLive_eq sc same]

include same unique in
theorem liveAt_eq (k : Nat) : liveAt sc cs₁ k = liveAt sc cs₂ k := by
  induction k with
  | zero => funext g; rfl
  | succ k ih =>
    funext g
    simp only [liveAt, ih, deleted, register_eq sc same unique, mem_known_iff sc same,
      ownerAllows_eq sc same]

include same unique in
theorem live_eq : Live sc cs₁ = Live sc cs₂ := by
  funext g
  simp only [Live, liveAt_eq sc same unique]

include same unique in
theorem mem_liveMembers_iff (r m : Gid) :
    m ∈ liveMembers sc cs₁ r ↔ m ∈ liveMembers sc cs₂ r := by
  simp only [liveMembers, List.mem_filter, mem_known_iff sc same, canon_eq sc same,
    live_eq sc same unique]

/-- Whatever a register holds came from a change to that field. -/
theorem register_from (cs : List (Change Value)) {g : Gid} {name : String} {e : Entry Value}
    (h : register sc cs g name = some e) : ∃ c ∈ cs, c.field = name ∧ c.entry = e := by
  simp only [register, merged_at] at h
  obtain ⟨c, hc, hk, he⟩ := mem_entriesAt.mp (fold_apply_mem _ _ _ h)
  exact ⟨c, hc, by simp only [Change.key, Prod.mk.injEq] at hk; exact hk.2.2, he⟩

include same unique in
theorem groupEntry_eq (f : Field) (r : Gid) : groupEntry sc cs₁ f r = groupEntry sc cs₂ f r := by
  unfold groupEntry
  apply fold_apply_eq_of_mem_iff
  · intro e
    simp only [List.mem_filterMap, mem_liveMembers_iff sc same unique, register_eq sc same unique]
  · intro a ha b hb hab
    obtain ⟨ma, _, ra⟩ := List.mem_filterMap.mp ha
    obtain ⟨mb, _, rb⟩ := List.mem_filterMap.mp hb
    obtain ⟨ca, hca, fa, rfl⟩ := register_from sc cs₁ ra
    obtain ⟨cb, hcb, fb, rfl⟩ := register_from sc cs₁ rb
    have hv := unique ca hca cb hcb (fa.trans fb.symm) hab
    simp only [Change.entry] at hab ⊢
    rw [hv, hab]

include same unique in
theorem read_eq (f : Field) (e : Option (Entry Value)) : read sc cs₁ f e = read sc cs₂ f e := by
  cases e with
  | none => rfl
  | some e =>
    unfold read
    rw [namesLive_eq sc same, live_eq sc same unique, canon_eq sc same]

include same unique in
/-- **The view is a function of the set of changes.** Two replicas that have seen the same set of
changes show the same data, whatever order the changes arrived in and however often. -/
theorem view_eq_of_mem_iff : view sc cs₁ = view sc cs₂ := by
  funext r
  simp only [view, mem_known_iff sc same, canon_eq sc same, live_eq sc same unique,
    groupEntry_eq sc same unique, read_eq sc same unique]

end Invariance

theorem view_perm (sc : Schema) {cs₁ cs₂ : List (Change Value)} (p : cs₁.Perm cs₂)
    (unique : ClockDetermines cs₁) : view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc (fun _ => p.mem_iff) unique

theorem view_dup (sc : Schema) (cs : List (Change Value)) (unique : ClockDetermines cs) :
    view sc (cs ++ cs) = view sc cs :=
  view_eq_of_mem_iff sc (by simp) (by
    intro a ha b hb; simp only [List.mem_append, or_self] at ha hb; exact unique a ha b hb)

end FongoSync
