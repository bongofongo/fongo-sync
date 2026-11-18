import FongoSync.State
import FongoSync.Alias
import FongoSync.GidSet

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

The view is computed in two steps, as in the Rust: the changes give the *inputs* (registers, the
gids seen, and each gid's canonical gid), and the inputs give the rows. Group members are visited
in ascending gid order, as a `BTreeMap` visits them.

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


/-- An alias change is valid only if it names another gid. -/
def aliasOk (c : Change Value) : Bool :=
  if c.field = ALIAS then
    match c.value with
    | .ref t => t ≠ c.gid
    | _ => false
  else true

section Inputs

variable (sc : Schema) (cs : List (Change Value))

/-- One change applied to the registers. An alias change only joins groups, as in the Rust. -/
def step (st : State Value) (c : Change Value) : State Value :=
  if c.field = ALIAS then st else State.apply sc.strategyOf st c

def registers : State Value := cs.foldl (step sc) State.empty

/-- The changes to this collection, without aliases that do not name another gid: those are
ignored outright, so they do not make a gid known. -/
def ours : List (Change Value) := cs.filter fun c => c.collection = sc.collection ∧ aliasOk c

/-- Every gid a change to this collection has named, ascending. -/
def known : List Gid := gidSet ((ours sc cs).map (·.gid))

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

/-- What the rows are computed from. -/
structure Inputs where
  registers : State Value
  gids : List Gid
  canon : Gid → Gid

def inputs : Inputs := ⟨registers sc cs, known sc cs, canonical (edges sc cs)⟩

end Inputs

section Present

variable (sc : Schema) (inp : Inputs)

def register (g : Gid) (field : String) : Option (Entry Value) :=
  inp.registers (sc.collection, g, field)

def deleted (g : Gid) : Prop :=
  ∃ e, register sc inp g DELETED = some e ∧ e.value = .int 1

/-- `o` is a known gid whose group has a member in `live`. -/
def namesLive (live : Gid → Prop) (o : Gid) : Prop :=
  o ∈ inp.gids ∧ ∃ m ∈ inp.gids, inp.canon m = inp.canon o ∧ live m

/-- An owner field allows its entity to live: it is unset, null, or names a live group. -/
def ownerAllows (live : Gid → Prop) : Option (Entry Value) → Prop
  | none => True
  | some e =>
    match e.value with
    | .null => True
    | .ref o => namesLive inp live o
    | _ => False

/-- The members found live after `k` rounds of "add every member the rule now allows". -/
def liveAt : Nat → Gid → Prop
  | 0, _ => False
  | k + 1, g =>
    liveAt k g ∨
      (g ∈ inp.gids ∧ ¬ deleted sc inp g ∧
        ∀ f ∈ sc.fields, f.kind = .owner → ownerAllows inp (liveAt k) (register sc inp g f.name))

/-- A member is live if it is live at some stage: the least set closed under the rule. -/
def Live (g : Gid) : Prop := ∃ k, liveAt sc inp k g

/-- A group's live members, ascending. -/
noncomputable def liveMembers (r : Gid) : List Gid :=
  inp.gids.filter fun m => decide (inp.canon m = r ∧ Live sc inp m)

/-- A live group's register for a field: the strategy winner among its live members. -/
noncomputable def groupEntry (f : Field) (r : Gid) : Option (Entry Value) :=
  ((liveMembers sc inp r).filterMap fun m => register sc inp m f.name).foldl (apply f.strategy) none

/-- How a field reads: references to a live group read as its canonical gid, anything else as
null. -/
noncomputable def read (f : Field) : Option (Entry Value) → Value
  | none => .null
  | some e =>
    match f.kind with
    | .plain => e.value
    | .reference | .owner =>
      match e.value with
      | .ref o => if namesLive inp (Live sc inp) o then .ref (inp.canon o) else .null
      | _ => .null

abbrev Row := List (String × Value)

/-- For each canonical gid of a live group, its row. -/
noncomputable def present (r : Gid) : Option Row :=
  if ∃ m ∈ inp.gids, inp.canon m = r ∧ Live sc inp m then
    some (sc.fields.map fun f => (f.name, read sc inp f (groupEntry sc inp f r)))
  else none

end Present

/-- **The view** of the changes `cs`. -/
noncomputable def view (sc : Schema) (cs : List (Change Value)) : Gid → Option Row :=
  present sc (inputs sc cs)

/-! ## Sanity: the definitions are not vacuous -/

/-- A known, undeleted entity with no owner fields set is live. -/
theorem live_of_unowned (sc : Schema) (inp : Inputs) (g : Gid)
    (hk : g ∈ inp.gids) (hd : ¬ deleted sc inp g)
    (ho : ∀ f ∈ sc.fields, f.kind = .owner → register sc inp g f.name = none) :
    Live sc inp g :=
  ⟨1, .inr ⟨hk, hd, fun f hf hkind => by rw [ho f hf hkind]; trivial⟩⟩

/-- A deleted entity is never live. -/
theorem not_live_of_deleted (sc : Schema) (inp : Inputs) (g : Gid)
    (hd : deleted sc inp g) : ¬ Live sc inp g := by
  rintro ⟨k, hk⟩
  induction k with
  | zero => exact hk
  | succ k ih =>
    rcases hk with hk | ⟨_, hnd, _⟩
    · exact ih hk
    · exact hnd hd

/-- An entity whose owner field names something unknown is never live. -/
theorem not_live_of_unknown_owner (sc : Schema) (inp : Inputs) (g o : Gid) (f : Field)
    (hf : f ∈ sc.fields) (hkind : f.kind = .owner) (e : Entry Value)
    (he : register sc inp g f.name = some e) (hv : e.value = .ref o) (ho : o ∉ inp.gids) :
    ¬ Live sc inp g := by
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

/-! ## The view depends only on its inputs, and the inputs only on the set of changes -/

/-- Replicas whose registers, gids and groups agree show the same view. -/
theorem view_from_inputs (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (h : inputs sc cs₁ = inputs sc cs₂) : view sc cs₁ = view sc cs₂ := by
  simp only [view, h]

theorem mem_ours_iff (sc : Schema) {cs₁ cs₂ : List (Change Value)} (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂)
    (c : Change Value) : c ∈ ours sc cs₁ ↔ c ∈ ours sc cs₂ := by
  simp [ours, same]

theorem known_eq (sc : Schema) {cs₁ cs₂ : List (Change Value)} (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) :
    known sc cs₁ = known sc cs₂ :=
  gidSet_eq_of_mem_iff fun g => by simp only [List.mem_map, mem_ours_iff sc same]

/-- Changes with the same members give the same inputs once their registers agree. -/
theorem inputs_eq (sc : Schema) {cs₁ cs₂ : List (Change Value)} (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂)
    (regs : registers sc cs₁ = registers sc cs₂) : inputs sc cs₁ = inputs sc cs₂ := by
  have hk := known_eq sc same
  have he : ∀ e, e ∈ edges sc cs₁ ↔ e ∈ edges sc cs₂ := by
    intro e
    simp only [edges, List.mem_filterMap, mem_ours_iff sc same, hk]
  simp only [inputs, regs, hk, canonical_eq_of_mem_iff _ _ he]

theorem step_swap (sc : Schema) (st : State Value) (a b : Change Value)
    (h : a.field = ALIAS ∨ b.field = ALIAS ∨ Compatible a b) :
    step sc (step sc st a) b = step sc (step sc st b) a := by
  by_cases ha : a.field = ALIAS
  · simp [step, ha]
  · by_cases hb : b.field = ALIAS
    · simp [step, hb]
    · simp only [step, ha, hb, ↓reduceIte]
      exact State.apply_swap _ _ _ _ (by simpa [ha, hb] using h)

theorem step_twice (sc : Schema) (st : State Value) (a : Change Value) :
    step sc (step sc st a) a = step sc st a := by
  by_cases ha : a.field = ALIAS
  · simp [step, ha]
  · simp only [step, ha, ↓reduceIte]; exact State.apply_twice _ _ _

theorem registers_eq_merged (sc : Schema) (cs : List (Change Value)) :
    registers sc cs = merged sc.strategyOf (cs.filter fun c => c.field ≠ ALIAS) := by
  simp only [registers, merged, List.foldl_filter]
  congr
  funext st c
  by_cases h : c.field = ALIAS <;> simp [h, step]

/-- Swapping two neighbouring changes leaves the view the same, if either is an alias or they do
not write one register at one clock with different values. -/
theorem view_swap (sc : Schema) (pre post : List (Change Value)) (a b : Change Value)
    (h : a.field = ALIAS ∨ b.field = ALIAS ∨ Compatible a b) :
    view sc (pre ++ a :: b :: post) = view sc (pre ++ b :: a :: post) := by
  apply view_from_inputs
  apply inputs_eq
  · intro c; simp only [List.mem_append, List.mem_cons]
    constructor <;> rintro (h | h | h | h) <;> simp [h]
  · simp only [registers, List.foldl_append, List.foldl_cons]
    rw [step_swap sc _ a b h]

/-- Receiving a change twice in a row leaves the view as receiving it once. -/
theorem view_twice (sc : Schema) (pre post : List (Change Value)) (a : Change Value) :
    view sc (pre ++ a :: a :: post) = view sc (pre ++ a :: post) := by
  apply view_from_inputs
  apply inputs_eq
  · intro c; simp only [List.mem_append, List.mem_cons]
    constructor
    · rintro (h | h | h | h) <;> simp [h]
    · rintro (h | h | h) <;> simp [h]
  · simp only [registers, List.foldl_append, List.foldl_cons]
    rw [step_twice]

/-- **The view is a function of the set of changes.** Two replicas that have seen the same set of
changes show the same data, whatever order the changes arrived in and however often. -/
theorem view_eq_of_mem_iff (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁) :
    view sc cs₁ = view sc cs₂ := by
  apply view_from_inputs
  apply inputs_eq sc same
  rw [registers_eq_merged, registers_eq_merged]
  apply merged_eq_of_mem_iff
  · intro c; simp [same]
  · intro a ha b hb
    exact unique a (List.mem_filter.mp ha).1 b (List.mem_filter.mp hb).1

theorem view_perm (sc : Schema) {cs₁ cs₂ : List (Change Value)} (p : cs₁.Perm cs₂)
    (unique : ClockDetermines cs₁) : view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc (fun _ => p.mem_iff) unique

theorem view_dup (sc : Schema) (cs : List (Change Value)) (unique : ClockDetermines cs) :
    view sc (cs ++ cs) = view sc cs :=
  view_eq_of_mem_iff sc (by simp) (by
    intro a ha b hb; simp only [List.mem_append, or_self] at ha hb; exact unique a ha b hb)

end FongoSync
