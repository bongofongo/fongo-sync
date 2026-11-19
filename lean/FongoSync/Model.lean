import FongoSync.OrdSet

/-!
# The view over several collections

Closer to `model::view`: a schema of several collections, references and owners naming a target
collection, the rule for which collection a gid belongs to, the changes ignored as invalid, and
`together` field sets, and natural keys.

1. A change is ignored if its clock is not encodable, it is an alias whose value is not a
   reference to another gid, it is a `$deleted` whose value is neither 0 nor 1, or its collection
   is unknown. Of the rest, a gid belongs to the
   collection of its change with the smallest clock, ties going to the smaller collection name,
   and a change to that gid in another collection is ignored. In a natural collection, of the
   changes to each key field of a gid only those among the `KEY_ENTRIES` smallest
   `(clock, value)` pairs count (`earliest`). A gid is verified if some key derives it: one value
   per key field, each written to that field by a change that counts, whose `derive` is the gid.
   A change to a key field is ignored unless it counts and its value is part of that key.
2. Each (collection, gid, field) holds the winner among its changes; `$deleted` and undeclared
   fields merge `Lww`.
3. An alias whose target belongs to the same collection joins the two gids, unless either is
   unverified. Groups are also closed under natural keys that become equal once references are
   read through groups: the least such grouping. The canonical gid of a group is its smallest
   member.
4. A member is live if it is verified, not deleted, and every owner field it holds is unset, null, or names
   a gid of the owner collection whose group has a live member: the least such set.
5. A live group shows, for each field outside a `together` set, the strategy winner among its
   live members, and for each `together` set, the fields of the live member holding the latest
   clock written to any of them, a field not written at that clock reading as null.
-/

namespace FongoSync.Full

open Std Classical FongoSync

set_option linter.unusedSectionVars false

inductive Kind where
  | plain
  | reference (target : String)
  | owner (target : String)
  deriving DecidableEq, Repr

structure Field where
  name : String
  kind : Kind
  strategy : Strategy

structure Collection where
  name : String
  fields : List Field
  together : List (List String)
  /-- The key fields of a natural collection; `none` for a collection of minted gids. -/
  natural : Option (List String)

/-- `derive` is `Gid::derive`: the gid a natural collection's key names. -/
structure Schema where
  collections : List Collection
  derive : String → List Value → Gid

def Schema.find (sc : Schema) (name : String) : Option Collection :=
  sc.collections.find? (·.name = name)

def Collection.declared (c : Collection) (field : String) : Option Field :=
  c.fields.find? (·.name = field)

def Collection.inTogether (c : Collection) (field : String) : Bool :=
  c.together.any (·.contains field)

/-- How a field merges: `$deleted` and undeclared fields merge `Lww`. -/
def Schema.strategyOf (sc : Schema) (collection field : String) : Strategy :=
  match sc.find collection with
  | some c => ((c.declared field).map (·.strategy)).getD .lww
  | none => .lww

/-- A field a `together` set may hold: declared, plain, and merging `Lww`. A replica writes a set
whole as it reads it, and a reference reads as null while its target is not live there yet, so a
reference in a set would be erased everywhere by a fresh null. -/
def Collection.groupable (c : Collection) (name : String) : Prop :=
  match c.declared name with
  | some f => f.kind = .plain ∧ f.strategy = .lww
  | none => False

instance (c : Collection) (name : String) : Decidable (c.groupable name) := by
  unfold Collection.groupable; split <;> infer_instance

/-- The collections a natural collection's key fields refer to, as references or owners. -/
def Collection.keyTargets (c : Collection) : List String :=
  match c.natural with
  | none => []
  | some key => key.filterMap fun f =>
      match (c.declared f).map Field.kind with
      | some (Kind.reference t) | some (Kind.owner t) => some t
      | _ => none

def Schema.keyTargetsOf (sc : Schema) (name : String) : List String :=
  ((sc.find name).map (·.keyTargets)).getD []

/-- The collections natural keys lead to from `start`, in one step or more. A path back to `start`
passes through distinct collections, so as many rounds as there are collections find it. -/
def Schema.keyReachable (sc : Schema) (start : String) : List String :=
  (List.range sc.collections.length).foldl
    (fun acc _ => (acc ++ acc.flatMap sc.keyTargetsOf).eraseDups) (sc.keyTargetsOf start)

/-- The rules `Schema::check` enforces on self-ownership, on `together` sets and on natural keys:
no collection owns its own rows; each set is non-empty, holds only fields it may hold, and shares
none with another set; and no natural key leads, through the key fields of the collections it
refers to, back to its own collection. The convergence theorem does not need them. -/
def Schema.Restricted (sc : Schema) : Prop :=
  ∀ c ∈ sc.collections,
    (∀ f ∈ c.fields, f.kind ≠ .owner c.name) ∧
    (∀ set ∈ c.together, set ≠ [] ∧ ∀ name ∈ set, c.groupable name) ∧
    c.together.flatten.Nodup ∧
    c.name ∉ sc.keyReachable c.name

instance (sc : Schema) : Decidable sc.Restricted := by
  unfold Schema.Restricted; infer_instance

/-! ## From changes to inputs -/

/-- The changes not ignored outright. -/
def candidates (sc : Schema) (cs : List (Change Value)) : List (Change Value) :=
  cs.filter fun c =>
    decide c.clock.encodable && aliasOk c && deletedOk c && (sc.find c.collection).isSome

/-- Claims compare by clock, then collection name. -/
def claimCmp (a b : Hlc × String) : Ordering :=
  compareLex (compareOn (·.1)) (compareOn (·.2)) a b

/-- Keeping the smaller claim: `keep` keeps the greater key, so compare the other way round. -/
def claimFirst (a b : Hlc × String) : Ordering := claimCmp b a

/-- The collection a gid belongs to: that of its change with the smallest claim. -/
def home (_sc : Schema) (cs : List (Change Value)) (g : Gid) : Option String :=
  (((cs.filter (·.gid = g)).map fun c => (c.clock, c.collection)).foldl
    (keep claimFirst id) none).map (·.2)

section Inputs

/-! In this section `cs` is a list of candidate changes. -/

variable (sc : Schema) (cs : List (Change Value))


/-- The changes that count: valid, and to the collection their gid belongs to. -/
def claimed : List (Change Value) :=
  cs.filter fun c => home sc cs c.gid = some c.collection

/-- The collection a gid belongs to, as declared. -/
def homeCollection (g : Gid) : Option Collection := (home sc cs g).bind sc.find

/-- The `(clock, value)` pairs that claimed changes wrote to field `f` of `g`. -/
def keyWrites (g : Gid) (f : String) : List (Hlc × Value) :=
  ((claimed sc cs).filter fun x => x.gid = g ∧ x.field = f).map fun x => (x.clock, x.value)

/-- A change to a key field counts if it is among the `KEY_ENTRIES` earliest writes to it. -/
def Counts (x : Change Value) : Prop :=
  (x.clock, x.value) ∈ earliest (keyWrites sc cs x.gid x.field)

/-- `k` derives `g`: one value per key field, each written to that field by a change to `g` that
counts, and `derive` of them is `g`. -/
def Derives (g : Gid) (c : Collection) (fields : List String) (k : List Value) : Prop :=
  k.length = fields.length ∧
    (∀ p ∈ fields.zip k, ∃ w ∈ earliest (keyWrites sc cs g p.1), w.2 = p.2) ∧
    sc.derive c.name k = g

/-- The key that derives a natural gid, if any. The Rust takes the first in sorted order; the two
agree unless two keys derive one gid, which takes a hash collision. -/
noncomputable def key (g : Gid) : Option (List Value) :=
  match homeCollection sc cs g with
  | some c =>
    match c.natural with
    | some fields => if h : ∃ k, Derives sc cs g c fields k then some (Classical.choose h) else none
    | none => none
  | none => none

/-- A natural gid that no key derives yet. -/
def Unverified (g : Gid) : Prop :=
  ∃ c fields, homeCollection sc cs g = some c ∧ c.natural = some fields ∧ key sc cs g = none

/-- A change to a key field is kept only if it counts and its value is part of the key. -/
noncomputable def keyFieldOk (x : Change Value) : Prop :=
  match homeCollection sc cs x.gid with
  | some c =>
    match c.natural with
    | some fields =>
      match fields.findIdx? (· = x.field) with
      | some i => Counts sc cs x ∧ ∃ k, key sc cs x.gid = some k ∧ k[i]? = some x.value
      | none => True
    | none => True
  | none => True

noncomputable def accepted : List (Change Value) :=
  (claimed sc cs).filter fun x => decide (keyFieldOk sc cs x)

noncomputable def registers : State Value :=
  merged sc.strategyOf ((accepted sc cs).filter fun c => c.field ≠ ALIAS)

/-- Every gid that belongs to a collection, ascending. -/
def gids : List Gid := gidSet ((claimed sc cs).map (·.gid))

/-- Every alias between verified gids of one collection. -/
noncomputable def edges : List Edge :=
  (accepted sc cs).filterMap fun c =>
    if c.field = ALIAS then
      match c.value with
      | .ref t =>
        if home sc cs t = some c.collection ∧ ¬ Unverified sc cs c.gid ∧ ¬ Unverified sc cs t then
          some (c.gid, t)
        else none
      | _ => none
    else none

/-- Two values agree once references are read through `R`. -/
def SameUnder (R : Gid → Gid → Prop) (v w : Value) : Prop :=
  v = w ∨ ∃ x y, v = .ref x ∧ w = .ref y ∧ R x y

/-- `a` and `b` are verified gids of one natural collection whose keys agree under `R`. -/
def KeyPair (R : Gid → Gid → Prop) (a b : Gid) : Prop :=
  ∃ c fields, homeCollection sc cs a = some c ∧ c.natural = some fields ∧
    home sc cs b = home sc cs a ∧ ¬ Unverified sc cs a ∧ ¬ Unverified sc cs b ∧
    ∀ f ∈ fields, ∃ ea eb, registers sc cs (c.name, a, f) = some ea ∧
      registers sc cs (c.name, b, f) = some eb ∧ SameUnder R ea.value eb.value

/-- The equivalence relation a relation generates. -/
inductive Joined (E : Gid → Gid → Prop) : Gid → Gid → Prop
  | refl (a : Gid) : Joined E a a
  | edge {a b : Gid} : E a b → Joined E a b
  | symm {a b : Gid} : Joined E a b → Joined E b a
  | trans {a b c : Gid} : Joined E a b → Joined E b c → Joined E a c

/-- Groups after `k` rounds of joining gids whose keys agree. -/
noncomputable def groupedAt : Nat → Gid → Gid → Prop
  | 0 => Joined fun a b => (a, b) ∈ edges sc cs
  | k + 1 => Joined fun a b => (a, b) ∈ edges sc cs ∨ KeyPair sc cs (groupedAt k) a b

/-- The least grouping closed under aliases and under keys that agree. -/
def Grouped (a b : Gid) : Prop := ∃ k, groupedAt sc cs k a b

/-- The smallest gid grouped with `g`. -/
noncomputable def canon (g : Gid) : Gid :=
  ((List.range (g + 1)).find? fun h => decide (Grouped sc cs g h)).getD g

structure Inputs where
  registers : State Value
  home : Gid → Option String
  gids : List Gid
  canon : Gid → Gid
  unverified : Gid → Prop

noncomputable def inputs : Inputs :=
  ⟨registers sc cs, home sc cs, gids sc cs, canon sc cs, Unverified sc cs⟩

end Inputs

/-! ## From inputs to rows -/

section Present

variable (sc : Schema) (inp : Inputs)

def register (g : Gid) (field : String) : Option (Entry Value) :=
  match inp.home g with
  | some c => inp.registers (c, g, field)
  | none => none

def deleted (g : Gid) : Prop :=
  ∃ e, register inp g DELETED = some e ∧ e.value = .int 1

/-- `names_live`: `o` belongs to `target` and its group has a member in `live`. -/
def namesLive (live : Gid → Prop) (target : String) (o : Gid) : Prop :=
  inp.home o = some target ∧ ∃ m ∈ inp.gids, inp.canon m = inp.canon o ∧ live m

def ownerAllows (live : Gid → Prop) (target : String) : Option (Entry Value) → Prop
  | none => True
  | some e =>
    match e.value with
    | .null => True
    | .ref o => namesLive inp live target o
    | _ => False

def liveAt : Nat → Gid → Prop
  | 0, _ => False
  | k + 1, g =>
    liveAt k g ∨
      (g ∈ inp.gids ∧ ¬ inp.unverified g ∧ ¬ deleted inp g ∧
        ∃ c, (inp.home g).bind sc.find = some c ∧
          ∀ f ∈ c.fields, ∀ t, f.kind = .owner t →
            ownerAllows inp (liveAt k) t (register inp g f.name))

def Live (g : Gid) : Prop := ∃ k, liveAt sc inp k g

/-! The rows, given which members are live. `present` takes `Live`; a runnable view decides it. -/

variable (live : Gid → Prop) [DecidablePred live]

instance (target : String) (o : Gid) : Decidable (namesLive inp live target o) := by
  unfold namesLive; infer_instance

def liveMembers (r : Gid) : List Gid :=
  inp.gids.filter fun m => decide (inp.canon m = r ∧ live m)

def read (f : Field) (v : Value) : Value :=
  match f.kind with
  | .plain => v
  | .reference t | .owner t =>
    match v with
    | .ref o => if namesLive inp live t o then .ref (inp.canon o) else .null
    | _ => .null

/-- A field outside any `together` set: the strategy winner among the live members. -/
def plainValue (f : Field) (r : Gid) : Value :=
  match ((liveMembers inp live r).filterMap fun m => register inp m f.name).foldl
      (apply f.strategy) none with
  | some e => read inp live f e.value
  | none => .null

/-- The latest clock a member has written to any field of a set. -/
def latest (set : List String) (m : Gid) : Option Hlc :=
  (set.filterMap fun name => (register inp m name).map (·.clock)).foldl
    (fun best k => match best with
      | none => some k
      | some b => if b < k then some k else some b) none

/-- The live member holding the latest clock across a set, and that clock. The first member in
ascending order wins a tie. -/
def setHolder (set : List String) (r : Gid) : Option (Hlc × Gid) :=
  (liveMembers inp live r).foldl (fun best m =>
    match latest inp set m with
    | none => best
    | some at_ =>
      match best with
      | none => some (at_, m)
      | some (held, _) => if held < at_ then some (at_, m) else best) none

/-- A field of a `together` set: the holder's value if written at the holder's clock. -/
def setValue (c : Collection) (set : List String) (name : String) (r : Gid) : Value :=
  match setHolder inp live set r, c.declared name with
  | some (at_, m), some f =>
    match register inp m name with
    | some e => if e.clock = at_ then read inp live f e.value else .null
    | none => .null
  | _, _ => .null

abbrev Row := List (String × Value)

def row (c : Collection) (r : Gid) : Row :=
  (c.fields.filter fun f => !c.inTogether f.name).map (fun f => (f.name, plainValue inp live f r)) ++
    c.together.flatMap fun set => set.map fun name => (name, setValue inp live c set name r)

/-- The rows of one collection, by canonical gid, given the live members. -/
def presentWith (collection : String) (r : Gid) : Option Row :=
  match sc.find collection with
  | none => none
  | some c =>
    if inp.home r = some collection ∧ ∃ m ∈ inp.gids, inp.canon m = r ∧ live m then
      some (row inp live c r)
    else none

end Present

/-- The rows of one collection, by canonical gid. -/
noncomputable def present (sc : Schema) (inp : Inputs) : String → Gid → Option Row :=
  presentWith sc inp (Live sc inp)

/-- **The view** of the changes `cs`: for each collection, its live groups' rows. -/
noncomputable def view (sc : Schema) (cs : List (Change Value)) : String → Gid → Option Row :=
  present sc (inputs sc (candidates sc cs))

/-! ## The view depends only on the set of changes -/

instance : TransCmp claimCmp :=
  inferInstanceAs (TransCmp (compareLex (compareOn (fun p : Hlc × String => p.1))
    (compareOn (fun p : Hlc × String => p.2))))

instance : LawfulEqCmp claimCmp where
  eq_of_compare {a b} h := by
    simp only [claimCmp, compareLex_eq_eq, compareOn] at h
    exact Prod.ext (LawfulEqOrd.eq_of_compare h.1) (LawfulEqOrd.eq_of_compare h.2)

instance : TransCmp claimFirst := inferInstanceAs (TransCmp fun a b => claimCmp b a)

instance : LawfulEqCmp claimFirst := inferInstanceAs (LawfulEqCmp fun a b => claimCmp b a)

section Invariance

/-! Here `cs₁` and `cs₂` are lists of candidate changes with the same members. -/

variable (sc : Schema) {cs₁ cs₂ : List (Change Value)} (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂)

include same

theorem home_eq : home sc cs₁ = home sc cs₂ := by
  funext g
  simp only [home]
  congr 1
  exact fold_keep_eq_of_mem_iff (cmp := claimFirst) (key := id) _ _ (fun x => by
    simp only [List.mem_map, List.mem_filter, same])
    (fun a _ b _ h => h)

theorem mem_claimed_iff (c : Change Value) : c ∈ claimed sc cs₁ ↔ c ∈ claimed sc cs₂ := by
  simp only [claimed, List.mem_filter, same, home_eq sc same]

theorem gids_eq : gids sc cs₁ = gids sc cs₂ :=
  gidSet_eq_of_mem_iff fun g => by simp only [List.mem_map, mem_claimed_iff sc same]

theorem homeCollection_eq : homeCollection sc cs₁ = homeCollection sc cs₂ := by
  funext g; simp only [homeCollection, home_eq sc same]

theorem earliest_keyWrites_eq (g : Gid) (f : String) :
    earliest (keyWrites sc cs₁ g f) = earliest (keyWrites sc cs₂ g f) :=
  earliest_eq_of_mem_iff fun w => by
    simp only [keyWrites, List.mem_map, List.mem_filter, mem_claimed_iff sc same]

theorem counts_eq : Counts sc cs₁ = Counts sc cs₂ := by
  funext x
  simp only [Counts, earliest_keyWrites_eq sc same]

theorem derives_eq : Derives sc cs₁ = Derives sc cs₂ := by
  funext g c fields k
  simp only [Derives, earliest_keyWrites_eq sc same]

theorem key_eq : key sc cs₁ = key sc cs₂ := by
  funext g
  unfold key
  rw [homeCollection_eq sc same, derives_eq sc same]

theorem unverified_eq : Unverified sc cs₁ = Unverified sc cs₂ := by
  funext g
  simp only [Unverified, homeCollection_eq sc same, key_eq sc same]

theorem mem_accepted_iff (c : Change Value) : c ∈ accepted sc cs₁ ↔ c ∈ accepted sc cs₂ := by
  have : keyFieldOk sc cs₁ = keyFieldOk sc cs₂ := by
    funext x; unfold keyFieldOk; rw [homeCollection_eq sc same, key_eq sc same, counts_eq sc same]
  simp only [accepted, List.mem_filter, mem_claimed_iff sc same, this]

theorem mem_edges_iff (e : Edge) : e ∈ edges sc cs₁ ↔ e ∈ edges sc cs₂ := by
  unfold edges
  rw [unverified_eq sc same, home_eq sc same]
  simp only [List.mem_filterMap, mem_accepted_iff sc same]

theorem registers_eq (unique : ClockDetermines cs₁) : registers sc cs₁ = registers sc cs₂ := by
  apply merged_eq_of_mem_iff
  · intro c; simp only [List.mem_filter, mem_accepted_iff sc same]
  · intro a ha b hb
    have hsub : ∀ x, x ∈ (accepted sc cs₁).filter (fun c => c.field ≠ ALIAS) → x ∈ cs₁ := by
      intro x hx
      simp only [List.mem_filter, accepted, claimed] at hx
      exact hx.1.1.1
    exact unique a (hsub a ha) b (hsub b hb)

theorem groupedAt_eq (unique : ClockDetermines cs₁) (k : Nat) :
    groupedAt sc cs₁ k = groupedAt sc cs₂ k := by
  have he : (fun a b => (a, b) ∈ edges sc cs₁) = (fun a b => (a, b) ∈ edges sc cs₂) := by
    funext a b; exact propext (mem_edges_iff sc same _)
  have hk : KeyPair sc cs₁ = KeyPair sc cs₂ := by
    funext R a b
    simp only [KeyPair, homeCollection_eq sc same, home_eq sc same, unverified_eq sc same,
      registers_eq sc same unique]
  induction k with
  | zero => simp only [groupedAt, he]
  | succ k ih =>
    simp only [groupedAt, ih, hk]
    congr
    funext a b
    rw [propext (mem_edges_iff sc same (a, b))]

theorem canon_eq (unique : ClockDetermines cs₁) : canon sc cs₁ = canon sc cs₂ := by
  have : Grouped sc cs₁ = Grouped sc cs₂ := by
    funext a b; simp only [Grouped, groupedAt_eq sc same unique]
  funext g; unfold canon; rw [this]

theorem inputs_eq (unique : ClockDetermines cs₁) : inputs sc cs₁ = inputs sc cs₂ := by
  simp only [inputs, registers_eq sc same unique, home_eq sc same, gids_eq sc same,
    canon_eq sc same unique, unverified_eq sc same]

end Invariance

/-- **The view over several collections depends only on the set of changes**, given that no two
changes to one register share a clock with different values. -/
theorem view_eq_of_mem_iff (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁) :
    view sc cs₁ = view sc cs₂ := by
  simp only [view]
  rw [inputs_eq sc (cs₁ := candidates sc cs₁) (cs₂ := candidates sc cs₂)
    (fun c => by simp [candidates, same])
    (fun a ha b hb => unique a (List.mem_filter.mp ha).1 b (List.mem_filter.mp hb).1)]

theorem view_perm (sc : Schema) {cs₁ cs₂ : List (Change Value)} (p : cs₁.Perm cs₂)
    (unique : ClockDetermines cs₁) : view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc (fun _ => p.mem_iff) unique

theorem view_dup (sc : Schema) (cs : List (Change Value)) (unique : ClockDetermines cs) :
    view sc (cs ++ cs) = view sc cs :=
  view_eq_of_mem_iff sc (by simp) (by
    intro a ha b hb; simp only [List.mem_append, or_self] at ha hb; exact unique a ha b hb)

/-! ## Changes that are ignored -/

/-- A change that is not a candidate does not change the view. -/
theorem view_ignores (sc : Schema) (c : Change Value) (cs : List (Change Value))
    (h : ¬ (c.clock.encodable ∧ aliasOk c = true ∧ deletedOk c = true ∧
      (sc.find c.collection).isSome)) :
    view sc (c :: cs) = view sc cs := by
  have hc : candidates sc (c :: cs) = candidates sc cs := by
    simp only [candidates, List.filter_cons]
    have : (decide c.clock.encodable && aliasOk c && deletedOk c &&
        (sc.find c.collection).isSome) = false := by
      simp only [Bool.and_eq_false_iff, decide_eq_false_iff_not]
      by_cases h₁ : c.clock.encodable
      · by_cases h₂ : aliasOk c = true
        · by_cases h₃ : deletedOk c = true
          · exact .inr (by simpa [h₁, h₂, h₃] using h)
          · exact .inl (.inr (by simpa using h₃))
        · exact .inl (.inl (.inr (by simpa using h₂)))
      · exact .inl (.inl (.inl h₁))
    simp [this]
  simp only [view, hc]

/-- A change whose clock the text form cannot hold is ignored. -/
theorem view_ignores_unencodable (sc : Schema) (c : Change Value) (cs : List (Change Value))
    (h : ¬ c.clock.encodable) : view sc (c :: cs) = view sc cs :=
  view_ignores sc c cs fun h' => h h'.1

/-- A change to a collection the schema does not declare is ignored. -/
theorem view_ignores_unknown_collection (sc : Schema) (c : Change Value) (cs : List (Change Value))
    (h : sc.find c.collection = none) : view sc (c :: cs) = view sc cs :=
  view_ignores sc c cs fun h' => by simp [h] at h'

/-- An alias that does not name another gid is ignored. -/
theorem view_ignores_bad_alias (sc : Schema) (c : Change Value) (cs : List (Change Value))
    (h : aliasOk c = false) : view sc (c :: cs) = view sc cs :=
  view_ignores sc c cs fun h' => by simp [h] at h'

/-- A `$deleted` that is neither 0 nor 1 is ignored. -/
theorem view_ignores_bad_tombstone (sc : Schema) (c : Change Value) (cs : List (Change Value))
    (h : deletedOk c = false) : view sc (c :: cs) = view sc cs :=
  view_ignores sc c cs fun h' => by simp [h] at h'

end FongoSync.Full
