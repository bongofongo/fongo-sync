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
   A change to a key field is ignored unless it counts and its value is part of the first such
   key, comparing keys field by field (only a hash collision lets two keys derive one gid).
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

/-- The collections `next` leads to from `start`, in one step or more. A path back to `start`
passes through distinct collections, so as many rounds as there are collections find it. -/
def Schema.reachable (sc : Schema) (next : String → List String) (start : String) : List String :=
  (List.range sc.collections.length).foldl
    (fun acc _ => (acc ++ acc.flatMap next).eraseDups) (next start)

/-- The collections natural keys lead to from `start`, in one step or more. -/
def Schema.keyReachable (sc : Schema) (start : String) : List String :=
  sc.reachable sc.keyTargetsOf start

/-- The collections a collection's owner fields name. -/
def Schema.ownerTargetsOf (sc : Schema) (name : String) : List String :=
  ((sc.find name).map fun c => c.fields.filterMap fun f =>
    match f.kind with
    | .owner t => some t
    | _ => none).getD []

/-- The collections ownership leads to from `start`, in one step or more. -/
def Schema.ownerReachable (sc : Schema) (start : String) : List String :=
  sc.reachable sc.ownerTargetsOf start

/-- A reference or owner field names a declared collection. -/
def Schema.targetDeclared (sc : Schema) (f : Field) : Bool :=
  match f.kind with
  | .plain => true
  | .reference t | .owner t => (sc.find t).isSome

/-- A natural collection's key is non-empty and names only declared fields. -/
def Collection.keyDeclared (c : Collection) : Bool :=
  c.natural.all fun key => !key.isEmpty && key.all fun f => (c.declared f).isSome

/-- The rules `Schema::check` enforces, but for those on identity keys, which are not modelled
here: no two collections share a name, and no two fields of one collection; no field name starts
with `$`; every reference and owner names a declared collection; a natural key is non-empty and
names only declared fields; no collection owns its own rows; each `together` set is non-empty,
holds only fields it may hold, and shares none with another set; no natural key leads, through
the key fields of the collections it refers to, back to its own collection; and no ownership
leads back to the collection it starts from. The convergence theorem does not need them. -/
def Schema.Restricted (sc : Schema) : Prop :=
  (sc.collections.map (·.name)).Nodup ∧
  ∀ c ∈ sc.collections,
    (c.fields.map (·.name)).Nodup ∧
    (∀ f ∈ c.fields, f.name.startsWith "$" = false ∧ sc.targetDeclared f = true) ∧
    c.keyDeclared = true ∧
    (∀ f ∈ c.fields, f.kind ≠ .owner c.name) ∧
    (∀ set ∈ c.together, set ≠ [] ∧ ∀ name ∈ set, c.groupable name) ∧
    c.together.flatten.Nodup ∧
    c.name ∉ sc.keyReachable c.name ∧
    c.name ∉ sc.ownerReachable c.name

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

/-- Keys compare field by field, each by the value order. -/
def KeyLt : List Value → List Value → Prop := List.Lex fun a b => compare a b = .lt

/-- `k` is the first key, comparing keys field by field, that derives `g`, as the Rust takes it.
Only a hash collision lets two keys derive one gid. -/
def FirstKey (g : Gid) (c : Collection) (fields : List String) (k : List Value) : Prop :=
  Derives sc cs g c fields k ∧ ∀ k', Derives sc cs g c fields k' → k = k' ∨ KeyLt k k'

/-- The first key that derives a natural gid, if any. -/
noncomputable def key (g : Gid) : Option (List Value) :=
  match homeCollection sc cs g with
  | some c =>
    match c.natural with
    | some fields =>
      if h : ∃ k, FirstKey sc cs g c fields k then some (Classical.choose h) else none
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
  unfold key FirstKey
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

theorem registers_eq : registers sc cs₁ = registers sc cs₂ := by
  apply merged_eq_of_mem_iff
  intro c; simp only [List.mem_filter, mem_accepted_iff sc same]

theorem groupedAt_eq (k : Nat) :
    groupedAt sc cs₁ k = groupedAt sc cs₂ k := by
  have he : (fun a b => (a, b) ∈ edges sc cs₁) = (fun a b => (a, b) ∈ edges sc cs₂) := by
    funext a b; exact propext (mem_edges_iff sc same _)
  have hk : KeyPair sc cs₁ = KeyPair sc cs₂ := by
    funext R a b
    simp only [KeyPair, homeCollection_eq sc same, home_eq sc same, unverified_eq sc same,
      registers_eq sc same]
  induction k with
  | zero => simp only [groupedAt, he]
  | succ k ih =>
    simp only [groupedAt, ih, hk]
    congr
    funext a b
    rw [propext (mem_edges_iff sc same (a, b))]

theorem canon_eq : canon sc cs₁ = canon sc cs₂ := by
  have : Grouped sc cs₁ = Grouped sc cs₂ := by
    funext a b; simp only [Grouped, groupedAt_eq sc same]
  funext g; unfold canon; rw [this]

theorem inputs_eq : inputs sc cs₁ = inputs sc cs₂ := by
  simp only [inputs, registers_eq sc same, home_eq sc same, gids_eq sc same,
    canon_eq sc same, unverified_eq sc same]

end Invariance

/-- **The view over several collections depends only on the set of changes.** -/
theorem view_eq_of_mem_iff (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) :
    view sc cs₁ = view sc cs₂ := by
  simp only [view]
  rw [inputs_eq sc (cs₁ := candidates sc cs₁) (cs₂ := candidates sc cs₂)
    (fun c => by simp [candidates, same])]

theorem view_perm (sc : Schema) {cs₁ cs₂ : List (Change Value)} (p : cs₁.Perm cs₂) :
    view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc (fun _ => p.mem_iff)

theorem view_dup (sc : Schema) (cs : List (Change Value)) :
    view sc (cs ++ cs) = view sc cs :=
  view_eq_of_mem_iff sc (by simp)

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


/-! ## A `together` set shows one member's fields at one clock -/

theorem Hlc.not_lt_trans {a b c : Hlc} (h₁ : ¬ b < a) (h₂ : ¬ c < b) : ¬ c < a := by
  rw [Hlc.lt_iff] at *; dsimp only [Gid] at *; omega

theorem Hlc.not_lt_of_lt {a b : Hlc} (h : a < b) : ¬ b < a := by
  rw [Hlc.lt_iff] at *; dsimp only [Gid] at *; omega

theorem Hlc.lt_irrefl (a : Hlc) : ¬ a < a := by
  rw [Hlc.lt_iff]; dsimp only [Gid]; omega

/-- Keep the one with the later clock, the one held on a tie. -/
def laterStep {α : Type} (key : α → Hlc) : Option α → α → Option α
  | none, x => some x
  | some b, x => if key b < key x then some x else some b

theorem fold_laterStep_none {α : Type} (key : α → Hlc) (l : List α) (init : Option α) :
    l.foldl (laterStep key) init = none → init = none ∧ l = [] := by
  induction l generalizing init with
  | nil => intro h; exact ⟨h, rfl⟩
  | cons x xs ih =>
    intro h
    have := (ih _ h).1
    cases init with
    | none => simp [laterStep] at this
    | some b =>
      simp only [laterStep] at this
      split at this <;> cases this

theorem fold_laterStep_some {α : Type} (key : α → Hlc) (l : List α) (init : Option α) (b : α)
    (h : l.foldl (laterStep key) init = some b) :
    (init = some b ∨ b ∈ l) ∧ (∀ x ∈ l, ¬ key b < key x) ∧
      (∀ i, init = some i → ¬ key b < key i) := by
  induction l generalizing init with
  | nil =>
    simp only [List.foldl] at h
    subst h
    exact ⟨.inl rfl, by simp, fun i hi => by cases hi; exact Hlc.lt_irrefl _⟩
  | cons x xs ih =>
    simp only [List.foldl] at h
    obtain ⟨hm, hall, hinit⟩ := ih _ h
    -- what the step holds is no earlier than `x` or than what `init` held
    have step : ∀ s, laterStep key init x = some s →
        ¬ key s < key x ∧ (∀ i, init = some i → ¬ key s < key i) ∧ (s = x ∨ init = some s) := by
      intro s hs
      cases init with
      | none =>
        simp only [laterStep, Option.some.injEq] at hs
        subst hs
        exact ⟨Hlc.lt_irrefl _, by simp, .inl rfl⟩
      | some i =>
        simp only [laterStep] at hs
        split at hs
        · rename_i hlt
          cases hs
          refine ⟨Hlc.lt_irrefl _, fun j hj => ?_, .inl rfl⟩
          cases hj
          exact Hlc.not_lt_of_lt hlt
        · rename_i hlt
          cases hs
          exact ⟨hlt, fun j hj => by cases hj; exact Hlc.lt_irrefl _, .inr rfl⟩
    obtain ⟨s, hs⟩ : ∃ s, laterStep key init x = some s := by
      cases init with
      | none => exact ⟨x, rfl⟩
      | some i => simp only [laterStep]; split <;> exact ⟨_, rfl⟩
    obtain ⟨sx, si, sw⟩ := step s hs
    have hbs := hinit s hs
    refine ⟨?_, ?_, fun i hi => Hlc.not_lt_trans (si i hi) hbs⟩
    · rcases hm with hm | hm
      · rw [hs] at hm
        cases hm
        rcases sw with rfl | e
        · exact .inr (.head _)
        · exact .inl e
      · exact .inr (.tail _ hm)
    · intro y hy
      rcases List.mem_cons.mp hy with rfl | hy
      · exact Hlc.not_lt_trans sx hbs
      · exact hall y hy

section Together

variable (inp : Inputs)

theorem latest_eq (set : List String) (m : Gid) :
    latest inp set m =
      (set.filterMap fun name => (register inp m name).map (·.clock)).foldl (laterStep id) none := by
  unfold latest
  congr
  funext best k
  cases best <;> rfl

theorem latest_none {set : List String} {m : Gid} (h : latest inp set m = none) :
    ∀ name ∈ set, register inp m name = none := by
  rw [latest_eq] at h
  have := (fold_laterStep_none id _ none h).2
  intro name hn
  rw [List.filterMap_eq_nil_iff] at this
  have := this name hn
  cases hr : register inp m name with
  | none => rfl
  | some e => rw [hr] at this; cases this

theorem latest_some {set : List String} {m : Gid} {at_ : Hlc} (h : latest inp set m = some at_) :
    (∃ name ∈ set, ∃ e, register inp m name = some e ∧ e.clock = at_) ∧
      ∀ name ∈ set, ∀ e, register inp m name = some e → ¬ at_ < e.clock := by
  rw [latest_eq] at h
  obtain ⟨hm, hall, _⟩ := fold_laterStep_some id _ none at_ h
  refine ⟨?_, fun name hn e he => hall e.clock (List.mem_filterMap.mpr ⟨name, hn, by simp [he]⟩)⟩
  rcases hm with hm | hm
  · cases hm
  · obtain ⟨name, hn, hc⟩ := List.mem_filterMap.mp hm
    cases hr : register inp m name with
    | none => rw [hr] at hc; cases hc
    | some e =>
      rw [hr] at hc
      cases hc
      exact ⟨name, hn, e, hr, rfl⟩

end Together


section Holder

variable (inp : Inputs) (live : Gid → Prop) [DecidablePred live]

theorem setHolder_eq (set : List String) (r : Gid) :
    setHolder inp live set r =
      ((liveMembers inp live r).filterMap fun m => (latest inp set m).map (·, m)).foldl
        (laterStep Prod.fst) none := by
  unfold setHolder
  generalize liveMembers inp live r = l
  generalize (none : Option (Hlc × Gid)) = init
  induction l generalizing init with
  | nil => rfl
  | cons m ms ih =>
    simp only [List.foldl_cons, List.filterMap_cons]
    cases hl : latest inp set m with
    | none => exact ih init
    | some at_ =>
      simp only [Option.map_some, List.foldl_cons]
      rw [← ih]
      congr 1
      cases init with
      | none => rfl
      | some p => obtain ⟨held, x⟩ := p; simp only [laterStep]

theorem setHolder_none {set : List String} {r : Gid} (h : setHolder inp live set r = none) :
    ∀ m ∈ liveMembers inp live r, latest inp set m = none := by
  rw [setHolder_eq] at h
  have := (fold_laterStep_none _ _ none h).2
  rw [List.filterMap_eq_nil_iff] at this
  intro m hm
  have := this m hm
  cases hl : latest inp set m with
  | none => rfl
  | some _ => rw [hl] at this; cases this

theorem setHolder_some {set : List String} {r : Gid} {at_ : Hlc} {m : Gid}
    (h : setHolder inp live set r = some (at_, m)) :
    m ∈ liveMembers inp live r ∧ latest inp set m = some at_ ∧
      ∀ m' ∈ liveMembers inp live r, ∀ at', latest inp set m' = some at' → ¬ at_ < at' := by
  rw [setHolder_eq] at h
  obtain ⟨hm, hall, _⟩ := fold_laterStep_some _ _ none _ h
  have mem : ∀ m' at', m' ∈ liveMembers inp live r → latest inp set m' = some at' →
      (at', m') ∈ (liveMembers inp live r).filterMap fun m => (latest inp set m).map (·, m) :=
    fun m' at' hm' hl => List.mem_filterMap.mpr ⟨m', hm', by simp [hl]⟩
  refine ⟨?_, ?_, fun m' hm' at' hl => hall _ (mem m' at' hm' hl)⟩
  · rcases hm with hm | hm
    · cases hm
    · obtain ⟨m', hm', he⟩ := List.mem_filterMap.mp hm
      cases hl : latest inp set m' with
      | none => rw [hl] at he; cases he
      | some _ => rw [hl] at he; cases he; exact hm'
  · rcases hm with hm | hm
    · cases hm
    · obtain ⟨m', _, he⟩ := List.mem_filterMap.mp hm
      cases hl : latest inp set m' with
      | none => rw [hl] at he; cases he
      | some _ => rw [hl] at he; cases he; exact hl

theorem flatten_nodup_unique {l : List (List String)} (h : l.flatten.Nodup) {s s' : List String}
    (hs : s ∈ l) (hs' : s' ∈ l) {x : String} (hx : x ∈ s) (hx' : x ∈ s') : s = s' := by
  induction l with
  | nil => cases hs
  | cons a t ih =>
    rw [List.flatten_cons, List.nodup_append] at h
    obtain ⟨_, ht, hdis⟩ := h
    rcases List.mem_cons.mp hs with h₁ | h₁ <;> rcases List.mem_cons.mp hs' with h₂ | h₂
    · exact h₁.trans h₂.symm
    · subst h₁; exact absurd rfl (hdis x hx x (List.mem_flatten.mpr ⟨s', h₂, hx'⟩))
    · subst h₂; exact absurd rfl (hdis x hx' x (List.mem_flatten.mpr ⟨s, h₁, hx⟩))
    · exact ih ht h₁ h₂

/-- In a row, a field of a `together` set holds its set's value. -/
theorem mem_row_set {c : Collection} (hnd : c.together.flatten.Nodup) {set : List String}
    (hset : set ∈ c.together) {name : String} (hn : name ∈ set) {r : Gid} {v : Value}
    (hv : (name, v) ∈ row inp live c r) : v = setValue inp live c set name r := by
  rcases List.mem_append.mp hv with h | h
  · obtain ⟨f, hf, he⟩ := List.mem_map.mp h
    have hin := (List.mem_filter.mp hf).2
    simp only [Prod.mk.injEq] at he
    obtain ⟨rfl, _⟩ := he
    have : c.inTogether f.name = true :=
      List.any_eq_true.mpr ⟨set, hset, List.contains_iff_mem.mpr hn⟩
    simp [this] at hin
  · obtain ⟨set', hs', hm⟩ := List.mem_flatMap.mp h
    obtain ⟨name', hn', he⟩ := List.mem_map.mp hm
    simp only [Prod.mk.injEq] at he
    obtain ⟨rfl, rfl⟩ := he
    rw [flatten_nodup_unique hnd hs' hset hn' hn]

end Holder

section Registers

variable (sc : Schema) (cs : List (Change Value))

theorem home_of_mem_claimed {x : Change Value} (h : x ∈ claimed sc cs) :
    home sc cs x.gid = some x.collection :=
  of_decide_eq_true (List.mem_filter.mp h).2

theorem mem_claimed_of_mem_accepted {x : Change Value} (h : x ∈ accepted sc cs) :
    x ∈ claimed sc cs :=
  (List.mem_filter.mp h).1

theorem mem_of_mem_accepted {x : Change Value} (h : x ∈ accepted sc cs) : x ∈ cs :=
  (List.mem_filter.mp (mem_claimed_of_mem_accepted sc cs h)).1

theorem fold_lww_none {l : List (Entry Value)} (h : l.foldl (apply .lww) none = none) : l = [] := by
  rw [show apply (V := Value) .lww = keep lwwCmp Entry.stamp from
    funext fun h => funext fun e => apply_lww h e] at h
  exact ((fold_keep_none l none).mp h).2

theorem register_at {m : Gid} {coll name : String} (hh : home sc cs m = some coll) :
    register (inputs sc cs) m name =
      (entriesAt ((accepted sc cs).filter fun c => c.field ≠ ALIAS) (coll, m, name)).foldl
        (apply (strategyAt sc.strategyOf (coll, m, name))) none := by
  simp only [register, inputs, hh, registers, merged_at]

theorem strategyAt_lww {coll name : String} {m : Gid} (hs : sc.strategyOf coll name = .lww) :
    strategyAt sc.strategyOf (coll, m, name) = .lww := by
  simp only [strategyAt, hs]; split <;> rfl

/-- A register written to holds one of the changes that count at the latest clock among them,
and of those at that clock the greatest value. -/
theorem register_some {m : Gid} {coll name : String} {e : Entry Value}
    (hh : home sc cs m = some coll) (hs : sc.strategyOf coll name = .lww) (hna : name ≠ ALIAS)
    (h : register (inputs sc cs) m name = some e) :
    (∃ x ∈ accepted sc cs, x.gid = m ∧ x.field = name ∧ x.clock = e.clock ∧ x.value = e.value) ∧
      ∀ x ∈ accepted sc cs, x.gid = m → x.field = name →
        x.clock < e.clock ∨ (x.clock = e.clock ∧ (compare x.value e.value).isLE) := by
  rw [register_at sc cs hh, strategyAt_lww sc hs] at h
  obtain ⟨hmem, hall⟩ := fold_lww_winner _ e h
  refine ⟨?_, fun x hx hg hf => hall x.entry (mem_entriesAt.mpr ⟨x, ?_, ?_, rfl⟩)⟩
  · obtain ⟨x, hx, hk, rfl⟩ := mem_entriesAt.mp hmem
    simp only [Change.key, Prod.mk.injEq] at hk
    exact ⟨x, (List.mem_filter.mp hx).1, hk.2.1, hk.2.2, rfl, rfl⟩
  · exact List.mem_filter.mpr ⟨hx, by simp [hf, hna]⟩
  · have := home_of_mem_claimed sc cs (mem_claimed_of_mem_accepted sc cs hx)
    rw [hg, hh] at this
    cases this
    simp [Change.key, hg, hf]

/-- A register not written to has no change that counts. -/
theorem register_none {m : Gid} {coll name : String} (hh : home sc cs m = some coll)
    (hs : sc.strategyOf coll name = .lww) (hna : name ≠ ALIAS)
    (h : register (inputs sc cs) m name = none) :
    ∀ x ∈ accepted sc cs, x.gid = m → x.field ≠ name := by
  rw [register_at sc cs hh, strategyAt_lww sc hs] at h
  have := fold_lww_none h
  intro x hx hg hf
  have : x.entry ∈ entriesAt ((accepted sc cs).filter fun c => c.field ≠ ALIAS) (coll, m, name) := by
    refine mem_entriesAt.mpr ⟨x, List.mem_filter.mpr ⟨hx, by simp [hf, hna]⟩, ?_, rfl⟩
    have := home_of_mem_claimed sc cs (mem_claimed_of_mem_accepted sc cs hx)
    rw [hg, hh] at this
    cases this
    simp [Change.key, hg, hf]
  rw [‹entriesAt _ _ = []›] at this
  cases this

/-! Members of one group belong to one collection. -/

theorem joined_eq {P : Gid → Gid → Prop} {f : Gid → Option String}
    (hP : ∀ a b, P a b → f a = f b) {a b : Gid} (h : Joined P a b) : f a = f b := by
  induction h with
  | refl => rfl
  | edge he => exact hP _ _ he
  | symm _ ih => exact ih.symm
  | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂

theorem home_eq_of_edge {a b : Gid} (h : (a, b) ∈ edges sc cs) : home sc cs a = home sc cs b := by
  obtain ⟨c, hc, he⟩ := List.mem_filterMap.mp h
  have hh := home_of_mem_claimed sc cs (mem_claimed_of_mem_accepted sc cs hc)
  split at he
  · split at he
    · split at he
      · rename_i hcond
        cases he
        exact hh.trans hcond.1.symm
      · cases he
    · cases he
  · cases he

theorem home_eq_of_groupedAt (k : Nat) {a b : Gid} (h : groupedAt sc cs k a b) :
    home sc cs a = home sc cs b := by
  cases k with
  | zero => exact joined_eq (fun _ _ he => home_eq_of_edge sc cs he) h
  | succ k =>
    exact joined_eq (fun _ _ he => he.elim (home_eq_of_edge sc cs) fun hp => by
      obtain ⟨_, _, _, _, hb, _⟩ := hp; exact hb.symm) h

theorem grouped_canon (g : Gid) : Grouped sc cs g (canon sc cs g) := by
  unfold canon
  cases hf : (List.range (g + 1)).find? (fun h => decide (Grouped sc cs g h)) with
  | none => exact ⟨0, .refl g⟩
  | some h =>
    have := List.find?_some hf
    exact of_decide_eq_true this

theorem home_canon (g : Gid) : home sc cs (canon sc cs g) = home sc cs g := by
  obtain ⟨k, hk⟩ := grouped_canon sc cs g
  exact (home_eq_of_groupedAt sc cs k hk).symm

end Registers


/-- `m` is a live member of the group whose canonical gid is `r`. -/
def LiveMember (sc : Schema) (cs : List (Change Value)) (r m : Gid) : Prop :=
  m ∈ (inputs sc cs).gids ∧ (inputs sc cs).canon m = r ∧ Live sc (inputs sc cs) m

/-- **A `together` set shows one member's fields at one clock.** Of a row of a schema that
`Restricted` accepts, either no live member of the group wrote to the set, and all its fields read
null, or some live member `m` made, at clock `at_`, the latest write to the set by any live member,
and each field of the set reads the greatest value `m` wrote to it at `at_`, or null if `m` wrote
it nothing at `at_`. -/
theorem together_shown (sc : Schema) (cs : List (Change Value)) (restricted : sc.Restricted)
    {coll : String} {c : Collection} {r : Gid} {row : Row} {set : List String}
    (hc : sc.find coll = some c) (hset : set ∈ c.together)
    (hrow : present sc (inputs sc cs) coll r = some row) :
    ((∀ x ∈ accepted sc cs, LiveMember sc cs r x.gid → x.field ∉ set) ∧
      ∀ name ∈ set, ∀ v, (name, v) ∈ row → v = .null) ∨
    ∃ m at_, LiveMember sc cs r m ∧
      (∃ x ∈ accepted sc cs, x.gid = m ∧ x.field ∈ set ∧ x.clock = at_) ∧
      (∀ x ∈ accepted sc cs, LiveMember sc cs r x.gid → x.field ∈ set → ¬ at_ < x.clock) ∧
      ∀ name ∈ set, ∀ v, (name, v) ∈ row →
        (∃ x ∈ accepted sc cs, x.gid = m ∧ x.field = name ∧ x.clock = at_ ∧ x.value = v ∧
          ∀ y ∈ accepted sc cs, y.gid = m → y.field = name → y.clock = at_ →
            (compare y.value v).isLE) ∨
        (v = .null ∧ ∀ x ∈ accepted sc cs, x.gid = m → x.field = name → x.clock ≠ at_) := by
  have hcmem : c ∈ sc.collections := List.mem_of_find?_eq_some hc
  obtain ⟨_, hfields, _, _, hsets, hnd, _, _⟩ := restricted.2 c hcmem
  -- the row is `row` of the live members
  unfold present presentWith at hrow
  rw [hc] at hrow
  simp only at hrow
  by_cases hcond : (inputs sc cs).home r = some coll ∧ ∃ m ∈ (inputs sc cs).gids, (inputs sc cs).canon m = r ∧ Live sc (inputs sc cs) m
  · rw [ite_eq_left hcond] at hrow
    cases hrow
    have hhome : ∀ m, LiveMember sc cs r m → home sc cs m = some coll := by
      intro m ⟨_, hcm, _⟩
      have := home_canon sc cs m
      rw [show canon sc cs m = r from hcm] at this
      rw [← this]; exact hcond.1
    have hmem : ∀ m, m ∈ liveMembers (inputs sc cs) (Live sc (inputs sc cs)) r ↔ LiveMember sc cs r m := by
      intro m
      simp only [liveMembers, List.mem_filter, decide_eq_true_eq, LiveMember]
    have facts : ∀ name ∈ set, ∃ f, c.declared name = some f ∧ f.kind = .plain ∧
        name ≠ ALIAS ∧ sc.strategyOf coll name = .lww := by
      intro name hn
      have hg := (hsets set hset).2 name hn
      unfold Collection.groupable at hg
      cases hd : c.declared name with
      | none => rw [hd] at hg; exact hg.elim
      | some f =>
        rw [hd] at hg
        have hf : f ∈ c.fields := List.mem_of_find?_eq_some hd
        have hname : f.name = name := by
          unfold Collection.declared at hd
          have := List.find?_some hd
          exact of_decide_eq_true this
        refine ⟨f, rfl, hg.1, ?_, ?_⟩
        · intro he
          have := (hfields f hf).1
          rw [hname, he] at this
          simp [ALIAS] at this
        · simp only [Schema.strategyOf, hc, hd, Option.map_some, Option.getD_some, hg.2]
    cases hh : setHolder (inputs sc cs) (Live sc (inputs sc cs)) set r with
    | none =>
      refine .inl ⟨fun x hx hm hf => ?_, fun name hn v hv => ?_⟩
      · have hl := setHolder_none (inputs sc cs) _ hh x.gid ((hmem _).mpr hm)
        have hr := latest_none (inputs sc cs) hl x.field hf
        obtain ⟨_, _, _, hna, hs⟩ := facts x.field hf
        exact register_none sc cs (hhome _ hm) hs hna hr x hx rfl rfl
      · rw [mem_row_set (inputs sc cs) _ hnd hset hn hv]
        simp only [setValue, hh]
    | some p =>
      obtain ⟨at_, m⟩ := p
      obtain ⟨hm, hl, hmax⟩ := setHolder_some (inputs sc cs) _ hh
      have mm := (hmem m).mp hm
      obtain ⟨⟨name₀, hn₀, e₀, hr₀, he₀⟩, hbelow⟩ := latest_some (inputs sc cs) hl
      refine .inr ⟨m, at_, mm, ?_, ?_, fun name hn v hv => ?_⟩
      · obtain ⟨_, _, _, hna, hs⟩ := facts name₀ hn₀
        obtain ⟨⟨x, hx, hg, hf, hk, _⟩, _⟩ := register_some sc cs (hhome m mm) hs hna hr₀
        exact ⟨x, hx, hg, hf ▸ hn₀, hk.trans he₀⟩
      · intro x hx hxm hf
        obtain ⟨_, _, _, hna, hs⟩ := facts x.field hf
        cases hr : register (inputs sc cs) x.gid x.field with
        | none => exact absurd rfl (register_none sc cs (hhome _ hxm) hs hna hr x hx rfl)
        | some e =>
          have hxe : ¬ e.clock < x.clock := by
            rcases (register_some sc cs (hhome _ hxm) hs hna hr).2 x hx rfl rfl with h | ⟨h, _⟩
            · exact Hlc.not_lt_of_lt h
            · rw [h]; exact Hlc.lt_irrefl _
          cases hl' : latest (inputs sc cs) set x.gid with
          | none => rw [latest_none (inputs sc cs) hl' x.field hf] at hr; cases hr
          | some at' =>
            have h₁ := (latest_some (inputs sc cs) hl').2 x.field hf e hr
            have h₂ := hmax x.gid ((hmem _).mpr hxm) at' hl'
            exact Hlc.not_lt_trans hxe (Hlc.not_lt_trans h₁ h₂)
      · rw [mem_row_set (inputs sc cs) _ hnd hset hn hv]
        obtain ⟨f, hd, hk, hna, hs⟩ := facts name hn
        simp only [setValue, hh, hd]
        cases hr : register (inputs sc cs) m name with
        | none =>
          exact .inr ⟨rfl, fun x hx hg hf _ => register_none sc cs (hhome m mm) hs hna hr x hx hg hf⟩
        | some e =>
          have hrs := register_some sc cs (hhome m mm) hs hna hr
          by_cases hce : e.clock = at_
          · simp only [hce, ↓reduceIte, read, hk]
            obtain ⟨x, hx, hg, hf, hk', hv'⟩ := hrs.1
            refine .inl ⟨x, hx, hg, hf, hk'.trans hce, hv', fun y hy hyg hyf hyc => ?_⟩
            rcases hrs.2 y hy hyg hyf with h | ⟨_, h⟩
            · rw [hyc, hce] at h; exact absurd h (Hlc.lt_irrefl _)
            · exact h
          · simp only [hce, ↓reduceIte]
            refine .inr ⟨trivial, fun x hx hg hf hxc => ?_⟩
            have hb := hbelow name hn e hr
            rcases hrs.2 x hx hg hf with h | ⟨h, _⟩
            · rw [hxc] at h; exact hb h
            · rw [hxc] at h; exact hce h.symm
  · rw [ite_eq_right hcond] at hrow
    cases hrow

/-- A live member of a shown group belongs to the group's collection. -/
theorem liveMember_home (sc : Schema) (cs : List (Change Value)) {coll : String} {r m : Gid}
    {row : Row} (hrow : present sc (inputs sc cs) coll r = some row)
    (hm : LiveMember sc cs r m) : home sc cs m = some coll := by
  have hr : home sc cs r = some coll := by
    unfold present presentWith at hrow
    cases hf : sc.find coll with
    | none => rw [hf] at hrow; cases hrow
    | some c =>
      rw [hf] at hrow
      simp only at hrow
      by_cases hcond : (inputs sc cs).home r = some coll ∧
          ∃ m ∈ (inputs sc cs).gids, (inputs sc cs).canon m = r ∧ Live sc (inputs sc cs) m
      · exact hcond.1
      · rw [ite_eq_right hcond] at hrow; cases hrow
  have := home_canon sc cs m
  rw [show canon sc cs m = r from hm.2.1] at this
  rw [← this]; exact hr

/-- **Where clocks do not tie, a `together` set shows one write.** If no two changes to one
register share a clock, then of a row either every field of a set reads null, or some live member
`m` wrote to the set at a clock `at_`, and each field of the set reads null or the value of every
change `m` made to it at `at_`, which is one value. -/
theorem view_together_untied (sc : Schema) (cs : List (Change Value)) (restricted : sc.Restricted)
    (untied : ∀ x ∈ cs, ∀ y ∈ cs, x.collection = y.collection → x.gid = y.gid →
      x.field = y.field → x.clock = y.clock → x.value = y.value)
    {coll : String} {c : Collection} {r : Gid} {row : Row} {set : List String}
    (hc : sc.find coll = some c) (hset : set ∈ c.together)
    (hrow : view sc cs coll r = some row) :
    (∀ name ∈ set, ∀ v, (name, v) ∈ row → v = .null) ∨
    ∃ m at_, LiveMember sc (candidates sc cs) r m ∧
      (∃ x ∈ cs, x.collection = coll ∧ x.gid = m ∧ x.field ∈ set ∧ x.clock = at_) ∧
      ∀ name ∈ set, ∀ v, (name, v) ∈ row → v = .null ∨
        ∀ y ∈ cs, y.collection = coll → y.gid = m → y.field = name → y.clock = at_ →
          y.value = v := by
  have sub : ∀ x ∈ accepted sc (candidates sc cs), x ∈ cs ∧ x.collection = coll ∨
      ¬ LiveMember sc (candidates sc cs) r x.gid := by
    intro x hx
    by_cases hm : LiveMember sc (candidates sc cs) r x.gid
    · refine .inl ⟨(List.mem_filter.mp (mem_of_mem_accepted sc _ hx)).1, ?_⟩
      have h1 := liveMember_home sc _ hrow hm
      have h2 := home_of_mem_claimed sc _ (mem_claimed_of_mem_accepted sc _ hx)
      rw [h1] at h2; cases h2; rfl
    · exact .inr hm
  rcases together_shown sc (candidates sc cs) restricted hc hset hrow with
    ⟨_, hnull⟩ | ⟨m, at_, hm, ⟨x, hx, hg, hf, hk⟩, _, hval⟩
  · exact .inl hnull
  · have inCs : ∀ x ∈ accepted sc (candidates sc cs), x.gid = m → x ∈ cs ∧ x.collection = coll := by
      intro x hx hg
      rcases sub x hx with h | h
      · exact h
      · exact absurd (hg ▸ hm) h
    obtain ⟨hxcs, hxc⟩ := inCs x hx hg
    refine .inr ⟨m, at_, hm, ⟨x, hxcs, hxc, hg, hf, hk⟩, fun name hn v hv => ?_⟩
    rcases hval name hn v hv with ⟨x, hx, hg, hf, hk, hv', _⟩ | ⟨hnull, _⟩
    · obtain ⟨hxcs, hxc⟩ := inCs x hx hg
      refine .inr fun y hy hyc hyg hyf hyk => ?_
      rw [← hv']
      exact (untied x hxcs y hy (hxc.trans hyc.symm) (hg.trans hyg.symm) (hf.trans hyf.symm)
        (hk.trans hyk.symm)).symm
    · exact .inl hnull

end FongoSync.Full
