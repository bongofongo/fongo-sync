import FongoSync.Model
import FongoSync.Run

/-!
# The view over several collections, runnable

`Full.view` states the key a natural gid takes by choice, the groups as the least relation closed
under aliases and equal keys, and liveness as the least set closed under a rule. None of these can
be run. `runView` computes each instead, the way `model::view` does:

- the key is the first, in ascending order of each key field's values, that derives the gid;
- the groups are found by rounds of joining gids whose keys agree, as many rounds as there are
  gids, each round reading references through the groups of the round before;
- liveness is the rule applied as many times as there are gids.

`runView` gives the rows from these inputs exactly as `Full.view` gives them from its own:
`runView_eq` proves that its liveness is `Live`. That its inputs are `Full.inputs` is not proven;
`make lean-diff` compares the result with `model::view` on fixed cases.
-/

namespace FongoSync.Full

open Std

set_option linter.unusedSectionVars false

/-- `Value::total_cmp` without `Real`. -/
def valueCmp (a b : Value) : Ordering := compare a b

instance : TransCmp valueCmp := inferInstanceAs (TransCmp (compare : Value → Value → Ordering))

instance : LawfulEqCmp valueCmp := inferInstanceAs (LawfulEqCmp (compare : Value → Value → Ordering))

section RunInputs

/-! In this section `cs` is a list of candidate changes. -/

variable (sc : Schema) (cs : List (Change Value))

/-- The values the counted writes gave field `f` of `g`, ascending, each once. -/
def keyOptions (g : Gid) (f : String) : List Value :=
  ordSet valueCmp ((earliest (keyWrites sc cs g f)).map (·.2))

/-- The first key, one value from each list in turn, that derives `g`. -/
def firstKey (name : String) (g : Gid) : List (List Value) → List Value → Option (List Value)
  | [], chosen => if sc.derive name chosen = g then some chosen else none
  | options :: rest, chosen => options.findSome? fun v => firstKey name g rest (chosen ++ [v])

def runKey (g : Gid) : Option (List Value) :=
  match homeCollection sc cs g with
  | some c =>
    match c.natural with
    | some fields => firstKey sc c.name g (fields.map (keyOptions sc cs g)) []
    | none => none
  | none => none

def runUnverified (g : Gid) : Bool :=
  match homeCollection sc cs g with
  | some c => c.natural.isSome && (runKey sc cs g).isNone
  | none => false

def runKeyFieldOk (x : Change Value) : Bool :=
  match homeCollection sc cs x.gid with
  | some c =>
    match c.natural with
    | some fields =>
      match fields.findIdx? (· = x.field) with
      | some i =>
        decide ((x.clock, x.value) ∈ earliest (keyWrites sc cs x.gid x.field)) &&
          match runKey sc cs x.gid with
          | some k => k[i]? == some x.value
          | none => false
      | none => true
    | none => true
  | none => true

def runAccepted : List (Change Value) := (claimed sc cs).filter (runKeyFieldOk sc cs)

def runRegisters : State Value :=
  merged sc.strategyOf ((runAccepted sc cs).filter fun c => c.field ≠ ALIAS)

def runEdges : List Edge :=
  (runAccepted sc cs).filterMap fun c =>
    if c.field = ALIAS then
      match c.value with
      | .ref t =>
        if home sc cs t = some c.collection ∧ runUnverified sc cs c.gid = false ∧
            runUnverified sc cs t = false then
          some (c.gid, t)
        else none
      | _ => none
    else none

/-- A value with every reference read as its group's canonical gid. -/
def throughGroups (canon : Gid → Gid) : Value → Value
  | .ref g => .ref (canon g)
  | v => v

/-- `a` and `b` are verified gids of one natural collection whose keys agree once references are
read through `canon`. -/
def keysAgree (canon : Gid → Gid) (a b : Gid) : Bool :=
  match homeCollection sc cs a with
  | some c =>
    match c.natural with
    | some fields =>
      home sc cs b == home sc cs a && !runUnverified sc cs a && !runUnverified sc cs b &&
        fields.all fun f =>
          match runRegisters sc cs (c.name, a, f), runRegisters sc cs (c.name, b, f) with
          | some ea, some eb => throughGroups canon ea.value == throughGroups canon eb.value
          | _, _ => false
    | none => false
  | none => false

/-- The edges after `k` rounds: the aliases, and the pairs whose keys agree through the groups of
the round before. -/
def groupEdges : Nat → List Edge
  | 0 => runEdges sc cs
  | k + 1 =>
    let canon := canonical (groupEdges k)
    runEdges sc cs ++ (gids sc cs).flatMap fun a =>
      (gids sc cs).filterMap fun b => if keysAgree sc cs canon a b then some (a, b) else none

def runCanon : Gid → Gid := canonical (groupEdges sc cs (gids sc cs).length)

/-- The inputs, computed. -/
structure RunInputs where
  registers : State Value
  home : Gid → Option String
  gids : List Gid
  canon : Gid → Gid
  unverified : Gid → Bool

def runInputs : RunInputs :=
  ⟨runRegisters sc cs, home sc cs, gids sc cs, runCanon sc cs, runUnverified sc cs⟩

end RunInputs

def RunInputs.toInputs (inp : RunInputs) : Inputs :=
  ⟨inp.registers, inp.home, inp.gids, inp.canon, fun g => inp.unverified g = true⟩

/-! ## Liveness, computed -/

section Live

variable (sc : Schema) (inp : RunInputs)

def deletedB (g : Gid) : Bool :=
  match register inp.toInputs g DELETED with
  | some e => e.value == .int 1
  | none => false

/-- An owner field allows its entity to live, given the members found live so far. -/
def ownerAllowsB (live : Gid → Bool) (t : String) : Option (Entry Value) → Bool
  | none => true
  | some e =>
    match e.value with
    | .null => true
    | .ref o => inp.home o == some t && inp.gids.any fun m => inp.canon m == inp.canon o && live m
    | _ => false

/-- The rule, given the members found live so far. -/
def ruleB (live : Gid → Bool) (g : Gid) : Bool :=
  inp.gids.contains g && !inp.unverified g && !deletedB inp g &&
    match (inp.home g).bind sc.find with
    | some c =>
      c.fields.all fun f =>
        match f.kind with
        | .owner t => ownerAllowsB inp live t (register inp.toInputs g f.name)
        | _ => true
    | none => false

/-- The members found live after `k` rounds. -/
def liveRound : Nat → List Gid
  | 0 => []
  | k + 1 =>
    let before := liveRound k
    inp.gids.filter fun g => before.contains g || ruleB sc inp (before.contains ·) g

def runLive (g : Gid) : Bool := (liveRound sc inp inp.gids.length).contains g

end Live

/-- **The view over several collections, computed.** -/
def runView (sc : Schema) (cs : List (Change Value)) : String → Gid → Option Row :=
  let inp := runInputs sc (candidates sc cs)
  presentWith sc inp.toInputs (fun g => runLive sc inp g = true)

end FongoSync.Full

/-! ## The computed liveness is `Live` -/

namespace FongoSync.Full

open Classical

section Fuel

/-! The least set closed under a rule that looks only at gids in `gids` is reached within as many
rounds as there are gids. -/

variable (gids : List Gid) (R : (Gid → Prop) → Gid → Prop)
  (hmem : ∀ p g, R p g → g ∈ gids)
  (hcongr : ∀ p q, (∀ m ∈ gids, p m ↔ q m) → ∀ g, R p g ↔ R q g)

/-- Rounds of adding every gid the rule allows. -/
def rounds : Nat → Gid → Prop
  | 0, _ => False
  | k + 1, g => rounds k g ∨ R (rounds k) g

include hmem in
theorem mem_of_rounds {k : Nat} {g : Gid} (h : rounds R k g) : g ∈ gids := by
  induction k with
  | zero => exact h.elim
  | succ k ih =>
    rcases h with h | h
    · exact ih h
    · exact hmem _ _ h

theorem rounds_mono {k j : Nat} (hkj : k ≤ j) {g : Gid} (h : rounds R k g) : rounds R j g := by
  induction hkj with
  | refl => exact h
  | step _ ih => exact .inl ih

include hmem hcongr in
theorem rounds_stable {k : Nat} (hs : ∀ g ∈ gids, rounds R (k + 1) g → rounds R k g)
    (j : Nat) (g : Gid) : rounds R (k + j) g ↔ rounds R k g := by
  induction j generalizing g with
  | zero => exact Iff.rfl
  | succ j ih =>
    rw [← Nat.add_assoc]
    show rounds R (k + j) g ∨ R (rounds R (k + j)) g ↔ _
    rw [ih]
    constructor
    · rintro (h | h)
      · exact h
      · have hr : R (rounds R k) g := (hcongr _ _ (fun m _ => ih m) g).mp h
        exact hs g (hmem _ _ hr) (.inr hr)
    · exact .inl

noncomputable def roundCount (k : Nat) : Nat :=
  (gids.filter fun g => decide (rounds R k g)).length

theorem exists_stable : ∃ k ≤ gids.length,
    ∀ g ∈ gids, rounds R (k + 1) g → rounds R k g := by
  apply Classical.byContradiction
  intro hno
  have grow : ∀ k ≤ gids.length + 1, k ≤ roundCount gids R k := by
    intro k hk
    induction k with
    | zero => exact Nat.zero_le _
    | succ k ih =>
      have hk' : k ≤ gids.length := by omega
      have hlt : roundCount gids R k < roundCount gids R (k + 1) := by
        apply length_filter_lt
        · intro x _ hx; exact decide_eq_true (.inl (of_decide_eq_true hx))
        · have h : ¬ ∀ g ∈ gids, rounds R (k + 1) g → rounds R k g :=
            fun hs => hno ⟨k, hk', hs⟩
          simp only [Classical.not_forall] at h
          obtain ⟨g, hg, h1, h2⟩ := h
          exact ⟨g, hg, by simpa using h1, by simpa using h2⟩
      have := ih (by omega)
      omega
  have := grow (gids.length + 1) (Nat.le_refl _)
  have : roundCount gids R (gids.length + 1) ≤ gids.length := List.length_filter_le _ _
  omega

include hmem hcongr in
/-- Some round up to the number of gids holds every gid any round holds. -/
theorem rounds_length_iff (g : Gid) : (∃ k, rounds R k g) ↔ rounds R gids.length g := by
  obtain ⟨k, hk, hs⟩ := exists_stable gids R
  have stable : ∀ j, rounds R j g → rounds R k g := by
    intro j hj
    rcases Nat.le_total j k with hjk | hkj
    · exact rounds_mono R hjk hj
    · obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hkj
      exact (rounds_stable gids R hmem hcongr hs d g).mp hj
  constructor
  · rintro ⟨j, hj⟩; exact rounds_mono R hk (stable j hj)
  · intro h; exact ⟨_, h⟩

end Fuel

section Live

variable (sc : Schema) (inp : RunInputs)

/-- The rule `liveAt` applies, given the members found live so far. -/
def ruleP (live : Gid → Prop) (g : Gid) : Prop :=
  g ∈ inp.gids ∧ ¬ inp.toInputs.unverified g ∧ ¬ deleted inp.toInputs g ∧
    ∃ c, (inp.home g).bind sc.find = some c ∧
      ∀ f ∈ c.fields, ∀ t, f.kind = .owner t →
        ownerAllows inp.toInputs live t (register inp.toInputs g f.name)

theorem liveAt_iff_rounds (k : Nat) (g : Gid) :
    liveAt sc inp.toInputs k g ↔ rounds (ruleP sc inp) k g := by
  induction k generalizing g with
  | zero => exact Iff.rfl
  | succ k ih =>
    have h : liveAt sc inp.toInputs k = rounds (ruleP sc inp) k := funext fun g => propext (ih g)
    show liveAt sc inp.toInputs k g ∨ _ ↔
      rounds (ruleP sc inp) k g ∨ ruleP sc inp (rounds (ruleP sc inp) k) g
    rw [h]
    exact Iff.rfl

theorem ruleP_mem (p : Gid → Prop) (g : Gid) (h : ruleP sc inp p g) : g ∈ inp.gids := h.1

theorem ruleP_congr (p q : Gid → Prop) (h : ∀ m ∈ inp.gids, p m ↔ q m) (g : Gid) :
    ruleP sc inp p g ↔ ruleP sc inp q g := by
  have hn : namesLive inp.toInputs p = namesLive inp.toInputs q := by
    funext t o
    apply propext
    simp only [namesLive]
    constructor
    · rintro ⟨ho, m, hm, hc, hp⟩; exact ⟨ho, m, hm, hc, (h m hm).mp hp⟩
    · rintro ⟨ho, m, hm, hc, hq⟩; exact ⟨ho, m, hm, hc, (h m hm).mpr hq⟩
  have ho : ownerAllows inp.toInputs p = ownerAllows inp.toInputs q := by
    funext t v
    cases v with
    | none => rfl
    | some e => simp only [ownerAllows, hn]
  simp only [ruleP, ho]

theorem deletedB_iff (g : Gid) : deletedB inp g = true ↔ deleted inp.toInputs g := by
  unfold deletedB deleted
  cases register inp.toInputs g DELETED <;> simp

theorem ownerAllowsB_iff {live : Gid → Bool} {p : Gid → Prop} (h : ∀ m, live m = true ↔ p m)
    (t : String) (v : Option (Entry Value)) :
    ownerAllowsB inp live t v = true ↔ ownerAllows inp.toInputs p t v := by
  cases v with
  | none => simp [ownerAllowsB, ownerAllows]
  | some e =>
    simp only [ownerAllowsB, ownerAllows]
    cases e.value <;> simp [namesLive, RunInputs.toInputs, h]

theorem ruleB_iff {live : Gid → Bool} {p : Gid → Prop} (h : ∀ m, live m = true ↔ p m) (g : Gid) :
    ruleB sc inp live g = true ↔ ruleP sc inp p g := by
  have hu : inp.unverified g = false ↔ ¬ inp.toInputs.unverified g := by
    simp [RunInputs.toInputs]
  have hd : deletedB inp g = false ↔ ¬ deleted inp.toInputs g := by
    rw [← deletedB_iff]; simp
  unfold ruleB ruleP
  simp only [Bool.and_eq_true, List.contains_iff_mem, Bool.not_eq_true', hu, hd]
  constructor
  · rintro ⟨⟨⟨hm, hu⟩, hd⟩, hc⟩
    refine ⟨hm, hu, hd, ?_⟩
    revert hc
    cases (inp.home g).bind sc.find with
    | none => simp
    | some c =>
      intro hc
      refine ⟨c, rfl, fun f hf t hk => ?_⟩
      have := List.all_eq_true.mp hc f hf
      simp only [hk] at this
      exact (ownerAllowsB_iff inp h t _).mp this
  · rintro ⟨hm, hu, hd, c, hc, hall⟩
    refine ⟨⟨⟨hm, hu⟩, hd⟩, ?_⟩
    simp only [hc]
    apply List.all_eq_true.mpr
    intro f hf
    cases hk : f.kind with
    | owner t => exact (ownerAllowsB_iff inp h t _).mpr (hall f hf t hk)
    | plain => rfl
    | reference _ => rfl

theorem liveRound_iff (k : Nat) (g : Gid) :
    (liveRound sc inp k).contains g = true ↔ rounds (ruleP sc inp) k g := by
  induction k generalizing g with
  | zero => simp [liveRound, rounds]
  | succ k ih =>
    have hb := ruleB_iff sc inp (live := ((liveRound sc inp k).contains ·))
      (p := rounds (ruleP sc inp) k) ih g
    simp only [liveRound, List.contains_iff_mem, List.mem_filter, Bool.or_eq_true]
    show _ ↔ rounds (ruleP sc inp) k g ∨ ruleP sc inp (rounds (ruleP sc inp) k) g
    have ih' : g ∈ liveRound sc inp k ↔ rounds (ruleP sc inp) k g := by
      rw [← List.contains_iff_mem]; exact ih g
    rw [ih', hb]
    constructor
    · exact fun h => h.2
    · intro h
      refine ⟨?_, h⟩
      rcases h with h | h
      · exact mem_of_rounds inp.gids (ruleP sc inp) (ruleP_mem sc inp) h
      · exact h.1

/-- **The computed liveness is `Live`.** -/
theorem runLive_iff (g : Gid) : runLive sc inp g = true ↔ Live sc inp.toInputs g := by
  unfold runLive Live
  rw [liveRound_iff,
    ← rounds_length_iff inp.gids (ruleP sc inp) (ruleP_mem sc inp) (ruleP_congr sc inp) g]
  exact exists_congr fun k => (liveAt_iff_rounds sc inp k g).symm

end Live

theorem presentWith_congr (sc : Schema) (inp : Inputs) {p q : Gid → Prop} [DecidablePred p]
    [DecidablePred q] (h : ∀ g, p g ↔ q g) : presentWith sc inp p = presentWith sc inp q := by
  have : p = q := funext fun g => propext (h g)
  subst this
  congr

/-- **The computed view is `present` of the computed inputs.** What is left unproven is that the
computed inputs are `inputs`. -/
theorem runView_eq (sc : Schema) (cs : List (Change Value)) :
    runView sc cs = present sc (runInputs sc (candidates sc cs)).toInputs :=
  presentWith_congr sc _ (runLive_iff sc _)

end FongoSync.Full
