import FongoSync.Model
import FongoSync.Run

/-!
# The view over several collections, runnable

`Full.view` states the key a natural gid takes as the first of the keys that derive it, the groups
as the least relation closed under aliases and equal keys, and liveness as the least set closed
under a rule. None of these can be run. `runView` computes each instead, the way `model::view`
does:

- the key is the first, in ascending order of each key field's values, that derives the gid;
- the groups are found by rounds of joining gids whose keys agree, as many rounds as there are
  gids, each round reading references through the groups of the round before;
- liveness is the rule applied as many times as there are gids.

`runView_eq` proves that its liveness is `Live`, `runInputs_eq` that its inputs are `Full.inputs`,
and so `runView_eq_view` that it is `Full.view`. `make lean-diff` compares the result with
`model::view` on fixed cases.
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

/-- **The computed view is `present` of the computed inputs.** `runInputs_eq` proves that the
computed inputs are `inputs`. -/
theorem runView_eq (sc : Schema) (cs : List (Change Value)) :
    runView sc cs = present sc (runInputs sc (candidates sc cs)).toInputs :=
  presentWith_congr sc _ (runLive_iff sc _)

end FongoSync.Full

/-! ## The computed inputs are `inputs` -/

namespace FongoSync.Full

open Std Classical

/-! ### The first key -/

theorem keyLt_asymm {a b : List Value} (h : KeyLt a b) : ¬ KeyLt b a := by
  induction h with
  | nil => intro h; cases h
  | rel r =>
    intro h
    cases h with
    | rel r' => exact absurd (TransCmp.lt_trans r r') (lt_irrefl_cmp compare _)
    | cons _ => exact absurd r (lt_irrefl_cmp compare _)
  | cons _ ih =>
    intro h
    cases h with
    | rel r' => exact absurd r' (lt_irrefl_cmp compare _)
    | cons h' => exact ih h'

/-- One value from each list, in order. -/
def Choice : List (List Value) → List Value → Prop
  | [], [] => True
  | o :: os, v :: vs => v ∈ o ∧ Choice os vs
  | _, _ => False

section Keys

variable (sc : Schema) (cs : List (Change Value))

theorem mem_keyOptions {g : Gid} {f : String} {v : Value} :
    v ∈ keyOptions sc cs g f ↔ ∃ w ∈ earliest (keyWrites sc cs g f), w.2 = v := by
  simp only [keyOptions, mem_ordSet valueCmp, List.mem_map]

theorem keyOptions_ascending (g : Gid) (f : String) :
    (keyOptions sc cs g f).Pairwise (fun a b => compare a b = .lt) :=
  ascending_ordSet valueCmp _

theorem choice_iff (g : Gid) (fields : List String) (k : List Value) :
    Choice (fields.map (keyOptions sc cs g)) k ↔ k.length = fields.length ∧
      ∀ p ∈ fields.zip k, ∃ w ∈ earliest (keyWrites sc cs g p.1), w.2 = p.2 := by
  induction fields generalizing k with
  | nil => cases k <;> simp [Choice]
  | cons f fs ih =>
    cases k with
    | nil => simp [Choice]
    | cons v vs =>
      simp only [List.map_cons, Choice, ih, mem_keyOptions, List.length_cons,
        Nat.add_right_cancel_iff, List.zip_cons_cons, List.mem_cons, forall_eq_or_imp]
      constructor
      · rintro ⟨h1, h2, h3⟩; exact ⟨h2, h1, h3⟩
      · rintro ⟨h2, h1, h3⟩; exact ⟨h1, h2, h3⟩

variable {sc}

theorem firstKey_none (name : String) (g : Gid) (opts : List (List Value)) (chosen : List Value)
    (h : firstKey sc name g opts chosen = none) (rest : List Value) (hc : Choice opts rest) :
    sc.derive name (chosen ++ rest) ≠ g := by
  induction opts generalizing chosen rest with
  | nil =>
    cases rest with
    | nil => simp only [firstKey] at h; simpa using h
    | cons _ _ => exact hc.elim
  | cons o os ih =>
    cases rest with
    | nil => exact hc.elim
    | cons v vs =>
      simp only [firstKey, List.findSome?_eq_none_iff] at h
      have := ih (chosen ++ [v]) (h v hc.1) vs hc.2
      simpa using this

/-- What `firstKey` finds: a choice that derives `g`, and the first such. -/
theorem firstKey_spec (name : String) (g : Gid) (opts : List (List Value))
    (asc : ∀ o ∈ opts, o.Pairwise (fun a b => compare a b = .lt)) (chosen k : List Value)
    (h : firstKey sc name g opts chosen = some k) :
    ∃ rest, k = chosen ++ rest ∧ Choice opts rest ∧ sc.derive name k = g ∧
      ∀ rest', Choice opts rest' → sc.derive name (chosen ++ rest') = g →
        rest = rest' ∨ KeyLt rest rest' := by
  induction opts generalizing chosen k with
  | nil =>
    simp only [firstKey] at h
    split at h
    · rename_i hd
      cases h
      refine ⟨[], by simp, trivial, hd, fun rest' hc _ => ?_⟩
      cases rest' with
      | nil => exact .inl rfl
      | cons _ _ => exact hc.elim
    · cases h
  | cons o os ih =>
    simp only [firstKey] at h
    obtain ⟨l₁, a, l₂, ho, ha, hl₁⟩ := List.findSome?_eq_some_iff.mp h
    have asc' : ∀ o ∈ os, o.Pairwise (fun a b => compare a b = .lt) :=
      fun o' h' => asc o' (.tail _ h')
    obtain ⟨rest, hk, hc, hd, least⟩ := ih asc' (chosen ++ [a]) k ha
    refine ⟨a :: rest, by simp [hk], ⟨by simp [ho], hc⟩, hd, fun rest' hc' hd' => ?_⟩
    cases rest' with
    | nil => exact hc'.elim
    | cons v vs =>
      obtain ⟨hv, hvs⟩ := hc'
      have found : firstKey sc name g os (chosen ++ [v]) ≠ none := by
        intro hn
        exact firstKey_none name g os (chosen ++ [v]) hn vs hvs (by simpa using hd')
      rw [ho] at hv
      rcases List.mem_append.mp hv with hv | hv
      · exact absurd (hl₁ v hv) found
      · rcases List.mem_cons.mp hv with rfl | hv
        · rcases least vs hvs (by simpa using hd') with e | lt
          · exact .inl (by rw [e])
          · exact .inr (.cons lt)
        · have hp := asc o (.head _)
          rw [ho, List.pairwise_append, List.pairwise_cons] at hp
          exact .inr (.rel (hp.2.1.1 v hv))

variable (sc)

theorem key_eq_runKey (g : Gid) : key sc cs g = runKey sc cs g := by
  unfold key runKey
  cases homeCollection sc cs g with
  | none => rfl
  | some c =>
    simp only
    cases c.natural with
    | none => rfl
    | some fields =>
      simp only
      have asc : ∀ o ∈ fields.map (keyOptions sc cs g),
          o.Pairwise (fun a b => compare a b = .lt) := by
        intro o ho
        obtain ⟨f, _, rfl⟩ := List.mem_map.mp ho
        exact keyOptions_ascending sc cs g f
      cases hf : firstKey sc c.name g (fields.map (keyOptions sc cs g)) [] with
      | none =>
        rw [dite_eq_right]
        rintro ⟨k, ⟨hl, hw, hd⟩, _⟩
        exact firstKey_none c.name g _ [] hf k ((choice_iff sc cs g fields k).mpr ⟨hl, hw⟩)
          (by simpa using hd)
      | some k =>
        obtain ⟨rest, hk, hc, hd, least⟩ := firstKey_spec c.name g _ asc [] k hf
        simp only [List.nil_append] at hk
        subst hk
        have hder : Derives sc cs g c fields k := by
          obtain ⟨hl, hw⟩ := (choice_iff sc cs g fields k).mp hc
          exact ⟨hl, hw, hd⟩
        have first : FirstKey sc cs g c fields k := ⟨hder, fun k' hk' =>
          least k' ((choice_iff sc cs g fields k').mpr ⟨hk'.1, hk'.2.1⟩)
            (by simpa using hk'.2.2)⟩
        have ex : ∃ k, FirstKey sc cs g c fields k := ⟨k, first⟩
        rw [dite_eq_left ex]
        congr 1
        have hch := Classical.choose_spec ex
        rcases hch.2 k first.1 with e | lt
        · exact e
        · rcases first.2 _ hch.1 with e | lt'
          · exact e.symm
          · exact absurd lt (keyLt_asymm lt')

theorem unverified_iff (g : Gid) : Unverified sc cs g ↔ runUnverified sc cs g = true := by
  unfold Unverified runUnverified
  rw [key_eq_runKey]
  cases homeCollection sc cs g with
  | none => simp
  | some c =>
    cases h : c.natural <;> simp [h]

theorem keyFieldOk_iff (x : Change Value) : keyFieldOk sc cs x ↔ runKeyFieldOk sc cs x = true := by
  unfold keyFieldOk runKeyFieldOk
  rw [key_eq_runKey]
  cases homeCollection sc cs x.gid with
  | none => simp
  | some c =>
    simp only
    cases c.natural with
    | none => simp
    | some fields =>
      simp only
      cases fields.findIdx? (· = x.field) with
      | none => simp
      | some i =>
        simp only [Counts, Bool.and_eq_true, decide_eq_true_eq]
        cases runKey sc cs x.gid <;> simp

theorem accepted_eq_run : accepted sc cs = runAccepted sc cs := by
  unfold accepted runAccepted
  apply List.filter_congr
  intro x _
  by_cases h : keyFieldOk sc cs x
  · simp [h, (keyFieldOk_iff sc cs x).mp h]
  · have : runKeyFieldOk sc cs x = false := by
      cases hb : runKeyFieldOk sc cs x
      · rfl
      · exact absurd ((keyFieldOk_iff sc cs x).mpr hb) h
    simp [h, this]

theorem registers_eq_run : registers sc cs = runRegisters sc cs := by
  simp only [registers, runRegisters, accepted_eq_run]

theorem mem_edges_iff_run (e : Edge) : e ∈ edges sc cs ↔ e ∈ runEdges sc cs := by
  have hu : ∀ g, ¬ Unverified sc cs g ↔ runUnverified sc cs g = false := by
    intro g; rw [unverified_iff]; simp
  simp only [edges, runEdges, accepted_eq_run, List.mem_filterMap, hu]
  apply exists_congr; intro a; apply and_congr_right; intro _
  by_cases hf : a.field = ALIAS
  · simp only [ite_eq_left hf]
    cases a.value with
    | ref t =>
      by_cases hc : home sc cs t = some a.collection ∧ runUnverified sc cs a.gid = false ∧
          runUnverified sc cs t = false
      · simp only [ite_eq_left hc]
      · simp only [ite_eq_right hc]
    | _ => rfl
  · simp only [ite_eq_right hf]

end Keys

/-! ### The groups -/

theorem Joined.mono {P Q : Gid → Gid → Prop} (hpq : ∀ a b, P a b → Q a b) {a b : Gid}
    (h : Joined P a b) : Joined Q a b := by
  induction h with
  | refl a => exact .refl a
  | edge he => exact .edge (hpq _ _ he)
  | symm _ ih => exact .symm ih
  | trans _ _ ih₁ ih₂ => exact .trans ih₁ ih₂

theorem joined_iff_connected {P : Gid → Gid → Prop} {E : List Edge} (h : ∀ a b, P a b ↔ (a, b) ∈ E)
    (a b : Gid) : Joined P a b ↔ Connected E a b := by
  constructor
  · intro j
    induction j with
    | refl a => exact .refl a
    | edge he => exact .edge ((h _ _).mp he)
    | symm _ ih => exact .symm ih
    | trans _ _ ih₁ ih₂ => exact .trans ih₁ ih₂
  · intro c
    induction c with
    | refl a => exact .refl a
    | edge he => exact .edge ((h _ _).mpr he)
    | symm _ ih => exact .symm ih
    | trans _ _ ih₁ ih₂ => exact .trans ih₁ ih₂

/-- A relation whose pairs lie in `S`, closed up, relates only equal gids or gids of `S`. -/
theorem joined_within {P : Gid → Gid → Prop} {S : List Gid} (hP : ∀ a b, P a b → a ∈ S ∧ b ∈ S)
    {a b : Gid} (h : Joined P a b) : a = b ∨ (a ∈ S ∧ b ∈ S) := by
  induction h with
  | refl a => exact .inl rfl
  | edge he => exact .inr (hP _ _ he)
  | symm _ ih =>
    rcases ih with e | ⟨h₁, h₂⟩
    · exact .inl e.symm
    · exact .inr ⟨h₂, h₁⟩
  | trans _ _ ih₁ ih₂ =>
    rcases ih₁ with rfl | ⟨h₁, _⟩
    · exact ih₂
    · rcases ih₂ with rfl | ⟨_, h₄⟩
      · exact .inr ⟨h₁, by assumption⟩
      · exact .inr ⟨h₁, h₄⟩

section Groups

variable (sc : Schema) (cs : List (Change Value))

theorem mem_gids_of_home {g : Gid} {n : String} (h : home sc cs g = some n) : g ∈ gids sc cs := by
  unfold home at h
  obtain ⟨p, hp, rfl⟩ := Option.map_eq_some_iff.mp h
  have hm := (fold_keep_spec (cmp := claimFirst) (key := id) _ none p hp).1
  simp only [reduceCtorEq, false_or, List.mem_map, List.mem_filter, decide_eq_true_eq] at hm
  obtain ⟨c, ⟨hc, rfl⟩, rfl⟩ := hm
  unfold gids
  rw [mem_gidSet]
  refine List.mem_map.mpr ⟨c, ?_, rfl⟩
  simp only [claimed, List.mem_filter, hc, true_and, decide_eq_true_eq]
  unfold home
  rw [hp]
  rfl

theorem mem_gids_of_homeCollection {g : Gid} {c : Collection}
    (h : homeCollection sc cs g = some c) : g ∈ gids sc cs := by
  unfold homeCollection at h
  obtain ⟨n, hn, _⟩ := Option.bind_eq_some_iff.mp h
  exact mem_gids_of_home sc cs hn

theorem keyPair_mem {R : Gid → Gid → Prop} {a b : Gid} (h : KeyPair sc cs R a b) :
    a ∈ gids sc cs ∧ b ∈ gids sc cs := by
  obtain ⟨c, _, hc, _, hb, _⟩ := h
  have ha := mem_gids_of_homeCollection sc cs hc
  refine ⟨ha, ?_⟩
  unfold homeCollection at hc
  obtain ⟨n, hn, _⟩ := Option.bind_eq_some_iff.mp hc
  exact mem_gids_of_home sc cs (hb.trans hn)

theorem edges_mem {a b : Gid} (h : (a, b) ∈ edges sc cs) : a ∈ gids sc cs ∧ b ∈ gids sc cs := by
  obtain ⟨c, hc, he⟩ := List.mem_filterMap.mp h
  have hcl : c ∈ claimed sc cs := (List.mem_filter.mp hc).1
  split at he
  · split at he
    · rename_i t _
      split at he
      · rename_i hcond
        cases he
        refine ⟨?_, mem_gids_of_home sc cs hcond.1⟩
        unfold gids; rw [mem_gidSet]; exact List.mem_map.mpr ⟨c, hcl, rfl⟩
      · cases he
    · cases he
  · cases he

theorem keyPair_mono {R R' : Gid → Gid → Prop} (h : ∀ x y, R x y → R' x y) {a b : Gid}
    (hp : KeyPair sc cs R a b) : KeyPair sc cs R' a b := by
  obtain ⟨c, fields, h1, h2, h3, h4, h5, hall⟩ := hp
  refine ⟨c, fields, h1, h2, h3, h4, h5, fun f hf => ?_⟩
  obtain ⟨ea, eb, ha, hb, hs⟩ := hall f hf
  refine ⟨ea, eb, ha, hb, ?_⟩
  rcases hs with e | ⟨x, y, hx, hy, hr⟩
  · exact .inl e
  · exact .inr ⟨x, y, hx, hy, h x y hr⟩

theorem sameUnder_iff {R : Gid → Gid → Prop} {canon : Gid → Gid}
    (hR : ∀ x y, R x y ↔ canon x = canon y) (v w : Value) :
    SameUnder R v w ↔ (throughGroups canon v == throughGroups canon w) = true := by
  cases v <;> cases w <;> simp [SameUnder, throughGroups, hR]
  rintro rfl; rfl

theorem keysAgree_iff {R : Gid → Gid → Prop} {canon : Gid → Gid}
    (hR : ∀ x y, R x y ↔ canon x = canon y) (a b : Gid) :
    keysAgree sc cs canon a b = true ↔ KeyPair sc cs R a b := by
  have hu : ∀ g, ¬ Unverified sc cs g ↔ runUnverified sc cs g = false := by
    intro g; rw [unverified_iff]; simp
  have hf : ∀ (c : Collection) f,
      ((match runRegisters sc cs (c.name, a, f), runRegisters sc cs (c.name, b, f) with
        | some ea, some eb => throughGroups canon ea.value == throughGroups canon eb.value
        | _, _ => false) = true) ↔
      ∃ ea eb, registers sc cs (c.name, a, f) = some ea ∧
        registers sc cs (c.name, b, f) = some eb ∧ SameUnder R ea.value eb.value := by
    intro c f
    rw [registers_eq_run]
    cases runRegisters sc cs (c.name, a, f) <;> cases runRegisters sc cs (c.name, b, f) <;>
      simp [sameUnder_iff hR]
  unfold keysAgree KeyPair
  cases hc : homeCollection sc cs a with
  | none => simp
  | some c =>
    cases hn : c.natural with
    | none => simp [hn]
    | some fields =>
      simp only [hn, Bool.and_eq_true, beq_iff_eq, Bool.not_eq_true', List.all_eq_true, hf, hu,
        Option.some.injEq]
      constructor
      · rintro ⟨⟨⟨h1, h2⟩, h3⟩, h4⟩
        exact ⟨c, fields, rfl, hn, h1, h2, h3, h4⟩
      · rintro ⟨c', fields', rfl, hn', h1, h2, h3, h4⟩
        rw [hn] at hn'
        cases hn'
        exact ⟨⟨⟨h1, h2⟩, h3⟩, h4⟩

/-- Round `k` of the computed groups is round `k` of `groupedAt`. -/
theorem groupedAt_iff (k : Nat) (a b : Gid) :
    groupedAt sc cs k a b ↔ Connected (groupEdges sc cs k) a b := by
  induction k generalizing a b with
  | zero => exact joined_iff_connected (fun a b => mem_edges_iff_run sc cs (a, b)) a b
  | succ k ih =>
    have hR : ∀ x y, groupedAt sc cs k x y ↔
        canonical (groupEdges sc cs k) x = canonical (groupEdges sc cs k) y := by
      intro x y; rw [ih, canonical_eq_iff]
    apply joined_iff_connected
    intro a b
    simp only [groupEdges, List.mem_append, List.mem_flatMap, List.mem_filterMap,
      ← mem_edges_iff_run]
    apply or_congr_right
    constructor
    · intro h
      have hm := keyPair_mem sc cs h
      refine ⟨a, hm.1, b, hm.2, ?_⟩
      rw [(keysAgree_iff sc cs hR a b).mpr h]
      rfl
    · rintro ⟨a', _, b', _, he⟩
      split at he
      · rename_i h
        cases he
        exact (keysAgree_iff sc cs hR a b).mp h
      · cases he

/-! The rounds of `groupedAt` grow, and stop growing within as many rounds as there are gids:
each round that grows merges two groups, which the count of gids smallest in their group sees. -/

theorem groupedAt_refl (k : Nat) (a : Gid) : groupedAt sc cs k a a := by
  cases k <;> exact .refl a

theorem groupedAt_symm {k : Nat} {a b : Gid} (h : groupedAt sc cs k a b) :
    groupedAt sc cs k b a := by
  cases k <;> exact .symm h

theorem groupedAt_trans {k : Nat} {a b c : Gid} (h₁ : groupedAt sc cs k a b)
    (h₂ : groupedAt sc cs k b c) : groupedAt sc cs k a c := by
  cases k <;> exact .trans h₁ h₂

theorem groupedAt_succ_of (k : Nat) {a b : Gid} (h : groupedAt sc cs k a b) :
    groupedAt sc cs (k + 1) a b := by
  induction k generalizing a b with
  | zero => exact Joined.mono (fun _ _ h => .inl h) h
  | succ k ih => exact Joined.mono (fun _ _ h => h.imp_right (keyPair_mono sc cs fun _ _ => ih)) h

theorem groupedAt_mono {k j : Nat} (hkj : k ≤ j) {a b : Gid} (h : groupedAt sc cs k a b) :
    groupedAt sc cs j a b := by
  induction hkj with
  | refl => exact h
  | step _ ih => exact groupedAt_succ_of sc cs _ ih

theorem groupedAt_within (k : Nat) {a b : Gid} (h : groupedAt sc cs k a b) :
    a = b ∨ (a ∈ gids sc cs ∧ b ∈ gids sc cs) := by
  cases k with
  | zero => exact joined_within (fun _ _ he => edges_mem sc cs he) h
  | succ k =>
    exact joined_within (fun _ _ he => he.elim (edges_mem sc cs) (keyPair_mem sc cs)) h

/-- `r` is the smallest gid of `gids` in its group after `k` rounds. -/
def Least (k : Nat) (r : Gid) : Prop := ∀ b ∈ gids sc cs, b < r → ¬ groupedAt sc cs k r b

theorem exists_least (k : Nat) (a : Gid) (ha : a ∈ gids sc cs) :
    ∃ r ∈ gids sc cs, groupedAt sc cs k a r ∧ Least sc cs k r := by
  induction a using Nat.strongRecOn with
  | _ a ih =>
    by_cases hmin : Least sc cs k a
    · exact ⟨a, ha, groupedAt_refl sc cs k a, hmin⟩
    · simp only [Least, Classical.not_forall, Classical.not_not] at hmin
      obtain ⟨b, hb, hlt, hab⟩ := hmin
      obtain ⟨r, hr, hbr, hl⟩ := ih b hlt hb
      exact ⟨r, hr, groupedAt_trans sc cs hab hbr, hl⟩

noncomputable def leastCount (k : Nat) : Nat :=
  ((gids sc cs).filter fun a => decide (Least sc cs k a)).length

theorem leastCount_lt {k : Nat} (h : ¬ ∀ a b, groupedAt sc cs (k + 1) a b → groupedAt sc cs k a b) :
    leastCount sc cs (k + 1) < leastCount sc cs k := by
  apply length_filter_lt
  · intro a _ ha
    simp only [decide_eq_true_eq] at ha ⊢
    exact fun b hb hlt hk => ha b hb hlt (groupedAt_succ_of sc cs k hk)
  · simp only [Classical.not_forall] at h
    obtain ⟨a, b, h1, h2⟩ := h
    rcases groupedAt_within sc cs (k + 1) h1 with rfl | ⟨ha, hb⟩
    · exact absurd (groupedAt_refl sc cs k a) h2
    obtain ⟨ra, hra, har, hla⟩ := exists_least sc cs k a ha
    obtain ⟨rb, hrb, hbr, hlb⟩ := exists_least sc cs k b hb
    have hne : ra ≠ rb := by
      rintro rfl
      exact h2 (groupedAt_trans sc cs har (groupedAt_symm sc cs hbr))
    have hj : groupedAt sc cs (k + 1) ra rb :=
      groupedAt_trans sc cs (groupedAt_succ_of sc cs k (groupedAt_symm sc cs har))
        (groupedAt_trans sc cs h1 (groupedAt_succ_of sc cs k hbr))
    rcases Nat.lt_or_gt_of_ne hne with hlt | hlt
    · refine ⟨rb, hrb, decide_eq_true hlb, fun hd => ?_⟩
      exact of_decide_eq_true hd ra hra hlt (groupedAt_symm sc cs hj)
    · refine ⟨ra, hra, decide_eq_true hla, fun hd => ?_⟩
      exact of_decide_eq_true hd rb hrb hlt hj

theorem groupedAt_exists_stable : ∃ k ≤ (gids sc cs).length,
    ∀ a b, groupedAt sc cs (k + 1) a b → groupedAt sc cs k a b := by
  apply Classical.byContradiction
  intro hno
  have shrink : ∀ k ≤ (gids sc cs).length + 1, leastCount sc cs k + k ≤ (gids sc cs).length := by
    intro k hk
    induction k with
    | zero => exact List.length_filter_le _ _
    | succ k ih =>
      have := leastCount_lt sc cs (fun hs => hno ⟨k, by omega, hs⟩)
      have := ih (by omega)
      omega
  have := shrink _ (Nat.le_refl _)
  omega

theorem groupedAt_stable {k : Nat} (hs : ∀ a b, groupedAt sc cs (k + 1) a b → groupedAt sc cs k a b)
    (j : Nat) : groupedAt sc cs (k + j) = groupedAt sc cs k := by
  have h1 : groupedAt sc cs (k + 1) = groupedAt sc cs k :=
    funext fun a => funext fun b => propext ⟨hs a b, groupedAt_succ_of sc cs k⟩
  induction j with
  | zero => rfl
  | succ j ih =>
    calc groupedAt sc cs (k + (j + 1)) = groupedAt sc cs (k + 1) := by
          rw [← Nat.add_assoc]; simp only [groupedAt, ih]
      _ = groupedAt sc cs k := h1

/-- The least grouping is reached within as many rounds as there are gids. -/
theorem grouped_iff (a b : Gid) :
    Grouped sc cs a b ↔ groupedAt sc cs (gids sc cs).length a b := by
  obtain ⟨k, hk, hs⟩ := groupedAt_exists_stable sc cs
  constructor
  · rintro ⟨j, hj⟩
    rcases Nat.le_total j k with hjk | hkj
    · exact groupedAt_mono sc cs (Nat.le_trans hjk hk) hj
    · obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hkj
      rw [groupedAt_stable sc cs hs d] at hj
      exact groupedAt_mono sc cs hk hj
  · intro h; exact ⟨_, h⟩

theorem canon_eq_runCanon (g : Gid) : canon sc cs g = runCanon sc cs g := by
  have hG : ∀ x y, Grouped sc cs x y ↔ Connected (groupEdges sc cs (gids sc cs).length) x y :=
    fun x y => (grouped_iff sc cs x y).trans (groupedAt_iff sc cs _ x y)
  have hc : IsCanonical (groupEdges sc cs (gids sc cs).length) g (runCanon sc cs g) :=
    canonical_spec _ g
  have hle : runCanon sc cs g ≤ g := hc.2 g (.refl g)
  unfold canon
  have hfind : (List.range (g + 1)).find? (fun h => decide (Grouped sc cs g h)) =
      some (runCanon sc cs g) := by
    rw [List.find?_range_eq_some]
    refine ⟨decide_eq_true ((hG _ _).mpr hc.1), List.mem_range.mpr (Nat.lt_succ_of_le hle), fun j hj => ?_⟩
    simp only [Bool.not_eq_true', decide_eq_false_iff_not]
    intro hgj
    exact Nat.lt_irrefl _ (Nat.lt_of_lt_of_le hj (hc.2 j ((hG _ _).mp hgj)))
  rw [hfind]
  rfl

/-- **The computed inputs are `inputs`.** -/
theorem runInputs_eq : (runInputs sc cs).toInputs = inputs sc cs := by
  have hc : runCanon sc cs = canon sc cs := funext fun g => (canon_eq_runCanon sc cs g).symm
  have hu : (fun g => runUnverified sc cs g = true) = Unverified sc cs :=
    funext fun g => propext (unverified_iff sc cs g).symm
  simp only [RunInputs.toInputs, runInputs, inputs, registers_eq_run, hc, hu]

end Groups

/-- **The computed view over several collections is the view.** -/
theorem runView_eq_view (sc : Schema) (cs : List (Change Value)) : runView sc cs = view sc cs := by
  rw [runView_eq, runInputs_eq]
  rfl

end FongoSync.Full
