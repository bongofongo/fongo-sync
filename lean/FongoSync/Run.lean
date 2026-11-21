import FongoSync.View

/-!
# The single-collection view, runnable

`view` states liveness as the least set closed under a rule, which cannot be run. `runView`
computes it instead, by applying the rule as many times as there are gids, and is proven equal
to `view`.
-/

namespace FongoSync

open Classical

section Run

variable (sc : Schema) (inp : Inputs)

/-! ## Liveness reaches its fixed point within as many rounds as there are gids -/

theorem liveAt_succ_of {k : Nat} {g : Gid} (h : liveAt sc inp k g) : liveAt sc inp (k + 1) g :=
  .inl h

theorem liveAt_mono {k j : Nat} (hkj : k ≤ j) {g : Gid} (h : liveAt sc inp k g) :
    liveAt sc inp j g := by
  induction hkj with
  | refl => exact h
  | step _ ih => exact liveAt_succ_of sc inp ih

theorem mem_of_liveAt {k : Nat} {g : Gid} (h : liveAt sc inp k g) : g ∈ inp.gids := by
  induction k with
  | zero => exact h.elim
  | succ k ih =>
    rcases h with h | ⟨hm, _⟩
    · exact ih h
    · exact hm

/-- The rule, applied to a candidate live set. -/
def rule (live : Gid → Prop) (g : Gid) : Prop :=
  g ∈ inp.gids ∧ ¬ deleted sc inp g ∧
    ∀ f ∈ sc.fields, f.kind = .owner → ownerAllows inp live (register sc inp g f.name)

theorem liveAt_succ (k : Nat) (g : Gid) :
    liveAt sc inp (k + 1) g ↔ liveAt sc inp k g ∨ rule sc inp (liveAt sc inp k) g := Iff.rfl

/-- The rule only looks at live sets on the gids. -/
theorem rule_congr {p q : Gid → Prop} (h : ∀ m ∈ inp.gids, p m ↔ q m) (g : Gid) :
    rule sc inp p g ↔ rule sc inp q g := by
  have hn : namesLive inp p = namesLive inp q := by
    funext o
    apply propext
    simp only [namesLive]
    constructor
    · rintro ⟨ho, m, hm, hc, hp⟩; exact ⟨ho, m, hm, hc, (h m hm).mp hp⟩
    · rintro ⟨ho, m, hm, hc, hq⟩; exact ⟨ho, m, hm, hc, (h m hm).mpr hq⟩
  have ho : ownerAllows inp p = ownerAllows inp q := by
    funext v
    cases v with
    | none => rfl
    | some e => simp only [ownerAllows, hn]
  simp only [rule, ho]

/-- Once a round adds nothing, no later round does. -/
theorem liveAt_stable {k : Nat} (hs : ∀ g ∈ inp.gids, liveAt sc inp (k + 1) g → liveAt sc inp k g)
    (j : Nat) (g : Gid) : liveAt sc inp (k + j) g ↔ liveAt sc inp k g := by
  induction j generalizing g with
  | zero => exact Iff.rfl
  | succ j ih =>
    rw [← Nat.add_assoc, liveAt_succ, ih]
    constructor
    · rintro (h | h)
      · exact h
      · have hr : rule sc inp (liveAt sc inp k) g :=
          (rule_congr sc inp (fun m _ => ih m) g).mp h
        exact hs g hr.1 (.inr hr)
    · exact .inl

/-- How many gids are live after `k` rounds. -/
noncomputable def liveCount (k : Nat) : Nat :=
  (inp.gids.filter fun g => decide (liveAt sc inp k g)).length

theorem length_filter_mono {l : List Gid} {p q : Gid → Bool} (hpq : ∀ x ∈ l, p x → q x) :
    (l.filter p).length ≤ (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
    have ih' := ih fun x hx => hpq x (.tail _ hx)
    have ha := hpq a (.head _)
    cases hpa : p a <;> cases hqa : q a <;> simp_all <;> omega

theorem length_filter_lt {l : List Gid} {p q : Gid → Bool} (hpq : ∀ x ∈ l, p x → q x)
    (hx : ∃ x ∈ l, q x ∧ ¬ p x) : (l.filter p).length < (l.filter q).length := by
  induction l with
  | nil => obtain ⟨x, hx, _⟩ := hx; cases hx
  | cons a t ih =>
    have hpq' : ∀ x ∈ t, p x → q x := fun x hx => hpq x (.tail _ hx)
    have hle := length_filter_mono hpq'
    obtain ⟨x, hxl, hqx, hpx⟩ := hx
    rcases List.mem_cons.mp hxl with rfl | hxt
    · simp only [List.filter_cons, hqx, Bool.not_eq_true] at hpx ⊢
      simp only [hpx]
      simp; omega
    · have := ih hpq' ⟨x, hxt, hqx, hpx⟩
      have ha := hpq a (.head _)
      cases hpa : p a <;> cases hqa : q a <;> simp_all <;> omega

theorem liveCount_le (k : Nat) : liveCount sc inp k ≤ inp.gids.length :=
  List.length_filter_le _ _

/-- If round `k` is not stable, it adds a gid. -/
theorem liveCount_lt {k : Nat} (h : ¬ ∀ g ∈ inp.gids, liveAt sc inp (k + 1) g → liveAt sc inp k g) :
    liveCount sc inp k < liveCount sc inp (k + 1) := by
  apply length_filter_lt
  · intro x _ hx; simpa using liveAt_succ_of sc inp (by simpa using hx)
  · simp only [Classical.not_forall] at h
    obtain ⟨g, hg, h1, h2⟩ := h
    exact ⟨g, hg, by simpa using h1, by simpa using h2⟩

/-- Some round up to the number of gids is stable. -/
theorem exists_stable : ∃ k ≤ inp.gids.length,
    ∀ g ∈ inp.gids, liveAt sc inp (k + 1) g → liveAt sc inp k g := by
  apply Classical.byContradiction
  intro hno
  have grow : ∀ k ≤ inp.gids.length + 1, k ≤ liveCount sc inp k := by
    intro k hk
    induction k with
    | zero => exact Nat.zero_le _
    | succ k ih =>
      have hk' : k ≤ inp.gids.length := by omega
      have := liveCount_lt sc inp (fun hs => hno ⟨k, hk', hs⟩)
      have := ih (by omega)
      omega
  have := grow (inp.gids.length + 1) (Nat.le_refl _)
  have := liveCount_le sc inp (inp.gids.length + 1)
  omega

/-- Live is live after as many rounds as there are gids. -/
theorem live_iff_liveAt_length (g : Gid) : Live sc inp g ↔ liveAt sc inp inp.gids.length g := by
  obtain ⟨k, hk, hs⟩ := exists_stable sc inp
  have stable : ∀ j, liveAt sc inp j g → liveAt sc inp k g := by
    intro j hj
    rcases Nat.le_total j k with hjk | hkj
    · exact liveAt_mono sc inp hjk hj
    · obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le hkj
      exact (liveAt_stable sc inp hs d g).mp hj
  constructor
  · rintro ⟨j, hj⟩; exact liveAt_mono sc inp hk (stable j hj)
  · intro h; exact ⟨_, h⟩

/-! ## A computable liveness -/

def deletedB (g : Gid) : Bool :=
  match register sc inp g DELETED with
  | some e => e.value == .int 1
  | none => false

def namesLiveB (live : Gid → Bool) (o : Gid) : Bool :=
  inp.gids.contains o && inp.gids.any fun m => inp.canon m == inp.canon o && live m

def ownerAllowsB (live : Gid → Bool) : Option (Entry Value) → Bool
  | none => true
  | some e =>
    match e.value with
    | .null => true
    | .ref o => namesLiveB inp live o
    | _ => false

def liveB : Nat → Gid → Bool
  | 0, _ => false
  | k + 1, g =>
    liveB k g ||
      (inp.gids.contains g && !deletedB sc inp g &&
        sc.fields.all fun f => f.kind != .owner || ownerAllowsB inp (liveB k) (register sc inp g f.name))

theorem deletedB_iff (g : Gid) : deletedB sc inp g = true ↔ deleted sc inp g := by
  unfold deletedB deleted
  cases register sc inp g DELETED <;> simp

theorem namesLiveB_iff {live : Gid → Bool} {p : Gid → Prop} (h : ∀ m, live m = true ↔ p m)
    (o : Gid) : namesLiveB inp live o = true ↔ namesLive inp p o := by
  simp [namesLiveB, namesLive, h]

theorem ownerAllowsB_iff {live : Gid → Bool} {p : Gid → Prop} (h : ∀ m, live m = true ↔ p m)
    (v : Option (Entry Value)) : ownerAllowsB inp live v = true ↔ ownerAllows inp p v := by
  cases v with
  | none => simp [ownerAllowsB, ownerAllows]
  | some e =>
    simp only [ownerAllowsB, ownerAllows]
    cases e.value <;> simp [namesLiveB_iff inp h]

theorem liveB_iff (k : Nat) (g : Gid) : liveB sc inp k g = true ↔ liveAt sc inp k g := by
  induction k generalizing g with
  | zero => simp [liveB, liveAt]
  | succ k ih =>
    simp only [liveB, liveAt, Bool.or_eq_true, Bool.and_eq_true, List.contains_iff_mem,
      Bool.not_eq_true', ih, List.all_eq_true, Bool.or_eq_true, bne_iff_ne, ne_eq]
    rw [← deletedB_iff]
    simp only [Bool.not_eq_true]
    constructor
    · rintro (h | ⟨⟨hm, hd⟩, hf⟩)
      · exact .inl h
      · refine .inr ⟨hm, hd, fun f hf' hk => ?_⟩
        rcases hf f hf' with h | h
        · exact absurd hk h
        · exact (ownerAllowsB_iff inp ih _).mp h
    · rintro (h | ⟨hm, hd, hf⟩)
      · exact .inl h
      · refine .inr ⟨⟨hm, hd⟩, fun f hf' => ?_⟩
        by_cases hk : f.kind = .owner
        · exact .inr ((ownerAllowsB_iff inp ih _).mpr (hf f hf' hk))
        · exact .inl hk

/-- Liveness, computed. -/
def liveNow (g : Gid) : Bool := liveB sc inp inp.gids.length g

theorem liveNow_iff (g : Gid) : liveNow sc inp g = true ↔ Live sc inp g := by
  rw [live_iff_liveAt_length, liveNow, liveB_iff]

/-! ## The view, computed -/

def liveMembersR (r : Gid) : List Gid :=
  inp.gids.filter fun m => inp.canon m == r && liveNow sc inp m

def groupEntryR (f : Field) (r : Gid) : Option (Entry Value) :=
  ((liveMembersR sc inp r).filterMap fun m => register sc inp m f.name).foldl (apply f.strategy) none

def readR (f : Field) : Option (Entry Value) → Value
  | none => .null
  | some e =>
    match f.kind with
    | .plain => e.value
    | .reference | .owner =>
      match e.value with
      | .ref o => if namesLiveB inp (liveNow sc inp) o then .ref (inp.canon o) else .null
      | _ => .null

def presentR (r : Gid) : Option Row :=
  if inp.gids.any fun m => inp.canon m == r && liveNow sc inp m then
    some (sc.fields.map fun f => (f.name, readR sc inp f (groupEntryR sc inp f r)))
  else none

theorem liveMembersR_eq (r : Gid) : liveMembersR sc inp r = liveMembers sc inp r := by
  unfold liveMembersR liveMembers
  congr
  funext m
  have := liveNow_iff sc inp m
  cases hb : liveNow sc inp m <;> by_cases hc : inp.canon m = r <;> simp_all

theorem presentR_eq : presentR sc inp = present sc inp := by
  have hlive : ∀ m, liveNow sc inp m = true ↔ Live sc inp m := liveNow_iff sc inp
  funext r
  unfold presentR present
  have hg : ∀ f, groupEntryR sc inp f r = groupEntry sc inp f r := by
    intro f; unfold groupEntryR groupEntry; rw [liveMembersR_eq]
  have hr : ∀ f e, readR sc inp f e = read sc inp f e := by
    intro f e
    cases e with
    | none => rfl
    | some e =>
      simp only [readR, read]
      cases f.kind <;> simp only
      all_goals
        cases e.value <;> simp only
        rename_i o
        by_cases h : namesLive inp (Live sc inp) o
        · simp [h, (namesLiveB_iff inp hlive o).mpr h]
        · have : namesLiveB inp (liveNow sc inp) o = false := by
            cases hb : namesLiveB inp (liveNow sc inp) o
            · rfl
            · exact absurd ((namesLiveB_iff inp hlive o).mp hb) h
          simp [h, this]
  simp only [hg, hr]
  by_cases h : ∃ m ∈ inp.gids, inp.canon m = r ∧ Live sc inp m
  · have : (inp.gids.any fun m => inp.canon m == r && liveNow sc inp m) = true := by
      simp only [List.any_eq_true, Bool.and_eq_true, beq_iff_eq, hlive]; exact h
    simp [h, this]
  · have : (inp.gids.any fun m => inp.canon m == r && liveNow sc inp m) = false := by
      simp only [List.any_eq_false, Bool.and_eq_true, beq_iff_eq, hlive]
      intro m hm ⟨hc, hl⟩; exact h ⟨m, hm, hc, hl⟩
    simp [h, this]

end Run

/-- The view, computed. -/
def runView (sc : Schema) (cs : List (Change Value)) : Gid → Option Row :=
  presentR sc (inputs sc cs)

/-- **The computed view is the view.** -/
theorem runView_eq_view (sc : Schema) (cs : List (Change Value)) : runView sc cs = view sc cs :=
  presentR_eq sc (inputs sc cs)

/-- Two gids aliased into one group show one row, under the smaller gid, with the later title. -/
example :
    let sc : Schema := ⟨"things", [⟨"title", .plain, .lww⟩]⟩
    let cs : List (Change Value) :=
      [⟨"things", 1, "title", .text "a", ⟨1, 0, 9⟩⟩,
       ⟨"things", 2, "title", .text "b", ⟨2, 0, 9⟩⟩,
       ⟨"things", 2, ALIAS, .ref 1, ⟨3, 0, 9⟩⟩]
    runView sc cs 1 = some [("title", .text "b")] ∧ runView sc cs 2 = none := by
  decide

end FongoSync
