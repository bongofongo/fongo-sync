import FongoSync.Clock

/-!
# Registers

Mirrors `wins` and `State::apply` in `crates/core/src/change.rs`.

A register holds the winning `(value, clock)` for one field of one entity. The merge is proven
against an abstract "keep the entry with the greater key" (`keep`), where the key is the clock for
`Lww` and the pair `(value, clock)` for `Max`. Values are any type with a lawful total order: the
Rust `Value::total_cmp` is assumed, not proven, to be one, since `f64` is not modelled.
-/

namespace FongoSync

open Std

set_option linter.unusedSectionVars false

/-! ## The abstract merge -/

section Keep

variable {E K : Type} (cmp : K → K → Ordering) (key : E → K)

/-- Keep the held entry unless the incoming one has a strictly greater key. -/
def keep : Option E → E → Option E
  | none, e => some e
  | some s, e => if cmp (key s) (key e) = .lt then some e else some s

/-- `keep` as a binary operation on registers. -/
def join : Option E → Option E → Option E
  | a, none => a
  | a, some e => keep cmp key a e

/-- No two different entries of `l` share a key. For `Lww` this is "clocks are unique". -/
def KeyInj (l : List E) : Prop := ∀ a ∈ l, ∀ b ∈ l, key a = key b → a = b

variable [TransCmp cmp] [LawfulEqCmp cmp]

variable {cmp key}

@[simp] theorem keep_none (e : E) : keep cmp key none e = some e := rfl

@[simp] theorem keep_held (s e : E) :
    keep cmp key (some s) e = if cmp (key s) (key e) = .lt then some e else some s := rfl

theorem keep_some (h : Option E) (e : E) : ∃ m, keep cmp key h e = some m := by
  cases h with
  | none => exact ⟨e, rfl⟩
  | some s => rw [keep_held]; split <;> exact ⟨_, rfl⟩

theorem keep_idem (h : Option E) (e : E) : keep cmp key (keep cmp key h e) e = keep cmp key h e := by
  cases h with
  | none => simp [ReflCmp.compare_self]
  | some s =>
    by_cases hlt : cmp (key s) (key e) = .lt
    · simp [hlt, ReflCmp.compare_self]
    · simp [hlt]

/-- Two entries may arrive in either order, provided equal keys mean equal entries. -/
theorem keep_swap (h : Option E) (a b : E) (inj : key a = key b → a = b) :
    keep cmp key (keep cmp key h a) b = keep cmp key (keep cmp key h b) a := by
  have tri : cmp (key a) (key b) = .lt ∨ a = b ∨ cmp (key b) (key a) = .lt := by
    cases hab : cmp (key a) (key b) with
    | lt => exact .inl rfl
    | eq => exact .inr (.inl (inj (LawfulEqCmp.eq_of_compare hab)))
    | gt => exact .inr (.inr (OrientedCmp.lt_of_gt hab))
  rcases tri with hab | rfl | hba
  · have hba : cmp (key b) (key a) ≠ .lt := fun h' => by
      have := OrientedCmp.gt_of_lt hab; simp_all
    cases h with
    | none => simp [hab, hba]
    | some s =>
      by_cases hsa : cmp (key s) (key a) = .lt
      · have hsb : cmp (key s) (key b) = .lt := TransCmp.lt_trans hsa hab
        simp [*]
      · by_cases hsb : cmp (key s) (key b) = .lt
        · simp [*]
        · simp [hsa, hsb]
  · rfl
  · have hab : cmp (key a) (key b) ≠ .lt := fun h' => by
      have := OrientedCmp.gt_of_lt hba; simp_all
    cases h with
    | none => simp [hab, hba]
    | some s =>
      by_cases hsb : cmp (key s) (key b) = .lt
      · have hsa : cmp (key s) (key a) = .lt := TransCmp.lt_trans hsb hba
        simp [*]
      · by_cases hsa : cmp (key s) (key a) = .lt
        · simp [*]
        · simp [hsa, hsb]

theorem join_idem (a : Option E) : join cmp key a a = a := by
  cases a with
  | none => rfl
  | some e => simp [join, keep, ReflCmp.compare_self]

theorem join_comm (a b : Option E) (inj : ∀ x ∈ a, ∀ y ∈ b, key x = key y → x = y) :
    join cmp key a b = join cmp key b a := by
  cases a with
  | none => cases b <;> rfl
  | some x =>
    cases b with
    | none => rfl
    | some y =>
      have := keep_swap (cmp := cmp) (key := key) none x y (inj x rfl y rfl)
      simpa [join, keep] using this

theorem lt_asymm' {x y : K} (h : cmp x y = .lt) : cmp y x ≠ .lt := by
  intro h'; have := OrientedCmp.gt_of_lt h; simp_all

theorem trichotomy {x y : E} (inj : key x = key y → x = y) :
    cmp (key x) (key y) = .lt ∨ x = y ∨ cmp (key y) (key x) = .lt := by
  cases hxy : cmp (key x) (key y) with
  | lt => exact .inl rfl
  | eq => exact .inr (.inl (inj (LawfulEqCmp.eq_of_compare hxy)))
  | gt => exact .inr (.inr (OrientedCmp.lt_of_gt hxy))

theorem isLE_of_not_lt {x y : K} (h : cmp x y ≠ .lt) : (cmp y x).isLE := by
  cases h' : cmp y x with
  | lt => rfl
  | eq => rfl
  | gt => exact absurd (OrientedCmp.lt_of_gt h') h

theorem join_none_left (b : Option E) : join cmp key none b = b := by
  cases b <;> rfl

/-- Associativity needs no hypothesis: on equal keys the left operand is kept throughout. -/
theorem join_assoc (a b c : Option E) :
    join cmp key (join cmp key a b) c = join cmp key a (join cmp key b c) := by
  cases a with
  | none => simp only [join_none_left]
  | some x =>
    cases b with
    | none => cases c <;> rfl
    | some y =>
      cases c with
      | none => rfl
      | some z =>
        simp only [join, keep_held]
        by_cases hyz : cmp (key y) (key z) = .lt
        · by_cases hxy : cmp (key x) (key y) = .lt
          · simp [hxy, hyz, TransCmp.lt_trans hxy hyz]
          · simp [hxy, hyz]
        · by_cases hxy : cmp (key x) (key y) = .lt
          · simp [hxy, hyz]
          · have hzx : (cmp (key z) (key x)).isLE :=
              TransCmp.isLE_trans (isLE_of_not_lt hyz) (isLE_of_not_lt hxy)
            have hxz : cmp (key x) (key z) ≠ .lt := fun h =>
              absurd (TransCmp.lt_of_isLE_of_lt hzx h) (by simp [ReflCmp.compare_self])
            simp [hxy, hyz, hxz]

/-- Folding `keep` from `h` over `l` ends at an entry of `h` or `l` whose key is at least that of
every entry seen. -/
theorem fold_keep_spec (l : List E) (h : Option E) (m : E)
    (hm : l.foldl (keep cmp key) h = some m) :
    (h = some m ∨ m ∈ l) ∧ (∀ x ∈ l, (cmp (key x) (key m)).isLE) ∧
      (∀ s, h = some s → (cmp (key s) (key m)).isLE) := by
  induction l generalizing h with
  | nil =>
    simp only [List.foldl] at hm
    subst hm
    exact ⟨.inl rfl, by simp, fun s hs => by cases hs; exact ReflCmp.isLE_rfl⟩
  | cons x xs ih =>
    simp only [List.foldl] at hm
    have ⟨hmem, hall, hheld⟩ := ih _ hm
    -- what `keep h x` holds is at least `x` and at least what `h` held
    have hx : ∀ s, keep cmp key h x = some s →
        (cmp (key x) (key s)).isLE ∧ (∀ t, h = some t → (cmp (key t) (key s)).isLE) ∧
        (s = x ∨ h = some s) := by
      intro s hs
      cases h with
      | none =>
        simp only [keep, Option.some.injEq] at hs; subst hs
        exact ⟨ReflCmp.isLE_rfl, by simp, .inl rfl⟩
      | some t =>
        rw [keep_held] at hs
        by_cases hlt : cmp (key t) (key x) = .lt
        · simp only [hlt, ↓reduceIte, Option.some.injEq] at hs; subst hs
          exact ⟨ReflCmp.isLE_rfl, fun t' ht' => by cases ht'; rw [hlt]; rfl, .inl rfl⟩
        · simp only [hlt, ↓reduceIte, Option.some.injEq] at hs; subst hs
          exact ⟨isLE_of_not_lt hlt, fun t' ht' => by cases ht'; exact ReflCmp.isLE_rfl, .inr rfl⟩
    have ⟨s, hs⟩ := keep_some (cmp := cmp) (key := key) h x
    have ⟨hxs, hts, hsx⟩ := hx s hs
    have hsm := hheld s hs
    refine ⟨?_, ?_, ?_⟩
    · rcases hmem with hmem | hmem
      · rw [hs] at hmem
        cases hmem
        rcases hsx with rfl | rfl
        · exact .inr (.head _)
        · exact .inl rfl
      · exact .inr (.tail _ hmem)
    · intro y hy
      rcases List.mem_cons.mp hy with rfl | hy
      · exact TransCmp.isLE_trans hxs hsm
      · exact hall y hy
    · intro t ht
      exact TransCmp.isLE_trans (hts t ht) hsm

theorem fold_keep_none (l : List E) (h : Option E) :
    l.foldl (keep cmp key) h = none ↔ h = none ∧ l = [] := by
  induction l generalizing h with
  | nil => simp
  | cons x xs ih =>
    simp only [List.foldl, ih, reduceCtorEq, and_false, iff_false, not_and]
    intro hk
    have ⟨m, hm⟩ := keep_some (cmp := cmp) (key := key) h x
    rw [hm] at hk; cases hk

/-- **Set invariance.** Folding from nothing depends only on which entries the list holds: not
their order, and not how often each appears. -/
theorem fold_keep_eq_of_mem_iff (l₁ l₂ : List E) (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂)
    (inj : KeyInj key l₁) :
    l₁.foldl (keep cmp key) none = l₂.foldl (keep cmp key) none := by
  cases h₁ : l₁.foldl (keep cmp key) none with
  | none =>
    have ⟨_, e₁⟩ := (fold_keep_none l₁ none).mp h₁
    have e₂ : l₂ = [] := List.eq_nil_iff_forall_not_mem.mpr fun x hx => by
      have := (same x).mpr hx; simp_all
    simp [e₂]
  | some m₁ =>
    cases h₂ : l₂.foldl (keep cmp key) none with
    | none =>
      have ⟨_, e₂⟩ := (fold_keep_none l₂ none).mp h₂
      have e₁ : l₁ = [] := List.eq_nil_iff_forall_not_mem.mpr fun x hx => by
        have := (same x).mp hx; simp_all
      simp [e₁] at h₁
    | some m₂ =>
      have ⟨mem₁, all₁, _⟩ := fold_keep_spec l₁ none m₁ h₁
      have ⟨mem₂, all₂, _⟩ := fold_keep_spec l₂ none m₂ h₂
      simp only [reduceCtorEq, false_or] at mem₁ mem₂
      have mem₂' := (same m₂).mpr mem₂
      have le₁ := all₁ m₂ mem₂'
      have le₂ := all₂ m₁ ((same m₁).mp mem₁)
      have heq := OrientedCmp.isLE_antisymm le₁ le₂
      rw [inj m₂ mem₂' m₁ mem₁ (LawfulEqCmp.eq_of_compare heq)]

theorem fold_keep_perm {l₁ l₂ : List E} (p : l₁.Perm l₂) (inj : KeyInj key l₁) (h : Option E) :
    l₁.foldl (keep cmp key) h = l₂.foldl (keep cmp key) h :=
  p.foldl_eq' (fun x hx y hy z => keep_swap z x y (inj x hx y hy)) h

theorem fold_keep_dup (l : List E) (inj : KeyInj key l) :
    (l ++ l).foldl (keep cmp key) none = l.foldl (keep cmp key) none :=
  fold_keep_eq_of_mem_iff _ _ (by simp) (by intro a ha b hb; simp at ha hb; exact inj a ha b hb)

end Keep

/-! ## The Rust merge rules -/

/-- `Strategy`. -/
inductive Strategy where
  | lww
  | max
  deriving DecidableEq, Repr

/-- The `(Value, Hlc)` a register holds. -/
structure Entry (V : Type) where
  value : V
  clock : Hlc
  deriving DecidableEq, Repr

section Wins

variable {V : Type} [Ord V]

/-- `wins`: whether an incoming change replaces what is stored. -/
def wins (strategy : Strategy) (stored : Option (Entry V)) (value : V) (clock : Hlc) : Bool :=
  match stored with
  | none => true
  | some s =>
    match strategy with
    | .lww => compare clock s.clock == .gt
    | .max =>
      match compare value s.value with
      | .gt => true
      | .eq => compare clock s.clock == .gt
      | .lt => false

/-- The body of `State::apply` for one register. -/
def apply (strategy : Strategy) (held : Option (Entry V)) (e : Entry V) : Option (Entry V) :=
  if wins strategy held e.value e.clock then some e else held

/-- The order `Max` compares entries by: value first, then clock. -/
def maxCmp (a b : V × Hlc) : Ordering := compareLex (compareOn (·.1)) (compareOn (·.2)) a b

def Entry.pair (e : Entry V) : V × Hlc := (e.value, e.clock)

theorem apply_lww (held : Option (Entry V)) (e : Entry V) :
    apply .lww held e = keep compare Entry.clock held e := by
  cases held with
  | none => rfl
  | some s =>
    simp only [apply, wins, keep_held]
    have := OrientedCmp.gt_iff_lt (cmp := (compare : Hlc → Hlc → Ordering)) (a := e.clock) (b := s.clock)
    by_cases h : compare s.clock e.clock = .lt <;> simp_all

variable [TransOrd V] [LawfulEqOrd V]

instance : TransCmp (maxCmp (V := V)) :=
  inferInstanceAs (TransCmp (compareLex (compareOn (fun p : V × Hlc => p.1))
    (compareOn (fun p : V × Hlc => p.2))))

instance : LawfulEqCmp (maxCmp (V := V)) where
  eq_of_compare {a b} h := by
    simp only [maxCmp, compareLex_eq_eq, compareOn] at h
    exact Prod.ext (LawfulEqOrd.eq_of_compare h.1) (LawfulEqOrd.eq_of_compare h.2)

theorem wins_max (s : Entry V) (v : V) (c : Hlc) :
    wins .max (some s) v c = ((compare s.value v).then (compare s.clock c) == .lt) := by
  simp only [wins]
  rw [OrientedCmp.eq_swap (cmp := (compare : V → V → Ordering)) (a := v) (b := s.value),
    OrientedCmp.eq_swap (cmp := (compare : Hlc → Hlc → Ordering)) (a := c) (b := s.clock)]
  cases compare s.value v <;> cases compare s.clock c <;> rfl

theorem apply_max (held : Option (Entry V)) (e : Entry V) :
    apply .max held e = keep maxCmp Entry.pair held e := by
  cases held with
  | none => rfl
  | some s =>
    simp only [apply, wins_max, keep_held, maxCmp, Entry.pair, compareLex, compareOn]
    by_cases h : (compare s.value e.value).then (compare s.clock e.clock) = .lt <;> simp [h]

theorem Entry.pair_inj {a b : Entry V} (h : a.pair = b.pair) : a = b := by
  cases a; cases b; simp_all [Entry.pair]

/-- Whatever was given to a register, it holds one of those entries. -/
theorem fold_apply_mem (s : Strategy) (l : List (Entry V)) (m : Entry V)
    (h : l.foldl (apply s) none = some m) : m ∈ l := by
  cases s
  · rw [show apply (V := V) .lww = keep compare Entry.clock from funext fun h => funext fun e => apply_lww h e] at h
    simpa using (fold_keep_spec l none m h).1
  · rw [show apply (V := V) .max = keep maxCmp Entry.pair from funext fun h => funext fun e => apply_max h e] at h
    simpa using (fold_keep_spec l none m h).1

/-- No two different entries of `l` share a clock. -/
def UniqueClocks (l : List (Entry V)) : Prop := KeyInj Entry.clock l

/-! ### Register laws, for both strategies -/

theorem apply_twice (s : Strategy) (h : Option (Entry V)) (e : Entry V) :
    apply s (apply s h e) e = apply s h e := by
  cases s
  · simp only [apply_lww]; exact keep_idem h e
  · simp only [apply_max]; exact keep_idem h e

/-- Two changes to one field may arrive in either order. `Max` needs nothing; `Lww` needs the
two clocks to differ unless the changes are the same. -/
theorem apply_any_order (s : Strategy) (h : Option (Entry V)) (a b : Entry V)
    (unique : a.clock = b.clock → a = b) :
    apply s (apply s h a) b = apply s (apply s h b) a := by
  cases s
  · simp only [apply_lww]; exact keep_swap h a b unique
  · simp only [apply_max]; exact keep_swap h a b Entry.pair_inj

/-- Bend's `merge_keeps_later_clock`: under `Lww` a register ends at the later of the two clocks. -/
theorem apply_lww_clock (held e : Entry V) :
    (apply .lww (some held) e).map (·.clock) =
      some (if held.clock < e.clock then e.clock else held.clock) := by
  rw [apply_lww, keep_held]
  by_cases h : held.clock < e.clock
  · have h' : compare held.clock e.clock = .lt := h
    simp [h, h']
  · have h' : compare held.clock e.clock ≠ .lt := h
    simp [h, h']

/-- `join` for a strategy: the register holding the winner of two registers. -/
def merge (s : Strategy) : Option (Entry V) → Option (Entry V) → Option (Entry V)
  | a, none => a
  | a, some e => apply s a e

theorem merge_eq_join (s : Strategy) (a b : Option (Entry V)) :
    merge s a b = match s with
      | .lww => join compare Entry.clock a b
      | .max => join maxCmp Entry.pair a b := by
  cases s <;> cases b <;> simp [merge, join, apply_lww, apply_max]

theorem merge_idem (s : Strategy) (a : Option (Entry V)) : merge s a a = a := by
  rw [merge_eq_join]; cases s <;> exact join_idem a

theorem merge_comm (s : Strategy) (a b : Option (Entry V))
    (unique : ∀ x ∈ a, ∀ y ∈ b, x.clock = y.clock → x = y) :
    merge s a b = merge s b a := by
  rw [merge_eq_join, merge_eq_join]
  cases s
  · exact join_comm a b unique
  · exact join_comm a b fun x _ y _ h => Entry.pair_inj h

theorem merge_assoc (s : Strategy) (a b c : Option (Entry V)) :
    merge s (merge s a b) c = merge s a (merge s b c) := by
  simp only [merge_eq_join]
  cases s
  · exact join_assoc a b c
  · exact join_assoc a b c

/-- **Register convergence.** A register fed the same set of entries ends the same, whatever the
order and however often each arrived, provided clocks are unique. -/
theorem fold_apply_eq_of_mem_iff (s : Strategy) (l₁ l₂ : List (Entry V))
    (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) (unique : UniqueClocks l₁) :
    l₁.foldl (apply s) none = l₂.foldl (apply s) none := by
  cases s
  · simp only [show apply (V := V) .lww = keep compare Entry.clock from funext fun h => funext fun e => apply_lww h e]
    exact fold_keep_eq_of_mem_iff l₁ l₂ same unique
  · simp only [show apply (V := V) .max = keep maxCmp Entry.pair from funext fun h => funext fun e => apply_max h e]
    exact fold_keep_eq_of_mem_iff l₁ l₂ same fun a _ b _ h => Entry.pair_inj h

theorem fold_apply_perm (s : Strategy) {l₁ l₂ : List (Entry V)} (p : l₁.Perm l₂)
    (unique : UniqueClocks l₁) (h : Option (Entry V)) :
    l₁.foldl (apply s) h = l₂.foldl (apply s) h :=
  p.foldl_eq' (fun x hx y hy z => apply_any_order s z x y (unique x hx y hy)) h

theorem fold_apply_dup (s : Strategy) (l : List (Entry V)) (unique : UniqueClocks l) :
    (l ++ l).foldl (apply s) none = l.foldl (apply s) none :=
  fold_apply_eq_of_mem_iff s _ _ (by simp) (by intro a ha b hb; simp at ha hb; exact unique a ha b hb)

end Wins

end FongoSync
