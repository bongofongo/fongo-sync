import FongoSync.Register

/-!
# A replica's merged state

Mirrors `Change` and `State` in `crates/core/src/change.rs`. The Rust `BTreeMap` keyed by
`(collection, gid, field)` is a function here, and the strategy the caller passes to
`State::apply` comes from `strategyOf collection field`.
-/

namespace FongoSync

open Std

set_option linter.unusedSectionVars false

/-- `DELETED`: the field that carries an entity's tombstone. -/
def DELETED : String := "$deleted"

/-- `Change`. -/
structure Change (V : Type) where
  collection : String
  gid : Gid
  field : String
  value : V
  clock : Hlc
  deriving DecidableEq, Repr

variable {V : Type}

abbrev Key := String × Gid × String

def Change.key (c : Change V) : Key := (c.collection, c.gid, c.field)

def Change.entry (c : Change V) : Entry V := ⟨c.value, c.clock⟩

/-- Two changes to one register (same collection, gid and field) at one clock carry one value. -/
def Compatible (a b : Change V) : Prop :=
  a.key = b.key → a.clock = b.clock → a.value = b.value

/-- No two changes to the same field share a clock with different values. Honest clocks give
every change its own clock, which is stronger. -/
def ClockDetermines (cs : List (Change V)) : Prop :=
  ∀ a ∈ cs, ∀ b ∈ cs, a.field = b.field → a.clock = b.clock → a.value = b.value

/-- Which strategy a field merges by; `$deleted` always merges `Lww`. -/
def strategyAt (strategyOf : String → String → Strategy) (k : Key) : Strategy :=
  if k.2.2 = DELETED then .lww else strategyOf k.1 k.2.2

abbrev State (V : Type) := Key → Option (Entry V)

def State.empty : State V := fun _ => none

variable [Ord V]

/-- `State::apply`. -/
def State.apply (strategyOf : String → String → Strategy) (st : State V) (c : Change V) : State V :=
  fun k => if k = c.key then FongoSync.apply (strategyAt strategyOf c.key) (st k) c.entry else st k

/-- The state a replica reaches by applying `cs` in order, starting empty. -/
def merged (strategyOf : String → String → Strategy) (cs : List (Change V)) : State V :=
  cs.foldl (State.apply strategyOf) State.empty

/-- The changes to one register, as entries. -/
def entriesAt (cs : List (Change V)) (k : Key) : List (Entry V) :=
  (cs.filter (·.key = k)).map Change.entry

theorem foldl_state_apply (σ : String → String → Strategy) (cs : List (Change V)) (st : State V)
    (k : Key) :
    (cs.foldl (State.apply σ) st) k = (entriesAt cs k).foldl (apply (strategyAt σ k)) (st k) := by
  induction cs generalizing st with
  | nil => rfl
  | cons c cs ih =>
    simp only [List.foldl, ih, entriesAt]
    by_cases h : c.key = k
    · simp [List.filter, h, State.apply]
    · have h' : k ≠ c.key := fun e => h e.symm
      simp [List.filter, h, h', State.apply]

/-- One register of the merged state is the fold of that register's changes. -/
theorem merged_at (σ : String → String → Strategy) (cs : List (Change V)) (k : Key) :
    merged σ cs k = (entriesAt cs k).foldl (apply (strategyAt σ k)) none :=
  foldl_state_apply σ cs State.empty k

omit [Ord V] in
theorem mem_entriesAt {cs : List (Change V)} {k : Key} {e : Entry V} :
    e ∈ entriesAt cs k ↔ ∃ c ∈ cs, c.key = k ∧ c.entry = e := by
  simp [entriesAt, and_assoc]

theorem uniqueClocks_entriesAt {cs : List (Change V)} (h : ClockDetermines cs) (k : Key) :
    UniqueClocks (entriesAt cs k) := by
  intro a ha b hb hab
  obtain ⟨ca, hca, kca, rfl⟩ := mem_entriesAt.mp ha
  obtain ⟨cb, hcb, kcb, rfl⟩ := mem_entriesAt.mp hb
  have hf : ca.field = cb.field := by
    have := kca.trans kcb.symm
    simp only [Change.key, Prod.mk.injEq] at this
    exact this.2.2
  have hv := h ca hca cb hcb hf hab
  simp only [Change.entry] at hab ⊢
  rw [hv, hab]

variable [TransOrd V] [LawfulEqOrd V]

/-- Two compatible changes may be applied in either order, to any state. -/
theorem State.apply_swap (σ : String → String → Strategy) (st : State V) (a b : Change V)
    (h : Compatible a b) :
    State.apply σ (State.apply σ st a) b = State.apply σ (State.apply σ st b) a := by
  funext k
  simp only [State.apply]
  by_cases ha : k = a.key <;> by_cases hb : k = b.key
  · subst ha
    have hab : a.key = b.key := hb
    simp only [hb, ↓reduceIte]
    rw [← hab]
    apply apply_any_order
    intro hc
    have hv := h hab hc
    simp only [Change.entry] at hc ⊢
    rw [hv, hc]
  · subst ha
    have hab : a.key ≠ b.key := hb
    simp [hab]
  · subst hb
    have hab : b.key ≠ a.key := ha
    simp [hab]
  · simp [ha, hb]

/-- Applying a change a second time changes nothing. -/
theorem State.apply_twice (σ : String → String → Strategy) (st : State V) (a : Change V) :
    State.apply σ (State.apply σ st a) a = State.apply σ st a := by
  funext k
  simp only [State.apply]
  by_cases ha : k = a.key
  · simp only [ha, ↓reduceIte]; exact FongoSync.apply_twice _ _ _
  · simp [ha]

/-- Swapping two neighbouring compatible changes anywhere in a delivery, from any state, leaves
every register the same. -/
theorem State.foldl_swap (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a b : Change V) (h : Compatible a b) :
    (pre ++ a :: b :: post).foldl (State.apply σ) st =
      (pre ++ b :: a :: post).foldl (State.apply σ) st := by
  simp only [List.foldl_append, List.foldl_cons, State.apply_swap σ _ a b h]

/-- Receiving a change twice in a row anywhere in a delivery is receiving it once. -/
theorem State.foldl_twice (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a : Change V) :
    (pre ++ a :: a :: post).foldl (State.apply σ) st =
      (pre ++ a :: post).foldl (State.apply σ) st := by
  simp only [List.foldl_append, List.foldl_cons, State.apply_twice]

/-- **Convergence of the merged state.** Two replicas that applied the same set of changes hold
the same state, whatever the order and however often each change arrived. -/
theorem merged_eq_of_mem_iff (σ : String → String → Strategy) (cs₁ cs₂ : List (Change V))
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (h : ClockDetermines cs₁) :
    merged σ cs₁ = merged σ cs₂ := by
  funext k
  rw [merged_at, merged_at]
  apply fold_apply_eq_of_mem_iff _ _ _ _ (uniqueClocks_entriesAt h k)
  intro e
  simp only [mem_entriesAt, same]

theorem merged_perm (σ : String → String → Strategy) {cs₁ cs₂ : List (Change V)}
    (p : cs₁.Perm cs₂) (h : ClockDetermines cs₁) : merged σ cs₁ = merged σ cs₂ :=
  merged_eq_of_mem_iff σ cs₁ cs₂ (fun _ => p.mem_iff) h

theorem merged_dup (σ : String → String → Strategy) (cs : List (Change V))
    (h : ClockDetermines cs) : merged σ (cs ++ cs) = merged σ cs :=
  merged_eq_of_mem_iff σ _ _ (by simp) (by
    intro a ha b hb; simp only [List.mem_append, or_self] at ha hb; exact h a ha b hb)

/-- Two replicas that received the same batches, each in its own order and with repeats, agree. -/
theorem merged_batches (σ : String → String → Strategy) (b₁ b₂ : List (List (Change V)))
    (same : ∀ b, b ∈ b₁ ↔ b ∈ b₂) (h : ClockDetermines b₁.flatten) :
    merged σ b₁.flatten = merged σ b₂.flatten :=
  merged_eq_of_mem_iff σ _ _ (by simp [same]) h

end FongoSync
