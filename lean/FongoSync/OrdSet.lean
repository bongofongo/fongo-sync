import FongoSync.View

/-!
# Ordered sets, and the earliest writes to a key field

`ordSet cmp l` is `l` sorted by `cmp` with each member once, as a `BTreeSet` iterates it; two
lists with the same members give the same list. `earliest` is the selection `model::view` makes
among the changes to one key field of a natural entity: the `KEY_ENTRIES` smallest
`(clock, value)` pairs. Being a function of the set of pairs, it does not depend on the order
changes arrive in or on how often they arrive.
-/

namespace FongoSync

open Std

set_option linter.unusedSectionVars false

section OrdSet

variable {α : Type} (cmp : α → α → Ordering) [TransCmp cmp] [LawfulEqCmp cmp]

/-- Insert into a list strictly increasing by `cmp`, keeping it so. -/
def ordInsert (x : α) : List α → List α
  | [] => [x]
  | h :: t =>
    match cmp x h with
    | .lt => x :: h :: t
    | .eq => h :: t
    | .gt => h :: ordInsert x t

/-- The members of `l`, ascending by `cmp`, each once. -/
def ordSet (l : List α) : List α := l.foldr (ordInsert cmp) []

theorem mem_ordInsert {x y : α} {l : List α} : y ∈ ordInsert cmp x l ↔ y = x ∨ y ∈ l := by
  induction l with
  | nil => simp [ordInsert]
  | cons h t ih =>
    unfold ordInsert
    split
    · simp
    · rename_i hx
      have := LawfulEqCmp.eq_of_compare hx
      subst this
      simp
    · simp only [List.mem_cons, ih]
      constructor
      · rintro (h | h | h) <;> simp_all
      · rintro (h | h | h) <;> simp_all

theorem ascending_ordInsert {x : α} {l : List α} (s : l.Pairwise (fun a b => cmp a b = .lt)) :
    (ordInsert cmp x l).Pairwise (fun a b => cmp a b = .lt) := by
  induction l with
  | nil => simp [ordInsert]
  | cons h t ih =>
    rw [List.pairwise_cons] at s
    unfold ordInsert
    split
    · rename_i hx
      simp only [List.pairwise_cons, List.mem_cons]
      refine ⟨?_, s.1, s.2⟩
      rintro y (rfl | hy)
      · exact hx
      · exact TransCmp.lt_trans hx (s.1 y hy)
    · simp only [List.pairwise_cons]; exact s
    · rename_i hx
      simp only [List.pairwise_cons]
      refine ⟨fun y hy => ?_, ih s.2⟩
      rcases (mem_ordInsert cmp).mp hy with rfl | hy
      · exact OrientedCmp.lt_of_gt hx
      · exact s.1 y hy

theorem mem_ordSet {x : α} {l : List α} : x ∈ ordSet cmp l ↔ x ∈ l := by
  induction l with
  | nil => simp [ordSet]
  | cons h t ih =>
    simp only [ordSet, List.foldr_cons] at *
    rw [mem_ordInsert, ih, List.mem_cons]

theorem ascending_ordSet (l : List α) : (ordSet cmp l).Pairwise (fun a b => cmp a b = .lt) := by
  induction l with
  | nil => simp [ordSet]
  | cons h t ih => exact ascending_ordInsert cmp ih

theorem lt_irrefl_cmp (x : α) : cmp x x ≠ .lt := by
  simp [ReflCmp.compare_self]

/-- Two strictly increasing lists with the same members are the same list. -/
theorem eq_of_ascending {l₁ l₂ : List α} (s₁ : l₁.Pairwise (fun a b => cmp a b = .lt))
    (s₂ : l₂.Pairwise (fun a b => cmp a b = .lt))
    (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
    cases l₂ with
    | nil => rfl
    | cons b u => exact absurd ((same b).mpr (.head _)) (by simp)
  | cons a t ih =>
    cases l₂ with
    | nil => exact absurd ((same a).mp (.head _)) (by simp)
    | cons b u =>
      rw [List.pairwise_cons] at s₁ s₂
      have hab : a = b := by
        rcases List.mem_cons.mp ((same a).mp (.head _)) with h | h
        · exact h
        · rcases List.mem_cons.mp ((same b).mpr (.head _)) with h' | h'
          · exact h'.symm
          · exact absurd (TransCmp.lt_trans (s₂.1 a h) (s₁.1 b h')) (lt_irrefl_cmp cmp b)
      subst hab
      congr 1
      apply ih s₁.2 s₂.2
      intro x
      constructor
      · intro hx
        rcases List.mem_cons.mp ((same x).mp (.tail _ hx)) with h | h
        · subst h; exact absurd (s₁.1 x hx) (lt_irrefl_cmp cmp x)
        · exact h
      · intro hx
        rcases List.mem_cons.mp ((same x).mpr (.tail _ hx)) with h | h
        · subst h; exact absurd (s₂.1 x hx) (lt_irrefl_cmp cmp x)
        · exact h

theorem ordSet_eq_of_mem_iff {l₁ l₂ : List α} (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    ordSet cmp l₁ = ordSet cmp l₂ :=
  eq_of_ascending cmp (ascending_ordSet cmp l₁) (ascending_ordSet cmp l₂)
    fun x => by simp [mem_ordSet, same]

end OrdSet

/-! ## The earliest writes to a key field -/

/-- `model::KEY_ENTRIES`. -/
def KEY_ENTRIES : Nat := 4

/-- `(clock, value)` pairs compare by clock, then by value. -/
def writeCmp (a b : Hlc × Value) : Ordering :=
  compareLex (compareOn (·.1)) (compareOn (·.2)) a b

instance : TransCmp writeCmp :=
  inferInstanceAs (TransCmp (compareLex (compareOn (fun p : Hlc × Value => p.1))
    (compareOn (fun p : Hlc × Value => p.2))))

instance : LawfulEqCmp writeCmp where
  eq_of_compare {a b} h := by
    simp only [writeCmp, compareLex_eq_eq, compareOn] at h
    exact Prod.ext (LawfulEqOrd.eq_of_compare h.1) (LawfulEqOrd.eq_of_compare h.2)

/-- Of the `(clock, value)` pairs written to one key field, the `KEY_ENTRIES` smallest, ascending,
each once. -/
def earliest (writes : List (Hlc × Value)) : List (Hlc × Value) :=
  (ordSet writeCmp writes).take KEY_ENTRIES

/-- The selection depends only on the set of pairs written. -/
theorem earliest_eq_of_mem_iff {l₁ l₂ : List (Hlc × Value)} (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    earliest l₁ = earliest l₂ := by
  simp only [earliest, ordSet_eq_of_mem_iff writeCmp same]

/-- Writes may arrive in any order. -/
theorem earliest_perm {l₁ l₂ : List (Hlc × Value)} (p : l₁.Perm l₂) : earliest l₁ = earliest l₂ :=
  earliest_eq_of_mem_iff fun _ => p.mem_iff

/-- A write that arrives again changes nothing. -/
theorem earliest_redelivered (l : List (Hlc × Value)) (x : Hlc × Value) (h : x ∈ l) :
    earliest (x :: l) = earliest l :=
  earliest_eq_of_mem_iff fun y => by
    simp only [List.mem_cons]; constructor
    · rintro (rfl | hy)
      · exact h
      · exact hy
    · exact .inr

theorem mem_of_mem_earliest {l : List (Hlc × Value)} {x : Hlc × Value} (h : x ∈ earliest l) :
    x ∈ l :=
  (mem_ordSet writeCmp).mp (List.mem_of_mem_take h)

end FongoSync
