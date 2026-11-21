import FongoSync.Clock

/-!
# Sets of gids as sorted lists

The Rust iterates a `BTreeMap` or `BTreeSet` of gids in ascending order. Here such a set is a
strictly increasing list, so two lists holding the same gids are equal, not just alike.
-/

namespace FongoSync

/-- Insert into a strictly increasing list, keeping it so. -/
def insertGid (g : Gid) : List Gid → List Gid
  | [] => [g]
  | h :: t => if g < h then g :: h :: t else if g = h then h :: t else h :: insertGid g t

/-- The gids of `l`, ascending, each once. -/
def gidSet (l : List Gid) : List Gid := l.foldr insertGid []

theorem mem_insertGid {g x : Gid} {l : List Gid} : x ∈ insertGid g l ↔ x = g ∨ x ∈ l := by
  induction l with
  | nil => simp [insertGid]
  | cons h t ih =>
    unfold insertGid
    by_cases h₁ : g < h
    · simp [h₁]
    · by_cases h₂ : g = h
      · subst h₂; simp [h₁]
      · simp only [h₁, h₂, ↓reduceIte, List.mem_cons, ih]
        constructor
        · rintro (h | h | h) <;> simp_all
        · rintro (h | h | h) <;> simp_all

theorem sorted_insertGid {g : Gid} {l : List Gid} (s : l.Pairwise (· < ·)) :
    (insertGid g l).Pairwise (· < ·) := by
  induction l with
  | nil => simp [insertGid]
  | cons h t ih =>
    rw [List.pairwise_cons] at s
    unfold insertGid
    by_cases h₁ : g < h
    · simp only [h₁, ↓reduceIte, List.pairwise_cons, List.mem_cons]
      refine ⟨?_, s.1, s.2⟩
      rintro x (rfl | hx)
      · exact h₁
      · exact Nat.lt_trans h₁ (s.1 x hx)
    · by_cases h₂ : g = h
      · subst h₂; simp only [Nat.lt_irrefl, ↓reduceIte, List.pairwise_cons]; exact s
      · simp only [h₁, h₂, ↓reduceIte, List.pairwise_cons]
        refine ⟨fun x hx => ?_, ih s.2⟩
        rcases mem_insertGid.mp hx with rfl | hx
        · exact Nat.lt_of_le_of_ne (Nat.not_lt.mp h₁) (Ne.symm h₂)
        · exact s.1 x hx

theorem mem_gidSet {x : Gid} {l : List Gid} : x ∈ gidSet l ↔ x ∈ l := by
  induction l with
  | nil => simp [gidSet]
  | cons h t ih =>
    simp only [gidSet, List.foldr_cons] at *
    rw [mem_insertGid, ih, List.mem_cons]

theorem sorted_gidSet (l : List Gid) : (gidSet l).Pairwise (· < ·) := by
  induction l with
  | nil => simp [gidSet]
  | cons h t ih => exact sorted_insertGid ih

/-- Two strictly increasing lists with the same members are the same list. -/
theorem eq_of_sorted {l₁ l₂ : List Gid} (s₁ : l₁.Pairwise (· < ·)) (s₂ : l₂.Pairwise (· < ·))
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
          · exact absurd (Nat.lt_trans (s₂.1 a h) (s₁.1 b h')) (Nat.lt_irrefl b)
      subst hab
      congr 1
      apply ih s₁.2 s₂.2
      intro x
      constructor
      · intro hx
        rcases List.mem_cons.mp ((same x).mp (.tail _ hx)) with h | h
        · subst h; exact absurd (s₁.1 x hx) (Nat.lt_irrefl x)
        · exact h
      · intro hx
        rcases List.mem_cons.mp ((same x).mpr (.tail _ hx)) with h | h
        · subst h; exact absurd (s₂.1 x hx) (Nat.lt_irrefl x)
        · exact h

theorem gidSet_eq_of_mem_iff {l₁ l₂ : List Gid} (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    gidSet l₁ = gidSet l₂ :=
  eq_of_sorted (sorted_gidSet l₁) (sorted_gidSet l₂) fun x => by simp [mem_gidSet, same]

end FongoSync
