import FongoSync.Clock

/-!
# Alias groups

An alias edge says two gids are one entity. Every edge ever seen counts, so the groups are the
connected components of all edges, and a group's canonical gid is its smallest member.

`canonical` computes this by folding over the edges and relabelling: joining two groups gives
every member of both the smaller of the two labels. A union-find whose smaller root wins gives the
same answer; the fold is simply easier to reason about, and the answer is what is proven here.
-/

namespace FongoSync

abbrev Edge := Gid × Gid

/-- The equivalence relation the edges generate. -/
inductive Connected (E : List Edge) : Gid → Gid → Prop
  | refl (a : Gid) : Connected E a a
  | edge {a b : Gid} : (a, b) ∈ E → Connected E a b
  | symm {a b : Gid} : Connected E a b → Connected E b a
  | trans {a b c : Gid} : Connected E a b → Connected E b c → Connected E a c

/-- `r` is the smallest gid in `g`'s group. -/
def IsCanonical (E : List Edge) (g r : Gid) : Prop :=
  Connected E g r ∧ ∀ h, Connected E g h → r ≤ h

/-- Join the groups of an edge's two ends under the smaller of their labels. -/
def union (label : Gid → Gid) (e : Edge) : Gid → Gid :=
  let ra := label e.1
  let rb := label e.2
  fun g => if label g = ra ∨ label g = rb then min ra rb else label g

/-- Each gid's canonical gid, from every edge seen. -/
def canonical (E : List Edge) : Gid → Gid :=
  E.foldl union id

theorem Connected.mono {E₁ E₂ : List Edge} (sub : ∀ e ∈ E₁, e ∈ E₂) {a b : Gid}
    (h : Connected E₁ a b) : Connected E₂ a b := by
  induction h with
  | refl a => exact .refl a
  | edge he => exact .edge (sub _ he)
  | symm _ ih => exact .symm ih
  | trans _ _ ih₁ ih₂ => exact .trans ih₁ ih₂

theorem Connected.nil {a b : Gid} (h : Connected [] a b) : a = b := by
  induction h with
  | refl => rfl
  | edge he => cases he
  | symm _ ih => exact ih.symm
  | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂

theorem IsCanonical.unique {E : List Edge} {g r r' : Gid} (h : IsCanonical E g r)
    (h' : IsCanonical E g r') : r = r' :=
  Nat.le_antisymm (h.2 r' h'.1) (h'.2 r h.1)

theorem isCanonical_nil (g : Gid) : IsCanonical [] g g :=
  ⟨.refl g, fun _ h => Nat.le_of_eq h.nil⟩

section Step

variable {P : List Edge} {L : Gid → Gid} (inv : ∀ g, IsCanonical P g (L g))
include inv

theorem label_eq_of_connected {g h : Gid} (c : Connected P g h) : L g = L h :=
  Nat.le_antisymm ((inv g).2 _ (.trans c (inv h).1)) ((inv h).2 _ (.trans (.symm c) (inv g).1))

theorem connected_of_label_eq {g h : Gid} (e : L g = L h) : Connected P g h :=
  .trans (inv g).1 (e ▸ .symm (inv h).1)

theorem label_le (g : Gid) : L g ≤ g := (inv g).2 g (.refl g)

/-- Adding one edge keeps every label canonical. -/
theorem isCanonical_union (e : Edge) (g : Gid) :
    IsCanonical (P ++ [e]) g (union L e g) := by
  have sub : ∀ x ∈ P, x ∈ P ++ [e] := fun x hx => List.mem_append_left _ hx
  have new : Connected (P ++ [e]) e.1 e.2 := .edge (by simp)
  -- the labels after the union are constant on the new groups
  have same : ∀ {x y}, Connected (P ++ [e]) x y → union L e x = union L e y := by
    intro x y c
    induction c with
    | refl => rfl
    | edge he =>
      rcases List.mem_append.mp he with he | he
      · simp only [union, label_eq_of_connected inv (.edge he)]
      · simp only [List.mem_singleton] at he
        subst he
        simp [union]
    | symm _ ih => exact ih.symm
    | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂
  -- and never above the gid
  have below : ∀ x, union L e x ≤ x := by
    intro x
    dsimp only [union]
    split
    · rename_i hx
      have := label_le inv x
      rcases hx with hx | hx
      · exact Nat.le_trans (Nat.min_le_left _ _) (hx ▸ this)
      · exact Nat.le_trans (Nat.min_le_right _ _) (hx ▸ this)
    · exact label_le inv x
  refine ⟨?_, fun h c => (same c) ▸ below h⟩
  -- the new label is in the group
  dsimp only [union]
  by_cases ha : L g = L e.1
  · have g1 : Connected (P ++ [e]) g e.1 := (connected_of_label_eq inv ha).mono sub
    simp only [ha, true_or, ↓reduceIte]
    rcases Nat.le_total (L e.1) (L e.2) with le | le
    · rw [Nat.min_eq_left le]; exact ha ▸ ((inv g).1.mono sub)
    · rw [Nat.min_eq_right le]; exact .trans g1 (.trans new ((inv e.2).1.mono sub))
  · by_cases hb : L g = L e.2
    · have g2 : Connected (P ++ [e]) g e.2 := (connected_of_label_eq inv hb).mono sub
      simp only [hb, or_true, ↓reduceIte]
      rcases Nat.le_total (L e.1) (L e.2) with le | le
      · rw [Nat.min_eq_left le]; exact .trans g2 (.trans (.symm new) ((inv e.1).1.mono sub))
      · rw [Nat.min_eq_right le]; exact hb ▸ ((inv g).1.mono sub)
    · simp only [ha, hb, or_self, ↓reduceIte]
      exact (inv g).1.mono sub

end Step

theorem isCanonical_foldl (E P : List Edge) (L : Gid → Gid) (inv : ∀ g, IsCanonical P g (L g)) :
    ∀ g, IsCanonical (P ++ E) g (E.foldl union L g) := by
  induction E generalizing P L with
  | nil => simpa using inv
  | cons e E ih =>
    simp only [List.foldl]
    have := ih (P ++ [e]) (union L e) (isCanonical_union inv e)
    simpa using this

/-- **`canonical` is right**: each gid's label is the smallest gid in its group. -/
theorem canonical_spec (E : List Edge) (g : Gid) : IsCanonical E g (canonical E g) := by
  have := isCanonical_foldl E [] id isCanonical_nil g
  simpa [canonical] using this

/-- Two gids are in one group exactly when they have the same canonical gid. -/
theorem canonical_eq_iff (E : List Edge) (a b : Gid) :
    canonical E a = canonical E b ↔ Connected E a b :=
  ⟨connected_of_label_eq (canonical_spec E), label_eq_of_connected (canonical_spec E)⟩

theorem isCanonical_of_mem_iff {E₁ E₂ : List Edge} (same : ∀ e, e ∈ E₁ ↔ e ∈ E₂) {g r : Gid}
    (h : IsCanonical E₁ g r) : IsCanonical E₂ g r :=
  ⟨h.1.mono fun e he => (same e).mp he,
   fun x c => h.2 x (c.mono fun e he => (same e).mpr he)⟩

/-- **Alias groups converge.** The groups and canonical gids depend only on which edges were
seen: not their order, and not how often each arrived. -/
theorem canonical_eq_of_mem_iff (E₁ E₂ : List Edge) (same : ∀ e, e ∈ E₁ ↔ e ∈ E₂) :
    canonical E₁ = canonical E₂ := by
  funext g
  exact (isCanonical_of_mem_iff same (canonical_spec E₁ g)).unique (canonical_spec E₂ g)

theorem connected_iff_of_mem_iff {E₁ E₂ : List Edge} (same : ∀ e, e ∈ E₁ ↔ e ∈ E₂) (a b : Gid) :
    Connected E₁ a b ↔ Connected E₂ a b :=
  ⟨fun h => h.mono fun e he => (same e).mp he, fun h => h.mono fun e he => (same e).mpr he⟩

theorem canonical_perm {E₁ E₂ : List Edge} (p : E₁.Perm E₂) : canonical E₁ = canonical E₂ :=
  canonical_eq_of_mem_iff E₁ E₂ fun _ => p.mem_iff

theorem canonical_dup (E : List Edge) : canonical (E ++ E) = canonical E :=
  canonical_eq_of_mem_iff _ _ (by simp)

end FongoSync
