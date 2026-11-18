import FongoSync.Examples
import FongoSync.Model

/-!
# The laws

Everything this project claims, in one place. Each law is stated in full here and proven
elsewhere in `FongoSync`; read this file for what is claimed, the others for why.
`make lean` checks that each rests only on Lean's standard axioms.

The laws are about the Lean restatement of the Rust, not about the Rust itself.
-/

namespace FongoSync.Laws

open FongoSync

/-! ## Clock -/

/-- A tick's reading is later than the clock's last reading, whatever the wall says. -/
theorem tick_later (c : Clock) (wall : Nat) : c.last < (c.tick wall).1 :=
  c.tick_lt wall

/-- Observing a reading the text form cannot hold changes nothing. -/
theorem observe_ignores_unencodable (c : Clock) (remote : Hlc) (h : ¬ remote.encodable) :
    c.observe remote = c :=
  c.observe_ignores remote h

/-- The next tick after observing an encodable reading is later than it. -/
theorem tick_after_observe (c : Clock) (remote : Hlc) (wall : Nat) (h : remote.encodable) :
    remote < ((c.observe remote).tick wall).1 :=
  c.observe_then_tick remote wall h

/-- After any history, the next tick is later than the start, than every reading issued, and
than every encodable reading observed. -/
theorem tick_after_any_history (c : Clock) (events : List Event) (wall : Nat) :
    c.last < ((c.run events).1.tick wall).1 ∧
      ∀ r ∈ (c.run events).2, r < ((c.run events).1.tick wall).1 :=
  c.tick_after_any_history events wall

/-! ## One register, for any value type with a total order -/

section Register

variable {V : Type} [Ord V] [Std.TransOrd V] [Std.LawfulEqOrd V]

/-- Merging two registers is commutative when equal clocks mean equal entries. -/
theorem merge_comm (s : Strategy) (a b : Option (Entry V))
    (unique : ∀ x ∈ a, ∀ y ∈ b, x.clock = y.clock → x = y) :
    merge s a b = merge s b a :=
  FongoSync.merge_comm s a b unique

/-- Merging registers is associative, with no condition. -/
theorem merge_assoc (s : Strategy) (a b c : Option (Entry V)) :
    merge s (merge s a b) c = merge s a (merge s b c) :=
  FongoSync.merge_assoc s a b c

/-- Merging a register with itself changes nothing. -/
theorem merge_idem (s : Strategy) (a : Option (Entry V)) : merge s a a = a :=
  FongoSync.merge_idem s a

/-- Under `Lww` a register ends at the later of the two clocks. -/
theorem lww_keeps_later_clock (held e : Entry V) :
    (apply .lww (some held) e).map (·.clock) =
      some (if held.clock < e.clock then e.clock else held.clock) :=
  apply_lww_clock held e

/-- Receiving a change twice is receiving it once, under either strategy. -/
theorem apply_twice (s : Strategy) (h : Option (Entry V)) (e : Entry V) :
    apply s (apply s h e) e = apply s h e :=
  FongoSync.apply_twice s h e

/-- Two changes may arrive in either order, unless they share a clock and differ. -/
theorem apply_any_order (s : Strategy) (h : Option (Entry V)) (a b : Entry V)
    (unique : a.clock = b.clock → a = b) :
    apply s (apply s h a) b = apply s (apply s h b) a :=
  FongoSync.apply_any_order s h a b unique

/-- Under `Max`, two changes may arrive in either order, with no condition. -/
theorem max_any_order (h : Option (Entry V)) (a b : Entry V) :
    apply .max (apply .max h a) b = apply .max (apply .max h b) a := by
  simp only [apply_max]; exact keep_swap h a b Entry.pair_inj

/-- A register fed the same set of entries ends the same, whatever the order and repeats. -/
theorem register_set (s : Strategy) (l₁ l₂ : List (Entry V)) (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂)
    (unique : UniqueClocks l₁) :
    l₁.foldl (apply s) none = l₂.foldl (apply s) none :=
  fold_apply_eq_of_mem_iff s l₁ l₂ same unique

/-! ## A replica's state -/

/-- Swapping two neighbouring changes anywhere in a delivery, from any state, leaves every
register the same, unless they write one register at one clock with different values. -/
theorem state_swap (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a b : Change V) (h : Compatible a b) :
    (pre ++ a :: b :: post).foldl (State.apply σ) st =
      (pre ++ b :: a :: post).foldl (State.apply σ) st :=
  State.foldl_swap σ st pre post a b h

/-- Receiving a change twice in a row anywhere in a delivery is receiving it once. -/
theorem state_twice (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a : Change V) :
    (pre ++ a :: a :: post).foldl (State.apply σ) st =
      (pre ++ a :: post).foldl (State.apply σ) st :=
  State.foldl_twice σ st pre post a

/-- Replicas that applied the same set of changes hold the same state. -/
theorem state_set (σ : String → String → Strategy) (cs₁ cs₂ : List (Change V))
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁) :
    merged σ cs₁ = merged σ cs₂ :=
  merged_eq_of_mem_iff σ cs₁ cs₂ same unique

end Register

/-- Without unique clocks the order of arrival can matter. -/
theorem unique_clocks_needed :
    let σ : String → String → Strategy := fun _ _ => .lww
    let a : Change Value := ⟨"things", 0, "title", .int 1, ⟨1, 0, 1⟩⟩
    let b : Change Value := ⟨"things", 0, "title", .int 2, ⟨1, 0, 1⟩⟩
    merged σ [a, b] ("things", 0, "title") ≠ merged σ [b, a] ("things", 0, "title") :=
  order_matters_without_unique_clocks

/-! ## Alias groups -/

/-- Each gid's canonical gid is the smallest gid in its group. -/
theorem canonical_is_smallest (E : List Edge) (g : Gid) :
    Connected E g (canonical E g) ∧ ∀ h, Connected E g h → canonical E g ≤ h :=
  canonical_spec E g

/-- Two gids share a canonical gid exactly when aliases connect them. -/
theorem canonical_same_iff (E : List Edge) (a b : Gid) :
    canonical E a = canonical E b ↔ Connected E a b :=
  canonical_eq_iff E a b

/-- Swapping two neighbouring aliases anywhere leaves every canonical gid the same. -/
theorem groups_swap (pre post : List Edge) (x y : Edge) :
    canonical (pre ++ x :: y :: post) = canonical (pre ++ y :: x :: post) :=
  canonical_perm (List.Perm.append_left pre (List.Perm.swap y x post))

/-- An alias received twice in a row groups gids as receiving it once. -/
theorem groups_twice (pre post : List Edge) (x : Edge) :
    canonical (pre ++ x :: x :: post) = canonical (pre ++ x :: post) :=
  canonical_eq_of_mem_iff _ _ fun e => by simp

/-- Groups depend only on the set of aliases seen. -/
theorem groups_set (E₁ E₂ : List Edge) (same : ∀ e, e ∈ E₁ ↔ e ∈ E₂) :
    canonical E₁ = canonical E₂ :=
  canonical_eq_of_mem_iff E₁ E₂ same

/-! ## The view -/

/-- Replicas whose registers, gids and groups agree show the same view. -/
theorem view_from_inputs (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (h : inputs sc cs₁ = inputs sc cs₂) : view sc cs₁ = view sc cs₂ :=
  FongoSync.view_from_inputs sc h

/-- Swapping two neighbouring changes leaves the view the same, unless they write one register at
one clock with different values. -/
theorem view_swap_writes (sc : Schema) (pre post : List (Change Value)) (a b : Change Value)
    (h : Compatible a b) :
    view sc (pre ++ a :: b :: post) = view sc (pre ++ b :: a :: post) :=
  view_swap sc pre post a b (.inr (.inr h))

/-- Swapping an alias with a neighbouring change leaves the view the same. -/
theorem view_swap_alias (sc : Schema) (pre post : List (Change Value)) (a b : Change Value)
    (h : a.field = ALIAS) :
    view sc (pre ++ a :: b :: post) = view sc (pre ++ b :: a :: post) :=
  view_swap sc pre post a b (.inl h)

/-- Receiving a change twice in a row leaves the view as receiving it once. -/
theorem view_twice (sc : Schema) (pre post : List (Change Value)) (a : Change Value) :
    view sc (pre ++ a :: a :: post) = view sc (pre ++ a :: post) :=
  FongoSync.view_twice sc pre post a

/-- **The view depends only on the set of changes**, given unique clocks per register. -/
theorem view_set (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁) :
    view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc same unique

/-! ## The view over several collections, with `together` sets -/

/-- **The view over several collections depends only on the set of changes**, given unique
clocks per register. -/
theorem full_view_set (sc : Full.Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) (unique : ClockDetermines cs₁) :
    Full.view sc cs₁ = Full.view sc cs₂ :=
  Full.view_eq_of_mem_iff sc same unique

/-- A change whose clock the text form cannot hold is ignored. -/
theorem full_view_ignores_unencodable (sc : Full.Schema) (c : Change Value)
    (cs : List (Change Value)) (h : ¬ c.clock.encodable) :
    Full.view sc (c :: cs) = Full.view sc cs :=
  Full.view_ignores_unencodable sc c cs h

/-- A change to a collection the schema does not declare is ignored. -/
theorem full_view_ignores_unknown_collection (sc : Full.Schema) (c : Change Value)
    (cs : List (Change Value)) (h : sc.find c.collection = none) :
    Full.view sc (c :: cs) = Full.view sc cs :=
  Full.view_ignores_unknown_collection sc c cs h

/-- An alias that does not name another gid is ignored. -/
theorem full_view_ignores_bad_alias (sc : Full.Schema) (c : Change Value)
    (cs : List (Change Value)) (h : Full.aliasOk c = false) :
    Full.view sc (c :: cs) = Full.view sc cs :=
  Full.view_ignores_bad_alias sc c cs h

end FongoSync.Laws
