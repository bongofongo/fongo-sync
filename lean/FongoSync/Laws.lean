import FongoSync.Examples
import FongoSync.Model
import FongoSync.Run
import FongoSync.RunModel

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

/-- A tick's reading is later than the clock's last reading, whatever the wall says, unless the
clock has issued the last reading the text form holds. -/
theorem tick_later (c : Clock) (wall : Nat) (room : c.HasRoom) : c.last < (c.tick wall).1 :=
  c.tick_lt wall room

/-- A clock whose last reading the text form holds issues only readings it holds. -/
theorem tick_encodable (c : Clock) (wall : Nat) (h : c.last.encodable) :
    (c.tick wall).1.encodable :=
  c.tick_encodable wall h

/-- Observing a reading the text form cannot hold changes nothing. -/
theorem observe_ignores_unencodable (c : Clock) (remote : Hlc) (h : ¬ remote.encodable) :
    c.observe remote = c :=
  c.observe_ignores remote h

/-- Observing never takes away a clock's room to tick. -/
theorem observe_keeps_room (c : Clock) (remote : Hlc) (room : c.HasRoom) :
    (c.observe remote).HasRoom :=
  c.observe_hasRoom remote room

/-- The next tick after observing a reading before the last millisecond the text form holds is
later than it. -/
theorem tick_after_observe (c : Clock) (remote : Hlc) (wall : Nat) (h : remote.wall < WALL_MAX) :
    remote < ((c.observe remote).tick wall).1 :=
  c.observe_then_tick remote wall h

/-- After any history that leaves the clock room to tick, the next tick is later than the start,
than every reading issued, and than every reading observed before the last millisecond. -/
theorem tick_after_any_history (c : Clock) (events : List Event) (wall : Nat)
    (room : (c.run events).1.HasRoom) :
    c.last < ((c.run events).1.tick wall).1 ∧
      ∀ r ∈ (c.run events).2, r < ((c.run events).1.tick wall).1 :=
  c.tick_after_any_history events wall room

/-! ## One register, for any value type with a total order -/

section Register

variable {V : Type} [Ord V] [Std.TransOrd V] [Std.LawfulEqOrd V]

/-- Merging two registers is commutative, with no condition. -/
theorem merge_comm (s : Strategy) (a b : Option (Entry V)) :
    merge s a b = merge s b a :=
  FongoSync.merge_comm s a b

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

/-- Under `Lww`, of two changes at one clock the one with the greater value wins. -/
theorem lww_tie (held e : Entry V) (h : held.clock = e.clock) :
    apply .lww (some held) e = some (if compare held.value e.value = .lt then e else held) :=
  apply_lww_tie held e h

/-- Two changes may arrive in either order, under either strategy, with no condition. -/
theorem apply_any_order (s : Strategy) (h : Option (Entry V)) (a b : Entry V) :
    apply s (apply s h a) b = apply s (apply s h b) a :=
  FongoSync.apply_any_order s h a b

/-- Under `Max`, two changes may arrive in either order, with no condition. -/
theorem max_any_order (h : Option (Entry V)) (a b : Entry V) :
    apply .max (apply .max h a) b = apply .max (apply .max h b) a :=
  FongoSync.apply_any_order .max h a b

/-- A register fed the same set of entries ends the same, whatever the order and repeats. -/
theorem register_set (s : Strategy) (l₁ l₂ : List (Entry V)) (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    l₁.foldl (apply s) none = l₂.foldl (apply s) none :=
  fold_apply_eq_of_mem_iff s l₁ l₂ same

/-- What a `Lww` register fed a set of entries holds: one of them, at the latest clock among
them, and of the entries at that clock the one with the greatest value. -/
theorem lww_winner (l : List (Entry V)) (m : Entry V) (h : l.foldl (apply .lww) none = some m) :
    m ∈ l ∧ ∀ x ∈ l, x.clock < m.clock ∨ (x.clock = m.clock ∧ (compare x.value m.value).isLE) :=
  fold_lww_winner l m h

/-- What a `Max` register fed a set of entries holds: one of them, with the greatest value among
them, and of the entries with that value the one with the latest clock. -/
theorem max_winner (l : List (Entry V)) (m : Entry V) (h : l.foldl (apply .max) none = some m) :
    m ∈ l ∧ ∀ x ∈ l, compare x.value m.value = .lt ∨
      (x.value = m.value ∧ (compare x.clock m.clock).isLE) :=
  fold_max_winner l m h

/-! ## A replica's state -/

/-- Swapping two neighbouring changes anywhere in a delivery, from any state, leaves every
register the same. -/
theorem state_swap (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a b : Change V) :
    (pre ++ a :: b :: post).foldl (State.apply σ) st =
      (pre ++ b :: a :: post).foldl (State.apply σ) st :=
  State.foldl_swap σ st pre post a b

/-- Receiving a change twice in a row anywhere in a delivery is receiving it once. -/
theorem state_twice (σ : String → String → Strategy) (st : State V)
    (pre post : List (Change V)) (a : Change V) :
    (pre ++ a :: a :: post).foldl (State.apply σ) st =
      (pre ++ a :: post).foldl (State.apply σ) st :=
  State.foldl_twice σ st pre post a

/-- Replicas that applied the same set of changes hold the same state, even where two changes
to one register share a clock with different values. -/
theorem state_set (σ : String → String → Strategy) (cs₁ cs₂ : List (Change V))
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) :
    merged σ cs₁ = merged σ cs₂ :=
  merged_eq_of_mem_iff σ cs₁ cs₂ same

end Register

/-- Two different `Lww` values at one clock: the greater wins, whichever arrives first. -/
theorem equal_clocks_take_the_greater_value :
    let σ : String → String → Strategy := fun _ _ => .lww
    let a : Change Value := ⟨"things", 0, "title", .int 1, ⟨1, 0, 1⟩⟩
    let b : Change Value := ⟨"things", 0, "title", .int 2, ⟨1, 0, 1⟩⟩
    merged σ [a, b] ("things", 0, "title") = some ⟨.int 2, ⟨1, 0, 1⟩⟩ ∧
      merged σ [b, a] ("things", 0, "title") = some ⟨.int 2, ⟨1, 0, 1⟩⟩ :=
  FongoSync.equal_clocks_take_the_greater_value

/-! ## The order on values

Of the Rust `Value::total_cmp`, all but `Real`: a `Real` is compared with an `Int` exactly, which
the Rust tests check against an exact comparison of the two. -/

/-- Of two values, one is smaller, or they are the same value. -/
theorem value_total (a b : Value) : compare a b = .lt ∨ a = b ∨ compare b a = .lt :=
  Value.compare_total a b

/-- Values that compare equal are the same value. -/
theorem value_antisymm (a b : Value) (h : compare a b = .eq) : a = b :=
  Std.LawfulEqOrd.eq_of_compare h

/-- Comparing the other way round gives the opposite answer. -/
theorem value_swap (a b : Value) : compare a b = (compare b a).swap :=
  Std.OrientedCmp.eq_swap

/-- The order is transitive. -/
theorem value_trans (a b c : Value) (hab : compare a b = .lt) (hbc : compare b c = .lt) :
    compare a c = .lt :=
  Std.TransCmp.lt_trans hab hbc

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

/-- Swapping two neighbouring changes leaves the view the same, even where they write one
register at one clock with different values. -/
theorem view_swap_writes (sc : Schema) (pre post : List (Change Value)) (a b : Change Value) :
    view sc (pre ++ a :: b :: post) = view sc (pre ++ b :: a :: post) :=
  view_swap sc pre post a b

/-- Swapping an alias with a neighbouring change leaves the view the same. -/
theorem view_swap_alias (sc : Schema) (pre post : List (Change Value)) (a b : Change Value)
    (h : a.field = ALIAS) :
    view sc (pre ++ a :: b :: post) = view sc (pre ++ b :: a :: post) := by
  clear h; exact view_swap sc pre post a b

/-- Receiving a change twice in a row leaves the view as receiving it once. -/
theorem view_twice (sc : Schema) (pre post : List (Change Value)) (a : Change Value) :
    view sc (pre ++ a :: a :: post) = view sc (pre ++ a :: post) :=
  FongoSync.view_twice sc pre post a

/-- The view can be computed: `runView` applies the liveness rule once per gid and gives the
same answer. -/
theorem view_runs (sc : Schema) (cs : List (Change Value)) : runView sc cs = view sc cs :=
  runView_eq_view sc cs

/-- **The view depends only on the set of changes.** -/
theorem view_set (sc : Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) :
    view sc cs₁ = view sc cs₂ :=
  view_eq_of_mem_iff sc same

/-! ## The view over several collections, with `together` sets and natural keys -/

/-- Of the writes to a natural key field, the ones that count depend only on the set of writes,
not on the order they arrive in. -/
theorem key_writes_set {l₁ l₂ : List (Hlc × Value)} (same : ∀ x, x ∈ l₁ ↔ x ∈ l₂) :
    earliest l₁ = earliest l₂ :=
  earliest_eq_of_mem_iff same

/-- A write to a natural key field that arrives again does not change which writes count. -/
theorem key_write_redelivered (l : List (Hlc × Value)) (x : Hlc × Value) (h : x ∈ l) :
    earliest (x :: l) = earliest l :=
  earliest_redelivered l x h

/-- A write to a natural key field that does not count among the writes seen never counts once
more writes arrive, so a store may drop it for good. -/
theorem key_write_dropped_for_good {l₁ l₂ : List (Hlc × Value)} (sub : ∀ y ∈ l₁, y ∈ l₂)
    {x : Hlc × Value} (hx : x ∈ l₁) (out : x ∉ earliest l₁) : x ∉ earliest l₂ :=
  not_mem_earliest_of_subset sub hx out

/-- The view over several collections can be computed from computed inputs: `Full.runView` gives
the rows of those inputs as `Full.view` gives the rows of its own, liveness computed in as many
rounds as there are gids. That the computed inputs are `Full.view`'s is not proven here. -/
theorem full_view_runs_from_inputs (sc : Full.Schema) (cs : List (Change Value)) :
    Full.runView sc cs = Full.present sc (Full.runInputs sc (Full.candidates sc cs)).toInputs :=
  Full.runView_eq sc cs

/-- **The view over several collections depends only on the set of changes.** Where two changes
to one register share a clock with different values, the greater value counts, so writes to a
`together` set that share a clock can show a mix of the two. -/
theorem full_view_set (sc : Full.Schema) {cs₁ cs₂ : List (Change Value)}
    (same : ∀ c, c ∈ cs₁ ↔ c ∈ cs₂) :
    Full.view sc cs₁ = Full.view sc cs₂ :=
  Full.view_eq_of_mem_iff sc same

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
    (cs : List (Change Value)) (h : aliasOk c = false) :
    Full.view sc (c :: cs) = Full.view sc cs :=
  Full.view_ignores_bad_alias sc c cs h

/-- A `$deleted` whose value is neither 0 nor 1 is ignored. -/
theorem full_view_ignores_bad_tombstone (sc : Full.Schema) (c : Change Value)
    (cs : List (Change Value)) (h : deletedOk c = false) :
    Full.view sc (c :: cs) = Full.view sc cs :=
  Full.view_ignores_bad_tombstone sc c cs h

end FongoSync.Laws
