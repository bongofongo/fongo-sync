/-!
# Hybrid logical clocks

Mirrors `crates/core/src/clock.rs`. Rust's `u64` wall time and `u32` counter are `Nat` here; the
counter's `u32` bound is kept explicitly (`COUNTER_MAX`), because `tick` carries into the wall
when the counter is full.

A `Gid` is sixteen bytes compared lexicographically, which is the order of the big-endian number
they spell, so it is a `Nat` here.
-/

namespace FongoSync

abbrev Gid := Nat

/-- `Hlc { wall_ms, counter, node }`. -/
structure Hlc where
  wall : Nat
  counter : Nat
  node : Gid
  deriving DecidableEq, Repr

/-- `#[derive(Ord)]` on `Hlc`: by wall time, then counter, then node. -/
def Hlc.cmp (a b : Hlc) : Ordering :=
  (compare a.wall b.wall).then ((compare a.counter b.counter).then (compare a.node b.node))

instance : Ord Hlc := ⟨Hlc.cmp⟩

/-- `a < b` is `compare a b = .lt`, as in Rust. -/
instance : LT Hlc := ltOfOrd
instance : LE Hlc := leOfOrd

instance : Std.TransOrd Hlc :=
  inferInstanceAs <| Std.TransCmp
    (compareLex (compareOn Hlc.wall) (compareLex (compareOn Hlc.counter) (compareOn Hlc.node)))

instance : Std.LawfulEqOrd Hlc where
  eq_of_compare {a b} h := by
    change Hlc.cmp a b = .eq at h
    simp only [Hlc.cmp, Ordering.then_eq_eq, Nat.compare_eq_eq] at h
    cases a; cases b
    simp_all

theorem Hlc.lt_iff {a b : Hlc} :
    a < b ↔ a.wall < b.wall ∨ a.wall = b.wall ∧
      (a.counter < b.counter ∨ a.counter = b.counter ∧ a.node < b.node) := by
  change Hlc.cmp a b = .lt ↔ _
  simp only [Hlc.cmp, Ordering.then_eq_lt, Nat.compare_eq_lt, Nat.compare_eq_eq]

instance : DecidableRel (α := Hlc) (· < ·) := fun _ _ =>
  decidable_of_iff _ Hlc.lt_iff.symm

/-- The largest wall time `Hlc::encode` can write: twelve hex digits. -/
def WALL_MAX : Nat := 0xFFFF_FFFF_FFFF

/-- `u32::MAX`, the largest counter. -/
def COUNTER_MAX : Nat := 0xFFFF_FFFF

/-- `Clock { last }`: one node's clock. -/
structure Clock where
  last : Hlc
  deriving DecidableEq, Repr

/-- `Clock::tick`: the reading for an event at `wall`, and the clock afterwards. -/
def Clock.tick (c : Clock) (wall : Nat) : Hlc × Clock :=
  let wall := min wall WALL_MAX
  let last :=
    if wall > c.last.wall then
      { c.last with wall := wall, counter := 0 }
    else if c.last.counter + 1 ≤ COUNTER_MAX then
      { c.last with counter := c.last.counter + 1 }
    else if c.last.wall < WALL_MAX then
      { c.last with wall := c.last.wall + 1, counter := 0 }
    else
      c.last
  (last, ⟨last⟩)

/-- `Hlc::encodable`: the text form holds this reading. -/
def Hlc.encodable (h : Hlc) : Prop := h.wall ≤ WALL_MAX

instance (h : Hlc) : Decidable h.encodable := inferInstanceAs (Decidable (h.wall ≤ WALL_MAX))

/-- `Clock::observe`: moves past a reading from elsewhere, ignoring one the text form cannot
hold and one in the last millisecond it holds. The Rust compares `(wall_ms, counter)` tuples; the
node is ignored. -/
def Clock.observe (c : Clock) (remote : Hlc) : Clock :=
  if remote.wall < WALL_MAX ∧
      (remote.wall > c.last.wall ∨ remote.wall = c.last.wall ∧ remote.counter > c.last.counter) then
    ⟨{ c.last with wall := remote.wall, counter := remote.counter }⟩
  else
    c

/-! ## Laws -/

/-- A reading's `(wall, counter)` part, the part a clock advances. -/
def Hlc.Before (a b : Hlc) : Prop :=
  a.wall < b.wall ∨ a.wall = b.wall ∧ a.counter < b.counter

def Hlc.NotAfter (a b : Hlc) : Prop :=
  a.wall < b.wall ∨ a.wall = b.wall ∧ a.counter ≤ b.counter

theorem Hlc.lt_of_before {a b : Hlc} (h : a.Before b) : a < b := by
  rw [Hlc.lt_iff]; unfold Hlc.Before at h; omega

theorem Hlc.before_of_notAfter_of_before {a b c : Hlc} (h₁ : a.NotAfter b) (h₂ : b.Before c) :
    a.Before c := by
  unfold Hlc.NotAfter at h₁; unfold Hlc.Before at *; omega

theorem Hlc.before_of_before_of_notAfter {a b c : Hlc} (h₁ : a.Before b) (h₂ : b.NotAfter c) :
    a.Before c := by
  unfold Hlc.NotAfter at h₂; unfold Hlc.Before at *; omega

theorem Hlc.notAfter_trans {a b c : Hlc} (h₁ : a.NotAfter b) (h₂ : b.NotAfter c) :
    a.NotAfter c := by
  unfold Hlc.NotAfter at *; omega

theorem Hlc.notAfter_refl (a : Hlc) : a.NotAfter a := by
  unfold Hlc.NotAfter; omega

/-- The clock has not issued the last reading the text form holds, so its next tick is later. -/
def Clock.HasRoom (c : Clock) : Prop := c.last.wall < WALL_MAX ∨ c.last.counter < COUNTER_MAX

/-- A tick never moves the clock back, whatever `wall` says. -/
theorem Clock.tick_notAfter (c : Clock) (wall : Nat) : c.last.NotAfter (c.tick wall).1 := by
  unfold Clock.tick Hlc.NotAfter
  simp only
  split
  · simp_all
  · split
    · simp
    · split <;> simp <;> omega

/-- A tick's reading is later than the clock's last reading, if the clock has room. -/
theorem Clock.tick_before (c : Clock) (wall : Nat) (room : c.HasRoom) :
    c.last.Before (c.tick wall).1 := by
  unfold Clock.HasRoom at room
  unfold Clock.tick Hlc.Before
  simp only
  split
  · simp_all
  · split
    · simp
    · split <;> simp <;> omega

theorem Clock.tick_lt (c : Clock) (wall : Nat) (room : c.HasRoom) : c.last < (c.tick wall).1 :=
  Hlc.lt_of_before (c.tick_before wall room)

/-- A clock whose last reading the text form holds issues only readings it holds. -/
theorem Clock.tick_encodable (c : Clock) (wall : Nat) (h : c.last.encodable) :
    (c.tick wall).1.encodable := by
  unfold Hlc.encodable at *
  unfold Clock.tick
  simp only
  split
  · simp only; omega
  · split
    · exact h
    · split
      · simp only; omega
      · exact h

/-- The clock remembers the reading it issued. -/
theorem Clock.tick_last (c : Clock) (wall : Nat) : (c.tick wall).2.last = (c.tick wall).1 := rfl

/-- A tick keeps the node's own id. -/
theorem Clock.tick_node (c : Clock) (wall : Nat) : (c.tick wall).1.node = c.last.node := by
  unfold Clock.tick; simp only; split
  · rfl
  · split
    · rfl
    · split <;> rfl

/-- A reading the text form cannot hold is ignored. -/
theorem Clock.observe_ignores (c : Clock) (remote : Hlc) (h : ¬ remote.encodable) :
    c.observe remote = c := by
  unfold Hlc.encodable at h
  have : ¬ remote.wall < WALL_MAX := by omega
  simp [Clock.observe, this]

/-- Observing never moves the clock back, and leaves it at or past any remote reading before the
last millisecond. -/
theorem Clock.observe_notAfter (c : Clock) (remote : Hlc) :
    c.last.NotAfter (c.observe remote).last ∧
      (remote.wall < WALL_MAX → remote.NotAfter (c.observe remote).last) := by
  unfold Clock.observe Hlc.NotAfter
  split
  · rename_i h; dsimp only; omega
  · rename_i h; omega

/-- Observing never takes away a clock's room to tick. -/
theorem Clock.observe_hasRoom (c : Clock) (remote : Hlc) (room : c.HasRoom) :
    (c.observe remote).HasRoom := by
  unfold Clock.observe Clock.HasRoom at *
  split
  · rename_i h; simp only; omega
  · exact room

/-- The next tick after observing a reading before the last millisecond is later than it. -/
theorem Clock.observe_then_tick (c : Clock) (remote : Hlc) (wall : Nat)
    (h : remote.wall < WALL_MAX) : remote < ((c.observe remote).tick wall).1 := by
  have seen := (c.observe_notAfter remote).2 h
  apply Hlc.lt_of_before
  by_cases room : (c.observe remote).HasRoom
  · exact Hlc.before_of_notAfter_of_before seen ((c.observe remote).tick_before wall room)
  · have full : WALL_MAX ≤ (c.observe remote).last.wall := by
      unfold Clock.HasRoom at room; omega
    have early : remote.Before (c.observe remote).last := by unfold Hlc.Before; omega
    exact Hlc.before_of_before_of_notAfter early ((c.observe remote).tick_notAfter wall)

/-- What a clock is asked to do. -/
inductive Event where
  | tick (wall : Nat)
  | observe (remote : Hlc)

/-- Runs `events` in order: the clock afterwards, and every reading it issued or took into
account (the readings before the last millisecond it observed). -/
def Clock.run (c : Clock) : List Event → Clock × List Hlc
  | [] => (c, [])
  | .tick wall :: rest =>
    let (reading, c') := c.tick wall
    let (final, seen) := c'.run rest
    (final, reading :: seen)
  | .observe remote :: rest =>
    let (final, seen) := (c.observe remote).run rest
    (final, if remote.wall < WALL_MAX then remote :: seen else seen)

theorem Clock.run_notAfter (c : Clock) (events : List Event) :
    c.last.NotAfter (c.run events).1.last ∧ ∀ r ∈ (c.run events).2, r.NotAfter (c.run events).1.last := by
  induction events generalizing c with
  | nil => exact ⟨Hlc.notAfter_refl _, by simp [Clock.run]⟩
  | cons e rest ih =>
    cases e with
    | tick wall =>
      have ⟨h₁, h₂⟩ := ih (c.tick wall).2
      have step : c.last.NotAfter (c.tick wall).2.last := by
        rw [Clock.tick_last]; exact c.tick_notAfter wall
      refine ⟨Hlc.notAfter_trans step h₁, ?_⟩
      intro r hr
      simp only [Clock.run, List.mem_cons] at hr
      rcases hr with rfl | hr
      · rw [← Clock.tick_last]; exact h₁
      · exact h₂ r hr
    | observe remote =>
      have ⟨h₁, h₂⟩ := ih (c.observe remote)
      have ⟨s₁, s₂⟩ := c.observe_notAfter remote
      refine ⟨Hlc.notAfter_trans s₁ h₁, ?_⟩
      intro r hr
      simp only [Clock.run] at hr
      by_cases he : remote.wall < WALL_MAX
      · simp only [he, ↓reduceIte, List.mem_cons] at hr
        rcases hr with rfl | hr
        · exact Hlc.notAfter_trans (s₂ he) h₁
        · exact h₂ r hr
      · simp only [he, ↓reduceIte] at hr
        exact h₂ r hr

/-- **The clock law.** After any history of ticks and observations that leaves the clock room to
tick, the next tick is later than the clock's starting point, than every reading it issued, and
than every reading it took into account, whatever wall time it is given. -/
theorem Clock.tick_after_any_history (c : Clock) (events : List Event) (wall : Nat)
    (room : (c.run events).1.HasRoom) :
    c.last < ((c.run events).1.tick wall).1 ∧
      ∀ r ∈ (c.run events).2, r < ((c.run events).1.tick wall).1 := by
  have ⟨h₁, h₂⟩ := c.run_notAfter events
  have t := (c.run events).1.tick_before wall room
  exact ⟨Hlc.lt_of_before (Hlc.before_of_notAfter_of_before h₁ t),
    fun r hr => Hlc.lt_of_before (Hlc.before_of_notAfter_of_before (h₂ r hr) t)⟩

end FongoSync
