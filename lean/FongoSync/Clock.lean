/-!
# Hybrid logical clocks

Mirrors `crates/core/src/clock.rs`. Rust's `u64` wall time and `u32` counter are `Nat` here; the
counter's `u32` bound is kept explicitly (`COUNTER_MAX`), because `tick` carries into the wall
when the counter is full. The `u64` wall is not bounded: the Rust `wall_ms += 1` can only overflow
for a clock resumed at `u64::MAX`, which this model does not cover.

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
    else
      { c.last with wall := c.last.wall + 1, counter := 0 }
  (last, ⟨last⟩)

/-- `Clock::observe`. The Rust compares `(wall_ms, counter)` tuples; the node is ignored. -/
def Clock.observe (c : Clock) (remote : Hlc) : Clock :=
  if remote.wall > c.last.wall ∨ remote.wall = c.last.wall ∧ remote.counter > c.last.counter then
    ⟨{ c.last with wall := min remote.wall WALL_MAX, counter := remote.counter }⟩
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

theorem Hlc.notAfter_trans {a b c : Hlc} (h₁ : a.NotAfter b) (h₂ : b.NotAfter c) :
    a.NotAfter c := by
  unfold Hlc.NotAfter at *; omega

theorem Hlc.notAfter_refl (a : Hlc) : a.NotAfter a := by
  unfold Hlc.NotAfter; omega

/-- A tick's reading is later than the clock's last reading, whatever `wall` says. -/
theorem Clock.tick_before (c : Clock) (wall : Nat) : c.last.Before (c.tick wall).1 := by
  unfold Clock.tick Hlc.Before
  simp only
  split
  · simp_all
  · split <;> simp <;> omega

theorem Clock.tick_lt (c : Clock) (wall : Nat) : c.last < (c.tick wall).1 :=
  Hlc.lt_of_before (c.tick_before wall)

/-- The clock remembers the reading it issued. -/
theorem Clock.tick_last (c : Clock) (wall : Nat) : (c.tick wall).2.last = (c.tick wall).1 := rfl

/-- A tick keeps the node's own id. -/
theorem Clock.tick_node (c : Clock) (wall : Nat) : (c.tick wall).1.node = c.last.node := by
  unfold Clock.tick; simp only; split
  · rfl
  · split <;> rfl

/-- Observing a remote reading whose wall time `Hlc::encode` can write never moves the clock
back, and leaves it at or past the remote reading. -/
theorem Clock.observe_notAfter (c : Clock) (remote : Hlc) (h : remote.wall ≤ WALL_MAX) :
    c.last.NotAfter (c.observe remote).last ∧ remote.NotAfter (c.observe remote).last := by
  unfold Clock.observe Hlc.NotAfter
  split
  · simp only; omega
  · omega

/-- The next tick after observing such a remote reading is later than it. -/
theorem Clock.observe_then_tick (c : Clock) (remote : Hlc) (wall : Nat)
    (h : remote.wall ≤ WALL_MAX) : remote < ((c.observe remote).tick wall).1 :=
  Hlc.lt_of_before <| Hlc.before_of_notAfter_of_before
    (c.observe_notAfter remote h).2 ((c.observe remote).tick_before wall)

/-- What a clock is asked to do. -/
inductive Event where
  | tick (wall : Nat)
  | observe (remote : Hlc)

/-- Runs `events` in order: the clock afterwards, and every reading it issued or observed. -/
def Clock.run (c : Clock) : List Event → Clock × List Hlc
  | [] => (c, [])
  | .tick wall :: rest =>
    let (reading, c') := c.tick wall
    let (final, seen) := c'.run rest
    (final, reading :: seen)
  | .observe remote :: rest =>
    let (final, seen) := (c.observe remote).run rest
    (final, remote :: seen)

/-- Every remote reading in `events` has a wall time `Hlc::encode` can write. -/
def Encodable : List Event → Prop
  | [] => True
  | .tick _ :: rest => Encodable rest
  | .observe remote :: rest => remote.wall ≤ WALL_MAX ∧ Encodable rest

theorem Clock.run_notAfter (c : Clock) (events : List Event) (h : Encodable events) :
    c.last.NotAfter (c.run events).1.last ∧ ∀ r ∈ (c.run events).2, r.NotAfter (c.run events).1.last := by
  induction events generalizing c with
  | nil => exact ⟨Hlc.notAfter_refl _, by simp [Clock.run]⟩
  | cons e rest ih =>
    cases e with
    | tick wall =>
      have ⟨h₁, h₂⟩ := ih (c.tick wall).2 h
      have step : c.last.NotAfter (c.tick wall).2.last := by
        have := c.tick_before wall
        unfold Hlc.Before at this; unfold Hlc.NotAfter; rw [Clock.tick_last]; omega
      refine ⟨Hlc.notAfter_trans step h₁, ?_⟩
      intro r hr
      simp only [Clock.run, List.mem_cons] at hr
      rcases hr with rfl | hr
      · rw [← Clock.tick_last]; exact h₁
      · exact h₂ r hr
    | observe remote =>
      have ⟨hw, hrest⟩ := h
      have ⟨h₁, h₂⟩ := ih (c.observe remote) hrest
      have ⟨s₁, s₂⟩ := c.observe_notAfter remote hw
      refine ⟨Hlc.notAfter_trans s₁ h₁, ?_⟩
      intro r hr
      simp only [Clock.run, List.mem_cons] at hr
      rcases hr with rfl | hr
      · exact Hlc.notAfter_trans s₂ h₁
      · exact h₂ r hr

/-- **The clock law.** After any history of ticks and observations of encodable remote readings,
the next tick is later than the clock's starting point and than every reading issued or observed
along the way, whatever wall time it is given. -/
theorem Clock.tick_after_any_history (c : Clock) (events : List Event) (wall : Nat)
    (h : Encodable events) :
    c.last < ((c.run events).1.tick wall).1 ∧
      ∀ r ∈ (c.run events).2, r < ((c.run events).1.tick wall).1 := by
  have ⟨h₁, h₂⟩ := c.run_notAfter events h
  have t := (c.run events).1.tick_before wall
  exact ⟨Hlc.lt_of_before (Hlc.before_of_notAfter_of_before h₁ t),
    fun r hr => Hlc.lt_of_before (Hlc.before_of_notAfter_of_before (h₂ r hr) t)⟩

/-! ## Where the law needs its hypothesis

`Clock::observe` clamps the remote wall time to `WALL_MAX` but compares it unclamped. A remote
reading past `WALL_MAX` therefore moves the clock back, and the next tick repeats a reading below
one already issued. Wire decoding accepts any `u64` wall time, so the hypothesis `Encodable` is a
real obligation on callers. -/

theorem Clock.observe_can_move_back :
    let c : Clock := ⟨⟨WALL_MAX, 5, 1⟩⟩
    let remote : Hlc := ⟨WALL_MAX + 1, 0, 2⟩
    (c.observe remote).last < c.last ∧ ((c.observe remote).tick 0).1 < c.last := by
  decide

end FongoSync
