import FongoSync.View

/-!
# The Rust unit tests, checked against the model

Each `example` restates a test from `crates/core` and is checked by evaluation, so the model's
definitions are known to compute what the Rust computes on these inputs.
-/

namespace FongoSync

/-- `the_later_write_wins_and_the_greater_value_wins` in `change.rs`. -/
example :
    let clock (wall : Nat) : Hlc := ⟨wall, 0, 1⟩
    let change (field : String) (value : Int) (wall : Nat) : Change Value :=
      ⟨"things", 0, field, .int value, clock wall⟩
    let σ : String → String → Strategy := fun _ f => if f = "high" then .max else .lww
    let state := merged σ
      [change "title" 9 1, change "title" 2 5, change "high" 9 1, change "high" 2 5]
    (state ("things", 0, "title")).map (·.value) = some (.int 2) ∧
      (state ("things", 0, "high")).map (·.value) = some (.int 9) := by
  decide

/-- A `$deleted` field merges `Lww` whatever the caller asks for. -/
example :
    let σ : String → String → Strategy := fun _ _ => .max
    let state := merged σ
      [⟨"things", 0, DELETED, .int 1, ⟨1, 0, 1⟩⟩, ⟨"things", 0, DELETED, Value.int 0, ⟨2, 0, 1⟩⟩]
    (state ("things", 0, DELETED)).map (·.value) = some (.int 0) := by
  decide

/-- `readings_only_increase_even_when_the_wall_goes_backwards` in `clock.rs`. -/
example :
    let c₀ : Clock := ⟨⟨0, 0, 1⟩⟩
    let (a, c₁) := c₀.tick 1000
    let (b, c₂) := c₁.tick 900
    let (c, c₃) := c₂.tick 1000
    let (d, _) := c₃.tick 1001
    a < b ∧ b < c ∧ c < d ∧ d.counter = 0 := by
  decide

/-- `a_full_counter_carries_into_the_wall` in `clock.rs`. -/
example : (((⟨⟨5, COUNTER_MAX, 1⟩⟩ : Clock).tick 5).1.wall, ((⟨⟨5, COUNTER_MAX, 1⟩⟩ : Clock).tick 5).1.counter) = (6, 0) := by
  decide

/-- Groups join through any path of edges, and the smallest member names the group. -/
example :
    let E : List Edge := [(5, 3), (9, 7), (3, 9), (12, 11)]
    canonical E 7 = 3 ∧ canonical E 5 = 3 ∧ canonical E 12 = 11 ∧ canonical E 4 = 4 := by
  decide

/-- Unique clocks are needed: two different `Lww` values at one clock give an answer that depends
on the order they arrived in. -/
theorem order_matters_without_unique_clocks :
    let σ : String → String → Strategy := fun _ _ => .lww
    let a : Change Value := ⟨"things", 0, "title", .int 1, ⟨1, 0, 1⟩⟩
    let b : Change Value := ⟨"things", 0, "title", .int 2, ⟨1, 0, 1⟩⟩
    merged σ [a, b] ("things", 0, "title") ≠ merged σ [b, a] ("things", 0, "title") := by
  decide

end FongoSync
