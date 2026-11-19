import FongoSync.Run

/-!
Prints `runView` for a fixed set of cases. `crates/core/examples/view_cases.rs` prints
`model::view` for the same cases in the same form, and `make lean-diff` compares the two.
-/

open FongoSync

def schema : Schema :=
  ⟨"things", [⟨"high", .plain, .max⟩, ⟨"link", .reference, .lww⟩, ⟨"title", .plain, .lww⟩]⟩

def ch (gid : Nat) (field : String) (value : Value) (wall : Nat) : Change Value :=
  ⟨"things", gid, field, value, ⟨wall, 0, 9⟩⟩

def cases : List (String × List (Change Value)) := [
  ("later write wins", [ch 1 "title" (.text "a") 1, ch 1 "title" (.text "b") 2]),
  ("greater value wins", [ch 1 "high" (.int 3) 2, ch 1 "high" (.int 9) 1]),
  ("alias joins", [ch 1 "title" (.text "a") 1, ch 2 "title" (.text "b") 2, ch 2 ALIAS (.ref 1) 3]),
  ("deleted", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 1) 2]),
  ("restored", [ch 1 "title" (.text "a") 1, ch 1 DELETED (.int 1) 2, ch 1 DELETED (.int 0) 3]),
  ("deleted member", [ch 1 "title" (.text "a") 1, ch 2 "title" (.text "b") 2,
    ch 2 ALIAS (.ref 1) 3, ch 2 DELETED (.int 1) 4]),
  ("references", [ch 1 "link" (.ref 3) 1, ch 2 "title" (.text "x") 2, ch 3 ALIAS (.ref 2) 3,
    ch 4 "link" (.ref 7) 4, ch 5 "link" (.int 1) 5]),
  ("reference to deleted", [ch 1 "link" (.ref 2) 1, ch 2 DELETED (.int 1) 2]),
  ("ignored aliases", [ch 1 "title" (.text "a") 1, ch 1 ALIAS (.ref 1) 2, ch 2 ALIAS (.ref 8) 3,
    ch 3 ALIAS (.int 1) 4]),
  ("chain", [ch 5 "title" (.text "e") 1, ch 4 ALIAS (.ref 5) 2, ch 3 ALIAS (.ref 4) 3,
    ch 3 "high" (.int 2) 4, ch 4 "high" (.text "z") 5])
]

def showValue : Value → String
  | .null => "null"
  | .int i => s!"int:{i}"
  | .text s => s!"text:{s}"
  | .blob b => s!"blob:{b}"
  | .ref g => s!"ref:{g}"

def render (cs : List (Change Value)) : List String :=
  (known schema cs).filterMap fun r =>
    (runView schema cs r).map fun row =>
      s!"{r} " ++ " ".intercalate (row.map fun (f, v) => s!"{f}={showValue v}")

def reading (wall counter node : Nat) : Hlc := ⟨wall, counter, node⟩

/-- A clock's start and what it is asked to do. -/
def clockCases : List (String × Hlc × List Event) := [
  ("ticks and observes", (reading 0 0 1, [.tick 1000, .tick 900, .observe (reading 5000 3 2),
    .tick 10, .observe (reading 4000 0 2), .tick 5001])),
  ("full counter carries", (reading 5 COUNTER_MAX 1, [.tick 5, .tick 0])),
  ("carries into the last millisecond", (reading (WALL_MAX - 1) COUNTER_MAX 1, [.tick 0, .tick 0])),
  ("the last millisecond is not observed", (reading 0 0 1, [.tick 1000,
    .observe (reading WALL_MAX 5 7), .tick 1000, .observe (reading WALL_MAX COUNTER_MAX 7),
    .tick 1000])),
  ("beyond the text form is not observed", (reading WALL_MAX 5 1,
    [.observe (reading (WALL_MAX + 1) 0 2), .tick 0, .observe (reading (2 ^ 64 - 1) 0 2), .tick 0])),
  ("a clock that issued every reading repeats the last", (reading WALL_MAX COUNTER_MAX 1,
    [.tick 0, .tick WALL_MAX, .tick (2 ^ 64 - 1)]))
]

def showReading (h : Hlc) : String := s!"{h.wall} {h.counter} {h.node}"

/-- After each event, the reading a tick issued or the clock after an observation. -/
def runClock (c : Clock) : List Event → List String
  | [] => []
  | .tick wall :: rest =>
    let (r, c') := c.tick wall
    s!"tick {wall}: {showReading r}" :: runClock c' rest
  | .observe remote :: rest =>
    let c' := c.observe remote
    s!"observe {showReading remote}: {showReading c'.last}" :: runClock c' rest

def main : IO Unit := do
  for (name, cs) in cases do
    IO.println s!"case {name}"
    for line in render cs do
      IO.println line
  for (name, start, events) in clockCases do
    IO.println s!"clock {name}"
    for line in runClock ⟨start⟩ events do
      IO.println line
