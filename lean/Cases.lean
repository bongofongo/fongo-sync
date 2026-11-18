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

def main : IO Unit := do
  for (name, cs) in cases do
    IO.println s!"case {name}"
    for line in render cs do
      IO.println line
