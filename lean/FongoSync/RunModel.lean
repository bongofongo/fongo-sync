import FongoSync.Model
import FongoSync.Run

/-!
# The view over several collections, runnable

`Full.view` states the key a natural gid takes by choice, the groups as the least relation closed
under aliases and equal keys, and liveness as the least set closed under a rule. None of these can
be run. `runView` computes each instead, the way `model::view` does:

- the key is the first, in ascending order of each key field's values, that derives the gid;
- the groups are found by rounds of joining gids whose keys agree, as many rounds as there are
  gids, each round reading references through the groups of the round before;
- liveness is the rule applied as many times as there are gids.

`runView` gives the rows from these inputs exactly as `Full.view` gives them from its own:
`runView_eq` proves that its liveness is `Live`. That its inputs are `Full.inputs` is not proven;
`make lean-diff` compares the result with `model::view` on fixed cases.
-/

namespace FongoSync.Full

open Std

set_option linter.unusedSectionVars false

/-- `Value::total_cmp` without `Real`. -/
def valueCmp (a b : Value) : Ordering := compare a b

instance : TransCmp valueCmp := inferInstanceAs (TransCmp (compare : Value → Value → Ordering))

instance : LawfulEqCmp valueCmp := inferInstanceAs (LawfulEqCmp (compare : Value → Value → Ordering))

section RunInputs

/-! In this section `cs` is a list of candidate changes. -/

variable (sc : Schema) (cs : List (Change Value))

/-- The values the counted writes gave field `f` of `g`, ascending, each once. -/
def keyOptions (g : Gid) (f : String) : List Value :=
  ordSet valueCmp ((earliest (keyWrites sc cs g f)).map (·.2))

/-- The first key, one value from each list in turn, that derives `g`. -/
def firstKey (name : String) (g : Gid) : List (List Value) → List Value → Option (List Value)
  | [], chosen => if sc.derive name chosen = g then some chosen else none
  | options :: rest, chosen => options.findSome? fun v => firstKey name g rest (chosen ++ [v])

def runKey (g : Gid) : Option (List Value) :=
  match homeCollection sc cs g with
  | some c =>
    match c.natural with
    | some fields => firstKey sc c.name g (fields.map (keyOptions sc cs g)) []
    | none => none
  | none => none

def runUnverified (g : Gid) : Bool :=
  match homeCollection sc cs g with
  | some c => c.natural.isSome && (runKey sc cs g).isNone
  | none => false

def runKeyFieldOk (x : Change Value) : Bool :=
  match homeCollection sc cs x.gid with
  | some c =>
    match c.natural with
    | some fields =>
      match fields.findIdx? (· = x.field) with
      | some i =>
        decide ((x.clock, x.value) ∈ earliest (keyWrites sc cs x.gid x.field)) &&
          match runKey sc cs x.gid with
          | some k => k[i]? == some x.value
          | none => false
      | none => true
    | none => true
  | none => true

def runAccepted : List (Change Value) := (claimed sc cs).filter (runKeyFieldOk sc cs)

def runRegisters : State Value :=
  merged sc.strategyOf ((runAccepted sc cs).filter fun c => c.field ≠ ALIAS)

def runEdges : List Edge :=
  (runAccepted sc cs).filterMap fun c =>
    if c.field = ALIAS then
      match c.value with
      | .ref t =>
        if home sc cs t = some c.collection ∧ runUnverified sc cs c.gid = false ∧
            runUnverified sc cs t = false then
          some (c.gid, t)
        else none
      | _ => none
    else none

/-- A value with every reference read as its group's canonical gid. -/
def throughGroups (canon : Gid → Gid) : Value → Value
  | .ref g => .ref (canon g)
  | v => v

/-- `a` and `b` are verified gids of one natural collection whose keys agree once references are
read through `canon`. -/
def keysAgree (canon : Gid → Gid) (a b : Gid) : Bool :=
  match homeCollection sc cs a with
  | some c =>
    match c.natural with
    | some fields =>
      home sc cs b == home sc cs a && !runUnverified sc cs a && !runUnverified sc cs b &&
        fields.all fun f =>
          match runRegisters sc cs (c.name, a, f), runRegisters sc cs (c.name, b, f) with
          | some ea, some eb => throughGroups canon ea.value == throughGroups canon eb.value
          | _, _ => false
    | none => false
  | none => false

/-- The edges after `k` rounds: the aliases, and the pairs whose keys agree through the groups of
the round before. -/
def groupEdges : Nat → List Edge
  | 0 => runEdges sc cs
  | k + 1 =>
    let canon := canonical (groupEdges k)
    runEdges sc cs ++ (gids sc cs).flatMap fun a =>
      (gids sc cs).filterMap fun b => if keysAgree sc cs canon a b then some (a, b) else none

def runCanon : Gid → Gid := canonical (groupEdges sc cs (gids sc cs).length)

/-- The inputs, computed. -/
structure RunInputs where
  registers : State Value
  home : Gid → Option String
  gids : List Gid
  canon : Gid → Gid
  unverified : Gid → Bool

def runInputs : RunInputs :=
  ⟨runRegisters sc cs, home sc cs, gids sc cs, runCanon sc cs, runUnverified sc cs⟩

end RunInputs

def RunInputs.toInputs (inp : RunInputs) : Inputs :=
  ⟨inp.registers, inp.home, inp.gids, inp.canon, fun g => inp.unverified g = true⟩

/-! ## Liveness, computed -/

section Live

variable (sc : Schema) (inp : RunInputs)

/-- The rule, given the members found live so far. -/
def ruleB (live : Gid → Bool) (g : Gid) : Bool :=
  inp.gids.contains g && !inp.unverified g &&
    !(match register inp.toInputs g DELETED with
      | some e => e.value == .int 1
      | none => false) &&
    match (inp.home g).bind sc.find with
    | some c =>
      c.fields.all fun f =>
        match f.kind with
        | .owner t =>
          match register inp.toInputs g f.name with
          | none => true
          | some e =>
            match e.value with
            | .null => true
            | .ref o =>
              inp.home o == some t && inp.gids.any fun m => inp.canon m == inp.canon o && live m
            | _ => false
        | _ => true
    | none => false

/-- The members found live after `k` rounds. -/
def liveRound : Nat → List Gid
  | 0 => []
  | k + 1 =>
    let before := liveRound k
    inp.gids.filter fun g => before.contains g || ruleB sc inp (before.contains ·) g

def runLive (g : Gid) : Bool := (liveRound sc inp inp.gids.length).contains g

end Live

/-- **The view over several collections, computed.** -/
def runView (sc : Schema) (cs : List (Change Value)) : String → Gid → Option Row :=
  let inp := runInputs sc (candidates sc cs)
  presentWith sc inp.toInputs (fun g => runLive sc inp g = true)

end FongoSync.Full
