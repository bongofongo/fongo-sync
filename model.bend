# A model of the merge rules in crates/core, small enough to prove things about.
# A clock is a Nat here; in the Rust it is an Hlc, which is totally ordered the same way.

import Base

# What a replica holds for one field: the value and the clock of the write that set it.
type Entry is Data:
  Entry{clock: Nat, value: Nat}

def Entry.clock(e: Entry) -> Nat:
  match e:
    case Entry{clock, value}:
      clock

# `y` when clock `b` is later than clock `a`, else `x`. A tie keeps `x`.
def later(-T: Type, a: Nat, b: Nat, x: T, y: T) -> T:
  match a b:
    case 0n 0n:
      x
    case 0n 1n+q:
      y
    case 1n+p 0n:
      x
    case 1n+p 1n+q:
      later(T, p, q, x, y)

# `wins` under `Strategy::Lww`: the incoming write replaces the held one only if it is later.
def merge(+held: Entry, +incoming: Entry) -> Entry:
  later(Entry, Entry.clock(held), Entry.clock(incoming), held, incoming)

# `State::apply` for one field, which may hold nothing yet.
def apply(held: Maybe<&2, Entry>, +incoming: Entry) -> Maybe<&2, Entry>:
  match held:
    case None{}:
      Some{incoming}
    case Some{+value}:
      Some{merge(value, incoming)}

# A clock reading without its node: wall time, then a counter.
type Stamp is Data:
  Stamp{wall: Nat, counter: Nat}

def lex(c: Cmp, d: Cmp) -> Cmp:
  match c:
    case LT{}:
      LT{}
    case EQ{}:
      d
    case GT{}:
      GT{}

def Stamp.cmp(a: Stamp, b: Stamp) -> Cmp:
  match a b:
    case Stamp{aw, ak} Stamp{bw, bk}:
      lex(Nat.cmp(aw, bw), Nat.cmp(ak, bk))

# `Clock::tick`: a wall time past the last reading starts a new count, else the count goes up.
def tick(+last: Stamp, +now: Nat) -> Stamp:
  match last:
    case Stamp{wall, counter}:
      later(Stamp, wall, now, Stamp{wall, 1n+counter}, Stamp{now, 0n})
